// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Push decoder stream implementation for SST parquet files.

use std::ops::Range;

use bytes::{Bytes, BytesMut};
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::ObjectStore;
use parquet::DecodeResult;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, RowSelection};
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use snafu::{ResultExt, ensure};

use crate::cache::file_cache::{FileType, IndexKey};
use crate::cache::{CacheStrategy, PageRangePart};
use crate::error::{OpenDalSnafu, ReadParquetSnafu, Result, UnexpectedSnafu};
use crate::metrics::{READ_STAGE_ELAPSED, READ_STAGE_FETCH_PAGES};
use crate::read::series_compact::{CompactReadMetrics, MeasuredKeyRead, measure_operation};
use crate::sst::file::RegionFileId;
use crate::sst::parquet::helper::fetch_byte_ranges;
use crate::sst::parquet::row_group::{ParquetFetchMetrics, compute_total_range_size};

/// Fetches parquet byte ranges through Greptime's cache hierarchy.
///
/// The push decoder decides which ranges are required for decoding, while this
/// fetcher keeps cache lookup, local write-cache reads, and remote I/O explicit
/// in Greptime code.
pub struct SstParquetRangeFetcher {
    read_metrics: Option<CompactReadMetrics>,
    key_read_bytes: Option<datafusion::physical_plan::metrics::Count>,
    compact_audit: Option<(
        usize,
        Range<u64>,
        crate::read::series_compact::CompactMetrics,
    )>,
    /// Region file ID for cache key.
    region_file_id: RegionFileId,
    /// Path to the parquet file in object storage.
    file_path: String,
    /// Object store for reading data.
    object_store: ObjectStore,
    /// Cache strategy for reading pages.
    cache_strategy: CacheStrategy,
    /// Row group index for cache key.
    row_group_idx: usize,
    /// Optional metrics for tracking fetch operations.
    fetch_metrics: Option<ParquetFetchMetrics>,
}

impl SstParquetRangeFetcher {
    /// Creates a new [SstParquetRangeFetcher].
    pub fn new(
        region_file_id: RegionFileId,
        file_path: String,
        object_store: ObjectStore,
        cache_strategy: CacheStrategy,
        row_group_idx: usize,
        fetch_metrics: Option<ParquetFetchMetrics>,
    ) -> Self {
        Self {
            read_metrics: None,
            compact_audit: None,
            key_read_bytes: None,
            region_file_id,
            file_path,
            object_store,
            cache_strategy,
            row_group_idx,
            fetch_metrics,
        }
    }

    pub(crate) fn with_key_read_metrics(mut self, metrics: Option<MeasuredKeyRead>) -> Self {
        if let Some((bytes, reads)) = metrics {
            self.key_read_bytes = Some(bytes);
            self.read_metrics = Some(reads);
        }
        self
    }

    pub(crate) fn with_compact_audit(
        mut self,
        column: usize,
        range: (u64, u64),
        metrics: crate::read::series_compact::CompactMetrics,
    ) -> Self {
        self.read_metrics = Some(metrics.data_read.clone());
        self.compact_audit = Some((column, range.0..range.0 + range.1, metrics));
        self
    }

    /// Fetches byte ranges from page cache, write cache, or object store.
    async fn fetch_bytes_with_cache(&self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        if let Some((_, key, metrics)) = &self.compact_audit
            && ranges
                .iter()
                .any(|r| r.start < key.end && key.start < r.end)
        {
            metrics.key_decode_violations.add(1);
            return UnexpectedSnafu {
                reason: "compact data decoder requested SST primary-key bytes",
            }
            .fail();
        }
        if let Some(bytes) = &self.key_read_bytes {
            bytes.add(ranges.iter().map(|r| (r.end - r.start) as usize).sum());
        }
        let _fetch_timer = self.read_metrics.as_ref().map(|m| m.fetch_elapsed.timer());
        if let Some(m) = &self.read_metrics {
            m.fetch_calls.add(1);
            m.requested_bytes
                .add(ranges.iter().map(|r| (r.end - r.start) as usize).sum());
        }
        let fetch_start = self
            .fetch_metrics
            .as_ref()
            .map(|_| std::time::Instant::now());
        let _timer = READ_STAGE_FETCH_PAGES.start_timer();

        let mut page_lookup =
            measure_operation(self.read_metrics.as_ref().map(|m| &m.cache_lookup), || {
                self.cache_strategy.get_page_ranges(
                    self.region_file_id.file_id(),
                    self.row_group_idx,
                    &ranges,
                )
            });
        if let (Some(m), Some(lookup)) = (&self.read_metrics, &page_lookup) {
            m.cache_bytes.add(lookup.cached_bytes as usize);
        }
        if let Some(lookup) = &page_lookup
            && lookup.cached_bytes > 0
            && let Some(metrics) = &self.fetch_metrics
        {
            let mut metrics_data = metrics.data.lock().unwrap();
            metrics_data.page_cache_hit += 1;
            metrics_data.pages_to_fetch_mem += lookup.cached_range_count;
            metrics_data.page_size_to_fetch_mem += lookup.cached_bytes;
            metrics_data.page_size_needed += lookup.cached_bytes;
        }

        // Fast path: all requested ranges can be assembled from cached fragments.
        if page_lookup
            .as_ref()
            .map(|lookup| lookup.is_fully_cached())
            .unwrap_or(false)
        {
            let lookup = page_lookup.take().unwrap();
            if let Some(metrics) = &self.fetch_metrics
                && let Some(start) = fetch_start
            {
                metrics.data.lock().unwrap().total_fetch_elapsed += start.elapsed();
            }
            return measure_operation(
                self.read_metrics.as_ref().map(|m| &m.range_assembly),
                || assemble_ranges(&ranges, lookup.cached_parts, &[]),
            );
        }

        let missing_ranges = page_lookup
            .as_ref()
            .map(|lookup| lookup.missing_ranges.clone())
            .unwrap_or_else(|| ranges.clone());

        // Calculate total range size for metrics.
        let (_, unaligned_size) = compute_total_range_size(&missing_ranges);

        // Check write cache.
        let key = IndexKey::new(
            self.region_file_id.region_id(),
            self.region_file_id.file_id(),
            FileType::Parquet,
        );
        let fetch_write_cache_start = self
            .fetch_metrics
            .as_ref()
            .map(|_| std::time::Instant::now());
        let write_timer = self
            .read_metrics
            .as_ref()
            .map(|m| m.write_cache_elapsed.timer());
        let write_cache_result = match self.cache_strategy.write_cache() {
            Some(cache) => cache.file_cache().read_ranges(key, &missing_ranges).await,
            None => None,
        };

        drop(write_timer);
        let fetched_pages = match write_cache_result {
            Some(data) => {
                if let Some(m) = &self.read_metrics {
                    m.write_cache_bytes.add(data.iter().map(|b| b.len()).sum());
                }
                if let Some(metrics) = &self.fetch_metrics {
                    let elapsed = fetch_write_cache_start
                        .map(|start| start.elapsed())
                        .unwrap_or_default();
                    let range_size_needed: u64 =
                        missing_ranges.iter().map(|r| r.end - r.start).sum();
                    let mut metrics_data = metrics.data.lock().unwrap();
                    metrics_data.write_cache_fetch_elapsed += elapsed;
                    metrics_data.write_cache_hit += 1;
                    metrics_data.pages_to_fetch_write_cache += missing_ranges.len();
                    metrics_data.page_size_to_fetch_write_cache += unaligned_size;
                    metrics_data.page_size_needed += range_size_needed;
                }
                data
            }
            None => {
                // Fetch data from object store.
                let _timer = READ_STAGE_ELAPSED
                    .with_label_values(&["cache_miss_read"])
                    .start_timer();

                let start = self
                    .fetch_metrics
                    .as_ref()
                    .map(|_| std::time::Instant::now());
                let store_timer = self.read_metrics.as_ref().map(|m| m.store_elapsed.timer());
                if let Some(m) = &self.read_metrics {
                    m.store_calls.add(1);
                }
                let data =
                    fetch_byte_ranges(&self.file_path, self.object_store.clone(), &missing_ranges)
                        .await
                        .context(OpenDalSnafu)?;
                drop(store_timer);
                if let Some(m) = &self.read_metrics {
                    m.store_bytes.add(data.iter().map(|b| b.len()).sum());
                }

                if let Some(metrics) = &self.fetch_metrics {
                    let elapsed = start.map(|start| start.elapsed()).unwrap_or_default();
                    let range_size_needed: u64 =
                        missing_ranges.iter().map(|r| r.end - r.start).sum();
                    let mut metrics_data = metrics.data.lock().unwrap();
                    metrics_data.store_fetch_elapsed += elapsed;
                    metrics_data.cache_miss += 1;
                    metrics_data.pages_to_fetch_store += missing_ranges.len();
                    metrics_data.page_size_to_fetch_store += unaligned_size;
                    metrics_data.page_size_needed += range_size_needed;
                }
                data
            }
        };
        ensure!(
            fetched_pages.len() == missing_ranges.len(),
            UnexpectedSnafu {
                reason: format!(
                    "Invalid parquet range fetch: {} missing ranges but {} fetched byte ranges",
                    missing_ranges.len(),
                    fetched_pages.len()
                ),
            }
        );

        measure_operation(self.read_metrics.as_ref().map(|m| &m.cache_insert), || {
            self.cache_strategy.put_page_ranges(
                self.region_file_id.file_id(),
                self.row_group_idx,
                &missing_ranges,
                &fetched_pages,
            )
        });

        if let (Some(metrics), Some(start)) = (&self.fetch_metrics, fetch_start) {
            metrics.data.lock().unwrap().total_fetch_elapsed += start.elapsed();
        }

        if let Some(lookup) = page_lookup {
            let fetched_parts = missing_ranges
                .into_iter()
                .zip(fetched_pages)
                .map(|(range, bytes)| PageRangePart { range, bytes })
                .collect::<Vec<_>>();
            return measure_operation(
                self.read_metrics.as_ref().map(|m| &m.range_assembly),
                || assemble_ranges(&ranges, lookup.cached_parts, &fetched_parts),
            );
        }

        Ok(fetched_pages)
    }
}

fn assemble_ranges(
    ranges: &[Range<u64>],
    cached_parts: Vec<Vec<PageRangePart>>,
    fetched_parts: &[PageRangePart],
) -> Result<Vec<Bytes>> {
    ensure!(
        ranges.len() == cached_parts.len(),
        UnexpectedSnafu {
            reason: format!(
                "Invalid parquet range assembly: {} requested ranges but {} cached part groups",
                ranges.len(),
                cached_parts.len()
            ),
        }
    );

    ranges
        .iter()
        .zip(cached_parts)
        .map(|(range, mut parts)| {
            parts.extend(
                fetched_parts
                    .iter()
                    .filter_map(|part| overlapping_part(range, part)),
            );
            assemble_range(range, parts)
        })
        .collect()
}

fn overlapping_part(range: &Range<u64>, part: &PageRangePart) -> Option<PageRangePart> {
    let start = range.start.max(part.range.start);
    let end = range.end.min(part.range.end);
    if start >= end {
        return None;
    }

    let slice_start = (start - part.range.start) as usize;
    let slice_end = (end - part.range.start) as usize;
    Some(PageRangePart {
        range: start..end,
        bytes: part.bytes.slice(slice_start..slice_end),
    })
}

fn assemble_range(range: &Range<u64>, mut parts: Vec<PageRangePart>) -> Result<Bytes> {
    if range.start >= range.end {
        return Ok(Bytes::new());
    }

    parts.sort_unstable_by_key(|part| part.range.start);
    if parts.len() == 1 && parts[0].range == *range {
        return Ok(parts.pop().unwrap().bytes);
    }

    let mut cursor = range.start;
    let mut output = BytesMut::with_capacity((range.end - range.start) as usize);
    for part in parts {
        ensure!(
            part.range.start <= cursor,
            UnexpectedSnafu {
                reason: format!(
                    "Missing cached parquet bytes for range {}..{}, next part starts at {}",
                    range.start, range.end, part.range.start
                ),
            }
        );
        if part.range.end <= cursor {
            continue;
        }

        let slice_start = (cursor - part.range.start) as usize;
        let slice_end = (part.range.end.min(range.end) - part.range.start) as usize;
        output.extend_from_slice(&part.bytes.slice(slice_start..slice_end));
        cursor = part.range.end.min(range.end);
        if cursor >= range.end {
            break;
        }
    }

    ensure!(
        cursor == range.end,
        UnexpectedSnafu {
            reason: format!(
                "Missing cached parquet bytes for range {}..{}, assembled through {}",
                range.start, range.end, cursor
            ),
        }
    );

    Ok(output.freeze())
}

/// Builds a parquet record batch stream driven directly by [ParquetPushDecoderBuilder].
pub fn build_sst_parquet_record_batch_stream(
    arrow_metadata: ArrowReaderMetadata,
    row_group_idx: usize,
    row_selection: Option<RowSelection>,
    projection: ProjectionMask,
    fetcher: SstParquetRangeFetcher,
    file_path: String,
    batch_size: usize,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    if let Some((key, _, metrics)) = &fetcher.compact_audit {
        if projection.leaf_included(*key) {
            metrics.key_decode_violations.add(1);
            return UnexpectedSnafu {
                reason: "compact data decoder projects SST primary key",
            }
            .fail();
        }
        metrics.audited_decoders.add(1);
        metrics.data_readers.add(1);
    }
    let mut builder = ParquetPushDecoderBuilder::new_with_metadata(arrow_metadata)
        .with_row_groups(vec![row_group_idx])
        .with_projection(projection)
        .with_batch_size(batch_size);

    if let Some(selection) = row_selection {
        builder = builder.with_row_selection(selection);
    }

    let mut decoder = builder
        .build()
        .context(ReadParquetSnafu { path: &file_path })?;

    Ok(async_stream::try_stream! {
        loop {
            match measure_operation(fetcher.read_metrics.as_ref().map(|m| &m.decode), || decoder.try_decode()).context(ReadParquetSnafu { path: &file_path })? {
                DecodeResult::NeedsData(ranges) => {
                    let data = fetcher.fetch_bytes_with_cache(ranges.clone()).await?;
                    measure_operation(fetcher.read_metrics.as_ref().map(|m| &m.decode), || decoder.push_ranges(ranges, data))
                        .context(ReadParquetSnafu { path: &file_path })?;
                }
                DecodeResult::Data(batch) => {
                    if let Some(m) = &fetcher.read_metrics {
                        m.decoded_batches.add(1);
                        m.decoded_rows.add(batch.num_rows());
                    }
                    yield batch;
                },
                DecodeResult::Finished => break,
            }
        }
    }
    .boxed())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn compact_read_metrics_separate_store_and_cached_payload() {
        use crate::cache::CacheManager;
        use crate::read::series_compact::CompactMetrics;
        use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
        use std::sync::Arc;
        use store_api::storage::{FileId, RegionId};
        let metrics = CompactMetrics::new(&ExecutionPlanMetricsSet::default(), 0);
        let reads = metrics.preflight_read.clone();
        let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
        store
            .write("keys", Bytes::from_static(b"0123456789abcdef"))
            .await
            .unwrap();
        let cache = CacheStrategy::EnableAll(Arc::new(
            CacheManager::builder().page_cache_size(1024 * 1024).build(),
        ));
        let fetcher = SstParquetRangeFetcher::new(
            RegionFileId::new(RegionId::new(1, 1), FileId::random()),
            "keys".into(),
            store.clone(),
            cache,
            0,
            None,
        )
        .with_key_read_metrics(Some((metrics.preflight_key_bytes.clone(), reads.clone())));
        let first = fetcher
            .fetch_bytes_with_cache(std::iter::once(0..8).collect())
            .await
            .unwrap();
        assert_eq!(b"01234567", first[0].as_ref());
        let second = fetcher
            .fetch_bytes_with_cache(std::iter::once(4..12).collect())
            .await
            .unwrap();
        assert_eq!(b"456789ab", second[0].as_ref());
        assert_eq!(12, reads.store_bytes.value());
        assert_eq!(4, reads.cache_bytes.value());
        store.delete("keys").await.unwrap();
        let cached = fetcher
            .fetch_bytes_with_cache(std::iter::once(0..12).collect())
            .await
            .unwrap();
        assert_eq!(b"0123456789ab", cached[0].as_ref());
        assert_eq!(28, reads.requested_bytes.value());
        assert_eq!(28, metrics.preflight_key_bytes.value());
        assert_eq!(16, reads.cache_bytes.value());
        assert_eq!(12, reads.store_bytes.value());
        assert_eq!(2, reads.store_calls.value());
        assert_eq!(3, reads.fetch_calls.value());
        assert_eq!(3, reads.cache_lookup.calls.value());
        assert_eq!(2, reads.cache_insert.calls.value());
        assert!(reads.fetch_elapsed.value() > 0);
        assert!(
            fetcher
                .fetch_bytes_with_cache(std::iter::once(12..16).collect())
                .await
                .is_err()
        );
        assert_eq!(3, reads.store_calls.value());
        assert_eq!(12, reads.store_bytes.value());
    }

    #[tokio::test]
    async fn compact_key_audit_rejects_reads_before_cache_lookup() {
        use crate::read::series_compact::CompactMetrics;
        use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
        use store_api::storage::{FileId, RegionId};
        let set = ExecutionPlanMetricsSet::default();
        let metrics = CompactMetrics::new(&set, 0);
        let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
        let fetcher = SstParquetRangeFetcher::new(
            RegionFileId::new(RegionId::new(1, 1), FileId::random()),
            "unused".into(),
            store,
            CacheStrategy::Disabled,
            0,
            None,
        )
        .with_compact_audit(0, (100, 100), metrics.clone());
        assert!(
            fetcher
                .fetch_bytes_with_cache(std::iter::once(150..160).collect())
                .await
                .is_err()
        );
        assert_eq!(1, metrics.key_decode_violations.value());
        assert_eq!(0, metrics.data_readers.value());
    }

    #[test]
    fn test_assemble_range_from_cached_subrange_and_fetched_tail() {
        let cached_parts = vec![vec![PageRangePart {
            range: 400..500,
            bytes: Bytes::from(vec![1; 100]),
        }]];
        let fetched_parts = vec![PageRangePart {
            range: 500..600,
            bytes: Bytes::from(vec![2; 100]),
        }];

        let requested = 400..600;
        let output = assemble_ranges(
            std::slice::from_ref(&requested),
            cached_parts,
            &fetched_parts,
        )
        .unwrap();
        assert_eq!(1, output.len());
        assert_eq!(vec![1; 100].as_slice(), &output[0][..100]);
        assert_eq!(vec![2; 100].as_slice(), &output[0][100..]);
    }

    #[test]
    fn test_assemble_range_returns_single_covering_part_without_copy() {
        let bytes = Bytes::from_static(b"abcdef");
        let cached_parts = vec![vec![PageRangePart {
            range: 10..16,
            bytes: bytes.clone(),
        }]];

        let requested = 10..16;
        let output = assemble_ranges(std::slice::from_ref(&requested), cached_parts, &[]).unwrap();
        assert_eq!(bytes, output[0]);
    }

    #[test]
    fn test_assemble_range_clamps_overlapping_part_to_requested_end() {
        let parts = vec![PageRangePart {
            range: 0..10,
            bytes: Bytes::from_static(b"0123456789"),
        }];

        let output = assemble_range(&(2..5), parts).unwrap();
        assert_eq!(Bytes::from_static(b"234"), output);
    }
}
