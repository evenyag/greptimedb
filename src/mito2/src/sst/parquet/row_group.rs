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

//! Parquet row group reading utilities.

use std::collections::BTreeSet;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use crate::sst::parquet::helper::MERGE_GAP;

/// Inner data for ParquetFetchMetrics.
#[derive(Default, Debug, Clone)]
pub struct ParquetFetchMetricsData {
    /// Number of page cache hits.
    pub page_cache_hit: usize,
    /// Number of write cache hits.
    pub write_cache_hit: usize,
    /// Number of cache misses.
    pub cache_miss: usize,
    /// Number of pages to fetch from mem cache.
    pub pages_to_fetch_mem: usize,
    /// Total size in bytes of pages to fetch from mem cache.
    pub page_size_to_fetch_mem: u64,
    /// Number of pages to fetch from write cache.
    pub pages_to_fetch_write_cache: usize,
    /// Total size in bytes of pages to fetch from write cache.
    pub page_size_to_fetch_write_cache: u64,
    /// Number of pages to fetch from store.
    pub pages_to_fetch_store: usize,
    /// Total size in bytes of pages to fetch from store.
    pub page_size_to_fetch_store: u64,
    /// Total size in bytes of pages actually returned.
    pub page_size_needed: u64,
    /// Elapsed time fetching from write cache.
    pub write_cache_fetch_elapsed: std::time::Duration,
    /// Elapsed time fetching from object store.
    pub store_fetch_elapsed: std::time::Duration,
    /// Total elapsed time for fetching row groups.
    pub total_fetch_elapsed: std::time::Duration,
    /// Elapsed time for prefilter execution.
    pub prefilter_cost: std::time::Duration,
    /// Number of rows filtered out by prefiltering.
    pub prefilter_filtered_rows: usize,
    /// Number of rows selected by prefiltering for the projection pass.
    pub prefilter_selected_rows: usize,
    /// Number of candidate rows considered by prefiltering, including cache hits.
    pub prefilter_candidate_rows: usize,
    /// Number of rows physically decoded by the prefilter pass.
    pub prefilter_rows_read: usize,
    /// Number of batches physically decoded by the prefilter pass.
    pub prefilter_batches_read: usize,
    /// Time polling the reduced-column stream, including fetch and decode.
    pub prefilter_column_read_elapsed: Duration,
    /// CPU time evaluating predicates and combining their masks.
    pub prefilter_filter_eval_elapsed: Duration,
    /// CPU time constructing and combining the final row selection.
    pub prefilter_selection_elapsed: Duration,
    /// Number of predicate-result cache hits.
    pub prefilter_result_cache_hits: usize,
    /// Number of predicate-result cache misses.
    pub prefilter_result_cache_misses: usize,
    /// Physical columns actually read for prefilter cache misses.
    pub prefilter_columns_read: BTreeSet<String>,
    /// Fetch metrics attributable to the prefilter phase.
    pub prefilter_io: ParquetPhaseFetchMetricsData,
    /// Fetch metrics attributable to encoded primary-key reads.
    pub primary_key_io: ParquetPhaseFetchMetricsData,
    /// Fetch metrics attributable to final projection reads.
    pub projection_io: ParquetPhaseFetchMetricsData,
}

/// Fetch metrics attributed to one Parquet read phase.
#[derive(Default, Debug, Clone)]
pub struct ParquetPhaseFetchMetricsData {
    /// Number of page-cache hits.
    pub page_cache_hit: usize,
    /// Number of write-cache hits.
    pub write_cache_hit: usize,
    /// Number of cache misses that required an object-store fetch.
    pub cache_miss: usize,
    /// Number of ranges fetched from the page cache.
    pub pages_to_fetch_mem: usize,
    /// Bytes fetched from the page cache.
    pub page_size_to_fetch_mem: u64,
    /// Number of ranges fetched from the write cache.
    pub pages_to_fetch_write_cache: usize,
    /// Bytes fetched from the write cache.
    pub page_size_to_fetch_write_cache: u64,
    /// Number of ranges fetched from the object store.
    pub pages_to_fetch_store: usize,
    /// Bytes fetched from the object store.
    pub page_size_to_fetch_store: u64,
    /// Bytes from the fetched ranges needed by the decoder.
    pub page_size_needed: u64,
    /// Elapsed time fetching from the write cache.
    pub write_cache_fetch_elapsed: Duration,
    /// Elapsed time fetching from the object store.
    pub store_fetch_elapsed: Duration,
    /// Total elapsed time in cache-aware fetch operations.
    pub total_fetch_elapsed: Duration,
}

impl ParquetPhaseFetchMetricsData {
    fn is_empty(&self) -> bool {
        self.total_fetch_elapsed.is_zero()
            && self.write_cache_fetch_elapsed.is_zero()
            && self.store_fetch_elapsed.is_zero()
            && self.page_cache_hit == 0
            && self.write_cache_hit == 0
            && self.cache_miss == 0
            && self.pages_to_fetch_mem == 0
            && self.page_size_to_fetch_mem == 0
            && self.pages_to_fetch_write_cache == 0
            && self.page_size_to_fetch_write_cache == 0
            && self.pages_to_fetch_store == 0
            && self.page_size_to_fetch_store == 0
            && self.page_size_needed == 0
    }

    fn merge_from(&mut self, other: &Self) {
        self.page_cache_hit += other.page_cache_hit;
        self.write_cache_hit += other.write_cache_hit;
        self.cache_miss += other.cache_miss;
        self.pages_to_fetch_mem += other.pages_to_fetch_mem;
        self.page_size_to_fetch_mem += other.page_size_to_fetch_mem;
        self.pages_to_fetch_write_cache += other.pages_to_fetch_write_cache;
        self.page_size_to_fetch_write_cache += other.page_size_to_fetch_write_cache;
        self.pages_to_fetch_store += other.pages_to_fetch_store;
        self.page_size_to_fetch_store += other.page_size_to_fetch_store;
        self.page_size_needed += other.page_size_needed;
        self.write_cache_fetch_elapsed += other.write_cache_fetch_elapsed;
        self.store_fetch_elapsed += other.store_fetch_elapsed;
        self.total_fetch_elapsed += other.total_fetch_elapsed;
    }
}

impl ParquetFetchMetricsData {
    /// Returns true if the metrics are empty (contain no meaningful data).
    fn is_empty(&self) -> bool {
        self.total_fetch_elapsed.is_zero() && self.prefilter_cost.is_zero()
    }
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
enum ParquetReadPhase {
    #[default]
    Projection,
    Prefilter,
    PrimaryKey,
}

impl ParquetReadPhase {
    fn metrics_mut(self, data: &mut ParquetFetchMetricsData) -> &mut ParquetPhaseFetchMetricsData {
        match self {
            Self::Projection => &mut data.projection_io,
            Self::Prefilter => &mut data.prefilter_io,
            Self::PrimaryKey => &mut data.primary_key_io,
        }
    }
}

/// Metrics for tracking page/row group fetch operations.
#[derive(Default, Clone)]
pub struct ParquetFetchMetrics {
    pub data: Arc<std::sync::Mutex<ParquetFetchMetricsData>>,
    phase: ParquetReadPhase,
}

fn write_phase_fetch_metrics(
    f: &mut fmt::Formatter<'_>,
    metrics: &ParquetPhaseFetchMetricsData,
) -> fmt::Result {
    write!(
        f,
        "{{\"total_fetch_elapsed\":\"{:?}\", \"page_cache_hit\":{}, \"write_cache_hit\":{}, \"cache_miss\":{}, \"pages_to_fetch_mem\":{}, \"page_size_to_fetch_mem\":{}, \"pages_to_fetch_write_cache\":{}, \"page_size_to_fetch_write_cache\":{}, \"pages_to_fetch_store\":{}, \"page_size_to_fetch_store\":{}, \"page_size_needed\":{}, \"write_cache_fetch_elapsed\":\"{:?}\", \"store_fetch_elapsed\":\"{:?}\"}}",
        metrics.total_fetch_elapsed,
        metrics.page_cache_hit,
        metrics.write_cache_hit,
        metrics.cache_miss,
        metrics.pages_to_fetch_mem,
        metrics.page_size_to_fetch_mem,
        metrics.pages_to_fetch_write_cache,
        metrics.page_size_to_fetch_write_cache,
        metrics.pages_to_fetch_store,
        metrics.page_size_to_fetch_store,
        metrics.page_size_needed,
        metrics.write_cache_fetch_elapsed,
        metrics.store_fetch_elapsed,
    )
}

impl fmt::Debug for ParquetFetchMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let data = self.data.lock().unwrap();
        if data.is_empty() {
            return write!(f, "{{}}");
        }

        let ParquetFetchMetricsData {
            page_cache_hit,
            write_cache_hit,
            cache_miss,
            pages_to_fetch_mem,
            page_size_to_fetch_mem,
            pages_to_fetch_write_cache,
            page_size_to_fetch_write_cache,
            pages_to_fetch_store,
            page_size_to_fetch_store,
            page_size_needed,
            write_cache_fetch_elapsed,
            store_fetch_elapsed,
            total_fetch_elapsed,
            prefilter_cost,
            prefilter_filtered_rows,
            prefilter_selected_rows,
            prefilter_candidate_rows,
            prefilter_rows_read,
            prefilter_batches_read,
            prefilter_column_read_elapsed,
            prefilter_filter_eval_elapsed,
            prefilter_selection_elapsed,
            prefilter_result_cache_hits,
            prefilter_result_cache_misses,
            prefilter_columns_read,
            prefilter_io,
            primary_key_io,
            projection_io,
        } = data.clone();

        write!(f, "{{")?;

        write!(f, "\"total_fetch_elapsed\":\"{:?}\"", total_fetch_elapsed)?;

        if page_cache_hit > 0 {
            write!(f, ", \"page_cache_hit\":{}", page_cache_hit)?;
        }
        if write_cache_hit > 0 {
            write!(f, ", \"write_cache_hit\":{}", write_cache_hit)?;
        }
        if cache_miss > 0 {
            write!(f, ", \"cache_miss\":{}", cache_miss)?;
        }
        if pages_to_fetch_mem > 0 {
            write!(f, ", \"pages_to_fetch_mem\":{}", pages_to_fetch_mem)?;
        }
        if page_size_to_fetch_mem > 0 {
            write!(f, ", \"page_size_to_fetch_mem\":{}", page_size_to_fetch_mem)?;
        }
        if pages_to_fetch_write_cache > 0 {
            write!(
                f,
                ", \"pages_to_fetch_write_cache\":{}",
                pages_to_fetch_write_cache
            )?;
        }
        if page_size_to_fetch_write_cache > 0 {
            write!(
                f,
                ", \"page_size_to_fetch_write_cache\":{}",
                page_size_to_fetch_write_cache
            )?;
        }
        if pages_to_fetch_store > 0 {
            write!(f, ", \"pages_to_fetch_store\":{}", pages_to_fetch_store)?;
        }
        if page_size_to_fetch_store > 0 {
            write!(
                f,
                ", \"page_size_to_fetch_store\":{}",
                page_size_to_fetch_store
            )?;
        }
        if page_size_needed > 0 {
            write!(f, ", \"page_size_needed\":{}", page_size_needed)?;
        }
        if !write_cache_fetch_elapsed.is_zero() {
            write!(
                f,
                ", \"write_cache_fetch_elapsed\":\"{:?}\"",
                write_cache_fetch_elapsed
            )?;
        }
        if !store_fetch_elapsed.is_zero() {
            write!(f, ", \"store_fetch_elapsed\":\"{:?}\"", store_fetch_elapsed)?;
        }
        if !prefilter_cost.is_zero() {
            write!(f, ", \"prefilter_cost\":\"{:?}\"", prefilter_cost)?;
        }
        if prefilter_filtered_rows > 0 {
            write!(
                f,
                ", \"prefilter_filtered_rows\":{}",
                prefilter_filtered_rows
            )?;
        }
        if prefilter_selected_rows > 0 {
            write!(
                f,
                ", \"prefilter_selected_rows\":{}",
                prefilter_selected_rows
            )?;
        }
        if prefilter_candidate_rows > 0 {
            write!(
                f,
                ", \"prefilter_candidate_rows\":{}",
                prefilter_candidate_rows
            )?;
        }
        if prefilter_rows_read > 0 {
            write!(f, ", \"prefilter_rows_read\":{}", prefilter_rows_read)?;
        }
        if prefilter_batches_read > 0 {
            write!(f, ", \"prefilter_batches_read\":{}", prefilter_batches_read)?;
        }
        if !prefilter_column_read_elapsed.is_zero() {
            write!(
                f,
                ", \"prefilter_column_read_elapsed\":\"{:?}\"",
                prefilter_column_read_elapsed
            )?;
        }
        if !prefilter_filter_eval_elapsed.is_zero() {
            write!(
                f,
                ", \"prefilter_filter_eval_elapsed\":\"{:?}\"",
                prefilter_filter_eval_elapsed
            )?;
        }
        if !prefilter_selection_elapsed.is_zero() {
            write!(
                f,
                ", \"prefilter_selection_elapsed\":\"{:?}\"",
                prefilter_selection_elapsed
            )?;
        }
        if prefilter_result_cache_hits > 0 {
            write!(
                f,
                ", \"prefilter_result_cache_hits\":{}",
                prefilter_result_cache_hits
            )?;
        }
        if prefilter_result_cache_misses > 0 {
            write!(
                f,
                ", \"prefilter_result_cache_misses\":{}",
                prefilter_result_cache_misses
            )?;
        }
        if !prefilter_columns_read.is_empty() {
            write!(
                f,
                ", \"prefilter_columns_read\":{:?}",
                prefilter_columns_read.iter().collect::<Vec<_>>()
            )?;
        }
        if !prefilter_io.is_empty() {
            write!(f, ", \"prefilter_io\":")?;
            write_phase_fetch_metrics(f, &prefilter_io)?;
        }
        if !primary_key_io.is_empty() {
            write!(f, ", \"primary_key_io\":")?;
            write_phase_fetch_metrics(f, &primary_key_io)?;
        }
        if !projection_io.is_empty() {
            write!(f, ", \"projection_io\":")?;
            write_phase_fetch_metrics(f, &projection_io)?;
        }

        write!(f, "}}")
    }
}

impl ParquetFetchMetrics {
    /// Returns true if the metrics are empty (contain no meaningful data).
    pub fn is_empty(&self) -> bool {
        self.data.lock().unwrap().is_empty()
    }

    /// Returns a handle that attributes page fetches to prefiltering.
    pub(crate) fn prefilter_phase(&self) -> Self {
        Self {
            data: self.data.clone(),
            phase: ParquetReadPhase::Prefilter,
        }
    }

    /// Returns a handle that attributes page fetches to encoded primary-key reads.
    pub(crate) fn primary_key_phase(&self) -> Self {
        Self {
            data: self.data.clone(),
            phase: ParquetReadPhase::PrimaryKey,
        }
    }

    /// Records a page-cache hit.
    pub(crate) fn record_page_cache_hit(&self, ranges: usize, bytes: u64) {
        let mut data = self.data.lock().unwrap();
        data.page_cache_hit += 1;
        data.pages_to_fetch_mem += ranges;
        data.page_size_to_fetch_mem += bytes;
        data.page_size_needed += bytes;
        let phase = self.phase.metrics_mut(&mut data);
        phase.page_cache_hit += 1;
        phase.pages_to_fetch_mem += ranges;
        phase.page_size_to_fetch_mem += bytes;
        phase.page_size_needed += bytes;
    }

    /// Records a write-cache fetch.
    pub(crate) fn record_write_cache_fetch(
        &self,
        ranges: usize,
        fetched_bytes: u64,
        needed_bytes: u64,
        elapsed: Duration,
    ) {
        let mut data = self.data.lock().unwrap();
        data.write_cache_hit += 1;
        data.pages_to_fetch_write_cache += ranges;
        data.page_size_to_fetch_write_cache += fetched_bytes;
        data.page_size_needed += needed_bytes;
        data.write_cache_fetch_elapsed += elapsed;
        let phase = self.phase.metrics_mut(&mut data);
        phase.write_cache_hit += 1;
        phase.pages_to_fetch_write_cache += ranges;
        phase.page_size_to_fetch_write_cache += fetched_bytes;
        phase.page_size_needed += needed_bytes;
        phase.write_cache_fetch_elapsed += elapsed;
    }

    /// Records an object-store fetch.
    pub(crate) fn record_store_fetch(
        &self,
        ranges: usize,
        fetched_bytes: u64,
        needed_bytes: u64,
        elapsed: Duration,
    ) {
        let mut data = self.data.lock().unwrap();
        data.cache_miss += 1;
        data.pages_to_fetch_store += ranges;
        data.page_size_to_fetch_store += fetched_bytes;
        data.page_size_needed += needed_bytes;
        data.store_fetch_elapsed += elapsed;
        let phase = self.phase.metrics_mut(&mut data);
        phase.cache_miss += 1;
        phase.pages_to_fetch_store += ranges;
        phase.page_size_to_fetch_store += fetched_bytes;
        phase.page_size_needed += needed_bytes;
        phase.store_fetch_elapsed += elapsed;
    }

    /// Records total time spent in one cache-aware fetch operation.
    pub(crate) fn record_total_fetch_elapsed(&self, elapsed: Duration) {
        let mut data = self.data.lock().unwrap();
        data.total_fetch_elapsed += elapsed;
        self.phase.metrics_mut(&mut data).total_fetch_elapsed += elapsed;
    }

    /// Merges metrics from another [ParquetFetchMetrics].
    pub fn merge_from(&self, other: &ParquetFetchMetrics) {
        let other = other.data.lock().unwrap().clone();
        let mut data = self.data.lock().unwrap();
        data.page_cache_hit += other.page_cache_hit;
        data.write_cache_hit += other.write_cache_hit;
        data.cache_miss += other.cache_miss;
        data.pages_to_fetch_mem += other.pages_to_fetch_mem;
        data.page_size_to_fetch_mem += other.page_size_to_fetch_mem;
        data.pages_to_fetch_write_cache += other.pages_to_fetch_write_cache;
        data.page_size_to_fetch_write_cache += other.page_size_to_fetch_write_cache;
        data.pages_to_fetch_store += other.pages_to_fetch_store;
        data.page_size_to_fetch_store += other.page_size_to_fetch_store;
        data.page_size_needed += other.page_size_needed;
        data.write_cache_fetch_elapsed += other.write_cache_fetch_elapsed;
        data.store_fetch_elapsed += other.store_fetch_elapsed;
        data.total_fetch_elapsed += other.total_fetch_elapsed;
        data.prefilter_cost += other.prefilter_cost;
        data.prefilter_filtered_rows += other.prefilter_filtered_rows;
        data.prefilter_selected_rows += other.prefilter_selected_rows;
        data.prefilter_candidate_rows += other.prefilter_candidate_rows;
        data.prefilter_rows_read += other.prefilter_rows_read;
        data.prefilter_batches_read += other.prefilter_batches_read;
        data.prefilter_column_read_elapsed += other.prefilter_column_read_elapsed;
        data.prefilter_filter_eval_elapsed += other.prefilter_filter_eval_elapsed;
        data.prefilter_selection_elapsed += other.prefilter_selection_elapsed;
        data.prefilter_result_cache_hits += other.prefilter_result_cache_hits;
        data.prefilter_result_cache_misses += other.prefilter_result_cache_misses;
        data.prefilter_columns_read
            .extend(other.prefilter_columns_read);
        data.prefilter_io.merge_from(&other.prefilter_io);
        data.primary_key_io.merge_from(&other.primary_key_io);
        data.projection_io.merge_from(&other.projection_io);
    }
}

/// Computes the max possible buffer size to read the given `ranges`.
/// Returns (aligned_size, unaligned_size) where:
/// - aligned_size: total size aligned to pooled buffer size
/// - unaligned_size: actual total size without alignment
// See https://github.com/apache/opendal/blob/v0.54.0/core/src/types/read/reader.rs#L166-L192
pub(crate) fn compute_total_range_size(ranges: &[Range<u64>]) -> (u64, u64) {
    if ranges.is_empty() {
        return (0, 0);
    }

    let gap = MERGE_GAP as u64;
    let mut sorted_ranges = ranges.to_vec();
    sorted_ranges.sort_unstable_by_key(|a| a.start);

    let mut total_size_aligned = 0;
    let mut total_size_unaligned = 0;
    let mut cur = sorted_ranges[0].clone();

    for range in sorted_ranges.into_iter().skip(1) {
        if range.start <= cur.end + gap {
            // There is an overlap or the gap is small enough to merge
            cur.end = cur.end.max(range.end);
        } else {
            // No overlap and the gap is too large, add current range to total and start a new one
            let range_size = cur.end - cur.start;
            total_size_aligned += align_to_pooled_buf_size(range_size);
            total_size_unaligned += range_size;
            cur = range;
        }
    }

    // Add the last range
    let range_size = cur.end - cur.start;
    total_size_aligned += align_to_pooled_buf_size(range_size);
    total_size_unaligned += range_size;

    (total_size_aligned, total_size_unaligned)
}

/// Aligns the given size to the multiple of the pooled buffer size.
// See:
// - https://github.com/apache/opendal/blob/v0.54.0/core/src/services/fs/backend.rs#L178
// - https://github.com/apache/opendal/blob/v0.54.0/core/src/services/fs/reader.rs#L36-L46
fn align_to_pooled_buf_size(size: u64) -> u64 {
    const POOLED_BUF_SIZE: u64 = 2 * 1024 * 1024;
    size.div_ceil(POOLED_BUF_SIZE) * POOLED_BUF_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fetch_metrics_attribute_read_phase_io() {
        let metrics = ParquetFetchMetrics::default();
        let prefilter = metrics.prefilter_phase();

        prefilter.record_page_cache_hit(2, 128);
        prefilter.record_store_fetch(3, 512, 384, Duration::from_millis(5));
        prefilter.record_total_fetch_elapsed(Duration::from_millis(7));
        let primary_key = metrics.primary_key_phase();
        primary_key.record_page_cache_hit(1, 64);
        primary_key.record_total_fetch_elapsed(Duration::from_millis(1));
        metrics.record_write_cache_fetch(1, 256, 192, Duration::from_millis(2));
        metrics.record_total_fetch_elapsed(Duration::from_millis(3));

        let data = metrics.data.lock().unwrap();
        assert_eq!(2, data.page_cache_hit);
        assert_eq!(1, data.write_cache_hit);
        assert_eq!(1, data.cache_miss);
        assert_eq!(Duration::from_millis(11), data.total_fetch_elapsed);
        assert_eq!(1, data.prefilter_io.page_cache_hit);
        assert_eq!(0, data.prefilter_io.write_cache_hit);
        assert_eq!(1, data.prefilter_io.cache_miss);
        assert_eq!(
            Duration::from_millis(7),
            data.prefilter_io.total_fetch_elapsed
        );
        assert_eq!(1, data.primary_key_io.page_cache_hit);
        assert_eq!(
            Duration::from_millis(1),
            data.primary_key_io.total_fetch_elapsed
        );

        assert_eq!(0, data.projection_io.page_cache_hit);
        assert_eq!(1, data.projection_io.write_cache_hit);
        assert_eq!(0, data.projection_io.cache_miss);
        assert_eq!(
            Duration::from_millis(3),
            data.projection_io.total_fetch_elapsed
        );
        drop(data);

        metrics
            .data
            .lock()
            .unwrap()
            .prefilter_columns_read
            .insert("hostname".to_string());
        let output = format!("{metrics:?}");
        assert!(output.contains("\"prefilter_columns_read\":[\"hostname\"]"));
        assert!(output.contains("\"prefilter_io\":"));
        assert!(output.contains("\"primary_key_io\":"));
        assert!(output.contains("\"projection_io\":"));
    }

    #[test]
    fn test_fetch_metrics_merge_prefilter_details() {
        let target = ParquetFetchMetrics::default();
        let source = ParquetFetchMetrics::default();
        source
            .prefilter_phase()
            .record_store_fetch(2, 100, 80, Duration::from_millis(4));
        {
            let mut data = source.data.lock().unwrap();
            data.prefilter_columns_read.insert("hostname".to_string());
            data.prefilter_candidate_rows = 100;
            data.prefilter_rows_read = 100;
            data.prefilter_selected_rows = 10;
        }

        target.merge_from(&source);

        let data = target.data.lock().unwrap();
        assert_eq!(1, data.cache_miss);
        assert_eq!(1, data.prefilter_io.cache_miss);
        assert_eq!(100, data.prefilter_candidate_rows);
        assert_eq!(100, data.prefilter_rows_read);
        assert_eq!(10, data.prefilter_selected_rows);
        assert_eq!(
            BTreeSet::from(["hostname".to_string()]),
            data.prefilter_columns_read
        );
    }
}
