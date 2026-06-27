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

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::Arc;

use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, RowSelection};

use crate::sst::file::RegionFileId;
use crate::sst::parquet::helper::MERGE_GAP;

/// Page skip/total stats for a single column.
#[derive(Default, Debug, Clone, Copy)]
pub struct ColumnPageStats {
    /// Number of data pages skipped by row selection.
    pub pages_skipped: usize,
    /// Total number of data pages (skipped + read).
    pub pages_total: usize,
}

/// Collects Parquet data page skip/total stats for projected leaf columns in a row group.
///
/// For each projected leaf column, the number of selected pages equals the number of
/// byte ranges returned by [`RowSelection::scan_ranges`] (it emits one range per
/// selected data page without coalescing), so skipped pages are the remaining ones.
/// Returns an empty map when the offset index is unavailable.
pub fn collect_column_page_stats_for_row_group(
    arrow_metadata: &ArrowReaderMetadata,
    row_group_idx: usize,
    selection: Option<&RowSelection>,
    projection: &ProjectionMask,
) -> BTreeMap<String, ColumnPageStats> {
    let Some(rg_cols) = arrow_metadata
        .metadata()
        .offset_index()
        .and_then(|offset_index| offset_index.get(row_group_idx))
    else {
        return BTreeMap::new();
    };

    let schema_descr = arrow_metadata.metadata().file_metadata().schema_descr();
    let mut stats = BTreeMap::new();
    for (leaf_idx, col) in rg_cols.iter().enumerate() {
        if !projection.leaf_included(leaf_idx) {
            continue;
        }

        let locations = col.page_locations();
        if locations.is_empty() {
            continue;
        }

        let pages_total = locations.len();
        let pages_skipped = selection
            .map(|selection| pages_total - selection.scan_ranges(locations).len())
            .unwrap_or(0);
        stats.insert(
            schema_descr.column(leaf_idx).name().to_string(),
            ColumnPageStats {
                pages_skipped,
                pages_total,
            },
        );
    }

    stats
}

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
    /// Number of data pages skipped by row selection (summed over projected leaf columns).
    pub pages_skipped: usize,
    /// Total number of data pages over projected leaf columns (skipped + read).
    pub pages_total: usize,
    /// Per-file, per-column page skip/total stats. Keyed by file id then leaf column name.
    ///
    /// Used to surface a per-column breakdown in the per-file scan metrics. Not printed in
    /// the aggregate fetch-metrics output.
    pub per_file_column_pages: HashMap<RegionFileId, BTreeMap<String, ColumnPageStats>>,
}

impl ParquetFetchMetricsData {
    /// Returns true if the metrics are empty (contain no meaningful data).
    fn is_empty(&self) -> bool {
        self.total_fetch_elapsed.is_zero() && self.prefilter_cost.is_zero()
    }
}

/// Metrics for tracking page/row group fetch operations.
#[derive(Default, Clone)]
pub struct ParquetFetchMetrics {
    pub data: Arc<std::sync::Mutex<ParquetFetchMetricsData>>,
}

impl std::fmt::Debug for ParquetFetchMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
            pages_skipped,
            pages_total,
            per_file_column_pages: _,
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
        if pages_skipped > 0 {
            write!(f, ", \"pages_skipped\":{}", pages_skipped)?;
        }
        if pages_total > 0 {
            write!(f, ", \"pages_total\":{}", pages_total)?;
        }

        write!(f, "}}")
    }
}

impl ParquetFetchMetrics {
    /// Returns true if the metrics are empty (contain no meaningful data).
    pub fn is_empty(&self) -> bool {
        self.data.lock().unwrap().is_empty()
    }

    /// Merges metrics from another [ParquetFetchMetrics].
    pub fn merge_from(&self, other: &ParquetFetchMetrics) {
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
            pages_skipped,
            pages_total,
            per_file_column_pages,
        } = other.data.lock().unwrap().clone();

        let mut data = self.data.lock().unwrap();
        data.page_cache_hit += page_cache_hit;
        data.write_cache_hit += write_cache_hit;
        data.cache_miss += cache_miss;
        data.pages_to_fetch_mem += pages_to_fetch_mem;
        data.page_size_to_fetch_mem += page_size_to_fetch_mem;
        data.pages_to_fetch_write_cache += pages_to_fetch_write_cache;
        data.page_size_to_fetch_write_cache += page_size_to_fetch_write_cache;
        data.pages_to_fetch_store += pages_to_fetch_store;
        data.page_size_to_fetch_store += page_size_to_fetch_store;
        data.page_size_needed += page_size_needed;
        data.write_cache_fetch_elapsed += write_cache_fetch_elapsed;
        data.store_fetch_elapsed += store_fetch_elapsed;
        data.total_fetch_elapsed += total_fetch_elapsed;
        data.prefilter_cost += prefilter_cost;
        data.prefilter_filtered_rows += prefilter_filtered_rows;
        data.pages_skipped += pages_skipped;
        data.pages_total += pages_total;
        for (file_id, columns) in per_file_column_pages {
            let file_entry = data.per_file_column_pages.entry(file_id).or_default();
            for (column, stats) in columns {
                let entry = file_entry.entry(column).or_default();
                entry.pages_skipped += stats.pages_skipped;
                entry.pages_total += stats.pages_total;
            }
        }
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
