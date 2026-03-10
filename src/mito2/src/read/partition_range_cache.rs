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

//! Cache key types for partition-range scan outputs.

use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;

use async_stream::try_stream;
use datatypes::arrow::array::{Array, BinaryArray, DictionaryArray, UInt32Array};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use store_api::region_engine::PartitionRange;
use store_api::storage::{ColumnId, FileId, RegionId};

use crate::cache::CacheStrategy;
use crate::memtable::record_batch_estimated_size;
use crate::read::BoxedRecordBatchStream;
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::PartitionMetrics;
use crate::sst::parquet::flat_format::primary_key_column_index;

/// Fingerprint of request-relevant scan options.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ScanRequestFingerprint {
    pub(crate) read_column_ids: Vec<ColumnId>,
    pub(crate) filters: Vec<String>,
    pub(crate) series_row_selector: Option<String>,
    pub(crate) distribution: Option<String>,
    pub(crate) append_mode: bool,
    pub(crate) filter_deleted: bool,
    pub(crate) merge_mode: &'static str,
    pub(crate) flat_format: bool,
    pub(crate) compaction: bool,
}

/// Cache key for partition-range scan outputs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PartitionRangeScanCacheKey {
    pub(crate) region_id: RegionId,
    pub(crate) range_index: usize,
    pub(crate) file_ids: Vec<FileId>,
    pub(crate) scan: ScanRequestFingerprint,
}

impl PartitionRangeScanCacheKey {
    pub(crate) fn digest(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    pub(crate) fn estimated_size(&self) -> usize {
        mem::size_of::<Self>()
            + self.file_ids.capacity() * mem::size_of::<FileId>()
            + self.scan.read_column_ids.capacity() * mem::size_of::<ColumnId>()
            + self
                .scan
                .filters
                .iter()
                .map(|filter| filter.capacity())
                .sum::<usize>()
            + self
                .scan
                .series_row_selector
                .as_ref()
                .map_or(0, String::capacity)
            + self.scan.distribution.as_ref().map_or(0, String::capacity)
    }
}

/// Cached result for one partition-range scan.
pub(crate) struct PartitionRangeScanCacheValue {
    pub(crate) batches: Vec<RecordBatch>,
}

impl PartitionRangeScanCacheValue {
    pub(crate) fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }

    pub(crate) fn estimated_size(&self) -> usize {
        mem::size_of::<Self>()
            + self.batches.capacity() * mem::size_of::<RecordBatch>()
            + self
                .batches
                .iter()
                .map(record_batch_estimated_size)
                .sum::<usize>()
    }
}

/// File IDs and whether all sources are file-only for a partition range.
pub(crate) struct PartitionRangeFiles {
    pub(crate) file_ids: Vec<FileId>,
    pub(crate) only_file_sources: bool,
}

/// Collects file IDs from a partition range's row group indices.
pub(crate) fn collect_partition_range_file_ids(
    stream_ctx: &StreamContext,
    part_range: &PartitionRange,
) -> PartitionRangeFiles {
    let range_meta = &stream_ctx.ranges[part_range.identifier];
    let mut file_ids = Vec::new();
    let mut seen = HashSet::new();
    let mut only_file_sources = true;

    for index in &range_meta.row_group_indices {
        if stream_ctx.is_file_range_index(*index) {
            let file_id = stream_ctx.input.file_from_index(*index).file_id().file_id();
            if seen.insert(file_id) {
                file_ids.push(file_id);
            }
        } else {
            only_file_sources = false;
        }
    }

    PartitionRangeFiles {
        file_ids,
        only_file_sources,
    }
}

/// Builds a cache key for the given partition range if it is eligible for caching.
pub(crate) fn build_partition_range_cache_key(
    stream_ctx: &StreamContext,
    part_range: &PartitionRange,
) -> Option<PartitionRangeScanCacheKey> {
    let files = collect_partition_range_file_ids(stream_ctx, part_range);
    let eligible = stream_ctx.input.flat_format
        && stream_ctx.input.has_tag_filter
        && !stream_ctx.input.compaction
        && files.only_file_sources
        && !files.file_ids.is_empty()
        && !matches!(stream_ctx.input.cache_strategy, CacheStrategy::Disabled);

    if eligible {
        Some(PartitionRangeScanCacheKey {
            region_id: stream_ctx.input.region_metadata().region_id,
            range_index: part_range.identifier,
            file_ids: files.file_ids,
            scan: stream_ctx.input.scan_request_fingerprint(),
        })
    } else {
        None
    }
}

/// Returns a stream that replays cached record batches.
pub(crate) fn cached_flat_partition_range_stream(
    value: Arc<PartitionRangeScanCacheValue>,
) -> BoxedRecordBatchStream {
    Box::pin(futures::stream::iter(
        value.batches.clone().into_iter().map(Ok),
    ))
}

/// Compacts the `__primary_key` dictionary column in a record batch by removing
/// unreferenced values. This reduces memory usage when caching batches whose
/// dictionary values array contains entries not referenced by any key.
///
/// Only compacts when the values array has more than 4 entries to avoid
/// unnecessary work for small dictionaries.
fn compact_pk_dictionary(batch: RecordBatch) -> RecordBatch {
    let pk_idx = primary_key_column_index(batch.num_columns());
    let pk_col = match batch
        .column(pk_idx)
        .as_any()
        .downcast_ref::<DictionaryArray<UInt32Type>>()
    {
        Some(dict) if dict.values().len() > 4 => dict,
        _ => return batch,
    };

    let old_values = pk_col
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("primary key dictionary values must be BinaryArray");
    let keys = pk_col.keys();

    // Single linear pass: since keys are sorted/grouped, we track when the key changes.
    let mut remap = vec![0u32; old_values.len()];
    let mut new_values: Vec<&[u8]> = Vec::new();
    let mut prev_key: Option<u32> = None;

    for key in keys.iter().flatten() {
        if prev_key != Some(key) {
            let new_index = new_values.len() as u32;
            new_values.push(old_values.value(key as usize));
            remap[key as usize] = new_index;
            prev_key = Some(key);
        }
    }

    if new_values.len() == old_values.len() {
        return batch; // all values are in use
    }

    let new_keys = UInt32Array::from_iter(keys.iter().map(|k| k.map(|v| remap[v as usize])));
    let new_values_array = BinaryArray::from_iter_values(new_values);
    let new_dict = DictionaryArray::new(new_keys, Arc::new(new_values_array));

    let mut columns: Vec<_> = batch.columns().to_vec();
    columns[pk_idx] = Arc::new(new_dict);
    RecordBatch::try_new(batch.schema(), columns).expect("schema should match after compaction")
}

/// Wraps a stream to cache its output for future partition-range cache hits.
pub(crate) fn cache_flat_partition_range_stream(
    mut stream: BoxedRecordBatchStream,
    cache_strategy: CacheStrategy,
    key: PartitionRangeScanCacheKey,
    part_metrics: PartitionMetrics,
) -> BoxedRecordBatchStream {
    Box::pin(try_stream! {
        let mut batches = Vec::new();
        let mut num_rows = 0usize;
        while let Some(batch) = stream.try_next().await? {
            let batch = compact_pk_dictionary(batch);
            num_rows += batch.num_rows();
            batches.push(batch.clone());
            yield batch;
        }

        if !batches.is_empty() {
            let value = Arc::new(PartitionRangeScanCacheValue::new(batches));

            part_metrics.inc_partition_range_cache_size(key.estimated_size() + value.estimated_size());
            common_telemetry::debug!(
                "Partition range cache put, digest: {:x}, num_batches: {}, num_rows: {}",
                key.digest(),
                value.batches.len(),
                num_rows,
            );
            cache_strategy.put_partition_range_result(key, value);
        } else {
            part_metrics.inc_partition_range_cache_size(key.estimated_size());
            let value = Arc::new(PartitionRangeScanCacheValue::new(batches));
            cache_strategy.put_partition_range_result(key, value);
        }
    })
}
