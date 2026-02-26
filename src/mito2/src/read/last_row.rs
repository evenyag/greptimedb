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

//! Utilities to read the last row of each time series.

use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use datatypes::arrow::array::BinaryArray;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::timestamp::timestamp_array_to_primitive;
use datatypes::vectors::UInt32Vector;
use store_api::storage::{FileId, TimeSeriesRowSelector};

use crate::cache::{
    CacheStrategy, SelectorResult, SelectorResultKey, SelectorResultValue,
    selector_result_cache_hit, selector_result_cache_miss,
};
use crate::error::Result;
use crate::read::{Batch, BatchReader, BoxedBatchReader};
use crate::sst::parquet::flat_format::{primary_key_column_index, time_index_column_index};
use crate::sst::parquet::format::PrimaryKeyArray;
use crate::sst::parquet::reader::{FlatRowGroupReader, ReaderMetrics, RowGroupReader};

/// Reader to keep the last row for each time series.
/// It assumes that batches from the input reader are
/// - sorted
/// - all deleted rows has been filtered.
/// - not empty
///
/// This reader is different from the [MergeMode](crate::region::options::MergeMode) as
/// it focus on time series (the same key).
#[allow(dead_code)]
pub(crate) struct LastRowReader {
    /// Inner reader.
    reader: BoxedBatchReader,
    /// The last batch pending to return.
    selector: LastRowSelector,
}

#[allow(dead_code)]
impl LastRowReader {
    /// Creates a new `LastRowReader`.
    pub(crate) fn new(reader: BoxedBatchReader) -> Self {
        Self {
            reader,
            selector: LastRowSelector::default(),
        }
    }

    /// Returns the last row of the next key.
    pub(crate) async fn next_last_row(&mut self) -> Result<Option<Batch>> {
        while let Some(batch) = self.reader.next_batch().await? {
            if let Some(yielded) = self.selector.on_next(batch) {
                return Ok(Some(yielded));
            }
        }
        Ok(self.selector.finish())
    }
}

#[async_trait]
impl BatchReader for LastRowReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.next_last_row().await
    }
}

/// Cached last row reader for specific row group.
/// If the last rows for current row group are already cached, this reader returns the cached value.
/// If cache misses, [RowGroupLastRowReader] reads last rows from row group and updates the cache
/// upon finish.
#[allow(dead_code)]
pub(crate) enum RowGroupLastRowCachedReader {
    /// Cache hit, reads last rows from cached value.
    Hit(LastRowCacheReader),
    /// Cache miss, reads from row group reader and update cache.
    Miss(RowGroupLastRowReader),
}

#[allow(dead_code)]
impl RowGroupLastRowCachedReader {
    pub(crate) fn new(
        file_id: FileId,
        row_group_idx: usize,
        cache_strategy: CacheStrategy,
        row_group_reader: RowGroupReader,
    ) -> Self {
        let key = SelectorResultKey {
            file_id,
            row_group_idx,
            selector: TimeSeriesRowSelector::LastRow,
        };

        if let Some(value) = cache_strategy.get_selector_result(&key) {
            let schema_matches =
                value.projection == row_group_reader.read_format().projection_indices();
            let cache_matches = matches!(value.result, SelectorResult::PrimaryKey(_));
            if schema_matches && cache_matches {
                // Schema matches, use cache batches.
                Self::new_hit(value)
            } else {
                Self::new_miss(key, row_group_reader, cache_strategy)
            }
        } else {
            Self::new_miss(key, row_group_reader, cache_strategy)
        }
    }

    /// Gets the underlying reader metrics if uncached.
    pub(crate) fn metrics(&self) -> Option<&ReaderMetrics> {
        match self {
            RowGroupLastRowCachedReader::Hit(_) => None,
            RowGroupLastRowCachedReader::Miss(reader) => Some(reader.metrics()),
        }
    }

    /// Creates new Hit variant and updates metrics.
    fn new_hit(value: Arc<SelectorResultValue>) -> Self {
        selector_result_cache_hit();
        Self::Hit(LastRowCacheReader { value, idx: 0 })
    }

    /// Creates new Miss variant and updates metrics.
    fn new_miss(
        key: SelectorResultKey,
        row_group_reader: RowGroupReader,
        cache_strategy: CacheStrategy,
    ) -> Self {
        selector_result_cache_miss();
        Self::Miss(RowGroupLastRowReader::new(
            key,
            row_group_reader,
            cache_strategy,
        ))
    }
}

#[async_trait]
impl BatchReader for RowGroupLastRowCachedReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            RowGroupLastRowCachedReader::Hit(r) => r.next_batch().await,
            RowGroupLastRowCachedReader::Miss(r) => r.next_batch().await,
        }
    }
}

/// Last row reader that returns the cached last rows for row group.
pub(crate) struct LastRowCacheReader {
    value: Arc<SelectorResultValue>,
    idx: usize,
}

impl LastRowCacheReader {
    /// Iterates cached last rows.
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        match &self.value.result {
            SelectorResult::PrimaryKey(result) => {
                if self.idx < result.len() {
                    let res = Ok(Some(result[self.idx].clone()));
                    self.idx += 1;
                    res
                } else {
                    Ok(None)
                }
            }
            SelectorResult::Flat(_) => Ok(None),
        }
    }
}

pub(crate) struct RowGroupLastRowReader {
    key: SelectorResultKey,
    reader: RowGroupReader,
    selector: LastRowSelector,
    yielded_batches: Vec<Batch>,
    cache_strategy: CacheStrategy,
    /// Index buffer to take a new batch from the last row.
    take_index: UInt32Vector,
}

#[allow(dead_code)]
impl RowGroupLastRowReader {
    fn new(key: SelectorResultKey, reader: RowGroupReader, cache_strategy: CacheStrategy) -> Self {
        Self {
            key,
            reader,
            selector: LastRowSelector::default(),
            yielded_batches: vec![],
            cache_strategy,
            take_index: UInt32Vector::from_vec(vec![0]),
        }
    }

    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        while let Some(batch) = self.reader.next_batch().await? {
            if let Some(yielded) = self.selector.on_next(batch) {
                push_yielded_batches(yielded.clone(), &self.take_index, &mut self.yielded_batches)?;
                return Ok(Some(yielded));
            }
        }
        let last_batch = if let Some(last_batch) = self.selector.finish() {
            push_yielded_batches(
                last_batch.clone(),
                &self.take_index,
                &mut self.yielded_batches,
            )?;
            Some(last_batch)
        } else {
            None
        };

        // All last rows in row group are yielded, update cache.
        self.maybe_update_cache();
        Ok(last_batch)
    }

    /// Updates row group's last row cache if cache manager is present.
    fn maybe_update_cache(&mut self) {
        if self.yielded_batches.is_empty() {
            // we always expect that row groups yields batches.
            return;
        }
        let value = Arc::new(SelectorResultValue::new(
            std::mem::take(&mut self.yielded_batches),
            self.reader.read_format().projection_indices().to_vec(),
        ));
        self.cache_strategy.put_selector_result(self.key, value);
    }

    fn metrics(&self) -> &ReaderMetrics {
        self.reader.metrics()
    }
}

/// Push last row into `yielded_batches`.
fn push_yielded_batches(
    mut batch: Batch,
    take_index: &UInt32Vector,
    yielded_batches: &mut Vec<Batch>,
) -> Result<()> {
    assert_eq!(1, batch.num_rows());
    batch.take_in_place(take_index)?;
    yielded_batches.push(batch);

    Ok(())
}

/// Cached flat last row reader for specific row group.
/// If the last rows for current row group are already cached, this reader returns the cached value.
/// If cache misses, [FlatRowGroupLastRowReader] reads last rows from row group and updates the cache
/// upon finish.
pub(crate) enum FlatRowGroupLastRowCachedReader {
    /// Cache hit, reads last rows from cached value.
    Hit(FlatLastRowCacheReader),
    /// Cache miss, reads from row group reader and update cache.
    Miss(FlatRowGroupLastRowReader),
}

impl FlatRowGroupLastRowCachedReader {
    pub(crate) fn new(
        file_id: FileId,
        row_group_idx: usize,
        cache_strategy: CacheStrategy,
        row_group_reader: FlatRowGroupReader,
        projection: Vec<usize>,
    ) -> Self {
        let key = SelectorResultKey {
            file_id,
            row_group_idx,
            selector: TimeSeriesRowSelector::LastRow,
        };

        if let Some(value) = cache_strategy.get_selector_result(&key) {
            let cache_matches = matches!(value.result, SelectorResult::Flat(_));
            if value.projection == projection && cache_matches {
                selector_result_cache_hit();
                return Self::Hit(FlatLastRowCacheReader { value, idx: 0 });
            }
        }

        selector_result_cache_miss();
        Self::Miss(FlatRowGroupLastRowReader::new(
            key,
            row_group_reader,
            projection,
            cache_strategy,
        ))
    }

    /// Gets the underlying reader metrics if uncached.
    pub(crate) fn metrics(&self) -> Option<&ReaderMetrics> {
        let _ = self;
        None
    }

    pub(crate) fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self {
            FlatRowGroupLastRowCachedReader::Hit(r) => r.next_batch(),
            FlatRowGroupLastRowCachedReader::Miss(r) => r.next_batch(),
        }
    }
}

/// Flat last row reader that returns the cached last rows for row group.
pub(crate) struct FlatLastRowCacheReader {
    value: Arc<SelectorResultValue>,
    idx: usize,
}

impl FlatLastRowCacheReader {
    /// Iterates cached last rows.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match &self.value.result {
            SelectorResult::Flat(result) => {
                if self.idx < result.len() {
                    let res = Ok(Some(result[self.idx].clone()));
                    self.idx += 1;
                    res
                } else {
                    Ok(None)
                }
            }
            SelectorResult::PrimaryKey(_) => Ok(None),
        }
    }
}

/// Pending rows for the same key and timestamp.
struct PendingFlatRows {
    key: Vec<u8>,
    timestamp: i64,
    batches: Vec<RecordBatch>,
}

pub(crate) struct FlatRowGroupLastRowReader {
    cache_key: SelectorResultKey,
    reader: FlatRowGroupReader,
    projection: Vec<usize>,
    selector: FlatLastTimestampSelector,
    output: VecDeque<RecordBatch>,
    yielded_batches: Vec<RecordBatch>,
    cache_strategy: CacheStrategy,
    finished: bool,
}

impl FlatRowGroupLastRowReader {
    fn new(
        key: SelectorResultKey,
        reader: FlatRowGroupReader,
        projection: Vec<usize>,
        cache_strategy: CacheStrategy,
    ) -> Self {
        Self {
            cache_key: key,
            reader,
            projection,
            selector: FlatLastTimestampSelector::default(),
            output: VecDeque::new(),
            yielded_batches: vec![],
            cache_strategy,
            finished: false,
        }
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(batch) = self.output.pop_front() {
            self.yielded_batches.push(batch.clone());
            return Ok(Some(batch));
        }
        if self.finished {
            return Ok(None);
        }

        loop {
            if let Some(batch) = self.reader.next_batch()? {
                self.selector.on_next(batch, &mut self.output);
                if let Some(batch) = self.output.pop_front() {
                    self.yielded_batches.push(batch.clone());
                    return Ok(Some(batch));
                }
            } else {
                self.selector.finish(&mut self.output);
                self.finished = true;
                self.maybe_update_cache();
                if let Some(batch) = self.output.pop_front() {
                    self.yielded_batches.push(batch.clone());
                    return Ok(Some(batch));
                }
                return Ok(None);
            }
        }
    }

    /// Updates row group's last row cache if cache manager is present.
    fn maybe_update_cache(&mut self) {
        if self.yielded_batches.is_empty() {
            // we always expect that row groups yields batches.
            return;
        }
        let value = Arc::new(SelectorResultValue::new_flat(
            std::mem::take(&mut self.yielded_batches),
            self.projection.clone(),
        ));
        self.cache_strategy
            .put_selector_result(self.cache_key, value);
    }
}

/// Common struct that selects only the last row of each time series.
#[derive(Default)]
pub struct LastRowSelector {
    last_batch: Option<Batch>,
}

impl LastRowSelector {
    /// Handles next batch. Return the yielding batch if present.
    pub fn on_next(&mut self, batch: Batch) -> Option<Batch> {
        if let Some(last) = &self.last_batch {
            if last.primary_key() == batch.primary_key() {
                // Same key, update last batch.
                self.last_batch = Some(batch);
                None
            } else {
                // Different key, return the last row in `last` and update `last_batch` by
                // current batch.
                debug_assert!(!last.is_empty());
                let last_row = last.slice(last.num_rows() - 1, 1);
                self.last_batch = Some(batch);
                Some(last_row)
            }
        } else {
            self.last_batch = Some(batch);
            None
        }
    }

    /// Finishes the selector and returns the pending batch if any.
    pub fn finish(&mut self) -> Option<Batch> {
        if let Some(last) = self.last_batch.take() {
            // This is the last key.
            let last_row = last.slice(last.num_rows() - 1, 1);
            return Some(last_row);
        }
        None
    }
}

/// Common struct that selects rows with the last timestamp of each key in flat format.
#[derive(Default)]
pub struct FlatLastTimestampSelector {
    pending: Option<PendingFlatRows>,
}

impl FlatLastTimestampSelector {
    /// Handles next batch and appends yielded batches.
    pub fn on_next(&mut self, batch: RecordBatch, output: &mut VecDeque<RecordBatch>) {
        if batch.num_rows() == 0 {
            return;
        }

        let groups = split_by_key(&batch);
        for (idx, (start, end)) in groups.iter().enumerate() {
            let is_last_group = idx + 1 == groups.len();
            let key = primary_key_at(&batch, end - 1).to_vec();
            let (timestamp, selected_rows) = last_timestamp_rows(&batch, *start, *end);

            if idx == 0
                && let Some(mut pending) = self.pending.take()
            {
                if pending.key == key {
                    if timestamp > pending.timestamp {
                        pending.timestamp = timestamp;
                        pending.batches = vec![selected_rows];
                    } else if timestamp == pending.timestamp {
                        pending.batches.push(selected_rows);
                    }

                    if is_last_group {
                        self.pending = Some(pending);
                    } else {
                        output.extend(pending.batches);
                    }
                    continue;
                } else {
                    output.extend(pending.batches);
                }
            }

            if is_last_group {
                self.pending = Some(PendingFlatRows {
                    key,
                    timestamp,
                    batches: vec![selected_rows],
                });
            } else {
                output.push_back(selected_rows);
            }
        }
    }

    /// Finishes the selector and appends pending rows.
    pub fn finish(&mut self, output: &mut VecDeque<RecordBatch>) {
        if let Some(pending) = self.pending.take() {
            output.extend(pending.batches);
        }
    }
}

/// Gets the primary key at `idx` in a flat-format record batch.
fn primary_key_at(batch: &RecordBatch, idx: usize) -> &[u8] {
    let primary_key = batch
        .column(primary_key_column_index(batch.num_columns()))
        .as_any()
        .downcast_ref::<PrimaryKeyArray>()
        .unwrap();
    let key = primary_key.keys().value(idx);
    let binary_values = primary_key
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    binary_values.value(key as usize)
}

/// Splits rows into contiguous key ranges [start, end).
fn split_by_key(batch: &RecordBatch) -> Vec<(usize, usize)> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Vec::new();
    }

    let mut groups = Vec::with_capacity(8);
    let mut start = 0;
    for i in 1..num_rows {
        if primary_key_at(batch, i - 1) != primary_key_at(batch, i) {
            groups.push((start, i));
            start = i;
        }
    }
    groups.push((start, num_rows));
    groups
}

/// Returns rows with the last timestamp in [start, end).
fn last_timestamp_rows(batch: &RecordBatch, start: usize, end: usize) -> (i64, RecordBatch) {
    let timestamps = batch.column(time_index_column_index(batch.num_columns()));
    let (ts_values, _unit) = timestamp_array_to_primitive(timestamps).unwrap();
    let ts = ts_values.values();
    let last_ts = ts[end - 1];

    let mut keep_start = end - 1;
    while keep_start > start && ts[keep_start - 1] == last_ts {
        keep_start -= 1;
    }

    (last_ts, batch.slice(keep_start, end - keep_start))
}
#[cfg(test)]
mod tests {
    use api::v1::OpType;

    use super::*;
    use crate::test_util::{VecBatchReader, check_reader_result, new_batch};

    #[tokio::test]
    async fn test_last_row_one_batch() {
        let input = [new_batch(
            b"k1",
            &[1, 2],
            &[11, 11],
            &[OpType::Put, OpType::Put],
            &[21, 22],
        )];
        let reader = VecBatchReader::new(&input);
        let mut reader = LastRowReader::new(Box::new(reader));
        check_reader_result(
            &mut reader,
            &[new_batch(b"k1", &[2], &[11], &[OpType::Put], &[22])],
        )
        .await;

        // Only one row.
        let input = [new_batch(b"k1", &[1], &[11], &[OpType::Put], &[21])];
        let reader = VecBatchReader::new(&input);
        let mut reader = LastRowReader::new(Box::new(reader));
        check_reader_result(
            &mut reader,
            &[new_batch(b"k1", &[1], &[11], &[OpType::Put], &[21])],
        )
        .await;
    }

    #[tokio::test]
    async fn test_last_row_multi_batch() {
        let input = [
            new_batch(
                b"k1",
                &[1, 2],
                &[11, 11],
                &[OpType::Put, OpType::Put],
                &[21, 22],
            ),
            new_batch(
                b"k1",
                &[3, 4],
                &[11, 11],
                &[OpType::Put, OpType::Put],
                &[23, 24],
            ),
            new_batch(
                b"k2",
                &[1, 2],
                &[11, 11],
                &[OpType::Put, OpType::Put],
                &[31, 32],
            ),
        ];
        let reader = VecBatchReader::new(&input);
        let mut reader = LastRowReader::new(Box::new(reader));
        check_reader_result(
            &mut reader,
            &[
                new_batch(b"k1", &[4], &[11], &[OpType::Put], &[24]),
                new_batch(b"k2", &[2], &[11], &[OpType::Put], &[32]),
            ],
        )
        .await;
    }
}
