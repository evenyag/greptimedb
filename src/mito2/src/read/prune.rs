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

use std::ops::BitAnd;
use std::sync::Arc;

use common_recordbatch::filter::SimpleFilterEvaluator;
use common_time::Timestamp;
use datatypes::arrow::array::{Array, BinaryArray, BooleanArray};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::record_batch::RecordBatch;
use snafu::ResultExt;

use crate::error::{RecordBatchSnafu, Result};
use crate::memtable::{BoxedBatchIterator, PrimaryKeyRange};
use crate::read::last_row::RowGroupLastRowCachedReader;
use crate::read::{Batch, BatchReader};
use crate::sst::file::FileTimeRange;
use crate::sst::parquet::file_range::FileRangeContextRef;
use crate::sst::parquet::format::PrimaryKeyArray;
use crate::sst::parquet::reader::{FlatRowGroupReader, ReaderMetrics, RowGroupReader};

pub enum Source {
    RowGroup(RowGroupReader),
    LastRow(RowGroupLastRowCachedReader),
}

impl Source {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            Source::RowGroup(r) => r.next_batch().await,
            Source::LastRow(r) => r.next_batch().await,
        }
    }
}

pub struct PruneReader {
    /// Context for file ranges.
    context: FileRangeContextRef,
    source: Source,
    metrics: ReaderMetrics,
    /// Whether to skip field filters for this row group.
    skip_fields: bool,
    /// Primary key range for filtering.
    key_range: PrimaryKeyRange,
}

impl PruneReader {
    pub(crate) fn new_with_row_group_reader(
        ctx: FileRangeContextRef,
        reader: RowGroupReader,
        skip_fields: bool,
        key_range: PrimaryKeyRange,
    ) -> Self {
        Self {
            context: ctx,
            source: Source::RowGroup(reader),
            metrics: Default::default(),
            skip_fields,
            key_range,
        }
    }

    pub(crate) fn new_with_last_row_reader(
        ctx: FileRangeContextRef,
        reader: RowGroupLastRowCachedReader,
        skip_fields: bool,
        key_range: PrimaryKeyRange,
    ) -> Self {
        Self {
            context: ctx,
            source: Source::LastRow(reader),
            metrics: Default::default(),
            skip_fields,
            key_range,
        }
    }

    pub(crate) fn reset_source(&mut self, source: Source, skip_fields: bool) {
        self.source = source;
        self.skip_fields = skip_fields;
    }

    /// Merge metrics with the inner reader and return the merged metrics.
    pub(crate) fn metrics(&self) -> ReaderMetrics {
        let mut metrics = self.metrics.clone();
        match &self.source {
            Source::RowGroup(r) => {
                metrics.merge_from(r.metrics());
            }
            Source::LastRow(r) => {
                if let Some(inner_metrics) = r.metrics() {
                    metrics.merge_from(inner_metrics);
                }
            }
        }

        metrics
    }

    pub(crate) async fn next_batch(&mut self) -> Result<Option<Batch>> {
        while let Some(b) = self.source.next_batch().await? {
            match self.prune(b)? {
                Some(b) => {
                    return Ok(Some(b));
                }
                None => {
                    continue;
                }
            }
        }
        Ok(None)
    }

    /// Prunes batches by the key range and pushed down predicate.
    fn prune(&mut self, batch: Batch) -> Result<Option<Batch>> {
        // Check key range first (efficient batch-level rejection).
        // Each Batch represents a single primary key.
        if !self.key_range.contains(batch.primary_key()) {
            let num_rows_filtered = batch.num_rows();
            self.metrics.filter_metrics.rows_precise_filtered += num_rows_filtered;
            return Ok(None);
        }

        // fast path
        if self.context.filters().is_empty() {
            return Ok(Some(batch));
        }

        let num_rows_before_filter = batch.num_rows();
        let Some(batch_filtered) = self.context.precise_filter(batch, self.skip_fields)? else {
            // the entire batch is filtered out
            self.metrics.filter_metrics.rows_precise_filtered += num_rows_before_filter;
            return Ok(None);
        };

        // update metric
        let filtered_rows = num_rows_before_filter - batch_filtered.num_rows();
        self.metrics.filter_metrics.rows_precise_filtered += filtered_rows;

        if !batch_filtered.is_empty() {
            Ok(Some(batch_filtered))
        } else {
            Ok(None)
        }
    }
}

/// An iterator that prunes batches by time range and key range.
pub(crate) struct PruneTimeIterator {
    iter: BoxedBatchIterator,
    time_range: FileTimeRange,
    /// Precise time filters.
    time_filters: Option<Arc<Vec<SimpleFilterEvaluator>>>,
    /// Primary key range for filtering.
    key_range: PrimaryKeyRange,
}

impl PruneTimeIterator {
    /// Creates a new `PruneTimeIterator` with the given iterator, time range, and key range.
    pub(crate) fn new(
        iter: BoxedBatchIterator,
        time_range: FileTimeRange,
        time_filters: Option<Arc<Vec<SimpleFilterEvaluator>>>,
        key_range: PrimaryKeyRange,
    ) -> Self {
        Self {
            iter,
            time_range,
            time_filters,
            key_range,
        }
    }

    /// Prune batch by key range and time range.
    fn prune(&self, batch: Batch) -> Result<Batch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        // Apply key range filter first (can reject entire batch).
        // Each Batch represents rows for a single primary key.
        if !self.key_range.contains(batch.primary_key()) {
            // Skip this batch - key is out of range.
            return Ok(Batch::empty());
        }

        // fast path, the batch is within the time range.
        // Note that the time range is inclusive.
        if self.time_range.0 <= batch.first_timestamp().unwrap()
            && batch.last_timestamp().unwrap() <= self.time_range.1
        {
            return self.prune_by_time_filters(batch, Vec::new());
        }

        // slow path, prune the batch by time range.
        // Note that the timestamp precision may be different from the time range.
        // Safety: We know this is the timestamp type.
        let unit = batch
            .timestamps()
            .data_type()
            .as_timestamp()
            .unwrap()
            .unit();
        let mut mask = Vec::with_capacity(batch.timestamps().len());
        let timestamps = batch.timestamps_native().unwrap();
        for ts in timestamps {
            let ts = Timestamp::new(*ts, unit);
            if self.time_range.0 <= ts && ts <= self.time_range.1 {
                mask.push(true);
            } else {
                mask.push(false);
            }
        }

        self.prune_by_time_filters(batch, mask)
    }

    /// Prunes the batch by time filters.
    /// Also applies existing mask to the batch if the mask is not empty.
    fn prune_by_time_filters(&self, mut batch: Batch, existing_mask: Vec<bool>) -> Result<Batch> {
        if let Some(filters) = &self.time_filters {
            let mut mask = BooleanBuffer::new_set(batch.num_rows());
            for filter in filters.iter() {
                let result = filter
                    .evaluate_vector(batch.timestamps())
                    .context(RecordBatchSnafu)?;
                mask = mask.bitand(&result);
            }

            if !existing_mask.is_empty() {
                mask = mask.bitand(&BooleanBuffer::from(existing_mask));
            }

            batch.filter(&BooleanArray::from(mask).into())?;
        } else if !existing_mask.is_empty() {
            batch.filter(&BooleanArray::from(existing_mask).into())?;
        }

        Ok(batch)
    }

    // Prune and return the next non-empty batch.
    fn next_non_empty_batch(&mut self) -> Result<Option<Batch>> {
        while let Some(batch) = self.iter.next() {
            let batch = batch?;
            let pruned_batch = self.prune(batch)?;
            if !pruned_batch.is_empty() {
                return Ok(Some(pruned_batch));
            }
        }
        Ok(None)
    }
}

impl Iterator for PruneTimeIterator {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_non_empty_batch().transpose()
    }
}

pub enum FlatSource {
    RowGroup(FlatRowGroupReader),
}

impl FlatSource {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self {
            FlatSource::RowGroup(r) => r.next_batch(),
        }
    }
}

/// A flat format reader that returns RecordBatch instead of Batch.
pub struct FlatPruneReader {
    /// Context for file ranges.
    context: FileRangeContextRef,
    source: FlatSource,
    metrics: ReaderMetrics,
    /// Whether to skip field filters for this row group.
    skip_fields: bool,
    /// Primary key range for filtering.
    key_range: PrimaryKeyRange,
}

impl FlatPruneReader {
    pub(crate) fn new_with_row_group_reader(
        ctx: FileRangeContextRef,
        reader: FlatRowGroupReader,
        skip_fields: bool,
        key_range: PrimaryKeyRange,
    ) -> Self {
        Self {
            context: ctx,
            source: FlatSource::RowGroup(reader),
            metrics: Default::default(),
            skip_fields,
            key_range,
        }
    }

    /// Merge metrics with the inner reader and return the merged metrics.
    pub(crate) fn metrics(&self) -> ReaderMetrics {
        // FlatRowGroupReader doesn't collect metrics, so just return our own
        self.metrics.clone()
    }

    pub(crate) fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        while let Some(record_batch) = {
            let start = std::time::Instant::now();
            let batch = self.source.next_batch()?;
            self.metrics.scan_cost += start.elapsed();
            batch
        } {
            // Update metrics for the received batch
            self.metrics.num_rows += record_batch.num_rows();
            self.metrics.num_batches += 1;

            match self.prune_flat(record_batch)? {
                Some(filtered_batch) => {
                    return Ok(Some(filtered_batch));
                }
                None => {
                    continue;
                }
            }
        }

        Ok(None)
    }

    /// Prunes batches by the key range and pushed down predicate and returns RecordBatch.
    fn prune_flat(&mut self, record_batch: RecordBatch) -> Result<Option<RecordBatch>> {
        // Apply key range filter first
        let record_batch = match self.filter_by_key_range(record_batch)? {
            Some(filtered) => filtered,
            None => return Ok(None),
        };

        // fast path
        if self.context.filters().is_empty() {
            return Ok(Some(record_batch));
        }

        let num_rows_before_filter = record_batch.num_rows();
        let Some(filtered_batch) = self
            .context
            .precise_filter_flat(record_batch, self.skip_fields)?
        else {
            // the entire batch is filtered out
            self.metrics.filter_metrics.rows_precise_filtered += num_rows_before_filter;
            return Ok(None);
        };

        // update metric
        let filtered_rows = num_rows_before_filter - filtered_batch.num_rows();
        self.metrics.filter_metrics.rows_precise_filtered += filtered_rows;

        if filtered_batch.num_rows() > 0 {
            Ok(Some(filtered_batch))
        } else {
            Ok(None)
        }
    }

    /// Filters RecordBatch by key range by iterating through the __primary_key column.
    fn filter_by_key_range(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        // Short circuit if range is unbounded
        if self.key_range.is_unbounded() {
            return Ok(Some(batch));
        }

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(Some(batch));
        }

        let pk_col_idx =
            crate::sst::parquet::flat_format::primary_key_column_index(batch.num_columns());
        let pk_column = batch.column(pk_col_idx);
        let dict_array = pk_column
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .unwrap();
        let values_array = dict_array
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        let mut mask = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            // Primary key array is not null.
            let dict_index = dict_array.keys().value(i) as usize;
            let key = values_array.value(dict_index);
            mask.push(self.key_range.contains(key));
        }

        let mask_array = BooleanArray::from(mask);
        let filtered = datatypes::arrow::compute::filter_record_batch(&batch, &mask_array)
            .context(crate::error::ComputeArrowSnafu)?;

        let filtered_count = num_rows - filtered.num_rows();
        self.metrics.filter_metrics.rows_precise_filtered += filtered_count;

        if filtered.num_rows() > 0 {
            Ok(Some(filtered))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use api::v1::OpType;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{Expr, col, lit};

    use super::*;
    use crate::test_util::new_batch;

    #[test]
    fn test_prune_time_iter_empty() {
        let input = [];
        let iter = input.into_iter().map(Ok);
        let iter = PruneTimeIterator::new(
            Box::new(iter),
            (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(1000),
            ),
            None,
            PrimaryKeyRange::unbounded(),
        );
        let actual: Vec<_> = iter.map(|batch| batch.unwrap()).collect();
        assert!(actual.is_empty());
    }

    #[test]
    fn test_prune_time_iter_filter() {
        let input = [
            new_batch(
                b"k1",
                &[10, 11],
                &[20, 20],
                &[OpType::Put, OpType::Put],
                &[110, 111],
            ),
            new_batch(
                b"k1",
                &[15, 16],
                &[20, 20],
                &[OpType::Put, OpType::Put],
                &[115, 116],
            ),
            new_batch(
                b"k1",
                &[17, 18],
                &[20, 20],
                &[OpType::Put, OpType::Put],
                &[117, 118],
            ),
        ];

        let iter = input.clone().into_iter().map(Ok);
        let iter = PruneTimeIterator::new(
            Box::new(iter),
            (
                Timestamp::new_millisecond(10),
                Timestamp::new_millisecond(15),
            ),
            None,
            PrimaryKeyRange::unbounded(),
        );
        let actual: Vec<_> = iter.map(|batch| batch.unwrap()).collect();
        assert_eq!(
            actual,
            [
                new_batch(
                    b"k1",
                    &[10, 11],
                    &[20, 20],
                    &[OpType::Put, OpType::Put],
                    &[110, 111],
                ),
                new_batch(b"k1", &[15], &[20], &[OpType::Put], &[115],),
            ]
        );

        let iter = input.clone().into_iter().map(Ok);
        let iter = PruneTimeIterator::new(
            Box::new(iter),
            (
                Timestamp::new_millisecond(11),
                Timestamp::new_millisecond(20),
            ),
            None,
            PrimaryKeyRange::unbounded(),
        );
        let actual: Vec<_> = iter.map(|batch| batch.unwrap()).collect();
        assert_eq!(
            actual,
            [
                new_batch(b"k1", &[11], &[20], &[OpType::Put], &[111],),
                new_batch(
                    b"k1",
                    &[15, 16],
                    &[20, 20],
                    &[OpType::Put, OpType::Put],
                    &[115, 116],
                ),
                new_batch(
                    b"k1",
                    &[17, 18],
                    &[20, 20],
                    &[OpType::Put, OpType::Put],
                    &[117, 118],
                ),
            ]
        );

        let iter = input.into_iter().map(Ok);
        let iter = PruneTimeIterator::new(
            Box::new(iter),
            (
                Timestamp::new_millisecond(10),
                Timestamp::new_millisecond(18),
            ),
            None,
            PrimaryKeyRange::unbounded(),
        );
        let actual: Vec<_> = iter.map(|batch| batch.unwrap()).collect();
        assert_eq!(
            actual,
            [
                new_batch(
                    b"k1",
                    &[10, 11],
                    &[20, 20],
                    &[OpType::Put, OpType::Put],
                    &[110, 111],
                ),
                new_batch(
                    b"k1",
                    &[15, 16],
                    &[20, 20],
                    &[OpType::Put, OpType::Put],
                    &[115, 116],
                ),
                new_batch(
                    b"k1",
                    &[17, 18],
                    &[20, 20],
                    &[OpType::Put, OpType::Put],
                    &[117, 118],
                ),
            ]
        );
    }

    fn create_time_filters(expr: &[Expr]) -> Option<Arc<Vec<SimpleFilterEvaluator>>> {
        let filters = expr
            .iter()
            .map(|expr| SimpleFilterEvaluator::try_new(expr).unwrap())
            .collect();
        Some(Arc::new(filters))
    }

    #[test]
    fn test_prune_time_iter_with_time_filters() {
        let input = [
            new_batch(
                b"k1",
                &[10, 11],
                &[20, 20],
                &[OpType::Put, OpType::Put],
                &[110, 111],
            ),
            new_batch(
                b"k1",
                &[15, 16],
                &[20, 20],
                &[OpType::Put, OpType::Put],
                &[115, 116],
            ),
            new_batch(
                b"k1",
                &[17, 18],
                &[20, 20],
                &[OpType::Put, OpType::Put],
                &[117, 118],
            ),
        ];

        let iter = input.clone().into_iter().map(Ok);
        // We won't use the column name.
        let time_filters = create_time_filters(&[
            col("ts").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(10), None))),
            col("ts").lt(lit(ScalarValue::TimestampMillisecond(Some(16), None))),
        ]);
        let iter = PruneTimeIterator::new(
            Box::new(iter),
            (
                Timestamp::new_millisecond(10),
                Timestamp::new_millisecond(20),
            ),
            time_filters,
            PrimaryKeyRange::unbounded(),
        );
        let actual: Vec<_> = iter.map(|batch| batch.unwrap()).collect();
        assert_eq!(
            actual,
            [
                new_batch(
                    b"k1",
                    &[10, 11],
                    &[20, 20],
                    &[OpType::Put, OpType::Put],
                    &[110, 111],
                ),
                new_batch(b"k1", &[15], &[20], &[OpType::Put], &[115],),
            ]
        );
    }

    #[test]
    fn test_prune_time_iter_in_range_with_time_filters() {
        let input = [
            new_batch(
                b"k1",
                &[10, 11],
                &[20, 20],
                &[OpType::Put, OpType::Put],
                &[110, 111],
            ),
            new_batch(
                b"k1",
                &[15, 16],
                &[20, 20],
                &[OpType::Put, OpType::Put],
                &[115, 116],
            ),
            new_batch(
                b"k1",
                &[17, 18],
                &[20, 20],
                &[OpType::Put, OpType::Put],
                &[117, 118],
            ),
        ];

        let iter = input.clone().into_iter().map(Ok);
        // We won't use the column name.
        let time_filters = create_time_filters(&[
            col("ts").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(10), None))),
            col("ts").lt(lit(ScalarValue::TimestampMillisecond(Some(16), None))),
        ]);
        let iter = PruneTimeIterator::new(
            Box::new(iter),
            (
                Timestamp::new_millisecond(5),
                Timestamp::new_millisecond(18),
            ),
            time_filters,
            PrimaryKeyRange::unbounded(),
        );
        let actual: Vec<_> = iter.map(|batch| batch.unwrap()).collect();
        assert_eq!(
            actual,
            [
                new_batch(
                    b"k1",
                    &[10, 11],
                    &[20, 20],
                    &[OpType::Put, OpType::Put],
                    &[110, 111],
                ),
                new_batch(b"k1", &[15], &[20], &[OpType::Put], &[115],),
            ]
        );
    }
}
