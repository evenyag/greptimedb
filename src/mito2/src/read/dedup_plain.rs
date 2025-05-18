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

//! Utilities to remove duplicate rows from a sorted batch.

use std::cmp::Ordering;
use std::ops::Range;

use async_stream::try_stream;
use common_telemetry::debug;
use datatypes::arrow::array::{
    make_comparator, Array, ArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::compute::kernels::cmp::distinct;
use datatypes::arrow::compute::{
    concat_batches, partition, take_record_batch, Partitions, SortOptions, TakeOptions,
};
use datatypes::arrow::datatypes::{DataType, TimeUnit};
use datatypes::arrow::error::ArrowError;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::compute::take;
use futures::{Stream, TryStreamExt};
use snafu::ResultExt;

use crate::error::{ComputeArrowSnafu, NewRecordBatchSnafu, Result};
use crate::metrics::MERGE_FILTER_ROWS_TOTAL;
use crate::read::batch::plain::PlainBatch;
use crate::read::BoxedPlainBatchStream;

/// A reader that dedup sorted batches from a source based on the
/// dedup strategy.
pub(crate) struct PlainDedupReader<R, S> {
    source: R,
    strategy: S,
    metrics: DedupMetrics,
}

impl<R, S> PlainDedupReader<R, S> {
    /// Creates a new dedup reader.
    pub(crate) fn new(source: R, strategy: S) -> Self {
        Self {
            source,
            strategy,
            metrics: DedupMetrics::default(),
        }
    }
}

impl<
        R: Stream<Item = Result<PlainBatch>> + Unpin + Send + 'static,
        S: PlainDedupStrategy + 'static,
    > PlainDedupReader<R, S>
{
    /// Converts the reader into a stream.
    pub(crate) fn into_stream(mut self) -> BoxedPlainBatchStream {
        let stream = try_stream! {
            while let Some(batch) = self.fetch_next_batch().await? {
                yield batch;
            }
        };

        Box::pin(stream)
    }

    /// Returns the next deduplicated batch.
    async fn fetch_next_batch(&mut self) -> Result<Option<PlainBatch>> {
        while let Some(batch) = self.source.try_next().await? {
            if let Some(batch) = self.strategy.push_batch(batch, &mut self.metrics)? {
                return Ok(Some(batch));
            }
        }

        self.strategy.finish(&mut self.metrics)
    }
}

// #[async_trait]
// impl<R: Stream<Item = Result<PlainBatch>> + Unpin + Send, S: PlainDedupStrategy> BatchReader
//     for PlainDedupReader<R, S>
// {
//     async fn next_batch(&mut self) -> Result<Option<Batch>> {
//         self.fetch_next_batch().await
//     }
// }

impl<R, S> Drop for PlainDedupReader<R, S> {
    fn drop(&mut self) {
        debug!("Dedup reader finished, metrics: {:?}", self.metrics);

        MERGE_FILTER_ROWS_TOTAL
            .with_label_values(&["dedup"])
            .inc_by(self.metrics.num_unselected_rows as u64);
        MERGE_FILTER_ROWS_TOTAL
            .with_label_values(&["delete"])
            .inc_by(self.metrics.num_unselected_rows as u64);
    }
}

#[cfg(test)]
impl<R, S> PlainDedupReader<R, S> {
    fn metrics(&self) -> &DedupMetrics {
        &self.metrics
    }
}

/// Strategy to remove duplicate rows from sorted batches.
pub(crate) trait PlainDedupStrategy: Send {
    /// Pushes a batch to the dedup strategy.
    /// Returns the deduplicated batch.
    fn push_batch(
        &mut self,
        batch: PlainBatch,
        metrics: &mut DedupMetrics,
    ) -> Result<Option<PlainBatch>>;

    /// Finishes the deduplication and resets the strategy.
    ///
    /// Users must ensure that `push_batch` is called for all batches before
    /// calling this method.
    fn finish(&mut self, metrics: &mut DedupMetrics) -> Result<Option<PlainBatch>>;
}

/// Returns true if the left row at `left` and the right row at `righ` has the same key.
/// The key columns are compared in the order of `key_indices`.
pub(crate) fn has_same_key(
    key_indices: &[usize],
    left: &RecordBatch,
    left_row_idx: usize,
    right: &RecordBatch,
    right_row_idx: usize,
) -> Result<bool> {
    for &idx in key_indices {
        let left_array = left.column(idx);
        let right_array = right.column(idx);

        let comparator = make_comparator(
            left_array.as_ref(),
            right_array.as_ref(),
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        )
        .context(ComputeArrowSnafu)?;

        // Compare values at the specified row indices
        match comparator(left_row_idx, right_row_idx) {
            Ordering::Equal => {
                // Keys match so far, continue checking other key columns
                continue;
            }
            _ => {
                // Keys don't match, return false
                return Ok(false);
            }
        }
    }

    // All key columns matched
    Ok(true)
}

/// Gets the timestamp value from the timestamp array.
///
/// # Panics
/// Panics if the array is not a timestamp array or
/// the index is out of bound.
fn timestamp_value(array: &dyn Array, idx: usize) -> i64 {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, None) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            array.value(idx)
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            array.value(idx)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            array.value(idx)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            array.value(idx)
        }
        _ => panic!("Expected timestamp array, got: {:?}", array.data_type()),
    }
}

/// Dedup strategy that keeps the row with latest sequence of each key.
pub(crate) struct PlainLastRow {
    /// Meta of the last row in the previous batch that has the same key
    /// as the batch to push.
    prev_batch: Option<BatchLastRow>,
    /// Indices of the primary key columns.
    pk_indices: Vec<usize>,
    /// Index of the time index column.
    timestamp_index: usize,
    /// Filter deleted rows.
    filter_deleted: bool,
}

impl PlainLastRow {
    /// Creates a new strategy with the given `filter_deleted` flag.
    pub(crate) fn new(
        pk_indices: Vec<usize>,
        timestamp_index: usize,
        filter_deleted: bool,
    ) -> Self {
        Self {
            prev_batch: None,
            pk_indices,
            timestamp_index,
            filter_deleted,
        }
    }

    /// Remove duplications from the batch without considering previous rows.
    fn dedup_one_batch(
        batch: RecordBatch,
        pk_indices: &[usize],
        timestamp_index: usize,
    ) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        if num_rows < 2 {
            return Ok(batch);
        }

        let timestamps = batch.column(timestamp_index);
        // Checks duplications based on the timestamp.
        let mask = find_boundaries(timestamps).context(ComputeArrowSnafu)?;
        if mask.count_set_bits() == num_rows - 1 {
            // Fast path: No duplication.
            return Ok(batch);
        }

        // The batch has duplicated timestamps, but it doesn't mean it must
        // has duplicated rows.
        // Partitions the batch by the primary key and time index.
        let columns: Vec<_> = pk_indices
            .iter()
            .chain([&timestamp_index])
            .map(|index| batch.column(*index).clone())
            .collect();
        let partitions = partition(&columns).context(ComputeArrowSnafu)?;

        Self::dedup_by_partitions(batch, &partitions)
    }

    /// Remove depulications for each partition.
    fn dedup_by_partitions(batch: RecordBatch, partitions: &Partitions) -> Result<RecordBatch> {
        let ranges = partitions.ranges();
        // Each range at least has 1 row.
        let num_duplications: usize = ranges.iter().map(|r| r.end - r.start - 1).sum();
        if num_duplications == 0 {
            // Fast path, no duplications.
            return Ok(batch);
        }

        // Always takes the first row in each range.
        let take_indices: UInt64Array = ranges.iter().map(|r| Some(r.start as u64)).collect();
        take_record_batch(&batch, &take_indices).context(ComputeArrowSnafu)
    }
}

impl PlainDedupStrategy for PlainLastRow {
    fn push_batch(
        &mut self,
        batch: PlainBatch,
        metrics: &mut DedupMetrics,
    ) -> Result<Option<PlainBatch>> {
        if batch.is_empty() {
            return Ok(None);
        }

        // Dedup current batch to ensure no duplication before we checking the previous row.
        let mut batch = Self::dedup_one_batch(
            batch.into_record_batch(),
            &self.pk_indices,
            self.timestamp_index,
        )?;

        if let Some(prev_batch) = &self.prev_batch {
            // If we have previous batch.
            if prev_batch.is_last_row_duplicated(&self.pk_indices, self.timestamp_index, &batch)? {
                // Duplicated with the last batch, skip the first row.
                batch = batch.slice(1, batch.num_rows() - 1);
            }
        }

        if batch.num_rows() == 0 {
            // We don't need to update `prev_batch` because they have the same
            // key and timestamp.
            return Ok(None);
        }

        // Store current batch to `prev_batch` so we could compare the next batch
        // with this batch. We store batch before filtering it as rows with `OpType::Delete`
        // would be removed from the batch after filter, then we may store an incorrect `last row`
        // of previous batch.
        // Safety: We checked the batch is not empty before.
        self.prev_batch = Some(BatchLastRow {
            last_batch: batch.clone(),
        });

        // FIXME(yingwen): filter deleted
        let batch = PlainBatch::new(batch);
        Ok(Some(batch))
    }

    // fn push_batch(
    //     &mut self,
    //     mut batch: PlainBatch,
    //     metrics: &mut DedupMetrics,
    // ) -> Result<Option<PlainBatch>> {
    //     if batch.is_empty() {
    //         return Ok(None);
    //     }
    //     debug_assert!(batch.first_timestamp().is_some());
    //     let prev_timestamp = match &self.prev_batch {
    //         Some(prev_batch) => {
    //             if prev_batch.primary_key != batch.primary_key() {
    //                 // The key has changed. This is the first batch of the
    //                 // new key.
    //                 None
    //             } else {
    //                 Some(prev_batch.timestamp)
    //             }
    //         }
    //         None => None,
    //     };
    //     if batch.first_timestamp() == prev_timestamp {
    //         metrics.num_unselected_rows += 1;
    //         // This batch contains a duplicate row, skip it.
    //         if batch.num_rows() == 1 {
    //             // We don't need to update `prev_batch` because they have the same
    //             // key and timestamp.
    //             return Ok(None);
    //         }
    //         // Skips the first row.
    //         batch = batch.slice(1, batch.num_rows() - 1);
    //     }

    //     // Store current batch to `prev_batch` so we could compare the next batch
    //     // with this batch. We store batch before filtering it as rows with `OpType::Delete`
    //     // would be removed from the batch after filter, then we may store an incorrect `last row`
    //     // of previous batch.
    //     match &mut self.prev_batch {
    //         Some(prev) => {
    //             // Reuse the primary key buffer.
    //             prev.primary_key.clear();
    //             prev.primary_key.extend_from_slice(batch.primary_key());
    //             prev.timestamp = batch.last_timestamp().unwrap();
    //         }
    //         None => {
    //             self.prev_batch = Some(BatchLastRow {
    //                 primary_key: batch.primary_key().to_vec(),
    //                 timestamp: batch.last_timestamp().unwrap(),
    //             })
    //         }
    //     }

    //     // Filters deleted rows.
    //     if self.filter_deleted {
    //         filter_deleted_from_batch(&mut batch, metrics)?;
    //     }

    //     // The batch can become empty if all rows are deleted.
    //     if batch.is_empty() {
    //         Ok(None)
    //     } else {
    //         Ok(Some(batch))
    //     }
    // }

    fn finish(&mut self, _metrics: &mut DedupMetrics) -> Result<Option<PlainBatch>> {
        Ok(None)
    }
}

/// State of the batch with the last row for dedup.
struct BatchLastRow {
    /// The record batch that contains the last row.
    /// The record batch must has at least one row.
    last_batch: RecordBatch,
}

impl BatchLastRow {
    /// Returns a new [BatchLastRow] if the record batch is not empty.
    fn try_new(record_batch: RecordBatch) -> Option<Self> {
        if record_batch.num_rows() > 0 {
            Some(Self {
                last_batch: record_batch,
            })
        } else {
            None
        }
    }

    /// Returns true if the first row of the input `batch` is duplicated with the last row.
    fn is_last_row_duplicated(
        &self,
        key_indices: &[usize],
        timestamp_index: usize,
        batch: &RecordBatch,
    ) -> Result<bool> {
        if batch.num_rows() == 0 {
            return Ok(false);
        }

        let last_timestamp = timestamp_value(
            self.last_batch.column(timestamp_index),
            self.last_batch.num_rows() - 1,
        );
        let batch_timestamp = timestamp_value(batch.column(timestamp_index), 0);
        if batch_timestamp != last_timestamp {
            return Ok(false);
        }

        has_same_key(
            key_indices,
            &self.last_batch,
            self.last_batch.num_rows() - 1,
            batch,
            0,
        )
    }
}

// Port from https://github.com/apache/arrow-rs/blob/55.0.0/arrow-ord/src/partition.rs#L155-L168
/// Returns a mask with bits set whenever the value or nullability changes
fn find_boundaries(v: &dyn Array) -> Result<BooleanBuffer, ArrowError> {
    let slice_len = v.len() - 1;
    let v1 = v.slice(0, slice_len);
    let v2 = v.slice(1, slice_len);

    if !v.data_type().is_nested() {
        return Ok(distinct(&v1, &v2)?.values().clone());
    }
    // Given that we're only comparing values, null ordering in the input or
    // sort options do not matter.
    let cmp = make_comparator(&v1, &v2, SortOptions::default())?;
    Ok((0..slice_len).map(|i| !cmp(i, i).is_eq()).collect())
}

// /// Removes deleted rows from the batch and updates metrics.
// fn filter_deleted_from_batch(batch: &mut Batch, metrics: &mut DedupMetrics) -> Result<()> {
//     let num_rows = batch.num_rows();
//     batch.filter_deleted()?;
//     let num_rows_after_filter = batch.num_rows();
//     let num_deleted = num_rows - num_rows_after_filter;
//     metrics.num_deleted_rows += num_deleted;
//     metrics.num_unselected_rows += num_deleted;

//     Ok(())
// }

/// Metrics for deduplication.
#[derive(Debug, Default)]
pub(crate) struct DedupMetrics {
    /// Number of rows removed during deduplication.
    pub(crate) num_unselected_rows: usize,
    /// Number of deleted rows.
    pub(crate) num_deleted_rows: usize,
}

// /// Buffer to store fields in the last row to merge.
// ///
// /// Usage:
// /// We should call `maybe_init()` to initialize the builder and then call `push_first_row()`
// /// to push the first row of batches that the timestamp is the same as the row in this builder.
// /// Finally we should call `merge_last_non_null()` to merge the last non-null fields and
// /// return the merged batch.
// struct LastFieldsBuilder {
//     /// Filter deleted rows.
//     filter_deleted: bool,
//     /// Fields builders, lazy initialized.
//     builders: Vec<Box<dyn MutableVector>>,
//     /// Last fields to merge, lazy initialized.
//     /// Only initializes this field when `skip_merge()` is false.
//     last_fields: Vec<Value>,
//     /// Whether the last row (including `last_fields`) has null field.
//     /// Only sets this field when `contains_deletion` is false.
//     contains_null: bool,
//     /// Whether the last row has delete op. If true, skips merging fields.
//     contains_deletion: bool,
//     /// Whether the builder is initialized.
//     initialized: bool,
// }

// impl LastFieldsBuilder {
//     /// Returns a new builder with the given `filter_deleted` flag.
//     fn new(filter_deleted: bool) -> Self {
//         Self {
//             filter_deleted,
//             builders: Vec::new(),
//             last_fields: Vec::new(),
//             contains_null: false,
//             contains_deletion: false,
//             initialized: false,
//         }
//     }

//     /// Initializes the builders with the last row of the batch.
//     fn maybe_init(&mut self, batch: &Batch) {
//         debug_assert!(!batch.is_empty());

//         if self.initialized {
//             // Already initialized or no fields to merge.
//             return;
//         }

//         self.initialized = true;

//         if batch.fields().is_empty() {
//             // No fields to merge.
//             return;
//         }

//         let last_idx = batch.num_rows() - 1;
//         let fields = batch.fields();
//         // Safety: The last_idx is valid.
//         self.contains_deletion =
//             batch.op_types().get_data(last_idx).unwrap() == OpType::Delete as u8;
//         // If the row has been deleted, then we don't need to merge fields.
//         if !self.contains_deletion {
//             self.contains_null = fields.iter().any(|col| col.data.is_null(last_idx));
//         }

//         if self.skip_merge() {
//             // No null field or the row has been deleted, no need to merge.
//             return;
//         }
//         if self.builders.is_empty() {
//             self.builders = fields
//                 .iter()
//                 .map(|col| col.data.data_type().create_mutable_vector(1))
//                 .collect();
//         }
//         self.last_fields = fields.iter().map(|col| col.data.get(last_idx)).collect();
//     }

//     /// Returns true if the builder don't need to merge the rows.
//     fn skip_merge(&self) -> bool {
//         debug_assert!(self.initialized);

//         // No null field or the row has been deleted, no need to merge.
//         self.contains_deletion || !self.contains_null
//     }

//     /// Pushes first row of a batch to the builder.
//     fn push_first_row(&mut self, batch: &Batch) {
//         debug_assert!(self.initialized);
//         debug_assert!(!batch.is_empty());

//         if self.skip_merge() {
//             // No remaining null field, skips this batch.
//             return;
//         }

//         // Both `maybe_init()` and `push_first_row()` can update the builder. If the delete
//         // op is not in the latest row, then we can't set the deletion flag in the `maybe_init()`.
//         // We must check the batch and update the deletion flag here to prevent
//         // the builder from merging non-null fields in rows that insert before the deleted row.
//         self.contains_deletion = batch.op_types().get_data(0).unwrap() == OpType::Delete as u8;
//         if self.contains_deletion {
//             // Deletes this row.
//             return;
//         }

//         let fields = batch.fields();
//         for (idx, value) in self.last_fields.iter_mut().enumerate() {
//             if value.is_null() && !fields[idx].data.is_null(0) {
//                 // Updates the value.
//                 *value = fields[idx].data.get(0);
//             }
//         }
//         // Updates the flag.
//         self.contains_null = self.last_fields.iter().any(Value::is_null);
//     }

//     /// Merges last non-null fields, builds a new batch and resets the builder.
//     /// It may overwrites the last row of the `buffer`. The `buffer` is the batch
//     /// that initialized the builder.
//     fn merge_last_non_null(
//         &mut self,
//         buffer: Batch,
//         metrics: &mut DedupMetrics,
//     ) -> Result<Option<Batch>> {
//         debug_assert!(self.initialized);

//         let mut output = if self.last_fields.is_empty() {
//             // No need to overwrite the last row.
//             buffer
//         } else {
//             // Builds last fields.
//             for (builder, value) in self.builders.iter_mut().zip(&self.last_fields) {
//                 // Safety: Vectors of the batch has the same type.
//                 builder.push_value_ref(value.as_value_ref());
//             }
//             let fields = self
//                 .builders
//                 .iter_mut()
//                 .zip(buffer.fields())
//                 .map(|(builder, col)| BatchColumn {
//                     column_id: col.column_id,
//                     data: builder.to_vector(),
//                 })
//                 .collect();

//             if buffer.num_rows() == 1 {
//                 // Replaces the buffer directly if it only has one row.
//                 buffer.with_fields(fields)?
//             } else {
//                 // Replaces the last row of the buffer.
//                 let front = buffer.slice(0, buffer.num_rows() - 1);
//                 let last = buffer.slice(buffer.num_rows() - 1, 1);
//                 let last = last.with_fields(fields)?;
//                 Batch::concat(vec![front, last])?
//             }
//         };

//         // Resets itself. `self.builders` is already reset in `to_vector()`.
//         self.clear();

//         if self.filter_deleted {
//             filter_deleted_from_batch(&mut output, metrics)?;
//         }

//         if output.is_empty() {
//             Ok(None)
//         } else {
//             Ok(Some(output))
//         }
//     }

//     /// Clears the builder.
//     fn clear(&mut self) {
//         self.last_fields.clear();
//         self.contains_null = false;
//         self.contains_deletion = false;
//         self.initialized = false;
//     }
// }

/// Dedup strategy that keeps the last non-null field for the same key.
pub(crate) struct PlainLastNonNull {
    /// Indices of the primary key columns.
    pk_indices: Vec<usize>,
    /// Index of the time index column.
    timestamp_index: usize,
    /// Filter deleted rows.
    filter_deleted: bool,
    /// Buffered batch to check whether the next batch have duplicated rows with this batch.
    /// Fields in the last row of this batch may be updated by the next batch.
    /// The buffered batch should contain no duplication.
    buffer: Option<BatchLastRow>,
}

// TODO(yingwen): metrics.
impl PlainDedupStrategy for PlainLastNonNull {
    fn push_batch(
        &mut self,
        batch: PlainBatch,
        metrics: &mut DedupMetrics,
    ) -> Result<Option<PlainBatch>> {
        if batch.is_empty() {
            return Ok(None);
        }

        let Some(buffer) = self.buffer.take() else {
            // If the buffer is None, dedup the batch, put the batch into the buffer and return.
            let record_batch = Self::dedup_one_batch(
                batch.into_record_batch(),
                &self.pk_indices,
                self.timestamp_index,
            )?;
            self.buffer = BatchLastRow::try_new(record_batch);

            return Ok(None);
        };

        if !buffer.is_last_row_duplicated(
            &self.pk_indices,
            self.timestamp_index,
            batch.as_record_batch(),
        )? {
            // The first row of batch has different key from the buffer.
            // We can replace the buffer with the new batch.
            // Dedup the batch.
            let record_batch = Self::dedup_one_batch(
                batch.into_record_batch(),
                &self.pk_indices,
                self.timestamp_index,
            )?;
            debug_assert!(record_batch.num_rows() > 0);
            self.buffer = BatchLastRow::try_new(record_batch);

            return Ok(Some(PlainBatch::new(buffer.last_batch)));
        }

        // The next batch has duplicated rows.
        // We can return rows except the last row in the buffer.
        let output = if buffer.last_batch.num_rows() > 1 {
            let dedup_batch = buffer.last_batch.slice(0, buffer.last_batch.num_rows() - 1);
            debug_assert_eq!(buffer.last_batch.num_rows() - 1, dedup_batch.num_rows());
            Some(PlainBatch::new(dedup_batch))
        } else {
            None
        };
        let last_row = buffer.last_batch.slice(buffer.last_batch.num_rows() - 1, 1);

        // We concat the last row with the next batch.
        let next_batch = batch.into_record_batch();
        let schema = next_batch.schema();
        let merged = concat_batches(&schema, &[last_row, next_batch]).context(ComputeArrowSnafu)?;
        // Dedup the merged batch and update the buffer.
        let record_batch = Self::dedup_one_batch(merged, &self.pk_indices, self.timestamp_index)?;
        debug_assert!(record_batch.num_rows() > 0);
        self.buffer = BatchLastRow::try_new(record_batch);

        Ok(output)
    }

    fn finish(&mut self, metrics: &mut DedupMetrics) -> Result<Option<PlainBatch>> {
        let Some(buffer) = self.buffer.take() else {
            return Ok(None);
        };

        Ok(Some(PlainBatch::new(buffer.last_batch)))
    }
}

impl PlainLastNonNull {
    /// Creates a new strategy with the given `filter_deleted` flag.
    pub(crate) fn new(
        pk_indices: Vec<usize>,
        timestamp_index: usize,
        filter_deleted: bool,
    ) -> Self {
        Self {
            pk_indices,
            timestamp_index,
            filter_deleted,
            buffer: None,
        }
    }

    /// Remove duplications from the batch without considering the previous and next rows.
    // FIXME(yingwen): Avoid repeating code.
    fn dedup_one_batch(
        batch: RecordBatch,
        pk_indices: &[usize],
        timestamp_index: usize,
    ) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        if num_rows < 2 {
            return Ok(batch);
        }

        let timestamps = batch.column(timestamp_index);
        // Checks duplications based on the timestamp.
        let mask = find_boundaries(timestamps).context(ComputeArrowSnafu)?;
        if mask.count_set_bits() == num_rows - 1 {
            // Fast path: No duplication.
            return Ok(batch);
        }

        // The batch has duplicated timestamps, but it doesn't mean it must
        // has duplicated rows.
        // Partitions the batch by the primary key and time index.
        let columns: Vec<_> = pk_indices
            .iter()
            .chain([&timestamp_index])
            .map(|index| batch.column(*index).clone())
            .collect();
        let partitions = partition(&columns).context(ComputeArrowSnafu)?;

        Self::dedup_by_partitions(batch, &partitions, pk_indices, timestamp_index)
    }

    /// Remove depulications for each partition.
    fn dedup_by_partitions(
        batch: RecordBatch,
        partitions: &Partitions,
        pk_indices: &[usize],
        timestamp_index: usize,
    ) -> Result<RecordBatch> {
        let ranges = partitions.ranges();
        // Each range at least has 1 row.
        let num_duplications: usize = ranges.iter().map(|r| r.end - r.start - 1).sum();
        if num_duplications == 0 {
            // Fast path, no duplication.
            return Ok(batch);
        }

        let mut is_field = vec![true; batch.num_columns()];
        is_field[timestamp_index] = false;
        for idx in pk_indices {
            is_field[*idx] = false;
        }

        let take_options = Some(TakeOptions {
            check_bounds: false,
        });
        // Always takes the first value for non-field columns in each range.
        let non_field_indices: UInt64Array = ranges.iter().map(|r| Some(r.start as u64)).collect();
        let new_columns = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(col_idx, column)| {
                if is_field[col_idx] {
                    let field_indices = Self::compute_field_indices(&ranges, column);
                    take(column, &field_indices, take_options.clone()).context(ComputeArrowSnafu)
                } else {
                    take(column, &non_field_indices, take_options.clone())
                        .context(ComputeArrowSnafu)
                }
            })
            .collect::<Result<Vec<ArrayRef>>>()?;

        RecordBatch::try_new(batch.schema(), new_columns).context(NewRecordBatchSnafu)
    }

    /// Returns an array of indices of the latest non null value for
    /// each input range.
    /// If all values in a range are null, the returned index is unspecific.
    fn compute_field_indices(ranges: &[Range<usize>], field_array: &ArrayRef) -> UInt64Array {
        ranges
            .iter()
            .map(|r| {
                let value_index = r
                    .clone()
                    .filter(|&i| field_array.is_valid(i))
                    .next()
                    .map(|i| i as u64)
                    // if all field values are none, pick one arbitrarily
                    .unwrap_or(r.start as u64);
                Some(value_index)
            })
            .collect()
    }
}

// /// Dedup strategy that keeps the last non-null field for the same key.
// ///
// /// It assumes that batches from files and memtables don't contain duplicate rows
// /// and the merge reader never concatenates batches from different source.
// ///
// /// We might implement a new strategy if we need to process files with duplicate rows.
// pub(crate) struct LastNonNull {
//     /// Buffered batch that fields in the last row may be updated.
//     buffer: Option<Batch>,
//     /// Fields that overlaps with the last row of the `buffer`.
//     last_fields: LastFieldsBuilder,
// }

// impl LastNonNull {
//     /// Creates a new strategy with the given `filter_deleted` flag.
//     pub(crate) fn new(filter_deleted: bool) -> Self {
//         Self {
//             buffer: None,
//             last_fields: LastFieldsBuilder::new(filter_deleted),
//         }
//     }
// }

// impl PlainDedupStrategy for LastNonNull {
//     fn push_batch(&mut self, batch: Batch, metrics: &mut DedupMetrics) -> Result<Option<Batch>> {
//         if batch.is_empty() {
//             return Ok(None);
//         }

//         let Some(buffer) = self.buffer.as_mut() else {
//             // The buffer is empty, store the batch and return. We need to observe the next batch.
//             self.buffer = Some(batch);
//             return Ok(None);
//         };

//         // Initializes last fields with the first buffer.
//         self.last_fields.maybe_init(buffer);

//         if buffer.primary_key() != batch.primary_key() {
//             // Next key is different.
//             let buffer = std::mem::replace(buffer, batch);
//             let merged = self.last_fields.merge_last_non_null(buffer, metrics)?;
//             return Ok(merged);
//         }

//         if buffer.last_timestamp() != batch.first_timestamp() {
//             // The next batch has a different timestamp.
//             let buffer = std::mem::replace(buffer, batch);
//             let merged = self.last_fields.merge_last_non_null(buffer, metrics)?;
//             return Ok(merged);
//         }

//         // The next batch has the same key and timestamp.

//         metrics.num_unselected_rows += 1;
//         // We assumes each batch doesn't contain duplicate rows so we only need to check the first row.
//         if batch.num_rows() == 1 {
//             self.last_fields.push_first_row(&batch);
//             return Ok(None);
//         }

//         // The next batch has the same key and timestamp but contains multiple rows.
//         // We can merge the first row and buffer the remaining rows.
//         let first = batch.slice(0, 1);
//         self.last_fields.push_first_row(&first);
//         // Moves the remaining rows to the buffer.
//         let batch = batch.slice(1, batch.num_rows() - 1);
//         let buffer = std::mem::replace(buffer, batch);
//         let merged = self.last_fields.merge_last_non_null(buffer, metrics)?;

//         Ok(merged)
//     }

//     fn finish(&mut self, metrics: &mut DedupMetrics) -> Result<Option<Batch>> {
//         let Some(buffer) = self.buffer.take() else {
//             return Ok(None);
//         };

//         // Initializes last fields with the first buffer.
//         self.last_fields.maybe_init(&buffer);

//         let merged = self.last_fields.merge_last_non_null(buffer, metrics)?;

//         Ok(merged)
//     }
// }

// /// An iterator that dedup rows by [LastNonNull] strategy.
// /// The input iterator must returns sorted batches.
// pub(crate) struct LastNonNullIter<I> {
//     /// Inner iterator that returns sorted batches.
//     iter: Option<I>,
//     /// Dedup strategy.
//     strategy: LastNonNull,
//     /// Dedup metrics.
//     metrics: DedupMetrics,
//     /// The current batch returned by the iterator. If it is None, we need to
//     /// fetch a new batch.
//     /// The batch is always not empty.
//     current_batch: Option<Batch>,
//     /// The index of the current row in the current batch.
//     /// more to check issue #5229.
//     current_index: usize,
// }

// impl<I> LastNonNullIter<I> {
//     /// Creates a new iterator with the given inner iterator.
//     pub(crate) fn new(iter: I) -> Self {
//         Self {
//             iter: Some(iter),
//             // We only use the iter in memtables. Memtables never filter deleted.
//             strategy: LastNonNull::new(false),
//             metrics: DedupMetrics::default(),
//             current_batch: None,
//             current_index: 0,
//         }
//     }
// }

// impl<I: Iterator<Item = Result<Batch>>> LastNonNullIter<I> {
//     /// Fetches the next batch from the inner iterator. It will slice the batch if it
//     /// contains duplicate rows.
//     fn next_batch_for_merge(&mut self) -> Result<Option<Batch>> {
//         if self.current_batch.is_none() {
//             // No current batch. Fetches a new batch from the inner iterator.
//             let Some(iter) = self.iter.as_mut() else {
//                 // The iterator is exhausted.
//                 return Ok(None);
//             };

//             self.current_batch = iter.next().transpose()?;
//             self.current_index = 0;
//             if self.current_batch.is_none() {
//                 // The iterator is exhausted.
//                 self.iter = None;
//                 return Ok(None);
//             }
//         }

//         if let Some(batch) = &self.current_batch {
//             let n = batch.num_rows();
//             // Safety: The batch is not empty when accessed.
//             let timestamps = batch.timestamps_native().unwrap();
//             let mut pos = self.current_index;
//             while pos + 1 < n && timestamps[pos] != timestamps[pos + 1] {
//                 pos += 1;
//             }
//             let segment = batch.slice(self.current_index, pos - self.current_index + 1);
//             if pos + 1 < n && timestamps[pos] == timestamps[pos + 1] {
//                 self.current_index = pos + 1;
//             } else {
//                 self.current_batch = None;
//                 self.current_index = 0;
//             }
//             return Ok(Some(segment));
//         }

//         Ok(None)
//     }

//     fn next_batch(&mut self) -> Result<Option<Batch>> {
//         while let Some(batch) = self.next_batch_for_merge()? {
//             if let Some(batch) = self.strategy.push_batch(batch, &mut self.metrics)? {
//                 return Ok(Some(batch));
//             }
//         }

//         self.strategy.finish(&mut self.metrics)
//     }
// }

// impl<I: Iterator<Item = Result<Batch>>> Iterator for LastNonNullIter<I> {
//     type Item = Result<Batch>;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.next_batch().transpose()
//     }
// }

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;

//     use api::v1::OpType;
//     use datatypes::arrow::array::{TimestampMillisecondArray, UInt64Array, UInt8Array};

//     use super::*;
//     use crate::read::BatchBuilder;
//     use crate::test_util::{check_reader_result, new_batch, VecBatchReader};

//     #[tokio::test]
//     async fn test_dedup_reader_no_duplications() {
//         let input = [
//             new_batch(
//                 b"k1",
//                 &[1, 2],
//                 &[11, 12],
//                 &[OpType::Put, OpType::Put],
//                 &[21, 22],
//             ),
//             new_batch(b"k1", &[3], &[13], &[OpType::Put], &[23]),
//             new_batch(
//                 b"k2",
//                 &[1, 2],
//                 &[111, 112],
//                 &[OpType::Put, OpType::Put],
//                 &[31, 32],
//             ),
//         ];

//         // Test last row.
//         let reader = VecBatchReader::new(&input);
//         let mut reader = PlainDedupReader::new(reader, LastRow::new(true));
//         check_reader_result(&mut reader, &input).await;
//         assert_eq!(0, reader.metrics().num_unselected_rows);
//         assert_eq!(0, reader.metrics().num_deleted_rows);

//         // Test last non-null.
//         let reader = VecBatchReader::new(&input);
//         let mut reader = PlainDedupReader::new(reader, LastNonNull::new(true));
//         check_reader_result(&mut reader, &input).await;
//         assert_eq!(0, reader.metrics().num_unselected_rows);
//         assert_eq!(0, reader.metrics().num_deleted_rows);
//     }

//     #[tokio::test]
//     async fn test_dedup_reader_duplications() {
//         let input = [
//             new_batch(
//                 b"k1",
//                 &[1, 2],
//                 &[13, 11],
//                 &[OpType::Put, OpType::Put],
//                 &[11, 12],
//             ),
//             // empty batch.
//             new_batch(b"k1", &[], &[], &[], &[]),
//             // Duplicate with the previous batch.
//             new_batch(
//                 b"k1",
//                 &[2, 3, 4],
//                 &[10, 13, 13],
//                 &[OpType::Put, OpType::Put, OpType::Delete],
//                 &[2, 13, 14],
//             ),
//             new_batch(
//                 b"k2",
//                 &[1, 2],
//                 &[20, 20],
//                 &[OpType::Put, OpType::Delete],
//                 &[101, 0],
//             ),
//             new_batch(b"k2", &[2], &[19], &[OpType::Put], &[102]),
//             new_batch(b"k3", &[2], &[20], &[OpType::Put], &[202]),
//             // This batch won't increase the deleted rows count as it
//             // is filtered out by the previous batch.
//             new_batch(b"k3", &[2], &[19], &[OpType::Delete], &[0]),
//         ];
//         // Filter deleted.
//         let reader = VecBatchReader::new(&input);
//         let mut reader = PlainDedupReader::new(reader, LastRow::new(true));
//         check_reader_result(
//             &mut reader,
//             &[
//                 new_batch(
//                     b"k1",
//                     &[1, 2],
//                     &[13, 11],
//                     &[OpType::Put, OpType::Put],
//                     &[11, 12],
//                 ),
//                 new_batch(b"k1", &[3], &[13], &[OpType::Put], &[13]),
//                 new_batch(b"k2", &[1], &[20], &[OpType::Put], &[101]),
//                 new_batch(b"k3", &[2], &[20], &[OpType::Put], &[202]),
//             ],
//         )
//         .await;
//         assert_eq!(5, reader.metrics().num_unselected_rows);
//         assert_eq!(2, reader.metrics().num_deleted_rows);

//         // Does not filter deleted.
//         let reader = VecBatchReader::new(&input);
//         let mut reader = PlainDedupReader::new(reader, LastRow::new(false));
//         check_reader_result(
//             &mut reader,
//             &[
//                 new_batch(
//                     b"k1",
//                     &[1, 2],
//                     &[13, 11],
//                     &[OpType::Put, OpType::Put],
//                     &[11, 12],
//                 ),
//                 new_batch(
//                     b"k1",
//                     &[3, 4],
//                     &[13, 13],
//                     &[OpType::Put, OpType::Delete],
//                     &[13, 14],
//                 ),
//                 new_batch(
//                     b"k2",
//                     &[1, 2],
//                     &[20, 20],
//                     &[OpType::Put, OpType::Delete],
//                     &[101, 0],
//                 ),
//                 new_batch(b"k3", &[2], &[20], &[OpType::Put], &[202]),
//             ],
//         )
//         .await;
//         assert_eq!(3, reader.metrics().num_unselected_rows);
//         assert_eq!(0, reader.metrics().num_deleted_rows);
//     }

//     /// Returns a new [Batch] whose field has column id 1, 2.
//     fn new_batch_multi_fields(
//         primary_key: &[u8],
//         timestamps: &[i64],
//         sequences: &[u64],
//         op_types: &[OpType],
//         fields: &[(Option<u64>, Option<u64>)],
//     ) -> Batch {
//         let mut builder = BatchBuilder::new(primary_key.to_vec());
//         builder
//             .timestamps_array(Arc::new(TimestampMillisecondArray::from_iter_values(
//                 timestamps.iter().copied(),
//             )))
//             .unwrap()
//             .sequences_array(Arc::new(UInt64Array::from_iter_values(
//                 sequences.iter().copied(),
//             )))
//             .unwrap()
//             .op_types_array(Arc::new(UInt8Array::from_iter_values(
//                 op_types.iter().map(|v| *v as u8),
//             )))
//             .unwrap()
//             .push_field_array(
//                 1,
//                 Arc::new(UInt64Array::from_iter(fields.iter().map(|field| field.0))),
//             )
//             .unwrap()
//             .push_field_array(
//                 2,
//                 Arc::new(UInt64Array::from_iter(fields.iter().map(|field| field.1))),
//             )
//             .unwrap();
//         builder.build().unwrap()
//     }

//     #[tokio::test]
//     async fn test_last_non_null_merge() {
//         let input = [
//             new_batch_multi_fields(
//                 b"k1",
//                 &[1, 2],
//                 &[13, 11],
//                 &[OpType::Put, OpType::Put],
//                 &[(Some(11), Some(11)), (None, None)],
//             ),
//             // empty batch.
//             new_batch_multi_fields(b"k1", &[], &[], &[], &[]),
//             // Duplicate with the previous batch.
//             new_batch_multi_fields(b"k1", &[2], &[10], &[OpType::Put], &[(Some(12), None)]),
//             new_batch_multi_fields(
//                 b"k1",
//                 &[2, 3, 4],
//                 &[10, 13, 13],
//                 &[OpType::Put, OpType::Put, OpType::Delete],
//                 &[(Some(2), Some(22)), (Some(13), None), (None, Some(14))],
//             ),
//             new_batch_multi_fields(
//                 b"k2",
//                 &[1, 2],
//                 &[20, 20],
//                 &[OpType::Put, OpType::Delete],
//                 &[(Some(101), Some(101)), (None, None)],
//             ),
//             new_batch_multi_fields(
//                 b"k2",
//                 &[2],
//                 &[19],
//                 &[OpType::Put],
//                 &[(Some(102), Some(102))],
//             ),
//             new_batch_multi_fields(
//                 b"k3",
//                 &[2],
//                 &[20],
//                 &[OpType::Put],
//                 &[(Some(202), Some(202))],
//             ),
//             // This batch won't increase the deleted rows count as it
//             // is filtered out by the previous batch. (All fields are null).
//             new_batch_multi_fields(b"k3", &[2], &[19], &[OpType::Delete], &[(None, None)]),
//         ];

//         // Filter deleted.
//         let reader = VecBatchReader::new(&input);
//         let mut reader = PlainDedupReader::new(reader, LastNonNull::new(true));
//         check_reader_result(
//             &mut reader,
//             &[
//                 new_batch_multi_fields(
//                     b"k1",
//                     &[1, 2],
//                     &[13, 11],
//                     &[OpType::Put, OpType::Put],
//                     &[(Some(11), Some(11)), (Some(12), Some(22))],
//                 ),
//                 new_batch_multi_fields(b"k1", &[3], &[13], &[OpType::Put], &[(Some(13), None)]),
//                 new_batch_multi_fields(
//                     b"k2",
//                     &[1],
//                     &[20],
//                     &[OpType::Put],
//                     &[(Some(101), Some(101))],
//                 ),
//                 new_batch_multi_fields(
//                     b"k3",
//                     &[2],
//                     &[20],
//                     &[OpType::Put],
//                     &[(Some(202), Some(202))],
//                 ),
//             ],
//         )
//         .await;
//         assert_eq!(6, reader.metrics().num_unselected_rows);
//         assert_eq!(2, reader.metrics().num_deleted_rows);

//         // Does not filter deleted.
//         let reader = VecBatchReader::new(&input);
//         let mut reader = PlainDedupReader::new(reader, LastNonNull::new(false));
//         check_reader_result(
//             &mut reader,
//             &[
//                 new_batch_multi_fields(
//                     b"k1",
//                     &[1, 2],
//                     &[13, 11],
//                     &[OpType::Put, OpType::Put],
//                     &[(Some(11), Some(11)), (Some(12), Some(22))],
//                 ),
//                 new_batch_multi_fields(
//                     b"k1",
//                     &[3, 4],
//                     &[13, 13],
//                     &[OpType::Put, OpType::Delete],
//                     &[(Some(13), None), (None, Some(14))],
//                 ),
//                 new_batch_multi_fields(
//                     b"k2",
//                     &[1, 2],
//                     &[20, 20],
//                     &[OpType::Put, OpType::Delete],
//                     &[(Some(101), Some(101)), (None, None)],
//                 ),
//                 new_batch_multi_fields(
//                     b"k3",
//                     &[2],
//                     &[20],
//                     &[OpType::Put],
//                     &[(Some(202), Some(202))],
//                 ),
//             ],
//         )
//         .await;
//         assert_eq!(4, reader.metrics().num_unselected_rows);
//         assert_eq!(0, reader.metrics().num_deleted_rows);
//     }

//     #[tokio::test]
//     async fn test_last_non_null_skip_merge_single() {
//         let input = [new_batch_multi_fields(
//             b"k1",
//             &[1, 2, 3],
//             &[13, 11, 13],
//             &[OpType::Put, OpType::Delete, OpType::Put],
//             &[(Some(11), Some(11)), (None, None), (Some(13), Some(13))],
//         )];

//         let reader = VecBatchReader::new(&input);
//         let mut reader = PlainDedupReader::new(reader, LastNonNull::new(true));
//         check_reader_result(
//             &mut reader,
//             &[new_batch_multi_fields(
//                 b"k1",
//                 &[1, 3],
//                 &[13, 13],
//                 &[OpType::Put, OpType::Put],
//                 &[(Some(11), Some(11)), (Some(13), Some(13))],
//             )],
//         )
//         .await;
//         assert_eq!(1, reader.metrics().num_unselected_rows);
//         assert_eq!(1, reader.metrics().num_deleted_rows);

//         let reader = VecBatchReader::new(&input);
//         let mut reader = PlainDedupReader::new(reader, LastNonNull::new(false));
//         check_reader_result(&mut reader, &input).await;
//         assert_eq!(0, reader.metrics().num_unselected_rows);
//         assert_eq!(0, reader.metrics().num_deleted_rows);
//     }

//     #[tokio::test]
//     async fn test_last_non_null_skip_merge_no_null() {
//         let input = [
//             new_batch_multi_fields(
//                 b"k1",
//                 &[1, 2],
//                 &[13, 11],
//                 &[OpType::Put, OpType::Put],
//                 &[(Some(11), Some(11)), (Some(12), Some(12))],
//             ),
//             new_batch_multi_fields(b"k1", &[2], &[10], &[OpType::Put], &[(None, Some(22))]),
//             new_batch_multi_fields(
//                 b"k1",
//                 &[2, 3],
//                 &[9, 13],
//                 &[OpType::Put, OpType::Put],
//                 &[(Some(32), None), (Some(13), Some(13))],
//             ),
//         ];

//         let reader = VecBatchReader::new(&input);
//         let mut reader = PlainDedupReader::new(reader, LastNonNull::new(true));
//         check_reader_result(
//             &mut reader,
//             &[
//                 new_batch_multi_fields(
//                     b"k1",
//                     &[1, 2],
//                     &[13, 11],
//                     &[OpType::Put, OpType::Put],
//                     &[(Some(11), Some(11)), (Some(12), Some(12))],
//                 ),
//                 new_batch_multi_fields(b"k1", &[3], &[13], &[OpType::Put], &[(Some(13), Some(13))]),
//             ],
//         )
//         .await;
//         assert_eq!(2, reader.metrics().num_unselected_rows);
//         assert_eq!(0, reader.metrics().num_deleted_rows);
//     }

//     #[tokio::test]
//     async fn test_last_non_null_merge_null() {
//         let input = [
//             new_batch_multi_fields(
//                 b"k1",
//                 &[1, 2],
//                 &[13, 11],
//                 &[OpType::Put, OpType::Put],
//                 &[(Some(11), Some(11)), (None, None)],
//             ),
//             new_batch_multi_fields(b"k1", &[2], &[10], &[OpType::Put], &[(None, Some(22))]),
//             new_batch_multi_fields(b"k1", &[3], &[13], &[OpType::Put], &[(Some(33), None)]),
//         ];

//         let reader = VecBatchReader::new(&input);
//         let mut reader = PlainDedupReader::new(reader, LastNonNull::new(true));
//         check_reader_result(
//             &mut reader,
//             &[
//                 new_batch_multi_fields(
//                     b"k1",
//                     &[1, 2],
//                     &[13, 11],
//                     &[OpType::Put, OpType::Put],
//                     &[(Some(11), Some(11)), (None, Some(22))],
//                 ),
//                 new_batch_multi_fields(b"k1", &[3], &[13], &[OpType::Put], &[(Some(33), None)]),
//             ],
//         )
//         .await;
//         assert_eq!(1, reader.metrics().num_unselected_rows);
//         assert_eq!(0, reader.metrics().num_deleted_rows);
//     }

//     fn check_dedup_strategy(
//         input: &[Batch],
//         strategy: &mut dyn PlainDedupStrategy,
//         expect: &[Batch],
//     ) {
//         let mut actual = Vec::new();
//         let mut metrics = DedupMetrics::default();
//         for batch in input {
//             if let Some(out) = strategy.push_batch(batch.clone(), &mut metrics).unwrap() {
//                 actual.push(out);
//             }
//         }
//         if let Some(out) = strategy.finish(&mut metrics).unwrap() {
//             actual.push(out);
//         }

//         assert_eq!(expect, actual);
//     }

//     #[test]
//     fn test_last_non_null_strategy_delete_last() {
//         let input = [
//             new_batch_multi_fields(b"k1", &[1], &[6], &[OpType::Put], &[(Some(11), None)]),
//             new_batch_multi_fields(
//                 b"k1",
//                 &[1, 2],
//                 &[1, 7],
//                 &[OpType::Put, OpType::Put],
//                 &[(Some(1), None), (Some(22), Some(222))],
//             ),
//             new_batch_multi_fields(b"k1", &[2], &[4], &[OpType::Put], &[(Some(12), None)]),
//             new_batch_multi_fields(
//                 b"k2",
//                 &[2, 3],
//                 &[2, 5],
//                 &[OpType::Put, OpType::Delete],
//                 &[(None, None), (Some(13), None)],
//             ),
//             new_batch_multi_fields(b"k2", &[3], &[3], &[OpType::Put], &[(None, Some(3))]),
//         ];

//         let mut strategy = LastNonNull::new(true);
//         check_dedup_strategy(
//             &input,
//             &mut strategy,
//             &[
//                 new_batch_multi_fields(b"k1", &[1], &[6], &[OpType::Put], &[(Some(11), None)]),
//                 new_batch_multi_fields(b"k1", &[2], &[7], &[OpType::Put], &[(Some(22), Some(222))]),
//                 new_batch_multi_fields(b"k2", &[2], &[2], &[OpType::Put], &[(None, None)]),
//             ],
//         );
//     }

//     #[test]
//     fn test_last_non_null_strategy_delete_one() {
//         let input = [
//             new_batch_multi_fields(b"k1", &[1], &[1], &[OpType::Delete], &[(None, None)]),
//             new_batch_multi_fields(b"k2", &[1], &[6], &[OpType::Put], &[(Some(11), None)]),
//         ];

//         let mut strategy = LastNonNull::new(true);
//         check_dedup_strategy(
//             &input,
//             &mut strategy,
//             &[new_batch_multi_fields(
//                 b"k2",
//                 &[1],
//                 &[6],
//                 &[OpType::Put],
//                 &[(Some(11), None)],
//             )],
//         );
//     }

//     #[test]
//     fn test_last_non_null_strategy_delete_all() {
//         let input = [
//             new_batch_multi_fields(b"k1", &[1], &[1], &[OpType::Delete], &[(None, None)]),
//             new_batch_multi_fields(b"k2", &[1], &[6], &[OpType::Delete], &[(Some(11), None)]),
//         ];

//         let mut strategy = LastNonNull::new(true);
//         check_dedup_strategy(&input, &mut strategy, &[]);
//     }

//     #[test]
//     fn test_last_non_null_strategy_same_batch() {
//         let input = [
//             new_batch_multi_fields(b"k1", &[1], &[6], &[OpType::Put], &[(Some(11), None)]),
//             new_batch_multi_fields(
//                 b"k1",
//                 &[1, 2],
//                 &[1, 7],
//                 &[OpType::Put, OpType::Put],
//                 &[(Some(1), None), (Some(22), Some(222))],
//             ),
//             new_batch_multi_fields(b"k1", &[2], &[4], &[OpType::Put], &[(Some(12), None)]),
//             new_batch_multi_fields(
//                 b"k1",
//                 &[2, 3],
//                 &[2, 5],
//                 &[OpType::Put, OpType::Put],
//                 &[(None, None), (Some(13), None)],
//             ),
//             new_batch_multi_fields(b"k1", &[3], &[3], &[OpType::Put], &[(None, Some(3))]),
//         ];

//         let mut strategy = LastNonNull::new(true);
//         check_dedup_strategy(
//             &input,
//             &mut strategy,
//             &[
//                 new_batch_multi_fields(b"k1", &[1], &[6], &[OpType::Put], &[(Some(11), None)]),
//                 new_batch_multi_fields(b"k1", &[2], &[7], &[OpType::Put], &[(Some(22), Some(222))]),
//                 new_batch_multi_fields(b"k1", &[3], &[5], &[OpType::Put], &[(Some(13), Some(3))]),
//             ],
//         );
//     }

//     #[test]
//     fn test_last_non_null_strategy_delete_middle() {
//         let input = [
//             new_batch_multi_fields(b"k1", &[1], &[7], &[OpType::Put], &[(Some(11), None)]),
//             new_batch_multi_fields(b"k1", &[1], &[4], &[OpType::Delete], &[(None, None)]),
//             new_batch_multi_fields(b"k1", &[1], &[1], &[OpType::Put], &[(Some(12), Some(1))]),
//             new_batch_multi_fields(b"k1", &[2], &[8], &[OpType::Put], &[(Some(21), None)]),
//             new_batch_multi_fields(b"k1", &[2], &[5], &[OpType::Delete], &[(None, None)]),
//             new_batch_multi_fields(b"k1", &[2], &[2], &[OpType::Put], &[(Some(22), Some(2))]),
//             new_batch_multi_fields(b"k1", &[3], &[9], &[OpType::Put], &[(Some(31), None)]),
//             new_batch_multi_fields(b"k1", &[3], &[6], &[OpType::Delete], &[(None, None)]),
//             new_batch_multi_fields(b"k1", &[3], &[3], &[OpType::Put], &[(Some(32), Some(3))]),
//         ];

//         let mut strategy = LastNonNull::new(true);
//         check_dedup_strategy(
//             &input,
//             &mut strategy,
//             &[
//                 new_batch_multi_fields(b"k1", &[1], &[7], &[OpType::Put], &[(Some(11), None)]),
//                 new_batch_multi_fields(b"k1", &[2], &[8], &[OpType::Put], &[(Some(21), None)]),
//                 new_batch_multi_fields(b"k1", &[3], &[9], &[OpType::Put], &[(Some(31), None)]),
//             ],
//         );
//     }

//     #[test]
//     fn test_last_non_null_iter_on_batch() {
//         let input = [new_batch_multi_fields(
//             b"k1",
//             &[1, 1, 2],
//             &[13, 12, 13],
//             &[OpType::Put, OpType::Put, OpType::Put],
//             &[(None, None), (Some(1), None), (Some(2), Some(22))],
//         )];
//         let iter = input.into_iter().map(Ok);
//         let iter = LastNonNullIter::new(iter);
//         let actual: Vec<_> = iter.map(|batch| batch.unwrap()).collect();
//         let expect = [
//             new_batch_multi_fields(b"k1", &[1], &[13], &[OpType::Put], &[(Some(1), None)]),
//             new_batch_multi_fields(b"k1", &[2], &[13], &[OpType::Put], &[(Some(2), Some(22))]),
//         ];
//         assert_eq!(&expect, &actual[..]);
//     }

//     #[test]
//     fn test_last_non_null_iter_same_row() {
//         let input = [
//             new_batch_multi_fields(
//                 b"k1",
//                 &[1, 1, 1],
//                 &[13, 12, 11],
//                 &[OpType::Put, OpType::Put, OpType::Put],
//                 &[(None, None), (Some(1), None), (Some(11), None)],
//             ),
//             new_batch_multi_fields(
//                 b"k1",
//                 &[1, 1],
//                 &[10, 9],
//                 &[OpType::Put, OpType::Put],
//                 &[(None, Some(11)), (Some(21), Some(31))],
//             ),
//         ];
//         let iter = input.into_iter().map(Ok);
//         let iter = LastNonNullIter::new(iter);
//         let actual: Vec<_> = iter.map(|batch| batch.unwrap()).collect();
//         let expect = [new_batch_multi_fields(
//             b"k1",
//             &[1],
//             &[13],
//             &[OpType::Put],
//             &[(Some(1), Some(11))],
//         )];
//         assert_eq!(&expect, &actual[..]);
//     }

//     #[test]
//     fn test_last_non_null_iter_multi_batch() {
//         let input = [
//             new_batch_multi_fields(
//                 b"k1",
//                 &[1, 1, 2],
//                 &[13, 12, 13],
//                 &[OpType::Put, OpType::Put, OpType::Put],
//                 &[(None, None), (Some(1), None), (Some(2), Some(22))],
//             ),
//             new_batch_multi_fields(
//                 b"k1",
//                 &[2, 3],
//                 &[12, 13],
//                 &[OpType::Put, OpType::Delete],
//                 &[(None, Some(12)), (None, None)],
//             ),
//             new_batch_multi_fields(
//                 b"k2",
//                 &[1, 1, 2],
//                 &[13, 12, 13],
//                 &[OpType::Put, OpType::Put, OpType::Put],
//                 &[(None, None), (Some(1), None), (Some(2), Some(22))],
//             ),
//         ];
//         let iter = input.into_iter().map(Ok);
//         let iter = LastNonNullIter::new(iter);
//         let actual: Vec<_> = iter.map(|batch| batch.unwrap()).collect();
//         let expect = [
//             new_batch_multi_fields(b"k1", &[1], &[13], &[OpType::Put], &[(Some(1), None)]),
//             new_batch_multi_fields(b"k1", &[2], &[13], &[OpType::Put], &[(Some(2), Some(22))]),
//             new_batch_multi_fields(b"k1", &[3], &[13], &[OpType::Delete], &[(None, None)]),
//             new_batch_multi_fields(b"k2", &[1], &[13], &[OpType::Put], &[(Some(1), None)]),
//             new_batch_multi_fields(b"k2", &[2], &[13], &[OpType::Put], &[(Some(2), Some(22))]),
//         ];
//         assert_eq!(&expect, &actual[..]);
//     }

//     /// Returns a new [Batch] without fields.
//     fn new_batch_no_fields(
//         primary_key: &[u8],
//         timestamps: &[i64],
//         sequences: &[u64],
//         op_types: &[OpType],
//     ) -> Batch {
//         let mut builder = BatchBuilder::new(primary_key.to_vec());
//         builder
//             .timestamps_array(Arc::new(TimestampMillisecondArray::from_iter_values(
//                 timestamps.iter().copied(),
//             )))
//             .unwrap()
//             .sequences_array(Arc::new(UInt64Array::from_iter_values(
//                 sequences.iter().copied(),
//             )))
//             .unwrap()
//             .op_types_array(Arc::new(UInt8Array::from_iter_values(
//                 op_types.iter().map(|v| *v as u8),
//             )))
//             .unwrap();
//         builder.build().unwrap()
//     }

//     #[test]
//     fn test_last_non_null_iter_no_batch() {
//         let input = [
//             new_batch_no_fields(
//                 b"k1",
//                 &[1, 1, 2],
//                 &[13, 12, 13],
//                 &[OpType::Put, OpType::Put, OpType::Put],
//             ),
//             new_batch_no_fields(b"k1", &[2, 3], &[12, 13], &[OpType::Put, OpType::Delete]),
//             new_batch_no_fields(
//                 b"k2",
//                 &[1, 1, 2],
//                 &[13, 12, 13],
//                 &[OpType::Put, OpType::Put, OpType::Put],
//             ),
//         ];
//         let iter = input.into_iter().map(Ok);
//         let iter = LastNonNullIter::new(iter);
//         let actual: Vec<_> = iter.map(|batch| batch.unwrap()).collect();
//         let expect = [
//             new_batch_no_fields(b"k1", &[1], &[13], &[OpType::Put]),
//             new_batch_no_fields(b"k1", &[2], &[13], &[OpType::Put]),
//             new_batch_no_fields(b"k1", &[3], &[13], &[OpType::Delete]),
//             new_batch_no_fields(b"k2", &[1], &[13], &[OpType::Put]),
//             new_batch_no_fields(b"k2", &[2], &[13], &[OpType::Put]),
//         ];
//         assert_eq!(&expect, &actual[..]);
//     }
// }
