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

//! Utilities for record batches with primary key encoding.

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::OpType;
use datafusion::execution::memory_pool::{MemoryConsumer, UnboundedMemoryPool};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::DataFusionError;
use datatypes::arrow::array::{ArrayRef, BinaryBuilder, DictionaryArray, UInt32Array};
use datatypes::arrow::compute::SortOptions;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::prelude::VectorRef;
use datatypes::value::ValueRef;
use datatypes::vectors::Helper;
use futures::TryStreamExt;
use itertools::Itertools;
use mito_codec::key_values::{KeyValue, KeyValues};
use mito_codec::row_converter::PrimaryKeyCodec;
use snafu::{IntoError, OptionExt, ResultExt};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::consts::{
    OP_TYPE_COLUMN_NAME, PRIMARY_KEY_COLUMN_NAME, SEQUENCE_COLUMN_NAME,
};
use store_api::storage::{ColumnId, SequenceNumber};

use crate::error::{
    ColumnNotFoundSnafu, ComputeArrowSnafu, ComputeVectorSnafu, DatafusionSnafu, EncodeSnafu,
    NewRecordBatchSnafu, Result,
};
use crate::memtable::bulk::buffer::BulkBuffer;
use crate::memtable::stats::WriteMetrics;
use crate::read::{BoxedRecordBatchIterator, Source};
use crate::sst::parquet::format::PrimaryKeyArray;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::to_sst_arrow_schema;

/// Writer for bulk buffer operations that encodes primary keys
pub struct PrimaryKeyBufferWriter {
    /// Primary key codec for encoding keys
    primary_key_codec: Arc<dyn PrimaryKeyCodec>,
    /// Region metadata for schema information
    metadata: RegionMetadataRef,
    /// Column indices mapping for efficient access
    column_indices: ColumnIndices,
}

impl PrimaryKeyBufferWriter {
    /// Creates a new PrimaryKeyBufferWriter
    pub fn new(primary_key_codec: Arc<dyn PrimaryKeyCodec>, metadata: RegionMetadataRef) -> Self {
        let column_indices = Self::create_column_indices(&metadata);

        Self {
            primary_key_codec,
            metadata,
            column_indices,
        }
    }

    /// Creates the column indices based on the schema
    /// Schema follows format: field 0, field 1, ..., field N, time index, primary key, sequence, op type
    pub(crate) fn create_column_indices(metadata: &RegionMetadataRef) -> ColumnIndices {
        let mut buffer_index = 0;

        // Add field columns first (in the order they appear in metadata)
        for field_column in metadata.field_columns() {
            buffer_index += 1;
        }

        // Add time index column
        let time_index = buffer_index;
        buffer_index += 1;

        // Add primary key column (dictionary encoded binary)
        let primary_key_index = buffer_index;
        buffer_index += 1;

        // Add sequence column
        let sequence_index = buffer_index;
        buffer_index += 1;

        // Add op type column
        let op_type_index = buffer_index;

        ColumnIndices {
            time_index,
            primary_key_index,
            sequence_index,
            op_type_index,
        }
    }

    /// Writes a single KeyValue to the buffer.
    pub fn write_one(
        &self,
        buffer: &mut BulkBuffer,
        key_value: KeyValue,
        metrics: &mut WriteMetrics,
    ) -> Result<()> {
        // Extract values from the key_value
        let timestamp = key_value.timestamp();
        let sequence = key_value.sequence();
        let op_type = key_value.op_type();

        // Write timestamp
        buffer.push_value(self.column_indices.time_index, timestamp)?;
        metrics.value_bytes += timestamp.data_size();

        // Handle primary key based on encoding type
        let primary_key_bytes = if self.primary_key_codec.encoding() == PrimaryKeyEncoding::Sparse {
            // For sparse encoding, the primary key is already encoded in the KeyValue
            // Get the first (and only) primary key value which contains the encoded key
            let mut primary_keys = key_value.primary_keys();
            // Safety: __primary_key should be binary type.
            let encoded_key = primary_keys
                .next()
                .context(ColumnNotFoundSnafu {
                    column: PRIMARY_KEY_COLUMN_NAME,
                })?
                .as_binary()
                .unwrap()
                .unwrap();
            encoded_key.to_vec()
        } else {
            // For dense encoding, we need to encode the primary key columns
            let mut primary_key_bytes = Vec::new();
            self.primary_key_codec
                .encode_key_value(&key_value, &mut primary_key_bytes)
                .context(EncodeSnafu)?;
            primary_key_bytes
        };

        // Write primary key
        buffer.push_value(
            self.column_indices.primary_key_index,
            ValueRef::Binary(&primary_key_bytes),
        )?;
        metrics.key_bytes += primary_key_bytes.len();

        // Write sequence
        buffer.push_value(
            self.column_indices.sequence_index,
            ValueRef::UInt64(sequence),
        )?;
        metrics.value_bytes += std::mem::size_of::<SequenceNumber>();

        // Write op type
        buffer.push_value(
            self.column_indices.op_type_index,
            ValueRef::UInt8(op_type as u8),
        )?;
        metrics.value_bytes += std::mem::size_of::<u8>();

        // Write field values
        for (buffer_index, value) in key_value.fields().enumerate() {
            buffer.push_value(buffer_index, value)?;
            metrics.value_bytes += value.data_size();
        }

        // Finish the row
        buffer.finish_one();

        // Update remaining metrics
        if let Some(ts_value) = timestamp.as_timestamp().unwrap() {
            let ts_millis = ts_value.value();
            metrics.min_ts = metrics.min_ts.min(ts_millis);
            metrics.max_ts = metrics.max_ts.max(ts_millis);
        }

        metrics.max_sequence = metrics.max_sequence.max(sequence);
        metrics.num_rows += 1;

        Ok(())
    }

    /// Writes multiple KeyValues to the buffer
    pub fn write(
        &self,
        buffer: &mut BulkBuffer,
        key_values: &KeyValues,
        metrics: &mut WriteMetrics,
    ) -> Result<()> {
        for key_value in key_values.iter() {
            self.write_one(buffer, key_value, metrics)?;
        }
        Ok(())
    }

    /// Returns the primary key codec
    pub(crate) fn primary_key_codec(&self) -> &Arc<dyn PrimaryKeyCodec> {
        &self.primary_key_codec
    }

    /// Encodes the `batch` into a new `RecordBatch` with primary key columns.
    /// `batch_encoding` is the encoding of the `batch`, if it is `sparse`, then the primary key is
    /// already encoded in the `__primary_key` column.
    ///
    /// The output record batch has the schema:
    /// `(fields, time index, primary key, sequence, op type)`.
    ///
    /// It doesn't sort the `batch`.
    pub(crate) fn encode_primary_key_record_batch(
        &self,
        batch: &RecordBatch,
        batch_encoding: PrimaryKeyEncoding,
        sequence: SequenceNumber,
        op_type: OpType,
    ) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        let batch_schema = batch.schema();

        // Build output schema: (fields, time index, primary key, sequence, op type)
        let mut output_fields = Vec::new();

        // Add field columns
        for field_column in self.metadata.field_columns() {
            output_fields.push(Field::new(
                &field_column.column_schema.name,
                field_column.column_schema.data_type.as_arrow_type(),
                field_column.column_schema.is_nullable(),
            ));
        }

        // Add time index column
        let time_column = &self.metadata.time_index_column().column_schema;
        output_fields.push(Field::new(
            &time_column.name,
            time_column.data_type.as_arrow_type(),
            time_column.is_nullable(),
        ));

        // Add primary key column (binary)
        output_fields.push(Field::new(
            PRIMARY_KEY_COLUMN_NAME,
            ArrowDataType::Binary,
            false,
        ));

        // Add sequence column
        output_fields.push(Field::new(
            SEQUENCE_COLUMN_NAME,
            ArrowDataType::UInt64,
            false,
        ));

        // Add op type column
        output_fields.push(Field::new(OP_TYPE_COLUMN_NAME, ArrowDataType::UInt8, false));

        let output_schema = Arc::new(Schema::new(output_fields));

        // Prepare output arrays
        let mut output_arrays: Vec<ArrayRef> = Vec::new();

        // Copy field columns
        for field_column in self.metadata.field_columns() {
            let column_name = &field_column.column_schema.name;
            let column_index =
                batch_schema
                    .index_of(column_name)
                    .ok()
                    .with_context(|| ColumnNotFoundSnafu {
                        column: column_name.clone(),
                    })?;
            output_arrays.push(batch.column(column_index).clone());
        }

        // Copy time index column
        let time_column_name = &self.metadata.time_index_column().column_schema.name;
        let time_index = batch_schema
            .index_of(time_column_name)
            .ok()
            .with_context(|| ColumnNotFoundSnafu {
                column: time_column_name.clone(),
            })?;
        output_arrays.push(batch.column(time_index).clone());

        // Handle primary key column based on encoding
        let primary_key_array = match batch_encoding {
            PrimaryKeyEncoding::Sparse => {
                // For sparse encoding, primary key is already encoded in __primary_key column
                let pk_index = batch_schema
                    .index_of(PRIMARY_KEY_COLUMN_NAME)
                    .ok()
                    .with_context(|| ColumnNotFoundSnafu {
                        column: PRIMARY_KEY_COLUMN_NAME.to_string(),
                    })?;
                batch.column(pk_index).clone()
            }
            PrimaryKeyEncoding::Dense => {
                // For dense encoding, we need to encode the primary key columns
                let mut binary_builder = BinaryBuilder::new();

                // Collect primary key columns and their column IDs
                let pk_columns: Result<Vec<(ColumnId, Option<VectorRef>)>> = self
                    .metadata
                    .primary_key_columns()
                    .map(|col| {
                        let column_name = &col.column_schema.name;
                        let vector_ref = if let Ok(idx) = batch_schema.index_of(column_name) {
                            Some(
                                Helper::try_into_vector(batch.column(idx).clone())
                                    .context(ComputeVectorSnafu)?,
                            )
                        } else {
                            None
                        };
                        Ok((col.column_id, vector_ref))
                    })
                    .collect();
                let pk_columns = pk_columns?;

                // Reusable vector for primary key values
                let mut primary_key_values = Vec::with_capacity(pk_columns.len());

                // Encode each row's primary key
                for row_idx in 0..num_rows {
                    primary_key_values.clear();
                    for (column_id, vector_ref) in &pk_columns {
                        let value = if let Some(vector) = vector_ref {
                            vector.get_ref(row_idx)
                        } else {
                            // Use null for missing primary key columns
                            ValueRef::Null
                        };
                        primary_key_values.push((*column_id, value));
                    }

                    let mut encoded_key = Vec::new();
                    self.primary_key_codec
                        .encode_value_refs(&primary_key_values, &mut encoded_key)
                        .context(EncodeSnafu)?;

                    binary_builder.append_value(&encoded_key);
                }

                // Create binary array from builder
                Arc::new(binary_builder.finish()) as ArrayRef
            }
        };

        output_arrays.push(primary_key_array);

        // Add sequence column (constant value for all rows)
        let sequence_array = Arc::new(datatypes::arrow::array::UInt64Array::from(vec![
            sequence;
            num_rows
        ])) as ArrayRef;
        output_arrays.push(sequence_array);

        // Add op type column (constant value for all rows)
        let op_type_array = Arc::new(datatypes::arrow::array::UInt8Array::from(vec![
            op_type as u8;
            num_rows
        ])) as ArrayRef;
        output_arrays.push(op_type_array);

        // Create the output record batch
        RecordBatch::try_new(output_schema, output_arrays).context(NewRecordBatchSnafu)
    }

    /// Gets the column indices.
    pub(crate) fn column_indices(&self) -> ColumnIndices {
        self.column_indices
    }

    /// Finishes the `buffer` and builds a sorted record batch.
    ///
    /// The record batch is sorted by (primary key, time index, sequence desc).
    /// Returns None if the `buffer` is empty.
    pub(crate) fn build_record_batch(
        &self,
        buffer: &mut BulkBuffer,
    ) -> Result<Option<RecordBatch>> {
        if buffer.is_empty() {
            return Ok(None);
        }

        // Convert buffer to vectors
        let vectors = buffer.finish_vectors()?;

        // Build schema for the record batch
        let mut fields = Vec::with_capacity(vectors.len());

        // Add field columns first
        for field_column in self.metadata.field_columns() {
            fields.push(Field::new(
                &field_column.column_schema.name,
                field_column.column_schema.data_type.as_arrow_type(),
                field_column.column_schema.is_nullable(),
            ));
        }

        // Add time index column
        let time_column = &self.metadata.time_index_column().column_schema;
        fields.push(Field::new(
            &time_column.name,
            time_column.data_type.as_arrow_type(),
            time_column.is_nullable(),
        ));

        // Add primary key column (binary)
        fields.push(Field::new(
            PRIMARY_KEY_COLUMN_NAME,
            ArrowDataType::Binary,
            false,
        ));

        // Add sequence column
        fields.push(Field::new(
            SEQUENCE_COLUMN_NAME,
            ArrowDataType::UInt64,
            false,
        ));

        // Add op type column
        fields.push(Field::new(OP_TYPE_COLUMN_NAME, ArrowDataType::UInt8, false));

        let schema = Arc::new(Schema::new(fields));

        // Convert vectors to arrow arrays
        let arrays: Vec<ArrayRef> = vectors
            .into_iter()
            .map(|vector| vector.to_arrow_array())
            .collect();

        // Create record batch
        let record_batch =
            RecordBatch::try_new(schema.clone(), arrays).context(NewRecordBatchSnafu)?;

        // Sort by (primary key, time index, sequence desc)
        let sorted_batch = self.column_indices.sort_primary_key_batch(&record_batch)?;

        Ok(Some(sorted_batch))
    }

    /// Sorts the input `batch` by (primary key, time index, sequence desc).
    /// The expected schema is: (fields, time index, primary key, sequence, op type)
    pub(crate) fn sort_primary_key_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        self.column_indices.sort_primary_key_batch(batch)
    }

    /// Sorts the partial sorted record batches.
    pub(crate) fn sort_partial_sorted(
        &self,
        buffers: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        self.column_indices.sort_partial_sorted(buffers)
    }
}

/// Mapping of column types to their buffer indices
#[derive(Debug, Clone, Copy)]
pub(crate) struct ColumnIndices {
    /// Index of time column in the buffer
    pub(crate) time_index: usize,
    /// Index of primary key column in the buffer
    pub(crate) primary_key_index: usize,
    /// Index of sequence column in the buffer
    pub(crate) sequence_index: usize,
    /// Index of op type column in the buffer
    pub(crate) op_type_index: usize,
}

impl ColumnIndices {
    /// Sorts the input `batch` by (primary key, time index, sequence desc).
    /// The expected schema is: (fields, time index, primary key, sequence, op type)
    pub fn sort_primary_key_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let sort_columns = vec![
            // Primary key column (ascending)
            datafusion::arrow::compute::SortColumn {
                values: batch.column(self.primary_key_index).clone(),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            },
            // Time index column (ascending)
            datafusion::arrow::compute::SortColumn {
                values: batch.column(self.time_index).clone(),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            },
            // Sequence column (descending)
            datafusion::arrow::compute::SortColumn {
                values: batch.column(self.sequence_index).clone(),
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
            },
        ];

        let indices = datafusion::arrow::compute::lexsort_to_indices(&sort_columns, None)
            .context(ComputeArrowSnafu)?;

        let sorted_batch = datafusion::arrow::compute::take_record_batch(batch, &indices)
            .context(ComputeArrowSnafu)?;

        Ok(sorted_batch)
    }

    /// Sorts the partial sorted record batches.
    pub fn sort_partial_sorted(&self, buffers: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        if buffers.is_empty() {
            return Ok(Vec::new());
        }

        if buffers.len() == 1 {
            return Ok(buffers);
        }

        // Concatenate all batches and sort the result
        let schema = buffers[0].schema();
        let concatenated_batch = datafusion::arrow::compute::concat_batches(&schema, &buffers)
            .context(ComputeArrowSnafu)?;

        // Sort the concatenated batch
        let sorted_batch = self.sort_primary_key_batch(&concatenated_batch)?;

        Ok(vec![sorted_batch])
    }
}

/// Merges a list of sorted RecordBatches and returns an iterator that yields merged RecordBatches.
///
/// Each input RecordBatch must be sorted by primary key, time index, sequence desc.
/// The function takes a closure to compare specific rows in two record batches,
/// and a batch size to control the output record batch size.
///
/// # Arguments
///
/// * `batches` - A vector of sorted RecordBatches to merge
/// * `compare_fn` - A closure that compares two rows from different batches a, b, returns true if a is ordered before b.
/// * `batch_size` - The target size for output record batches
///
/// # Returns
///
/// Returns a iterator that yields merged RecordBatches in sorted order.
pub fn merge_sorted_batches<F>(
    batches: Vec<RecordBatch>,
    compare_fn: F,
    batch_size: usize,
) -> Box<dyn Iterator<Item = Result<RecordBatch>>>
where
    F: Fn(&RecordBatch, usize, &RecordBatch, usize) -> bool + 'static,
{
    if batches.len() < 2 {
        return Box::new(batches.into_iter().map(Ok));
    }

    let batches_clone = batches.clone();
    let batche_lens = batches.iter().map(|batch| batch.num_rows()).collect_vec();
    let merge_iter = batche_lens
        .into_iter()
        .enumerate()
        .map(|(batch_idx, num_rows)| (0..num_rows).map(move |row_idx| (batch_idx, row_idx)))
        .kmerge_by(move |left, right| {
            compare_fn(
                &batches_clone[left.0],
                left.1,
                &batches_clone[right.0],
                right.1,
            )
        });
    let chunk_iter = ChunkIter {
        iter: merge_iter,
        chunks: Vec::new(),
        batch_size,
        end: false,
    };
    let iter = chunk_iter.map(move |indices| {
        let batches = batches.iter().collect_vec();
        datatypes::arrow::compute::interleave_record_batch(&batches, &indices)
            .context(ComputeArrowSnafu)
    });
    Box::new(iter)
}

struct ChunkIter<I> {
    iter: I,
    chunks: Vec<(usize, usize)>,
    batch_size: usize,
    end: bool,
}

impl<I: Iterator<Item = (usize, usize)>> Iterator for ChunkIter<I> {
    type Item = Vec<(usize, usize)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.end {
            return None;
        }

        for _ in 0..self.batch_size {
            if let Some(v) = self.iter.next() {
                self.chunks.push(v);
            } else {
                self.end = true;
                return Some(std::mem::take(&mut self.chunks));
            }
        }

        Some(std::mem::take(&mut self.chunks))
    }
}

/// A merge iterator for merging multiple BoxedRecordBatchIterator instances.
/// Each iterator must yield record batches sorted by primary key, time index, sequence desc.
/// Uses a KMergeBy-style implementation similar to itertools with a heap.
pub struct MergeIterator<F> {
    /// Iterator states - None means exhausted
    states: Vec<Option<IteratorState>>,
    /// Compare function for ordering record batch rows
    compare_fn: F,
    /// Target size for output batches
    batch_size: usize,
    /// Buffer for accumulating output rows
    output_buffer: Vec<(usize, usize)>,
    /// Heap of active iterator indices, maintained as a min-heap
    heap: Vec<usize>,
}

/// State for each iterator
#[derive(Debug)]
struct IteratorState {
    /// Current record batch
    current_batch: RecordBatch,
    /// Current row index in the batch
    current_row_idx: usize,
}

impl<F> MergeIterator<F>
where
    F: Fn(&RecordBatch, usize, &RecordBatch, usize) -> bool,
{
    /// Creates a new merge iterator for the given iterators
    pub fn new(
        iterators: &mut [BoxedRecordBatchIterator],
        compare_fn: F,
        batch_size: usize,
    ) -> Result<Self> {
        let num_iterators = iterators.len();
        let mut states = Vec::with_capacity(num_iterators);

        // Initialize each iterator by getting the first batch
        for iterator in iterators.iter_mut() {
            let state = if let Some(batch_result) = iterator.next() {
                let batch = batch_result?;
                if batch.num_rows() > 0 {
                    Some(IteratorState {
                        current_batch: batch,
                        current_row_idx: 0,
                    })
                } else {
                    None
                }
            } else {
                None
            };
            states.push(state);
        }

        // Initialize heap with all active iterators
        let mut heap = Vec::new();
        for (idx, state) in states.iter().enumerate() {
            if state.is_some() {
                heap.push(idx);
            }
        }

        let mut result = Self {
            states,
            compare_fn,
            batch_size,
            output_buffer: Vec::new(),
            heap,
        };

        // Build the heap
        result.heapify();

        Ok(result)
    }

    /// Peeks at the minimum iterator to check if it will advance to next batch
    fn peek_min_will_advance_to_next_batch(&self) -> bool {
        if self.heap.is_empty() {
            return false;
        }

        let min_idx = self.heap[0];
        if let Some(state) = &self.states[min_idx] {
            state.current_row_idx + 1 >= state.current_batch.num_rows()
        } else {
            false
        }
    }

    /// Consumes the current row from the minimum iterator without advancing to next batch
    /// Returns the iterator index and the row index that was consumed
    fn consume_min_row(&self) -> Option<(usize, usize)> {
        if self.heap.is_empty() {
            return None;
        }

        let min_idx = self.heap[0];
        if let Some(state) = &self.states[min_idx] {
            Some((min_idx, state.current_row_idx))
        } else {
            None
        }
    }

    /// Advances the minimum iterator to the next batch and adjusts the heap
    fn advance_min_iterator(&mut self, iterators: &mut [BoxedRecordBatchIterator]) -> Result<()> {
        if self.heap.is_empty() {
            return Ok(());
        }

        let min_idx = self.heap[0];

        // Advance the iterator
        let should_reinsert = if let Some(state) = &mut self.states[min_idx] {
            // Check if we can advance to the next row in the current batch
            if state.current_row_idx + 1 < state.current_batch.num_rows() {
                state.current_row_idx += 1;
                true
            } else {
                // Need to get the next batch from this iterator
                if let Some(next_batch_result) = iterators[min_idx].next() {
                    let next_batch = next_batch_result?;
                    if next_batch.num_rows() > 0 {
                        state.current_batch = next_batch;
                        state.current_row_idx = 0;
                        true
                    } else {
                        // Empty batch, mark this iterator as exhausted
                        self.states[min_idx] = None;
                        false
                    }
                } else {
                    // Iterator is exhausted
                    self.states[min_idx] = None;
                    false
                }
            }
        } else {
            false
        };

        if should_reinsert {
            // Re-establish heap property from the root
            self.sift_down(0);
        } else {
            // Remove the exhausted iterator from the heap
            if !self.heap.is_empty() {
                self.heap[0] = self.heap[self.heap.len() - 1];
                self.heap.pop();
                if !self.heap.is_empty() {
                    self.sift_down(0);
                }
            }
        }

        Ok(())
    }

    /// Pops the minimum iterator from the heap and advances it
    /// Returns the iterator index and the row index that was consumed
    fn pop_min_and_advance(
        &mut self,
        iterators: &mut [BoxedRecordBatchIterator],
    ) -> Result<Option<(usize, usize)>> {
        if self.heap.is_empty() {
            return Ok(None);
        }

        // Get the minimum iterator index
        let min_idx = self.heap[0];

        // Get the current row index before advancing
        let current_row_idx = if let Some(state) = &self.states[min_idx] {
            state.current_row_idx
        } else {
            return Ok(None);
        };

        // Advance the iterator
        let should_reinsert = if let Some(state) = &mut self.states[min_idx] {
            // Check if we can advance to the next row in the current batch
            if state.current_row_idx + 1 < state.current_batch.num_rows() {
                state.current_row_idx += 1;
                true
            } else {
                // Need to get the next batch from this iterator
                if let Some(next_batch_result) = iterators[min_idx].next() {
                    let next_batch = next_batch_result?;
                    if next_batch.num_rows() > 0 {
                        state.current_batch = next_batch;
                        state.current_row_idx = 0;
                        true
                    } else {
                        // Empty batch, mark this iterator as exhausted
                        self.states[min_idx] = None;
                        false
                    }
                } else {
                    // Iterator is exhausted
                    self.states[min_idx] = None;
                    false
                }
            }
        } else {
            false
        };

        if should_reinsert {
            // Re-establish heap property from the root
            self.sift_down(0);
        } else {
            // Remove the exhausted iterator from the heap
            if !self.heap.is_empty() {
                self.heap[0] = self.heap[self.heap.len() - 1];
                self.heap.pop();
                if !self.heap.is_empty() {
                    self.sift_down(0);
                }
            }
        }

        Ok(Some((min_idx, current_row_idx)))
    }

    /// Initialize the heap by building a min-heap from the current states
    fn heapify(&mut self) {
        let len = self.heap.len();
        for i in (0..len / 2).rev() {
            self.sift_down(i);
        }
    }

    /// Sift down operation to maintain heap property
    fn sift_down(&mut self, mut idx: usize) {
        let len = self.heap.len();

        loop {
            let mut smallest = idx;
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;

            // Check left child
            if left < len && self.compare_heap_elements(self.heap[left], self.heap[smallest]) {
                smallest = left;
            }

            // Check right child
            if right < len && self.compare_heap_elements(self.heap[right], self.heap[smallest]) {
                smallest = right;
            }

            // If heap property is satisfied, break
            if smallest == idx {
                break;
            }

            // Swap and continue sifting down
            self.heap.swap(idx, smallest);
            idx = smallest;
        }
    }

    /// Compare two heap elements (iterator indices) using the compare function
    /// Returns true if left < right (for min-heap)
    fn compare_heap_elements(&self, left_idx: usize, right_idx: usize) -> bool {
        if let (Some(left_state), Some(right_state)) =
            (&self.states[left_idx], &self.states[right_idx])
        {
            (self.compare_fn)(
                &left_state.current_batch,
                left_state.current_row_idx,
                &right_state.current_batch,
                right_state.current_row_idx,
            )
        } else {
            false
        }
    }
}

/// Merges batches from multiple sorted sources into a single sorted stream.
/// Input sources must be sorted by primary key and have the same schema.
pub(crate) fn merge_record_batch_df(
    metadata: &RegionMetadata,
    sources: Vec<BoxedRecordBatchIterator>,
) -> Result<Source> {
    // TODO(yingwen): Can we pass the schema as an argument?
    let schema = to_sst_arrow_schema(metadata);
    let streams: Vec<_> = sources
        .into_iter()
        .map(|source| {
            Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::iter(source).map_err(|e| DataFusionError::External(Box::new(e))),
            )) as _
        })
        .collect();
    let exprs = sort_expressions(&schema);

    common_telemetry::info!(
        "Merge plain, num_sources: {}, exprs: {:?}, schema: {:?}",
        streams.len(),
        exprs,
        schema
    );

    let memory_pool = Arc::new(UnboundedMemoryPool::default()) as _;
    let reservation = MemoryConsumer::new("merge_plain").register(&memory_pool);
    let metrics_set = ExecutionPlanMetricsSet::new();
    let baseline_metrics = BaselineMetrics::new(&metrics_set, 0);
    let mut stream = StreamingMergeBuilder::new()
        .with_schema(schema)
        .with_streams(streams)
        .with_expressions(&exprs)
        .with_batch_size(DEFAULT_READ_BATCH_SIZE)
        .with_reservation(reservation)
        .with_metrics(baseline_metrics.clone())
        .build()
        .unwrap();

    // Convert the stream to an iterator by polling manually
    // Since it's pure memory operation, Poll won't return Pending
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    use futures::Stream;

    struct ManualPollIter<S> {
        stream: Pin<Box<S>>,
        waker: Waker,
    }

    impl<S> ManualPollIter<S>
    where
        S: Stream<Item = Result<RecordBatch, DataFusionError>> + Send,
    {
        fn new(stream: S) -> Self {
            let waker = futures::task::noop_waker();
            Self {
                stream: Box::pin(stream),
                waker,
            }
        }
    }

    impl<S> Iterator for ManualPollIter<S>
    where
        S: Stream<Item = Result<RecordBatch, DataFusionError>> + Send,
    {
        type Item = Result<RecordBatch>;

        fn next(&mut self) -> Option<Self::Item> {
            let mut context = Context::from_waker(&self.waker);
            match self.stream.as_mut().poll_next(&mut context) {
                Poll::Ready(Some(Ok(batch))) => Some(Ok(batch)),
                Poll::Ready(Some(Err(e))) => Some(Err(e).context(DatafusionSnafu)),
                Poll::Ready(None) => None,
                Poll::Pending => {
                    // This should not happen for pure memory operations
                    unreachable!("Stream should not return Pending for pure memory operations")
                }
            }
        }
    }

    let iter = ManualPollIter::new(stream);
    Ok(Source::RecordBatchIter(Box::new(iter)))
}

/// Builds the sort expressions from the region metadata
/// to sort by:
/// (primary key ASC, time index ASC, sequence DESC)
pub(crate) fn sort_expressions(schema: &SchemaRef) -> LexOrdering {
    // TODO(yingwen): Error handling.
    // TODO(yingwen): Return time index column id from metadata.
    let time_index_pos = schema.fields.len() - 4;
    let time_index_expr =
        create_sort_expr(&schema.fields[time_index_pos].name(), time_index_pos, false);
    let sequence_index = schema.fields.len() - 2;
    let sequence_expr = create_sort_expr(SEQUENCE_COLUMN_NAME, sequence_index, true);
    let primary_key_expr =
        create_sort_expr(PRIMARY_KEY_COLUMN_NAME, schema.fields.len() - 3, false);

    LexOrdering::new(vec![primary_key_expr, time_index_expr, sequence_expr])
}

/// Helper function to create a sort expression for a column.
fn create_sort_expr(column_name: &str, column_index: usize, descending: bool) -> PhysicalSortExpr {
    let column = Column::new(column_name, column_index);
    PhysicalSortExpr {
        expr: Arc::new(column),
        options: SortOptions {
            descending,
            nulls_first: true,
        },
    }
}

/// Creates a merge iterator that merges multiple BoxedRecordBatchIterator instances
/// using a KMergeBy-style merge algorithm similar to merge_sorted_batches.
///
/// # Arguments
///
/// * `iterators` - Vector of BoxedRecordBatchIterator instances to merge
/// * `compare_fn` - Function to compare two rows from different batches
/// * `batch_size` - Target size for output batches
///
/// # Returns
///
/// Returns a BoxedRecordBatchIterator that yields merged record batches
pub fn merge_iterators<F>(
    iterators: Vec<BoxedRecordBatchIterator>,
    compare_fn: F,
    batch_size: usize,
) -> Result<BoxedRecordBatchIterator>
where
    F: Fn(&RecordBatch, usize, &RecordBatch, usize) -> bool + Send + 'static,
{
    if iterators.is_empty() {
        return Ok(Box::new(std::iter::empty()));
    }

    if iterators.len() == 1 {
        return Ok(iterators.into_iter().next().unwrap());
    }

    let merge_iter = MergeIteratorImpl::new(iterators, compare_fn, batch_size)?;
    Ok(Box::new(merge_iter))
}

/// Implementation of the merge iterator that implements the Iterator trait
struct MergeIteratorImpl<F> {
    iterators: Vec<BoxedRecordBatchIterator>,
    merge_state: MergeIterator<F>,
}

impl<F> MergeIteratorImpl<F>
where
    F: Fn(&RecordBatch, usize, &RecordBatch, usize) -> bool,
{
    fn new(
        mut iterators: Vec<BoxedRecordBatchIterator>,
        compare_fn: F,
        batch_size: usize,
    ) -> Result<Self> {
        let merge_state = MergeIterator::new(&mut iterators, compare_fn, batch_size)?;
        Ok(Self {
            iterators,
            merge_state,
        })
    }
}

impl<F> Iterator for MergeIteratorImpl<F>
where
    F: Fn(&RecordBatch, usize, &RecordBatch, usize) -> bool,
{
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.merge_state.output_buffer.clear();

        // Fill the output buffer with up to batch_size rows
        while self.merge_state.output_buffer.len() < self.merge_state.batch_size {
            // Check if the minimum iterator will advance to next batch
            let will_advance_to_next_batch = self.merge_state.peek_min_will_advance_to_next_batch();

            if will_advance_to_next_batch {
                // If the iterator will advance to next batch, consume the current row without advancing
                if let Some((min_idx, row_idx)) = self.merge_state.consume_min_row() {
                    // Add this row to the output buffer
                    self.merge_state.output_buffer.push((min_idx, row_idx));
                    // Break and return the current batch now
                    break;
                } else {
                    // No more items available
                    break;
                }
            } else {
                // Normal case: consume row and advance iterator
                match self.merge_state.pop_min_and_advance(&mut self.iterators) {
                    Ok(Some((min_idx, row_idx))) => {
                        // Add this row to the output buffer
                        self.merge_state.output_buffer.push((min_idx, row_idx));
                    }
                    Ok(None) => {
                        // No more items available
                        break;
                    }
                    Err(e) => {
                        return Some(Err(e));
                    }
                }
            }
        }

        if self.merge_state.output_buffer.is_empty() {
            return None;
        }

        // Convert the output buffer to a RecordBatch
        let batches: Vec<&RecordBatch> = self
            .merge_state
            .states
            .iter()
            .filter_map(|state| state.as_ref().map(|s| &s.current_batch))
            .collect();

        if batches.is_empty() {
            return None;
        }

        // Use the interleave function to create the output batch
        let result = match datatypes::arrow::compute::interleave_record_batch(
            &batches,
            &self.merge_state.output_buffer,
        ) {
            Ok(batch) => Some(Ok(batch)),
            Err(e) => Some(Err(ComputeArrowSnafu.into_error(e))),
        };

        // If we consumed a row that would advance to next batch, advance the iterator now
        if !self.merge_state.output_buffer.is_empty() {
            let last_consumed =
                &self.merge_state.output_buffer[self.merge_state.output_buffer.len() - 1];
            if self.merge_state.states[last_consumed.0].is_some() {
                let current_state = &self.merge_state.states[last_consumed.0].as_ref().unwrap();
                // Check if this was the last row in the current batch
                if current_state.current_row_idx + 1 >= current_state.current_batch.num_rows() {
                    // Advance the iterator for the next call
                    if let Err(e) = self.merge_state.advance_min_iterator(&mut self.iterators) {
                        return Some(Err(e));
                    }
                }
            }
        }

        result
    }
}

/// Merges multiple BoxedRecordBatchIterator instances using primary key ordering.
///
/// This function merges iterators where each yields record batches sorted by:
/// 1. Primary key (ascending)
/// 2. Timestamp (ascending)
/// 3. Sequence (descending)
///
/// The record batches are expected to have the same schema as produced by
/// `encode_primary_key_record_batch`: (fields, time index, primary key, sequence, op type)
/// where the last 4 columns are: time index, primary key, sequence, op type
///
/// The primary key column can be either:
/// - BinaryArray: Raw binary-encoded primary key values
/// - PrimaryKeyArray: Dictionary-encoded binary primary key values (DictionaryArray<UInt32Type>)
///
/// # Arguments
///
/// * `iterators` - Vector of BoxedRecordBatchIterator instances to merge
/// * `batch_size` - Target size for output batches
///
/// # Returns
///
/// Returns a BoxedRecordBatchIterator that yields merged record batches in sorted order
pub fn merge_iterators_by_primary_key(
    iterators: Vec<BoxedRecordBatchIterator>,
    batch_size: usize,
) -> Result<BoxedRecordBatchIterator> {
    let compare_fn =
        move |batch_a: &RecordBatch, row_a: usize, batch_b: &RecordBatch, row_b: usize| -> bool {
            let num_cols = batch_a.num_columns();

            // Fixed positions: time index, primary key, sequence, op type (last 4 columns)
            let time_index = num_cols - 4;
            let primary_key_index = num_cols - 3;
            let sequence_index = num_cols - 2;

            // Compare primary key column (binary data or dictionary-encoded binary)
            let pk_a = batch_a.column(primary_key_index);
            let pk_b = batch_b.column(primary_key_index);

            let pk_value_a = extract_primary_key_value(pk_a, row_a);
            let pk_value_b = extract_primary_key_value(pk_b, row_b);

            match pk_value_a.cmp(pk_value_b) {
                std::cmp::Ordering::Less => return true,
                std::cmp::Ordering::Greater => return false,
                std::cmp::Ordering::Equal => {
                    // Primary keys are equal, compare timestamp
                    let ts_a = batch_a.column(time_index);
                    let ts_b = batch_b.column(time_index);

                    // Compare timestamp values using arrow compute
                    let ts_cmp = compare_timestamp_arrays(ts_a, row_a, ts_b, row_b);
                    match ts_cmp {
                        std::cmp::Ordering::Less => return true,
                        std::cmp::Ordering::Greater => return false,
                        std::cmp::Ordering::Equal => {
                            // Timestamps are equal, compare sequence (descending)
                            let seq_a = batch_a.column(sequence_index);
                            let seq_b = batch_b.column(sequence_index);

                            let seq_array_a = seq_a
                                .as_any()
                                .downcast_ref::<datatypes::arrow::array::UInt64Array>()
                                .unwrap();
                            let seq_array_b = seq_b
                                .as_any()
                                .downcast_ref::<datatypes::arrow::array::UInt64Array>()
                                .unwrap();

                            let seq_value_a = seq_array_a.value(row_a);
                            let seq_value_b = seq_array_b.value(row_b);

                            // For sequence, we want descending order (higher sequence first)
                            seq_value_a > seq_value_b
                        }
                    }
                }
            }
        };

    merge_iterators(iterators, compare_fn, batch_size)
}

/// Helper function to extract primary key value from either BinaryArray or PrimaryKeyArray
fn extract_primary_key_value(array: &ArrayRef, row: usize) -> &[u8] {
    use datatypes::arrow::datatypes::DataType;

    match array.data_type() {
        DataType::Binary => {
            let binary_array = array
                .as_any()
                .downcast_ref::<datatypes::arrow::array::BinaryArray>()
                .unwrap();
            binary_array.value(row)
        }
        DataType::Dictionary(key_type, value_type) => {
            // Check if this is a PrimaryKeyArray (Dictionary<UInt32, Binary>)
            if let (DataType::UInt32, DataType::Binary) = (key_type.as_ref(), value_type.as_ref()) {
                let dict_array = array.as_any().downcast_ref::<PrimaryKeyArray>().unwrap();

                // Get the key (index into the dictionary)
                let key = dict_array.key(row).unwrap();

                // Get the value from the dictionary
                let values = dict_array.values();
                let binary_values = values
                    .as_any()
                    .downcast_ref::<datatypes::arrow::array::BinaryArray>()
                    .unwrap();
                binary_values.value(key as usize)
            } else {
                panic!("Unsupported dictionary type for primary key");
            }
        }
        _ => panic!("Unsupported array type for primary key"),
    }
}

/// Helper function to compare timestamp values from two arrays
fn compare_timestamp_arrays(
    ts_a: &ArrayRef,
    row_a: usize,
    ts_b: &ArrayRef,
    row_b: usize,
) -> std::cmp::Ordering {
    use datatypes::arrow::array::*;
    use datatypes::arrow::datatypes::TimeUnit;

    match (ts_a.data_type(), ts_b.data_type()) {
        (
            datatypes::arrow::datatypes::DataType::Timestamp(unit_a, _),
            datatypes::arrow::datatypes::DataType::Timestamp(unit_b, _),
        ) => {
            let ts_value_a = match unit_a {
                TimeUnit::Nanosecond => {
                    let array = ts_a
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    array.value(row_a)
                }
                TimeUnit::Microsecond => {
                    let array = ts_a
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    array.value(row_a)
                }
                TimeUnit::Millisecond => {
                    let array = ts_a
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    array.value(row_a)
                }
                TimeUnit::Second => {
                    let array = ts_a
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    array.value(row_a)
                }
            };

            let ts_value_b = match unit_b {
                TimeUnit::Nanosecond => {
                    let array = ts_b
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    array.value(row_b)
                }
                TimeUnit::Microsecond => {
                    let array = ts_b
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    array.value(row_b)
                }
                TimeUnit::Millisecond => {
                    let array = ts_b
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    array.value(row_b)
                }
                TimeUnit::Second => {
                    let array = ts_b
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    array.value(row_b)
                }
            };

            ts_value_a.cmp(&ts_value_b)
        }
        _ => {
            // Fallback for non-timestamp types
            std::cmp::Ordering::Equal
        }
    }
}
