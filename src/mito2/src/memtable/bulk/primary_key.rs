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
use datatypes::arrow::array::{
    make_comparator, ArrayRef, BinaryBuilder, BooleanArray, DictionaryArray, UInt32Array,
    UInt64Array, UInt8Array,
};
use datatypes::arrow::compute::{filter_record_batch, SortOptions};
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

/// Helper functions for RecordBatch deduplication
fn has_same_key_cross_batch(
    left_batch: &RecordBatch,
    left_row: usize,
    right_batch: &RecordBatch,
    right_row: usize,
) -> Result<bool> {
    // Get primary key columns (assuming same schema structure)
    let pk_index = left_batch.schema().fields().len() - 3; // primary key is 3rd from end

    let pk_array_left = left_batch.column(pk_index);
    let pk_array_right = right_batch.column(pk_index);

    // Use make_comparator for efficient comparison
    let comparator = make_comparator(pk_array_left, pk_array_right, SortOptions::default())
        .context(ComputeArrowSnafu)?;

    Ok(comparator(left_row, right_row) == std::cmp::Ordering::Equal)
}

/// Compares rows within the same batch
fn has_same_key_same_batch(batch: &RecordBatch, left_row: usize, right_row: usize) -> Result<bool> {
    has_same_key_cross_batch(batch, left_row, batch, right_row)
}

/// Compares two rows by (primary_key, timestamp, sequence) to determine ordering
/// Returns true if left row should come before right row
fn should_row_come_before(
    left_batch: &RecordBatch,
    left_row: usize,
    right_batch: &RecordBatch,
    right_row: usize,
) -> Result<bool> {
    let schema_len = left_batch.schema().fields().len();
    let pk_index = schema_len - 3;
    let time_index = schema_len - 4;
    let seq_index = schema_len - 2;

    // Compare primary keys first
    let pk_left = left_batch.column(pk_index);
    let pk_right = right_batch.column(pk_index);
    let pk_comparator =
        make_comparator(pk_left, pk_right, SortOptions::default()).context(ComputeArrowSnafu)?;

    match pk_comparator(left_row, right_row) {
        std::cmp::Ordering::Less => return Ok(true),
        std::cmp::Ordering::Greater => return Ok(false),
        std::cmp::Ordering::Equal => {
            // Same primary key, compare timestamps
            let ts_left = left_batch.column(time_index);
            let ts_right = right_batch.column(time_index);
            let ts_comparator = make_comparator(ts_left, ts_right, SortOptions::default())
                .context(ComputeArrowSnafu)?;

            match ts_comparator(left_row, right_row) {
                std::cmp::Ordering::Less => return Ok(true),
                std::cmp::Ordering::Greater => return Ok(false),
                std::cmp::Ordering::Equal => {
                    // Same timestamp, compare sequence (descending - higher sequence comes first)
                    let seq_left = left_batch.column(seq_index);
                    let seq_right = right_batch.column(seq_index);
                    let seq_comparator = make_comparator(
                        seq_left,
                        seq_right,
                        SortOptions {
                            descending: true,
                            nulls_first: false,
                        },
                    )
                    .context(ComputeArrowSnafu)?;

                    Ok(seq_comparator(left_row, right_row) == std::cmp::Ordering::Less)
                }
            }
        }
    }
}

/// Trait for deduplication strategies on RecordBatch
pub trait RecordBatchDedupStrategy: Send {
    /// Pushes a batch for deduplication processing
    fn push_batch(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>>;

    /// Finishes the deduplication process and returns any remaining batch
    fn finish(&mut self) -> Result<Option<RecordBatch>>;
}

/// Strategy that keeps the last row for each unique key
pub struct RecordBatchLastRow {
    /// Previous batch to handle cross-batch deduplication
    prev_batch: Option<RecordBatch>,
    /// Whether to filter out deleted rows (op_type == DELETE)
    filter_deleted: bool,
}

impl RecordBatchLastRow {
    pub fn new(filter_deleted: bool) -> Self {
        Self {
            prev_batch: None,
            filter_deleted,
        }
    }
}

impl RecordBatchDedupStrategy for RecordBatchLastRow {
    fn push_batch(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        // TODO: Implement cross-batch deduplication logic
        // For now, store current batch and return previous if exists
        let result = self.prev_batch.take();
        self.prev_batch = Some(batch);
        Ok(result)
    }

    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.prev_batch.take())
    }
}

/// Strategy that merges non-null fields for rows with the same key
pub struct RecordBatchLastNonNull {
    /// Buffer for merging non-null values
    buffer: Option<RecordBatch>,
    /// Whether to filter out deleted rows (op_type == DELETE)
    filter_deleted: bool,
}

impl RecordBatchLastNonNull {
    pub fn new(filter_deleted: bool) -> Self {
        Self {
            buffer: None,
            filter_deleted,
        }
    }
}

impl RecordBatchDedupStrategy for RecordBatchLastNonNull {
    fn push_batch(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        // TODO: Implement non-null merging logic
        // For now, store current batch and return previous if exists
        let result = self.buffer.take();
        self.buffer = Some(batch);
        Ok(result)
    }

    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.buffer.take())
    }
}

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

    /// Writes a single KeyValue to the buffer with separate primary key buffer.
    pub fn write_one_with_pk_buffer(
        &self,
        buffer: &mut BulkBuffer,
        primary_key_buffer: &mut BulkBuffer,
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

        // Write primary key columns to the separate primary key buffer
        let mut pk_column_index = 0;
        for value in key_value.primary_keys() {
            primary_key_buffer.push_value(pk_column_index, value)?;
            pk_column_index += 1;
        }
        primary_key_buffer.finish_one();

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
        primary_key_buffer: &mut BulkBuffer,
        key_values: &KeyValues,
        metrics: &mut WriteMetrics,
    ) -> Result<()> {
        for key_value in key_values.iter() {
            self.write_one_with_pk_buffer(buffer, primary_key_buffer, key_value, metrics)?;
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
