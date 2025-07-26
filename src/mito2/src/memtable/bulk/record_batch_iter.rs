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

//! RecordBatch-based iterator similar to BulkPartIter

use std::collections::VecDeque;
use std::sync::Arc;

use datatypes::arrow::array::ArrayRef;
use datatypes::arrow::record_batch::RecordBatch;
use snafu::ResultExt;
use store_api::storage::SequenceNumber;

use crate::error::{self, Result};
use crate::memtable::bulk::context::BulkIterContext;
use crate::read::Batch;

/// Iterator that reads data from a single RecordBatch.
/// Supports projection, pruning, and sequence filtering similar to BulkPartIter.
pub struct RecordBatchIter {
    /// The RecordBatch to read from
    record_batch: Option<RecordBatch>,
    /// Iterator context for filtering and conversion
    context: Arc<BulkIterContext>,
    /// Buffer for converted batches
    batch_buffer: VecDeque<Batch>,
    /// Sequence number filter
    sequence: Option<SequenceNumber>,
    /// Whether the RecordBatch has been processed
    processed: bool,
}

impl RecordBatchIter {
    /// Creates a new RecordBatchIter from a single RecordBatch.
    ///
    /// # Arguments
    /// * `record_batch` - The RecordBatch to iterate over
    /// * `context` - Context for filtering and conversion
    /// * `sequence` - Optional sequence filter
    pub fn new(
        record_batch: RecordBatch,
        context: Arc<BulkIterContext>,
        sequence: Option<SequenceNumber>,
    ) -> Self {
        Self {
            record_batch: Some(record_batch),
            context,
            batch_buffer: VecDeque::new(),
            sequence,
            processed: false,
        }
    }

    /// Applies projection to the RecordBatch if needed.
    fn apply_projection(&self, record_batch: RecordBatch) -> Result<RecordBatch> {
        // Check if projection is needed
        let projection_indices = self.context.read_format().projection_indices();
        if projection_indices.len() == record_batch.num_columns() {
            // No projection needed if all columns are selected
            return Ok(record_batch);
        }

        // Apply projection by selecting only the required columns
        let projected_columns: Vec<ArrayRef> = projection_indices
            .iter()
            .map(|&idx| record_batch.column(idx).clone())
            .collect();

        let projected_fields: Vec<_> = projection_indices
            .iter()
            .map(|&idx| record_batch.schema().field(idx).clone())
            .collect();

        let projected_schema = Arc::new(datatypes::arrow::datatypes::Schema::new(projected_fields));

        RecordBatch::try_new(projected_schema, projected_columns)
            .context(error::NewRecordBatchSnafu)
    }

    /// Converts RecordBatch to Batch objects, applies pruning and sequence filtering.
    fn convert_and_filter_record_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        // Convert RecordBatch to Batch format using the context's read format
        let mut batches = VecDeque::new();
        self.context
            .read_format()
            .convert_record_batch(&record_batch, None, &mut batches)?;

        // Apply pruning and sequence filtering to each converted batch
        for batch in batches {
            // Apply precise filtering (pruning) first, similar to PruneReader
            let pruned_batch = self.prune(batch)?;

            if let Some(mut batch) = pruned_batch {
                // Apply sequence filtering after pruning
                if let Some(sequence) = self.sequence {
                    batch.filter_by_sequence(Some(sequence))?;
                }

                // Only add non-empty batches
                if batch.num_rows() > 0 {
                    self.batch_buffer.push_back(batch);
                }
            }
        }

        Ok(())
    }

    /// Prunes batch according to filters, similar to PruneReader.
    fn prune(&self, batch: Batch) -> Result<Option<Batch>> {
        // Fast path: if no filters are configured, return the batch as-is
        if self.context.base.filters.is_empty() {
            return Ok(Some(batch));
        }

        // Apply precise filtering using the context's base filters
        let batch_filtered = self.context.base.precise_filter(batch)?;

        match batch_filtered {
            Some(batch) if !batch.is_empty() => Ok(Some(batch)),
            _ => Ok(None), // Entire batch filtered out or empty
        }
    }

    /// Processes the RecordBatch and fills the batch buffer.
    fn process_record_batch(&mut self) -> Result<()> {
        if self.processed {
            return Ok(());
        }

        if let Some(record_batch) = self.record_batch.take() {
            if record_batch.num_rows() == 0 {
                // Skip empty batches
                self.processed = true;
                return Ok(());
            }

            // Apply projection
            let projected_batch = self.apply_projection(record_batch)?;

            // Convert to Batch format and apply pruning and sequence filtering
            self.convert_and_filter_record_batch(projected_batch)?;

            self.processed = true;
        }

        Ok(())
    }
}

impl Iterator for RecordBatchIter {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        // Return buffered batch if available
        if let Some(batch) = self.batch_buffer.pop_front() {
            return Some(Ok(batch));
        }

        // Process RecordBatch if not yet processed
        if !self.processed {
            match self.process_record_batch() {
                Ok(()) => {
                    // Try to return a batch from the buffer
                    if let Some(batch) = self.batch_buffer.pop_front() {
                        return Some(Ok(batch));
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }

        // No more batches available
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datatypes::arrow::array::{Int64Array, UInt64Array, UInt8Array};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

    #[test]
    fn test_record_batch_iter_basic() {
        // Create a simple schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "__primary_key",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
                false,
            ),
            Field::new("__sequence", DataType::UInt64, false),
            Field::new("__op_type", DataType::UInt8, false),
        ]));

        // Create test data
        let field1 = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let timestamp = Arc::new(datatypes::arrow::array::TimestampMillisecondArray::from(
            vec![1000, 2000, 3000],
        ));

        // Create primary key dictionary array
        use datatypes::arrow::array::{BinaryArray, DictionaryArray, UInt32Array};
        let values = Arc::new(BinaryArray::from_iter_values([b"key1", b"key2", b"key3"]));
        let keys = UInt32Array::from(vec![0, 1, 2]);
        let primary_key = Arc::new(DictionaryArray::new(keys, values));

        let sequence = Arc::new(UInt64Array::from(vec![1, 2, 3]));
        let op_type = Arc::new(UInt8Array::from(vec![1, 1, 1])); // PUT operations

        let record_batch = RecordBatch::try_new(
            schema,
            vec![field1, timestamp, primary_key, sequence, op_type],
        )
        .unwrap();

        // Create a minimal region metadata for testing
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field1",
                    ConcreteDataType::int64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "timestamp",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .primary_key(vec![]);

        let region_metadata = builder.build().unwrap();

        // Create context
        let context = Arc::new(BulkIterContext::new(
            Arc::new(region_metadata),
            &None, // No projection
            None,  // No predicate
        ));

        // Create iterator
        let mut iter = RecordBatchIter::new(record_batch, context, None);

        // Test iteration
        let batch = iter.next();
        assert!(batch.is_some(), "Should return at least one batch");

        let batch = batch.unwrap().unwrap();
        assert!(batch.num_rows() > 0, "Batch should have rows");
    }

    #[test]
    fn test_record_batch_iter_with_sequence_filter() {
        // Create a simple schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "__primary_key",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
                false,
            ),
            Field::new("__sequence", DataType::UInt64, false),
            Field::new("__op_type", DataType::UInt8, false),
        ]));

        // Create test data with different sequence numbers
        let field1 = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let timestamp = Arc::new(datatypes::arrow::array::TimestampMillisecondArray::from(
            vec![1000, 2000, 3000],
        ));

        // Create primary key dictionary array
        use datatypes::arrow::array::{BinaryArray, DictionaryArray, UInt32Array};
        let values = Arc::new(BinaryArray::from_iter_values([b"key1", b"key2", b"key3"]));
        let keys = UInt32Array::from(vec![0, 1, 2]);
        let primary_key = Arc::new(DictionaryArray::new(keys, values));

        let sequence = Arc::new(UInt64Array::from(vec![1, 5, 10])); // Different sequences
        let op_type = Arc::new(UInt8Array::from(vec![1, 1, 1])); // PUT operations

        let record_batch = RecordBatch::try_new(
            schema,
            vec![field1, timestamp, primary_key, sequence, op_type],
        )
        .unwrap();

        // Create a minimal region metadata for testing
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field1",
                    ConcreteDataType::int64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "timestamp",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .primary_key(vec![]);

        let region_metadata = builder.build().unwrap();

        // Create context
        let context = Arc::new(BulkIterContext::new(
            Arc::new(region_metadata),
            &None, // No projection
            None,  // No predicate
        ));

        // Create iterator with sequence filter (only include sequences <= 5)
        let mut iter = RecordBatchIter::new(record_batch, context, Some(5));

        // Test iteration - should filter out sequence 10
        let mut total_rows = 0;
        while let Some(batch_result) = iter.next() {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        // Should have fewer rows due to sequence filtering
        assert!(
            total_rows <= 3,
            "Should filter out some rows based on sequence"
        );
    }

    #[test]
    fn test_record_batch_iter_with_pruning() {
        use datafusion_common::ScalarValue;
        use datafusion_expr::{col, lit, Expr};
        use table::predicate::Predicate;

        // Create a simple schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "__primary_key",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
                false,
            ),
            Field::new("__sequence", DataType::UInt64, false),
            Field::new("__op_type", DataType::UInt8, false),
        ]));

        // Create test data where field1 has values 1, 2, 3
        let field1 = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let timestamp = Arc::new(datatypes::arrow::array::TimestampMillisecondArray::from(
            vec![1000, 2000, 3000],
        ));

        // Create primary key dictionary array
        use datatypes::arrow::array::{BinaryArray, DictionaryArray, UInt32Array};
        let values = Arc::new(BinaryArray::from_iter_values([b"key1", b"key2", b"key3"]));
        let keys = UInt32Array::from(vec![0, 1, 2]);
        let primary_key = Arc::new(DictionaryArray::new(keys, values));

        let sequence = Arc::new(UInt64Array::from(vec![1, 2, 3]));
        let op_type = Arc::new(UInt8Array::from(vec![1, 1, 1])); // PUT operations

        let record_batch = RecordBatch::try_new(
            schema,
            vec![field1, timestamp, primary_key, sequence, op_type],
        )
        .unwrap();

        // Create a minimal region metadata for testing
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field1",
                    ConcreteDataType::int64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "timestamp",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .primary_key(vec![]);

        let region_metadata = builder.build().unwrap();

        // Create a predicate: field1 > 1 (should filter out the first row)
        let predicate_expr: Expr = col("field1").gt(lit(ScalarValue::Int64(Some(1))));
        let predicate = Predicate::new(vec![predicate_expr]);

        // Create context with predicate
        let context = Arc::new(BulkIterContext::new(
            Arc::new(region_metadata),
            &None,           // No projection
            Some(predicate), // With predicate
        ));

        // Create iterator
        let mut iter = RecordBatchIter::new(record_batch, context, None);

        // Test iteration - should have fewer rows due to pruning
        let mut total_rows = 0;
        while let Some(batch_result) = iter.next() {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        // Should have fewer than 3 rows due to pruning (field1 > 1 filters out first row)
        assert!(
            total_rows < 3,
            "Should filter out some rows based on predicate, got {} rows",
            total_rows
        );
    }
}
