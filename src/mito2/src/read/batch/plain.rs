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

//! Plain Batch.

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::OpType;
use datafusion::physical_plan::sorts::sort::sort_batch;
use datatypes::arrow::array::{ArrayRef, BooleanArray, UInt64Array, UInt8Array};
use datatypes::arrow::compute::filter_record_batch;
use datatypes::arrow::record_batch::RecordBatch;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadata;
use store_api::storage::SequenceNumber;

use crate::error::{
    ComputeArrowSnafu, CreateDefaultSnafu, InvalidRequestSnafu, NewRecordBatchSnafu, Result,
    SortBatchSnafu, UnexpectedImpureDefaultSnafu,
};
use crate::read::merge_plain::sort_expressions;
use crate::read::BoxedPlainBatchStream;
use crate::sst::parquet::plain_format::PLAIN_FIXED_POS_COLUMN_NUM;
use crate::sst::to_plain_sst_arrow_schema;

// TODO(yingwen): Should we require the internal columns to be present?
// TODO(yingwen): Maybe use datafusion SendableRecordBatchStream directly.
/// [PlainBatch] represents a batch of rows.
/// It is a wrapper around [RecordBatch] that provides additional functionality for multi-series data.
/// The columns order is the same as the order of the columns read the SST.
/// It also contains the internal columns.
#[derive(Debug)]
pub struct PlainBatch {
    /// The original record batch.
    record_batch: RecordBatch,
}

impl PlainBatch {
    /// Creates a new [PlainBatch] from a [RecordBatch].
    pub fn new(record_batch: RecordBatch) -> Self {
        Self { record_batch }
    }

    /// Returns a new [PlainBatch] with the given columns.
    pub fn with_new_columns(&self, columns: Vec<ArrayRef>) -> Result<Self> {
        let record_batch = RecordBatch::try_new(self.record_batch.schema(), columns)
            .context(NewRecordBatchSnafu)?;
        Ok(Self { record_batch })
    }

    /// Returns the number of columns in the batch.
    pub fn num_columns(&self) -> usize {
        self.record_batch.num_columns()
    }

    /// Returns the number of rows in the batch.
    pub fn num_rows(&self) -> usize {
        self.record_batch.num_rows()
    }

    /// Returns true if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Returns all columns.
    pub fn columns(&self) -> &[ArrayRef] {
        &self.record_batch.columns()
    }

    /// Returns the array of column at index `idx`.
    pub fn column(&self, idx: usize) -> &ArrayRef {
        self.record_batch.column(idx)
    }

    /// Returns the slice of internal columns.
    pub fn internal_columns(&self) -> &[ArrayRef] {
        &self.record_batch.columns()[self.record_batch.num_columns() - PLAIN_FIXED_POS_COLUMN_NUM..]
    }

    /// Returns the inner record batch.
    pub(crate) fn as_record_batch(&self) -> &RecordBatch {
        &self.record_batch
    }

    /// Converts this batch into a record batch.
    pub fn into_record_batch(self) -> RecordBatch {
        self.record_batch
    }

    /// Filters this batch by the boolean array.
    pub(crate) fn filter(&self, predicate: &BooleanArray) -> Result<Self> {
        let record_batch =
            filter_record_batch(&self.record_batch, predicate).context(ComputeArrowSnafu)?;
        Ok(Self::new(record_batch))
    }

    /// Returns the column index of the sequence column.
    pub(crate) fn sequence_column_index(&self) -> usize {
        self.record_batch.num_columns() - PLAIN_FIXED_POS_COLUMN_NUM
    }
}

/// Fill default values for the batch.
/// Also adds internal columns.
/// Now it doesn't consider the op type.
pub(crate) fn fill_values(
    metadata: &RegionMetadata,
    record_batch: &RecordBatch,
    sequence: SequenceNumber,
) -> Result<RecordBatch> {
    // Column name to index in the `record_batch`.
    let name_to_index: HashMap<_, _> = record_batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| (field.name().clone(), i))
        .collect();
    let mut new_columns =
        Vec::with_capacity(record_batch.num_columns() + PLAIN_FIXED_POS_COLUMN_NUM);
    // Fills default values.
    // Implementation based on `WriteRequest::fill_missing_columns()`.
    for column in &metadata.column_metadatas {
        let array = match name_to_index.get(&column.column_schema.name) {
            Some(index) => record_batch.column(*index).clone(),
            None => {
                // For put requests, we use the default value from column schema.
                if column.column_schema.is_default_impure() {
                    return UnexpectedImpureDefaultSnafu {
                        region_id: metadata.region_id,
                        column: &column.column_schema.name,
                        default_value: format!("{:?}", column.column_schema.default_constraint()),
                    }
                    .fail();
                }
                let vector = column
                    .column_schema
                    .create_default_vector(record_batch.num_rows())
                    .context(CreateDefaultSnafu {
                        region_id: metadata.region_id,
                        column: &column.column_schema.name,
                    })?
                    // This column doesn't have default value.
                    .with_context(|| InvalidRequestSnafu {
                        region_id: metadata.region_id,
                        reason: format!(
                            "column {} does not have default value",
                            column.column_schema.name
                        ),
                    })?;
                vector.to_arrow_array()
            }
        };

        new_columns.push(array);
    }
    // Adds internal columns.
    // Adds the sequence number.
    let sequence_array = Arc::new(UInt64Array::from(vec![sequence; record_batch.num_rows()]));
    // Adds the op type.
    let op_type_array = Arc::new(UInt8Array::from(vec![
        OpType::Put as u8;
        record_batch.num_rows()
    ]));
    new_columns.push(sequence_array);
    new_columns.push(op_type_array);

    // TODO(yingwen): How to avoid creating a new schema instance?
    let schema = to_plain_sst_arrow_schema(metadata);
    RecordBatch::try_new(schema, new_columns).context(NewRecordBatchSnafu)
}

// TODO(yingwen): Sort multiple record batches.
/// Sorts the record batch in the following order:
/// (primary key ASC, time index ASC, sequence DESC)
///
/// The record batch must contains all columns.
fn sort_record_batch(metadata: &RegionMetadata, record_batch: &RecordBatch) -> Result<RecordBatch> {
    let sort_exprs = sort_expressions(metadata, None);
    sort_batch(&record_batch, &sort_exprs, None).context(SortBatchSnafu)
}

/// Builds a [BoxedPlainBatchStream] from a [RecordBatch].
/// It fills missing columns and sorts the record batch.
pub(crate) fn build_plain_batch_stream(
    metadata: &RegionMetadata,
    record_batch: RecordBatch,
    sequence: SequenceNumber,
) -> Result<BoxedPlainBatchStream> {
    let record_batch = fill_values(metadata, &record_batch, sequence)?;
    let record_batch = sort_record_batch(metadata, &record_batch)?;

    let stream = futures::stream::once(async move { Ok(PlainBatch::new(record_batch)) });
    let stream = Box::pin(stream);
    Ok(stream)
}
