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

//! Multi Series Batch.

use std::collections::HashMap;
use std::sync::Arc;

use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, DictionaryArray, UInt32Array,
};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::compute::filter_record_batch;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::vectors::{MutableVector, VectorRef};
use snafu::{OptionExt, ResultExt};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadata;
use store_api::storage::ColumnId;

use crate::error::{ComputeArrowSnafu, CreateVectorSnafu, InvalidRecordBatchSnafu, Result};
use crate::row_converter::{CompositeValues, PrimaryKeyCodec, SparseValues};
use crate::sst::parquet::format::PrimaryKeyArray;

/// [MultiSeries] represents a batch of data with multiple series.
/// It is a wrapper around [RecordBatch] that provides additional functionality for multi-series data.
/// The columns order is the same as the order of the columns in the SST.
///
/// The schema of a SST is:
/// ```text
/// field 0, field 1, ..., field N, time index, primary key, sequence, op type
/// ```
#[derive(Debug)]
pub struct MultiSeries {
    /// The original record batch.
    record_batch: RecordBatch,
    /// Cached decoded composite values list.
    composite_values: Option<CompositeValuesList>,
    /// Cached decoded primary key values vec.
    pk_values_vec: Option<Vec<CompositeValues>>,
}

impl MultiSeries {
    /// Creates a new [MultiSeries] from a [RecordBatch].
    pub fn new(record_batch: RecordBatch) -> Self {
        Self {
            record_batch,
            composite_values: None,
            pk_values_vec: None,
        }
    }

    /// Returns the number of rows in the batch.
    pub fn num_rows(&self) -> usize {
        self.record_batch.num_rows()
    }

    /// Returns true if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Decodes the primary key part of the batch.
    pub(crate) fn decode_primary_key_array(
        &self,
        metadata: &RegionMetadata,
        codec: &dyn PrimaryKeyCodec,
    ) -> Result<CompositeValuesList> {
        let array = self.primary_key_array()?;
        decode_primary_key_array(array, metadata, codec)
    }

    /// Decodes the primary key part of the batch to a vec of [CompositeValues].
    pub(crate) fn decode_primary_key_to_vec(
        &self,
        codec: &dyn PrimaryKeyCodec,
    ) -> Result<Vec<CompositeValues>> {
        let array = self.primary_key_array()?;
        decode_primary_key_to_vec(array, codec)
    }

    /// Sets the decoded composite values.
    pub(crate) fn set_composite_values(&mut self, composite_values: CompositeValuesList) {
        self.composite_values = Some(composite_values);
    }

    /// Gets the decoded composite values.
    pub(crate) fn composite_values(&self) -> Option<&CompositeValuesList> {
        self.composite_values.as_ref()
    }

    /// Sets the decoded primary key values vec.
    pub(crate) fn set_pk_values_vec(&mut self, pk_values_vec: Vec<CompositeValues>) {
        self.pk_values_vec = Some(pk_values_vec);
    }

    /// Gets the decoded primary key values vec.
    pub(crate) fn pk_values_vec(&self) -> Option<&Vec<CompositeValues>> {
        self.pk_values_vec.as_ref()
    }

    /// Gets the field column by the field index.
    pub(crate) fn field_column(&self, field_index: usize) -> &ArrayRef {
        self.record_batch.column(field_index)
    }

    /// Gets the timestamp column.
    pub(crate) fn timestamp_column(&self) -> &ArrayRef {
        self.record_batch
            .column(self.record_batch.num_columns() - 4)
    }

    /// Gets the tag column as a dictionary array.
    pub(crate) fn tag_column(&self, values: &VectorRef) -> Result<ArrayRef> {
        let dict_array = self.primary_key_array()?;
        let keys = dict_array
            .keys()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!(
                    "keys of primary key array should not be {:?}",
                    dict_array.values().data_type()
                ),
            })?;
        let values = values.to_arrow_array();

        let array = DictionaryArray::try_new(keys.clone(), values).context(ComputeArrowSnafu)?;
        Ok(Arc::new(array))
    }

    /// Filters this batch by the boolean array.
    pub(crate) fn filter(&self, predicate: &BooleanArray) -> Result<Self> {
        let record_batch =
            filter_record_batch(&self.record_batch, predicate).context(ComputeArrowSnafu)?;
        // We don't inherit the cached values as some of them may be filtered out.
        Ok(Self::new(record_batch))
    }

    /// Replaces the primary key array with the given array.
    /// It won't touch the cached values.
    pub(crate) fn replace_primary_key_array(&mut self, values: BinaryArray) -> Result<()> {
        let dict_array = self.primary_key_array()?;
        let keys = dict_array
            .keys()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!(
                    "keys of primary key array should not be {:?}",
                    dict_array.values().data_type()
                ),
            })?;

        let new_array =
            DictionaryArray::try_new(keys.clone(), Arc::new(values)).context(ComputeArrowSnafu)?;
        let mut columns = self.record_batch.columns().to_vec();
        columns[self.record_batch.num_columns() - 3] = Arc::new(new_array);
        let record_batch =
            RecordBatch::try_new(self.record_batch.schema(), columns).context(ComputeArrowSnafu)?;

        self.record_batch = record_batch;
        Ok(())
    }

    /// Returns a builder for the primary key array.
    pub(crate) fn primary_key_values_builder(&self) -> Result<BinaryBuilder> {
        let dict_array = self.primary_key_array()?;
        let encoded_pks = dict_array
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!(
                    "values of primary key array should not be {:?}",
                    dict_array.values().data_type()
                ),
            })?;

        Ok(BinaryBuilder::with_capacity(
            dict_array.keys().len(),
            encoded_pks.values().len(),
        ))
    }

    /// Returns the encoded primary key array.
    fn primary_key_array(&self) -> Result<&PrimaryKeyArray> {
        let array = self
            .record_batch
            .column(self.record_batch.num_columns() - 3);
        array
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!("primary key array should not be {:?}", array.data_type()),
            })
    }
}

fn decode_primary_key_array(
    dict_array: &PrimaryKeyArray,
    metadata: &RegionMetadata,
    codec: &dyn PrimaryKeyCodec,
) -> Result<CompositeValuesList> {
    let encoded_pks = dict_array
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .with_context(|| InvalidRecordBatchSnafu {
            reason: format!(
                "values of primary key array should not be {:?}",
                dict_array.values().data_type()
            ),
        })?;
    let mut values_builder =
        CompositeValuesListBuilder::new(codec.encoding(), metadata, encoded_pks.len());
    for encoded_pk in encoded_pks.iter() {
        let encoded_pk = encoded_pk.context(InvalidRecordBatchSnafu {
            reason: "primary key array should not contain null values",
        })?;
        let pk_values = codec.decode(encoded_pk)?;
        values_builder.try_push(pk_values)?;
    }

    values_builder.build()
}

fn decode_primary_key_to_vec(
    dict_array: &PrimaryKeyArray,
    codec: &dyn PrimaryKeyCodec,
) -> Result<Vec<CompositeValues>> {
    let encoded_pks = dict_array
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .with_context(|| InvalidRecordBatchSnafu {
            reason: format!(
                "values of primary key array should not be {:?}",
                dict_array.values().data_type()
            ),
        })?;
    let mut values = Vec::with_capacity(encoded_pks.len());
    for encoded_pk in encoded_pks.iter() {
        let encoded_pk = encoded_pk.context(InvalidRecordBatchSnafu {
            reason: "primary key array should not contain null values",
        })?;
        let pk_values = codec.decode(encoded_pk)?;
        values.push(pk_values);
    }

    Ok(values)
}

/// A list of composite values.
#[derive(Debug)]
pub(crate) enum CompositeValuesList {
    Dense(HashMap<ColumnId, VectorRef>),
    Sparse(Vec<SparseValues>),
}

impl CompositeValuesList {
    /// Returns the value vector.
    ///
    /// # Panics
    /// Panic if the column id is not found in the list.
    pub(crate) fn value_vector(
        &self,
        column_id: ColumnId,
        data_type: &ConcreteDataType,
    ) -> Result<VectorRef> {
        let vector = match self {
            CompositeValuesList::Dense(vectors) => {
                // Column not found.
                let vector = vectors.get(&column_id).unwrap();
                vector.clone()
            }
            CompositeValuesList::Sparse(values) => {
                let mut builder = data_type.create_mutable_vector(values.len());
                for sparse_values in values {
                    let v = sparse_values.get_or_null(column_id);
                    builder
                        .try_push_value_ref(v.as_value_ref())
                        .context(CreateVectorSnafu)?;
                }
                builder.to_vector()
            }
        };

        Ok(vector)
    }
}

enum CompositeValuesListBuilder {
    Dense(Vec<(ColumnId, Box<dyn MutableVector>)>),
    Sparse(Vec<SparseValues>),
}

impl CompositeValuesListBuilder {
    fn new(encoding: PrimaryKeyEncoding, metadata: &RegionMetadata, capacity: usize) -> Self {
        match encoding {
            PrimaryKeyEncoding::Dense => {
                let values_builder = metadata
                    .primary_key_columns()
                    .map(|col| {
                        let builder = col.column_schema.data_type.create_mutable_vector(capacity);
                        (col.column_id, builder)
                    })
                    .collect();

                Self::Dense(values_builder)
            }
            PrimaryKeyEncoding::Sparse => Self::Sparse(Vec::with_capacity(capacity)),
        }
    }

    fn try_push(&mut self, values: CompositeValues) -> Result<()> {
        match (self, values) {
            (Self::Dense(values_builder), CompositeValues::Dense(column_values)) => {
                assert_eq!(column_values.len(), values_builder.len());
                for (idx, (column_id, value)) in column_values.into_iter().enumerate() {
                    assert_eq!(column_id, values_builder[idx].0);
                    let builder = &mut values_builder[idx].1;
                    builder
                        .try_push_value_ref(value.as_value_ref())
                        .context(CreateVectorSnafu)?;
                }
            }
            (Self::Sparse(values_list), CompositeValues::Sparse(values)) => {
                values_list.push(values);
            }
            _ => InvalidRecordBatchSnafu {
                reason: "mismatched composite values",
            }
            .fail()?,
        }
        Ok(())
    }

    fn build(self) -> Result<CompositeValuesList> {
        match self {
            Self::Dense(values_builder) => {
                let vectors = values_builder
                    .into_iter()
                    .map(|(column_id, mut builder)| {
                        let vector = builder.to_vector();
                        (column_id, vector)
                    })
                    .collect();
                Ok(CompositeValuesList::Dense(vectors))
            }
            Self::Sparse(values_list) => Ok(CompositeValuesList::Sparse(values_list)),
        }
    }
}
