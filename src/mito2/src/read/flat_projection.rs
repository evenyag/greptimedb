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

//! Utilities for projection on flat format.

use std::sync::Arc;

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_recordbatch::error::{ArrowComputeSnafu, ExternalSnafu};
use common_recordbatch::{DfRecordBatch, RecordBatch};
use datatypes::arrow::datatypes::Field;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::schema::{Schema, SchemaRef};
use datatypes::vectors::{Helper, VectorRef};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;

use crate::error::{InvalidRequestSnafu, RecordBatchSnafu, Result};
use crate::sst::parquet::flat_format::sst_column_id_indices;
use crate::sst::parquet::format::FormatProjection;
use crate::sst::{internal_fields, tag_maybe_to_dictionary_field};

/// Returns ids and datatypes of columns of the output batch after applying the `projection`.
///
/// It adds the time index column if it doesn't present in the projection.
pub(crate) fn flat_projected_columns(
    metadata: &RegionMetadata,
    format_projection: &FormatProjection,
) -> Vec<(ColumnId, ConcreteDataType)> {
    let time_index = metadata.time_index_column();
    let num_columns = if format_projection
        .column_id_to_projected_index
        .contains_key(&time_index.column_id)
    {
        format_projection.column_id_to_projected_index.len()
    } else {
        format_projection.column_id_to_projected_index.len() + 1
    };
    let mut schema = vec![None; num_columns];
    for (column_id, index) in &format_projection.column_id_to_projected_index {
        // Safety: FormatProjection ensures the id is valid.
        schema[*index] = Some((
            *column_id,
            metadata
                .column_by_id(*column_id)
                .unwrap()
                .column_schema
                .data_type
                .clone(),
        ));
    }
    if num_columns != format_projection.column_id_to_projected_index.len() {
        schema[num_columns - 1] = Some((
            time_index.column_id,
            time_index.column_schema.data_type.clone(),
        ));
    }

    // Safety: FormatProjection ensures all indices can be unwrapped.
    schema.into_iter().map(|id_type| id_type.unwrap()).collect()
}

/// Computes the Arrow schema for input batches.
///
/// # Panics
/// Panics if it can't find the column by the column id in the batch_schema.
pub(crate) fn compute_input_arrow_schema(
    metadata: &RegionMetadata,
    batch_schema: &[(ColumnId, ConcreteDataType)],
) -> datatypes::arrow::datatypes::SchemaRef {
    let mut new_fields = Vec::with_capacity(batch_schema.len() + 3);
    for (column_id, _) in batch_schema {
        let column_metadata = metadata.column_by_id(*column_id).unwrap();
        let field = Arc::new(Field::new(
            &column_metadata.column_schema.name,
            column_metadata.column_schema.data_type.as_arrow_type(),
            column_metadata.column_schema.is_nullable(),
        ));
        let field = if column_metadata.semantic_type == SemanticType::Tag {
            tag_maybe_to_dictionary_field(&column_metadata.column_schema.data_type, &field)
        } else {
            field
        };
        new_fields.push(field);
    }
    new_fields.extend_from_slice(&internal_fields());

    Arc::new(datatypes::arrow::datatypes::Schema::new(new_fields))
}

/// Projects columns from a flat-format input batch and converts them into vectors.
pub(crate) fn project_flat_vectors(
    batch: &datatypes::arrow::record_batch::RecordBatch,
    batch_indices: &[usize],
    output_columns: usize,
) -> common_recordbatch::error::Result<Vec<VectorRef>> {
    let mut columns = Vec::with_capacity(output_columns);
    for index in batch_indices {
        let mut array = batch.column(*index).clone();
        // Casts dictionary values to the target type.
        if let datatypes::arrow::datatypes::DataType::Dictionary(_key_type, value_type) =
            array.data_type()
        {
            let casted = datatypes::arrow::compute::cast(&array, value_type).context(ArrowComputeSnafu)?;
            array = casted;
        }
        let vector = Helper::try_into_vector(array)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        columns.push(vector);
    }
    Ok(columns)
}

/// Helper to project compaction batches into flat format columns
/// (fields + time index + __primary_key + __sequence + __op_type).
pub(crate) struct CompactionProjectionMapper {
    batch_indices: Vec<usize>,
    assembler: DfBatchAssembler,
}

impl CompactionProjectionMapper {
    pub(crate) fn try_new(metadata: &RegionMetadataRef) -> Result<Self> {
        let projection = metadata
            .column_metadatas
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if matches!(col.semantic_type, SemanticType::Field) {
                    Some(idx)
                } else {
                    None
                }
            })
            .chain([metadata.time_index_column_pos()])
            .collect::<Vec<_>>();

        let read_column_ids = metadata
            .column_metadatas
            .iter()
            .map(|col| col.column_id)
            .collect::<Vec<_>>();

        let mut output_column_ids = Vec::with_capacity(projection.len());
        let mut output_column_schemas = Vec::with_capacity(projection.len());
        for idx in &projection {
            let column = metadata
                .column_metadatas
                .get(*idx)
                .with_context(|| InvalidRequestSnafu {
                    region_id: metadata.region_id,
                    reason: format!("projection index {} is out of bound", idx),
                })?;
            output_column_ids.push(column.column_id);
            output_column_schemas.push(metadata.schema.column_schemas()[*idx].clone());
        }

        let id_to_index = sst_column_id_indices(metadata);
        let format_projection = FormatProjection::compute_format_projection(
            &id_to_index,
            metadata.column_metadatas.len() + 3,
            read_column_ids.iter().copied(),
        );

        let batch_indices = output_column_ids
            .iter()
            .map(|id| {
                format_projection
                    .column_id_to_projected_index
                    .get(id)
                    .copied()
                    .with_context(|| {
                        let name = metadata
                            .column_by_id(*id)
                            .map(|column| column.column_schema.name.clone())
                            .unwrap_or_else(|| id.to_string());
                        InvalidRequestSnafu {
                            region_id: metadata.region_id,
                            reason: format!("output column {} is missing in read projection", name),
                        }
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        let output_schema = Arc::new(Schema::new(output_column_schemas));
        let assembler = DfBatchAssembler::new(output_schema);

        Ok(Self {
            batch_indices,
            assembler,
        })
    }

    /// Projects columns and appends internal columns for compaction output.
    ///
    /// The input batch is expected to be in flat format with internal columns appended.
    pub(crate) fn project(&self, batch: DfRecordBatch) -> Result<DfRecordBatch> {
        let columns = project_flat_vectors(
            &batch,
            &self.batch_indices,
            self.assembler.output_columns_without_internal(),
        )
        .context(RecordBatchSnafu)?;
        self.assembler
            .build_df_record_batch_with_internal(&batch, columns)
            .context(RecordBatchSnafu)
    }
}

/// Builds [DfRecordBatch] with internal columns appended.
pub(crate) struct DfBatchAssembler {
    output_columns_without_internal: usize,
    output_arrow_schema_with_internal: datatypes::arrow::datatypes::SchemaRef,
}

impl DfBatchAssembler {
    /// Precomputes the output schema with internal columns.
    pub(crate) fn new(output_schema: SchemaRef) -> Self {
        let output_columns_without_internal = output_schema.num_columns();
        let fields = output_schema
            .arrow_schema()
            .fields()
            .into_iter()
            .chain(internal_fields().iter())
            .cloned()
            .collect::<Vec<_>>();
        let output_arrow_schema_with_internal =
            Arc::new(datatypes::arrow::datatypes::Schema::new(fields));
        Self {
            output_columns_without_internal,
            output_arrow_schema_with_internal,
        }
    }

    fn output_columns_without_internal(&self) -> usize {
        self.output_columns_without_internal
    }

    /// Builds a [DfRecordBatch] from projected vectors plus internal columns.
    ///
    /// Assumes the input batch already contains internal columns as the last three fields
    /// ("__primary_key", "__sequence", "__op_type").
    pub(crate) fn build_df_record_batch_with_internal(
        &self,
        batch: &datatypes::arrow::record_batch::RecordBatch,
        mut columns: Vec<VectorRef>,
    ) -> common_recordbatch::error::Result<DfRecordBatch> {
        let num_columns = batch.columns().len();
        // The last 3 columns are the internal columns.
        let internal_indices = [num_columns - 3, num_columns - 2, num_columns - 1];
        for index in internal_indices.iter() {
            let array = batch.column(*index).clone();
            let vector = Helper::try_into_vector(array)
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            columns.push(vector);
        }
        RecordBatch::to_df_record_batch(self.output_arrow_schema_with_internal.clone(), columns)
    }
}
