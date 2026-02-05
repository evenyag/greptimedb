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

// Forked and adapted from DataFusion parquet row filter utilities.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;

use api::v1::SemanticType;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr::{PhysicalExpr, split_conjunction};
use datatypes::arrow::array::BooleanArray;
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datatypes::arrow::error::{ArrowError, Result as ArrowResult};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::DataType as _;
use datatypes::prelude::ConcreteDataType;
use mito_codec::row_converter::PrimaryKeyCodec;
use parquet::arrow::{FieldLevels, ProjectionMask};
use parquet::file::metadata::ParquetMetaData;
use snafu::ResultExt;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;

use crate::error::{DatafusionSnafu, Result};
use crate::sst::parquet::flat_format::{
    DecodedPrimaryKeys, decode_primary_keys, primary_key_column_index,
};
use crate::sst::parquet::format::ReadFormat;

pub(crate) struct RowFilter {
    pub(crate) predicate: std::sync::Mutex<MitoArrowPredicate>,
    pub(crate) field_levels: FieldLevels,
}

pub(crate) struct FilterExpr {
    pub(crate) expr: Arc<dyn PhysicalExpr>,
    pub(crate) schema: SchemaRef,
}

/// A "compiled" predicate passed to the parquet decoder to perform row-level filtering.
#[derive(Debug)]
pub(crate) struct MitoArrowPredicate {
    physical_expr: Arc<dyn PhysicalExpr>,
    projection_mask: ProjectionMask,
    filter_schema: SchemaRef,
    required_columns: Vec<ColumnRequirement>,
    codec: Arc<dyn PrimaryKeyCodec>,
    metadata: RegionMetadataRef,
}

impl MitoArrowPredicate {
    pub fn try_new(
        candidate: FilterCandidate,
        metadata: &ParquetMetaData,
        codec: Arc<dyn PrimaryKeyCodec>,
        region_metadata: RegionMetadataRef,
    ) -> Result<Self> {
        let physical_expr = reassign_expr_columns(candidate.expr, &candidate.filter_schema)
            .context(DatafusionSnafu)?;

        Ok(Self {
            physical_expr,
            projection_mask: ProjectionMask::roots(
                metadata.file_metadata().schema_descr(),
                candidate.projection,
            ),
            filter_schema: candidate.filter_schema,
            required_columns: candidate.required_columns,
            codec,
            metadata: region_metadata,
        })
    }

    pub fn projection(&self) -> &ProjectionMask {
        &self.projection_mask
    }
}

impl parquet::arrow::arrow_reader::ArrowPredicate for MitoArrowPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection_mask
    }

    fn evaluate(&mut self, batch: RecordBatch) -> ArrowResult<BooleanArray> {
        let mut decoded_pks: Option<DecodedPrimaryKeys> = None;
        let mut columns = Vec::with_capacity(self.required_columns.len());

        for req in &self.required_columns {
            match &req.source {
                ColumnSource::File(idx) => {
                    columns.push(batch.column(*idx).clone());
                }
                ColumnSource::TagFromPrimaryKey {
                    column_id,
                    pk_index,
                    data_type,
                } => {
                    if decoded_pks.is_none() {
                        decoded_pks = Some(
                            decode_primary_keys(self.codec.as_ref(), &batch).map_err(|e| {
                                ArrowError::ComputeError(format!(
                                    "Failed to decode primary key for row filter: {e}"
                                ))
                            })?,
                        );
                    }
                    let decoded = decoded_pks.as_ref().unwrap();
                    let tag_column = decoded
                        .get_tag_column(*column_id, *pk_index, data_type)
                        .map_err(|e| {
                            ArrowError::ComputeError(format!(
                                "Failed to build tag column for row filter: {e}"
                            ))
                        })?;
                    columns.push(tag_column);
                }
            }
        }

        let record_batch =
            RecordBatch::try_new(self.filter_schema.clone(), columns).map_err(|e| {
                ArrowError::ComputeError(format!("Failed to build RecordBatch for row filter: {e}"))
            })?;

        self.physical_expr
            .evaluate(&record_batch)
            .and_then(|v| v.into_array(record_batch.num_rows()))
            .and_then(|array| {
                let bool_arr = datafusion_common::cast::as_boolean_array(&array)?.clone();
                Ok(bool_arr)
            })
            .map_err(|e| {
                ArrowError::ComputeError(format!("Error evaluating row filter predicate: {e:?}"))
            })
    }
}

/// A candidate expression for creating a row filter.
pub(crate) struct FilterCandidate {
    expr: Arc<dyn PhysicalExpr>,
    required_bytes: usize,
    can_use_index: bool,
    projection: Vec<usize>,
    filter_schema: SchemaRef,
    required_columns: Vec<ColumnRequirement>,
}

struct FilterCandidateBuilder<'a> {
    expr: Arc<dyn PhysicalExpr>,
    expr_schema: SchemaRef,
    read_format: &'a ReadFormat,
    expected_metadata: Option<&'a RegionMetadataRef>,
}

impl<'a> FilterCandidateBuilder<'a> {
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        expr_schema: SchemaRef,
        read_format: &'a ReadFormat,
        expected_metadata: Option<&'a RegionMetadataRef>,
    ) -> Self {
        Self {
            expr,
            expr_schema,
            read_format,
            expected_metadata,
        }
    }

    pub fn build(
        self,
        parquet_meta: &ParquetMetaData,
        skip_fields: bool,
    ) -> Result<Option<FilterCandidate>> {
        let Some(required_columns) = pushdown_columns(&self.expr, &self.expr_schema)? else {
            return Ok(None);
        };

        let file_meta = self.read_format.metadata();
        let file_schema = self.read_format.arrow_schema();

        let mut requirements = Vec::with_capacity(required_columns.len());
        let mut projection_indices = BTreeSet::new();
        let mut needs_primary_key = false;

        for column_name in required_columns {
            let expected = self.expected_metadata.map(|m| m.as_ref());
            let Some(column_meta) = resolve_column_meta(&column_name, expected, file_meta) else {
                return Ok(None);
            };

            // Column is not in file metadata, cannot push down
            if file_meta.column_by_id(column_meta.column_id).is_none() {
                return Ok(None);
            }

            if skip_fields && column_meta.semantic_type == SemanticType::Field {
                return Ok(None);
            }

            let Some((source, maybe_projection_idx)) =
                resolve_column_source(&column_name, column_meta, file_schema, file_meta)?
            else {
                return Ok(None);
            };

            if let Some(idx) = maybe_projection_idx {
                projection_indices.insert(idx);
            }
            if matches!(source, ColumnSource::TagFromPrimaryKey { .. }) {
                needs_primary_key = true;
            }

            requirements.push(ColumnRequirement {
                name: column_name,
                data_type: column_meta.column_schema.data_type.clone(),
                source,
            });
        }

        if needs_primary_key {
            let pk_index = primary_key_column_index(file_schema.fields().len());
            projection_indices.insert(pk_index);
        }

        let required_bytes = size_of_columns(&projection_indices, parquet_meta)?;

        let filter_schema = build_filter_schema(&self.expr_schema, &requirements)?;

        Ok(Some(FilterCandidate {
            expr: self.expr,
            required_bytes,
            can_use_index: false,
            projection: projection_indices.into_iter().collect(),
            filter_schema,
            required_columns: requirements,
        }))
    }
}

#[derive(Debug, Clone)]
struct ColumnRequirement {
    name: String,
    data_type: ConcreteDataType,
    source: ColumnSource,
}

#[derive(Debug, Clone)]
enum ColumnSource {
    File(usize),
    TagFromPrimaryKey {
        column_id: ColumnId,
        pk_index: Option<usize>,
        data_type: ConcreteDataType,
    },
}

fn build_filter_schema(
    expr_schema: &SchemaRef,
    requirements: &[ColumnRequirement],
) -> Result<SchemaRef> {
    let mut fields = Vec::with_capacity(requirements.len());
    for req in requirements {
        let field = expr_schema
            .field_with_name(&req.name)
            .map(|f| f.as_ref().clone())
            .unwrap_or_else(|_| Field::new(&req.name, req.data_type.as_arrow_type(), true));
        fields.push(field);
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn resolve_column_meta<'a>(
    name: &str,
    expected: Option<&'a RegionMetadata>,
    file_meta: &'a RegionMetadata,
) -> Option<&'a ColumnMetadata> {
    expected
        .and_then(|m| m.column_by_name(name))
        .or_else(|| file_meta.column_by_name(name))
}

fn resolve_column_source(
    name: &str,
    column_meta: &ColumnMetadata,
    file_schema: &SchemaRef,
    file_meta: &RegionMetadata,
) -> Result<Option<(ColumnSource, Option<usize>)>> {
    if let Ok(idx) = file_schema.index_of(name) {
        return Ok(Some((ColumnSource::File(idx), Some(idx))));
    }

    if column_meta.semantic_type == SemanticType::Tag {
        let pk_index = if file_meta.primary_key_encoding == PrimaryKeyEncoding::Sparse {
            None
        } else {
            file_meta.primary_key_index(column_meta.column_id)
        };
        if file_meta.primary_key_index(column_meta.column_id).is_some() {
            return Ok(Some((
                ColumnSource::TagFromPrimaryKey {
                    column_id: column_meta.column_id,
                    pk_index,
                    data_type: column_meta.column_schema.data_type.clone(),
                },
                None,
            )));
        }
    }

    Ok(None)
}

// Checks if a given expression can be pushed down and returns required column names.
fn pushdown_columns(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<Option<Vec<String>>> {
    let mut checker = PushdownChecker::new(schema);
    expr.visit(&mut checker).context(DatafusionSnafu)?;
    Ok((!checker.prevents_pushdown()).then_some(
        checker
            .required_columns
            .into_iter()
            .map(|idx| checker.schema.field(idx).name().clone())
            .collect(),
    ))
}

struct PushdownChecker<'schema> {
    non_primitive_columns: bool,
    projected_columns: bool,
    required_columns: BTreeSet<usize>,
    schema: &'schema Schema,
}

impl<'schema> PushdownChecker<'schema> {
    fn new(schema: &'schema Schema) -> Self {
        Self {
            non_primitive_columns: false,
            projected_columns: false,
            required_columns: BTreeSet::default(),
            schema,
        }
    }

    fn check_single_column(&mut self, column_name: &str) -> Option<TreeNodeRecursion> {
        if let Ok(idx) = self.schema.index_of(column_name) {
            self.required_columns.insert(idx);
            if DataType::is_nested(self.schema.field(idx).data_type()) {
                self.non_primitive_columns = true;
                return Some(TreeNodeRecursion::Jump);
            }
        } else {
            self.projected_columns = true;
            return Some(TreeNodeRecursion::Jump);
        }
        None
    }

    fn prevents_pushdown(&self) -> bool {
        self.non_primitive_columns || self.projected_columns
    }
}

impl TreeNodeVisitor<'_> for PushdownChecker<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(&mut self, node: &Self::Node) -> datafusion_common::Result<TreeNodeRecursion> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            if let Some(recursion) = self.check_single_column(column.name()) {
                return Ok(recursion);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

fn size_of_columns(indices: &BTreeSet<usize>, metadata: &ParquetMetaData) -> Result<usize> {
    let mut total_size = 0;
    let row_groups = metadata.row_groups();
    for idx in indices {
        for rg in row_groups.iter() {
            total_size += rg.column(*idx).compressed_size() as usize;
        }
    }
    Ok(total_size)
}

/// Build row filters from predicate expressions if possible.
pub(crate) fn build_row_filters(
    exprs: &[FilterExpr],
    read_format: &ReadFormat,
    expected_metadata: Option<&RegionMetadataRef>,
    parquet_meta: &ParquetMetaData,
    codec: Arc<dyn PrimaryKeyCodec>,
    skip_fields: bool,
    reorder_predicates: bool,
) -> Result<Vec<MitoArrowPredicate>> {
    if exprs.is_empty() {
        return Ok(Vec::new());
    }

    let mut predicates = Vec::new();
    for filter_expr in exprs {
        let expr_predicates = split_conjunction(&filter_expr.expr);
        predicates.extend(expr_predicates.into_iter().map(|expr| FilterExpr {
            expr: Arc::clone(&expr),
            schema: filter_expr.schema.clone(),
        }));
    }

    let mut candidates: Vec<FilterCandidate> = predicates
        .into_iter()
        .map(|expr| {
            FilterCandidateBuilder::new(expr.expr, expr.schema, read_format, expected_metadata)
                .build(parquet_meta, skip_fields)
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();

    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    if reorder_predicates {
        candidates.sort_unstable_by(|c1, c2| match c1.can_use_index.cmp(&c2.can_use_index) {
            Ordering::Equal => c1.required_bytes.cmp(&c2.required_bytes),
            ord => ord,
        });
    }

    let region_metadata = read_format.metadata().clone();
    candidates
        .into_iter()
        .map(|candidate| {
            MitoArrowPredicate::try_new(
                candidate,
                parquet_meta,
                codec.clone(),
                region_metadata.clone(),
            )
        })
        .collect()
}
