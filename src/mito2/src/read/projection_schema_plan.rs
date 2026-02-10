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

//! Unified projection and schema computation for scan and compaction.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_recordbatch::RecordBatch;
use common_recordbatch::error::ExternalSnafu;
use datafusion_expr::Expr;
use datafusion_expr::utils::expr_to_columns;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::schema::{Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec, build_primary_key_codec};
use snafu::{OptionExt, ResultExt};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use tracing::warn;

use crate::cache::CacheStrategy;
use crate::error::{InvalidRequestSnafu, Result, UnexpectedSnafu};
use crate::read::Batch;
use crate::read::compat::{self, FlatCompatBatch, PrimaryKeyCompatBatch};
use crate::read::flat_projection::{
    CompactionProjectionMapper, compute_input_arrow_schema, flat_projected_columns,
    project_flat_vectors,
};
use crate::read::scan_region::PredicateGroup;
use crate::sst::parquet::flat_format::sst_column_id_indices;
use crate::sst::parquet::format::{FormatProjection, ReadFormat, StatValues};
use crate::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};

/// Only cache vector when its length `<=` this value.
const MAX_VECTOR_LENGTH_TO_CACHE: usize = 16384;

/// Projection + schema computation plan built from request + expected metadata.
pub(crate) struct ProjectionSchemaPlan {
    expected_meta: RegionMetadataRef,
    flat_format: bool,
    projection: Option<Vec<usize>>,
    read_column_ids: Vec<ColumnId>,
    output_schema: SchemaRef,
    is_empty_projection: bool,
    exec: ProjectionExec,
}

enum ProjectionExec {
    PrimaryKey(PrimaryKeyProjectionExec),
    Flat(FlatProjectionExec),
}

struct PrimaryKeyProjectionExec {
    /// Maps column in [RecordBatch] to index in [Batch].
    batch_indices: Vec<BatchIndex>,
    /// Output record batch contains tags.
    has_tags: bool,
    /// Decoder for primary key.
    codec: Arc<dyn PrimaryKeyCodec>,
    /// Ids and DataTypes of field columns in the read [Batch].
    batch_fields: Vec<(ColumnId, ConcreteDataType)>,
}

struct FlatProjectionExec {
    /// Ids and DataTypes of columns of the expected batch.
    ///
    /// It doesn't contain internal columns but always contains the time index column.
    batch_schema: Vec<(ColumnId, ConcreteDataType)>,
    /// The index in flat format [RecordBatch] for each column in the output [RecordBatch].
    batch_indices: Vec<usize>,
    /// Precomputed Arrow schema for input batches.
    input_arrow_schema: datatypes::arrow::datatypes::SchemaRef,
}

/// Index of a vector in a [Batch].
#[derive(Debug, Clone, Copy)]
enum BatchIndex {
    /// Index in primary keys.
    Tag((usize, ColumnId)),
    /// The time index column.
    Timestamp,
    /// Index in fields.
    Field(usize),
}

pub(crate) enum FileCompatBatch {
    PrimaryKey(PrimaryKeyCompatBatch),
    Flat(FlatCompatBatch),
}

impl ProjectionSchemaPlan {
    /// Builds a plan from request projection + filters using the expected metadata.
    pub(crate) fn new_from_request(
        expected_meta: RegionMetadataRef,
        flat_format: bool,
        projection: Option<Vec<usize>>,
        filters: &[Expr],
        predicate: &PredicateGroup,
    ) -> Result<Self> {
        let read_column_ids = match &projection {
            Some(p) => compute_read_column_ids(&expected_meta, p, filters, predicate)?,
            None => expected_meta
                .column_metadatas
                .iter()
                .map(|col| col.column_id)
                .collect(),
        };
        let projection_indices = projection
            .clone()
            .unwrap_or_else(|| (0..expected_meta.column_metadatas.len()).collect());

        Self::new_impl(
            expected_meta,
            flat_format,
            projection,
            projection_indices,
            read_column_ids,
        )
    }

    /// Builds a plan that reads all columns.
    pub(crate) fn new_all(expected_meta: RegionMetadataRef, flat_format: bool) -> Result<Self> {
        let projection_indices = (0..expected_meta.column_metadatas.len()).collect::<Vec<_>>();
        let read_column_ids = expected_meta
            .column_metadatas
            .iter()
            .map(|col| col.column_id)
            .collect();
        Self::new_impl(
            expected_meta,
            flat_format,
            None,
            projection_indices,
            read_column_ids,
        )
    }

    fn new_impl(
        expected_meta: RegionMetadataRef,
        flat_format: bool,
        projection: Option<Vec<usize>>,
        projection_indices: Vec<usize>,
        read_column_ids: Vec<ColumnId>,
    ) -> Result<Self> {
        let is_empty_projection = projection_indices.is_empty();
        let output_schema = build_output_schema(&expected_meta, &projection_indices)?;

        let exec = if flat_format {
            ProjectionExec::Flat(build_flat_exec(
                &expected_meta,
                &projection_indices,
                &read_column_ids,
                is_empty_projection,
            )?)
        } else {
            ProjectionExec::PrimaryKey(build_primary_key_exec(
                &expected_meta,
                &projection_indices,
                &read_column_ids,
                is_empty_projection,
            )?)
        };

        Ok(Self {
            expected_meta,
            flat_format,
            projection,
            read_column_ids,
            output_schema,
            is_empty_projection,
            exec,
        })
    }

    /// Returns expected metadata used by this plan.
    pub(crate) fn expected_metadata(&self) -> &RegionMetadataRef {
        &self.expected_meta
    }

    /// Returns the projection indices requested by the user, if any.
    pub(crate) fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    /// Returns ids of columns to read from memtables and SSTs.
    pub(crate) fn read_column_ids(&self) -> &[ColumnId] {
        &self.read_column_ids
    }

    /// Returns the output schema for the final projected batch.
    pub(crate) fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    pub(crate) fn has_tags(&self) -> bool {
        match &self.exec {
            ProjectionExec::PrimaryKey(exec) => exec.has_tags,
            ProjectionExec::Flat(_) => false,
        }
    }

    pub(crate) fn empty_record_batch(&self) -> RecordBatch {
        RecordBatch::new_empty(self.output_schema.clone())
    }

    pub(crate) fn convert_primary_key_batch(
        &self,
        batch: &Batch,
        cache_strategy: &CacheStrategy,
    ) -> common_recordbatch::error::Result<RecordBatch> {
        let exec = match &self.exec {
            ProjectionExec::PrimaryKey(exec) => exec,
            ProjectionExec::Flat(_) => panic!("Primary key mapper required"),
        };

        if self.is_empty_projection {
            return RecordBatch::new_with_count(self.output_schema.clone(), batch.num_rows());
        }

        debug_assert_eq!(exec.batch_fields.len(), batch.fields().len());
        debug_assert!(
            exec.batch_fields
                .iter()
                .zip(batch.fields())
                .all(|((id, _), batch_col)| *id == batch_col.column_id)
        );

        // Skips decoding pk if we don't need to output it.
        let pk_values = if exec.has_tags {
            match batch.pk_values() {
                Some(v) => v.clone(),
                None => exec
                    .codec
                    .decode(batch.primary_key())
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?,
            }
        } else {
            CompositeValues::Dense(vec![])
        };

        let mut columns = Vec::with_capacity(self.output_schema.num_columns());
        let num_rows = batch.num_rows();
        for (index, column_schema) in exec
            .batch_indices
            .iter()
            .zip(self.output_schema.column_schemas())
        {
            match index {
                BatchIndex::Tag((idx, column_id)) => {
                    let value = match &pk_values {
                        CompositeValues::Dense(v) => &v[*idx].1,
                        CompositeValues::Sparse(v) => v.get_or_null(*column_id),
                    };
                    let vector = repeated_vector_with_cache(
                        &column_schema.data_type,
                        value,
                        num_rows,
                        cache_strategy,
                    )?;
                    columns.push(vector);
                }
                BatchIndex::Timestamp => {
                    columns.push(batch.timestamps().clone());
                }
                BatchIndex::Field(idx) => {
                    columns.push(batch.fields()[*idx].data.clone());
                }
            }
        }

        RecordBatch::new(self.output_schema.clone(), columns)
    }

    pub(crate) fn convert_flat_batch(
        &self,
        batch: &datatypes::arrow::record_batch::RecordBatch,
    ) -> common_recordbatch::error::Result<RecordBatch> {
        let exec = match &self.exec {
            ProjectionExec::PrimaryKey(_) => panic!("Flat mapper required"),
            ProjectionExec::Flat(exec) => exec,
        };

        if self.is_empty_projection {
            return RecordBatch::new_with_count(self.output_schema.clone(), batch.num_rows());
        }

        let columns = project_flat_vectors(batch, &exec.batch_indices, self.output_schema.num_columns())?;
        RecordBatch::new(self.output_schema.clone(), columns)
    }

    pub(crate) fn flat_input_arrow_schema(
        &self,
        compaction: bool,
    ) -> datatypes::arrow::datatypes::SchemaRef {
        let exec = match &self.exec {
            ProjectionExec::PrimaryKey(_) => panic!("Flat mapper required"),
            ProjectionExec::Flat(exec) => exec,
        };

        if !compaction {
            exec.input_arrow_schema.clone()
        } else {
            // For compaction, we need to build a different schema from encoding.
            to_flat_sst_arrow_schema(
                &self.expected_meta,
                &FlatSchemaOptions::from_encoding(self.expected_meta.primary_key_encoding),
            )
        }
    }

    pub(crate) fn flat_field_column_start(&self) -> usize {
        let exec = match &self.exec {
            ProjectionExec::PrimaryKey(_) => panic!("Flat mapper required"),
            ProjectionExec::Flat(exec) => exec,
        };

        for (idx, column_id) in exec
            .batch_schema
            .iter()
            .map(|(column_id, _)| column_id)
            .enumerate()
        {
            // Safety: We get the column id from the metadata in new().
            if self.expected_meta.column_by_id(*column_id).unwrap().semantic_type == SemanticType::Field {
                return idx;
            }
        }

        exec.batch_schema.len()
    }

    pub(crate) fn ensure_primary_key_format(&self) -> Result<()> {
        if matches!(self.exec, ProjectionExec::PrimaryKey(_)) {
            Ok(())
        } else {
            UnexpectedSnafu {
                reason: "Unexpected format",
            }
            .fail()
        }
    }

    /// Builds a read format for a specific file metadata.
    pub(crate) fn build_read_format(
        &self,
        file_meta: RegionMetadataRef,
        parquet_column_num: Option<usize>,
        file_path: &str,
        skip_auto_convert: bool,
    ) -> Result<ReadFormat> {
        ReadFormat::new(
            file_meta,
            Some(&self.read_column_ids),
            self.flat_format,
            parquet_column_num,
            file_path,
            skip_auto_convert,
        )
    }

    /// Computes compat batch for a read format and current plan.
    fn compat_for_read_format(
        &self,
        read_format: &ReadFormat,
        compaction: bool,
    ) -> Result<Option<FileCompatBatch>> {
        let need_compat = !compat::has_same_columns_and_pk_encoding(
            self.expected_meta.as_ref(),
            read_format.metadata(),
        );
        if !need_compat {
            return Ok(None);
        }

        let compat = if let Some(flat_format) = read_format.as_flat() {
            let flat_exec = match &self.exec {
                ProjectionExec::PrimaryKey(_) => panic!("flat format mapper missing"),
                ProjectionExec::Flat(exec) => exec,
            };
            FlatCompatBatch::try_new(
                &self.expected_meta,
                &flat_exec.batch_schema,
                flat_format.metadata(),
                flat_format.format_projection(),
                compaction,
            )?
            .map(FileCompatBatch::Flat)
        } else {
            let pk_exec = match &self.exec {
                ProjectionExec::PrimaryKey(exec) => exec,
                ProjectionExec::Flat(_) => panic!("primary key mapper missing"),
            };
            let compact_batch = PrimaryKeyCompatBatch::new_with_projection(
                &self.expected_meta,
                &self.read_column_ids,
                &pk_exec.batch_fields,
                read_format.metadata().clone(),
            )?;
            Some(FileCompatBatch::PrimaryKey(compact_batch))
        };

        Ok(compat)
    }

    /// Computes compaction projection mapper for a file.
    fn compaction_projection_mapper(
        &self,
        file_meta: &RegionMetadataRef,
        compaction: bool,
        is_same_region_partition: bool,
    ) -> Result<Option<CompactionProjectionMapper>> {
        if !compaction
            || is_same_region_partition
            || !self.flat_format
            || file_meta.primary_key_encoding != PrimaryKeyEncoding::Sparse
        {
            return Ok(None);
        }

        Ok(Some(CompactionProjectionMapper::try_new(file_meta)?))
    }

    /// Builds a per-file projection/schema view.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn for_file(
        self: &Arc<Self>,
        file_meta: RegionMetadataRef,
        parquet_column_num: Option<usize>,
        file_path: &str,
        skip_auto_convert: bool,
        compaction: bool,
        is_same_region_partition: bool,
        decode_primary_key_values: bool,
        override_sequence: Option<u64>,
    ) -> Result<FileProjectionSchema> {
        let mut read_format = self.build_read_format(
            file_meta.clone(),
            parquet_column_num,
            file_path,
            skip_auto_convert,
        )?;
        if decode_primary_key_values {
            read_format.set_decode_primary_key_values(true);
        }
        if let Some(sequence) = override_sequence {
            read_format.set_override_sequence(Some(sequence));
        }

        let compat = self.compat_for_read_format(&read_format, compaction)?;
        let compaction_projection =
            self.compaction_projection_mapper(&file_meta, compaction, is_same_region_partition)?;

        Ok(FileProjectionSchema {
            expected_meta: self.expected_meta.clone(),
            read_format,
            compat,
            compaction_projection,
        })
    }
}

fn build_output_schema(metadata: &RegionMetadataRef, projection: &[usize]) -> Result<SchemaRef> {
    if projection.is_empty() {
        return Ok(Arc::new(Schema::new(vec![])));
    }

    let mut column_schemas = Vec::with_capacity(projection.len());
    for idx in projection {
        column_schemas.push(
            metadata
                .schema
                .column_schemas()
                .get(*idx)
                .with_context(|| InvalidRequestSnafu {
                    region_id: metadata.region_id,
                    reason: format!("projection index {} is out of bound", idx),
                })?
                .clone(),
        );
    }

    Ok(Arc::new(Schema::new(column_schemas)))
}

fn build_primary_key_exec(
    metadata: &RegionMetadataRef,
    projection: &[usize],
    read_column_ids: &[ColumnId],
    is_empty_projection: bool,
) -> Result<PrimaryKeyProjectionExec> {
    let codec = build_primary_key_codec(metadata);
    let batch_fields = Batch::projected_fields(metadata, read_column_ids);

    let field_id_to_index: HashMap<_, _> = batch_fields
        .iter()
        .enumerate()
        .map(|(index, (column_id, _))| (*column_id, index))
        .collect();

    let mut batch_indices = Vec::with_capacity(projection.len());
    let mut has_tags = false;
    if !is_empty_projection {
        for idx in projection {
            let column = &metadata.column_metadatas[*idx];
            let batch_index = match column.semantic_type {
                SemanticType::Tag => {
                    let index = metadata.primary_key_index(column.column_id).unwrap();
                    has_tags = true;
                    BatchIndex::Tag((index, column.column_id))
                }
                SemanticType::Timestamp => BatchIndex::Timestamp,
                SemanticType::Field => {
                    let index = *field_id_to_index.get(&column.column_id).context(
                        InvalidRequestSnafu {
                            region_id: metadata.region_id,
                            reason: format!(
                                "field column {} is missing in read projection",
                                column.column_schema.name
                            ),
                        },
                    )?;
                    BatchIndex::Field(index)
                }
            };
            batch_indices.push(batch_index);
        }
    }

    Ok(PrimaryKeyProjectionExec {
        batch_indices,
        has_tags,
        codec,
        batch_fields,
    })
}

fn build_flat_exec(
    metadata: &RegionMetadataRef,
    projection: &[usize],
    read_column_ids: &[ColumnId],
    is_empty_projection: bool,
) -> Result<FlatProjectionExec> {
    let mut output_column_ids = Vec::with_capacity(projection.len());
    for idx in projection {
        let column = metadata
            .column_metadatas
            .get(*idx)
            .with_context(|| InvalidRequestSnafu {
                region_id: metadata.region_id,
                reason: format!("projection index {} is out of bound", idx),
            })?;
        output_column_ids.push(column.column_id);
    }

    let id_to_index = sst_column_id_indices(metadata);
    let format_projection = FormatProjection::compute_format_projection(
        &id_to_index,
        metadata.column_metadatas.len() + 3,
        read_column_ids.iter().copied(),
    );

    let batch_schema = flat_projected_columns(metadata, &format_projection);
    let input_arrow_schema = compute_input_arrow_schema(metadata, &batch_schema);

    let batch_indices = if is_empty_projection {
        vec![]
    } else {
        output_column_ids
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
                            reason: format!(
                                "output column {} is missing in read projection",
                                name
                            ),
                        }
                    })
            })
            .collect::<Result<Vec<_>>>()?
    };

    Ok(FlatProjectionExec {
        batch_schema,
        batch_indices,
        input_arrow_schema,
    })
}

/// Per-file projection/schema info.
pub(crate) struct FileProjectionSchema {
    expected_meta: RegionMetadataRef,
    read_format: ReadFormat,
    compat: Option<FileCompatBatch>,
    compaction_projection: Option<CompactionProjectionMapper>,
}

impl FileProjectionSchema {
    fn new(
        expected_meta: RegionMetadataRef,
        read_format: ReadFormat,
        compat: Option<FileCompatBatch>,
        compaction_projection: Option<CompactionProjectionMapper>,
    ) -> Self {
        Self {
            expected_meta,
            read_format,
            compat,
            compaction_projection,
        }
    }

    pub(crate) fn new_for_memtable(
        expected_meta: RegionMetadataRef,
        projection: Option<&[ColumnId]>,
        skip_auto_convert: bool,
    ) -> Result<Self> {
        let read_format = ReadFormat::new(
            expected_meta.clone(),
            projection,
            true,
            None,
            "memtable",
            skip_auto_convert,
        )?;

        Ok(Self::new(expected_meta, read_format, None, None))
    }

    pub(crate) fn expected_metadata(&self) -> &RegionMetadataRef {
        &self.expected_meta
    }

    pub(crate) fn metadata(&self) -> &RegionMetadataRef {
        self.read_format.metadata()
    }

    pub(crate) fn arrow_schema(&self) -> &datatypes::arrow::datatypes::SchemaRef {
        self.read_format.arrow_schema()
    }

    pub(crate) fn projection_indices(&self) -> &[usize] {
        self.read_format.projection_indices()
    }

    pub(crate) fn is_flat(&self) -> bool {
        self.read_format.as_flat().is_some()
    }

    pub(crate) fn is_primary_key(&self) -> bool {
        self.read_format.as_primary_key().is_some()
    }

    pub(crate) fn flat_projected_index_by_id(&self, column_id: ColumnId) -> Option<usize> {
        self.read_format
            .as_flat()
            .and_then(|format| format.projected_index_by_id(column_id))
    }

    pub(crate) fn primary_key_field_index_by_id(&self, column_id: ColumnId) -> Option<usize> {
        self.read_format
            .as_primary_key()
            .and_then(|format| format.field_index_by_id(column_id))
    }

    pub(crate) fn convert_primary_key_record_batch(
        &self,
        record_batch: &datatypes::arrow::record_batch::RecordBatch,
        override_sequence_array: Option<&datatypes::arrow::array::ArrayRef>,
        batches: &mut std::collections::VecDeque<crate::read::Batch>,
    ) -> Result<()> {
        let format = self
            .read_format
            .as_primary_key()
            .context(crate::error::UnexpectedSnafu {
                reason: "Expected primary key format",
            })?;
        format.convert_record_batch(record_batch, override_sequence_array, batches)
    }

    pub(crate) fn convert_flat_record_batch(
        &self,
        record_batch: datatypes::arrow::record_batch::RecordBatch,
        override_sequence_array: Option<&datatypes::arrow::array::ArrayRef>,
    ) -> Result<datatypes::arrow::record_batch::RecordBatch> {
        let format = self
            .read_format
            .as_flat()
            .context(crate::error::UnexpectedSnafu {
                reason: "Expected flat format",
            })?;
        format.convert_batch(record_batch, override_sequence_array)
    }

    pub(crate) fn new_override_sequence_array(
        &self,
        length: usize,
    ) -> Option<datatypes::arrow::array::ArrayRef> {
        self.read_format.new_override_sequence_array(length)
    }

    pub(crate) fn min_values(
        &self,
        row_groups: &[impl std::borrow::Borrow<parquet::file::metadata::RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        self.read_format.min_values(row_groups, column_id)
    }

    pub(crate) fn max_values(
        &self,
        row_groups: &[impl std::borrow::Borrow<parquet::file::metadata::RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        self.read_format.max_values(row_groups, column_id)
    }

    pub(crate) fn null_counts(
        &self,
        row_groups: &[impl std::borrow::Borrow<parquet::file::metadata::RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        self.read_format.null_counts(row_groups, column_id)
    }

    pub(crate) fn apply_primary_key_compat(&self, batch: crate::read::Batch) -> Result<Batch> {
        let Some(compat) = self.compat.as_ref() else {
            return Ok(batch);
        };
        let primary_key = match compat {
            FileCompatBatch::PrimaryKey(primary_key) => primary_key,
            FileCompatBatch::Flat(_) => {
                return UnexpectedSnafu {
                    reason: "Invalid compat for primary key format",
                }
                .fail();
            }
        };
        primary_key.compat_batch(batch)
    }

    pub(crate) fn apply_flat_compat(
        &self,
        record_batch: datatypes::arrow::record_batch::RecordBatch,
    ) -> Result<datatypes::arrow::record_batch::RecordBatch> {
        let Some(compat) = self.compat.as_ref() else {
            return Ok(record_batch);
        };
        let flat_compat = match compat {
            FileCompatBatch::Flat(flat_compat) => flat_compat,
            FileCompatBatch::PrimaryKey(_) => {
                return UnexpectedSnafu {
                    reason: "Invalid compat for flat format",
                }
                .fail();
            }
        };
        flat_compat.compat(record_batch)
    }

    pub(crate) fn apply_compaction_projection(
        &self,
        record_batch: datatypes::arrow::record_batch::RecordBatch,
    ) -> Result<datatypes::arrow::record_batch::RecordBatch> {
        let Some(mapper) = self.compaction_projection.as_ref() else {
            return Ok(record_batch);
        };
        mapper.project(record_batch)
    }
}

fn compute_read_column_ids(
    metadata: &RegionMetadataRef,
    projection: &[usize],
    filters: &[Expr],
    predicate: &PredicateGroup,
) -> Result<Vec<ColumnId>> {
    // Use Vec for read_column_ids to keep the order of columns.
    let mut read_column_ids = Vec::new();
    let mut seen = HashSet::new();

    for idx in projection {
        let column = metadata
            .column_metadatas
            .get(*idx)
            .with_context(|| InvalidRequestSnafu {
                region_id: metadata.region_id,
                reason: format!("projection index {} is out of bound", idx),
            })?;
        seen.insert(column.column_id);
        // Keep projection order.
        read_column_ids.push(column.column_id);
    }

    if projection.is_empty() {
        let time_index = metadata.time_index_column().column_id;
        if seen.insert(time_index) {
            read_column_ids.push(time_index);
        }
    }

    let mut extra_names = HashSet::new();
    let mut columns = HashSet::new();

    for expr in filters {
        columns.clear();
        if expr_to_columns(expr, &mut columns).is_err() {
            continue;
        }
        extra_names.extend(columns.iter().map(|column| column.name.clone()));
    }

    if let Some(expr) = predicate.region_partition_expr() {
        expr.collect_column_names(&mut extra_names);
    }

    if !extra_names.is_empty() {
        for column in &metadata.column_metadatas {
            if extra_names.contains(column.column_schema.name.as_str())
                && !seen.contains(&column.column_id)
            {
                read_column_ids.push(column.column_id);
            }
            extra_names.remove(column.column_schema.name.as_str());
        }
        if !extra_names.is_empty() {
            warn!(
                "Some columns in filters are not found in region {}: {:?}",
                metadata.region_id, extra_names
            );
        }
    }

    Ok(read_column_ids)
}

/// Gets a vector with repeated values from specific cache or creates a new one.
fn repeated_vector_with_cache(
    data_type: &ConcreteDataType,
    value: &Value,
    num_rows: usize,
    cache_strategy: &CacheStrategy,
) -> common_recordbatch::error::Result<VectorRef> {
    if let Some(vector) = cache_strategy.get_repeated_vector(data_type, value) {
        // Tries to get the vector from cache manager. If the vector doesn't
        // have enough length, creates a new one.
        match vector.len().cmp(&num_rows) {
            Ordering::Less => (),
            Ordering::Equal => return Ok(vector),
            Ordering::Greater => return Ok(vector.slice(0, num_rows)),
        }
    }

    // Creates a new one.
    let vector = new_repeated_vector(data_type, value, num_rows)?;
    // Updates cache.
    if vector.len() <= MAX_VECTOR_LENGTH_TO_CACHE {
        cache_strategy.put_repeated_vector(value.clone(), vector.clone());
    }

    Ok(vector)
}

/// Returns a vector with repeated values.
fn new_repeated_vector(
    data_type: &ConcreteDataType,
    value: &Value,
    num_rows: usize,
) -> common_recordbatch::error::Result<VectorRef> {
    let mut mutable_vector = data_type.create_mutable_vector(1);
    mutable_vector
        .try_push_value_ref(&value.as_value_ref())
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    // This requires an additional allocation.
    let base_vector = mutable_vector.to_vector();
    Ok(base_vector.replicate(&[num_rows]))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_expr::{col, lit};

    use super::*;
    use crate::read::scan_region::PredicateGroup;
    use crate::test_util::memtable_util::metadata_with_primary_key;

    #[tokio::test]
    async fn test_read_column_ids_includes_filters() {
        let metadata = Arc::new(metadata_with_primary_key(vec![0, 1], false));
        let filters = vec![
            col("v0").gt(lit(1)),
            col("ts").gt(lit(0)),
            col("k0").eq(lit("foo")),
        ];
        let predicate = PredicateGroup::new(metadata.as_ref(), &filters).unwrap();
        let projection = vec![4];
        let read_ids =
            compute_read_column_ids(&metadata, &projection, &filters, &predicate).unwrap();
        assert_eq!(vec![4, 0, 2, 3], read_ids);
    }

    #[tokio::test]
    async fn test_read_column_ids_empty_projection() {
        let metadata = Arc::new(metadata_with_primary_key(vec![0, 1], false));
        let filters = vec![];
        let predicate = PredicateGroup::new(metadata.as_ref(), &filters).unwrap();
        let projection = vec![];
        let read_ids =
            compute_read_column_ids(&metadata, &projection, &filters, &predicate).unwrap();
        // Empty projection should still read the time index column (id 2 in this test schema).
        assert_eq!(vec![2], read_ids);
    }

    #[tokio::test]
    async fn test_read_column_ids_keeps_projection_order() {
        let metadata = Arc::new(metadata_with_primary_key(vec![0, 1], false));
        let filters = vec![col("v0").gt(lit(1))];
        let predicate = PredicateGroup::new(metadata.as_ref(), &filters).unwrap();
        let projection = vec![4, 1];
        let read_ids =
            compute_read_column_ids(&metadata, &projection, &filters, &predicate).unwrap();
        // Projection order preserved, extra columns appended in schema order.
        assert_eq!(vec![4, 1, 3], read_ids);
    }
}
