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

use std::collections::HashSet;
use std::sync::Arc;

use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::Expr;
use snafu::OptionExt;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use tracing::warn;

use crate::error::{InvalidRequestSnafu, Result};
use crate::read::compat::{self, CompatBatch, FlatCompatBatch, PrimaryKeyCompatBatch};
use crate::read::flat_projection::CompactionProjectionMapper;
use crate::read::projection::ProjectionMapper;
use crate::read::scan_region::PredicateGroup;
use crate::sst::parquet::format::{FormatProjection, ReadFormat};

/// Projection + schema computation plan built from request + expected metadata.
pub(crate) struct ProjectionSchemaPlan {
    expected_meta: RegionMetadataRef,
    flat_format: bool,
    projection: Option<Vec<usize>>,
    read_column_ids: Vec<ColumnId>,
    mapper: Arc<ProjectionMapper>,
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

        let mapper = match &projection {
            Some(p) => ProjectionMapper::new_with_read_columns(
                &expected_meta,
                p.iter().copied(),
                flat_format,
                read_column_ids.clone(),
            )?,
            None => ProjectionMapper::all(&expected_meta, flat_format)?,
        };

        Ok(Self {
            expected_meta,
            flat_format,
            projection,
            read_column_ids,
            mapper: Arc::new(mapper),
        })
    }

    /// Builds a plan that reads all columns.
    pub(crate) fn new_all(expected_meta: RegionMetadataRef, flat_format: bool) -> Result<Self> {
        let read_column_ids = expected_meta
            .column_metadatas
            .iter()
            .map(|col| col.column_id)
            .collect();
        let mapper = ProjectionMapper::all(&expected_meta, flat_format)?;

        Ok(Self {
            expected_meta,
            flat_format,
            projection: None,
            read_column_ids,
            mapper: Arc::new(mapper),
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

    /// Returns the mapper built from expected metadata.
    pub(crate) fn mapper(&self) -> &Arc<ProjectionMapper> {
        &self.mapper
    }

    /// Returns the output schema for the final projected batch.
    pub(crate) fn output_schema(&self) -> datatypes::schema::SchemaRef {
        self.mapper.output_schema()
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
    pub(crate) fn compat_for_read_format(
        &self,
        read_format: &ReadFormat,
        compaction: bool,
    ) -> Result<Option<CompatBatch>> {
        let need_compat = !compat::has_same_columns_and_pk_encoding(
            self.expected_meta.as_ref(),
            read_format.metadata(),
        );
        if !need_compat {
            return Ok(None);
        }

        let compat = if let Some(flat_format) = read_format.as_flat() {
            let mapper = self
                .mapper
                .as_flat()
                .expect("flat format mapper missing");
            FlatCompatBatch::try_new(
                mapper,
                flat_format.metadata(),
                flat_format.format_projection(),
                compaction,
            )?
            .map(CompatBatch::Flat)
        } else {
            let compact_batch =
                PrimaryKeyCompatBatch::new(self.mapper.as_ref(), read_format.metadata().clone())?;
            Some(CompatBatch::PrimaryKey(compact_batch))
        };

        Ok(compat)
    }

    /// Computes compaction projection mapper for a file.
    pub(crate) fn compaction_projection_mapper(
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
    pub(crate) fn for_file(
        self: &Arc<Self>,
        file_meta: RegionMetadataRef,
        parquet_column_num: Option<usize>,
        file_path: &str,
        skip_auto_convert: bool,
        compaction: bool,
        is_same_region_partition: bool,
    ) -> Result<FileProjectionSchema> {
        let read_format = self.build_read_format(
            file_meta.clone(),
            parquet_column_num,
            file_path,
            skip_auto_convert,
        )?;

        let compat = self.compat_for_read_format(&read_format, compaction)?;
        let compaction_projection =
            self.compaction_projection_mapper(&file_meta, compaction, is_same_region_partition)?;

        Ok(FileProjectionSchema {
            plan: Arc::clone(self),
            file_meta,
            read_format,
            compat,
            compaction_projection,
        })
    }
}

/// Per-file projection/schema info.
pub(crate) struct FileProjectionSchema {
    plan: Arc<ProjectionSchemaPlan>,
    file_meta: RegionMetadataRef,
    read_format: ReadFormat,
    compat: Option<CompatBatch>,
    compaction_projection: Option<CompactionProjectionMapper>,
}

impl FileProjectionSchema {
    pub(crate) fn read_format(&self) -> &ReadFormat {
        &self.read_format
    }

    pub(crate) fn projection_indices(&self) -> &[usize] {
        self.read_format.projection_indices()
    }

    pub(crate) fn arrow_schema(&self) -> &datatypes::arrow::datatypes::SchemaRef {
        self.read_format.arrow_schema()
    }

    pub(crate) fn format_projection(&self) -> Option<&FormatProjection> {
        self.read_format.as_flat().map(|f| f.format_projection())
    }

    pub(crate) fn compat_batch(&self) -> Option<&CompatBatch> {
        self.compat.as_ref()
    }

    pub(crate) fn compaction_projection_mapper(&self) -> Option<&CompactionProjectionMapper> {
        self.compaction_projection.as_ref()
    }

    pub(crate) fn read_column_ids(&self) -> &[ColumnId] {
        self.plan.read_column_ids()
    }

    pub(crate) fn mapper(&self) -> &Arc<ProjectionMapper> {
        self.plan.mapper()
    }

    pub(crate) fn output_schema(&self) -> datatypes::schema::SchemaRef {
        self.plan.output_schema()
    }

    pub(crate) fn expected_metadata(&self) -> &RegionMetadataRef {
        self.plan.expected_metadata()
    }

    pub(crate) fn file_metadata(&self) -> &RegionMetadataRef {
        &self.file_meta
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
