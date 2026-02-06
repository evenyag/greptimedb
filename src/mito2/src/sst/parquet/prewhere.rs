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

//! Prewhere optimization for parquet reader.
//!
//! Prewhere optimization reduces I/O by:
//! 1. Reading only filter columns first (prewhere phase)
//! 2. Applying filters to get a refined row selection
//! 3. Reading remaining columns with the refined selection

use std::collections::HashSet;
use std::ops::BitAnd;
use std::sync::Arc;

use api::v1::SemanticType;
use common_telemetry::warn;
use datatypes::arrow::array::BooleanArray;
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use mito_codec::row_converter::{PrimaryKeyCodec, build_primary_key_codec};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::schema::types::SchemaDescriptor;
use snafu::{OptionExt, ResultExt};
use store_api::storage::ColumnId;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

use crate::config::PrewhereConfig;
use crate::error::{
    EvalPartitionFilterSnafu, NewRecordBatchSnafu, ReadParquetSnafu, RecordBatchSnafu, Result,
    UnexpectedSnafu,
};
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::reader::{
    MaybeFilter, MaybePhysicalFilter, PhysicalFilterContext, RowGroupReaderBuilder,
    SimpleFilterContext,
};
use crate::sst::parquet::row_group::ParquetFetchMetrics;
use crate::sst::parquet::tag_decode::{TagDecodeState, maybe_decode_tag_column};

/// Context for prewhere optimization.
///
/// Contains information about which columns are needed for filtering (prewhere).
pub struct PrewhereContext {
    /// Projection mask for reading prewhere columns.
    prewhere_projection_mask: ProjectionMask,
}

impl PrewhereContext {
    /// Computes the prewhere context from filters and read format.
    ///
    /// Returns `None` if prewhere optimization should not be used (e.g., no filters,
    /// all columns are filter columns, or heuristics suggest it's not beneficial).
    pub fn compute(
        filters: &[SimpleFilterContext],
        physical_filters: &[PhysicalFilterContext],
        read_format: &ReadFormat,
        parquet_schema: &SchemaDescriptor,
        config: &PrewhereConfig,
        skip_fields: bool,
    ) -> Option<Self> {
        if !config.enabled {
            return None;
        }

        // Collect column IDs and names that are used in filters.
        let mut prewhere_column_ids = HashSet::new();
        let mut prewhere_column_names = HashSet::new();
        let mut needs_primary_key = false;
        let metadata = read_format.metadata();
        let arrow_schema = read_format.arrow_schema();
        for filter_ctx in filters {
            let filter = match filter_ctx.filter() {
                MaybeFilter::Filter(f) => f,
                _ => continue,
            };

            if skip_fields && filter_ctx.semantic_type() == SemanticType::Field {
                continue;
            }

            if metadata.column_by_id(filter_ctx.column_id()).is_none() {
                continue;
            }

            prewhere_column_ids.insert(filter_ctx.column_id());
            prewhere_column_names.insert(filter.column_name().to_string());

            if filter_ctx.semantic_type() == SemanticType::Tag
                && arrow_schema
                    .column_with_name(filter.column_name())
                    .is_none()
            {
                needs_primary_key = true;
            }
        }

        for filter_ctx in physical_filters {
            let filter = match filter_ctx.filter() {
                MaybePhysicalFilter::Filter(_) => filter_ctx,
                _ => continue,
            };

            if skip_fields && filter_ctx.semantic_type() == SemanticType::Field {
                continue;
            }

            if metadata.column_by_id(filter_ctx.column_id()).is_none() {
                continue;
            }

            prewhere_column_ids.insert(filter_ctx.column_id());
            prewhere_column_names.insert(filter.column_name().to_string());

            if filter_ctx.semantic_type() == SemanticType::Tag
                && arrow_schema
                    .column_with_name(filter.column_name())
                    .is_none()
            {
                needs_primary_key = true;
            }
        }

        if needs_primary_key {
            prewhere_column_names.insert(PRIMARY_KEY_COLUMN_NAME.to_string());
        }

        let output_column_ids: HashSet<ColumnId> = match read_format {
            ReadFormat::Flat(flat) => flat
                .format_projection()
                .column_id_to_projected_index
                .keys()
                .copied()
                .collect(),
            ReadFormat::PrimaryKey(pk) => {
                pk.field_id_to_projected_index().keys().copied().collect()
            }
        };

        let repeat_read_count = output_column_ids
            .iter()
            .filter(|id| prewhere_column_ids.contains(id))
            .count();

        let (prewhere_projection_mask, prewhere_count) =
            compute_projection_mask(&prewhere_column_names, arrow_schema, parquet_schema);

        if prewhere_count == 0 {
            return None;
        }

        // Apply heuristics to decide if prewhere is beneficial.
        let total_count = read_format.projection_indices().len();
        let remaining_count = total_count.saturating_sub(repeat_read_count);
        if !should_use_prewhere(prewhere_count, remaining_count, total_count, config) {
            return None;
        }

        Some(Self {
            prewhere_projection_mask,
        })
    }

    /// Returns the projection mask for prewhere columns.
    pub fn prewhere_projection_mask(&self) -> &ProjectionMask {
        &self.prewhere_projection_mask
    }
}

/// Result of the prewhere phase.
///
/// Contains the refined row selection after applying filters.
pub struct PrewhereResult {
    /// Refined row selection after applying filters.
    refined_selection: RowSelection,
}

impl PrewhereResult {
    /// Creates a new prewhere result.
    pub fn new(refined_selection: RowSelection) -> Self {
        Self { refined_selection }
    }

    /// Returns the refined row selection.
    pub fn refined_selection(&self) -> &RowSelection {
        &self.refined_selection
    }
}

/// Determines whether prewhere optimization should be used based on heuristics.
fn should_use_prewhere(
    prewhere_count: usize,
    remaining_count: usize,
    total_count: usize,
    config: &PrewhereConfig,
) -> bool {
    // Must have remaining columns.
    if remaining_count == 0 {
        return false;
    }

    // Check minimum remaining columns threshold.
    if remaining_count < config.min_remaining_columns {
        return false;
    }

    // Check column ratio threshold.
    let ratio = prewhere_count as f64 / total_count as f64;
    ratio <= config.column_ratio_threshold()
}

/// Computes the projection mask and indices for given column names.
fn compute_projection_mask(
    column_names: &HashSet<String>,
    arrow_schema: &SchemaRef,
    parquet_schema: &SchemaDescriptor,
) -> (ProjectionMask, usize) {
    let mut projection_indices: Vec<usize> = column_names
        .iter()
        .filter_map(|name| arrow_schema.column_with_name(name).map(|(index, _)| index))
        .collect();
    projection_indices.sort_unstable();
    projection_indices.dedup();

    let prewhere_count = projection_indices.len();
    let projection_mask = ProjectionMask::roots(parquet_schema, projection_indices.iter().copied());

    (projection_mask, prewhere_count)
}

/// Applies filters to a record batch and returns the combined filter mask.
///
/// Returns `None` if the entire batch is filtered out.
pub fn apply_filters_to_batch(
    batch: &RecordBatch,
    filters: &[SimpleFilterContext],
    physical_filters: &[PhysicalFilterContext],
    read_format: &ReadFormat,
    skip_fields: bool,
) -> Result<Option<BooleanBuffer>> {
    let mut mask = BooleanBuffer::new_set(batch.num_rows());
    let metadata = read_format.metadata();
    let mut tag_decode_state = TagDecodeState::new();
    let mut pk_codec: Option<Arc<dyn PrimaryKeyCodec>> = None;

    for filter_ctx in filters {
        let filter = match filter_ctx.filter() {
            MaybeFilter::Filter(f) => f,
            MaybeFilter::Matched => continue,
            MaybeFilter::Pruned => return Ok(None),
        };

        // Skip field filters if requested.
        if skip_fields && filter_ctx.semantic_type() == SemanticType::Field {
            continue;
        }

        // Find the column in the batch by name.
        if let Some((idx, _)) = batch.schema().column_with_name(filter.column_name()) {
            let column = batch.column(idx);
            let result = filter.evaluate_array(column).context(RecordBatchSnafu)?;
            mask = mask.bitand(&result);
            continue;
        }

        common_telemetry::info!(
            "Prewhere apply simple filter for column: {}",
            filter_ctx.column_name(),
        ):

        if filter_ctx.semantic_type() == SemanticType::Tag {
            if let Some(tag_column) = maybe_decode_tag_column(
                metadata,
                filter_ctx.column_id(),
                batch,
                &mut tag_decode_state,
                pk_codec
                    .get_or_insert_with(|| build_primary_key_codec(metadata))
                    .as_ref(),
            )? {
                let result = filter
                    .evaluate_array(&tag_column)
                    .context(RecordBatchSnafu)?;
                mask = mask.bitand(&result);
            }
        } else {
            warn!(
                "Filter column '{}' not found in record batch, skip filter. column_id={}, semantic_type={:?}",
                filter.column_name(),
                filter_ctx.column_id(),
                filter_ctx.semantic_type()
            );
        }
    }

    for filter_ctx in physical_filters {
        let filter = match filter_ctx.filter() {
            MaybePhysicalFilter::Filter(f) => f,
            MaybePhysicalFilter::Matched => continue,
            MaybePhysicalFilter::Pruned => return Ok(None),
        };

        // Skip field filters if requested.
        if skip_fields && filter_ctx.semantic_type() == SemanticType::Field {
            continue;
        }

        let column =
            if let Some((idx, _)) = batch.schema().column_with_name(filter_ctx.column_name()) {
                Some(batch.column(idx).clone())
            } else if filter_ctx.semantic_type() == SemanticType::Tag {
                maybe_decode_tag_column(
                    metadata,
                    filter_ctx.column_id(),
                    batch,
                    &mut tag_decode_state,
                    pk_codec
                        .get_or_insert_with(|| build_primary_key_codec(metadata))
                        .as_ref(),
                )?
            } else {
                None
            };

        let Some(column) = column else {
            warn!(
                "Filter column '{}' not found in record batch, skip filter. column_id={}, semantic_type={:?}",
                filter_ctx.column_name(),
                filter_ctx.column_id(),
                filter_ctx.semantic_type()
            );
            continue;
        };

        common_telemetry::info!(
            "Prewhere apply physical filter for column: {}, expr: {:?}",
            filter_ctx.column_name(),
            filter
        );

        let record_batch = RecordBatch::try_new(filter_ctx.schema().clone(), vec![column.clone()])
            .context(NewRecordBatchSnafu)?;
        let evaluated = filter
            .evaluate(&record_batch)
            .context(EvalPartitionFilterSnafu)?;
        let array = evaluated
            .into_array(record_batch.num_rows())
            .context(EvalPartitionFilterSnafu)?;
        let boolean_array =
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context(UnexpectedSnafu {
                    reason: "Failed to downcast physical filter result to BooleanArray".to_string(),
                })?;
        mask = mask.bitand(boolean_array.values());
    }

    if mask.count_set_bits() == 0 {
        return Ok(None);
    }

    Ok(Some(mask))
}

/// Executes the prewhere phase: reads prewhere columns, applies filters, and returns the result.
///
/// This function:
/// 1. Reads the prewhere columns from the parquet file
/// 2. Applies the filters to compute a refined row selection
/// 3. Returns the refined row selection for reading remaining columns
///
/// # Arguments
/// * `reader_builder` - The row group reader builder
/// * `row_group_idx` - The row group index to read
/// * `row_selection` - Optional initial row selection
/// * `prewhere_ctx` - The prewhere context with projection masks
/// * `filters` - The filters to apply
/// * `read_format` - The read format
/// * `skip_fields` - Whether to skip field filters
/// * `fetch_metrics` - Optional metrics for tracking fetch operations
///
/// # Returns
/// * `Ok(Some(PrewhereResult))` - If rows pass the filter
/// * `Ok(None)` - If all rows are filtered out
#[allow(clippy::too_many_arguments)]
pub async fn execute_prewhere(
    reader_builder: &RowGroupReaderBuilder,
    row_group_idx: usize,
    row_selection: Option<RowSelection>,
    prewhere_ctx: &PrewhereContext,
    filters: &[SimpleFilterContext],
    physical_filters: &[PhysicalFilterContext],
    read_format: &ReadFormat,
    skip_fields: bool,
    fetch_metrics: Option<&ParquetFetchMetrics>,
) -> Result<Option<PrewhereResult>> {
    // Phase 1: Read prewhere columns
    let mut prewhere_stream = reader_builder
        .build_with_projection(
            row_group_idx,
            row_selection.clone(),
            prewhere_ctx.prewhere_projection_mask().clone(),
            fetch_metrics,
        )
        .await?;

    // Collect all batches and build the combined filter mask
    let mut filter_arrays: Vec<BooleanArray> = Vec::new();
    let mut rows_selected = 0usize;

    while let Some(batch_result) = prewhere_stream.next().await {
        let batch = batch_result.context(ReadParquetSnafu {
            path: reader_builder.file_path(),
        })?;

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }
        // Apply filters to get the mask for this batch
        let batch_mask = match apply_filters_to_batch(
            &batch,
            filters,
            physical_filters,
            read_format,
            skip_fields,
        )? {
            Some(mask) => mask,
            None => BooleanBuffer::new_unset(num_rows),
        };

        rows_selected += batch_mask.count_set_bits();

        filter_arrays.push(BooleanArray::from(batch_mask));
    }

    if filter_arrays.is_empty() {
        return Ok(None);
    };

    if rows_selected == 0 {
        return Ok(None);
    }

    common_telemetry::info!(
        "Prewhere selected: {} for row group {}",
        rows_selected,
        row_group_idx
    );

    // Convert the filter mask to a row selection
    let prewhere_selection = RowSelection::from_filters(&filter_arrays);

    // Intersect with the original row selection if present
    let refined_selection = match row_selection {
        Some(original) => original.and_then(&prewhere_selection),
        None => prewhere_selection,
    };

    Ok(Some(PrewhereResult::new(refined_selection)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_use_prewhere() {
        let config = PrewhereConfig::default();

        // Should use: 1 prewhere column, 5 remaining, 6 total.
        assert!(should_use_prewhere(1, 5, 6, &config));

        // Should not use: 0 remaining columns.
        assert!(!should_use_prewhere(1, 0, 1, &config));

        // Should not use: less than min_remaining_columns.
        assert!(!should_use_prewhere(4, 1, 5, &config));

        // Should not use: ratio exceeds threshold.
        assert!(!should_use_prewhere(4, 3, 6, &config));

        // Should use: exactly at threshold.
        assert!(should_use_prewhere(3, 3, 6, &config));
    }

    #[test]
    fn test_should_use_prewhere_disabled() {
        let config = PrewhereConfig {
            enabled: false,
            ..Default::default()
        };

        // Even good conditions should return false when disabled.
        assert!(!config.enabled);
    }
}
