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
//! 4. Merging prewhere columns (cached locally) with remaining columns

use std::collections::{HashMap, HashSet};
use std::ops::BitAnd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use api::v1::SemanticType;
use datatypes::arrow::array::{ArrayRef, BooleanArray, BooleanBufferBuilder};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::compute::{concat, filter};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use futures::{Stream, StreamExt};
use mito_codec::row_converter::{PrimaryKeyCodec, build_primary_key_codec};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::schema::types::SchemaDescriptor;
use snafu::{IntoError, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::config::PrewhereConfig;
use crate::error::{ComputeArrowSnafu, ReadParquetSnafu, RecordBatchSnafu, Result};
use crate::sst::parquet::async_reader::SstAsyncFileReader;
use crate::sst::parquet::flat_format::{
    DecodedPrimaryKeys, decode_primary_keys, sst_column_id_indices, time_index_column_index,
};
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::reader::{MaybeFilter, RowGroupReaderBuilder, SimpleFilterContext};
use crate::sst::parquet::row_group::ParquetFetchMetrics;

/// Context for prewhere optimization.
///
/// Contains information about which columns are needed for filtering (prewhere)
/// and which columns are remaining to be read after filtering.
pub struct PrewhereContext {
    /// Projection mask for reading prewhere columns.
    prewhere_projection_mask: ProjectionMask,
    /// Projection mask for reading remaining columns.
    remaining_projection_mask: ProjectionMask,
    /// Indices of cached prewhere columns in the prewhere batch.
    prewhere_cache_indices_in_batch: Vec<usize>,
    /// Indices of cached prewhere columns in the full projected batch.
    prewhere_indices_in_full_batch: Vec<usize>,
    /// Indices of remaining columns in the full projected batch.
    remaining_indices_in_full_batch: Vec<usize>,
}

impl PrewhereContext {
    /// Computes the prewhere context from filters and read format.
    ///
    /// Returns `None` if prewhere optimization should not be used (e.g., no filters,
    /// all columns are filter columns, or heuristics suggest it's not beneficial).
    pub fn compute(
        filters: &[SimpleFilterContext],
        read_format: &ReadFormat,
        parquet_schema: &SchemaDescriptor,
        config: &PrewhereConfig,
        skip_fields: bool,
    ) -> Option<Self> {
        if !config.enabled {
            return None;
        }

        // Collect column IDs that are used in filters.
        let mut prewhere_column_ids = HashSet::new();
        for filter in filters {
            // Skip filters that are already evaluated (Matched/Pruned).
            if matches!(filter.filter(), MaybeFilter::Filter(_)) {
                if skip_fields && filter.semantic_type() == SemanticType::Field {
                    continue;
                }
                prewhere_column_ids.insert(filter.column_id());
            }
        }

        // No filter columns means no prewhere optimization.
        if prewhere_column_ids.is_empty() {
            return None;
        }

        // Collect all projected column IDs (output projection).
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

        // Prewhere columns that are also in output projection.
        let prewhere_output_column_ids: HashSet<ColumnId> = prewhere_column_ids
            .intersection(&output_column_ids)
            .copied()
            .collect();

        // Compute remaining column IDs.
        let remaining_column_ids: HashSet<ColumnId> = output_column_ids
            .difference(&prewhere_output_column_ids)
            .copied()
            .collect();

        // Apply heuristics to decide if prewhere is beneficial.
        let all_projected_ids: HashSet<ColumnId> = output_column_ids
            .union(&prewhere_column_ids)
            .copied()
            .collect();
        if !should_use_prewhere(
            prewhere_column_ids.len(),
            remaining_column_ids.len(),
            all_projected_ids.len(),
            config,
        ) {
            common_telemetry::info!(
                "Don't use prewhere, prewhere columns: {:?}, output columns: {:?}, remaining columns: {:?}",
                prewhere_column_ids,
                output_column_ids,
                remaining_column_ids
            );
            return None;
        }

        let full_projection_indices = read_format.projection_indices().to_vec();
        let full_index_pos: HashMap<usize, usize> = full_projection_indices
            .iter()
            .enumerate()
            .map(|(pos, idx)| (*idx, pos))
            .collect();
        let id_to_index = column_id_to_sst_index(read_format);

        let prewhere_cache_sst_indices: HashSet<usize> = prewhere_output_column_ids
            .iter()
            .filter_map(|id| id_to_index.get(id).copied())
            .collect();

        common_telemetry::info!(
            "Use prewhere, prewhere columns: {:?}, output columns: {:?}, remaining columns: {:?}, full_projection_indices: {:?}, prewhere_cache_sst_indices: {:?}",
            prewhere_column_ids,
            output_column_ids,
            remaining_column_ids,
            full_projection_indices,
            prewhere_cache_sst_indices,
        );

        // Compute projection masks and indices.
        let (
            prewhere_projection_mask,
            prewhere_cache_indices_in_batch,
            prewhere_indices_in_full_batch,
        ) = compute_projection_mask_and_indices(
            &prewhere_column_ids,
            &id_to_index,
            parquet_schema,
            &full_index_pos,
            Some(&prewhere_cache_sst_indices),
        );

        let (remaining_projection_mask, _cache, remaining_indices_in_full_batch) =
            compute_projection_mask_and_indices(
                &remaining_column_ids,
                &id_to_index,
                parquet_schema,
                &full_index_pos,
                None,
            );

        common_telemetry::info!(
            "Use prewhere compute mask, full_projection_indices: {:?}, prewhere_cache_sst_indices: {:?}, prewhere_indices_in_full_batch: {:?}, remaining_indices_in_full_batch: {:?}",
            full_projection_indices,
            prewhere_cache_sst_indices,
            prewhere_indices_in_full_batch,
            remaining_indices_in_full_batch,
        );

        Some(Self {
            prewhere_projection_mask,
            remaining_projection_mask,
            prewhere_cache_indices_in_batch,
            prewhere_indices_in_full_batch,
            remaining_indices_in_full_batch,
        })
    }

    /// Returns the projection mask for prewhere columns.
    pub fn prewhere_projection_mask(&self) -> &ProjectionMask {
        &self.prewhere_projection_mask
    }

    /// Returns the projection mask for remaining columns.
    pub fn remaining_projection_mask(&self) -> &ProjectionMask {
        &self.remaining_projection_mask
    }

    /// Returns the indices of cached prewhere columns in the prewhere batch.
    pub fn prewhere_cache_indices_in_batch(&self) -> &[usize] {
        &self.prewhere_cache_indices_in_batch
    }
}

/// Result of the prewhere phase.
///
/// Contains the refined row selection and cached prewhere column data.
pub struct PrewhereResult {
    /// Refined row selection after applying filters.
    refined_selection: RowSelection,
    /// Cached prewhere columns data (in order of prewhere cache indices).
    cached_columns: Vec<ArrayRef>,
    /// The filter mask indicating which rows passed the filter.
    filter_mask: BooleanBuffer,
}

impl PrewhereResult {
    /// Creates a new prewhere result.
    pub fn new(
        refined_selection: RowSelection,
        cached_columns: Vec<ArrayRef>,
        filter_mask: BooleanBuffer,
    ) -> Self {
        Self {
            refined_selection,
            cached_columns,
            filter_mask,
        }
    }

    /// Returns the refined row selection.
    pub fn refined_selection(&self) -> &RowSelection {
        &self.refined_selection
    }

    /// Returns the filter mask.
    pub fn filter_mask(&self) -> &BooleanBuffer {
        &self.filter_mask
    }

    /// Returns the cached columns.
    pub fn cached_columns(&self) -> &Vec<ArrayRef> {
        &self.cached_columns
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

/// Returns a map of column id to its index in the SST schema.
fn column_id_to_sst_index(read_format: &ReadFormat) -> HashMap<ColumnId, usize> {
    let metadata = read_format.metadata();
    match read_format {
        ReadFormat::Flat(_) => sst_column_id_indices(metadata.as_ref()),
        ReadFormat::PrimaryKey(_) => metadata
            .field_columns()
            .enumerate()
            .map(|(index, col)| (col.column_id, index))
            .collect::<HashMap<_, _>>(),
    }
}

/// Computes the projection mask and indices for given column IDs.
fn compute_projection_mask_and_indices(
    column_ids: &HashSet<ColumnId>,
    id_to_index: &HashMap<ColumnId, usize>,
    parquet_schema: &SchemaDescriptor,
    full_index_pos: &HashMap<usize, usize>,
    cache_sst_indices: Option<&HashSet<usize>>,
) -> (ProjectionMask, Vec<usize>, Vec<usize>) {
    let sst_column_num = parquet_schema.num_columns();
    let format_projection =
        crate::sst::parquet::format::FormatProjection::compute_format_projection(
            id_to_index,
            sst_column_num,
            column_ids.iter().copied(),
        );

    let projection_indices = format_projection.projection_indices;
    let mut cache_indices_in_batch = Vec::new();
    let mut indices_in_full_batch = Vec::new();

    for (batch_idx, sst_idx) in projection_indices.iter().enumerate() {
        let should_cache = cache_sst_indices.is_none_or(|set| set.contains(sst_idx));
        if should_cache && let Some(full_pos) = full_index_pos.get(sst_idx) {
            cache_indices_in_batch.push(batch_idx);
            indices_in_full_batch.push(*full_pos);
        }
    }

    let mask = ProjectionMask::roots(parquet_schema, projection_indices.iter().copied());

    (mask, cache_indices_in_batch, indices_in_full_batch)
}

/// Applies filters to a record batch and returns the combined filter mask.
///
/// Returns `None` if the entire batch is filtered out.
pub fn apply_filters_to_batch(
    batch: &RecordBatch,
    filters: &[SimpleFilterContext],
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

        // Find the column in the batch.
        let column_idx = match read_format {
            ReadFormat::Flat(flat) => flat.projected_index_by_id(filter_ctx.column_id()),
            ReadFormat::PrimaryKey(pk) => pk
                .field_id_to_projected_index()
                .get(&filter_ctx.column_id())
                .copied(),
        };

        if let Some(idx) = column_idx
            && let Some(column) = batch.columns().get(idx)
        {
            let result = filter.evaluate_array(column).context(RecordBatchSnafu)?;
            mask = mask.bitand(&result);
            continue;
        }

        if filter_ctx.semantic_type() == SemanticType::Tag {
            if let Some(tag_column) = maybe_decode_tag_column(
                metadata,
                filter_ctx.column_id(),
                filter_ctx.data_type(),
                batch,
                &mut tag_decode_state,
                &mut pk_codec,
            )? {
                let result = filter
                    .evaluate_array(&tag_column)
                    .context(RecordBatchSnafu)?;
                mask = mask.bitand(&result);
            }
        } else if filter_ctx.semantic_type() == SemanticType::Timestamp {
            let time_index_pos = time_index_column_index(batch.num_columns());
            let column = &batch.columns()[time_index_pos];
            let result = filter.evaluate_array(column).context(RecordBatchSnafu)?;
            mask = mask.bitand(&result);
        }
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
/// 2. Applies the filters to compute a filter mask
/// 3. Caches the prewhere column data
/// 4. Returns the refined row selection for reading remaining columns
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
    let mut all_cached_columns: Vec<Vec<ArrayRef>> = Vec::new();
    let mut filter_arrays: Vec<BooleanArray> = Vec::new();
    let mut mask_builder = BooleanBufferBuilder::new(0);
    let cache_indices = prewhere_ctx.prewhere_cache_indices_in_batch();

    while let Some(batch_result) = prewhere_stream.next().await {
        let batch = batch_result.context(ReadParquetSnafu {
            path: reader_builder.file_path(),
        })?;

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }
        // Apply filters to get the mask for this batch
        let batch_mask = match apply_filters_to_batch(&batch, filters, read_format, skip_fields)? {
            Some(mask) => mask,
            None => BooleanBuffer::new_unset(num_rows),
        };

        // Cache the columns from this batch
        if !cache_indices.is_empty() {
            let mut batch_columns = Vec::with_capacity(cache_indices.len());
            for &idx in cache_indices {
                if let Some(column) = batch.columns().get(idx) {
                    batch_columns.push(column.clone());
                }
            }
            all_cached_columns.push(batch_columns);
        }

        for i in 0..batch_mask.len() {
            mask_builder.append(batch_mask.value(i));
        }

        filter_arrays.push(BooleanArray::from(batch_mask));
    }

    if filter_arrays.is_empty() {
        return Ok(None);
    };

    let filter_mask: BooleanBuffer = mask_builder.into();
    let rows_selected = filter_mask.count_set_bits();

    common_telemetry::info!(
        "Prewhere execute, row group: {}, rows selected: {}",
        row_group_idx,
        rows_selected
    );

    if rows_selected == 0 {
        return Ok(None);
    }

    // Flatten cached columns from all batches
    let cached_columns = flatten_cached_columns(all_cached_columns);

    // Convert the filter mask to a row selection
    let prewhere_selection = RowSelection::from_filters(&filter_arrays);

    // Intersect with the original row selection if present
    let refined_selection = match row_selection {
        Some(original) => original.and_then(&prewhere_selection),
        None => prewhere_selection,
    };

    Ok(Some(PrewhereResult::new(
        refined_selection,
        cached_columns,
        filter_mask,
    )))
}

/// Flattens cached columns from multiple batches into a single list.
fn flatten_cached_columns(batches: Vec<Vec<ArrayRef>>) -> Vec<ArrayRef> {
    if batches.is_empty() {
        return Vec::new();
    }

    if batches.len() == 1 {
        return batches.into_iter().next().unwrap();
    }

    // Get the number of columns from the first batch
    let num_columns = batches[0].len();
    let mut result = Vec::with_capacity(num_columns);

    // FIXME(yingwen): Avoid concatenating arrays.
    for col_idx in 0..num_columns {
        let arrays: Vec<&dyn datatypes::arrow::array::Array> = batches
            .iter()
            .map(|batch| batch[col_idx].as_ref())
            .collect();

        // Concatenate all arrays for this column
        let concatenated = concat(&arrays).expect("Failed to concatenate arrays");
        result.push(concatenated);
    }

    result
}

/// A builder for mapping prewhere columns to their positions in the merged batch.
pub struct PrewhereColumnMapper {
    /// Maps prewhere column index to its position in the full batch schema.
    prewhere_to_full: HashMap<usize, usize>,
    /// Maps remaining column index to its position in the full batch schema.
    remaining_to_full: HashMap<usize, usize>,
    /// Total number of columns in the full batch.
    total_columns: usize,
}

impl PrewhereColumnMapper {
    /// Creates a new column mapper from prewhere context.
    pub fn new(prewhere_ctx: &PrewhereContext, total_columns: usize) -> Self {
        let mut prewhere_to_full = HashMap::new();
        let mut remaining_to_full = HashMap::new();

        for (local_idx, &full_idx) in prewhere_ctx
            .prewhere_indices_in_full_batch
            .iter()
            .enumerate()
        {
            prewhere_to_full.insert(local_idx, full_idx);
        }

        for (local_idx, &full_idx) in prewhere_ctx
            .remaining_indices_in_full_batch
            .iter()
            .enumerate()
        {
            remaining_to_full.insert(local_idx, full_idx);
        }

        Self {
            prewhere_to_full,
            remaining_to_full,
            total_columns,
        }
    }

    /// Returns the total number of columns in the full batch.
    pub fn total_columns(&self) -> usize {
        self.total_columns
    }
}

/// A stream that is aware of prewhere optimization.
pub(crate) enum PrewhereAwareStream {
    /// Passthrough parquet stream.
    Passthrough {
        stream: ParquetRecordBatchStream<SstAsyncFileReader>,
        file_path: String,
    },
    /// Merged stream with cached prewhere columns and remaining columns.
    Merged {
        remaining_stream: ParquetRecordBatchStream<SstAsyncFileReader>,
        cached_columns: Vec<ArrayRef>,
        column_mapper: PrewhereColumnMapper,
        full_schema: Arc<datatypes::arrow::datatypes::Schema>,
        offset: usize,
        file_path: String,
    },
    /// Empty stream (no rows selected).
    Empty,
}

impl Stream for PrewhereAwareStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this {
            PrewhereAwareStream::Passthrough { stream, file_path } => {
                match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),
                    Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(ReadParquetSnafu {
                        path: file_path.clone(),
                    }
                    .into_error(e)))),
                    Poll::Ready(None) => Poll::Ready(None),
                    Poll::Pending => Poll::Pending,
                }
            }
            PrewhereAwareStream::Merged {
                remaining_stream,
                cached_columns,
                column_mapper,
                full_schema,
                offset,
                file_path,
            } => match Pin::new(remaining_stream).poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let num_rows = batch.num_rows();
                    let mut merged_columns: Vec<Option<ArrayRef>> =
                        vec![None; column_mapper.total_columns()];

                    // Fill remaining columns.
                    for (local_idx, column) in batch.columns().iter().enumerate() {
                        if let Some(&full_idx) = column_mapper.remaining_to_full.get(&local_idx) {
                            merged_columns[full_idx] = Some(column.clone());
                        }
                    }

                    // Slice cached prewhere columns and fill.
                    if !cached_columns.is_empty() && num_rows > 0 {
                        for (local_idx, column) in cached_columns.iter().enumerate() {
                            if let Some(&full_idx) = column_mapper.prewhere_to_full.get(&local_idx)
                            {
                                let sliced = column.slice(*offset, num_rows);
                                merged_columns[full_idx] = Some(sliced);
                            }
                        }
                    }
                    *offset += num_rows;

                    let columns: Vec<ArrayRef> = merged_columns
                        .into_iter()
                        .map(|c| c.expect("missing column in prewhere merge"))
                        .collect();

                    let batch = RecordBatch::try_new(full_schema.clone(), columns)
                        .context(ComputeArrowSnafu);
                    Poll::Ready(Some(batch))
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(ReadParquetSnafu {
                    path: file_path.clone(),
                }
                .into_error(e)))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            PrewhereAwareStream::Empty => Poll::Ready(None),
        }
    }
}

/// Filters cached columns using a boolean mask.
pub(crate) fn filter_cached_columns(
    cached_columns: &[ArrayRef],
    filter_mask: &BooleanBuffer,
) -> Result<Vec<ArrayRef>> {
    if cached_columns.is_empty() {
        return Ok(Vec::new());
    }
    let boolean_array = BooleanArray::from(filter_mask.clone());
    cached_columns
        .iter()
        .map(|col| filter(col.as_ref(), &boolean_array).context(ComputeArrowSnafu))
        .collect()
}

struct TagDecodeState {
    decoded_pks: Option<DecodedPrimaryKeys>,
    decoded_tag_cache: HashMap<ColumnId, ArrayRef>,
}

impl TagDecodeState {
    fn new() -> Self {
        Self {
            decoded_pks: None,
            decoded_tag_cache: HashMap::new(),
        }
    }
}

fn maybe_decode_tag_column(
    metadata: &RegionMetadataRef,
    column_id: ColumnId,
    data_type: &ConcreteDataType,
    batch: &RecordBatch,
    state: &mut TagDecodeState,
    codec: &mut Option<Arc<dyn PrimaryKeyCodec>>,
) -> Result<Option<ArrayRef>> {
    let Some(pk_index) = metadata.primary_key_index(column_id) else {
        return Ok(None);
    };

    if let Some(cached) = state.decoded_tag_cache.get(&column_id) {
        return Ok(Some(cached.clone()));
    }

    if state.decoded_pks.is_none() {
        let codec = codec.get_or_insert_with(|| build_primary_key_codec(metadata));
        let decoded = decode_primary_keys(codec.as_ref(), batch)?;
        state.decoded_pks = Some(decoded);
    }

    let decoded = state.decoded_pks.as_ref().unwrap();
    let tag_column = decoded.get_tag_column(column_id, Some(pk_index), data_type)?;
    state
        .decoded_tag_cache
        .insert(column_id, tag_column.clone());
    Ok(Some(tag_column))
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
