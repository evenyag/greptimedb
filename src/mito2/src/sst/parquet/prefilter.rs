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

//! Helpers for parquet prefiltering.

use std::ops::Range;
use std::sync::Arc;

use api::v1::SemanticType;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datatypes::arrow::array::{BinaryArray, BooleanArray};
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use mito_codec::primary_key_filter::is_partition_column;
use mito_codec::row_converter::{PrimaryKeyFilter, build_primary_key_codec};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, RowSelection};
use parquet::schema::types::SchemaDescriptor;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};

use crate::config::PrefilterConfig;
use crate::error::{ComputeArrowSnafu, ReadParquetSnafu, Result, UnexpectedSnafu};
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::async_reader::SstAsyncFileReader;
use crate::sst::parquet::flat_format::primary_key_column_index;
use crate::sst::parquet::format::PrimaryKeyArray;
use crate::sst::parquet::reader::RowGroupReaderBuilder;
use crate::sst::parquet::row_group::ParquetFetchMetrics;
use crate::sst::parquet::row_selection::row_selection_from_row_ranges;

pub(crate) struct PrefilterContext {
    projection_mask: ProjectionMask,
}

impl PrefilterContext {
    pub(crate) fn compute(
        read_format: &crate::sst::parquet::format::ReadFormat,
        parquet_schema: &SchemaDescriptor,
        primary_key_filters: Option<&Arc<Vec<SimpleFilterEvaluator>>>,
        config: &PrefilterConfig,
    ) -> Option<Self> {
        if !config.enable {
            return None;
        }

        let flat = read_format.as_flat()?;
        if !flat.raw_batch_has_primary_key_dictionary() {
            return None;
        }

        let primary_key_filters = primary_key_filters?;
        if primary_key_filters.is_empty() {
            return None;
        }

        let primary_key_index = primary_key_column_index(flat.arrow_schema().fields().len());
        Some(Self {
            projection_mask: ProjectionMask::roots(parquet_schema, [primary_key_index]),
        })
    }

    pub(crate) fn projection_mask(&self) -> &ProjectionMask {
        &self.projection_mask
    }
}

pub(crate) struct PrefilterResult {
    refined_selection: RowSelection,
    filtered_rows: usize,
}

impl PrefilterResult {
    pub(crate) fn new(refined_selection: RowSelection, filtered_rows: usize) -> Self {
        Self {
            refined_selection,
            filtered_rows,
        }
    }

    pub(crate) fn refined_selection(&self) -> &RowSelection {
        &self.refined_selection
    }

    pub(crate) fn filtered_rows(&self) -> usize {
        self.filtered_rows
    }
}

pub(crate) async fn execute_prefilter(
    reader_builder: &RowGroupReaderBuilder,
    row_group_idx: usize,
    row_selection: Option<RowSelection>,
    prefilter_ctx: &PrefilterContext,
    read_format: &crate::sst::parquet::format::ReadFormat,
    primary_key_filters: &Arc<Vec<SimpleFilterEvaluator>>,
    _fetch_metrics: Option<&ParquetFetchMetrics>,
) -> Result<PrefilterResult> {
    let codec = build_primary_key_codec(read_format.metadata());
    let mut primary_key_filter =
        codec.primary_key_filter(read_format.metadata(), Arc::clone(primary_key_filters));
    let async_reader = SstAsyncFileReader::new(
        reader_builder.file_handle().region_id(),
        reader_builder.file_handle().file_id().file_id(),
        reader_builder.file_path().to_string(),
        reader_builder.object_store().clone(),
        reader_builder.cache_strategy().clone(),
        reader_builder.parquet_metadata().clone(),
    )
    .with_row_group_idx(row_group_idx)
    .with_fetch_metrics(None);
    let arrow_reader_options =
        ArrowReaderOptions::new().with_schema(read_format.arrow_schema().clone());
    let arrow_metadata = ArrowReaderMetadata::try_new(
        reader_builder.parquet_metadata().clone(),
        arrow_reader_options,
    )
    .context(ReadParquetSnafu {
        path: reader_builder.file_path(),
    })?;
    let mut builder =
        parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new_with_metadata(
            async_reader,
            arrow_metadata,
        )
        .with_row_groups(vec![row_group_idx])
        .with_projection(prefilter_ctx.projection_mask().clone())
        .with_batch_size(DEFAULT_READ_BATCH_SIZE);

    if let Some(selection) = row_selection.clone() {
        builder = builder.with_row_selection(selection);
    }

    let mut stream = builder.build().context(ReadParquetSnafu {
        path: reader_builder.file_path(),
    })?;
    let mut matched_row_ranges = Vec::new();
    let mut row_offset = 0;
    let mut total_rows = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.context(ReadParquetSnafu {
            path: reader_builder.file_path(),
        })?;
        total_rows += batch.num_rows();
        for range in matching_row_ranges_by_primary_key(&batch, primary_key_filter.as_mut())? {
            matched_row_ranges.push((row_offset + range.start)..(row_offset + range.end));
        }
        row_offset += batch.num_rows();
    }

    let refined_selection =
        row_selection_from_row_ranges(matched_row_ranges.into_iter(), total_rows);
    let filtered_rows = total_rows.saturating_sub(refined_selection.row_count());
    let refined_selection = if let Some(selection) = row_selection {
        selection.intersection(&refined_selection)
    } else {
        refined_selection
    };

    Ok(PrefilterResult::new(refined_selection, filtered_rows))
}

pub(crate) fn matching_row_ranges_by_primary_key(
    input: &RecordBatch,
    pk_filter: &mut dyn PrimaryKeyFilter,
) -> Result<Vec<Range<usize>>> {
    let primary_key_index = primary_key_column_index(input.num_columns());
    let pk_dict_array = input
        .column(primary_key_index)
        .as_any()
        .downcast_ref::<PrimaryKeyArray>()
        .context(UnexpectedSnafu {
            reason: "Primary key column is not a dictionary array".to_string(),
        })?;
    let pk_values = pk_dict_array
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context(UnexpectedSnafu {
            reason: "Primary key values are not binary array".to_string(),
        })?;
    let keys = pk_dict_array.keys();
    let key_values = keys.values();

    if key_values.is_empty() {
        return Ok(std::iter::once(0..input.num_rows()).collect());
    }

    let mut matched_row_ranges: Vec<Range<usize>> = Vec::new();
    let mut start = 0;
    while start < key_values.len() {
        let key = key_values[start];
        let mut end = start + 1;
        while end < key_values.len() && key_values[end] == key {
            end += 1;
        }

        if pk_filter.matches(pk_values.value(key as usize)) {
            if let Some(last) = matched_row_ranges.last_mut()
                && last.end == start
            {
                last.end = end;
            } else {
                matched_row_ranges.push(start..end);
            }
        }

        start = end;
    }

    Ok(matched_row_ranges)
}

#[allow(dead_code)]
pub(crate) fn prefilter_flat_batch_by_primary_key(
    input: RecordBatch,
    pk_filter: &mut dyn PrimaryKeyFilter,
) -> Result<Option<RecordBatch>> {
    if input.num_rows() == 0 {
        return Ok(Some(input));
    }

    let matched_row_ranges = matching_row_ranges_by_primary_key(&input, pk_filter)?;
    if matched_row_ranges.is_empty() {
        return Ok(None);
    }

    if matched_row_ranges.len() == 1
        && matched_row_ranges[0].start == 0
        && matched_row_ranges[0].end == input.num_rows()
    {
        return Ok(Some(input));
    }

    if matched_row_ranges.len() == 1 {
        let span = &matched_row_ranges[0];
        return Ok(Some(input.slice(span.start, span.end - span.start)));
    }

    let mut mask = vec![false; input.num_rows()];
    for span in matched_row_ranges {
        mask[span].fill(true);
    }

    let filtered =
        datatypes::arrow::compute::filter_record_batch(&input, &BooleanArray::from(mask))
            .context(ComputeArrowSnafu)?;
    if filtered.num_rows() == 0 {
        Ok(None)
    } else {
        Ok(Some(filtered))
    }
}

pub(crate) fn retain_usable_primary_key_filters(
    sst_metadata: &RegionMetadataRef,
    expected_metadata: Option<&RegionMetadata>,
    filters: &mut Vec<SimpleFilterEvaluator>,
) {
    filters.retain(|filter| is_usable_primary_key_filter(sst_metadata, expected_metadata, filter));
}

pub(crate) fn is_usable_primary_key_filter(
    sst_metadata: &RegionMetadataRef,
    expected_metadata: Option<&RegionMetadata>,
    filter: &SimpleFilterEvaluator,
) -> bool {
    if is_partition_column(filter.column_name()) {
        return false;
    }

    let sst_column = match expected_metadata {
        Some(expected_metadata) => {
            let Some(expected_column) = expected_metadata.column_by_name(filter.column_name())
            else {
                return false;
            };
            let Some(sst_column) = sst_metadata.column_by_id(expected_column.column_id) else {
                return false;
            };

            if sst_column.column_schema.name != expected_column.column_schema.name
                || sst_column.semantic_type != expected_column.semantic_type
                || sst_column.column_schema.data_type != expected_column.column_schema.data_type
            {
                return false;
            }

            sst_column
        }
        None => {
            let Some(sst_column) = sst_metadata.column_by_name(filter.column_name()) else {
                return false;
            };
            sst_column
        }
    };

    sst_column.semantic_type == SemanticType::Tag
        && sst_metadata
            .primary_key_index(sst_column.column_id)
            .is_some()
}

#[allow(dead_code)]
pub(crate) struct CachedPrimaryKeyFilter {
    inner: Box<dyn PrimaryKeyFilter>,
    last_primary_key: Vec<u8>,
    last_match: Option<bool>,
}

impl CachedPrimaryKeyFilter {
    #[allow(dead_code)]
    pub(crate) fn new(inner: Box<dyn PrimaryKeyFilter>) -> Self {
        Self {
            inner,
            last_primary_key: Vec::new(),
            last_match: None,
        }
    }
}

impl PrimaryKeyFilter for CachedPrimaryKeyFilter {
    fn matches(&mut self, pk: &[u8]) -> bool {
        if let Some(last_match) = self.last_match
            && self.last_primary_key == pk
        {
            return last_match;
        }

        let matched = self.inner.matches(pk);
        self.last_primary_key.clear();
        self.last_primary_key.extend_from_slice(pk);
        self.last_match = Some(matched);
        matched
    }
}

#[allow(dead_code)]
pub(crate) fn batch_single_primary_key(batch: &RecordBatch) -> Result<Option<&[u8]>> {
    let primary_key_index = primary_key_column_index(batch.num_columns());
    let pk_dict_array = batch
        .column(primary_key_index)
        .as_any()
        .downcast_ref::<PrimaryKeyArray>()
        .context(UnexpectedSnafu {
            reason: "Primary key column is not a dictionary array".to_string(),
        })?;
    let pk_values = pk_dict_array
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context(UnexpectedSnafu {
            reason: "Primary key values are not binary array".to_string(),
        })?;
    let keys = pk_dict_array.keys();
    if keys.is_empty() {
        return Ok(None);
    }

    let first_key = keys.value(0);
    if first_key != keys.value(keys.len() - 1) {
        return Ok(None);
    }

    Ok(Some(pk_values.value(first_key as usize)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use api::v1::SemanticType;
    use common_recordbatch::filter::SimpleFilterEvaluator;
    use datafusion_expr::{col, lit};
    use datatypes::arrow::array::{
        ArrayRef, BinaryArray, DictionaryArray, TimestampMillisecondArray, UInt8Array, UInt32Array,
        UInt64Array,
    };
    use datatypes::arrow::datatypes::{Schema, UInt32Type};
    use datatypes::arrow::record_batch::RecordBatch;
    use datatypes::prelude::ConcreteDataType;
    use mito_codec::row_converter::{PrimaryKeyFilter, build_primary_key_codec};
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
    use store_api::storage::ColumnSchema;

    use super::*;
    use crate::sst::internal_fields;
    use crate::sst::parquet::format::ReadFormat;
    use crate::test_util::sst_util::{
        new_primary_key, sst_region_metadata, sst_region_metadata_with_encoding,
    };

    fn new_test_filters(exprs: &[datafusion_expr::Expr]) -> Vec<SimpleFilterEvaluator> {
        exprs
            .iter()
            .filter_map(SimpleFilterEvaluator::try_new)
            .collect()
    }

    fn expected_metadata_with_reused_tag_name(
        old_metadata: &RegionMetadata,
    ) -> Arc<RegionMetadata> {
        let mut builder = RegionMetadataBuilder::new(old_metadata.region_id);
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_0".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 10,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_1".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field_0".to_string(),
                    ConcreteDataType::uint64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts".to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![10, 1]);

        Arc::new(builder.build().unwrap())
    }

    fn new_raw_batch_with_metadata(
        metadata: Arc<RegionMetadata>,
        primary_keys: &[&[u8]],
        field_values: &[u64],
    ) -> RecordBatch {
        assert_eq!(primary_keys.len(), field_values.len());

        let arrow_schema = metadata.schema.arrow_schema();
        let field_column = arrow_schema
            .field(arrow_schema.index_of("field_0").unwrap())
            .clone();
        let time_index_column = arrow_schema
            .field(arrow_schema.index_of("ts").unwrap())
            .clone();
        let mut fields = vec![field_column, time_index_column];
        fields.extend(
            internal_fields()
                .into_iter()
                .map(|field| field.as_ref().clone()),
        );
        let schema = Arc::new(Schema::new(fields));

        let mut dict_values = Vec::new();
        let mut keys = Vec::with_capacity(primary_keys.len());
        for pk in primary_keys {
            let key = dict_values
                .iter()
                .position(|existing: &&[u8]| existing == pk)
                .unwrap_or_else(|| {
                    dict_values.push(*pk);
                    dict_values.len() - 1
                });
            keys.push(key as u32);
        }

        let pk_array: ArrayRef = Arc::new(DictionaryArray::<UInt32Type>::new(
            UInt32Array::from(keys),
            Arc::new(BinaryArray::from_iter_values(dict_values.iter().copied())),
        ));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(field_values.to_vec())),
                Arc::new(TimestampMillisecondArray::from_iter_values(
                    0..primary_keys.len() as i64,
                )),
                pk_array,
                Arc::new(UInt64Array::from(vec![1; primary_keys.len()])),
                Arc::new(UInt8Array::from(vec![1; primary_keys.len()])),
            ],
        )
        .unwrap()
    }

    fn new_raw_batch(primary_keys: &[&[u8]], field_values: &[u64]) -> RecordBatch {
        new_raw_batch_with_metadata(Arc::new(sst_region_metadata()), primary_keys, field_values)
    }

    fn field_values(batch: &RecordBatch) -> Vec<u64> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn test_retain_usable_primary_key_filters_skips_non_tag_filters() {
        let metadata = Arc::new(sst_region_metadata());
        let mut filters =
            new_test_filters(&[col("field_0").eq(lit(1_u64)), col("ts").gt(lit(0_i64))]);

        retain_usable_primary_key_filters(&metadata, None, &mut filters);

        assert!(filters.is_empty());
    }

    #[test]
    fn test_retain_usable_primary_key_filters_skips_reused_expected_tag_name() {
        let metadata = Arc::new(sst_region_metadata());
        let expected_metadata = expected_metadata_with_reused_tag_name(&metadata);
        let mut filters = new_test_filters(&[col("tag_0").eq(lit("b"))]);

        retain_usable_primary_key_filters(
            &metadata,
            Some(expected_metadata.as_ref()),
            &mut filters,
        );

        assert!(filters.is_empty());
    }

    #[test]
    fn test_is_usable_primary_key_filter_skips_legacy_primary_key_batches() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let read_format = ReadFormat::new_flat(
            metadata.clone(),
            metadata.column_metadatas.iter().map(|c| c.column_id),
            None,
            "test",
            true,
        )
        .unwrap();
        assert!(read_format.as_flat().is_some());

        let filter = SimpleFilterEvaluator::try_new(&col("tag_0").eq(lit("b"))).unwrap();
        assert!(is_usable_primary_key_filter(&metadata, None, &filter));
    }

    #[test]
    fn test_prefilter_primary_key_drops_single_dictionary_batch() {
        let metadata = Arc::new(sst_region_metadata());
        let filters = Arc::new(new_test_filters(&[col("tag_0").eq(lit("b"))]));
        let mut primary_key_filter =
            build_primary_key_codec(metadata.as_ref()).primary_key_filter(&metadata, filters);
        let pk_a = new_primary_key(&["a", "x"]);
        let batch = new_raw_batch(&[pk_a.as_slice(), pk_a.as_slice()], &[10, 11]);

        let filtered =
            prefilter_flat_batch_by_primary_key(batch, primary_key_filter.as_mut()).unwrap();

        assert!(filtered.is_none());
    }

    #[test]
    fn test_prefilter_primary_key_builds_mask_for_fragmented_matches() {
        let metadata = Arc::new(sst_region_metadata());
        let filters = Arc::new(new_test_filters(&[col("tag_0")
            .eq(lit("a"))
            .or(col("tag_0").eq(lit("c")))]));
        let mut primary_key_filter =
            build_primary_key_codec(metadata.as_ref()).primary_key_filter(&metadata, filters);
        let pk_a = new_primary_key(&["a", "x"]);
        let pk_b = new_primary_key(&["b", "x"]);
        let pk_c = new_primary_key(&["c", "x"]);
        let pk_d = new_primary_key(&["d", "x"]);
        let batch = new_raw_batch(
            &[
                pk_a.as_slice(),
                pk_a.as_slice(),
                pk_b.as_slice(),
                pk_b.as_slice(),
                pk_c.as_slice(),
                pk_c.as_slice(),
                pk_d.as_slice(),
                pk_d.as_slice(),
            ],
            &[10, 11, 12, 13, 14, 15, 16, 17],
        );

        let filtered = prefilter_flat_batch_by_primary_key(batch, primary_key_filter.as_mut())
            .unwrap()
            .unwrap();

        assert_eq!(filtered.num_rows(), 4);
        assert_eq!(field_values(&filtered), vec![10, 11, 14, 15]);
    }

    struct CountingPrimaryKeyFilter {
        hits: Arc<AtomicUsize>,
        expected: Vec<u8>,
    }

    impl PrimaryKeyFilter for CountingPrimaryKeyFilter {
        fn matches(&mut self, pk: &[u8]) -> bool {
            self.hits.fetch_add(1, Ordering::Relaxed);
            pk == self.expected.as_slice()
        }
    }

    #[test]
    fn test_cached_primary_key_filter_reuses_previous_result() {
        let expected = new_primary_key(&["a", "x"]);
        let hits = Arc::new(AtomicUsize::new(0));
        let mut filter = CachedPrimaryKeyFilter::new(Box::new(CountingPrimaryKeyFilter {
            hits: Arc::clone(&hits),
            expected: expected.clone(),
        }));

        assert!(filter.matches(expected.as_slice()));
        assert!(filter.matches(expected.as_slice()));
        assert!(!filter.matches(new_primary_key(&["b", "x"]).as_slice()));

        assert_eq!(hits.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_batch_single_primary_key() {
        let pk_a = new_primary_key(&["a", "x"]);
        let pk_b = new_primary_key(&["b", "x"]);

        let batch = new_raw_batch(&[pk_a.as_slice(), pk_a.as_slice()], &[10, 11]);
        assert_eq!(
            batch_single_primary_key(&batch).unwrap(),
            Some(pk_a.as_slice())
        );

        let batch = new_raw_batch(&[pk_a.as_slice(), pk_b.as_slice()], &[10, 11]);
        assert_eq!(batch_single_primary_key(&batch).unwrap(), None);
    }
}
