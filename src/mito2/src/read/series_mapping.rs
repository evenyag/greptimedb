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

//! Source-schema primary keys and absolute row-group runs for two-phase scans.

use std::collections::HashMap;
use std::mem;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datatypes::arrow::array::{
    Array, ArrayBuilder, ArrayRef, BinaryArray, BinaryBuilder, DictionaryArray, UInt32Array,
};
use datatypes::arrow::compute::take;
use datatypes::arrow::datatypes::{FieldRef, Schema, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use parquet::arrow::arrow_reader::RowSelection;
use snafu::{OptionExt, ResultExt, ensure};

use crate::error::{
    ComputeArrowSnafu, DecodeSnafu, NewRecordBatchSnafu, ReserveSeriesScanMemorySnafu, Result,
    UnexpectedSnafu,
};
use crate::read::memory_diagnostics::{self, MemoryUsage};
use crate::series_index::MetricSeriesId;

/// Query-local full keys retained after candidate merging, once per selected series.
/// SST and range-index mapping caches never own this payload.
#[derive(Debug)]
pub(crate) struct SeriesPrimaryKeys {
    inner: Mutex<(
        HashMap<MetricSeriesId, Vec<u8>>,
        MemoryReservation,
        MemoryUsage,
    )>,
}

impl SeriesPrimaryKeys {
    pub(crate) fn new(pool: &Arc<dyn MemoryPool>) -> Self {
        Self {
            inner: Mutex::new((
                HashMap::new(),
                MemoryConsumer::new("SeriesPrimaryKeys").register(pool),
                MemoryUsage::new("candidate", "primary_keys"),
            )),
        }
    }

    pub(crate) fn insert(&self, series: MetricSeriesId, key: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let (keys, reservation, diagnostics) = &mut *inner;
        if let std::collections::hash_map::Entry::Vacant(entry) = keys.entry(series) {
            reservation
                .try_grow(key.len() + 2 * mem::size_of::<(MetricSeriesId, Vec<u8>)>())
                .context(ReserveSeriesScanMemorySnafu)?;
            entry.insert(key.to_vec());
            diagnostics.set("primary_key_map", reservation.size());
        }
        Ok(())
    }

    /// Copies only the keys needed by the current row-group reader.
    pub(crate) fn select(
        &self,
        series: impl IntoIterator<Item = MetricSeriesId>,
    ) -> Result<BinaryArray> {
        let inner = self.inner.lock().unwrap();
        let mut builder = BinaryBuilder::new();
        for series in series {
            let key = inner.0.get(&series).context(UnexpectedSnafu {
                reason: "selected series has no retained candidate primary key",
            })?;
            builder.append_value(key);
        }
        Ok(builder.finish())
    }
}

/// One contiguous series run in absolute row-group coordinates.
#[derive(Debug, Clone)]
pub(crate) struct SeriesRowRange {
    pub(crate) series: MetricSeriesId,
    pub(crate) rows: Range<usize>,
}

/// Complete, unfiltered series-to-row mapping for an immutable SST row group.
/// Cached mappings never retain encoded primary keys or decoded tags.
pub(crate) struct SeriesRowMapping {
    pub(crate) runs: Vec<SeriesRowRange>,
    _diagnostics: MemoryUsage,
}

impl SeriesRowMapping {
    pub(crate) fn new(runs: Vec<SeriesRowRange>) -> Self {
        let mut diagnostics = MemoryUsage::new("mapping", "row_mapping");
        diagnostics.set(
            "row_runs",
            runs.capacity() * mem::size_of::<SeriesRowRange>(),
        );
        Self {
            runs,
            _diagnostics: diagnostics,
        }
    }

    pub(crate) fn estimated_size(&self) -> usize {
        mem::size_of::<Self>() + self.runs.capacity() * mem::size_of::<SeriesRowRange>()
    }

    /// Selects source runs for an optional sorted series assignment and intersects
    /// them with absolute row-group pruning selections.
    pub(crate) fn select(
        &self,
        series: Option<&[MetricSeriesId]>,
        selection: Option<&RowSelection>,
    ) -> Vec<(usize, Range<usize>)> {
        let mut cursor = 0;
        let mut runs = Vec::new();
        for (key, run) in self.runs.iter().enumerate() {
            if let Some(series) = series {
                while cursor < series.len() && series[cursor] < run.series {
                    cursor += 1;
                }
                if cursor == series.len() {
                    break;
                }
                if series[cursor] != run.series {
                    continue;
                }
            }
            runs.push((key, run.rows.clone()));
        }
        if let Some(selection) = selection {
            let mut intervals = Vec::new();
            let mut offset = 0;
            for selector in selection.iter() {
                if !selector.skip {
                    intervals.push(offset..offset + selector.row_count);
                }
                offset += selector.row_count;
            }
            let mut selected = Vec::new();
            let mut cursor = 0;
            for (key, run) in runs {
                while cursor < intervals.len() && intervals[cursor].end <= run.start {
                    cursor += 1;
                }
                let mut i = cursor;
                while i < intervals.len() && intervals[i].start < run.end {
                    let rows = run.start.max(intervals[i].start)..run.end.min(intervals[i].end);
                    if !rows.is_empty() {
                        selected.push((key, rows));
                    }
                    i += 1;
                }
            }
            selected
        } else {
            runs
        }
    }
}

/// Builds run-length metadata without retaining a primary key per data row.
#[derive(Default)]
pub(crate) struct SeriesRowMappingBuilder {
    last_key: Vec<u8>,
    runs: Vec<SeriesRowRange>,
    rows: usize,
}

impl SeriesRowMappingBuilder {
    pub(crate) fn append(&mut self, array: &ArrayRef) -> Result<()> {
        let (values, indices) =
            if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
                (dict.values(), Some(dict.keys()))
            } else {
                (array, None)
            };
        let values = values
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context(UnexpectedSnafu {
                reason: "series primary keys must be binary or dictionary binary",
            })?;
        ensure!(
            array.null_count() == 0 && values.null_count() == 0,
            UnexpectedSnafu {
                reason: "series primary keys contain nulls",
            }
        );
        let codec = SparsePrimaryKeyCodec::schemaless();
        for row in 0..array.len() {
            let index = indices.map_or(row, |keys| keys.value(row) as usize);
            let key = values.value(index);
            if self.runs.is_empty() || self.last_key != key {
                ensure!(
                    self.runs.is_empty() || self.last_key.as_slice() <= key,
                    UnexpectedSnafu {
                        reason: "series primary keys are not sorted",
                    }
                );
                let (table_id, tsid) = codec.decode_ids(key).context(DecodeSnafu)?;
                self.runs.push(SeriesRowRange {
                    series: MetricSeriesId { table_id, tsid },
                    rows: self.rows..self.rows,
                });
                self.last_key.clear();
                self.last_key.extend_from_slice(key);
            }
            self.rows += 1;
            if let Some(last) = self.runs.last_mut() {
                last.rows.end = self.rows;
            }
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> SeriesRowMapping {
        SeriesRowMapping::new(self.runs)
    }
}

/// Expands supplied series keys and decoded tags over selected absolute runs.
/// Both SST-key discovery and series-index discovery can supply this input.
/// Reconstruction happens before any row-level filtering.
pub(crate) struct SeriesBatchCursor {
    keys: BinaryArray,
    tags: RecordBatch,
    runs: Vec<(usize, Range<usize>)>,
    position: usize,
    consumed: usize,
    _reservation: MemoryReservation,
    _diagnostics: MemoryUsage,
}

impl SeriesBatchCursor {
    pub(crate) fn try_new(
        keys: BinaryArray,
        tags: RecordBatch,
        runs: Vec<(usize, Range<usize>)>,
        pool: &Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        ensure!(
            keys.len() == tags.num_rows()
                && runs
                    .iter()
                    .all(|(key, rows)| *key < keys.len() && !rows.is_empty()),
            UnexpectedSnafu {
                reason: "invalid series keys, tags or row runs"
            }
        );
        let reservation = MemoryConsumer::new("SeriesBatchCursor").register(pool);
        reservation
            .try_grow(keys.get_array_memory_size() + tags.get_array_memory_size())
            .context(ReserveSeriesScanMemorySnafu)?;
        let mut diagnostics = MemoryUsage::new("cursor", "cursor");
        diagnostics.batch(&tags, tags.num_columns());
        diagnostics.set("pk_values", keys.get_array_memory_size());
        diagnostics.set(
            "row_runs",
            runs.capacity() * mem::size_of::<(usize, Range<usize>)>(),
        );
        Ok(Self {
            _diagnostics: diagnostics,
            keys,
            tags,
            runs,
            position: 0,
            consumed: 0,
            _reservation: reservation,
        })
    }

    pub(crate) fn tag_schema(&self) -> datatypes::arrow::datatypes::SchemaRef {
        self.tags.schema()
    }

    pub(crate) fn row_ranges(&self) -> impl Iterator<Item = Range<usize>> + '_ {
        self.runs.iter().map(|(_, rows)| rows.clone())
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.position == self.runs.len()
    }

    /// Adds keys and tags to a data-only batch, using one cursor for all columns.
    pub(crate) fn materialize(
        &mut self,
        batch: RecordBatch,
        pk_field: FieldRef,
    ) -> Result<RecordBatch> {
        let mut tag_indices = Vec::with_capacity(batch.num_rows());
        let mut key_indices = Vec::with_capacity(batch.num_rows());
        let mut keys = BinaryBuilder::new();
        let mut last_key = None;
        let mut value_index = 0;
        while key_indices.len() < batch.num_rows() {
            let (key, rows) = self.runs.get(self.position).context(UnexpectedSnafu {
                reason: "data reader returned more rows than the series mapping",
            })?;
            let count = (rows.len() - self.consumed).min(batch.num_rows() - key_indices.len());
            if last_key != Some(*key) {
                value_index = u32::try_from(keys.len()).map_err(|_| {
                    UnexpectedSnafu {
                        reason: "too many series keys in an output batch",
                    }
                    .build()
                })?;
                keys.append_value(self.keys.value(*key));
                last_key = Some(*key);
            }
            let tag_index = u32::try_from(*key).map_err(|_| {
                UnexpectedSnafu {
                    reason: "too many series tags in an input batch",
                }
                .build()
            })?;
            key_indices.extend(std::iter::repeat_n(value_index, count));
            tag_indices.extend(std::iter::repeat_n(tag_index, count));
            self.consumed += count;
            if self.consumed == rows.len() {
                self.position += 1;
                self.consumed = 0;
            }
        }
        let tags = UInt32Array::from(tag_indices);
        let mut columns = self
            .tags
            .columns()
            .iter()
            .map(|column| take(column, &tags, None).context(ComputeArrowSnafu))
            .collect::<Result<Vec<_>>>()?;
        columns.extend_from_slice(batch.columns());
        let pk: ArrayRef = Arc::new(
            DictionaryArray::<UInt32Type>::try_new(
                UInt32Array::from(key_indices),
                Arc::new(keys.finish()),
            )
            .context(NewRecordBatchSnafu)?,
        );
        let pk_position = columns.len() - 2;
        columns.insert(pk_position, pk);
        let mut fields = self.tags.schema().fields().to_vec();
        fields.extend(batch.schema().fields().iter().cloned());
        fields.insert(pk_position, pk_field);
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        ));
        let result = RecordBatch::try_new(schema, columns).context(NewRecordBatchSnafu)?;
        for (component, bytes) in
            memory_diagnostics::batch_components(&result, self.tags.num_columns())
        {
            memory_diagnostics::produced("materialized", component, bytes);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use datatypes::arrow::array::UInt32Array;
    use parquet::arrow::arrow_reader::RowSelector;

    use super::*;

    fn key(table_id: u32, tsid: u64) -> Vec<u8> {
        let mut key = Vec::new();
        SparsePrimaryKeyCodec::schemaless()
            .encode_internal(table_id, tsid, &mut key)
            .unwrap();
        key
    }

    fn binary(keys: &[&[u8]]) -> ArrayRef {
        Arc::new(BinaryArray::from_iter_values(keys.iter().copied()))
    }

    #[test]
    fn query_keys_are_deduplicated_and_released_with_the_query() {
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        let keys = SeriesPrimaryKeys::new(&pool);
        let series = MetricSeriesId {
            table_id: 1,
            tsid: 7,
        };
        let encoded = key(1, 7);
        keys.insert(series, &encoded).unwrap();
        let reserved = pool.reserved();
        for _ in 0..10 {
            keys.insert(series, &encoded).unwrap();
        }
        assert_eq!(pool.reserved(), reserved);
        assert_eq!(keys.select([series]).unwrap().value(0), encoded);
        assert!(
            keys.select([MetricSeriesId {
                table_id: 2,
                tsid: 7
            }])
            .is_err()
        );
        drop(keys);
        assert_eq!(pool.reserved(), 0);
        let small: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        assert!(
            SeriesPrimaryKeys::new(&small)
                .insert(series, &encoded)
                .is_err()
        );
        assert_eq!(small.reserved(), 0);
    }

    #[test]
    fn cached_mapping_size_is_independent_of_full_key_size() {
        let short = key(1, 7);
        let mut long = short.clone();
        long.extend(std::iter::repeat_n(1, 64 * 1024));
        let build = |key: &[u8]| {
            let mut builder = SeriesRowMappingBuilder::default();
            builder.append(&binary(&[key, key])).unwrap();
            builder.finish()
        };
        let small = build(&short);
        let large = build(&long);
        assert_eq!(small.estimated_size(), large.estimated_size());
        assert_eq!(
            large.runs[0].series,
            MetricSeriesId {
                table_id: 1,
                tsid: 7
            }
        );
        assert_eq!(large.runs[0].rows, 0..2);
    }

    #[test]
    fn runs_span_binary_and_dictionary_batches() {
        let a = key(1, 7);
        let b = key(2, 7);
        let c = key(2, 9);
        let mut builder = SeriesRowMappingBuilder::default();
        builder.append(&binary(&[&a, &a])).unwrap();
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<UInt32Type>::try_new(
                UInt32Array::from(vec![1, 0, 0, 2]),
                binary(&[&b, &a, &c]),
            )
            .unwrap(),
        );
        builder.append(&dictionary).unwrap();
        let mapping = builder.finish();
        assert_eq!(
            vec![0..3, 3..5, 5..6],
            mapping
                .runs
                .iter()
                .map(|run| run.rows.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(mapping.runs.len(), 3);
        assert_eq!(
            vec![(1, 3..5)],
            mapping.select(
                Some(&[MetricSeriesId {
                    table_id: 2,
                    tsid: 7
                }]),
                None
            )
        );
    }

    #[test]
    fn selects_absolute_runs() {
        let a = key(1, 7);
        let b = key(2, 7);
        let mut builder = SeriesRowMappingBuilder::default();
        builder
            .append(&binary(&[&a, &a, &a, &a, &a, &b, &b, &b]))
            .unwrap();
        let mapping = builder.finish();
        let selection = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(2),
            RowSelector::skip(1),
            RowSelector::select(3),
            RowSelector::skip(1),
        ]);
        assert_eq!(
            mapping.select(None, Some(&selection)),
            vec![(0, 1..3), (0, 4..5), (1, 5..7)]
        );
        let empty = RowSelection::from(vec![RowSelector::skip(8)]);
        assert!(mapping.select(None, Some(&empty)).is_empty());
    }

    #[test]
    fn materializes_keys_and_nullable_tags_across_run_and_batch_boundaries() {
        use datafusion::execution::memory_pool::UnboundedMemoryPool;
        use datatypes::arrow::array::{StringArray, UInt8Array, UInt64Array};
        use datatypes::arrow::datatypes::{DataType, Field};
        let a = key(1, 7);
        let b = key(2, 7);
        let keys = BinaryArray::from_iter_values([a.as_slice(), b.as_slice()]);
        let tags = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("tag", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
        )
        .unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let mut cursor =
            SeriesBatchCursor::try_new(keys, tags, vec![(0, 1..3), (0, 4..5), (1, 5..7)], &pool)
                .unwrap();
        let pk_field = Arc::new(Field::new(
            "__primary_key",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
            false,
        ));
        let input = |rows| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("ts", DataType::UInt64, false),
                    Field::new("__sequence", DataType::UInt64, false),
                    Field::new("__op_type", DataType::UInt8, false),
                ])),
                vec![
                    Arc::new(UInt64Array::from_value(0, rows)),
                    Arc::new(UInt64Array::from_value(1, rows)),
                    Arc::new(UInt8Array::from_value(1, rows)),
                ],
            )
            .unwrap()
        };
        let mut actual_keys = Vec::new();
        let mut actual_tags = Vec::new();
        for rows in [2, 2, 1] {
            let batch = cursor.materialize(input(rows), pk_field.clone()).unwrap();
            let keys = batch
                .column(2)
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
                .unwrap();
            let values = keys
                .values()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            for row in 0..rows {
                actual_keys.push(values.value(keys.keys().value(row) as usize).to_vec());
                let tag = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                actual_tags.push((!tag.is_null(row)).then(|| tag.value(row).to_owned()));
            }
        }
        assert!(cursor.is_finished());
        assert_eq!(actual_keys, vec![a.clone(), a.clone(), a, b.clone(), b]);
        assert_eq!(
            actual_tags,
            vec![
                Some("a".into()),
                Some("a".into()),
                Some("a".into()),
                None,
                None
            ]
        );
        assert!(cursor.materialize(input(1), pk_field).is_err());
        drop(cursor);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn rejects_unsorted_or_null_source_keys() {
        let a = key(1, 7);
        let b = key(2, 7);
        let mut builder = SeriesRowMappingBuilder::default();
        builder.append(&binary(&[&b])).unwrap();
        assert!(builder.append(&binary(&[&a])).is_err());
        let null: ArrayRef = Arc::new(BinaryArray::from(vec![None::<&[u8]>]));
        assert!(SeriesRowMappingBuilder::default().append(&null).is_err());
    }
}
