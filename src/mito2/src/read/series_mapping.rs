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

use std::mem;
use std::ops::Range;
use std::sync::Arc;

use datafusion::execution::memory_pool::MemoryReservation;
use datatypes::arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, DictionaryArray};
use datatypes::arrow::datatypes::UInt32Type;
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use parquet::arrow::arrow_reader::RowSelection;
use snafu::{OptionExt, ResultExt, ensure};

use crate::error::{DecodeSnafu, ReserveSeriesScanMemorySnafu, Result, UnexpectedSnafu};
use crate::series_index::MetricSeriesId;

/// One contiguous series run in absolute row-group coordinates.
#[derive(Debug, Clone)]
pub(crate) struct SeriesRowRange {
    pub(crate) series: MetricSeriesId,
    pub(crate) rows: Range<usize>,
}

/// Complete, unfiltered primary-key mapping for an immutable SST row group.
/// Each run has one key in `primary_keys`, in the same order.
pub(crate) struct SeriesRowGroup {
    pub(crate) primary_keys: BinaryArray,
    pub(crate) runs: Vec<SeriesRowRange>,
}

impl SeriesRowGroup {
    pub(crate) fn estimated_size(&self) -> usize {
        mem::size_of::<Self>()
            + self.primary_keys.get_array_memory_size()
            + self.runs.capacity() * mem::size_of::<SeriesRowRange>()
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

    /// Returns one full source key per selected run, without expanding data rows.
    pub(crate) fn candidate_keys(&self, selection: Option<&RowSelection>) -> ArrayRef {
        let mut keys = BinaryBuilder::new();
        let mut last = None;
        for (key, _) in self.select(None, selection) {
            if last != Some(key) {
                keys.append_value(self.primary_keys.value(key));
                last = Some(key);
            }
        }
        Arc::new(keys.finish())
    }
}

/// Builds run-length metadata without retaining a primary key per data row.
#[derive(Default)]
pub(crate) struct SeriesRowGroupBuilder {
    keys: BinaryBuilder,
    last_key: Vec<u8>,
    runs: Vec<SeriesRowRange>,
    rows: usize,
}

impl SeriesRowGroupBuilder {
    pub(crate) fn append(
        &mut self,
        array: &ArrayRef,
        reservation: Option<&MemoryReservation>,
    ) -> Result<()> {
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
                if let Some(reservation) = reservation {
                    // Allow for geometric buffer growth plus the last-key scratch
                    // buffer. Charge per run, not per repeated data row.
                    reservation
                        .try_grow(3 * key.len() + 2 * mem::size_of::<SeriesRowRange>() + 16)
                        .context(ReserveSeriesScanMemorySnafu)?;
                }
                let (table_id, tsid) = codec.decode_ids(key).context(DecodeSnafu)?;
                self.keys.append_value(key);
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

    pub(crate) fn finish(mut self) -> SeriesRowGroup {
        SeriesRowGroup {
            primary_keys: self.keys.finish(),
            runs: self.runs,
        }
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
    fn runs_span_binary_and_dictionary_batches() {
        let a = key(1, 7);
        let b = key(2, 7);
        let c = key(2, 9);
        let mut builder = SeriesRowGroupBuilder::default();
        builder.append(&binary(&[&a, &a]), None).unwrap();
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<UInt32Type>::try_new(
                UInt32Array::from(vec![1, 0, 0, 2]),
                binary(&[&b, &a, &c]),
            )
            .unwrap(),
        );
        builder.append(&dictionary, None).unwrap();
        let mapping = builder.finish();
        assert_eq!(
            vec![0..3, 3..5, 5..6],
            mapping
                .runs
                .iter()
                .map(|run| run.rows.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(mapping.primary_keys.len(), 3);
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
    fn selects_absolute_runs_and_deduplicates_candidate_keys() {
        let a = key(1, 7);
        let b = key(2, 7);
        let mut builder = SeriesRowGroupBuilder::default();
        builder
            .append(&binary(&[&a, &a, &a, &a, &a, &b, &b, &b]), None)
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
        let candidates = mapping.candidate_keys(Some(&selection));
        assert_eq!(candidates.as_ref(), binary(&[&a, &b]).as_ref());
        let empty = RowSelection::from(vec![RowSelector::skip(8)]);
        assert_eq!(mapping.candidate_keys(Some(&empty)).len(), 0);
    }

    #[test]
    fn rejects_mapping_when_memory_budget_is_exhausted() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let reservation = MemoryConsumer::new("test").register(&pool);
        assert!(
            SeriesRowGroupBuilder::default()
                .append(&binary(&[&key(1, 1)]), Some(&reservation))
                .is_err()
        );
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn rejects_unsorted_or_null_source_keys() {
        let a = key(1, 7);
        let b = key(2, 7);
        let mut builder = SeriesRowGroupBuilder::default();
        builder.append(&binary(&[&b]), None).unwrap();
        assert!(builder.append(&binary(&[&a]), None).is_err());
        let null: ArrayRef = Arc::new(BinaryArray::from(vec![None::<&[u8]>]));
        assert!(
            SeriesRowGroupBuilder::default()
                .append(&null, None)
                .is_err()
        );
    }
}
