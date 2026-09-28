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

use std::cmp::Ordering;
use std::collections::HashSet;
use std::ops::Range;

use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{col, lit};
use datatypes::arrow::array::{Array, Int64Array, UInt32Array, UInt64Array};
use datatypes::arrow::datatypes::{DataType, SchemaRef};
use futures::TryStreamExt;
use object_store::ObjectStore;
use snafu::{OptionExt, ensure};
use table::predicate::Predicate;

use crate::error::{InvalidRecordBatchSnafu, Result, UnexpectedSnafu};
use crate::series_index::MetricSeriesId;
use crate::sst::parquet::format::column_values_by_type;
use crate::sst::parquet::index_reader::ParquetIndexReader;
use crate::sst::range_index::{
    END_COLUMN, ROW_GROUP_ID_COLUMN, START_COLUMN, TABLE_ID_COLUMN, TSID_COLUMN,
};

/// Searches per-SST range-index files for the rows of candidate metric series.
pub struct SstRangeIndexSearcher {
    reader: ParquetIndexReader,
}

impl SstRangeIndexSearcher {
    /// Opens the range-index file at `path` and loads its Parquet metadata.
    pub async fn open(object_store: ObjectStore, path: &str) -> Result<Self> {
        let reader = ParquetIndexReader::open(object_store, path).await?;
        validate_index_schema(reader.schema())?;
        Ok(Self { reader })
    }

    /// Retains source SST row groups that may match the range-index footer.
    /// Index row-group boundaries are independent of source SST boundaries, so
    /// their source-ID intervals are unioned rather than counted directly.
    pub(crate) fn retain_source_row_groups(
        &self,
        predicate: Option<&Predicate>,
        source_row_groups: &mut Vec<usize>,
    ) {
        // Keep whole expressions: dropping an unsupported branch of an OR
        // would make the estimate incorrectly exclude matching source groups.
        let exprs = predicate
            .into_iter()
            .flat_map(|predicate| predicate.exprs())
            .filter(|expr| {
                let mut columns = HashSet::new();
                expr_to_columns(expr, &mut columns).is_ok()
                    && columns
                        .iter()
                        .all(|column| column.name == TABLE_ID_COLUMN || column.name == TSID_COLUMN)
            })
            .cloned()
            .collect();
        let index_groups = self.reader.row_groups_to_read(&Predicate::new(exprs));
        if index_groups.is_empty() {
            source_row_groups.clear();
            return;
        }
        let Ok(column) = self.reader.schema().index_of(ROW_GROUP_ID_COLUMN) else {
            return;
        };
        let metadata = self.reader.row_group_metadata();
        let Some(mins) = column_values_by_type(metadata, &DataType::UInt32, column, true) else {
            return;
        };
        let Some(maxs) = column_values_by_type(metadata, &DataType::UInt32, column, false) else {
            return;
        };
        let (Some(mins), Some(maxs)) = (
            mins.as_any().downcast_ref::<UInt32Array>(),
            maxs.as_any().downcast_ref::<UInt32Array>(),
        ) else {
            return;
        };
        let mut intervals = Vec::with_capacity(index_groups.len());
        for index in index_groups {
            if mins.is_null(index) || maxs.is_null(index) || mins.value(index) > maxs.value(index) {
                return;
            }
            intervals.push((mins.value(index) as usize, maxs.value(index) as usize));
        }
        // Sort and merge overlapping intervals to avoid counting a source group
        // twice when its series span multiple index row groups.
        intervals.sort_unstable();
        let mut merged: Vec<(usize, usize)> = Vec::with_capacity(intervals.len());
        for (start, end) in intervals {
            if let Some(last) = merged.last_mut()
                && start <= last.1
            {
                last.1 = last.1.max(end);
            } else {
                merged.push((start, end));
            }
        }
        source_row_groups.retain(|group| {
            let position = merged.partition_point(|(start, _)| start <= group);
            position > 0 && *group <= merged[position - 1].1
        });
    }

    /// Returns the row ranges for `series` in one source SST row group.
    ///
    /// `series` is one batch emitted by a
    /// [`MetricSeriesIdStream`](crate::series_index::MetricSeriesIdStream). The
    /// returned half-open ranges are relative to the start of `row_group_id`,
    /// sorted, non-overlapping, and coalesced when adjacent. The number of
    /// returned ranges may be less than the number of input series if some
    /// series don't exist in the row group.
    pub async fn search(
        &self,
        row_group_id: u32,
        series: &[MetricSeriesId],
    ) -> Result<Vec<Range<usize>>> {
        if series.is_empty() {
            return Ok(Vec::new());
        }

        validate_sorted_series(series)?;
        let predicate = search_predicate(row_group_id, series)?;
        let mut batches = self.reader.read(
            &predicate,
            &[
                ROW_GROUP_ID_COLUMN,
                TABLE_ID_COLUMN,
                TSID_COLUMN,
                START_COLUMN,
                END_COLUMN,
            ],
        )?;
        let mut merge = RangeMergeState::new(row_group_id, series);

        while let Some(batch) = batches.try_next().await? {
            if merge.append_batch(&batch)? {
                break;
            }
        }

        Ok(merge.finish())
    }
}

fn validate_sorted_series(series: &[MetricSeriesId]) -> Result<()> {
    if let Some(pair) = series.windows(2).find(|pair| pair[0] > pair[1]) {
        return InvalidRecordBatchSnafu {
            reason: format!(
                "range index search series are not sorted: {:?} appears before {:?}",
                pair[0], pair[1]
            ),
        }
        .fail();
    }
    Ok(())
}

fn search_predicate(row_group_id: u32, series: &[MetricSeriesId]) -> Result<Predicate> {
    let min_table_id = series
        .first()
        .context(UnexpectedSnafu {
            reason: "cannot build a range-index predicate for an empty series set",
        })?
        .table_id;
    let max_table_id = series
        .last()
        .context(UnexpectedSnafu {
            reason: "cannot build a range-index predicate for an empty series set",
        })?
        .table_id;

    Ok(Predicate::new(vec![
        col(ROW_GROUP_ID_COLUMN).eq(lit(row_group_id)),
        col(TABLE_ID_COLUMN).gt_eq(lit(min_table_id)),
        col(TABLE_ID_COLUMN).lt_eq(lit(max_table_id)),
    ]))
}

fn validate_index_schema(schema: &SchemaRef) -> Result<()> {
    for (name, data_type) in [
        (ROW_GROUP_ID_COLUMN, DataType::UInt32),
        (TABLE_ID_COLUMN, DataType::UInt32),
        (TSID_COLUMN, DataType::UInt64),
        (START_COLUMN, DataType::Int64),
        (END_COLUMN, DataType::Int64),
    ] {
        let field = schema
            .field_with_name(name)
            .ok()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!("range index is missing column {name}"),
            })?;
        ensure!(
            field.data_type() == &data_type && !field.is_nullable(),
            InvalidRecordBatchSnafu {
                reason: format!(
                    "range index column {name} must be non-nullable {data_type:?}, got {:?}",
                    field.data_type()
                ),
            }
        );
    }
    Ok(())
}

struct RangeMergeState<'a> {
    /// Source SST row group whose ranges are being searched.
    row_group_id: u32,
    /// Sorted metric series to match against the range index.
    series: &'a [MetricSeriesId],
    /// Cursor to the next series to match.
    series_index: usize,
    /// Last range-index key read, used to validate ordering across batches.
    last_index_key: Option<(u32, MetricSeriesId)>,
    /// Matching row ranges, sorted and coalesced when adjacent.
    ranges: Vec<Range<usize>>,
}

impl<'a> RangeMergeState<'a> {
    fn new(row_group_id: u32, series: &'a [MetricSeriesId]) -> Self {
        Self {
            row_group_id,
            series,
            series_index: 0,
            last_index_key: None,
            ranges: Vec::new(),
        }
    }

    /// Appends matches from `batch` and returns whether the merge is complete.
    fn append_batch(
        &mut self,
        batch: &datatypes::arrow::record_batch::RecordBatch,
    ) -> Result<bool> {
        let row_group_ids = typed_column::<UInt32Array>(batch, ROW_GROUP_ID_COLUMN, "UInt32")?;
        let table_ids = typed_column::<UInt32Array>(batch, TABLE_ID_COLUMN, "UInt32")?;
        let tsids = typed_column::<UInt64Array>(batch, TSID_COLUMN, "UInt64")?;
        let starts = typed_column::<Int64Array>(batch, START_COLUMN, "Int64")?;
        let ends = typed_column::<Int64Array>(batch, END_COLUMN, "Int64")?;

        for row in 0..batch.num_rows() {
            let index_series = MetricSeriesId {
                table_id: table_ids.value(row),
                tsid: tsids.value(row),
            };
            let index_key = (row_group_ids.value(row), index_series);
            ensure!(
                self.last_index_key.is_none_or(|last| last < index_key),
                InvalidRecordBatchSnafu {
                    reason: format!(
                        "range index rows are not strictly sorted: {index_key:?} follows {:?}",
                        self.last_index_key
                    ),
                }
            );
            self.last_index_key = Some(index_key);

            match index_key.0.cmp(&self.row_group_id) {
                Ordering::Less => continue,
                Ordering::Greater => return Ok(true),
                Ordering::Equal => {}
            }

            while self.series_index < self.series.len()
                && self.series[self.series_index] < index_series
            {
                self.advance_series();
            }
            if self.series_index == self.series.len() {
                return Ok(true);
            }

            match self.series[self.series_index].cmp(&index_series) {
                Ordering::Less => {
                    return UnexpectedSnafu {
                        reason: "range-index merge cursor did not advance past a smaller series",
                    }
                    .fail();
                }
                Ordering::Greater => continue,
                Ordering::Equal => {
                    self.append_range(starts.value(row), ends.value(row), row)?;
                    self.advance_series();
                    if self.series_index == self.series.len() {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    fn advance_series(&mut self) {
        let current = self.series[self.series_index];
        while self.series_index < self.series.len() && self.series[self.series_index] == current {
            self.series_index += 1;
        }
    }

    fn append_range(&mut self, start: i64, end: i64, row: usize) -> Result<()> {
        let start = usize::try_from(start).map_err(|_| {
            InvalidRecordBatchSnafu {
                reason: format!("range index contains negative start offset at row {row}"),
            }
            .build()
        })?;
        let end = usize::try_from(end).map_err(|_| {
            InvalidRecordBatchSnafu {
                reason: format!("range index contains negative end offset at row {row}"),
            }
            .build()
        })?;
        ensure!(
            start < end,
            InvalidRecordBatchSnafu {
                reason: format!("range index contains invalid range {start}..{end} at row {row}"),
            }
        );

        if let Some(last) = self.ranges.last_mut() {
            ensure!(
                start >= last.end,
                InvalidRecordBatchSnafu {
                    reason: format!(
                        "range index contains overlapping or unsorted range {start}..{end} after {}..{}",
                        last.start, last.end
                    ),
                }
            );
            if start == last.end {
                last.end = end;
                return Ok(());
            }
        }
        self.ranges.push(start..end);
        Ok(())
    }

    fn finish(self) -> Vec<Range<usize>> {
        self.ranges
    }
}

fn typed_column<'a, T: 'static>(
    batch: &'a datatypes::arrow::record_batch::RecordBatch,
    name: &str,
    data_type: &str,
) -> Result<&'a T> {
    let index = batch
        .schema()
        .index_of(name)
        .ok()
        .with_context(|| InvalidRecordBatchSnafu {
            reason: format!("range index batch is missing column {name}"),
        })?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .with_context(|| InvalidRecordBatchSnafu {
            reason: format!("range index column {name} is not {data_type}"),
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{ArrayRef, BinaryArray};
    use datatypes::arrow::datatypes::{Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use object_store::services::Memory;
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::RegionMetadataRef;
    use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

    use super::*;
    use crate::sst::range_index::{
        SstRangeIndexWriter, SstRangeIndexWriterOptions, range_index_schema,
    };
    use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};

    fn object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap()
    }

    fn series(table_id: u32, tsid: u64) -> MetricSeriesId {
        MetricSeriesId { table_id, tsid }
    }

    fn primary_key_batch(metadata: &RegionMetadataRef, ids: &[(u32, u64)]) -> RecordBatch {
        let primary_keys = ids
            .iter()
            .map(|(table_id, tsid)| new_sparse_primary_key(&["a", "x"], metadata, *table_id, *tsid))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![Field::new(
            PRIMARY_KEY_COLUMN_NAME,
            DataType::Binary,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(BinaryArray::from_iter_values(
                primary_keys.iter().map(Vec::as_slice),
            ))],
        )
        .unwrap()
    }

    async fn write_index(store: &ObjectStore, path: &str) {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let mut writer = SstRangeIndexWriter::try_new(
            metadata.clone(),
            store.clone(),
            path,
            SstRangeIndexWriterOptions {
                index_row_group_size: 2,
            },
        )
        .await
        .unwrap();
        writer
            .write(
                0,
                &primary_key_batch(
                    &metadata,
                    &[(1, 10), (1, 10), (1, 20), (2, 10), (2, 20), (2, 20)],
                ),
            )
            .await
            .unwrap();
        writer
            .write(1, &primary_key_batch(&metadata, &[(2, 20), (2, 20)]))
            .await
            .unwrap();
        writer.finish().await.unwrap();
    }

    #[tokio::test]
    async fn search_filters_exact_series_pairs_and_coalesces_ranges() {
        let store = object_store();
        let path = "range-search.parquet";
        write_index(&store, path).await;
        let searcher = SstRangeIndexSearcher::open(store, path).await.unwrap();

        let ranges = searcher
            .search(0, &[series(1, 10), series(2, 20)])
            .await
            .unwrap();
        assert_eq!(ranges, vec![0..2, 4..6]);

        let ranges = searcher
            .search(0, &[series(1, 10), series(1, 20)])
            .await
            .unwrap();
        assert_eq!(ranges, vec![0..3]);

        let ranges = searcher
            .search(0, &[series(1, 15), series(2, 20)])
            .await
            .unwrap();
        assert_eq!(ranges, vec![4..6]);

        let ranges = searcher
            .search(0, &[series(1, 10), series(2, 30)])
            .await
            .unwrap();
        assert_eq!(ranges, vec![0..2]);

        let ranges = searcher
            .search(1, &[series(2, 20), series(2, 20)])
            .await
            .unwrap();
        assert_eq!(ranges, vec![0..2]);

        assert!(
            searcher
                .search(1, &[series(1, 10)])
                .await
                .unwrap()
                .is_empty()
        );

        assert!(searcher.search(0, &[]).await.unwrap().is_empty());

        let error = searcher
            .search(0, &[series(2, 20), series(1, 10)])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not sorted"), "{error}");
    }

    #[tokio::test]
    async fn opening_a_missing_index_fails() {
        assert!(
            SstRangeIndexSearcher::open(object_store(), "does-not-exist.parquet")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn footer_estimate_counts_source_groups_without_reading_pages() {
        let store = object_store();
        let path = "range-footer.parquet";
        write_index(&store, path).await;
        let searcher = SstRangeIndexSearcher::open(store.clone(), path)
            .await
            .unwrap();
        // Only the already-opened footer is needed by the estimate.
        store.delete(path).await.unwrap();

        let mut groups = vec![0, 1];
        searcher.retain_source_row_groups(None, &mut groups);
        assert_eq!(groups, vec![0, 1]);

        let mut groups = vec![0, 1];
        searcher.retain_source_row_groups(
            Some(&Predicate::new(vec![col(TABLE_ID_COLUMN).eq(lit(1u32))])),
            &mut groups,
        );
        assert_eq!(groups, vec![0]);

        let mut groups = vec![1];
        searcher.retain_source_row_groups(
            Some(&Predicate::new(vec![col(TABLE_ID_COLUMN).eq(lit(1u32))])),
            &mut groups,
        );
        assert!(groups.is_empty());

        let mut groups = vec![0, 1];
        searcher.retain_source_row_groups(
            Some(&Predicate::new(vec![col(TSID_COLUMN).gt(lit(100u64))])),
            &mut groups,
        );
        assert!(groups.is_empty());

        let mut groups = vec![0, 1];
        searcher.retain_source_row_groups(
            Some(&Predicate::new(vec![
                col(TABLE_ID_COLUMN)
                    .eq(lit(1u32))
                    .or(col("unknown_tag").eq(lit("matches"))),
            ])),
            &mut groups,
        );
        assert_eq!(groups, vec![0, 1]);
    }

    #[tokio::test]
    async fn footer_estimate_handles_overlapping_intervals_and_missing_statistics() {
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::{EnabledStatistics, WriterProperties};

        for statistics in [EnabledStatistics::Chunk, EnabledStatistics::None] {
            let batch = RecordBatch::try_new(
                range_index_schema(),
                vec![
                    Arc::new(UInt32Array::from(vec![0, 0, 0, 1, 2])) as ArrayRef,
                    Arc::new(UInt32Array::from(vec![1, 1, 2, 2, 1])),
                    Arc::new(UInt64Array::from(vec![10, 20, 10, 10, 10])),
                    Arc::new(Int64Array::from(vec![0, 1, 2, 0, 0])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 1, 1])),
                ],
            )
            .unwrap();
            let properties = WriterProperties::builder()
                .set_max_row_group_row_count(Some(2))
                .set_statistics_enabled(statistics)
                .build();
            let mut bytes = Vec::new();
            let mut writer =
                ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            let store = object_store();
            store.write("footer.parquet", bytes).await.unwrap();
            let searcher = SstRangeIndexSearcher::open(store.clone(), "footer.parquet")
                .await
                .unwrap();
            store.delete("footer.parquet").await.unwrap();

            let mut groups = vec![0, 1, 2];
            searcher.retain_source_row_groups(None, &mut groups);
            assert_eq!(groups, vec![0, 1, 2]);
            let mut groups = vec![0, 1, 2];
            searcher.retain_source_row_groups(
                Some(&Predicate::new(vec![col(TABLE_ID_COLUMN).eq(lit(1u32))])),
                &mut groups,
            );
            assert_eq!(
                groups,
                if statistics == EnabledStatistics::None {
                    vec![0, 1, 2]
                } else {
                    vec![0, 2]
                }
            );
        }
    }

    #[tokio::test]
    async fn pruning_uses_the_source_row_group_and_table_id_range() {
        let store = object_store();
        let path = "range-pruning.parquet";
        write_index(&store, path).await;
        let reader = ParquetIndexReader::open(store, path).await.unwrap();
        let predicate = search_predicate(0, &[series(1, 999), series(2, 999)]).unwrap();

        assert_eq!(reader.row_groups_to_read(&predicate), vec![0, 1]);
    }

    #[test]
    fn validates_schema_and_range_offsets() {
        let nullable_schema = Arc::new(Schema::new(vec![
            Field::new(ROW_GROUP_ID_COLUMN, DataType::UInt32, false),
            Field::new(TABLE_ID_COLUMN, DataType::UInt32, false),
            Field::new(TSID_COLUMN, DataType::UInt64, false),
            Field::new(START_COLUMN, DataType::Int64, true),
            Field::new(END_COLUMN, DataType::Int64, false),
        ]));
        assert!(validate_index_schema(&nullable_schema).is_err());

        let batch = RecordBatch::try_new(
            range_index_schema(),
            vec![
                Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![1])),
                Arc::new(UInt64Array::from(vec![10])),
                Arc::new(Int64Array::from(vec![-1])),
                Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .unwrap();
        let selected = [series(1, 10)];
        let mut merge = RangeMergeState::new(0, &selected);
        assert!(merge.append_batch(&batch).is_err());

        let unsorted_batch = RecordBatch::try_new(
            range_index_schema(),
            vec![
                Arc::new(UInt32Array::from(vec![0, 0])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![1, 1])),
                Arc::new(UInt64Array::from(vec![20, 10])),
                Arc::new(Int64Array::from(vec![0, 1])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let selected = [series(1, 20), series(1, 30)];
        let mut merge = RangeMergeState::new(0, &selected);
        assert!(merge.append_batch(&unsorted_batch).is_err());

        let make_batch = |tsid, start, end| {
            RecordBatch::try_new(
                range_index_schema(),
                vec![
                    Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
                    Arc::new(UInt32Array::from(vec![1])),
                    Arc::new(UInt64Array::from(vec![tsid])),
                    Arc::new(Int64Array::from(vec![start])),
                    Arc::new(Int64Array::from(vec![end])),
                ],
            )
            .unwrap()
        };
        let selected = [series(1, 10), series(1, 20)];
        let mut merge = RangeMergeState::new(0, &selected);
        assert!(!merge.append_batch(&make_batch(10, 0, 1)).unwrap());
        assert!(merge.append_batch(&make_batch(20, 1, 2)).unwrap());
        assert_eq!(merge.finish(), vec![0..2]);
    }
}
