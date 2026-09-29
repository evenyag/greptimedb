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
use std::collections::{BTreeMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_expr::utils::expr_to_columns;
use datatypes::arrow::array::{Array, Int64Array, UInt32Array, UInt64Array};
use datatypes::arrow::datatypes::{DataType, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt, ensure};
use table::predicate::Predicate;

use crate::cache::{CacheStrategy, RangeResultKey, RangeResultValue};
use crate::error::{
    InvalidRecordBatchSnafu, ReserveRangeIndexMemorySnafu, Result, UnexpectedSnafu,
};
use crate::read::series_mapping::SeriesRowRange;
use crate::series_index::MetricSeriesId;
use crate::sst::file::RegionFileId;
use crate::sst::parquet::index_reader::ParquetIndexReader;
use crate::sst::range_index::{
    END_COLUMN, ROW_GROUP_ID_COLUMN, START_COLUMN, TABLE_ID_COLUMN, TSID_COLUMN,
};

/// Query-scoped range-index batches shared by all series assignments for an SST.
pub struct SstRangeIndexSearcher {
    /// Zero-copy slices grouped by source SST row group.
    data: Arc<SstRangeIndexData>,
    _reservation: MemoryReservation,
}

/// Immutable index buffers, independent of query memory reservations and predicates.
pub(crate) struct SstRangeIndexData {
    batches: BTreeMap<u32, Vec<RecordBatch>>,
    size: usize,
}

impl SstRangeIndexData {
    pub(crate) fn estimated_size(&self) -> usize {
        self.size
    }
}

impl SstRangeIndexSearcher {
    /// Loads index batches pruned by the scan-wide predicate. Assignment-specific
    /// series filters must only be applied by `search`, so every partition can reuse this data.
    pub async fn open(
        object_store: ObjectStore,
        path: &str,
        predicate: Option<&Predicate>,
        memory_pool: &Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        let reader = ParquetIndexReader::open(object_store, path).await?;
        validate_index_schema(reader.schema())?;
        let mut stream = reader.read(
            &index_predicate(predicate),
            &[
                ROW_GROUP_ID_COLUMN,
                TABLE_ID_COLUMN,
                TSID_COLUMN,
                START_COLUMN,
                END_COLUMN,
            ],
        )?;
        let reservation = MemoryConsumer::new("SstRangeIndexSearcher").register(memory_pool);
        let mut batches: BTreeMap<u32, Vec<RecordBatch>> = BTreeMap::new();
        let mut last_group = None;
        while let Some(batch) = stream.try_next().await? {
            // Charge the buffers once before retaining slices that share them.
            reservation
                .try_grow(batch.get_array_memory_size())
                .context(ReserveRangeIndexMemorySnafu)?;
            let groups = typed_column::<UInt32Array>(&batch, ROW_GROUP_ID_COLUMN, "UInt32")?;
            let mut start = 0;
            while start < batch.num_rows() {
                let group = groups.value(start);
                ensure!(
                    last_group.is_none_or(|last| last <= group),
                    InvalidRecordBatchSnafu {
                        reason: "range index source row groups are not sorted",
                    }
                );
                last_group = Some(group);
                let mut end = start + 1;
                while end < batch.num_rows() && groups.value(end) == group {
                    end += 1;
                }
                batches
                    .entry(group)
                    .or_default()
                    .push(batch.slice(start, end - start));
                start = end;
            }
        }
        let size = reservation.size()
            + std::mem::size_of::<SstRangeIndexData>()
            + batches
                .values()
                .map(|batches| batches.capacity() * std::mem::size_of::<RecordBatch>())
                .sum::<usize>();
        reservation
            .try_resize(size)
            .context(ReserveRangeIndexMemorySnafu)?;
        Ok(Self {
            data: Arc::new(SstRangeIndexData { batches, size }),
            _reservation: reservation,
        })
    }

    /// Cache complete index contents, then apply assignment filters during search.
    pub(crate) async fn open_cached(
        object_store: ObjectStore,
        path: &str,
        file_id: RegionFileId,
        cache: &CacheStrategy,
        memory_pool: &Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        let key = RangeResultKey::RangeIndex(file_id);
        if let Some(RangeResultValue::RangeIndex(data)) = cache.get_series_mapping(&key) {
            let reservation = MemoryConsumer::new("SstRangeIndexSearcher").register(memory_pool);
            reservation
                .try_grow(data.estimated_size())
                .context(ReserveRangeIndexMemorySnafu)?;
            return Ok(Self {
                data,
                _reservation: reservation,
            });
        }
        let searcher = Self::open(object_store, path, None, memory_pool).await?;
        cache.put_series_mapping(key, RangeResultValue::RangeIndex(searcher.data.clone()));
        Ok(searcher)
    }

    pub(crate) fn contains_row_group(&self, row_group: usize) -> bool {
        u32::try_from(row_group).is_ok_and(|group| self.data.batches.contains_key(&group))
    }

    /// Returns the row ranges for `series` in one source SST row group.
    ///
    /// `series` is one batch emitted by a
    /// [`MetricSeriesIdStream`](crate::series_index::MetricSeriesIdStream). The
    /// returned half-open ranges are relative to the start of `row_group_id`,
    /// sorted, non-overlapping, and coalesced when adjacent. The number of
    /// returned ranges may be less than the number of input series if some
    /// series don't exist in the row group.
    pub fn search(
        &self,
        row_group_id: u32,
        series: &[MetricSeriesId],
    ) -> Result<Vec<Range<usize>>> {
        let runs = self.search_series(row_group_id, series)?;
        let mut ranges: Vec<Range<usize>> = Vec::with_capacity(runs.len());
        for run in runs {
            if let Some(last) = ranges.last_mut()
                && last.end == run.rows.start
            {
                last.end = run.rows.end;
            } else {
                ranges.push(run.rows);
            }
        }
        Ok(ranges)
    }

    /// Like `search`, but preserves identities at adjacent series boundaries.
    pub(crate) fn search_series(
        &self,
        row_group_id: u32,
        series: &[MetricSeriesId],
    ) -> Result<Vec<SeriesRowRange>> {
        if series.is_empty() {
            return Ok(Vec::new());
        }
        validate_sorted_series(series)?;
        let mut merge = RangeMergeState::new(row_group_id, series);
        if let Some(batches) = self.data.batches.get(&row_group_id) {
            for batch in batches {
                if merge.append_batch(batch)? {
                    break;
                }
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

/// Keep whole expressions: dropping an unsupported branch of an OR could
/// exclude matching series. Only identity columns have the same meaning here.
fn index_predicate(predicate: Option<&Predicate>) -> Predicate {
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
    Predicate::new(exprs)
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
    ranges: Vec<SeriesRowRange>,
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
                    self.append_range(index_series, starts.value(row), ends.value(row), row)?;
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

    fn append_range(
        &mut self,
        series: MetricSeriesId,
        start: i64,
        end: i64,
        row: usize,
    ) -> Result<()> {
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
                start >= last.rows.end,
                InvalidRecordBatchSnafu {
                    reason: format!(
                        "range index contains overlapping or unsorted range {start}..{end} after {}..{}",
                        last.rows.start, last.rows.end
                    ),
                }
            );
        }
        self.ranges.push(SeriesRowRange {
            series,
            rows: start..end,
        });
        Ok(())
    }

    fn finish(self) -> Vec<SeriesRowRange> {
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

    use datafusion::execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};
    use datafusion_expr::{col, lit};
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
    async fn cached_batches_support_independent_series_selections() {
        let store = object_store();
        let path = "range-search.parquet";
        write_index(&store, path).await;
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let searcher = SstRangeIndexSearcher::open(store.clone(), path, None, &pool)
            .await
            .unwrap();
        assert!(pool.reserved() > 0);
        // Every assignment and source group must reuse the decoded batches.
        store.delete(path).await.unwrap();

        let ranges = searcher.search(0, &[series(1, 10), series(2, 20)]).unwrap();
        assert_eq!(ranges, vec![0..2, 4..6]);

        let ranges = searcher.search(0, &[series(1, 10), series(1, 20)]).unwrap();
        assert_eq!(ranges, vec![0..3]);

        let ranges = searcher.search(0, &[series(1, 15), series(2, 20)]).unwrap();
        assert_eq!(ranges, vec![4..6]);

        let ranges = searcher.search(0, &[series(1, 10), series(2, 30)]).unwrap();
        assert_eq!(ranges, vec![0..2]);

        let ranges = searcher.search(1, &[series(2, 20), series(2, 20)]).unwrap();
        assert_eq!(ranges, vec![0..2]);

        assert!(searcher.search(1, &[series(1, 10)]).unwrap().is_empty());

        assert!(searcher.search(0, &[]).unwrap().is_empty());

        let error = searcher
            .search(0, &[series(2, 20), series(1, 10)])
            .unwrap_err();
        assert!(error.to_string().contains("not sorted"), "{error}");
        drop(searcher);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn shared_cache_retains_identities_and_releases_query_reservations() {
        let store = object_store();
        let path = "shared-range-index.parquet";
        write_index(&store, path).await;
        let cache = CacheStrategy::EnableAll(Arc::new(
            crate::cache::CacheManager::builder()
                .range_result_cache_size(1024 * 1024)
                .build(),
        ));
        let file = RegionFileId::new(
            store_api::storage::RegionId::new(1, 1),
            store_api::storage::FileId::random(),
        );
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let first = SstRangeIndexSearcher::open_cached(store.clone(), path, file, &cache, &pool)
            .await
            .unwrap();
        let runs = first
            .search_series(0, &[series(1, 10), series(1, 20)])
            .unwrap();
        assert_eq!(
            runs.iter()
                .map(|run| (run.series, run.rows.clone()))
                .collect::<Vec<_>>(),
            vec![(series(1, 10), 0..2), (series(1, 20), 2..3)]
        );
        drop(first);
        assert_eq!(
            pool.reserved(),
            0,
            "shared cache must not retain a query pool"
        );
        store.delete(path).await.unwrap();
        let next = SstRangeIndexSearcher::open_cached(store.clone(), path, file, &cache, &pool)
            .await
            .unwrap();
        assert_eq!(next.search(0, &[series(2, 20)]).unwrap(), vec![4..6]);
        let small: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        assert!(
            SstRangeIndexSearcher::open_cached(store.clone(), path, file, &cache, &small)
                .await
                .is_err()
        );
        assert_eq!(small.reserved(), 0);
        assert!(
            SstRangeIndexSearcher::open_cached(store, path, file, &CacheStrategy::Disabled, &pool)
                .await
                .is_err()
        );
        drop(next);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn loading_prunes_by_scan_predicate() {
        let store = object_store();
        let path = "range-pruning.parquet";
        write_index(&store, path).await;
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        for (predicate, expected_groups) in [
            (
                Predicate::new(vec![col(TABLE_ID_COLUMN).eq(lit(1u32))]),
                vec![0],
            ),
            (
                Predicate::new(vec![col(TSID_COLUMN).gt(lit(100u64))]),
                vec![],
            ),
            // An unsupported branch of an OR must not be dropped independently.
            (
                Predicate::new(vec![
                    col(TABLE_ID_COLUMN)
                        .eq(lit(1u32))
                        .or(col("unknown_tag").eq(lit("matches"))),
                ]),
                vec![0, 1],
            ),
        ] {
            let searcher =
                SstRangeIndexSearcher::open(store.clone(), path, Some(&predicate), &pool)
                    .await
                    .unwrap();
            assert_eq!(
                searcher.data.batches.keys().copied().collect::<Vec<_>>(),
                expected_groups
            );
        }
    }

    #[tokio::test]
    async fn loaded_groups_do_not_depend_on_footer_intervals() {
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::{EnabledStatistics, WriterProperties};

        for statistics in [EnabledStatistics::Chunk, EnabledStatistics::None] {
            // Group 0 spans index batches; groups 0 and 2 share one index batch.
            // Group 1 must not be counted merely because it lies between them.
            let batch = RecordBatch::try_new(
                range_index_schema(),
                vec![
                    Arc::new(UInt32Array::from(vec![0, 0, 0, 2, 2])) as ArrayRef,
                    Arc::new(UInt32Array::from(vec![1, 1, 2, 2, 3])),
                    Arc::new(UInt64Array::from(vec![10, 20, 10, 10, 10])),
                    Arc::new(Int64Array::from(vec![0, 1, 2, 0, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 1, 2])),
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
            let path = "range-groups.parquet";
            store.write(path, bytes).await.unwrap();
            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
            let searcher = SstRangeIndexSearcher::open(store.clone(), path, None, &pool)
                .await
                .unwrap();
            assert_eq!(
                searcher.data.batches.keys().copied().collect::<Vec<_>>(),
                vec![0, 2]
            );
            assert_eq!(
                searcher.search(0, &[series(1, 20), series(2, 10)]).unwrap(),
                vec![1..3]
            );
            assert_eq!(
                searcher.search(2, &[series(2, 10), series(3, 10)]).unwrap(),
                vec![0..2]
            );

            // Allow some batches but reject the complete index. Partial loading
            // must release its reservation so a subsequent attempt can succeed.
            let limited_pool: Arc<dyn MemoryPool> =
                Arc::new(GreedyMemoryPool::new(pool.reserved() - 1));
            assert!(
                SstRangeIndexSearcher::open(store.clone(), path, None, &limited_pool)
                    .await
                    .is_err()
            );
            assert_eq!(limited_pool.reserved(), 0);
            let predicate = Predicate::new(vec![col(TABLE_ID_COLUMN).eq(lit(1u32))]);
            if statistics == EnabledStatistics::Chunk {
                let retry =
                    SstRangeIndexSearcher::open(store, path, Some(&predicate), &limited_pool)
                        .await
                        .unwrap();
                assert_eq!(retry.search(0, &[series(1, 10)]).unwrap(), vec![0..1]);
            }
        }
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
        assert_eq!(
            merge
                .finish()
                .into_iter()
                .map(|run| run.rows)
                .collect::<Vec<_>>(),
            vec![0..1, 1..2]
        );
    }
}
