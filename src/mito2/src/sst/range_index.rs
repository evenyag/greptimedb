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

//! Per-SST `(table_id, tsid)` -> row ranges index.
//!
//! For a sparse-encoded SST, rows are globally sorted by encoded primary key
//! (`table_id, tsid, tags…`), so all rows of one `(table_id, tsid)` series are
//! contiguous in the file and, within a single parquet row group, form exactly
//! one `[start, end)` run. This index records, per row group, the run of each
//! series.
//!
//! With the matching tsid set resolved from the `.pk` aggregate index (see
//! [`crate::sst::pk_index`]), a scan can build a parquet `RowSelection` for a
//! covered file directly from this index, **without reading the `__primary_key`
//! column** during the prefilter pass.
//!
//! The index is a small parquet file kept only in the local file cache (never in
//! the manifest) and is (re)built by `ADMIN compact_table()` alongside the `.pk`
//! index. On a cache miss the scan simply falls back to reading `__primary_key`.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datatypes::arrow::array::{Array, ArrayRef, BinaryArray, UInt32Array, UInt64Array};
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{AsyncArrowWriter, ProjectionMask};
use snafu::{OptionExt, ResultExt};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use tokio_util::compat::FuturesAsyncWriteCompatExt;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheStrategy;
use crate::error::{
    DecodeSnafu, NewRecordBatchSnafu, OpenDalSnafu, ReadParquetSnafu, Result, UnexpectedSnafu,
    WriteParquetSnafu,
};
use crate::sst::file::FileHandle;
use crate::sst::parquet::format::PrimaryKeyArray;
use crate::sst::parquet::reader::ReaderMetrics;
use crate::sst::{DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_CONCURRENCY};

const ROW_GROUP_COL: &str = "row_group_id";
const TABLE_ID_COL: &str = "__table_id";
const TSID_COL: &str = "__tsid";
const START_COL: &str = "start";
const END_COL: &str = "end";

/// Placeholder path used in parquet read errors (the index is read from in-memory bytes).
const RANGE_INDEX_LABEL: &str = "<range index>";

/// Arrow schema of the ranges index parquet file.
///
/// One row per `(row_group, table_id, tsid)` run, with `[start, end)` offsets
/// **relative to the start of the row group**.
pub(crate) fn ranges_index_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(ROW_GROUP_COL, DataType::UInt32, false),
        Field::new(TABLE_ID_COL, DataType::UInt32, false),
        Field::new(TSID_COL, DataType::UInt64, false),
        Field::new(START_COL, DataType::UInt32, false),
        Field::new(END_COL, DataType::UInt32, false),
    ]))
}

/// One contiguous run of a `(table_id, tsid)` series within a row group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RangeEntry {
    pub(crate) table_id: u32,
    pub(crate) tsid: u64,
    /// First row offset within the row group (inclusive).
    pub(crate) start: u32,
    /// Exclusive end offset within the row group.
    pub(crate) end: u32,
}

/// Parsed, in-memory form of a per-SST ranges index, indexed by row-group id.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SstRangesIndex {
    /// `row_groups[rg]` holds the runs of every series present in row group `rg`,
    /// in ascending physical order.
    row_groups: Vec<Vec<RangeEntry>>,
}

impl SstRangesIndex {
    /// Returns the matching row ranges within `row_group_idx` for the series in
    /// `tsids`, coalescing adjacent runs. Ranges are ascending and non-overlapping,
    /// ready for [`crate::sst::parquet::row_selection::row_selection_from_row_ranges`].
    pub(crate) fn row_ranges(
        &self,
        row_group_idx: usize,
        tsids: &HashSet<(u32, u64)>,
    ) -> Vec<Range<usize>> {
        let Some(entries) = self.row_groups.get(row_group_idx) else {
            return Vec::new();
        };

        let mut ranges: Vec<Range<usize>> = Vec::new();
        for entry in entries {
            if !tsids.contains(&(entry.table_id, entry.tsid)) {
                continue;
            }
            let start = entry.start as usize;
            let end = entry.end as usize;
            if let Some(last) = ranges.last_mut()
                && last.end == start
            {
                last.end = end;
            } else {
                ranges.push(start..end);
            }
        }
        ranges
    }

    /// Encodes the index into a single record batch following [`ranges_index_schema`].
    fn to_record_batch(&self) -> Result<RecordBatch> {
        let mut rg_ids = Vec::new();
        let mut table_ids = Vec::new();
        let mut tsids = Vec::new();
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        for (rg, entries) in self.row_groups.iter().enumerate() {
            for entry in entries {
                rg_ids.push(rg as u32);
                table_ids.push(entry.table_id);
                tsids.push(entry.tsid);
                starts.push(entry.start);
                ends.push(entry.end);
            }
        }

        RecordBatch::try_new(
            ranges_index_schema(),
            vec![
                Arc::new(UInt32Array::from(rg_ids)) as ArrayRef,
                Arc::new(UInt32Array::from(table_ids)),
                Arc::new(UInt64Array::from(tsids)),
                Arc::new(UInt32Array::from(starts)),
                Arc::new(UInt32Array::from(ends)),
            ],
        )
        .context(NewRecordBatchSnafu)
    }

    /// Appends a parsed record batch into `row_groups`, growing it as needed.
    fn append_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let rg_ids = u32_column(batch, ROW_GROUP_COL)?;
        let table_ids = u32_column(batch, TABLE_ID_COL)?;
        let tsids = u64_column(batch, TSID_COL)?;
        let starts = u32_column(batch, START_COL)?;
        let ends = u32_column(batch, END_COL)?;

        for row in 0..batch.num_rows() {
            let rg = rg_ids.value(row) as usize;
            if rg >= self.row_groups.len() {
                self.row_groups.resize_with(rg + 1, Vec::new);
            }
            self.row_groups[rg].push(RangeEntry {
                table_id: table_ids.value(row),
                tsid: tsids.value(row),
                start: starts.value(row),
                end: ends.value(row),
            });
        }
        Ok(())
    }
}

/// Builds the ranges index for a sparse-encoded SST file by scanning its
/// `__primary_key` column row group by row group.
///
/// Returns an empty index for non-sparse files (the caller only builds for
/// sparse files, but this stays defensive).
pub(crate) async fn build_sst_ranges_index(
    access_layer: &AccessLayerRef,
    metadata: &RegionMetadataRef,
    file: &FileHandle,
    cache_strategy: CacheStrategy,
) -> Result<SstRangesIndex> {
    if metadata.primary_key_encoding != PrimaryKeyEncoding::Sparse {
        return Ok(SstRangesIndex::default());
    }

    let codec = SparsePrimaryKeyCodec::new(metadata);
    let builder = access_layer.read_sst(file.clone()).cache(cache_strategy);
    let mut metrics = ReaderMetrics::default();
    let Some((context, _selection)) = builder.build_reader_input(&mut metrics).await? else {
        return Ok(SstRangesIndex::default());
    };

    let reader_builder = context.reader_builder();
    let arrow_schema = context.read_format().arrow_schema();
    let (pk_idx, _) = arrow_schema
        .column_with_name(PRIMARY_KEY_COLUMN_NAME)
        .context(UnexpectedSnafu {
            reason: "primary key column not found in SST schema",
        })?;

    let parquet_meta = reader_builder.parquet_metadata().clone();
    let projection = ProjectionMask::roots(parquet_meta.file_metadata().schema_descr(), [pk_idx]);
    let num_row_groups = parquet_meta.num_row_groups();

    let mut row_groups = Vec::with_capacity(num_row_groups);
    for rg in 0..num_row_groups {
        let mut stream = reader_builder
            .build_with_projection(rg, None, projection.clone(), None)
            .await?;

        let mut entries: Vec<RangeEntry> = Vec::new();
        let mut offset: usize = 0;
        let mut current: Option<RangeEntry> = None;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            for (table_id, tsid, len) in decode_pk_runs(&codec, batch.column(0))? {
                let start = offset;
                offset += len;
                let end = offset;
                match &mut current {
                    Some(run) if run.table_id == table_id && run.tsid == tsid => {
                        run.end = end as u32;
                    }
                    _ => {
                        if let Some(run) = current.take() {
                            entries.push(run);
                        }
                        current = Some(RangeEntry {
                            table_id,
                            tsid,
                            start: start as u32,
                            end: end as u32,
                        });
                    }
                }
            }
        }
        if let Some(run) = current.take() {
            entries.push(run);
        }
        row_groups.push(entries);
    }

    Ok(SstRangesIndex { row_groups })
}

/// Decodes the `__primary_key` column into runs of `(table_id, tsid, run_len)`,
/// decoding each contiguous run once. Mirrors the run detection used by the
/// prefilter (`matching_row_ranges_from_dict` / `_from_binary`).
fn decode_pk_runs(
    codec: &SparsePrimaryKeyCodec,
    pk_col: &ArrayRef,
) -> Result<Vec<(u32, u64, usize)>> {
    if let Some(dict) = pk_col.as_any().downcast_ref::<PrimaryKeyArray>() {
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context(UnexpectedSnafu {
                reason: "primary key dictionary values are not a binary array",
            })?;
        let keys = dict.keys().values();
        let mut runs = Vec::new();
        let mut start = 0;
        while start < keys.len() {
            let key = keys[start];
            let mut end = start + 1;
            while end < keys.len() && keys[end] == key {
                end += 1;
            }
            let (table_id, tsid) = codec
                .read_table_id_tsid(values.value(key as usize))
                .context(DecodeSnafu)?;
            runs.push((table_id, tsid, end - start));
            start = end;
        }
        Ok(runs)
    } else if let Some(binary) = pk_col.as_any().downcast_ref::<BinaryArray>() {
        let mut runs = Vec::new();
        let mut start = 0;
        while start < binary.len() {
            let value = binary.value(start);
            let mut end = start + 1;
            while end < binary.len() && binary.value(end) == value {
                end += 1;
            }
            let (table_id, tsid) = codec.read_table_id_tsid(value).context(DecodeSnafu)?;
            runs.push((table_id, tsid, end - start));
            start = end;
        }
        Ok(runs)
    } else {
        UnexpectedSnafu {
            reason: format!(
                "primary key column is neither a dictionary nor a binary array, got {:?}",
                pk_col.data_type()
            ),
        }
        .fail()
    }
}

/// Writes the ranges index as a parquet file to `path` on `store`, returning the
/// written file size in bytes.
pub(crate) async fn write_sst_ranges_index(
    store: &ObjectStore,
    path: &str,
    index: &SstRangesIndex,
) -> Result<u64> {
    let writer = store
        .writer_with(path)
        .chunk(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
        .concurrent(DEFAULT_WRITE_CONCURRENCY)
        .await
        .context(OpenDalSnafu)?
        .into_futures_async_write()
        .compat_write();
    let mut writer = AsyncArrowWriter::try_new(writer, ranges_index_schema(), None)
        .context(WriteParquetSnafu)?;
    let batch = index.to_record_batch()?;
    if batch.num_rows() > 0 {
        writer.write(&batch).await.context(WriteParquetSnafu)?;
    }
    writer.close().await.context(WriteParquetSnafu)?;

    let file_size = store
        .stat(path)
        .await
        .context(OpenDalSnafu)?
        .content_length();
    Ok(file_size)
}

/// Parses a ranges index from in-memory parquet bytes.
pub(crate) fn parse_sst_ranges_index(bytes: Bytes) -> Result<SstRangesIndex> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .and_then(|builder| builder.build())
        .context(ReadParquetSnafu {
            path: RANGE_INDEX_LABEL,
        })?;
    let mut index = SstRangesIndex::default();
    for batch in reader {
        let batch = batch.context(NewRecordBatchSnafu)?;
        index.append_batch(&batch)?;
    }
    Ok(index)
}

fn u32_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt32Array> {
    column(batch, name)?
        .as_any()
        .downcast_ref::<UInt32Array>()
        .context(UnexpectedSnafu {
            reason: format!("range index column {name} is not UInt32Array"),
        })
}

fn u64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array> {
    column(batch, name)?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context(UnexpectedSnafu {
            reason: format!("range index column {name} is not UInt64Array"),
        })
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    let (idx, _) = batch
        .schema()
        .column_with_name(name)
        .context(UnexpectedSnafu {
            reason: format!("range index column {name} not found"),
        })?;
    Ok(batch.column(idx))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_index() -> SstRangesIndex {
        SstRangesIndex {
            row_groups: vec![
                vec![
                    RangeEntry {
                        table_id: 1,
                        tsid: 100,
                        start: 0,
                        end: 3,
                    },
                    RangeEntry {
                        table_id: 1,
                        tsid: 101,
                        start: 3,
                        end: 5,
                    },
                    RangeEntry {
                        table_id: 1,
                        tsid: 102,
                        start: 5,
                        end: 8,
                    },
                ],
                vec![RangeEntry {
                    table_id: 2,
                    tsid: 100,
                    start: 0,
                    end: 4,
                }],
            ],
        }
    }

    #[test]
    fn test_row_ranges_filters_and_coalesces() {
        let index = sample_index();
        let tsids = HashSet::from([(1, 100), (1, 101)]);
        // Adjacent matching runs (0..3 and 3..5) coalesce into 0..5.
        assert_eq!(index.row_ranges(0, &tsids), vec![0..5]);
        // Non-adjacent matches stay separate.
        let tsids = HashSet::from([(1, 100), (1, 102)]);
        assert_eq!(index.row_ranges(0, &tsids), vec![0..3, 5..8]);
        // Different table in a different row group.
        let tsids = HashSet::from([(2, 100)]);
        assert_eq!(index.row_ranges(1, &tsids), vec![0..4]);
        // No match and out-of-range row group.
        assert!(index.row_ranges(0, &HashSet::from([(9, 9)])).is_empty());
        assert!(index.row_ranges(9, &tsids).is_empty());
    }

    #[test]
    fn test_record_batch_round_trip() {
        let index = sample_index();
        let batch = index.to_record_batch().unwrap();
        let mut parsed = SstRangesIndex::default();
        parsed.append_batch(&batch).unwrap();
        assert_eq!(parsed, index);
    }
}
