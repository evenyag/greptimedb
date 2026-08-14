// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::compute::SortOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, Int64Array, MapArray, StringArray, StructArray, UInt32Array,
    UInt64Array,
};
use datatypes::arrow::buffer::OffsetBuffer;
use datatypes::arrow::datatypes::{DataType, Field, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt, ensure};

use crate::aggr_index::index_io::{IndexReader, IndexWriter};
use crate::aggr_index::schema::{IndexKind, MAP_KEY_FIELD, MAP_VALUE_FIELD};
use crate::error::{InvalidMetaSnafu, InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result};

const MERGE_BATCH_SIZE: usize = 8192;

pub async fn merge_index_files(
    kind: IndexKind,
    object_store: &ObjectStore,
    inputs: &[String],
    output: &str,
) -> Result<usize> {
    ensure!(
        kind != IndexKind::PkColumns
            && kind != IndexKind::PkColumnsV2
            && kind != IndexKind::PkMapName,
        InvalidMetaSnafu {
            reason: format!("{kind:?} cannot be generically merged")
        }
    );
    if inputs.is_empty() {
        return IndexWriter::try_new(object_store, output, kind.schema())
            .await?
            .close()
            .await;
    }

    let merged = streaming_merge(kind, object_store, inputs).await?;
    match kind {
        IndexKind::Pk => merge_pk(merged, object_store, output).await,
        IndexKind::TableTag => merge_table_tag(merged, object_store, output).await,
        IndexKind::Tag => merge_tag(merged, object_store, output).await,
        IndexKind::TableTagTsid => merge_table_tag_tsid(merged, object_store, output).await,
        IndexKind::PkMap => merge_pk_map(merged, object_store, output).await,
        IndexKind::PkMapName | IndexKind::PkColumns | IndexKind::PkColumnsV2 => {
            unreachable!("merge is rejected above for {kind:?}")
        }
    }
}

async fn streaming_merge(
    kind: IndexKind,
    object_store: &ObjectStore,
    inputs: &[String],
) -> Result<SendableRecordBatchStream> {
    let schema = kind.schema();
    let mut streams = Vec::with_capacity(inputs.len());
    for input in inputs {
        let reader = IndexReader::try_new(object_store, input, kind).await?;
        streams.push(
            Box::pin(IndexRecordBatchStream::new(schema.clone(), reader))
                as SendableRecordBatchStream,
        );
    }

    let ordering = sort_ordering(kind, schema.clone());
    let metrics = ExecutionPlanMetricsSet::new();
    let memory_pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    StreamingMergeBuilder::new()
        .with_streams(streams)
        .with_schema(schema)
        .with_expressions(&ordering)
        .with_metrics(BaselineMetrics::new(&metrics, 0))
        .with_batch_size(MERGE_BATCH_SIZE)
        .with_reservation(MemoryConsumer::new("aggr_index_streaming_merge").register(&memory_pool))
        .with_round_robin_tie_breaker(false)
        .build()
        .map_err(|e| external_error(e, "build aggregate index streaming merge".to_string()))
}

fn sort_ordering(kind: IndexKind, schema: SchemaRef) -> LexOrdering {
    let columns: Vec<usize> = match kind {
        IndexKind::Pk => vec![0],
        IndexKind::TableTag => vec![0, 1, 2],
        IndexKind::Tag => vec![0, 1],
        IndexKind::TableTagTsid => vec![0, 1, 2, 3],
        IndexKind::PkMap => vec![3, 4],
        IndexKind::PkMapName | IndexKind::PkColumns | IndexKind::PkColumnsV2 => {
            unreachable!("merge is rejected before sorting for {kind:?}")
        }
    };
    LexOrdering::new(columns.into_iter().map(|idx| {
        PhysicalSortExpr::new(
            Arc::new(Column::new(schema.field(idx).name(), idx)),
            SortOptions::default(),
        )
    }))
    .expect("aggregate index sort ordering is not empty")
}

async fn merge_pk(
    mut merged: SendableRecordBatchStream,
    object_store: &ObjectStore,
    output: &str,
) -> Result<usize> {
    let mut writer = IndexWriter::try_new(object_store, output, IndexKind::Pk.schema()).await?;
    let mut buffer = PkOutputBuffer::default();
    let mut current: Option<PkRow> = None;

    while let Some(batch) = merged.next().await {
        let batch =
            batch.map_err(|e| external_error(e, "merge aggregate pk index stream".to_string()))?;
        let pk = binary_col(&batch, 0)?;
        let min = int64_col(&batch, 1)?;
        let max = int64_col(&batch, 2)?;
        let cnt = u64_col(&batch, 3)?;
        for row in 0..batch.num_rows() {
            let row = PkRow {
                pk: pk.value(row).to_vec(),
                min_ts: min.value(row),
                max_ts: max.value(row),
                row_count: cnt.value(row),
            };
            match current.as_mut() {
                Some(current) if current.pk == row.pk => {
                    current.min_ts = current.min_ts.min(row.min_ts);
                    current.max_ts = current.max_ts.max(row.max_ts);
                    current.row_count += row.row_count;
                }
                Some(_) => {
                    let finished = current.replace(row).expect("current row exists");
                    buffer.push(&mut writer, finished).await?;
                }
                None => current = Some(row),
            }
        }
    }
    if let Some(row) = current.take() {
        buffer.push(&mut writer, row).await?;
    }
    buffer.flush(&mut writer).await?;
    writer.close().await
}

async fn merge_table_tag(
    mut merged: SendableRecordBatchStream,
    object_store: &ObjectStore,
    output: &str,
) -> Result<usize> {
    let mut writer =
        IndexWriter::try_new(object_store, output, IndexKind::TableTag.schema()).await?;
    let mut buffer = TableTagOutputBuffer::default();
    let mut last: Option<TableTagRow> = None;

    while let Some(batch) = merged.next().await {
        let batch = batch
            .map_err(|e| external_error(e, "merge aggregate table-tag index stream".to_string()))?;
        let table = u32_col(&batch, 0)?;
        let col = u32_col(&batch, 1)?;
        let val = string_col(&batch, 2)?;
        for row in 0..batch.num_rows() {
            let row = TableTagRow {
                table_id: table.value(row),
                column_id: col.value(row),
                value: val.value(row).to_string(),
            };
            if last.as_ref() != Some(&row) {
                last = Some(row.clone());
                buffer.push(&mut writer, row).await?;
            }
        }
    }
    buffer.flush(&mut writer).await?;
    writer.close().await
}

async fn merge_tag(
    mut merged: SendableRecordBatchStream,
    object_store: &ObjectStore,
    output: &str,
) -> Result<usize> {
    let mut writer = IndexWriter::try_new(object_store, output, IndexKind::Tag.schema()).await?;
    let mut buffer = TagOutputBuffer::default();
    let mut last: Option<TagRow> = None;

    while let Some(batch) = merged.next().await {
        let batch =
            batch.map_err(|e| external_error(e, "merge aggregate tag index stream".to_string()))?;
        let col = u32_col(&batch, 0)?;
        let val = string_col(&batch, 1)?;
        for row in 0..batch.num_rows() {
            let row = TagRow {
                column_id: col.value(row),
                value: val.value(row).to_string(),
            };
            if last.as_ref() != Some(&row) {
                last = Some(row.clone());
                buffer.push(&mut writer, row).await?;
            }
        }
    }
    buffer.flush(&mut writer).await?;
    writer.close().await
}

async fn merge_table_tag_tsid(
    mut merged: SendableRecordBatchStream,
    object_store: &ObjectStore,
    output: &str,
) -> Result<usize> {
    let mut writer =
        IndexWriter::try_new(object_store, output, IndexKind::TableTagTsid.schema()).await?;
    let mut last: Option<(u32, u32, String, u64)> = None;

    while let Some(batch) = merged.next().await {
        let batch = batch.map_err(|e| {
            external_error(e, "merge aggregate table-tag-tsid index stream".to_string())
        })?;
        let table = u32_col(&batch, 0)?;
        let col = u32_col(&batch, 1)?;
        let val = string_col(&batch, 2)?;
        let tsid = u64_col(&batch, 3)?;
        let mut rows = Vec::new();
        for row in 0..batch.num_rows() {
            let key = (
                table.value(row),
                col.value(row),
                val.value(row).to_string(),
                tsid.value(row),
            );
            if last.as_ref() != Some(&key) {
                last = Some(key.clone());
                rows.push(key);
            }
        }
        if !rows.is_empty() {
            let out = RecordBatch::try_new(
                IndexKind::TableTagTsid.schema(),
                vec![
                    Arc::new(UInt32Array::from(
                        rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                    )) as _,
                    Arc::new(UInt32Array::from(
                        rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                    )) as _,
                    Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|r| r.2.as_str()),
                    )) as _,
                    Arc::new(UInt64Array::from(
                        rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                    )) as _,
                ],
            )
            .context(NewRecordBatchSnafu)?;
            writer.write(&out).await?;
        }
    }
    writer.close().await
}

async fn merge_pk_map(
    mut merged: SendableRecordBatchStream,
    object_store: &ObjectStore,
    output: &str,
) -> Result<usize> {
    let mut writer = IndexWriter::try_new(object_store, output, IndexKind::PkMap.schema()).await?;
    let mut buffer = PkMapOutputBuffer::default();
    let mut current: Option<PkMapRow> = None;

    while let Some(batch) = merged.next().await {
        let batch = batch
            .map_err(|e| external_error(e, "merge aggregate pk-map index stream".to_string()))?;
        let min = int64_col(&batch, 0)?;
        let max = int64_col(&batch, 1)?;
        let cnt = u64_col(&batch, 2)?;
        let table = u32_col(&batch, 3)?;
        let tsid = u64_col(&batch, 4)?;
        let tags = map_col(&batch, 5)?;
        for row in 0..batch.num_rows() {
            let row = PkMapRow {
                min_ts: min.value(row),
                max_ts: max.value(row),
                row_count: cnt.value(row),
                table_id: table.value(row),
                tsid: tsid.value(row),
                tags: map_entries(tags, row)?,
            };
            match current.as_mut() {
                Some(current) if current.same_key(&row) => {
                    current.min_ts = current.min_ts.min(row.min_ts);
                    current.max_ts = current.max_ts.max(row.max_ts);
                    current.row_count += row.row_count;
                }
                Some(_) => {
                    let finished = current.replace(row).expect("current row exists");
                    buffer.push(&mut writer, finished).await?;
                }
                None => current = Some(row),
            }
        }
    }
    if let Some(row) = current.take() {
        buffer.push(&mut writer, row).await?;
    }
    buffer.flush(&mut writer).await?;
    writer.close().await
}

struct IndexRecordBatchStream {
    schema: SchemaRef,
    reader: IndexReader,
}

impl IndexRecordBatchStream {
    fn new(schema: SchemaRef, reader: IndexReader) -> Self {
        Self { schema, reader }
    }
}

impl Stream for IndexRecordBatchStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.reader.poll_next_unpin(cx).map(|opt| {
            opt.map(|result| result.map_err(|e| DataFusionError::Execution(e.to_string())))
        })
    }
}

impl RecordBatchStream for IndexRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Debug)]
struct PkRow {
    pk: Vec<u8>,
    min_ts: i64,
    max_ts: i64,
    row_count: u64,
}

#[derive(Debug)]
struct PkMapRow {
    min_ts: i64,
    max_ts: i64,
    row_count: u64,
    table_id: u32,
    tsid: u64,
    tags: Vec<(u32, String)>,
}

impl PkMapRow {
    fn same_key(&self, other: &Self) -> bool {
        self.table_id == other.table_id && self.tsid == other.tsid && self.tags == other.tags
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableTagRow {
    table_id: u32,
    column_id: u32,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TagRow {
    column_id: u32,
    value: String,
}

#[derive(Default)]
struct PkOutputBuffer {
    rows: Vec<PkRow>,
}

impl PkOutputBuffer {
    async fn push(&mut self, writer: &mut IndexWriter, row: PkRow) -> Result<()> {
        self.rows.push(row);
        if self.rows.len() >= MERGE_BATCH_SIZE {
            self.flush(writer).await?;
        }
        Ok(())
    }

    async fn flush(&mut self, writer: &mut IndexWriter) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let batch = RecordBatch::try_new(
            IndexKind::Pk.schema(),
            vec![
                Arc::new(BinaryArray::from_iter_values(
                    self.rows.iter().map(|row| row.pk.as_slice()),
                )) as _,
                Arc::new(Int64Array::from(
                    self.rows.iter().map(|row| row.min_ts).collect::<Vec<_>>(),
                )) as _,
                Arc::new(Int64Array::from(
                    self.rows.iter().map(|row| row.max_ts).collect::<Vec<_>>(),
                )) as _,
                Arc::new(UInt64Array::from(
                    self.rows
                        .iter()
                        .map(|row| row.row_count)
                        .collect::<Vec<_>>(),
                )) as _,
            ],
        )
        .context(NewRecordBatchSnafu)?;
        writer.write(&batch).await?;
        self.rows.clear();
        Ok(())
    }
}

#[derive(Default)]
struct PkMapOutputBuffer {
    rows: Vec<PkMapRow>,
}

impl PkMapOutputBuffer {
    async fn push(&mut self, writer: &mut IndexWriter, row: PkMapRow) -> Result<()> {
        self.rows.push(row);
        if self.rows.len() >= MERGE_BATCH_SIZE {
            self.flush(writer).await?;
        }
        Ok(())
    }

    async fn flush(&mut self, writer: &mut IndexWriter) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut lengths = Vec::new();
        for row in &self.rows {
            lengths.push(row.tags.len());
            for (key, value) in &row.tags {
                keys.push(*key);
                values.push(value.as_str());
            }
        }
        let key_field = Arc::new(Field::new(MAP_KEY_FIELD, DataType::UInt32, false));
        let value_field = Arc::new(Field::new(MAP_VALUE_FIELD, DataType::Utf8, false));
        let struct_array = StructArray::from(vec![
            (key_field, Arc::new(UInt32Array::from(keys)) as ArrayRef),
            (
                value_field,
                Arc::new(StringArray::from_iter_values(values)) as ArrayRef,
            ),
        ]);
        let entries = match IndexKind::PkMap.schema().field(5).data_type() {
            DataType::Map(entries, _) => entries.clone(),
            _ => unreachable!("pk_map tags field is a map"),
        };
        let map = MapArray::new(
            entries,
            OffsetBuffer::from_lengths(lengths),
            struct_array,
            None,
            false,
        );
        let batch = RecordBatch::try_new(
            IndexKind::PkMap.schema(),
            vec![
                Arc::new(Int64Array::from(
                    self.rows.iter().map(|row| row.min_ts).collect::<Vec<_>>(),
                )) as _,
                Arc::new(Int64Array::from(
                    self.rows.iter().map(|row| row.max_ts).collect::<Vec<_>>(),
                )) as _,
                Arc::new(UInt64Array::from(
                    self.rows
                        .iter()
                        .map(|row| row.row_count)
                        .collect::<Vec<_>>(),
                )) as _,
                Arc::new(UInt32Array::from(
                    self.rows.iter().map(|row| row.table_id).collect::<Vec<_>>(),
                )) as _,
                Arc::new(UInt64Array::from(
                    self.rows.iter().map(|row| row.tsid).collect::<Vec<_>>(),
                )) as _,
                Arc::new(map) as _,
            ],
        )
        .context(NewRecordBatchSnafu)?;
        writer.write(&batch).await?;
        self.rows.clear();
        Ok(())
    }
}

#[derive(Default)]
struct TableTagOutputBuffer {
    rows: Vec<TableTagRow>,
}

impl TableTagOutputBuffer {
    async fn push(&mut self, writer: &mut IndexWriter, row: TableTagRow) -> Result<()> {
        self.rows.push(row);
        if self.rows.len() >= MERGE_BATCH_SIZE {
            self.flush(writer).await?;
        }
        Ok(())
    }

    async fn flush(&mut self, writer: &mut IndexWriter) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let batch = RecordBatch::try_new(
            IndexKind::TableTag.schema(),
            vec![
                Arc::new(UInt32Array::from(
                    self.rows.iter().map(|row| row.table_id).collect::<Vec<_>>(),
                )) as _,
                Arc::new(UInt32Array::from(
                    self.rows
                        .iter()
                        .map(|row| row.column_id)
                        .collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringArray::from_iter_values(
                    self.rows.iter().map(|row| row.value.as_str()),
                )) as _,
            ],
        )
        .context(NewRecordBatchSnafu)?;
        writer.write(&batch).await?;
        self.rows.clear();
        Ok(())
    }
}

#[derive(Default)]
struct TagOutputBuffer {
    rows: Vec<TagRow>,
}

impl TagOutputBuffer {
    async fn push(&mut self, writer: &mut IndexWriter, row: TagRow) -> Result<()> {
        self.rows.push(row);
        if self.rows.len() >= MERGE_BATCH_SIZE {
            self.flush(writer).await?;
        }
        Ok(())
    }

    async fn flush(&mut self, writer: &mut IndexWriter) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let batch = RecordBatch::try_new(
            IndexKind::Tag.schema(),
            vec![
                Arc::new(UInt32Array::from(
                    self.rows
                        .iter()
                        .map(|row| row.column_id)
                        .collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringArray::from_iter_values(
                    self.rows.iter().map(|row| row.value.as_str()),
                )) as _,
            ],
        )
        .context(NewRecordBatchSnafu)?;
        writer.write(&batch).await?;
        self.rows.clear();
        Ok(())
    }
}

fn binary_col(batch: &RecordBatch, idx: usize) -> Result<&BinaryArray> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context(InvalidRecordBatchSnafu {
            reason: format!("column {idx} is not Binary"),
        })
}
fn int64_col(batch: &RecordBatch, idx: usize) -> Result<&Int64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context(InvalidRecordBatchSnafu {
            reason: format!("column {idx} is not Int64"),
        })
}
fn u64_col(batch: &RecordBatch, idx: usize) -> Result<&UInt64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context(InvalidRecordBatchSnafu {
            reason: format!("column {idx} is not UInt64"),
        })
}
fn u32_col(batch: &RecordBatch, idx: usize) -> Result<&UInt32Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .context(InvalidRecordBatchSnafu {
            reason: format!("column {idx} is not UInt32"),
        })
}

fn string_col(batch: &RecordBatch, idx: usize) -> Result<&StringArray> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .context(InvalidRecordBatchSnafu {
            reason: format!("column {idx} is not Utf8"),
        })
}

fn map_col(batch: &RecordBatch, idx: usize) -> Result<&MapArray> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<MapArray>()
        .context(InvalidRecordBatchSnafu {
            reason: format!("column {idx} is not Map"),
        })
}

fn map_entries(array: &MapArray, row: usize) -> Result<Vec<(u32, String)>> {
    let entries = array.value(row);
    let entries =
        entries
            .as_any()
            .downcast_ref::<StructArray>()
            .context(InvalidRecordBatchSnafu {
                reason: "map entries are not StructArray".to_string(),
            })?;
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .context(InvalidRecordBatchSnafu {
            reason: "map keys are not UInt32".to_string(),
        })?;
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .context(InvalidRecordBatchSnafu {
            reason: "map values are not Utf8".to_string(),
        })?;
    Ok((0..entries.len())
        .map(|idx| (keys.value(idx), values.value(idx).to_string()))
        .collect())
}

fn external_error(error: impl std::fmt::Display, context: String) -> crate::error::Error {
    let boxed = common_error::ext::BoxedError::new(common_error::ext::PlainError::new(
        error.to_string(),
        common_error::status_code::StatusCode::Unexpected,
    ));
    let result: std::result::Result<(), common_error::ext::BoxedError> = Err(boxed);
    result
        .context(crate::error::ExternalSnafu { context })
        .unwrap_err()
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use object_store::services::Fs;

    use super::*;

    fn temp_store(root: &str) -> ObjectStore {
        ObjectStore::new(Fs::default().root(root)).unwrap().finish()
    }

    fn pk_map_batch(rows: &[(i64, i64, u64, u32, u64, &[(u32, &str)])]) -> RecordBatch {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut lengths = Vec::new();
        for row in rows {
            lengths.push(row.5.len());
            for (key, value) in row.5 {
                keys.push(*key);
                values.push(*value);
            }
        }
        let key_field = Arc::new(Field::new(MAP_KEY_FIELD, DataType::UInt32, false));
        let value_field = Arc::new(Field::new(MAP_VALUE_FIELD, DataType::Utf8, false));
        let struct_array = StructArray::from(vec![
            (key_field, Arc::new(UInt32Array::from(keys)) as ArrayRef),
            (
                value_field,
                Arc::new(StringArray::from_iter_values(values)) as ArrayRef,
            ),
        ]);
        let entries = match IndexKind::PkMap.schema().field(5).data_type() {
            DataType::Map(entries, _) => entries.clone(),
            _ => unreachable!("pk_map tags field is a map"),
        };
        let map = MapArray::new(
            entries,
            OffsetBuffer::from_lengths(lengths),
            struct_array,
            None,
            false,
        );

        RecordBatch::try_new(
            IndexKind::PkMap.schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )) as _,
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )) as _,
                Arc::new(UInt64Array::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )) as _,
                Arc::new(UInt32Array::from(
                    rows.iter().map(|row| row.3).collect::<Vec<_>>(),
                )) as _,
                Arc::new(UInt64Array::from(
                    rows.iter().map(|row| row.4).collect::<Vec<_>>(),
                )) as _,
                Arc::new(map) as _,
            ],
        )
        .unwrap()
    }

    async fn write_pk_map_file(object_store: &ObjectStore, path: &str, batch: &RecordBatch) {
        let mut writer = IndexWriter::try_new(object_store, path, IndexKind::PkMap.schema())
            .await
            .unwrap();
        writer.write(batch).await.unwrap();
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_merge_pk_map_aggregates_duplicate_primary_keys() {
        let dir = create_temp_dir("aggr_index_merge_pk_map");
        let store = temp_store(dir.path().to_str().unwrap());
        let tags = &[(7, "host-a"), (8, "region-a")];
        write_pk_map_file(
            &store,
            "run1.parquet",
            &pk_map_batch(&[(10, 20, 3, 42, 100, tags)]),
        )
        .await;
        write_pk_map_file(
            &store,
            "run2.parquet",
            &pk_map_batch(&[(5, 30, 4, 42, 100, tags)]),
        )
        .await;

        let rows = merge_index_files(
            IndexKind::PkMap,
            &store,
            &["run1.parquet".to_string(), "run2.parquet".to_string()],
            "merged.parquet",
        )
        .await
        .unwrap();
        assert_eq!(rows, 1);

        let mut reader = IndexReader::try_new(&store, "merged.parquet", IndexKind::PkMap)
            .await
            .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema().field(0).name(), "min_ts");
        assert_eq!(int64_col(&batch, 0).unwrap().value(0), 5);
        assert_eq!(int64_col(&batch, 1).unwrap().value(0), 30);
        assert_eq!(u64_col(&batch, 2).unwrap().value(0), 7);
        assert_eq!(u32_col(&batch, 3).unwrap().value(0), 42);
        assert_eq!(u64_col(&batch, 4).unwrap().value(0), 100);
        assert_eq!(
            map_entries(map_col(&batch, 5).unwrap(), 0).unwrap(),
            vec![(7, "host-a".to_string()), (8, "region-a".to_string()),]
        );
    }
}
