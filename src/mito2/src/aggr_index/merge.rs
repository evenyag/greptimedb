// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::path::Path;
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
    Array, BinaryArray, Int64Array, StringArray, UInt32Array, UInt64Array,
};
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use snafu::{OptionExt, ResultExt};

use crate::aggr_index::index_io::{IndexReader, IndexWriter};
use crate::aggr_index::schema::IndexKind;
use crate::error::{InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result};

const MERGE_BATCH_SIZE: usize = 8192;

pub fn merge_index_files(
    kind: IndexKind,
    inputs: &[impl AsRef<Path>],
    output: impl AsRef<Path>,
) -> Result<usize> {
    if inputs.is_empty() {
        return IndexWriter::try_new(output, kind.schema())?.close();
    }

    let merged = streaming_merge(kind, inputs)?;
    match kind {
        IndexKind::Pk => merge_pk(merged, output),
        IndexKind::TableTag => merge_table_tag(merged, output),
        IndexKind::Tag => merge_tag(merged, output),
    }
}

fn streaming_merge(
    kind: IndexKind,
    inputs: &[impl AsRef<Path>],
) -> Result<SendableRecordBatchStream> {
    let schema = kind.schema();
    let streams = inputs
        .iter()
        .map(|input| {
            let reader = IndexReader::try_new(input, kind)?;
            Ok(
                Box::pin(IndexRecordBatchStream::new(schema.clone(), reader))
                    as SendableRecordBatchStream,
            )
        })
        .collect::<Result<Vec<_>>>()?;

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
    let columns = match kind {
        IndexKind::Pk => 1,
        IndexKind::TableTag => 3,
        IndexKind::Tag => 2,
    };
    LexOrdering::new((0..columns).map(|idx| {
        PhysicalSortExpr::new(
            Arc::new(Column::new(schema.field(idx).name(), idx)),
            SortOptions::default(),
        )
    }))
    .expect("aggregate index sort ordering is not empty")
}

fn merge_pk(mut merged: SendableRecordBatchStream, output: impl AsRef<Path>) -> Result<usize> {
    let mut writer = IndexWriter::try_new(output, IndexKind::Pk.schema())?;
    let mut buffer = PkOutputBuffer::default();
    let mut current: Option<PkRow> = None;

    futures::executor::block_on(async {
        while let Some(batch) = merged.next().await {
            let batch = batch
                .map_err(|e| external_error(e, "merge aggregate pk index stream".to_string()))?;
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
                        buffer.push(&mut writer, finished)?;
                    }
                    None => current = Some(row),
                }
            }
        }
        if let Some(row) = current.take() {
            buffer.push(&mut writer, row)?;
        }
        buffer.flush(&mut writer)
    })?;
    writer.close()
}

fn merge_table_tag(
    mut merged: SendableRecordBatchStream,
    output: impl AsRef<Path>,
) -> Result<usize> {
    let mut writer = IndexWriter::try_new(output, IndexKind::TableTag.schema())?;
    let mut buffer = TableTagOutputBuffer::default();
    let mut last: Option<TableTagRow> = None;

    futures::executor::block_on(async {
        while let Some(batch) = merged.next().await {
            let batch = batch.map_err(|e| {
                external_error(e, "merge aggregate table-tag index stream".to_string())
            })?;
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
                    buffer.push(&mut writer, row)?;
                }
            }
        }
        buffer.flush(&mut writer)
    })?;
    writer.close()
}

fn merge_tag(mut merged: SendableRecordBatchStream, output: impl AsRef<Path>) -> Result<usize> {
    let mut writer = IndexWriter::try_new(output, IndexKind::Tag.schema())?;
    let mut buffer = TagOutputBuffer::default();
    let mut last: Option<TagRow> = None;

    futures::executor::block_on(async {
        while let Some(batch) = merged.next().await {
            let batch = batch
                .map_err(|e| external_error(e, "merge aggregate tag index stream".to_string()))?;
            let col = u32_col(&batch, 0)?;
            let val = string_col(&batch, 1)?;
            for row in 0..batch.num_rows() {
                let row = TagRow {
                    column_id: col.value(row),
                    value: val.value(row).to_string(),
                };
                if last.as_ref() != Some(&row) {
                    last = Some(row.clone());
                    buffer.push(&mut writer, row)?;
                }
            }
        }
        buffer.flush(&mut writer)
    })?;
    writer.close()
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

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(
            self.reader
                .next()
                .map(|result| result.map_err(|e| DataFusionError::Execution(e.to_string()))),
        )
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
    fn push(&mut self, writer: &mut IndexWriter, row: PkRow) -> Result<()> {
        self.rows.push(row);
        if self.rows.len() >= MERGE_BATCH_SIZE {
            self.flush(writer)?;
        }
        Ok(())
    }

    fn flush(&mut self, writer: &mut IndexWriter) -> Result<()> {
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
        writer.write(&batch)?;
        self.rows.clear();
        Ok(())
    }
}

#[derive(Default)]
struct TableTagOutputBuffer {
    rows: Vec<TableTagRow>,
}

impl TableTagOutputBuffer {
    fn push(&mut self, writer: &mut IndexWriter, row: TableTagRow) -> Result<()> {
        self.rows.push(row);
        if self.rows.len() >= MERGE_BATCH_SIZE {
            self.flush(writer)?;
        }
        Ok(())
    }

    fn flush(&mut self, writer: &mut IndexWriter) -> Result<()> {
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
        writer.write(&batch)?;
        self.rows.clear();
        Ok(())
    }
}

#[derive(Default)]
struct TagOutputBuffer {
    rows: Vec<TagRow>,
}

impl TagOutputBuffer {
    fn push(&mut self, writer: &mut IndexWriter, row: TagRow) -> Result<()> {
        self.rows.push(row);
        if self.rows.len() >= MERGE_BATCH_SIZE {
            self.flush(writer)?;
        }
        Ok(())
    }

    fn flush(&mut self, writer: &mut IndexWriter) -> Result<()> {
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
        writer.write(&batch)?;
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
