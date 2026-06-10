// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::collections::BTreeSet;
use std::mem;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datatypes::arrow::array::{
    Array, BinaryArray, DictionaryArray, Int64Array, StringArray, UInt32Array, UInt64Array,
};
use datatypes::arrow::datatypes::{DataType, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use mito_codec::row_converter::{CompositeValues, build_primary_key_codec};
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::ReservedColumnId;

use crate::aggr_index::index_io::IndexWriter;
use crate::aggr_index::merge::merge_index_files;
use crate::aggr_index::schema::{IndexKind, is_reserved_tag_column};
use crate::error::{InvalidMetaSnafu, InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result};
use crate::read::BoxedRecordBatchStream;
use crate::sst::parquet::flat_format::{primary_key_column_index, time_index_column_index};

const PK_WRITE_BUFFER_ROWS: usize = 1024;

#[derive(Debug, Clone, Default)]
pub struct BuildCosts {
    pub sst_iteration_decode_collect: Duration,
    pub legacy_pk_write: Duration,
    pub table_tag_run_write: Duration,
    pub tag_run_write: Duration,
    pub table_tag_merge_final_write: Duration,
    pub tag_merge_final_write: Duration,
}

#[derive(Debug, Clone)]
pub struct BuildOutput {
    pub pk_path: String,
    pub table_tag_path: String,
    pub tag_path: String,
    pub pk_rows: usize,
    pub table_tag_rows: usize,
    pub tag_rows: usize,
    pub costs: BuildCosts,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TableTagKey {
    table_id: u32,
    column_id: u32,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TagKey {
    column_id: u32,
    value: String,
}

#[derive(Debug)]
struct PkIndexRow {
    pk: Vec<u8>,
    min_ts: i64,
    max_ts: i64,
    row_count: u64,
}

pub async fn build_indexes(
    metadata: RegionMetadataRef,
    mut stream: BoxedRecordBatchStream,
    object_store: ObjectStore,
    output_dir: &str,
    buffer_bytes: usize,
) -> Result<BuildOutput> {
    ensure!(
        metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse,
        InvalidMetaSnafu {
            reason: "aggregate index only supports sparse primary-key encoding"
        }
    );

    let output_dir = output_dir.trim_end_matches('/');
    let pk_path = format!("{output_dir}/{}", IndexKind::Pk.file_name());
    let table_tag_path = format!("{output_dir}/{}", IndexKind::TableTag.file_name());
    let tag_path = format!("{output_dir}/{}", IndexKind::Tag.file_name());
    let mut pk_writer =
        IndexWriter::try_new(&object_store, &pk_path, IndexKind::Pk.schema()).await?;
    let mut costs = BuildCosts::default();
    let iteration_start = Instant::now();
    let codec = build_primary_key_codec(&metadata);

    let mut current_pk: Option<Vec<u8>> = None;
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut row_count = 0u64;
    let mut pk_rows = 0usize;
    let mut pk_buffer = Vec::with_capacity(PK_WRITE_BUFFER_ROWS);
    let mut table_tags = BTreeSet::new();
    let mut tags = BTreeSet::new();
    let mut table_tag_runs = Vec::new();
    let mut tag_runs = Vec::new();
    let mut run_id = 0usize;
    let mut tag_buffer_bytes = 0usize;
    let tag_buffer_bytes_limit = buffer_bytes.max(1);

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let pk_idx = primary_key_column_index(batch.num_columns());
        let ts_idx = time_index_column_index(batch.num_columns());
        let ts = timestamp_values(batch.column(ts_idx))?;
        for (row, ts_value) in ts.iter().copied().enumerate().take(batch.num_rows()) {
            let pk = primary_key_value(batch.column(pk_idx), row)?;
            if current_pk.as_deref() != Some(pk.as_slice()) {
                if let Some(prev) = current_pk.replace(pk.clone()) {
                    buffer_pk_row(
                        &mut pk_writer,
                        &mut pk_buffer,
                        prev,
                        min_ts,
                        max_ts,
                        row_count,
                    )
                    .await?;
                    pk_rows += 1;
                }
                min_ts = ts_value;
                max_ts = ts_value;
                row_count = 0;
                tag_buffer_bytes += collect_tag_keys(
                    &metadata,
                    codec.decode(&pk).context(crate::error::DecodeSnafu)?,
                    &mut table_tags,
                    &mut tags,
                )?;
                if tag_buffer_bytes >= tag_buffer_bytes_limit {
                    flush_runs(
                        &object_store,
                        output_dir,
                        &mut table_tags,
                        &mut tags,
                        &mut table_tag_runs,
                        &mut tag_runs,
                        &mut run_id,
                        &mut costs,
                    )
                    .await?;
                    tag_buffer_bytes = 0;
                }
            }
            min_ts = min_ts.min(ts_value);
            max_ts = max_ts.max(ts_value);
            row_count += 1;
        }
    }
    if let Some(prev) = current_pk.take() {
        buffer_pk_row(
            &mut pk_writer,
            &mut pk_buffer,
            prev,
            min_ts,
            max_ts,
            row_count,
        )
        .await?;
        pk_rows += 1;
    }
    costs.sst_iteration_decode_collect = iteration_start.elapsed();
    let pk_write_start = Instant::now();
    flush_pk_rows(&mut pk_writer, &mut pk_buffer).await?;
    pk_writer.close().await?;
    costs.legacy_pk_write += pk_write_start.elapsed();
    flush_runs(
        &object_store,
        output_dir,
        &mut table_tags,
        &mut tags,
        &mut table_tag_runs,
        &mut tag_runs,
        &mut run_id,
        &mut costs,
    )
    .await?;

    let table_tag_merge_start = Instant::now();
    let table_tag_rows = merge_or_empty(
        IndexKind::TableTag,
        &object_store,
        &table_tag_runs,
        &table_tag_path,
    )
    .await?;
    costs.table_tag_merge_final_write = table_tag_merge_start.elapsed();
    let tag_merge_start = Instant::now();
    let tag_rows = merge_or_empty(IndexKind::Tag, &object_store, &tag_runs, &tag_path).await?;
    costs.tag_merge_final_write = tag_merge_start.elapsed();
    for path in table_tag_runs.into_iter().chain(tag_runs) {
        let _ = object_store.delete(&path).await;
    }

    Ok(BuildOutput {
        pk_path,
        table_tag_path,
        tag_path,
        pk_rows,
        table_tag_rows,
        tag_rows,
        costs,
    })
}

fn collect_tag_keys(
    metadata: &RegionMetadataRef,
    values: CompositeValues,
    table_tags: &mut BTreeSet<TableTagKey>,
    tags: &mut BTreeSet<TagKey>,
) -> Result<usize> {
    let mut inserted_bytes = 0;
    let CompositeValues::Sparse(sparse) = values else {
        return InvalidMetaSnafu {
            reason: "decoded primary key is not sparse",
        }
        .fail();
    };
    let table_id = match sparse.get(&ReservedColumnId::table_id()) {
        Some(datatypes::value::Value::UInt32(v)) => *v,
        other => {
            return InvalidMetaSnafu {
                reason: format!("missing/invalid sparse __table_id: {other:?}"),
            }
            .fail();
        }
    };
    for col in metadata.primary_key_columns() {
        if is_reserved_tag_column(col.column_id) {
            continue;
        }
        if let Some(value) = sparse.get(&col.column_id) {
            let datatypes::value::Value::String(value) = value else {
                return InvalidMetaSnafu {
                    reason: format!(
                        "aggregate tag index expects string tag value for column {}, got {value:?}",
                        col.column_id
                    ),
                }
                .fail();
            };
            let value = value.as_utf8().to_string();
            let table_tag_key = TableTagKey {
                table_id,
                column_id: col.column_id,
                value: value.clone(),
            };
            if table_tags.insert(table_tag_key) {
                inserted_bytes += mem::size_of::<TableTagKey>() + value.len();
            }
            let tag_value_len = value.len();
            let tag_key = TagKey {
                column_id: col.column_id,
                value,
            };
            if tags.insert(tag_key) {
                inserted_bytes += mem::size_of::<TagKey>() + tag_value_len;
            }
        }
    }
    Ok(inserted_bytes)
}

fn primary_key_value(array: &Arc<dyn Array>, row: usize) -> Result<Vec<u8>> {
    match array.data_type() {
        DataType::Binary => Ok(array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context(InvalidRecordBatchSnafu {
                reason: "__primary_key is not BinaryArray",
            })?
            .value(row)
            .to_vec()),
        DataType::Dictionary(key, value)
            if key.as_ref() == &DataType::UInt32 && value.as_ref() == &DataType::Binary =>
        {
            let dict = array
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
                .context(InvalidRecordBatchSnafu {
                    reason: "__primary_key is not Dictionary(UInt32, Binary)",
                })?;
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context(InvalidRecordBatchSnafu {
                    reason: "__primary_key dictionary values are not binary",
                })?;
            Ok(values.value(dict.keys().value(row) as usize).to_vec())
        }
        other => InvalidRecordBatchSnafu {
            reason: format!("unsupported __primary_key type {other:?}"),
        }
        .fail(),
    }
}

fn timestamp_values(array: &Arc<dyn Array>) -> Result<Vec<i64>> {
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok((0..a.len()).map(|i| a.value(i)).collect());
    }
    macro_rules! ts {
        ($ty:ty) => {
            if let Some(a) = array.as_any().downcast_ref::<$ty>() {
                return Ok((0..a.len()).map(|i| a.value(i)).collect());
            }
        };
    }
    ts!(datatypes::arrow::array::TimestampSecondArray);
    ts!(datatypes::arrow::array::TimestampMillisecondArray);
    ts!(datatypes::arrow::array::TimestampMicrosecondArray);
    ts!(datatypes::arrow::array::TimestampNanosecondArray);
    InvalidRecordBatchSnafu {
        reason: format!("unsupported time index type {:?}", array.data_type()),
    }
    .fail()
}

async fn buffer_pk_row(
    writer: &mut IndexWriter,
    buffer: &mut Vec<PkIndexRow>,
    pk: Vec<u8>,
    min_ts: i64,
    max_ts: i64,
    row_count: u64,
) -> Result<()> {
    buffer.push(PkIndexRow {
        pk,
        min_ts,
        max_ts,
        row_count,
    });
    if buffer.len() >= PK_WRITE_BUFFER_ROWS {
        flush_pk_rows(writer, buffer).await?;
    }
    Ok(())
}

async fn flush_pk_rows(writer: &mut IndexWriter, buffer: &mut Vec<PkIndexRow>) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }

    let batch = RecordBatch::try_new(
        IndexKind::Pk.schema(),
        vec![
            Arc::new(BinaryArray::from_iter_values(
                buffer.iter().map(|row| row.pk.as_slice()),
            )) as _,
            Arc::new(Int64Array::from(
                buffer.iter().map(|row| row.min_ts).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                buffer.iter().map(|row| row.max_ts).collect::<Vec<_>>(),
            )) as _,
            Arc::new(UInt64Array::from(
                buffer.iter().map(|row| row.row_count).collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .context(NewRecordBatchSnafu)?;
    writer.write(&batch).await?;
    buffer.clear();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn flush_runs(
    object_store: &ObjectStore,
    output_dir: &str,
    table_tags: &mut BTreeSet<TableTagKey>,
    tags: &mut BTreeSet<TagKey>,
    table_tag_runs: &mut Vec<String>,
    tag_runs: &mut Vec<String>,
    run_id: &mut usize,
    costs: &mut BuildCosts,
) -> Result<()> {
    if !table_tags.is_empty() {
        let path = format!("{output_dir}/.table_tag_run_{run_id}.parquet");
        let start = Instant::now();
        write_table_tag_file(object_store, &path, table_tags.iter()).await?;
        costs.table_tag_run_write += start.elapsed();
        table_tags.clear();
        table_tag_runs.push(path);
    }
    if !tags.is_empty() {
        let path = format!("{output_dir}/.tag_run_{run_id}.parquet");
        let start = Instant::now();
        write_tag_file(object_store, &path, tags.iter()).await?;
        costs.tag_run_write += start.elapsed();
        tags.clear();
        tag_runs.push(path);
    }
    *run_id += 1;
    Ok(())
}

async fn write_table_tag_file<'a>(
    object_store: &ObjectStore,
    path: &str,
    rows: impl Iterator<Item = &'a TableTagKey>,
) -> Result<usize> {
    let rows: Vec<_> = rows.collect();
    let mut writer = IndexWriter::try_new(object_store, path, IndexKind::TableTag.schema()).await?;
    let batch = RecordBatch::try_new(
        IndexKind::TableTag.schema(),
        vec![
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.table_id).collect::<Vec<_>>(),
            )) as _,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.column_id).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.value.as_str()),
            )) as _,
        ],
    )
    .context(NewRecordBatchSnafu)?;
    writer.write(&batch).await?;
    writer.close().await
}

async fn write_tag_file<'a>(
    object_store: &ObjectStore,
    path: &str,
    rows: impl Iterator<Item = &'a TagKey>,
) -> Result<usize> {
    let rows: Vec<_> = rows.collect();
    let mut writer = IndexWriter::try_new(object_store, path, IndexKind::Tag.schema()).await?;
    let batch = RecordBatch::try_new(
        IndexKind::Tag.schema(),
        vec![
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.column_id).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.value.as_str()),
            )) as _,
        ],
    )
    .context(NewRecordBatchSnafu)?;
    writer.write(&batch).await?;
    writer.close().await
}

async fn merge_or_empty(
    kind: IndexKind,
    object_store: &ObjectStore,
    runs: &[String],
    output: &str,
) -> Result<usize> {
    if runs.is_empty() {
        return IndexWriter::try_new(object_store, output, kind.schema())
            .await?
            .close()
            .await;
    }
    merge_index_files(kind, object_store, runs, output).await
}
