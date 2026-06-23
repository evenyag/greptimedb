// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! Primary-key aggregate index builder.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common_time::timestamp::{TimeUnit, Timestamp};
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, Int64Array, StringArray, UInt32Array,
    UInt64Array,
};
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::value::Value;
use futures::{StreamExt, stream};
use mito_codec::row_converter::{
    CompositeValues, PrimaryKeyCodec, SparseValues, build_primary_key_codec,
};
use object_store::{FuturesAsyncWriter, ObjectStore};
use parquet::arrow::AsyncArrowWriter;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::{ColumnId, FileId, RegionId, SequenceNumber};
use tokio::sync::Semaphore;
use tokio::sync::mpsc::Sender;
use tokio_util::compat::FuturesAsyncWriteCompatExt;

use crate::access_layer::AccessLayerRef;
use crate::error::{
    InvalidMetaSnafu, InvalidRecordBatchSnafu, NewRecordBatchSnafu, OpenDalSnafu, Result,
    WriteParquetSnafu,
};
use crate::manifest::action::{
    AggregatePkIndexMeta, RegionEdit, RegionMetaAction, RegionMetaActionList,
};
use crate::metrics::{
    INDEX_CREATE_BYTES_TOTAL, INDEX_CREATE_ELAPSED, INDEX_CREATE_ROWS_TOTAL,
    PK_INDEX_SOURCE_ROWS_TOTAL, PK_INDEX_TASK_TOTAL,
};
use crate::read::BoxedRecordBatchStream;
use crate::read::flat_merge::FlatMergeReader;
use crate::read::read_columns::ReadColumns;
use crate::region::{ManifestContextRef, RegionLeaderState};
use crate::request::{
    BackgroundNotify, PkIndexBuildFinished, WorkerRequest, WorkerRequestWithTime,
};
use crate::sst::file::{FileHandle, FileTimeRange};
use crate::sst::location::pk_index_file_path;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::flat_format::{
    FlatReadFormat, primary_key_column_index, time_index_column_index,
};
use crate::sst::{DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_CONCURRENCY};

const PK_INDEX_WRITE_BATCH_ROWS: usize = 8192;
const PK_INDEX_WINDOW_DAYS: i64 = 10;
const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;
const WINDOW_MILLIS: i64 = PK_INDEX_WINDOW_DAYS * MILLIS_PER_DAY;

const MIN_TS_COL: &str = "min_ts";
const MAX_TS_COL: &str = "max_ts";
const ROW_COUNT_COL: &str = "row_count";
const TABLE_ID_COL: &str = "__table_id";
const TSID_COL: &str = "__tsid";
const PK_INDEX_TYPE: &str = "pk_index";
const OPEN_SOURCE_STAGE: &str = "open_source";
const READ_SOURCE_STAGE: &str = "read_source";
const AGGREGATE_STAGE: &str = "aggregate";
const WRITE_PK_COLUMNS_STAGE: &str = "write_pk_columns";
const STAT_OUTPUT_STAGE: &str = "stat_output";
const TOTAL_STAGE: &str = "total";

#[derive(Debug, Clone)]
pub(crate) struct PkIndexBuildOutput {
    pub(crate) meta: AggregatePkIndexMeta,
    pub(crate) path: String,
}

#[derive(Debug)]
pub(crate) struct PkIndexBuildRequest {
    pub(crate) region_id: RegionId,
    pub(crate) metadata: RegionMetadataRef,
    pub(crate) access_layer: AccessLayerRef,
    pub(crate) manifest_ctx: ManifestContextRef,
    pub(crate) files: Vec<FileHandle>,
    pub(crate) existing_indexes: HashMap<FileId, AggregatePkIndexMeta>,
    pub(crate) max_sequence: SequenceNumber,
    pub(crate) request_sender: Sender<WorkerRequestWithTime>,
}

pub(crate) struct PkIndexBuildScheduler {
    semaphore: Arc<Semaphore>,
}

impl PkIndexBuildScheduler {
    pub(crate) fn new() -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(1)),
        }
    }

    pub(crate) fn schedule(&self, request: PkIndexBuildRequest) -> Result<()> {
        PK_INDEX_TASK_TOTAL.with_label_values(&["scheduled"]).inc();

        let semaphore = self.semaphore.clone();
        common_runtime::spawn_global(async move {
            let Ok(_permit) = semaphore.acquire_owned().await else {
                PK_INDEX_TASK_TOTAL.with_label_values(&["failed"]).inc();
                common_telemetry::warn!(
                    "Primary-key aggregate index task failed to acquire scheduler permit"
                );
                return;
            };
            request.run_with_metrics().await;
        });
        Ok(())
    }
}

impl PkIndexBuildRequest {
    async fn run_with_metrics(self) {
        let region_id = self.region_id;
        if let Err(err) = self.run().await {
            PK_INDEX_TASK_TOTAL.with_label_values(&["failed"]).inc();
            common_telemetry::warn!(err; "Primary-key aggregate index task failed, region: {}", region_id);
        }
    }

    async fn run(self) -> Result<()> {
        let buckets = select_pk_index_buckets(
            self.region_id,
            self.files.clone().into_iter(),
            &self.existing_indexes,
            self.max_sequence,
        );
        if buckets.is_empty() {
            PK_INDEX_TASK_TOTAL.with_label_values(&["skipped"]).inc();
            return Ok(());
        }
        for bucket in buckets {
            let index_file_id = FileId::random();
            let output = match build_pk_index(
                &self.access_layer,
                self.metadata.clone(),
                bucket,
                index_file_id,
                self.max_sequence,
            )
            .await
            {
                Ok(output) => output,
                Err(err) => {
                    delete_pk_index_file(&self.access_layer, self.region_id, index_file_id).await;
                    return Err(err);
                }
            };

            if self.manifest_has_covering_index(&output.meta).await {
                delete_pk_index_file(&self.access_layer, self.region_id, index_file_id).await;
                PK_INDEX_TASK_TOTAL.with_label_values(&["skipped"]).inc();
                continue;
            }

            let edit = RegionEdit {
                files_to_add: vec![],
                files_to_remove: vec![],
                pk_indexes_to_add: vec![output.meta.clone()],
                pk_indexes_to_remove: self.superseded_indexes(output.meta.time_range).await,
                timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            };
            if let Err(err) = self.update_manifest(edit.clone()).await {
                delete_pk_index_file(&self.access_layer, self.region_id, index_file_id).await;
                return Err(err);
            }

            let request = WorkerRequest::Background {
                region_id: self.region_id,
                notify: BackgroundNotify::PkIndexBuildFinished(PkIndexBuildFinished {
                    region_id: self.region_id,
                    edit,
                }),
            };
            let _ = self
                .request_sender
                .send(WorkerRequestWithTime::new(request))
                .await;
            common_telemetry::debug!(
                "Built primary-key aggregate index {}, region: {}, path: {}",
                index_file_id,
                self.region_id,
                output.path
            );
        }
        PK_INDEX_TASK_TOTAL.with_label_values(&["succeeded"]).inc();
        Ok(())
    }

    async fn update_manifest(&self, edit: RegionEdit) -> Result<()> {
        self.manifest_ctx
            .update_manifest(
                RegionLeaderState::Writable,
                RegionMetaActionList::with_action(RegionMetaAction::Edit(edit)),
                false,
            )
            .await?;
        Ok(())
    }

    async fn manifest_has_covering_index(&self, index: &AggregatePkIndexMeta) -> bool {
        let manager = self.manifest_ctx.manifest_manager.read().await;
        manager.manifest().pk_indexes.values().any(|existing| {
            existing.time_range == index.time_range && existing.max_sequence >= index.max_sequence
        })
    }

    async fn superseded_indexes(&self, time_range: FileTimeRange) -> Vec<AggregatePkIndexMeta> {
        let manager = self.manifest_ctx.manifest_manager.read().await;
        manager
            .manifest()
            .pk_indexes
            .values()
            .filter(|index| index.time_range == time_range)
            .cloned()
            .collect()
    }
}

#[derive(Debug)]
pub(crate) struct PkIndexBucket {
    pub(crate) time_range: FileTimeRange,
    pub(crate) files: Vec<FileHandle>,
}

struct SstStream {
    stream: BoxedRecordBatchStream,
    schema: SchemaRef,
}

#[derive(Debug, Default)]
struct PkIndexBuildCosts {
    open_source: Duration,
    read_source: Duration,
    aggregate: Duration,
    write: Duration,
    stat_output: Duration,
}

impl PkIndexBuildCosts {
    fn total_tracked(&self) -> Duration {
        self.open_source + self.read_source + self.aggregate + self.write + self.stat_output
    }

    fn observe(&self, total: Duration) {
        observe_pk_index_stage(OPEN_SOURCE_STAGE, self.open_source);
        observe_pk_index_stage(READ_SOURCE_STAGE, self.read_source);
        observe_pk_index_stage(AGGREGATE_STAGE, self.aggregate);
        observe_pk_index_stage(WRITE_PK_COLUMNS_STAGE, self.write);
        observe_pk_index_stage(STAT_OUTPUT_STAGE, self.stat_output);
        observe_pk_index_stage(TOTAL_STAGE, total);
    }
}

fn observe_pk_index_stage(stage: &str, cost: Duration) {
    INDEX_CREATE_ELAPSED
        .with_label_values(&[stage, PK_INDEX_TYPE])
        .observe(cost.as_secs_f64());
}

#[derive(Debug, Clone)]
struct PkStat {
    min_ts: i64,
    max_ts: i64,
    row_count: u64,
    table_id: u32,
    tsid: u64,
    tags: BTreeMap<ColumnId, String>,
}

pub(crate) fn select_pk_index_buckets(
    region_id: RegionId,
    files: impl Iterator<Item = FileHandle>,
    existing_indexes: &HashMap<FileId, AggregatePkIndexMeta>,
    max_sequence: SequenceNumber,
) -> Vec<PkIndexBucket> {
    let mut buckets: BTreeMap<i64, Vec<FileHandle>> = BTreeMap::new();
    for file in files {
        let meta = file.meta_ref();
        if meta.region_id != region_id || file.is_deleted() || file.compacting() {
            continue;
        }
        let Some(sequence) = meta.sequence.map(|seq| seq.get()) else {
            continue;
        };
        if sequence > max_sequence {
            continue;
        }
        let Some(bucket_start) = bucket_start_millis(meta.time_range.0) else {
            continue;
        };
        buckets.entry(bucket_start).or_default().push(file);
    }

    buckets
        .into_iter()
        .filter_map(|(bucket_start, files)| {
            let bucket_end = bucket_start + WINDOW_MILLIS - 1;
            let time_range = (
                Timestamp::new_millisecond(bucket_start),
                Timestamp::new_millisecond(bucket_end),
            );
            if existing_indexes
                .values()
                .any(|index| index.time_range == time_range && index.max_sequence >= max_sequence)
            {
                return None;
            }
            Some(PkIndexBucket { time_range, files })
        })
        .collect()
}

pub(crate) async fn build_pk_index(
    access_layer: &AccessLayerRef,
    metadata: RegionMetadataRef,
    bucket: PkIndexBucket,
    index_file_id: FileId,
    max_sequence: SequenceNumber,
) -> Result<PkIndexBuildOutput> {
    let build_start = Instant::now();
    let mut costs = PkIndexBuildCosts::default();
    ensure!(
        metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse,
        InvalidMetaSnafu {
            reason: "primary-key aggregate index only supports sparse primary-key encoding"
        }
    );

    let path = pk_index_file_path(
        access_layer.table_dir(),
        metadata.region_id,
        index_file_id,
        access_layer.path_type(),
    );
    let schema = pk_columns_schema(&metadata);
    let codec = build_primary_key_codec(&metadata);
    let tag_columns = tag_columns(&metadata);
    let source_file_ids = bucket
        .files
        .iter()
        .map(|file| file.meta_ref().file_id)
        .collect::<Vec<_>>();

    let stage_start = Instant::now();
    let mut stream = merge_sst_streams(access_layer, bucket.files).await?;
    costs.open_source += stage_start.elapsed();

    let stage_start = Instant::now();
    let mut writer =
        PkIndexWriter::try_new(access_layer.object_store(), &path, schema.clone()).await?;
    costs.write += stage_start.elapsed();

    let mut batch_rows = Vec::with_capacity(PK_INDEX_WRITE_BATCH_ROWS);
    let mut current_pk: Option<Vec<u8>> = None;
    let mut current_stat: Option<PkStat> = None;
    let mut row_count = 0;
    let mut source_row_count = 0;

    while let Some(stream) = stream.as_mut() {
        let stage_start = Instant::now();
        let Some(batch) = stream.next().await else {
            costs.read_source += stage_start.elapsed();
            break;
        };
        costs.read_source += stage_start.elapsed();

        let batch = batch?;
        source_row_count += batch.num_rows() as u64;
        if batch.num_rows() > 0 {
            let stage_start = Instant::now();
            let pk_idx = primary_key_column_index(batch.num_columns());
            let ts_idx = time_index_column_index(batch.num_columns());
            let timestamps = timestamp_values(batch.column(ts_idx))?;
            costs.aggregate += stage_start.elapsed();

            for (row, ts) in timestamps
                .iter()
                .copied()
                .enumerate()
                .take(batch.num_rows())
            {
                let stage_start = Instant::now();
                let pk = primary_key_value(batch.column(pk_idx), row)?;
                if let Some(stat) =
                    apply_sorted_pk_row(&mut current_pk, &mut current_stat, pk, ts, |pk, ts| {
                        let sparse = decode_sparse_pk(codec.as_ref(), pk)?;
                        let tags = collect_tags(&sparse, &tag_columns)?;
                        Ok(PkStat {
                            min_ts: ts,
                            max_ts: ts,
                            row_count: 0,
                            table_id: required_u32(
                                &sparse,
                                ReservedColumnId::table_id(),
                                "__table_id",
                            )?,
                            tsid: required_u64(&sparse, ReservedColumnId::tsid(), "__tsid")?,
                            tags,
                        })
                    })?
                {
                    costs.aggregate += stage_start.elapsed();
                    let stage_start = Instant::now();
                    write_or_buffer_pk_stat(
                        &mut writer,
                        schema.clone(),
                        &tag_columns,
                        &mut batch_rows,
                        stat,
                    )
                    .await?;
                    costs.write += stage_start.elapsed();
                    row_count += 1;
                } else {
                    costs.aggregate += stage_start.elapsed();
                }
            }
        }
    }

    if let Some(stat) = current_stat.take() {
        let stage_start = Instant::now();
        write_or_buffer_pk_stat(
            &mut writer,
            schema.clone(),
            &tag_columns,
            &mut batch_rows,
            stat,
        )
        .await?;
        costs.write += stage_start.elapsed();
        row_count += 1;
    }
    let stage_start = Instant::now();
    write_pk_columns_batch(&mut writer, schema, &tag_columns, &batch_rows).await?;
    writer.close().await?;
    costs.write += stage_start.elapsed();

    let stage_start = Instant::now();
    let file_size = access_layer
        .object_store()
        .stat(&path)
        .await
        .context(OpenDalSnafu)?
        .content_length();
    costs.stat_output += stage_start.elapsed();
    let total_cost = build_start.elapsed();

    costs.observe(total_cost);
    PK_INDEX_SOURCE_ROWS_TOTAL.inc_by(source_row_count);
    INDEX_CREATE_ROWS_TOTAL
        .with_label_values(&[PK_INDEX_TYPE])
        .inc_by(row_count);
    INDEX_CREATE_BYTES_TOTAL
        .with_label_values(&[PK_INDEX_TYPE])
        .inc_by(file_size);
    common_telemetry::info!(
        "Built primary-key aggregate index file, region: {}, index_file_id: {}, source_rows: {}, output_rows: {}, file_size: {}, open_source_cost: {:?}, read_source_cost: {:?}, aggregate_cost: {:?}, write_cost: {:?}, stat_output_cost: {:?}, tracked_cost: {:?}, total_cost: {:?}",
        metadata.region_id,
        index_file_id,
        source_row_count,
        row_count,
        file_size,
        costs.open_source,
        costs.read_source,
        costs.aggregate,
        costs.write,
        costs.stat_output,
        costs.total_tracked(),
        total_cost,
    );

    Ok(PkIndexBuildOutput {
        path,
        meta: AggregatePkIndexMeta {
            index_file_id,
            time_range: bucket.time_range,
            max_sequence,
            source_file_ids,
            file_size,
            row_count,
        },
    })
}

async fn merge_sst_streams(
    access_layer: &AccessLayerRef,
    files: Vec<FileHandle>,
) -> Result<Option<BoxedRecordBatchStream>> {
    let mut streams = Vec::with_capacity(files.len());
    let mut schema = None;
    for file in files {
        let Some(sst_stream) = open_sst_stream(access_layer, file).await? else {
            continue;
        };
        if let Some(expected) = &schema {
            validate_same_schema(expected, &sst_stream.schema)?;
        } else {
            schema = Some(sst_stream.schema.clone());
        }
        streams.push(sst_stream.stream);
    }

    let Some(schema) = schema else {
        return Ok(None);
    };
    if streams.len() == 1 {
        return Ok(streams.pop());
    }

    let reader = FlatMergeReader::new(schema, streams, DEFAULT_READ_BATCH_SIZE, None).await?;
    Ok(Some(Box::pin(reader.into_stream())))
}

async fn open_sst_stream(
    access_layer: &AccessLayerRef,
    file: FileHandle,
) -> Result<Option<SstStream>> {
    let file_path = file.file_path(access_layer.table_dir(), access_layer.path_type());
    let Some(mut reader) = access_layer
        .read_sst(file)
        .projection(Some(ReadColumns::from_deduped_column_ids(
            std::iter::empty(),
        )))
        .build()
        .await?
    else {
        return Ok(None);
    };

    let metadata = reader.metadata().clone();
    validate_sparse_flat_metadata(&metadata)?;
    reject_legacy_format(
        &metadata,
        reader
            .parquet_metadata()
            .file_metadata()
            .schema_descr()
            .num_columns(),
        &file_path,
    )?;

    let Some(first) = reader.next_record_batch().await? else {
        return Ok(None);
    };
    let schema = first.schema();
    let stream = Box::pin(stream::try_unfold(
        (Some(first), reader),
        |(pending, mut reader)| async move {
            if let Some(batch) = pending {
                return Ok(Some((batch, (None, reader))));
            }
            let batch = reader.next_record_batch().await?;
            Ok(batch.map(|batch| (batch, (None, reader))))
        },
    ));

    Ok(Some(SstStream { stream, schema }))
}

fn validate_sparse_flat_metadata(metadata: &RegionMetadataRef) -> Result<()> {
    ensure!(
        metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse,
        InvalidMetaSnafu {
            reason: "primary-key aggregate index only supports sparse primary-key encoding"
        }
    );
    Ok(())
}

fn reject_legacy_format(
    metadata: &RegionMetadataRef,
    num_columns: usize,
    file_path: &str,
) -> Result<()> {
    let legacy = FlatReadFormat::is_legacy_format(metadata, num_columns, file_path)?;
    ensure!(
        !legacy,
        InvalidMetaSnafu {
            reason: format!("legacy primary-key SST format is not supported: {file_path}")
        }
    );
    Ok(())
}

fn validate_same_schema(expected: &SchemaRef, actual: &SchemaRef) -> Result<()> {
    ensure!(
        expected.as_ref() == actual.as_ref(),
        InvalidMetaSnafu {
            reason: "mixed sparse-flat SST schemas are not supported"
        }
    );
    Ok(())
}

async fn write_or_buffer_pk_stat(
    writer: &mut PkIndexWriter,
    schema: SchemaRef,
    tag_columns: &[(ColumnId, String)],
    batch_rows: &mut Vec<PkStat>,
    row: PkStat,
) -> Result<()> {
    batch_rows.push(row);
    if batch_rows.len() >= PK_INDEX_WRITE_BATCH_ROWS {
        write_pk_columns_batch(writer, schema, tag_columns, batch_rows).await?;
        batch_rows.clear();
    }
    Ok(())
}

fn apply_sorted_pk_row(
    current_pk: &mut Option<Vec<u8>>,
    current_stat: &mut Option<PkStat>,
    pk: Vec<u8>,
    timestamp: i64,
    build_stat: impl FnOnce(&[u8], i64) -> Result<PkStat>,
) -> Result<Option<PkStat>> {
    let completed = if current_pk.as_deref() == Some(pk.as_slice()) {
        None
    } else {
        let completed = current_stat.take();
        *current_pk = Some(pk);
        let pk = current_pk
            .as_deref()
            .expect("current primary key initialized above");
        *current_stat = Some(build_stat(pk, timestamp)?);
        completed
    };

    let stat = current_stat
        .as_mut()
        .expect("current primary-key stat initialized above");
    stat.min_ts = stat.min_ts.min(timestamp);
    stat.max_ts = stat.max_ts.max(timestamp);
    stat.row_count += 1;
    Ok(completed)
}

pub(crate) async fn delete_pk_index_file(
    access_layer: &AccessLayerRef,
    region_id: RegionId,
    index_file_id: FileId,
) {
    let path = pk_index_file_path(
        access_layer.table_dir(),
        region_id,
        index_file_id,
        access_layer.path_type(),
    );
    if let Err(err) = access_layer.object_store().delete(&path).await {
        common_telemetry::warn!(err; "Failed to delete primary-key aggregate index file: {}", path);
    }
}

fn pk_columns_schema(metadata: &RegionMetadataRef) -> SchemaRef {
    let mut fields = vec![
        Arc::new(Field::new(MIN_TS_COL, DataType::Int64, false)),
        Arc::new(Field::new(MAX_TS_COL, DataType::Int64, false)),
        Arc::new(Field::new(ROW_COUNT_COL, DataType::UInt64, false)),
        Arc::new(Field::new(TABLE_ID_COL, DataType::UInt32, false)),
        Arc::new(Field::new(TSID_COL, DataType::UInt64, false)),
    ];
    fields.extend(
        tag_columns(metadata)
            .into_iter()
            .map(|(_, name)| Arc::new(Field::new(name, DataType::Utf8, true))),
    );
    Arc::new(Schema::new(fields))
}

async fn write_pk_columns_batch(
    writer: &mut PkIndexWriter,
    schema: SchemaRef,
    tag_columns: &[(ColumnId, String)],
    rows: &[PkStat],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.min_ts).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.max_ts).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.row_count).collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from(
            rows.iter().map(|row| row.table_id).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.tsid).collect::<Vec<_>>(),
        )),
    ];
    for (column_id, _) in tag_columns {
        arrays.push(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.tags.get(column_id).map(|value| value.as_str()))
                .collect::<Vec<_>>(),
        )));
    }
    let batch = RecordBatch::try_new(schema, arrays).context(NewRecordBatchSnafu)?;
    writer.write(&batch).await
}

fn bucket_start_millis(timestamp: Timestamp) -> Option<i64> {
    let millis = timestamp.convert_to(TimeUnit::Millisecond)?.value();
    Some(millis.div_euclid(WINDOW_MILLIS) * WINDOW_MILLIS)
}

fn tag_columns(metadata: &RegionMetadataRef) -> Vec<(ColumnId, String)> {
    metadata
        .primary_key_columns()
        .filter(|col| {
            col.column_id != ReservedColumnId::table_id()
                && col.column_id != ReservedColumnId::tsid()
        })
        .map(|col| (col.column_id, col.column_schema.name.clone()))
        .collect()
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

fn decode_sparse_pk(codec: &dyn PrimaryKeyCodec, pk: &[u8]) -> Result<SparseValues> {
    match codec.decode(pk).context(crate::error::DecodeSnafu)? {
        CompositeValues::Sparse(values) => Ok(values),
        other => InvalidMetaSnafu {
            reason: format!("decoded primary key is not sparse: {other:?}"),
        }
        .fail(),
    }
}

fn collect_tags(
    values: &SparseValues,
    tag_columns: &[(ColumnId, String)],
) -> Result<BTreeMap<ColumnId, String>> {
    let mut tags = BTreeMap::new();
    for (column_id, _) in tag_columns {
        if let Some(value) = values.get(column_id) {
            tags.insert(*column_id, string_value(*column_id, value)?);
        }
    }
    Ok(tags)
}

fn required_u32(values: &SparseValues, column_id: ColumnId, name: &str) -> Result<u32> {
    match values.get(&column_id) {
        Some(Value::UInt32(v)) => Ok(*v),
        other => InvalidMetaSnafu {
            reason: format!("missing/invalid sparse {name}: {other:?}"),
        }
        .fail(),
    }
}

fn required_u64(values: &SparseValues, column_id: ColumnId, name: &str) -> Result<u64> {
    match values.get(&column_id) {
        Some(Value::UInt64(v)) => Ok(*v),
        other => InvalidMetaSnafu {
            reason: format!("missing/invalid sparse {name}: {other:?}"),
        }
        .fail(),
    }
}

fn string_value(column_id: ColumnId, value: &Value) -> Result<String> {
    match value {
        Value::String(value) => Ok(value.as_utf8().to_string()),
        other => InvalidMetaSnafu {
            reason: format!(
                "primary-key aggregate index expects string primary-key value for column {column_id}, got {other:?}"
            ),
        }
        .fail(),
    }
}

struct PkIndexWriter {
    writer: AsyncArrowWriter<tokio_util::compat::Compat<FuturesAsyncWriter>>,
}

impl PkIndexWriter {
    async fn try_new(object_store: &ObjectStore, path: &str, schema: SchemaRef) -> Result<Self> {
        let writer = object_store
            .writer_with(path)
            .chunk(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
            .concurrent(DEFAULT_WRITE_CONCURRENCY)
            .await
            .context(OpenDalSnafu)?
            .into_futures_async_write()
            .compat_write();
        let writer = AsyncArrowWriter::try_new(writer, schema, None).context(WriteParquetSnafu)?;
        Ok(Self { writer })
    }

    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch).await.context(WriteParquetSnafu)
    }

    async fn close(self) -> Result<()> {
        self.writer.close().await.context(WriteParquetSnafu)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::sst::file::{FileMeta, Level};
    use crate::sst::file_purger::NoopFilePurger;

    fn file(region_id: RegionId, start_ms: i64, sequence: Option<u64>) -> FileHandle {
        FileHandle::new(
            FileMeta {
                region_id,
                file_id: FileId::random(),
                time_range: (
                    Timestamp::new_millisecond(start_ms),
                    Timestamp::new_millisecond(start_ms + 1000),
                ),
                level: 0 as Level,
                sequence: sequence.and_then(NonZeroU64::new),
                ..Default::default()
            },
            Arc::new(NoopFilePurger),
        )
    }

    #[test]
    fn test_select_pk_index_buckets_filters_sequence_and_existing_index() {
        let region_id = RegionId::new(1024, 1);
        let eligible = file(region_id, 0, Some(10));
        let too_new = file(region_id, 0, Some(20));
        let missing_sequence = file(region_id, 0, None);
        let next_bucket = file(region_id, WINDOW_MILLIS, Some(10));
        let existing_range = (
            Timestamp::new_millisecond(WINDOW_MILLIS),
            Timestamp::new_millisecond(WINDOW_MILLIS * 2 - 1),
        );
        let existing = HashMap::from([(
            FileId::random(),
            AggregatePkIndexMeta {
                index_file_id: FileId::random(),
                time_range: existing_range,
                max_sequence: 10,
                source_file_ids: vec![],
                file_size: 0,
                row_count: 0,
            },
        )]);

        let buckets = select_pk_index_buckets(
            region_id,
            vec![eligible.clone(), too_new, missing_sequence, next_bucket].into_iter(),
            &existing,
            10,
        );

        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].files.len(), 1);
        assert_eq!(
            buckets[0].files[0].meta_ref().file_id,
            eligible.meta_ref().file_id
        );
        assert_eq!(
            buckets[0].time_range,
            (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(WINDOW_MILLIS - 1)
            )
        );
    }

    #[test]
    fn test_apply_sorted_pk_row_merges_consecutive_primary_keys() {
        let mut current_pk = None;
        let mut current_stat = None;
        let mut completed = Vec::new();

        for (pk, timestamp) in [
            (b"pk-1".to_vec(), 100),
            (b"pk-1".to_vec(), 90),
            (b"pk-2".to_vec(), 120),
            (b"pk-2".to_vec(), 130),
            (b"pk-3".to_vec(), 110),
        ] {
            if let Some(stat) = apply_sorted_pk_row(
                &mut current_pk,
                &mut current_stat,
                pk,
                timestamp,
                |_pk, timestamp| {
                    Ok(PkStat {
                        min_ts: timestamp,
                        max_ts: timestamp,
                        row_count: 0,
                        table_id: 1,
                        tsid: 1,
                        tags: BTreeMap::new(),
                    })
                },
            )
            .unwrap()
            {
                completed.push(stat);
            }
        }
        completed.push(current_stat.take().unwrap());

        assert_eq!(completed.len(), 3);
        assert_eq!(completed[0].min_ts, 90);
        assert_eq!(completed[0].max_ts, 100);
        assert_eq!(completed[0].row_count, 2);
        assert_eq!(completed[1].min_ts, 120);
        assert_eq!(completed[1].max_ts, 130);
        assert_eq!(completed[1].row_count, 2);
        assert_eq!(completed[2].min_ts, 110);
        assert_eq!(completed[2].max_ts, 110);
        assert_eq!(completed[2].row_count, 1);
    }
}
