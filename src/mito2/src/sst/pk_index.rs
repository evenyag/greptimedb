// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! Primary-key aggregate index builder.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common_recordbatch::filter::SimpleFilterEvaluator;
use common_time::range::TimestampRange;
use common_time::timestamp::{TimeUnit, Timestamp};
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::Expr;
use datafusion_expr::utils::expr_to_columns;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, DictionaryArray, Int64Array, StringArray,
    UInt32Array, UInt64Array,
};
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::value::Value;
use futures::{StreamExt, stream};
use mito_codec::row_converter::{
    CompositeValues, PrimaryKeyCodec, PrimaryKeyFilter, SparsePrimaryKeyCodec, SparseValues,
    build_primary_key_codec,
};
use object_store::{FuturesAsyncWriter, ObjectStore};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::{ColumnId, FileId, RegionId, SequenceNumber};
use table::predicate::Predicate;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::Sender;
use tokio_util::compat::FuturesAsyncWriteCompatExt;

use crate::access_layer::AccessLayerRef;
use crate::cache::{CacheManagerRef, CacheStrategy};
use crate::error::{
    InvalidMetaSnafu, InvalidRecordBatchSnafu, NewRecordBatchSnafu, OpenDalSnafu, ReadParquetSnafu,
    RecordBatchSnafu, Result, WriteParquetSnafu,
};
use crate::manifest::action::{
    AggregatePkIndexMeta, RegionEdit, RegionMetaAction, RegionMetaActionList,
};
use crate::metrics::{
    INDEX_CREATE_BYTES_TOTAL, INDEX_CREATE_ELAPSED, INDEX_CREATE_ROWS_TOTAL,
    PK_INDEX_SCAN_BUILD_ELAPSED, PK_INDEX_SCAN_TOTAL, PK_INDEX_SCAN_TSIDS_TOTAL,
    PK_INDEX_SOURCE_ROWS_TOTAL, PK_INDEX_TASK_TOTAL,
};
use crate::read::BoxedRecordBatchStream;
use crate::read::flat_merge::FlatMergeReader;
use crate::read::read_columns::ReadColumns;
use crate::region::{ManifestContextRef, RegionLeaderState};
use crate::request::{
    BackgroundNotify, PkIndexBuildFinished, WorkerRequest, WorkerRequestWithTime,
};
use crate::sst::file::{FileHandle, FileTimeRange, RegionFileId};
use crate::sst::location::pk_index_file_path;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::flat_format::{primary_key_column_index, time_index_column_index};
use crate::sst::range_index::build_sst_ranges_index;
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

pub(crate) struct PkIndexBuildRequest {
    pub(crate) region_id: RegionId,
    pub(crate) metadata: RegionMetadataRef,
    pub(crate) access_layer: AccessLayerRef,
    pub(crate) manifest_ctx: ManifestContextRef,
    pub(crate) files: Vec<FileHandle>,
    pub(crate) existing_indexes: HashMap<FileId, AggregatePkIndexMeta>,
    pub(crate) max_sequence: SequenceNumber,
    pub(crate) request_sender: Sender<WorkerRequestWithTime>,
    pub(crate) cache_manager: CacheManagerRef,
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
        // Build per-SST ranges indexes (cache-only, never in the manifest) for the
        // sparse source files this pk-index pass covers. Done here, independent of
        // bucket selection, so files keep an index even when the `.pk` is up to date.
        self.ensure_range_indexes().await;

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

    /// Builds and caches the per-SST ranges index for each eligible sparse source
    /// file that does not already have one cached. Best-effort: failures are logged
    /// and skipped because the index is purely a scan optimization.
    async fn ensure_range_indexes(&self) {
        if self.metadata.primary_key_encoding != PrimaryKeyEncoding::Sparse {
            return;
        }

        let start = Instant::now();
        let mut candidates = 0;
        let mut built = 0;
        let mut skipped_cached = 0;
        let mut failed = 0;
        for file in &self.files {
            let meta = file.meta_ref();
            if meta.region_id != self.region_id || file.is_deleted() || file.compacting() {
                continue;
            }
            // Only files covered by the pk index (sequence within bound) can use the
            // ranges index at scan time, so skip the rest.
            let covered = meta
                .sequence
                .map(|seq| seq.get() <= self.max_sequence)
                .unwrap_or(false);
            if !covered {
                continue;
            }
            candidates += 1;

            let region_file_id = RegionFileId::new(self.region_id, meta.file_id);
            if self.cache_manager.has_pk_range_index(region_file_id) {
                skipped_cached += 1;
                continue;
            }

            let cache_strategy = CacheStrategy::Compaction(self.cache_manager.clone());
            match build_sst_ranges_index(&self.access_layer, &self.metadata, file, cache_strategy)
                .await
            {
                Ok(index) => {
                    if let Err(err) = self
                        .cache_manager
                        .store_pk_range_index(region_file_id, Arc::new(index))
                        .await
                    {
                        failed += 1;
                        common_telemetry::warn!(
                            err; "Failed to store ranges index, region: {}, file: {}",
                            self.region_id, meta.file_id
                        );
                    } else {
                        built += 1;
                    }
                }
                Err(err) => {
                    failed += 1;
                    common_telemetry::warn!(
                        err; "Failed to build ranges index, region: {}, file: {}",
                        self.region_id, meta.file_id
                    );
                }
            }
        }

        if candidates > 0 {
            common_telemetry::info!(
                "Ensured per-SST ranges indexes, region: {}, candidates: {}, built: {}, skipped_cached: {}, failed: {}, cost: {:?}",
                self.region_id,
                candidates,
                built,
                skipped_cached,
                failed,
                start.elapsed(),
            );
        }
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

/// Set of `(table_id, tsid)` series that match a query's tag filters, resolved
/// from the primary-key aggregate index files, together with the data SST files
/// those index files cover.
///
/// Built by [`build_pk_index_tsid_set`]. For a covered SST file the membership
/// test is an exact substitute for evaluating the tag predicates on that file,
/// because the index was aggregated from the same data.
#[derive(Debug, Clone)]
pub(crate) struct PkIndexTsidSet {
    tsids: Arc<HashSet<(u32, u64)>>,
    covered_files: HashSet<FileId>,
}

impl PkIndexTsidSet {
    /// Builds a set from an explicit `(table_id, tsid)` membership set and the
    /// SST files the set may be applied to.
    pub(crate) fn new(tsids: HashSet<(u32, u64)>, covered_files: HashSet<FileId>) -> Self {
        Self {
            tsids: Arc::new(tsids),
            covered_files,
        }
    }

    /// Builds a set from an explicit `(table_id, tsid)` membership set, used by
    /// tests in other modules that need a non-empty tsid set.
    #[cfg(test)]
    pub(crate) fn new_for_test(tsids: HashSet<(u32, u64)>) -> Self {
        Self {
            tsids: Arc::new(tsids),
            covered_files: HashSet::new(),
        }
    }

    /// Returns true if `file_id` is covered by one of the used index files.
    pub(crate) fn covers(&self, file_id: FileId) -> bool {
        self.covered_files.contains(&file_id)
    }

    /// Returns the resolved `(table_id, tsid)` membership set.
    pub(crate) fn tsids(&self) -> Arc<HashSet<(u32, u64)>> {
        self.tsids.clone()
    }

    /// Builds a [`PrimaryKeyFilter`] that keeps rows whose encoded primary key
    /// resolves to a `(table_id, tsid)` in this set.
    pub(crate) fn make_filter(&self, metadata: &RegionMetadataRef) -> Box<dyn PrimaryKeyFilter> {
        Box::new(PkIndexTsidFilter {
            codec: SparsePrimaryKeyCodec::new(metadata),
            tsids: self.tsids.clone(),
        })
    }
}

/// A [`PrimaryKeyFilter`] backed by a tsid allow-set resolved from the pk index.
struct PkIndexTsidFilter {
    codec: SparsePrimaryKeyCodec,
    tsids: Arc<HashSet<(u32, u64)>>,
}

impl PrimaryKeyFilter for PkIndexTsidFilter {
    fn matches(&mut self, pk: &[u8]) -> mito_codec::error::Result<bool> {
        let (table_id, tsid) = self.codec.read_table_id_tsid(pk)?;
        Ok(self.tsids.contains(&(table_id, tsid)))
    }
}

#[derive(Debug, Default)]
struct PkIndexScanCosts {
    lower_filter: Duration,
    select_indexes: Duration,
    read_indexes: Duration,
}

impl PkIndexScanCosts {
    fn total_tracked(&self) -> Duration {
        self.lower_filter + self.select_indexes + self.read_indexes
    }
}

#[derive(Debug, Default)]
struct PkIndexReadStats {
    scanned_rows: usize,
    matched_rows: usize,
    read_cost: Duration,
    evaluate_cost: Duration,
}

impl PkIndexReadStats {
    fn add_batch(&mut self, batch: PkIndexBatchStats) {
        self.scanned_rows += batch.scanned_rows;
        self.matched_rows += batch.matched_rows;
        self.evaluate_cost += batch.evaluate_cost;
    }
}

#[derive(Debug)]
struct PkIndexBatchStats {
    scanned_rows: usize,
    matched_rows: usize,
    evaluate_cost: Duration,
}

struct PkIndexPruningStats<'a> {
    row_groups: &'a [RowGroupMetaData],
    schema: SchemaRef,
}

impl<'a> PkIndexPruningStats<'a> {
    fn new(row_groups: &'a [RowGroupMetaData], schema: SchemaRef) -> Self {
        Self { row_groups, schema }
    }

    fn column_index(&self, column: &Column) -> Option<usize> {
        self.schema.index_of(&column.name).ok()
    }

    fn column_values(&self, column: &Column, is_min: bool) -> Option<ArrayRef> {
        let column_index = self.column_index(column)?;
        let null_scalar: ScalarValue = self
            .schema
            .field(column_index)
            .data_type()
            .try_into()
            .ok()?;
        let scalar_values = self
            .row_groups
            .iter()
            .map(|row_group| {
                let stats = row_group.column(column_index).statistics()?;
                stats_scalar_value(stats, is_min)
            })
            .map(|maybe_scalar| maybe_scalar.unwrap_or_else(|| null_scalar.clone()))
            .collect::<Vec<_>>();
        ScalarValue::iter_to_array(scalar_values).ok()
    }
}

impl PruningStatistics for PkIndexPruningStats<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.column_values(column, true)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.column_values(column, false)
    }

    fn num_containers(&self) -> usize {
        self.row_groups.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let column_index = self.column_index(column)?;
        let null_counts = self
            .row_groups
            .iter()
            .map(|row_group| {
                row_group
                    .column(column_index)
                    .statistics()?
                    .null_count_opt()
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Arc::new(UInt64Array::from(null_counts)))
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

fn stats_scalar_value(stats: &Statistics, is_min: bool) -> Option<ScalarValue> {
    match stats {
        Statistics::Boolean(s) => Some(ScalarValue::Boolean(Some(if is_min {
            *s.min_opt()?
        } else {
            *s.max_opt()?
        }))),
        Statistics::Int32(s) => Some(ScalarValue::Int32(Some(if is_min {
            *s.min_opt()?
        } else {
            *s.max_opt()?
        }))),
        Statistics::Int64(s) => Some(ScalarValue::Int64(Some(if is_min {
            *s.min_opt()?
        } else {
            *s.max_opt()?
        }))),
        Statistics::Int96(_) => None,
        Statistics::Float(s) => Some(ScalarValue::Float32(Some(if is_min {
            *s.min_opt()?
        } else {
            *s.max_opt()?
        }))),
        Statistics::Double(s) => Some(ScalarValue::Float64(Some(if is_min {
            *s.min_opt()?
        } else {
            *s.max_opt()?
        }))),
        Statistics::ByteArray(s) => {
            let bytes = if is_min {
                s.min_bytes_opt()?
            } else {
                s.max_bytes_opt()?
            };
            Some(ScalarValue::Utf8(String::from_utf8(bytes.to_vec()).ok()))
        }
        Statistics::FixedLenByteArray(_) => None,
    }
}

fn pk_index_row_groups_to_read(
    parquet_meta: &ParquetMetaData,
    schema: SchemaRef,
    tag_filters: &[Expr],
) -> Vec<usize> {
    let row_groups = parquet_meta.row_groups();
    let predicate = Predicate::new(tag_filters.to_vec());
    let stats = PkIndexPruningStats::new(row_groups, schema.clone());
    predicate
        .prune_with_stats(&stats, &schema)
        .into_iter()
        .enumerate()
        .filter_map(|(row_group, keep)| keep.then_some(row_group))
        .collect()
}

/// Lowers tag filters for use by primary-key-index and series-key scans.
///
/// Returns `None` if any filter cannot be evaluated exactly from tag columns.
pub(crate) fn tag_filter_evaluators(
    metadata: &RegionMetadataRef,
    tag_filters: &[Expr],
) -> Option<Vec<SimpleFilterEvaluator>> {
    let tag_cols: HashSet<String> = tag_columns(metadata)
        .into_iter()
        .map(|(_, name)| name)
        .collect();
    let mut evaluators = Vec::with_capacity(tag_filters.len());
    for expr in tag_filters {
        let evaluator = SimpleFilterEvaluator::try_new(expr)?;
        if !tag_cols.contains(evaluator.column_name()) {
            return None;
        }
        evaluators.push(evaluator);
    }
    Some(evaluators)
}

/// Resolves the matching tsid set by applying `tag_filters` to the primary-key
/// aggregate index files that intersect `time_range`.
///
/// Returns `Ok(None)` (the scan then falls back to the normal tag prefilter) when:
/// - the region does not use sparse primary-key encoding,
/// - there are no tag filters,
/// - any tag filter cannot be lowered to a [`SimpleFilterEvaluator`] or references
///   a column that is not a tag column in the index (so the set could not be exact),
/// - or no index file covers the scan time range.
pub(crate) async fn build_pk_index_tsid_set(
    access_layer: &AccessLayerRef,
    metadata: &RegionMetadataRef,
    pk_indexes: &HashMap<FileId, AggregatePkIndexMeta>,
    tag_filters: &[Expr],
    time_range: &TimestampRange,
) -> Result<Option<PkIndexTsidSet>> {
    if metadata.primary_key_encoding != PrimaryKeyEncoding::Sparse || pk_indexes.is_empty() {
        PK_INDEX_SCAN_TOTAL.with_label_values(&["skipped"]).inc();
        return Ok(None);
    }

    let start = Instant::now();
    let mut costs = PkIndexScanCosts::default();

    // Lower every tag filter to a SimpleFilterEvaluator. If any tag filter cannot
    // be lowered, the tsid set would not be exact, so bail out to the normal path.
    let stage_start = Instant::now();
    let Some(evaluators) = tag_filter_evaluators(metadata, tag_filters) else {
        costs.lower_filter += stage_start.elapsed();
        PK_INDEX_SCAN_TOTAL.with_label_values(&["skipped"]).inc();
        return Ok(None);
    };
    costs.lower_filter += stage_start.elapsed();

    let stage_start = Instant::now();
    let metas: Vec<&AggregatePkIndexMeta> = pk_indexes
        .values()
        .filter(|meta| index_intersects_range(meta, time_range))
        .collect();
    costs.select_indexes += stage_start.elapsed();
    if metas.is_empty() {
        PK_INDEX_SCAN_TOTAL.with_label_values(&["skipped"]).inc();
        return Ok(None);
    }

    let mut tsids = HashSet::new();
    let mut covered_files = HashSet::new();
    let index_count = metas.len();
    let mut scanned_rows = 0;
    let mut matched_rows = 0;
    for meta in &metas {
        let stage_start = Instant::now();
        let stats = read_pk_index_file(
            access_layer,
            metadata,
            meta,
            &evaluators,
            &tag_filters,
            &mut tsids,
        )
        .await?;
        costs.read_indexes += stage_start.elapsed();
        scanned_rows += stats.scanned_rows;
        matched_rows += stats.matched_rows;
        covered_files.extend(meta.source_file_ids.iter().copied());
    }

    PK_INDEX_SCAN_TOTAL.with_label_values(&["applied"]).inc();
    PK_INDEX_SCAN_TSIDS_TOTAL.inc_by(tsids.len() as u64);
    let total_cost = start.elapsed();
    PK_INDEX_SCAN_BUILD_ELAPSED.observe(total_cost.as_secs_f64());

    common_telemetry::info!(
        "Scanned primary-key aggregate indexes, region: {}, tag_filters: {}, index_files: {}, covered_files: {}, scanned_rows: {}, matched_rows: {}, matched_tsids: {}, lower_filter_cost: {:?}, select_indexes_cost: {:?}, read_indexes_cost: {:?}, tracked_cost: {:?}, total_cost: {:?}",
        metadata.region_id,
        tag_filters.len(),
        index_count,
        covered_files.len(),
        scanned_rows,
        matched_rows,
        tsids.len(),
        costs.lower_filter,
        costs.select_indexes,
        costs.read_indexes,
        costs.total_tracked(),
        total_cost,
    );

    Ok(Some(PkIndexTsidSet {
        tsids: Arc::new(tsids),
        covered_files,
    }))
}

fn index_intersects_range(meta: &AggregatePkIndexMeta, time_range: &TimestampRange) -> bool {
    let (min, max) = meta.time_range;
    TimestampRange::new_inclusive(Some(min), Some(max)).intersects(time_range)
}

async fn read_pk_index_file(
    access_layer: &AccessLayerRef,
    metadata: &RegionMetadataRef,
    meta: &AggregatePkIndexMeta,
    evaluators: &[SimpleFilterEvaluator],
    tag_filters: &[Expr],
    tsids: &mut HashSet<(u32, u64)>,
) -> Result<PkIndexReadStats> {
    let path = pk_index_file_path(
        access_layer.table_dir(),
        metadata.region_id,
        meta.index_file_id,
        access_layer.path_type(),
    );
    let start = Instant::now();
    let stage_start = Instant::now();
    let bytes = access_layer
        .object_store()
        .read(&path)
        .await
        .context(OpenDalSnafu)?
        .to_bytes();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .context(ReadParquetSnafu { path: &path })?;
    let row_groups_total = builder.metadata().num_row_groups();
    let row_groups =
        pk_index_row_groups_to_read(builder.metadata(), builder.schema().clone(), tag_filters);
    let row_groups_pruned = row_groups_total.saturating_sub(row_groups.len());
    let reader = builder
        .with_row_groups(row_groups)
        .build()
        .context(ReadParquetSnafu { path: &path })?;
    let mut stats = PkIndexReadStats {
        read_cost: stage_start.elapsed(),
        ..Default::default()
    };
    for batch in reader {
        let batch = batch.context(NewRecordBatchSnafu)?;
        stats.add_batch(collect_matching_tsids(&batch, evaluators, tsids)?);
    }
    common_telemetry::info!(
        "Read primary-key aggregate index file, region: {}, index_file_id: {}, path: {}, time_range: {:?}, source_files: {}, index_rows: {}, row_groups_total: {}, row_groups_pruned: {}, scanned_rows: {}, matched_rows: {}, read_cost: {:?}, evaluate_cost: {:?}, total_cost: {:?}",
        metadata.region_id,
        meta.index_file_id,
        path,
        meta.time_range,
        meta.source_file_ids.len(),
        meta.row_count,
        row_groups_total,
        row_groups_pruned,
        stats.scanned_rows,
        stats.matched_rows,
        stats.read_cost,
        stats.evaluate_cost,
        start.elapsed(),
    );
    Ok(stats)
}

fn collect_matching_tsids(
    batch: &RecordBatch,
    evaluators: &[SimpleFilterEvaluator],
    tsids: &mut HashSet<(u32, u64)>,
) -> Result<PkIndexBatchStats> {
    let start = Instant::now();
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(PkIndexBatchStats {
            scanned_rows: 0,
            matched_rows: 0,
            evaluate_cost: start.elapsed(),
        });
    }

    let mut mask = datatypes::arrow::buffer::BooleanBuffer::new_set(num_rows);
    for evaluator in evaluators {
        let column = pk_index_column(batch, evaluator.column_name())?;
        let evaluated = evaluator.evaluate_array(column).context(RecordBatchSnafu)?;
        mask = &mask & &evaluated;
    }

    let table_ids = pk_index_column(batch, TABLE_ID_COL)?
        .as_any()
        .downcast_ref::<UInt32Array>()
        .context(InvalidRecordBatchSnafu {
            reason: "pk index __table_id is not UInt32Array",
        })?;
    let series_ids = pk_index_column(batch, TSID_COL)?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context(InvalidRecordBatchSnafu {
            reason: "pk index __tsid is not UInt64Array",
        })?;

    for (row, matched) in mask.iter().enumerate() {
        if matched {
            tsids.insert((table_ids.value(row), series_ids.value(row)));
        }
    }
    let matched_rows = mask.iter().filter(|matched| *matched).count();
    Ok(PkIndexBatchStats {
        scanned_rows: num_rows,
        matched_rows,
        evaluate_cost: start.elapsed(),
    })
}

fn pk_index_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    let index = batch
        .schema()
        .index_of(name)
        .ok()
        .context(InvalidRecordBatchSnafu {
            reason: format!("pk index file is missing column {name}"),
        })?;
    Ok(batch.column(index))
}

/// Collects the tag predicates from `filters` that reference at least one tag
/// (primary-key) column of `metadata`.
///
/// A predicate touching a field/timestamp column in addition to a tag column is
/// still returned here; [`build_pk_index_tsid_set`] then rejects it because it
/// cannot be lowered to a tag-only [`SimpleFilterEvaluator`].
pub(crate) fn tag_filter_exprs(metadata: &RegionMetadataRef, filters: &[Expr]) -> Vec<Expr> {
    let tag_cols: HashSet<&str> = metadata
        .primary_key_columns()
        .filter(|col| {
            col.column_id != ReservedColumnId::table_id()
                && col.column_id != ReservedColumnId::tsid()
        })
        .map(|col| col.column_schema.name.as_str())
        .collect();

    let mut columns = HashSet::new();
    filters
        .iter()
        .filter(|expr| {
            columns.clear();
            if expr_to_columns(expr, &mut columns).is_err() {
                return false;
            }
            columns
                .iter()
                .any(|column| tag_cols.contains(column.name.as_str()))
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use api::v1::OpType;
    use datatypes::arrow::array::{
        BinaryDictionaryBuilder, StringDictionaryBuilder, TimestampMillisecondArray, UInt8Array,
    };
    use object_store::services::Fs;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use store_api::region_request::PathType;

    use super::*;
    use crate::access_layer::{AccessLayer, Metrics, RegionFilePathFactory, WriteType};
    use crate::config::IndexConfig;
    use crate::read::FlatSource;
    use crate::sst::file::{FileMeta, Level};
    use crate::sst::file_purger::NoopFilePurger;
    use crate::sst::index::intermediate::IntermediateManager;
    use crate::sst::index::puffin_manager::PuffinManagerFactory;
    use crate::sst::index::{Indexer, IndexerBuilder};
    use crate::sst::parquet::WriteOptions;
    use crate::sst::parquet::writer::ParquetWriter;
    use crate::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
    use crate::test_util::sst_util::new_sparse_primary_key;

    struct NoopIndexBuilder;

    #[async_trait::async_trait]
    impl IndexerBuilder for NoopIndexBuilder {
        async fn build(&self, _file_id: FileId, _index_version: u64) -> Indexer {
            Indexer::default()
        }
    }

    async fn new_test_access_layer(
        name: &str,
    ) -> (common_test_util::temp_dir::TempDir, AccessLayerRef) {
        let dir = common_test_util::temp_dir::create_temp_dir(name);
        let dir_path = dir.path().display().to_string();
        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();
        let object_store = ObjectStore::new(Fs::default().root(&dir_path))
            .unwrap()
            .finish();
        let access_layer: AccessLayerRef = Arc::new(AccessLayer::new(
            "test_table",
            PathType::Bare,
            object_store,
            puffin_mgr,
            intm_mgr,
        ));
        (dir, access_layer)
    }

    async fn write_pk_index_batches(
        access_layer: &AccessLayerRef,
        metadata: &RegionMetadataRef,
        index_file_id: FileId,
        batches: Vec<RecordBatch>,
        max_row_group_rows: usize,
    ) {
        let path = pk_index_file_path(
            access_layer.table_dir(),
            metadata.region_id,
            index_file_id,
            access_layer.path_type(),
        );
        let schema = pk_columns_schema(metadata);
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(max_row_group_rows))
            .build();
        let mut bytes = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
            for batch in batches {
                writer.write(&batch).unwrap();
            }
            writer.close().unwrap();
        }
        access_layer
            .object_store()
            .write(&path, bytes)
            .await
            .unwrap();
    }

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

    fn file_handle_from_sst_info(
        region_id: RegionId,
        info: crate::sst::parquet::SstInfo,
        sequence: u64,
    ) -> FileHandle {
        FileHandle::new(
            FileMeta {
                region_id,
                file_id: info.file_id,
                time_range: info.time_range,
                level: 0 as Level,
                file_size: info.file_size,
                max_row_group_uncompressed_size: info.max_row_group_uncompressed_size,
                num_rows: info.num_rows as u64,
                num_row_groups: info.num_row_groups,
                num_series: info.num_series,
                sequence: NonZeroU64::new(sequence),
                ..Default::default()
            },
            Arc::new(NoopFilePurger),
        )
    }

    fn sparse_flat_batch(metadata: &RegionMetadataRef) -> RecordBatch {
        let schema = to_flat_sst_arrow_schema(metadata, &FlatSchemaOptions::default());
        let table_ids = [1_u32, 1, 1];
        let tsids = [100_u64, 100, 101];
        let tag_0 = ["host-a", "host-a", "host-b"];
        let tag_1 = ["region-a", "region-a", "region-b"];
        let timestamps = [0_i64, 10, 20];
        let num_rows = timestamps.len();

        let mut tag_0_builder = StringDictionaryBuilder::<UInt32Type>::new();
        let mut tag_1_builder = StringDictionaryBuilder::<UInt32Type>::new();
        let mut pk_builder = BinaryDictionaryBuilder::<UInt32Type>::new();
        for row in 0..num_rows {
            tag_0_builder.append_value(tag_0[row]);
            tag_1_builder.append_value(tag_1[row]);
            let pk = new_sparse_primary_key(
                &[tag_0[row], tag_1[row]],
                metadata,
                table_ids[row],
                tsids[row],
            );
            pk_builder.append(&pk).unwrap();
        }

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from(table_ids.to_vec())) as ArrayRef,
                Arc::new(UInt64Array::from(tsids.to_vec())) as ArrayRef,
                Arc::new(tag_0_builder.finish()) as ArrayRef,
                Arc::new(tag_1_builder.finish()) as ArrayRef,
                Arc::new(UInt64Array::from(vec![1_u64, 2, 3])) as ArrayRef,
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())) as ArrayRef,
                Arc::new(pk_builder.finish()) as ArrayRef,
                Arc::new(UInt64Array::from_value(10, num_rows)) as ArrayRef,
                Arc::new(UInt8Array::from_value(OpType::Put as u8, num_rows)) as ArrayRef,
            ],
        )
        .unwrap()
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

    #[test]
    fn test_collect_matching_tsids_applies_tag_filters() {
        use datafusion_expr::{col, lit};

        let schema = Arc::new(Schema::new(vec![
            Field::new(TABLE_ID_COL, DataType::UInt32, false),
            Field::new(TSID_COL, DataType::UInt64, false),
            Field::new("host", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from(vec![1, 1, 2])),
                Arc::new(UInt64Array::from(vec![10_u64, 11, 12])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")])),
            ],
        )
        .unwrap();

        let evaluator = SimpleFilterEvaluator::try_new(&col("host").eq(lit("a"))).unwrap();
        let mut tsids = HashSet::new();
        collect_matching_tsids(&batch, &[evaluator], &mut tsids).unwrap();

        assert_eq!(tsids, HashSet::from([(1, 10), (2, 12)]));
    }

    #[test]
    fn test_pk_index_tsid_filter_matches_table_id_and_tsid() {
        use crate::test_util::sst_util::sst_region_metadata_with_encoding;

        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let codec = SparsePrimaryKeyCodec::new(&metadata);

        let encode = |table_id: u32, tsid: u64| {
            let mut pk = Vec::new();
            codec.encode_internal(table_id, tsid, &mut pk).unwrap();
            pk
        };

        let set = PkIndexTsidSet {
            tsids: Arc::new(HashSet::from([(7, 42)])),
            covered_files: HashSet::new(),
        };
        let mut filter = set.make_filter(&metadata);

        assert!(filter.matches(&encode(7, 42)).unwrap());
        // Same table, different tsid.
        assert!(!filter.matches(&encode(7, 43)).unwrap());
        // Same tsid, different table.
        assert!(!filter.matches(&encode(8, 42)).unwrap());
    }

    #[tokio::test]
    async fn test_build_pk_index_reads_legacy_sparse_sst() {
        use common_test_util::temp_dir::create_temp_dir;
        use object_store::services::Fs;
        use store_api::region_request::PathType;

        use crate::access_layer::AccessLayer;
        use crate::sst::index::intermediate::IntermediateManager;
        use crate::sst::index::puffin_manager::PuffinManagerFactory;
        use crate::test_util::sst_util::sst_region_metadata_with_encoding;

        let dir = create_temp_dir("pk-index-legacy-sparse-sst");
        let dir_path = dir.path().display().to_string();
        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();
        let object_store = ObjectStore::new(Fs::default().root(&dir_path))
            .unwrap()
            .finish();

        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let table_dir = "test_table";
        let access_layer: AccessLayerRef = Arc::new(AccessLayer::new(
            table_dir,
            PathType::Bare,
            object_store.clone(),
            puffin_mgr,
            intm_mgr,
        ));

        let mut metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            object_store,
            metadata.clone(),
            IndexConfig::default(),
            NoopIndexBuilder,
            RegionFilePathFactory::new(table_dir.to_string(), PathType::Bare),
            &mut metrics,
        )
        .await;
        let batch = sparse_flat_batch(&metadata);
        let source = FlatSource::new_iter(batch.schema(), Box::new(std::iter::once(Ok(batch))));
        let sst_info = writer
            .write_all_flat_as_primary_key(source, None, &WriteOptions::default())
            .await
            .unwrap()
            .remove(0);
        let source_file_id = sst_info.file_id;
        let source_file = file_handle_from_sst_info(metadata.region_id, sst_info, 10);

        let bucket = PkIndexBucket {
            time_range: (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(WINDOW_MILLIS - 1),
            ),
            files: vec![source_file],
        };
        let output = build_pk_index(&access_layer, metadata, bucket, FileId::random(), 10)
            .await
            .unwrap();

        assert_eq!(output.meta.source_file_ids, vec![source_file_id]);
        assert_eq!(output.meta.row_count, 2);
        assert!(output.meta.file_size > 0);
    }

    #[tokio::test]
    async fn test_build_sst_ranges_index_matches_series_runs() {
        use common_test_util::temp_dir::create_temp_dir;
        use object_store::services::Fs;
        use store_api::region_request::PathType;

        use crate::access_layer::AccessLayer;
        use crate::cache::CacheStrategy;
        use crate::sst::index::intermediate::IntermediateManager;
        use crate::sst::index::puffin_manager::PuffinManagerFactory;
        use crate::sst::range_index::build_sst_ranges_index;
        use crate::test_util::sst_util::sst_region_metadata_with_encoding;

        let dir = create_temp_dir("range-index-build");
        let dir_path = dir.path().display().to_string();
        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();
        let object_store = ObjectStore::new(Fs::default().root(&dir_path))
            .unwrap()
            .finish();

        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let table_dir = "test_table";
        let access_layer: AccessLayerRef = Arc::new(AccessLayer::new(
            table_dir,
            PathType::Bare,
            object_store.clone(),
            puffin_mgr,
            intm_mgr,
        ));

        let mut metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            object_store,
            metadata.clone(),
            IndexConfig::default(),
            NoopIndexBuilder,
            RegionFilePathFactory::new(table_dir.to_string(), PathType::Bare),
            &mut metrics,
        )
        .await;
        // table_id=1, tsids=[100, 100, 101]: series (1,100) at rows 0..2, (1,101) at 2..3.
        let batch = sparse_flat_batch(&metadata);
        let source = FlatSource::new_iter(batch.schema(), Box::new(std::iter::once(Ok(batch))));
        let sst_info = writer
            .write_all_flat_as_primary_key(source, None, &WriteOptions::default())
            .await
            .unwrap()
            .remove(0);
        let source_file = file_handle_from_sst_info(metadata.region_id, sst_info, 10);

        let index = build_sst_ranges_index(
            &access_layer,
            &metadata,
            &source_file,
            CacheStrategy::Disabled,
        )
        .await
        .unwrap();

        assert_eq!(index.row_ranges(0, &HashSet::from([(1, 100)])), vec![0..2]);
        assert_eq!(index.row_ranges(0, &HashSet::from([(1, 101)])), vec![2..3]);
        // Both series are adjacent, so they coalesce into a single range.
        assert_eq!(
            index.row_ranges(0, &HashSet::from([(1, 100), (1, 101)])),
            vec![0..3]
        );
        // A series not present in the file selects nothing.
        assert!(index.row_ranges(0, &HashSet::from([(2, 100)])).is_empty());
    }

    #[tokio::test]
    async fn test_build_pk_index_tsid_set_reads_and_filters() {
        use common_test_util::temp_dir::create_temp_dir;
        use datafusion_expr::{col, lit};
        use object_store::services::Fs;
        use store_api::region_request::PathType;

        use crate::access_layer::AccessLayer;
        use crate::sst::index::intermediate::IntermediateManager;
        use crate::sst::index::puffin_manager::PuffinManagerFactory;
        use crate::test_util::sst_util::sst_region_metadata_with_encoding;

        let dir = create_temp_dir("pk-index-scan");
        let dir_path = dir.path().display().to_string();
        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();
        let object_store = ObjectStore::new(Fs::default().root(&dir_path))
            .unwrap()
            .finish();

        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let table_dir = "test_table";
        let access_layer: AccessLayerRef = Arc::new(AccessLayer::new(
            table_dir,
            PathType::Bare,
            object_store,
            puffin_mgr,
            intm_mgr,
        ));

        // Write a `.pk` index file with three series under one source SST.
        let index_file_id = FileId::random();
        let source_file_id = FileId::random();
        let path = pk_index_file_path(
            access_layer.table_dir(),
            metadata.region_id,
            index_file_id,
            access_layer.path_type(),
        );
        let schema = pk_columns_schema(&metadata);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0_i64, 0, 0])),
                Arc::new(Int64Array::from(vec![100_i64, 100, 100])),
                Arc::new(UInt64Array::from(vec![1_u64, 1, 1])),
                Arc::new(UInt32Array::from(vec![1_u32, 1, 1])),
                Arc::new(UInt64Array::from(vec![100_u64, 101, 102])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")])),
                Arc::new(StringArray::from(vec![Some("x"), Some("x"), Some("y")])),
            ],
        )
        .unwrap();
        let mut writer = PkIndexWriter::try_new(access_layer.object_store(), &path, schema)
            .await
            .unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let mut pk_indexes = HashMap::new();
        pk_indexes.insert(
            index_file_id,
            AggregatePkIndexMeta {
                index_file_id,
                time_range: (
                    Timestamp::new_millisecond(0),
                    Timestamp::new_millisecond(1000),
                ),
                max_sequence: 10,
                source_file_ids: vec![source_file_id],
                file_size: 0,
                row_count: 3,
            },
        );

        let tag_filters = vec![col("tag_0").eq(lit("a"))];
        let time_range = TimestampRange::new_inclusive(
            Some(Timestamp::new_millisecond(0)),
            Some(Timestamp::new_millisecond(1000)),
        );
        let set = build_pk_index_tsid_set(
            &access_layer,
            &metadata,
            &pk_indexes,
            &tag_filters,
            &time_range,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(set.tsids.as_ref(), &HashSet::from([(1, 100), (1, 102)]));
        assert!(set.covers(source_file_id));
        assert!(!set.covers(FileId::random()));

        // A filter touching the time index cannot be enforced on the index file.
        let non_intersecting = TimestampRange::new_inclusive(
            Some(Timestamp::new_millisecond(2000)),
            Some(Timestamp::new_millisecond(3000)),
        );
        let none = build_pk_index_tsid_set(
            &access_layer,
            &metadata,
            &pk_indexes,
            &tag_filters,
            &non_intersecting,
        )
        .await
        .unwrap();
        assert!(none.is_none());
    }

    #[tokio::test]
    async fn test_read_pk_index_file_prunes_row_groups_by_stats() {
        use datafusion_expr::{col, lit};

        use crate::test_util::sst_util::sst_region_metadata_with_encoding;

        let (_dir, access_layer) = new_test_access_layer("pk-index-rg-prune").await;
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let index_file_id = FileId::random();
        let schema = pk_columns_schema(&metadata);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0_i64; 6])),
                Arc::new(Int64Array::from(vec![100_i64; 6])),
                Arc::new(UInt64Array::from(vec![1_u64; 6])),
                Arc::new(UInt32Array::from(vec![1_u32; 6])),
                Arc::new(UInt64Array::from(vec![100_u64, 101, 102, 103, 104, 105])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("m"),
                    Some("m"),
                    Some("z"),
                    Some("z"),
                ])),
                Arc::new(StringArray::from(vec![Some("x"); 6])),
            ],
        )
        .unwrap();
        write_pk_index_batches(&access_layer, &metadata, index_file_id, vec![batch], 2).await;

        let meta = AggregatePkIndexMeta {
            index_file_id,
            time_range: (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(1000),
            ),
            max_sequence: 10,
            source_file_ids: vec![FileId::random()],
            file_size: 0,
            row_count: 6,
        };
        let tag_filters = vec![col("tag_0").eq(lit("m"))];
        let evaluators = tag_filters
            .iter()
            .map(SimpleFilterEvaluator::try_new)
            .collect::<Option<Vec<_>>>()
            .unwrap();
        let mut tsids = HashSet::new();

        let stats = read_pk_index_file(
            &access_layer,
            &metadata,
            &meta,
            &evaluators,
            &tag_filters,
            &mut tsids,
        )
        .await
        .unwrap();

        assert_eq!(stats.scanned_rows, 2);
        assert_eq!(stats.matched_rows, 2);
        assert_eq!(tsids, HashSet::from([(1, 102), (1, 103)]));
    }

    #[tokio::test]
    async fn test_read_pk_index_file_skips_all_pruned_row_groups() {
        use datafusion_expr::{col, lit};

        use crate::test_util::sst_util::sst_region_metadata_with_encoding;

        let (_dir, access_layer) = new_test_access_layer("pk-index-rg-prune-all").await;
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let index_file_id = FileId::random();
        let schema = pk_columns_schema(&metadata);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0_i64; 4])),
                Arc::new(Int64Array::from(vec![100_i64; 4])),
                Arc::new(UInt64Array::from(vec![1_u64; 4])),
                Arc::new(UInt32Array::from(vec![1_u32; 4])),
                Arc::new(UInt64Array::from(vec![100_u64, 101, 102, 103])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("y"),
                    Some("z"),
                ])),
                Arc::new(StringArray::from(vec![Some("x"); 4])),
            ],
        )
        .unwrap();
        write_pk_index_batches(&access_layer, &metadata, index_file_id, vec![batch], 2).await;

        let meta = AggregatePkIndexMeta {
            index_file_id,
            time_range: (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(1000),
            ),
            max_sequence: 10,
            source_file_ids: vec![FileId::random()],
            file_size: 0,
            row_count: 4,
        };
        let tag_filters = vec![col("tag_0").eq(lit("m"))];
        let evaluators = tag_filters
            .iter()
            .map(SimpleFilterEvaluator::try_new)
            .collect::<Option<Vec<_>>>()
            .unwrap();
        let mut tsids = HashSet::new();

        let stats = read_pk_index_file(
            &access_layer,
            &metadata,
            &meta,
            &evaluators,
            &tag_filters,
            &mut tsids,
        )
        .await
        .unwrap();

        assert_eq!(stats.scanned_rows, 0);
        assert_eq!(stats.matched_rows, 0);
        assert!(tsids.is_empty());
    }

    #[tokio::test]
    async fn test_read_pk_index_file_keeps_row_groups_for_regex_filter() {
        use datafusion_expr::{BinaryExpr, Operator, col, lit};

        use crate::test_util::sst_util::sst_region_metadata_with_encoding;

        let (_dir, access_layer) = new_test_access_layer("pk-index-rg-regex").await;
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let index_file_id = FileId::random();
        let schema = pk_columns_schema(&metadata);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0_i64; 4])),
                Arc::new(Int64Array::from(vec![100_i64; 4])),
                Arc::new(UInt64Array::from(vec![1_u64; 4])),
                Arc::new(UInt32Array::from(vec![1_u32; 4])),
                Arc::new(UInt64Array::from(vec![100_u64, 101, 102, 103])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("m"),
                    Some("n"),
                ])),
                Arc::new(StringArray::from(vec![Some("x"); 4])),
            ],
        )
        .unwrap();
        write_pk_index_batches(&access_layer, &metadata, index_file_id, vec![batch], 2).await;

        let meta = AggregatePkIndexMeta {
            index_file_id,
            time_range: (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(1000),
            ),
            max_sequence: 10,
            source_file_ids: vec![FileId::random()],
            file_size: 0,
            row_count: 4,
        };
        let tag_filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("tag_0")),
            op: Operator::RegexMatch,
            right: Box::new(lit("^m$")),
        })];
        let evaluators = tag_filters
            .iter()
            .map(SimpleFilterEvaluator::try_new)
            .collect::<Option<Vec<_>>>()
            .unwrap();
        let mut tsids = HashSet::new();

        let stats = read_pk_index_file(
            &access_layer,
            &metadata,
            &meta,
            &evaluators,
            &tag_filters,
            &mut tsids,
        )
        .await
        .unwrap();

        assert_eq!(stats.scanned_rows, 4);
        assert_eq!(stats.matched_rows, 1);
        assert_eq!(tsids, HashSet::from([(1, 102)]));
    }

    #[test]
    fn test_tag_filter_exprs_keeps_only_tag_touching_filters() {
        use datafusion_expr::{col, lit};

        use crate::test_util::sst_util::sst_region_metadata_with_encoding;

        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let tag_name = metadata
            .primary_key_columns()
            .find(|col| {
                col.column_id != ReservedColumnId::table_id()
                    && col.column_id != ReservedColumnId::tsid()
            })
            .map(|col| col.column_schema.name.clone())
            .unwrap();
        let field_name = metadata
            .field_columns()
            .next()
            .map(|col| col.column_schema.name.clone())
            .unwrap();

        let tag_expr = col(&tag_name).eq(lit("a"));
        let field_expr = col(&field_name).gt(lit(1_u64));
        let filters = vec![tag_expr.clone(), field_expr];

        let kept = tag_filter_exprs(&metadata, &filters);
        assert_eq!(kept, vec![tag_expr]);
    }
}
