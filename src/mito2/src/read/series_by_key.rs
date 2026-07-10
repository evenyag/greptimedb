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

//! SeriesScan-by-key implementation for sparse primary keys.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use async_stream::try_stream;
use common_telemetry::tracing::Instrument;
use common_telemetry::{debug, info};
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool};
use datafusion::physical_expr::expressions::col as physical_col;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion_expr::utils::expr_to_columns;
use datatypes::arrow::array::{Array, ArrayRef, BinaryArray};
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef as ArrowSchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use futures::{StreamExt, TryStreamExt};
use mito_codec::row_converter::{PrimaryKeyCodec, PrimaryKeyFilter, SparsePrimaryKeyCodec};
use parquet::arrow::ProjectionMask;
use smallvec::SmallVec;
use snafu::{OptionExt, ResultExt};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::FileId;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::Sender;

use crate::error::{
    DatafusionSnafu, DecodeSnafu, Error, NewRecordBatchSnafu, Result, ScanSeriesSnafu,
    UnexpectedSnafu,
};
use crate::read::BoxedRecordBatchStream;
use crate::read::range::FileRangeBuilder;
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::{
    compute_average_batch_size, compute_parallel_channel_size, new_filter_metrics,
};
use crate::read::seq_scan::SeqScan;
use crate::read::stream::ScanBatch;
use crate::sst::file::RegionFileId;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::file_range::{FileRange, PreFilterMode};
use crate::sst::parquet::format::PrimaryKeyArray;
use crate::sst::parquet::reader::{MaybeFilter, ReaderMetrics, SimpleFilterContext};
use crate::sst::parquet::series_key_filter::SeriesKeyFilter;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct SeriesKey {
    table_id: u32,
    tsid: u64,
}

pub(crate) type SeriesByKeyReceiverList =
    Vec<Option<tokio::sync::mpsc::Receiver<Result<ScanBatch>>>>;

struct PreparedSeriesByKeyFile {
    file_id: RegionFileId,
    builder: Arc<FileRangeBuilder>,
}

struct SeriesByKeyPlan {
    files: Arc<Vec<PreparedSeriesByKeyFile>>,
    partition_keys: Vec<Vec<SeriesKey>>,
    covered_files: HashSet<FileId>,
}

pub(crate) fn start_series_scan_by_key(
    stream_ctx: Arc<StreamContext>,
    num_partitions: usize,
) -> SeriesByKeyReceiverList {
    let mut senders = Vec::with_capacity(num_partitions);
    let mut receivers = Vec::with_capacity(num_partitions);
    for _ in 0..num_partitions {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        senders.push(sender);
        receivers.push(Some(receiver));
    }

    let span = tracing::info_span!(
        "SeriesScan::by_key",
        region_id = %stream_ctx.input.region_metadata().region_id
    );
    common_runtime::spawn_query(
        async move {
            execute_series_scan_by_key(stream_ctx, senders).await;
        }
        .instrument(span),
    );

    receivers
}

pub(crate) fn can_use_series_scan_by_key(input: &crate::read::scan_region::ScanInput) -> bool {
    if !input.experimental_series_scan_by_key {
        debug!("SeriesScan-by-key is ineligible: disabled");
        return false;
    }
    if input.compaction || !input.memtables.is_empty() {
        debug!("SeriesScan-by-key is ineligible: compaction or memtables are present");
        return false;
    }
    #[cfg(feature = "enterprise")]
    if !input.extension_ranges().is_empty() {
        debug!("SeriesScan-by-key is ineligible: extension ranges are present");
        return false;
    }
    let metadata = input.region_metadata();
    if metadata.primary_key_encoding != PrimaryKeyEncoding::Sparse || metadata.primary_key.len() < 2
    {
        debug!("SeriesScan-by-key is ineligible: primary key is not sparse");
        return false;
    }

    supported_tag_filters(input).is_some_and(|filters| !filters.is_empty())
}

async fn execute_series_scan_by_key(
    stream_ctx: Arc<StreamContext>,
    senders: Vec<Sender<Result<ScanBatch>>>,
) {
    let result = resolve_series_by_key_plan(stream_ctx.clone(), senders.len()).await;
    let plan = match result {
        Ok(plan) => Arc::new(plan),
        Err(error) => {
            send_error_to_all(senders, error).await;
            return;
        }
    };

    for (partition, sender) in senders.into_iter().enumerate() {
        let stream_ctx = stream_ctx.clone();
        let plan = plan.clone();
        common_runtime::spawn_query(async move {
            let result = scan_series_by_key_partition(stream_ctx, plan, partition).await;
            match result {
                Ok(mut stream) => {
                    while let Some(batch) = stream.next().await {
                        let batch = batch.map(ScanBatch::RecordBatch);
                        if sender.send(batch).await.is_err() {
                            break;
                        }
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                }
            }
        });
    }
}

async fn send_error_to_all(senders: Vec<Sender<Result<ScanBatch>>>, error: Error) {
    let error = Arc::new(error);
    for sender in senders {
        let result = Err(error.clone()).context(ScanSeriesSnafu);
        let _ = sender.send(result).await;
    }
}

async fn resolve_series_by_key_plan(
    stream_ctx: Arc<StreamContext>,
    num_partitions: usize,
) -> Result<SeriesByKeyPlan> {
    let input = &stream_ctx.input;
    let metadata = input.region_metadata();
    let filters = supported_tag_filters(input).context(UnexpectedSnafu {
        reason: "unsupported tag filters for SeriesScan-by-key",
    })?;
    let codec = SparsePrimaryKeyCodec::new(metadata);
    let filters = Arc::new(filters);
    let mut prepared_files = Vec::with_capacity(input.files.len());
    let mut key_streams = Vec::new();
    let mut covered_files = HashSet::new();
    let mut reader_metrics = ReaderMetrics {
        filter_metrics: new_filter_metrics(false),
        ..Default::default()
    };
    let start = Instant::now();

    for file in &input.files {
        let builder = input
            .prune_file_for_series_scan_by_key(file, PreFilterMode::All, &mut reader_metrics)
            .await?;
        let file_id = file.file_id();
        let mut ranges = SmallVec::<[FileRange; 2]>::new();
        builder.build_ranges(-1, &mut ranges);
        if ranges.is_empty() {
            continue;
        }

        covered_files.insert(file.meta_ref().file_id);
        let builder = Arc::new(builder);
        key_streams.push(build_file_key_stream(
            Arc::clone(&builder),
            Some(codec.primary_key_filter(metadata, filters.clone())),
            series_key_stream_schema(),
        ));
        prepared_files.push(PreparedSeriesByKeyFile { file_id, builder });
    }

    let key_schema = series_key_stream_schema();
    let channel_size = compute_parallel_channel_size(input.series_scan_by_key_batch_size.max(1));
    let key_streams = input.create_parallel_flat_sources(
        key_streams,
        Arc::new(Semaphore::new(input.max_concurrent_scan_files.max(1))),
        channel_size,
    )?;
    let key_streams = key_streams
        .into_iter()
        .map(|stream| mito_to_df_stream(key_schema.clone(), stream))
        .collect();
    let merged = build_streaming_merge_key_stream(
        key_schema,
        key_streams,
        input.series_scan_by_key_batch_size,
    )?;
    let partition_keys = distribute_merged_keys(
        merged,
        metadata,
        input.series_scan_by_key_batch_size,
        num_partitions,
    )
    .await?;

    info!(
        "Resolved SeriesScan-by-key plan, region_id: {}, files: {}, partitions: {}, cost: {:?}",
        metadata.region_id,
        prepared_files.len(),
        num_partitions,
        start.elapsed()
    );

    Ok(SeriesByKeyPlan {
        files: Arc::new(prepared_files),
        partition_keys,
        covered_files,
    })
}

fn build_file_key_stream(
    builder: Arc<FileRangeBuilder>,
    mut pk_filter: Option<Box<dyn PrimaryKeyFilter>>,
    output_schema: ArrowSchemaRef,
) -> BoxedRecordBatchStream {
    Box::pin(try_stream! {
        let mut ranges = SmallVec::<[FileRange; 2]>::new();
        builder.build_ranges(-1, &mut ranges);
        let mut last_pk = None;

        for range in ranges {
            let reader_builder = range.context().reader_builder();
            let arrow_schema = range.context().read_format().arrow_schema();
            let (pk_idx, _) = arrow_schema
                .column_with_name(PRIMARY_KEY_COLUMN_NAME)
                .context(UnexpectedSnafu {
                    reason: "primary key column not found in SST schema",
                })?;
            let projection = ProjectionMask::roots(
                reader_builder
                    .parquet_metadata()
                    .file_metadata()
                    .schema_descr(),
                [pk_idx],
            );
            let mut stream = reader_builder
                .build_with_projection(
                    range.row_group_idx(),
                    range.row_selection().cloned(),
                    projection,
                    None,
                )
                .await?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let primary_keys =
                    collect_batch_primary_key_bytes(&mut pk_filter, batch.column(0), &mut last_pk)?;
                if primary_keys.is_empty() {
                    continue;
                }
                let pk_array = BinaryArray::from_iter_values(primary_keys.iter().map(Vec::as_slice));
                yield RecordBatch::try_new(
                    output_schema.clone(),
                    vec![Arc::new(pk_array) as ArrayRef],
                )
                .context(NewRecordBatchSnafu)?;
            }
        }
    })
}

async fn scan_series_by_key_partition(
    stream_ctx: Arc<StreamContext>,
    plan: Arc<SeriesByKeyPlan>,
    partition: usize,
) -> Result<BoxedRecordBatchStream> {
    let keys = plan
        .partition_keys
        .get(partition)
        .cloned()
        .unwrap_or_default();
    if keys.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }

    let filter = SeriesKeyFilter::new(
        keys.into_iter()
            .map(|key| (key.table_id, key.tsid))
            .collect(),
        plan.covered_files.clone(),
    );
    if filter.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }

    let mut sources = Vec::new();
    let mut estimated_batch_sizes = Vec::new();
    for prepared in plan.files.iter() {
        if !filter.covers(prepared.file_id.file_id()) {
            continue;
        }
        let stream = scan_prepared_file_with_series_key_filter(
            stream_ctx.clone(),
            prepared.builder.clone(),
            filter.clone(),
        );
        sources.push(Box::pin(stream) as BoxedRecordBatchStream);
        estimated_batch_sizes.push(DEFAULT_READ_BATCH_SIZE);
    }

    if sources.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }

    let channel_size =
        compute_parallel_channel_size(compute_average_batch_size(estimated_batch_sizes));
    SeqScan::build_flat_reader_from_sources(
        &stream_ctx,
        sources,
        Some(Arc::new(Semaphore::new(
            stream_ctx.input.max_concurrent_scan_files.max(1),
        ))),
        None,
        false,
        channel_size,
    )
    .await
}

fn scan_prepared_file_with_series_key_filter(
    stream_ctx: Arc<StreamContext>,
    builder: Arc<FileRangeBuilder>,
    filter: SeriesKeyFilter,
) -> impl futures::Stream<Item = Result<RecordBatch>> {
    try_stream! {
        let mut ranges = SmallVec::<[FileRange; 2]>::new();
        builder.build_ranges(-1, &mut ranges);
        for range in ranges {
            let Some(mut reader) = range
                .flat_reader_with_series_key_filter(
                    stream_ctx.input.series_row_selector,
                    None,
                    Some(&filter),
                )
                .await?
            else {
                continue;
            };

            let may_compat = range.compat_batch();
            let mapper = range.compaction_projection_mapper();
            while let Some(record_batch) = reader.next_batch().await? {
                let record_batch = if let Some(mapper) = mapper {
                    mapper.project(record_batch)?
                } else {
                    record_batch
                };
                if let Some(flat_compat) = may_compat {
                    yield flat_compat.compat(record_batch)?;
                } else {
                    yield record_batch;
                }
            }
        }
    }
}

async fn distribute_merged_keys(
    mut stream: DfSendableRecordBatchStream,
    metadata: &RegionMetadataRef,
    batch_size: usize,
    num_partitions: usize,
) -> Result<Vec<Vec<SeriesKey>>> {
    let codec = SparsePrimaryKeyCodec::new(metadata);
    let batch_size = batch_size.max(1);
    let mut partitions = vec![Vec::new(); num_partitions];
    let mut batch = Vec::with_capacity(batch_size);
    let mut batch_idx = 0usize;
    let mut last_key = None;

    while let Some(record_batch) = stream.next().await {
        let record_batch = record_batch.context(DatafusionSnafu)?;
        let pk_col = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context(UnexpectedSnafu {
                reason: "merged primary key column is not a binary array",
            })?;

        for row in 0..pk_col.len() {
            let pk = pk_col.value(row);
            if last_key.as_deref() == Some(pk) {
                continue;
            }
            last_key = Some(pk.to_vec());
            let (table_id, tsid) = codec.read_table_id_tsid(pk).context(DecodeSnafu)?;
            batch.push(SeriesKey { table_id, tsid });

            if batch.len() == batch_size {
                partitions[batch_idx % num_partitions].append(&mut batch);
                batch_idx += 1;
            }
        }
    }
    if !batch.is_empty() {
        partitions[batch_idx % num_partitions].append(&mut batch);
    }
    Ok(partitions)
}

fn collect_batch_primary_key_bytes(
    pk_filter: &mut Option<Box<dyn PrimaryKeyFilter>>,
    pk_col: &ArrayRef,
    last_pk: &mut Option<Vec<u8>>,
) -> Result<Vec<Vec<u8>>> {
    let mut primary_keys = Vec::new();
    if let Some(dict) = pk_col.as_any().downcast_ref::<PrimaryKeyArray>() {
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context(UnexpectedSnafu {
                reason: "primary key dictionary values are not binary",
            })?;
        let mut last_dict_key = None;
        for key in dict.keys().values().iter().copied() {
            if last_dict_key == Some(key) {
                continue;
            }
            last_dict_key = Some(key);
            push_primary_key_bytes(
                pk_filter,
                values.value(key as usize),
                last_pk,
                &mut primary_keys,
            )?;
        }
    } else if let Some(binary) = pk_col.as_any().downcast_ref::<BinaryArray>() {
        let mut row = 0;
        while row < binary.len() {
            let pk = binary.value(row);
            push_primary_key_bytes(pk_filter, pk, last_pk, &mut primary_keys)?;
            row += 1;
            while row < binary.len() && binary.value(row) == pk {
                row += 1;
            }
        }
    } else {
        return UnexpectedSnafu {
            reason: format!(
                "primary key column is neither a dictionary nor binary array, got {:?}",
                pk_col.data_type()
            ),
        }
        .fail();
    }
    Ok(primary_keys)
}

fn push_primary_key_bytes(
    pk_filter: &mut Option<Box<dyn PrimaryKeyFilter>>,
    pk: &[u8],
    last_pk: &mut Option<Vec<u8>>,
    primary_keys: &mut Vec<Vec<u8>>,
) -> Result<()> {
    if last_pk.as_deref() == Some(pk) {
        return Ok(());
    }
    *last_pk = Some(pk.to_vec());
    if let Some(filter) = pk_filter.as_deref_mut()
        && !filter.matches(pk).context(DecodeSnafu)?
    {
        return Ok(());
    }
    primary_keys.push(pk.to_vec());
    Ok(())
}

fn supported_tag_filters(
    input: &crate::read::scan_region::ScanInput,
) -> Option<Vec<common_recordbatch::filter::SimpleFilterEvaluator>> {
    let metadata = input.region_metadata();
    let tag_names = metadata
        .column_metadatas
        .iter()
        .filter(|column| column.semantic_type == api::v1::SemanticType::Tag)
        .map(|column| column.column_schema.name.as_str())
        .collect::<HashSet<_>>();
    let predicate = input.predicate_group().predicate_without_region()?;
    let mut filters = Vec::new();
    let mut columns = HashSet::new();
    for expr in predicate.exprs() {
        columns.clear();
        expr_to_columns(expr, &mut columns).ok()?;
        if columns.is_empty()
            || columns
                .iter()
                .any(|column| !tag_names.contains(column.name.as_str()))
        {
            return None;
        }
        let filter_ctx = SimpleFilterContext::new_opt(metadata, None, expr)?;
        if filter_ctx.semantic_type() != api::v1::SemanticType::Tag {
            return None;
        }
        let MaybeFilter::Filter(filter) = filter_ctx.filter() else {
            return None;
        };
        filters.push(filter.clone());
    }
    Some(filters)
}

fn build_streaming_merge_key_stream(
    schema: ArrowSchemaRef,
    streams: Vec<DfSendableRecordBatchStream>,
    batch_size: usize,
) -> Result<DfSendableRecordBatchStream> {
    if streams.is_empty() {
        return Ok(Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::empty(),
        )));
    }

    let sort_expr = physical_col(PRIMARY_KEY_COLUMN_NAME, schema.as_ref())
        .map(PhysicalSortExpr::new_default)
        .context(DatafusionSnafu)?;
    let ordering = LexOrdering::from([sort_expr]);
    let metrics_set = ExecutionPlanMetricsSet::new();
    let memory_pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let reservation = MemoryConsumer::new("SeriesScanByKeyStreamingMerge").register(&memory_pool);

    StreamingMergeBuilder::new()
        .with_streams(streams)
        .with_schema(schema)
        .with_expressions(&ordering)
        .with_metrics(BaselineMetrics::new(&metrics_set, 0))
        .with_batch_size(batch_size.max(1))
        .with_fetch(None)
        .with_reservation(reservation)
        .build()
        .context(DatafusionSnafu)
}

fn mito_to_df_stream(
    schema: ArrowSchemaRef,
    stream: BoxedRecordBatchStream,
) -> DfSendableRecordBatchStream {
    Box::pin(DfRecordBatchStreamAdapter::new(
        schema,
        stream.map_err(|error| DataFusionError::External(Box::new(error))),
    ))
}

fn series_key_stream_schema() -> ArrowSchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        PRIMARY_KEY_COLUMN_NAME,
        DataType::Binary,
        false,
    )]))
}
