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

//! Per-series scan implementation.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_stream::try_stream;
use common_error::ext::BoxedError;
use common_recordbatch::util::ChainedRecordBatchStream;
use common_recordbatch::{RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::tracing::{self, Instrument};
use common_telemetry::{debug, info, warn};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray};
use datatypes::arrow::compute::filter_record_batch;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::schema::SchemaRef;
use futures::{StreamExt, TryStreamExt};
use mito_codec::row_converter::{PrimaryKeyCodec, SparsePrimaryKeyCodec};
use parquet::arrow::ProjectionMask;
use smallvec::SmallVec;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    PartitionRange, PrepareRequest, QueryScanContext, RegionScanner, ScannerProperties,
};
use store_api::storage::FileId;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::error::{SendTimeoutError, TrySendError};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::error::{
    ComputeArrowSnafu, DecodeSnafu, Error, InvalidSenderSnafu, JoinSnafu, PartitionOutOfRangeSnafu,
    Result, ScanMultiTimesSnafu, ScanSeriesSnafu, TooManyFilesToReadSnafu, UnexpectedSnafu,
};
use crate::read::pruner::{PartitionPruner, Pruner};
use crate::read::range::{FileRangeBuilder, RowGroupIndex};
use crate::read::scan_region::{ScanInput, StreamContext};
use crate::read::scan_util::{
    PartitionMetrics, PartitionMetricsList, SeriesDistributorMetrics,
    build_flat_file_range_scan_stream, compute_average_batch_size, compute_parallel_channel_size,
    new_filter_metrics, scan_flat_mem_ranges,
};
use crate::read::seq_scan::SeqScan;
use crate::read::stream::{ConvertBatchStream, ScanBatch, ScanBatchStream};
use crate::read::{BoxedRecordBatchStream, ScannerMetrics};
use crate::sst::parquet::file_range::{FileRange, FileRangeContext, PreFilterMode};
use crate::sst::parquet::flat_format::primary_key_column_index;
use crate::sst::parquet::format::PrimaryKeyArray;
use crate::sst::parquet::reader::ReaderMetrics;
use crate::sst::parquet::row_selection::RowGroupSelection;
use crate::sst::pk_index::{
    PkIndexTsidSet, build_pk_index_tsid_set, tag_filter_evaluators, tag_filter_exprs,
};

/// Timeout to send a batch to a sender.
const SEND_TIMEOUT: Duration = Duration::from_micros(100);

/// List of receivers.
type ReceiverList = Vec<Option<PartitionReceiver>>;

enum PartitionReceiver {
    ScanBatch(Receiver<Result<ScanBatch>>),
    SeriesKey(Receiver<Result<SeriesKeyBatch>>),
}

/// Scans a region and returns sorted rows of a series in the same partition.
///
/// The output order is always order by `(primary key, time index)` inside every
/// partition.
/// Always returns the same series (primary key) to the same partition.
pub struct SeriesScan {
    /// Properties of the scanner.
    properties: ScannerProperties,
    /// Context of streams.
    stream_ctx: Arc<StreamContext>,
    /// Shared pruner for file range building.
    pruner: Arc<Pruner>,
    /// Receivers of each partition.
    receivers: Mutex<ReceiverList>,
    /// Metrics for each partition.
    /// The scanner only sets in query and keeps it empty during compaction.
    metrics_list: Arc<PartitionMetricsList>,
}

impl SeriesScan {
    /// Creates a new [SeriesScan].
    pub(crate) fn new(input: ScanInput) -> Self {
        let mut properties = ScannerProperties::default()
            .with_append_mode(input.append_mode)
            .with_total_rows(input.total_rows());
        let stream_ctx = Arc::new(StreamContext::seq_scan_ctx(input));
        properties.partitions = vec![stream_ctx.partition_ranges()];

        // Create the shared pruner with number of workers equal to CPU cores.
        let num_workers = common_stat::get_total_cpu_cores().max(1);
        let pruner = Arc::new(Pruner::new(stream_ctx.clone(), num_workers));

        Self {
            properties,
            stream_ctx,
            pruner,
            receivers: Mutex::new(Vec::new()),
            metrics_list: Arc::new(PartitionMetricsList::default()),
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(
            region_id = %self.stream_ctx.input.mapper.metadata().region_id,
            partition = partition
        )
    )]
    fn scan_partition_impl(
        &self,
        ctx: &QueryScanContext,
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        let metrics = new_partition_metrics(
            &self.stream_ctx,
            ctx.explain_verbose,
            metrics_set,
            partition,
            &self.metrics_list,
        );

        let batch_stream =
            self.scan_batch_in_partition(ctx, partition, metrics.clone(), metrics_set)?;

        let input = &self.stream_ctx.input;
        let record_batch_stream = ConvertBatchStream::new(
            batch_stream,
            input.mapper.clone(),
            input.cache_strategy.clone(),
            metrics,
        );

        Ok(Box::pin(RecordBatchStreamWrapper::new(
            input.mapper.output_schema(),
            Box::pin(record_batch_stream),
        )))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            region_id = %self.stream_ctx.input.mapper.metadata().region_id,
            partition = partition
        )
    )]
    fn scan_batch_in_partition(
        &self,
        ctx: &QueryScanContext,
        partition: usize,
        part_metrics: PartitionMetrics,
        metrics_set: &ExecutionPlanMetricsSet,
    ) -> Result<ScanBatchStream> {
        if ctx.explain_verbose {
            common_telemetry::info!(
                "SeriesScan partition {}, region_id: {}",
                partition,
                self.stream_ctx.input.region_metadata().region_id
            );
        }

        ensure!(
            partition < self.properties.num_partitions(),
            PartitionOutOfRangeSnafu {
                given: partition,
                all: self.properties.num_partitions(),
            }
        );

        self.maybe_start_distributor(metrics_set, &self.metrics_list, ctx.explain_verbose);

        match self.take_receiver(partition)? {
            PartitionReceiver::ScanBatch(receiver) => {
                Ok(Self::scan_distributed_batches(receiver, part_metrics))
            }
            PartitionReceiver::SeriesKey(receiver) => Ok(Self::scan_series_key_partition(
                self.stream_ctx.clone(),
                receiver,
                part_metrics,
            )),
        }
    }

    /// Takes the receiver for the partition.
    fn take_receiver(&self, partition: usize) -> Result<PartitionReceiver> {
        let mut rx_list = self.receivers.lock().unwrap();
        rx_list[partition]
            .take()
            .context(ScanMultiTimesSnafu { partition })
    }

    fn scan_distributed_batches(
        mut receiver: Receiver<Result<ScanBatch>>,
        part_metrics: PartitionMetrics,
    ) -> ScanBatchStream {
        let stream = try_stream! {
            part_metrics.on_first_poll();

            let mut fetch_start = Instant::now();
            while let Some(scan_batch) = receiver.recv().await {
                let scan_batch = scan_batch?;

                let mut metrics = ScannerMetrics::default();
                metrics.scan_cost += fetch_start.elapsed();
                fetch_start = Instant::now();

                match &scan_batch {
                    ScanBatch::Series(series) => {
                        metrics.num_batches += series.num_batches();
                        metrics.num_rows += series.num_rows();
                    }
                    ScanBatch::RecordBatch(batch) => {
                        metrics.num_batches += 1;
                        metrics.num_rows += batch.num_rows();
                    }
                }

                let yield_start = Instant::now();
                yield scan_batch;
                metrics.yield_cost += yield_start.elapsed();

                part_metrics.merge_metrics(&metrics);
            }

            part_metrics.on_finish();
        };
        Box::pin(stream)
    }

    fn scan_series_key_partition(
        stream_ctx: Arc<StreamContext>,
        mut receiver: Receiver<Result<SeriesKeyBatch>>,
        part_metrics: PartitionMetrics,
    ) -> ScanBatchStream {
        let stream = try_stream! {
            part_metrics.on_first_poll();

            let mut fetch_start = Instant::now();
            let mut reader =
                SeriesKeyPartitionReader::build_partition_reader(
                    stream_ctx,
                    &mut receiver,
                    &part_metrics,
                )
                .await?;

            while let Some(batch) = reader.try_next().await? {
                let mut metrics = ScannerMetrics::default();
                metrics.scan_cost += fetch_start.elapsed();
                metrics.num_batches += 1;
                metrics.num_rows += batch.num_rows();
                fetch_start = Instant::now();

                let yield_start = Instant::now();
                yield ScanBatch::RecordBatch(batch);
                metrics.yield_cost += yield_start.elapsed();

                part_metrics.merge_metrics(&metrics);
            }

            part_metrics.on_finish();
        };
        Box::pin(stream)
    }

    /// Starts the distributor if the receiver list is empty.
    #[tracing::instrument(
        skip(self, metrics_set, metrics_list),
        fields(region_id = %self.stream_ctx.input.mapper.metadata().region_id)
    )]
    fn maybe_start_distributor(
        &self,
        metrics_set: &ExecutionPlanMetricsSet,
        metrics_list: &Arc<PartitionMetricsList>,
        explain_verbose: bool,
    ) {
        let mut rx_list = self.receivers.lock().unwrap();
        if !rx_list.is_empty() {
            return;
        }

        if self.use_series_key_path() {
            info!(
                "Selected SeriesScan path, region_id: {}, series_scan_path: series_key",
                self.stream_ctx.input.mapper.metadata().region_id
            );
            *rx_list =
                SeriesKeyScan::start(self.stream_ctx.clone(), self.properties.num_partitions());
            return;
        }

        info!(
            "Selected SeriesScan path, region_id: {}, series_scan_path: distributor",
            self.stream_ctx.input.mapper.metadata().region_id
        );
        let (senders, receivers) = new_channel_list(self.properties.num_partitions());
        let mut distributor = SeriesDistributor {
            stream_ctx: self.stream_ctx.clone(),
            range_semaphore: Some(Arc::new(Semaphore::new(self.properties.num_partitions()))),
            final_merge_semaphore: Some(Arc::new(Semaphore::new(self.properties.num_partitions()))),
            partitions: self.properties.partitions.clone(),
            pruner: self.pruner.clone(),
            senders,
            metrics_set: metrics_set.clone(),
            metrics_list: metrics_list.clone(),
            explain_verbose,
        };
        let region_id = distributor.stream_ctx.input.mapper.metadata().region_id;
        let span = tracing::info_span!("SeriesScan::distributor", region_id = %region_id);
        common_runtime::spawn_query(
            async move {
                distributor.execute().await;
            }
            .instrument(span),
        );

        *rx_list = receivers;
    }

    fn use_series_key_path(&self) -> bool {
        let input = &self.stream_ctx.input;
        #[cfg(feature = "enterprise")]
        if !input.extension_ranges().is_empty() {
            debug!("Series key scan is ineligible: extension ranges are present");
            return false;
        }

        if !input.enable_pk_index_scan && !input.experimental_series_key_scan {
            debug!("Series key scan is ineligible: disabled");
            return false;
        }

        let metadata = input.region_metadata();
        let tag_filters = series_key_tag_filters(input);
        if tag_filter_evaluators(metadata, &tag_filters).is_none() {
            debug!("Series key scan is ineligible: unsupported tag filter");
            return false;
        }

        !input.compaction
            && input.memtables.is_empty()
            && input.region_metadata().primary_key_encoding == PrimaryKeyEncoding::Sparse
            && input.region_metadata().primary_key.len() >= 2
    }

    /// Scans the region and returns a stream.
    #[tracing::instrument(
        skip_all,
        fields(region_id = %self.stream_ctx.input.mapper.metadata().region_id)
    )]
    pub(crate) async fn build_stream(&self) -> Result<SendableRecordBatchStream, BoxedError> {
        let part_num = self.properties.num_partitions();
        let metrics_set = ExecutionPlanMetricsSet::default();
        let streams = (0..part_num)
            .map(|i| self.scan_partition(&QueryScanContext::default(), &metrics_set, i))
            .collect::<Result<Vec<_>, BoxedError>>()?;
        let chained_stream = ChainedRecordBatchStream::new(streams).map_err(BoxedError::new)?;
        Ok(Box::pin(chained_stream))
    }

    /// Scan [`Batch`] in all partitions one by one.
    pub(crate) fn scan_all_partitions(&self) -> Result<ScanBatchStream> {
        let metrics_set = ExecutionPlanMetricsSet::new();

        let streams = (0..self.properties.partitions.len())
            .map(|partition| {
                let metrics = new_partition_metrics(
                    &self.stream_ctx,
                    false,
                    &metrics_set,
                    partition,
                    &self.metrics_list,
                );

                self.scan_batch_in_partition(
                    &QueryScanContext::default(),
                    partition,
                    metrics,
                    &metrics_set,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Box::pin(futures::stream::iter(streams).flatten()))
    }

    /// Checks resource limit for the scanner.
    pub(crate) fn check_scan_limit(&self) -> Result<()> {
        // Sum the total number of files across all partitions
        let total_files: usize = self
            .properties
            .partitions
            .iter()
            .flat_map(|partition| partition.iter())
            .map(|part_range| {
                let range_meta = &self.stream_ctx.ranges[part_range.identifier];
                range_meta.indices.len()
            })
            .sum();

        let max_concurrent_files = self.stream_ctx.input.max_concurrent_scan_files;
        if total_files > max_concurrent_files {
            return TooManyFilesToReadSnafu {
                actual: total_files,
                max: max_concurrent_files,
            }
            .fail();
        }

        Ok(())
    }
}

fn new_channel_list(num_partitions: usize) -> (SenderList, ReceiverList) {
    let (senders, receivers): (Vec<_>, Vec<_>) = (0..num_partitions)
        .map(|_| {
            let (sender, receiver) = mpsc::channel(1);
            (Some(sender), Some(PartitionReceiver::ScanBatch(receiver)))
        })
        .unzip();
    (SenderList::new(senders), receivers)
}

impl RegionScanner for SeriesScan {
    fn name(&self) -> &str {
        "SeriesScan"
    }

    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.stream_ctx.input.mapper.output_schema()
    }

    fn metadata(&self) -> RegionMetadataRef {
        self.stream_ctx.input.mapper.metadata().clone()
    }

    fn scan_partition(
        &self,
        ctx: &QueryScanContext,
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        self.scan_partition_impl(ctx, metrics_set, partition)
            .map_err(BoxedError::new)
    }

    fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
        self.properties.prepare(request);

        self.check_scan_limit().map_err(BoxedError::new)?;

        Ok(())
    }

    fn has_predicate_without_region(&self) -> bool {
        let predicate = self
            .stream_ctx
            .input
            .predicate_group()
            .predicate_without_region();
        predicate.is_some()
    }

    fn add_dyn_filter_to_predicate(
        &mut self,
        filter_exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    ) -> Vec<bool> {
        self.stream_ctx.add_dyn_filter_to_predicate(filter_exprs)
    }

    fn set_logical_region(&mut self, logical_region: bool) {
        self.properties.set_logical_region(logical_region);
    }

    fn set_query_load_region_id(&mut self, region_id: store_api::storage::RegionId) {
        self.properties.set_query_load_region_id(region_id);
    }

    fn snapshot_sequence(&self) -> Option<u64> {
        self.stream_ctx.input.snapshot_sequence
    }
}

impl DisplayAs for SeriesScan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SeriesScan: region={}, ",
            self.stream_ctx.input.mapper.metadata().region_id
        )?;
        match t {
            DisplayFormatType::Default | DisplayFormatType::TreeRender => {
                self.stream_ctx.format_for_explain(false, f)
            }
            DisplayFormatType::Verbose => {
                self.stream_ctx.format_for_explain(true, f)?;
                self.metrics_list.format_verbose_metrics(f)
            }
        }
    }
}

impl fmt::Debug for SeriesScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeriesScan")
            .field("num_ranges", &self.stream_ctx.ranges.len())
            .finish()
    }
}

#[cfg(test)]
impl SeriesScan {
    /// Returns the input.
    pub(crate) fn input(&self) -> &ScanInput {
        &self.stream_ctx.input
    }
}

/// The distributor scans series and distributes them to different partitions.
struct SeriesDistributor {
    /// Context for the scan stream.
    stream_ctx: Arc<StreamContext>,
    /// Semaphore for file scanning and range-level merging.
    range_semaphore: Option<Arc<Semaphore>>,
    /// Semaphore for the final merge across all range streams.
    /// Must be separate from `range_semaphore` to avoid deadlock: final merge tasks
    /// hold a permit while waiting for data from range-level merge tasks, which also
    /// need permits to produce data.
    final_merge_semaphore: Option<Arc<Semaphore>>,
    /// Partition ranges to scan.
    partitions: Vec<Vec<PartitionRange>>,
    /// Shared pruner for file range building.
    pruner: Arc<Pruner>,
    /// Senders of all partitions.
    senders: SenderList,
    /// Metrics set to report.
    /// The distributor report the metrics as an additional partition.
    /// This may double the scan cost of the [SeriesScan] metrics. We can
    /// get per-partition metrics in verbose mode to see the metrics of the
    /// distributor.
    metrics_set: ExecutionPlanMetricsSet,
    metrics_list: Arc<PartitionMetricsList>,
    /// Whether to use verbose logging and collect detailed metrics.
    explain_verbose: bool,
}

impl SeriesDistributor {
    /// Executes the distributor.
    #[tracing::instrument(
        skip_all,
        fields(region_id = %self.stream_ctx.input.mapper.metadata().region_id)
    )]
    async fn execute(&mut self) {
        let result = self.scan_partitions_flat().await;

        if let Err(e) = result {
            self.senders.send_error(e).await;
        }
    }

    /// Scans all parts in flat format using FlatSeriesBatchDivider.
    #[tracing::instrument(
        skip_all,
        fields(region_id = %self.stream_ctx.input.mapper.metadata().region_id)
    )]
    async fn scan_partitions_flat(&mut self) -> Result<()> {
        // Initialize reference counts for all partition ranges.
        for partition_ranges in &self.partitions {
            self.pruner.add_partition_ranges(partition_ranges);
        }

        // Create PartitionPruner covering all partitions
        let all_partition_ranges: Vec<_> = self.partitions.iter().flatten().cloned().collect();
        let partition_pruner = Arc::new(PartitionPruner::new(
            self.pruner.clone(),
            &all_partition_ranges,
        ));

        let part_metrics = new_partition_metrics(
            &self.stream_ctx,
            self.explain_verbose,
            &self.metrics_set,
            self.partitions.len(),
            &self.metrics_list,
        );
        part_metrics.on_first_poll();
        // Start fetch time before building sources so scan cost contains
        // build part cost.
        let mut fetch_start = Instant::now();

        // Builds one deduped stream per partition range, then merges across ranges.
        let build_start = Instant::now();
        let mut tasks = Vec::new();
        for partition in &self.partitions {
            for part_range in partition {
                let stream_ctx = self.stream_ctx.clone();
                let part_range = *part_range;
                let part_metrics = part_metrics.clone();
                let partition_pruner = partition_pruner.clone();
                let file_scan_semaphore = self.range_semaphore.clone();
                let merge_semaphore = self.range_semaphore.clone();
                tasks.push(common_runtime::spawn_query(async move {
                    SeqScan::build_flat_partition_range_read(
                        &stream_ctx,
                        &part_range,
                        false,
                        &part_metrics,
                        partition_pruner,
                        file_scan_semaphore,
                        merge_semaphore,
                    )
                    .await
                }));
            }
        }
        let mut range_streams = Vec::with_capacity(tasks.len());
        let mut estimated_batch_sizes = Vec::with_capacity(tasks.len());
        for task in tasks {
            let (stream, estimated_batch_size) = task.await.context(JoinSnafu)??;
            range_streams.push(stream);
            estimated_batch_sizes.push(estimated_batch_size);
        }
        let channel_size =
            compute_parallel_channel_size(compute_average_batch_size(estimated_batch_sizes));
        common_telemetry::debug!(
            "SeriesDistributor built {} range_streams, region: {}, build cost: {:?}, channel_size: {}",
            range_streams.len(),
            self.stream_ctx.input.region_metadata().region_id,
            build_start.elapsed(),
            channel_size,
        );

        // Each partition range stream is already deduped, so skip dedup here.
        // Use a separate semaphore for the final merge to avoid deadlock with
        // range-level merge tasks that share the range_semaphore.
        let mut reader = SeqScan::build_flat_reader_from_sources(
            &self.stream_ctx,
            range_streams,
            self.final_merge_semaphore.clone(),
            Some(&part_metrics),
            true,
            channel_size,
        )
        .await?;
        let mut metrics = SeriesDistributorMetrics::default();

        let mut divider = FlatSeriesBatchDivider::default();
        while let Some(record_batch) = reader.try_next().await? {
            metrics.scan_cost += fetch_start.elapsed();
            metrics.num_batches += 1;
            metrics.num_rows += record_batch.num_rows();

            debug_assert!(record_batch.num_rows() > 0);
            if record_batch.num_rows() == 0 {
                fetch_start = Instant::now();
                continue;
            }

            // Use divider to split series
            let divider_start = Instant::now();
            let series_batch = divider.push(record_batch);
            metrics.divider_cost += divider_start.elapsed();
            if let Some(series_batch) = series_batch {
                let yield_start = Instant::now();
                self.senders
                    .send_batch(ScanBatch::Series(SeriesBatch::Flat(series_batch)))
                    .await?;
                metrics.yield_cost += yield_start.elapsed();
            }
            fetch_start = Instant::now();
        }

        // Send any remaining batch in the divider
        let divider_start = Instant::now();
        let series_batch = divider.finish();
        metrics.divider_cost += divider_start.elapsed();
        if let Some(series_batch) = series_batch {
            let yield_start = Instant::now();
            self.senders
                .send_batch(ScanBatch::Series(SeriesBatch::Flat(series_batch)))
                .await?;
            metrics.yield_cost += yield_start.elapsed();
        }

        metrics.scan_cost += fetch_start.elapsed();
        metrics.num_series_send_timeout = self.senders.num_timeout;
        metrics.num_series_send_full = self.senders.num_full;
        part_metrics.set_distributor_metrics(&metrics);

        part_metrics.on_finish();

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct SeriesKey {
    table_id: u32,
    tsid: u64,
}

#[derive(Debug)]
struct SeriesKeyBatch {
    keys: Vec<SeriesKey>,
}

fn build_series_key_batches(
    keys: impl IntoIterator<Item = (u32, u64)>,
    batch_size: usize,
) -> Vec<Vec<SeriesKey>> {
    let batch_size = batch_size.max(1);
    let mut keys = keys
        .into_iter()
        .map(|(table_id, tsid)| SeriesKey { table_id, tsid })
        .collect::<Vec<_>>();
    keys.sort_unstable();

    keys.chunks(batch_size)
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn series_key_tag_filters(input: &ScanInput) -> Vec<datafusion_expr::Expr> {
    input
        .predicate_group()
        .predicate_without_region()
        .map(|predicate| tag_filter_exprs(input.region_metadata(), predicate.exprs()))
        .unwrap_or_default()
}

struct SeriesKeyScan;

impl SeriesKeyScan {
    fn start(stream_ctx: Arc<StreamContext>, num_partitions: usize) -> ReceiverList {
        let mut key_senders = Vec::with_capacity(num_partitions);
        let mut receivers = Vec::with_capacity(num_partitions);

        for _ in 0..num_partitions {
            let (key_sender, key_receiver) = mpsc::channel(1);
            key_senders.push(Some(key_sender));
            receivers.push(Some(PartitionReceiver::SeriesKey(key_receiver)));
        }

        let worker = SeriesKeyWorker {
            stream_ctx,
            key_senders,
        };
        let region_id = worker.stream_ctx.input.mapper.metadata().region_id;
        let span = tracing::info_span!("SeriesScan::series_key_worker", region_id = %region_id);
        common_runtime::spawn_query(
            async move {
                worker.execute().await;
            }
            .instrument(span),
        );

        receivers
    }
}

struct SeriesKeyWorker {
    stream_ctx: Arc<StreamContext>,
    key_senders: Vec<Option<Sender<Result<SeriesKeyBatch>>>>,
}

impl SeriesKeyWorker {
    async fn execute(mut self) {
        if let Err(error) = self.distribute_keys().await {
            let error = Arc::new(error);
            for sender in self.key_senders.iter().flatten() {
                let result = Err(error.clone()).context(ScanSeriesSnafu);
                let _ = sender.send(result).await;
            }
        }
    }

    async fn distribute_keys(&mut self) -> Result<()> {
        let resolved = resolve_series_key_tsids(&self.stream_ctx.input).await?;
        let key_batches = build_series_key_batches(
            resolved.tsids.iter().copied(),
            self.stream_ctx.input.series_key_batch_size,
        );
        info!(
            "Resolved series keys for SeriesScan, region_id: {}, use_pk_index: {}, matched_tsids: {}, pk_index_files: {}, parquet_files: {}, key_batches: {}",
            self.stream_ctx.input.region_metadata().region_id,
            resolved.use_pk_index,
            resolved.tsids.len(),
            resolved.pk_index_files,
            resolved.parquet_files,
            key_batches.len()
        );
        let num_partitions = self.key_senders.len();
        for (idx, keys) in key_batches.into_iter().enumerate() {
            self.send_key_batch(idx % num_partitions, keys).await?;
        }
        self.key_senders.clear();
        Ok(())
    }

    async fn send_key_batch(&mut self, partition: usize, keys: Vec<SeriesKey>) -> Result<()> {
        ensure!(!self.key_senders.is_empty(), InvalidSenderSnafu);

        let mut batch = SeriesKeyBatch { keys };
        loop {
            let Some(sender) = &self.key_senders[partition] else {
                return Ok(());
            };

            match sender.send_timeout(Ok(batch), SEND_TIMEOUT).await {
                Ok(()) => return Ok(()),
                Err(SendTimeoutError::Timeout(res)) => {
                    batch = res.unwrap();
                }
                Err(SendTimeoutError::Closed(res)) => {
                    self.key_senders[partition] = None;
                    batch = res.unwrap();
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct ResolvedSeriesKeys {
    tsids: HashSet<(u32, u64)>,
    use_pk_index: bool,
    pk_index_files: usize,
    parquet_files: usize,
}

#[derive(Debug, Default)]
struct SeriesKeyCollectStats {
    row_groups: usize,
    batches: usize,
    rows: usize,
    primary_keys: usize,
    build_reader_cost: Duration,
    scan_cost: Duration,
}

struct PreparedSeriesKeyFile {
    file: crate::sst::file::FileHandle,
    context: FileRangeContext,
    selection: RowGroupSelection,
    stats: SeriesKeyCollectStats,
    start: Instant,
}

enum SeriesKeyFileReader {
    Prepared(PreparedSeriesKeyFile),
    Pruned(SeriesKeyCollectStats),
}

impl SeriesKeyCollectStats {
    fn merge(&mut self, other: SeriesKeyCollectStats) {
        self.row_groups += other.row_groups;
        self.batches += other.batches;
        self.rows += other.rows;
        self.primary_keys += other.primary_keys;
        self.build_reader_cost += other.build_reader_cost;
        self.scan_cost += other.scan_cost;
    }
}

async fn resolve_series_key_tsids(input: &ScanInput) -> Result<ResolvedSeriesKeys> {
    let metadata = input.region_metadata();
    let tag_filters = series_key_tag_filters(input);
    let evaluators = tag_filter_evaluators(metadata, &tag_filters).context(UnexpectedSnafu {
        reason: "unsupported tag filters for SeriesKeyScan",
    })?;

    let mut resolved = ResolvedSeriesKeys::default();
    let mut covered_files = HashSet::new();
    let mut pk_index_cost = Duration::ZERO;
    let mut parquet_collect_stats = SeriesKeyCollectStats::default();
    let start = Instant::now();
    let time_range = input
        .time_range
        .unwrap_or_else(common_time::range::TimestampRange::min_to_max);

    if input.enable_pk_index_scan
        && !input.pk_indexes.is_empty()
        && let Some(tsid_set) = {
            let stage_start = Instant::now();
            let tsid_set = build_pk_index_tsid_set(
                input.access_layer(),
                metadata,
                &input.pk_indexes,
                &tag_filters,
                &time_range,
            )
            .await?;
            pk_index_cost += stage_start.elapsed();
            tsid_set
        }
    {
        resolved.use_pk_index = true;
        for file in &input.files {
            if tsid_set.covers(file.meta_ref().file_id) {
                covered_files.insert(file.meta_ref().file_id);
            }
        }
        resolved.pk_index_files = covered_files.len();
        resolved.tsids.extend(tsid_set.tsids().iter().copied());
    }

    let parquet_files = input
        .files
        .iter()
        .filter(|file| !covered_files.contains(&file.meta_ref().file_id))
        .cloned()
        .collect::<Vec<_>>();
    resolved.parquet_files = parquet_files.len();
    let mut prepared_files = Vec::with_capacity(parquet_files.len());
    for file in parquet_files {
        match build_file_series_key_reader(input, &file).await? {
            SeriesKeyFileReader::Prepared(prepared) => prepared_files.push(prepared),
            SeriesKeyFileReader::Pruned(stats) => parquet_collect_stats.merge(stats),
        }
    }

    let max_concurrent_files = input.max_concurrent_scan_files.max(1);
    let metadata = metadata.clone();
    let evaluators = Arc::new(evaluators);
    let file_results = futures::stream::iter(prepared_files.into_iter().map(|prepared| {
        let metadata = metadata.clone();
        let evaluators = Arc::clone(&evaluators);
        async move { collect_prepared_file_series_keys(prepared, metadata, evaluators).await }
    }))
    .buffer_unordered(max_concurrent_files)
    .try_collect::<Vec<_>>()
    .await?;

    for (stats, file_tsids) in file_results {
        parquet_collect_stats.merge(stats);
        resolved.tsids.extend(file_tsids);
    }

    info!(
        "Resolved SeriesKeyScan series keys, region_id: {}, use_pk_index: {}, matched_tsids: {}, pk_index_files: {}, parquet_files: {}, pk_index_cost: {:?}, parquet_build_reader_cost: {:?}, parquet_scan_cost: {:?}, parquet_row_groups: {}, parquet_batches: {}, parquet_rows: {}, parquet_primary_keys: {}, total_cost: {:?}",
        metadata.region_id,
        resolved.use_pk_index,
        resolved.tsids.len(),
        resolved.pk_index_files,
        resolved.parquet_files,
        pk_index_cost,
        parquet_collect_stats.build_reader_cost,
        parquet_collect_stats.scan_cost,
        parquet_collect_stats.row_groups,
        parquet_collect_stats.batches,
        parquet_collect_stats.rows,
        parquet_collect_stats.primary_keys,
        start.elapsed()
    );

    Ok(resolved)
}

async fn build_file_series_key_reader(
    input: &ScanInput,
    file: &crate::sst::file::FileHandle,
) -> Result<SeriesKeyFileReader> {
    let mut stats = SeriesKeyCollectStats::default();
    let metadata = input.region_metadata();
    let start = Instant::now();

    let mut metrics = ReaderMetrics::default();
    let build_reader_start = Instant::now();
    let predicate = input.predicate_for_sst_file(file);
    let may_build_selective_row_selection = predicate.is_some();
    let reader = input.apply_index_appliers(
        input
            .access_layer()
            .read_sst(file.clone())
            .cache(input.cache_strategy.clone())
            .predicate(predicate),
    );
    let reader = reader
        .expected_metadata(Some(metadata.clone()))
        .pre_filter_mode(PreFilterMode::All);
    let reader = if may_build_selective_row_selection {
        reader.deferred_optional_page_index()
    } else {
        reader
    };
    let Some((context, selection)) = reader.build_reader_input(&mut metrics).await? else {
        stats.build_reader_cost += build_reader_start.elapsed();
        info!(
            "Collected SeriesKeyScan series keys from parquet file, region_id: {}, file_id: {}, pruned: true, row_groups: {}, batches: {}, rows: {}, primary_keys: {}, build_reader_cost: {:?}, scan_cost: {:?}, total_cost: {:?}",
            metadata.region_id,
            file.meta_ref().file_id,
            stats.row_groups,
            stats.batches,
            stats.rows,
            stats.primary_keys,
            stats.build_reader_cost,
            stats.scan_cost,
            start.elapsed()
        );
        return Ok(SeriesKeyFileReader::Pruned(stats));
    };
    stats.build_reader_cost += build_reader_start.elapsed();

    Ok(SeriesKeyFileReader::Prepared(PreparedSeriesKeyFile {
        file: file.clone(),
        context,
        selection,
        stats,
        start,
    }))
}

async fn collect_prepared_file_series_keys(
    prepared: PreparedSeriesKeyFile,
    metadata: RegionMetadataRef,
    evaluators: Arc<Vec<common_recordbatch::filter::SimpleFilterEvaluator>>,
) -> Result<(SeriesKeyCollectStats, HashSet<(u32, u64)>)> {
    let PreparedSeriesKeyFile {
        file,
        context,
        selection,
        mut stats,
        start,
    } = prepared;

    let codec = SparsePrimaryKeyCodec::new(&metadata);
    let mut pk_filter =
        (!evaluators.is_empty()).then(|| codec.primary_key_filter(&metadata, evaluators));
    let mut tsids = HashSet::new();
    let reader_builder = context.reader_builder();
    let arrow_schema = context.read_format().arrow_schema();
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

    for (row_group_idx, row_selection) in selection.iter() {
        stats.row_groups += 1;
        let scan_start = Instant::now();
        let mut stream = reader_builder
            .build_with_projection(
                *row_group_idx,
                Some(row_selection.clone()),
                projection.clone(),
                None,
            )
            .await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            stats.batches += 1;
            stats.rows += batch.num_rows();
            stats.primary_keys +=
                collect_batch_series_keys(&codec, &mut pk_filter, batch.column(0), &mut tsids)?;
        }
        stats.scan_cost += scan_start.elapsed();
    }

    info!(
        "Collected SeriesKeyScan series keys from parquet file, region_id: {}, file_id: {}, pruned: false, row_groups: {}, batches: {}, rows: {}, primary_keys: {}, build_reader_cost: {:?}, scan_cost: {:?}, total_cost: {:?}",
        metadata.region_id,
        file.meta_ref().file_id,
        stats.row_groups,
        stats.batches,
        stats.rows,
        stats.primary_keys,
        stats.build_reader_cost,
        stats.scan_cost,
        start.elapsed()
    );

    Ok((stats, tsids))
}

fn collect_batch_series_keys(
    codec: &SparsePrimaryKeyCodec,
    pk_filter: &mut Option<Box<dyn mito_codec::row_converter::PrimaryKeyFilter>>,
    pk_col: &ArrayRef,
    tsids: &mut HashSet<(u32, u64)>,
) -> Result<usize> {
    let mut primary_keys = 0;
    if let Some(dict) = pk_col.as_any().downcast_ref::<PrimaryKeyArray>() {
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context(UnexpectedSnafu {
                reason: "primary key dictionary values are not binary",
            })?;
        let mut last_key = None;
        for key in dict.keys().values().iter().copied() {
            if last_key == Some(key) {
                continue;
            }
            last_key = Some(key);
            let pk = values.value(key as usize);
            collect_primary_key_series_key(codec, pk_filter, pk, tsids)?;
            primary_keys += 1;
        }
    } else if let Some(binary) = pk_col.as_any().downcast_ref::<BinaryArray>() {
        let mut row = 0;
        while row < binary.len() {
            let pk = binary.value(row);
            collect_primary_key_series_key(codec, pk_filter, pk, tsids)?;
            primary_keys += 1;
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

fn collect_primary_key_series_key(
    codec: &SparsePrimaryKeyCodec,
    pk_filter: &mut Option<Box<dyn mito_codec::row_converter::PrimaryKeyFilter>>,
    pk: &[u8],
    tsids: &mut HashSet<(u32, u64)>,
) -> Result<()> {
    if let Some(filter) = pk_filter.as_deref_mut()
        && !filter.matches(pk).context(DecodeSnafu)?
    {
        return Ok(());
    }
    let key = codec.read_table_id_tsid(pk).context(DecodeSnafu)?;
    tsids.insert(key);
    Ok(())
}

struct SeriesKeyPartitionReader;

impl SeriesKeyPartitionReader {
    async fn build_partition_reader(
        stream_ctx: Arc<StreamContext>,
        key_receiver: &mut Receiver<Result<SeriesKeyBatch>>,
        part_metrics: &PartitionMetrics,
    ) -> Result<BoxedRecordBatchStream> {
        let mut keys = HashSet::new();
        while let Some(batch) = key_receiver.recv().await {
            let batch = batch?;
            keys.extend(batch.keys.into_iter().map(|key| (key.table_id, key.tsid)));
        }

        if keys.is_empty() {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let covered_files = stream_ctx
            .input
            .files
            .iter()
            .map(|file| file.meta_ref().file_id)
            .collect::<HashSet<FileId>>();
        let tsid_set = PkIndexTsidSet::new(keys.clone(), covered_files);
        let input = stream_ctx
            .input
            .clone_with_pk_index_tsid_set(Some(tsid_set));
        let filtered_ctx = Arc::new(StreamContext::seq_scan_ctx(input));
        let sources = Self::build_filtered_sources(&filtered_ctx, part_metrics, &keys).await?;
        if sources.is_empty() {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let channel_size =
            compute_parallel_channel_size(crate::sst::parquet::DEFAULT_READ_BATCH_SIZE);
        SeqScan::build_flat_reader_from_sources(
            &filtered_ctx,
            sources,
            Some(Arc::new(Semaphore::new(
                filtered_ctx.input.max_concurrent_scan_files,
            ))),
            Some(part_metrics),
            false,
            channel_size,
        )
        .await
    }

    async fn build_filtered_sources(
        stream_ctx: &Arc<StreamContext>,
        part_metrics: &PartitionMetrics,
        keys: &HashSet<(u32, u64)>,
    ) -> Result<Vec<BoxedRecordBatchStream>> {
        let mut sources = Vec::new();
        let mut file_builders = HashMap::new();
        let codec = Arc::new(SparsePrimaryKeyCodec::new(
            stream_ctx.input.region_metadata(),
        ));
        let keys = Arc::new(keys.clone());

        for part_range in stream_ctx.partition_ranges() {
            let range_meta = &stream_ctx.ranges[part_range.identifier];
            let pre_filter_mode = stream_ctx.range_pre_filter_mode(&part_range);

            for index in &range_meta.row_group_indices {
                if stream_ctx.is_mem_range_index(*index) {
                    let stream = scan_flat_mem_ranges(
                        stream_ctx.clone(),
                        part_metrics.clone(),
                        *index,
                        range_meta.time_range,
                    );
                    let stream = filter_mem_stream_by_series_keys(
                        Box::pin(stream),
                        Arc::clone(&codec),
                        Arc::clone(&keys),
                    );
                    sources.push(stream);
                } else if stream_ctx.is_file_range_index(*index) {
                    let ranges = Self::build_series_file_ranges(
                        stream_ctx,
                        *index,
                        pre_filter_mode,
                        part_metrics,
                        &mut file_builders,
                    )
                    .await?;
                    if ranges.is_empty() {
                        continue;
                    }
                    part_metrics.inc_num_file_ranges(ranges.len());
                    let stream = build_flat_file_range_scan_stream(
                        stream_ctx.clone(),
                        part_metrics.clone(),
                        "series_key_scan_files",
                        ranges,
                        None,
                    );
                    sources.push(Box::pin(stream));
                }
            }
        }

        Ok(sources)
    }

    async fn build_series_file_ranges(
        stream_ctx: &Arc<StreamContext>,
        index: RowGroupIndex,
        pre_filter_mode: PreFilterMode,
        part_metrics: &PartitionMetrics,
        file_builders: &mut HashMap<(usize, bool), Arc<FileRangeBuilder>>,
    ) -> Result<SmallVec<[FileRange; 2]>> {
        let file_index = index.index - stream_ctx.input.num_memtables();
        let cache_key = (file_index, pre_filter_mode.skip_fields());
        let builder = if let Some(builder) = file_builders.get(&cache_key) {
            builder.clone()
        } else {
            let mut reader_metrics = ReaderMetrics {
                filter_metrics: new_filter_metrics(part_metrics.explain_verbose()),
                ..Default::default()
            };
            let file = &stream_ctx.input.files[file_index];
            let builder = stream_ctx
                .input
                .prune_file(file, pre_filter_mode, &mut reader_metrics)
                .await?;
            part_metrics.merge_reader_metrics(&reader_metrics, None);
            let builder = Arc::new(builder);
            file_builders.insert(cache_key, builder.clone());
            builder
        };

        let mut ranges = SmallVec::new();
        builder.build_ranges(index.row_group_index, &mut ranges);
        Ok(ranges)
    }
}

fn filter_batch_by_series_keys(
    record_batch: &RecordBatch,
    keys: &HashSet<(u32, u64)>,
    codec: &SparsePrimaryKeyCodec,
) -> Result<Option<RecordBatch>> {
    if record_batch.num_rows() == 0 {
        return Ok(None);
    }

    let pk_column_idx = primary_key_column_index(record_batch.num_columns());
    let pk_array = record_batch
        .column(pk_column_idx)
        .as_any()
        .downcast_ref::<PrimaryKeyArray>()
        .context(UnexpectedSnafu {
            reason: "primary key column is not a dictionary array",
        })?;
    let pk_values = pk_array
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context(UnexpectedSnafu {
            reason: "primary key dictionary values are not binary",
        })?;

    let mut mask = Vec::with_capacity(record_batch.num_rows());
    let mut matched = 0;
    for row in 0..record_batch.num_rows() {
        let pk = primary_key_at(pk_array, pk_values, row);
        let key = codec.read_table_id_tsid(pk).context(DecodeSnafu)?;
        let keep = keys.contains(&key);
        matched += usize::from(keep);
        mask.push(keep);
    }

    if matched == 0 {
        return Ok(None);
    }
    if matched == record_batch.num_rows() {
        return Ok(Some(record_batch.clone()));
    }

    filter_record_batch(record_batch, &BooleanArray::from(mask))
        .map(Some)
        .context(ComputeArrowSnafu)
}

fn filter_mem_stream_by_series_keys(
    mut input: BoxedRecordBatchStream,
    codec: Arc<SparsePrimaryKeyCodec>,
    keys: Arc<HashSet<(u32, u64)>>,
) -> BoxedRecordBatchStream {
    Box::pin(try_stream! {
        while let Some(record_batch) = input.try_next().await? {
            let Some(filtered) = filter_batch_by_series_keys(&record_batch, &keys, &codec)? else {
                continue;
            };
            yield filtered;
        }
    })
}

/// Batches of the same series.
#[derive(Debug)]
pub enum SeriesBatch {
    Flat(FlatSeriesBatch),
}

impl SeriesBatch {
    /// Returns the number of batches.
    pub fn num_batches(&self) -> usize {
        match self {
            SeriesBatch::Flat(flat_batch) => flat_batch.batches.len(),
        }
    }

    /// Returns the total number of rows across all batches.
    pub fn num_rows(&self) -> usize {
        match self {
            SeriesBatch::Flat(flat_batch) => flat_batch.batches.iter().map(|x| x.num_rows()).sum(),
        }
    }
}

/// Batches of the same series in flat format.
#[derive(Default, Debug)]
pub struct FlatSeriesBatch {
    pub batches: SmallVec<[RecordBatch; 4]>,
}

/// List of senders.
struct SenderList {
    senders: Vec<Option<Sender<Result<ScanBatch>>>>,
    /// Number of None senders.
    num_nones: usize,
    /// Index of the current partition to send.
    sender_idx: usize,
    /// Number of timeout.
    num_timeout: usize,
    /// Number of full senders.
    num_full: usize,
}

impl SenderList {
    fn new(senders: Vec<Option<Sender<Result<ScanBatch>>>>) -> Self {
        let num_nones = senders.iter().filter(|sender| sender.is_none()).count();
        Self {
            senders,
            num_nones,
            sender_idx: 0,
            num_timeout: 0,
            num_full: 0,
        }
    }

    /// Finds a partition and tries to send the batch to the partition.
    /// Returns None if it sends successfully.
    fn try_send_batch(&mut self, mut batch: ScanBatch) -> Result<Option<ScanBatch>> {
        for _ in 0..self.senders.len() {
            ensure!(self.num_nones < self.senders.len(), InvalidSenderSnafu);

            let sender_idx = self.fetch_add_sender_idx();
            let Some(sender) = &self.senders[sender_idx] else {
                continue;
            };

            match sender.try_send(Ok(batch)) {
                Ok(()) => return Ok(None),
                Err(TrySendError::Full(res)) => {
                    self.num_full += 1;
                    // Safety: we send Ok.
                    batch = res.unwrap();
                }
                Err(TrySendError::Closed(res)) => {
                    self.senders[sender_idx] = None;
                    self.num_nones += 1;
                    // Safety: we send Ok.
                    batch = res.unwrap();
                }
            }
        }

        Ok(Some(batch))
    }

    /// Finds a partition and sends the batch to the partition.
    async fn send_batch(&mut self, mut batch: ScanBatch) -> Result<()> {
        // Sends the batch without blocking first.
        match self.try_send_batch(batch)? {
            Some(b) => {
                // Unable to send batch to partition.
                batch = b;
            }
            None => {
                return Ok(());
            }
        }

        loop {
            ensure!(self.num_nones < self.senders.len(), InvalidSenderSnafu);

            let sender_idx = self.fetch_add_sender_idx();
            let Some(sender) = &self.senders[sender_idx] else {
                continue;
            };
            // Adds a timeout to avoid blocking indefinitely and sending
            // the batch in a round-robin fashion when some partitions
            // don't poll their inputs. This may happen if we have a
            // node like sort merging. But it is rare when we are using SeriesScan.
            match sender.send_timeout(Ok(batch), SEND_TIMEOUT).await {
                Ok(()) => break,
                Err(SendTimeoutError::Timeout(res)) => {
                    self.num_timeout += 1;
                    // Safety: we send Ok.
                    batch = res.unwrap();
                }
                Err(SendTimeoutError::Closed(res)) => {
                    self.senders[sender_idx] = None;
                    self.num_nones += 1;
                    // Safety: we send Ok.
                    batch = res.unwrap();
                }
            }
        }

        Ok(())
    }

    async fn send_error(&self, error: Error) {
        let error = Arc::new(error);
        for sender in self.senders.iter().flatten() {
            let result = Err(error.clone()).context(ScanSeriesSnafu);
            let _ = sender.send(result).await;
        }
    }

    fn fetch_add_sender_idx(&mut self) -> usize {
        let sender_idx = self.sender_idx;
        self.sender_idx = (self.sender_idx + 1) % self.senders.len();
        sender_idx
    }
}

fn new_partition_metrics(
    stream_ctx: &StreamContext,
    explain_verbose: bool,
    metrics_set: &ExecutionPlanMetricsSet,
    partition: usize,
    metrics_list: &PartitionMetricsList,
) -> PartitionMetrics {
    let metrics = PartitionMetrics::new(
        stream_ctx.input.mapper.metadata().region_id,
        partition,
        "SeriesScan",
        stream_ctx.query_start,
        explain_verbose,
        metrics_set,
    );

    metrics_list.set(partition, metrics.clone());
    metrics
}

/// A divider to split flat record batches by time series.
///
/// It only ensures rows of the same series are returned in the same [FlatSeriesBatch].
/// However, a [FlatSeriesBatch] may contain rows from multiple series.
#[derive(Default)]
struct FlatSeriesBatchDivider {
    buffer: FlatSeriesBatch,
}

impl FlatSeriesBatchDivider {
    /// Pushes a record batch into the divider.
    ///
    /// Returns a [FlatSeriesBatch] if we ensure the batch contains all rows of the series in it.
    fn push(&mut self, batch: RecordBatch) -> Option<FlatSeriesBatch> {
        // If buffer is empty
        if self.buffer.batches.is_empty() {
            self.buffer.batches.push(batch);
            return None;
        }

        // Gets the primary key column from the incoming batch.
        let pk_column_idx = primary_key_column_index(batch.num_columns());
        let batch_pk_column = batch.column(pk_column_idx);
        let batch_pk_array = batch_pk_column
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .unwrap();
        let batch_pk_values = batch_pk_array
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        // Gets the last primary key of the incoming batch.
        let batch_last_pk =
            primary_key_at(batch_pk_array, batch_pk_values, batch_pk_array.len() - 1);
        // Gets the last primary key of the buffer.
        // Safety: the buffer is not empty.
        let buffer_last_batch = self.buffer.batches.last().unwrap();
        let buffer_pk_column = buffer_last_batch.column(pk_column_idx);
        let buffer_pk_array = buffer_pk_column
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .unwrap();
        let buffer_pk_values = buffer_pk_array
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let buffer_last_pk =
            primary_key_at(buffer_pk_array, buffer_pk_values, buffer_pk_array.len() - 1);

        // If last primary key in the batch is the same as last primary key in the buffer.
        if batch_last_pk == buffer_last_pk {
            self.buffer.batches.push(batch);
            return None;
        }
        // Otherwise, the batch must have a different primary key, we find the first offset of the
        // changed primary key.
        let batch_pk_keys = batch_pk_array.keys();
        let pk_indices = batch_pk_keys.values();
        let mut change_offset = 0;
        for (i, &key) in pk_indices.iter().enumerate() {
            let batch_pk = batch_pk_values.value(key as usize);

            if buffer_last_pk != batch_pk {
                change_offset = i;
                break;
            }
        }

        // Splits the batch at the change offset
        let (first_part, remaining_part) = if change_offset > 0 {
            let first_part = batch.slice(0, change_offset);
            let remaining_part = batch.slice(change_offset, batch.num_rows() - change_offset);
            (Some(first_part), Some(remaining_part))
        } else {
            (None, Some(batch))
        };

        // Creates the result from current buffer + first part of new batch
        let mut result = std::mem::take(&mut self.buffer);
        if let Some(first_part) = first_part {
            result.batches.push(first_part);
        }

        // Pushes remaining part to the buffer if it exists
        if let Some(remaining_part) = remaining_part {
            self.buffer.batches.push(remaining_part);
        }

        Some(result)
    }

    /// Returns the final [FlatSeriesBatch].
    fn finish(&mut self) -> Option<FlatSeriesBatch> {
        if self.buffer.batches.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.buffer))
        }
    }
}

/// Helper function to extract primary key bytes at a specific index from [PrimaryKeyArray].
fn primary_key_at<'a>(
    primary_key: &PrimaryKeyArray,
    primary_key_values: &'a BinaryArray,
    index: usize,
) -> &'a [u8] {
    let key = primary_key.keys().value(index);
    primary_key_values.value(key as usize)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use api::v1::OpType;
    use datatypes::arrow::array::{
        ArrayRef, BinaryArray, BinaryDictionaryBuilder, Int64Array, StringDictionaryBuilder,
        TimestampMillisecondArray, UInt8Array, UInt64Array,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, UInt32Type};
    use datatypes::arrow::record_batch::RecordBatch;
    use mito_codec::row_converter::PrimaryKeyFilter;

    use super::*;
    use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};

    fn new_test_record_batch(
        primary_keys: &[&[u8]],
        timestamps: &[i64],
        sequences: &[u64],
        op_types: &[OpType],
        fields: &[u64],
    ) -> RecordBatch {
        let num_rows = timestamps.len();
        debug_assert_eq!(sequences.len(), num_rows);
        debug_assert_eq!(op_types.len(), num_rows);
        debug_assert_eq!(fields.len(), num_rows);
        debug_assert_eq!(primary_keys.len(), num_rows);

        let columns: Vec<ArrayRef> = vec![
            build_test_pk_string_dict_array(primary_keys),
            Arc::new(Int64Array::from_iter(
                fields.iter().map(|v| Some(*v as i64)),
            )),
            Arc::new(TimestampMillisecondArray::from_iter_values(
                timestamps.iter().copied(),
            )),
            build_test_pk_array(primary_keys),
            Arc::new(UInt64Array::from_iter_values(sequences.iter().copied())),
            Arc::new(UInt8Array::from_iter_values(
                op_types.iter().map(|v| *v as u8),
            )),
        ];

        RecordBatch::try_new(build_test_flat_schema(), columns).unwrap()
    }

    fn build_test_pk_string_dict_array(primary_keys: &[&[u8]]) -> ArrayRef {
        let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
        for &pk in primary_keys {
            let pk_str = std::str::from_utf8(pk).unwrap();
            builder.append(pk_str).unwrap();
        }
        Arc::new(builder.finish())
    }

    fn build_test_pk_array(primary_keys: &[&[u8]]) -> ArrayRef {
        let mut builder = BinaryDictionaryBuilder::<UInt32Type>::new();
        for &pk in primary_keys {
            builder.append(pk).unwrap();
        }
        Arc::new(builder.finish())
    }

    fn build_test_flat_schema() -> SchemaRef {
        let fields = vec![
            Field::new(
                "k0",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("field0", DataType::Int64, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "__primary_key",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
                false,
            ),
            Field::new("__sequence", DataType::UInt64, false),
            Field::new("__op_type", DataType::UInt8, false),
        ];
        Arc::new(Schema::new(fields))
    }

    fn series_key(table_id: u32, tsid: u64) -> SeriesKey {
        SeriesKey { table_id, tsid }
    }

    struct CountingPrimaryKeyFilter {
        count: Arc<AtomicUsize>,
        allowed: Option<HashSet<Vec<u8>>>,
    }

    impl PrimaryKeyFilter for CountingPrimaryKeyFilter {
        fn matches(&mut self, pk: &[u8]) -> mito_codec::error::Result<bool> {
            self.count.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .allowed
                .as_ref()
                .is_none_or(|allowed| allowed.contains(pk)))
        }
    }

    fn counting_filter(count: Arc<AtomicUsize>) -> Box<dyn PrimaryKeyFilter> {
        Box::new(CountingPrimaryKeyFilter {
            count,
            allowed: None,
        })
    }

    fn selective_counting_filter(
        count: Arc<AtomicUsize>,
        allowed: HashSet<Vec<u8>>,
    ) -> Box<dyn PrimaryKeyFilter> {
        Box::new(CountingPrimaryKeyFilter {
            count,
            allowed: Some(allowed),
        })
    }

    fn sparse_test_keys() -> (SparsePrimaryKeyCodec, Vec<Vec<u8>>) {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let keys = vec![
            new_sparse_primary_key(&["host-a", "db"], &metadata, 1, 10),
            new_sparse_primary_key(&["host-b", "db"], &metadata, 1, 11),
            new_sparse_primary_key(&["host-c", "db"], &metadata, 2, 10),
        ];
        (codec, keys)
    }

    #[test]
    fn test_collect_batch_series_keys_dedups_dictionary_keys() {
        let (codec, keys) = sparse_test_keys();
        let pk_col = build_test_pk_array(&[
            keys[0].as_slice(),
            keys[0].as_slice(),
            keys[1].as_slice(),
            keys[1].as_slice(),
            keys[2].as_slice(),
        ]);
        let count = Arc::new(AtomicUsize::new(0));
        let mut pk_filter = Some(counting_filter(Arc::clone(&count)));
        let mut tsids = HashSet::new();

        collect_batch_series_keys(&codec, &mut pk_filter, &pk_col, &mut tsids).unwrap();

        assert_eq!(3, count.load(Ordering::Relaxed));
        assert_eq!(HashSet::from([(1, 10), (1, 11), (2, 10)]), tsids);
    }

    #[test]
    fn test_collect_batch_series_keys_dedups_binary_runs() {
        let (codec, keys) = sparse_test_keys();
        let pk_col: ArrayRef = Arc::new(BinaryArray::from_iter_values([
            keys[0].as_slice(),
            keys[0].as_slice(),
            keys[1].as_slice(),
            keys[1].as_slice(),
            keys[2].as_slice(),
        ]));
        let count = Arc::new(AtomicUsize::new(0));
        let mut pk_filter = Some(counting_filter(Arc::clone(&count)));
        let mut tsids = HashSet::new();

        collect_batch_series_keys(&codec, &mut pk_filter, &pk_col, &mut tsids).unwrap();

        assert_eq!(3, count.load(Ordering::Relaxed));
        assert_eq!(HashSet::from([(1, 10), (1, 11), (2, 10)]), tsids);
    }

    #[test]
    fn test_collect_batch_series_keys_applies_primary_key_filter() {
        let (codec, keys) = sparse_test_keys();
        let pk_col = build_test_pk_array(&[
            keys[0].as_slice(),
            keys[0].as_slice(),
            keys[1].as_slice(),
            keys[2].as_slice(),
            keys[2].as_slice(),
        ]);
        let count = Arc::new(AtomicUsize::new(0));
        let mut pk_filter = Some(selective_counting_filter(
            Arc::clone(&count),
            HashSet::from([keys[1].clone(), keys[2].clone()]),
        ));
        let mut tsids = HashSet::new();

        collect_batch_series_keys(&codec, &mut pk_filter, &pk_col, &mut tsids).unwrap();

        assert_eq!(3, count.load(Ordering::Relaxed));
        assert_eq!(HashSet::from([(1, 11), (2, 10)]), tsids);
    }

    #[test]
    fn test_build_series_key_batches_empty() {
        let batches = build_series_key_batches(std::iter::empty::<(u32, u64)>(), 500);
        assert!(batches.is_empty());
    }

    #[test]
    fn test_build_series_key_batches_sorts_and_chunks() {
        let batches = build_series_key_batches([(2, 3), (1, 3), (1, 1), (2, 1), (1, 2), (3, 1)], 2);

        assert_eq!(
            vec![
                vec![series_key(1, 1), series_key(1, 2)],
                vec![series_key(1, 3), series_key(2, 1)],
                vec![series_key(2, 3), series_key(3, 1)],
            ],
            batches
        );
    }

    #[test]
    fn test_build_series_key_batches_batch_larger_than_keys() {
        let batches = build_series_key_batches([(2, 1), (1, 1)], 500);

        assert_eq!(vec![vec![series_key(1, 1), series_key(2, 1)]], batches);
    }

    #[test]
    fn test_build_series_key_batches_zero_batch_size() {
        let batches = build_series_key_batches([(1, 1), (1, 2), (1, 3)], 0);

        assert_eq!(
            vec![
                vec![series_key(1, 1)],
                vec![series_key(1, 2)],
                vec![series_key(1, 3)],
            ],
            batches
        );
    }

    #[test]
    fn test_build_series_key_batches_has_no_duplicates_or_missing_keys() {
        let input = [(1, 10), (1, 11), (2, 10), (2, 11), (2, 12)];
        let batches = build_series_key_batches(input, 2);

        let mut actual = batches.into_iter().flatten().collect::<Vec<_>>();
        actual.sort_unstable();

        let mut expected = input
            .into_iter()
            .map(|(table_id, tsid)| series_key(table_id, tsid))
            .collect::<Vec<_>>();
        expected.sort_unstable();

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_empty_buffer_first_push() {
        let mut divider = FlatSeriesBatchDivider::default();
        let result = divider.finish();
        assert!(result.is_none());

        let mut divider = FlatSeriesBatchDivider::default();
        let batch = new_test_record_batch(
            &[b"series1", b"series1"],
            &[1000, 2000],
            &[1, 2],
            &[OpType::Put, OpType::Put],
            &[10, 20],
        );
        let result = divider.push(batch);
        assert!(result.is_none());
        assert_eq!(divider.buffer.batches.len(), 1);
    }

    #[test]
    fn test_same_series_accumulation() {
        let mut divider = FlatSeriesBatchDivider::default();

        let batch1 = new_test_record_batch(
            &[b"series1", b"series1"],
            &[1000, 2000],
            &[1, 2],
            &[OpType::Put, OpType::Put],
            &[10, 20],
        );

        let batch2 = new_test_record_batch(
            &[b"series1", b"series1"],
            &[3000, 4000],
            &[3, 4],
            &[OpType::Put, OpType::Put],
            &[30, 40],
        );

        divider.push(batch1);
        let result = divider.push(batch2);
        assert!(result.is_none());
        let series_batch = divider.finish().unwrap();
        assert_eq!(series_batch.batches.len(), 2);
    }

    #[test]
    fn test_series_boundary_detection() {
        let mut divider = FlatSeriesBatchDivider::default();

        let batch1 = new_test_record_batch(
            &[b"series1", b"series1"],
            &[1000, 2000],
            &[1, 2],
            &[OpType::Put, OpType::Put],
            &[10, 20],
        );

        let batch2 = new_test_record_batch(
            &[b"series2", b"series2"],
            &[3000, 4000],
            &[3, 4],
            &[OpType::Put, OpType::Put],
            &[30, 40],
        );

        divider.push(batch1);
        let series_batch = divider.push(batch2).unwrap();
        assert_eq!(series_batch.batches.len(), 1);

        assert_eq!(divider.buffer.batches.len(), 1);
    }

    #[test]
    fn test_series_boundary_within_batch() {
        let mut divider = FlatSeriesBatchDivider::default();

        let batch1 = new_test_record_batch(
            &[b"series1", b"series1"],
            &[1000, 2000],
            &[1, 2],
            &[OpType::Put, OpType::Put],
            &[10, 20],
        );

        let batch2 = new_test_record_batch(
            &[b"series1", b"series2"],
            &[3000, 4000],
            &[3, 4],
            &[OpType::Put, OpType::Put],
            &[30, 40],
        );

        divider.push(batch1);
        let series_batch = divider.push(batch2).unwrap();
        assert_eq!(series_batch.batches.len(), 2);
        assert_eq!(series_batch.batches[0].num_rows(), 2);
        assert_eq!(series_batch.batches[1].num_rows(), 1);

        assert_eq!(divider.buffer.batches.len(), 1);
        assert_eq!(divider.buffer.batches[0].num_rows(), 1);
    }

    #[test]
    fn test_series_splitting() {
        let mut divider = FlatSeriesBatchDivider::default();

        let batch1 = new_test_record_batch(&[b"series1"], &[1000], &[1], &[OpType::Put], &[10]);

        let batch2 = new_test_record_batch(
            &[b"series1", b"series2", b"series2", b"series3"],
            &[2000, 3000, 4000, 5000],
            &[2, 3, 4, 5],
            &[OpType::Put, OpType::Put, OpType::Put, OpType::Put],
            &[20, 30, 40, 50],
        );

        divider.push(batch1);
        let series_batch = divider.push(batch2).unwrap();
        assert_eq!(series_batch.batches.len(), 2);

        let total_rows: usize = series_batch.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        let final_batch = divider.finish().unwrap();
        assert_eq!(final_batch.batches.len(), 1);
        assert_eq!(final_batch.batches[0].num_rows(), 3);
    }
}
