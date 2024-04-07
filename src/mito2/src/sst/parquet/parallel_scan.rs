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

//! Scans row groups in parallel.

use std::collections::VecDeque;
use std::fs::{self, File};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use api::v1::SemanticType;
use async_stream::stream;
use common_recordbatch::DfSendableRecordBatchStream;
use common_telemetry::error;
use common_time::range::TimestampRange;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datatypes::arrow;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use futures::future::try_join_all;
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use snafu::ResultExt;
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::storage::consts::is_internal_column;
use store_api::storage::{ColumnId, RegionId};
use table::predicate::Predicate;
use tokio::sync::mpsc;

use crate::cache::CacheManagerRef;
use crate::error::{ArrowReaderSnafu, Error, Result};
use crate::metrics::READ_SST_COUNT;
use crate::read::scan_region::ScanParallism;
use crate::sst::file::{FileHandle, FileId};
use crate::sst::parquet::format::AppendReadFormat;
use crate::sst::parquet::reader::{ParquetPartition, ParquetReaderBuilder};

const DEFAULT_CHANNEL_SIZE: usize = 32;

// TODO(yingwen): Read memtables.
/// Parallel row group scanner.
pub struct RowGroupScan {
    /// Directory of the file.
    file_dir: String,
    /// Object store.
    object_store: ObjectStore,
    /// Latest region metadata.
    metadata: RegionMetadataRef,
    /// Column ids to read.
    projection: Vec<ColumnId>,
    /// Time range filter for time index.
    time_range: Option<TimestampRange>,
    /// Predicate to push down.
    predicate: Option<Predicate>,
    /// Handles to SST files to scan.
    files: Vec<FileHandle>,
    /// Cache.
    cache_manager: Option<CacheManagerRef>,
    /// Ignores file not found error.
    ignore_file_not_found: bool,
    /// Parallelism to scan data.
    parallelism: ScanParallism,
}

impl RowGroupScan {
    /// Creates a new row group scan.
    pub fn new(file_dir: String, object_store: ObjectStore, metadata: RegionMetadataRef) -> Self {
        Self {
            file_dir,
            object_store,
            metadata,
            projection: vec![],
            time_range: None,
            predicate: None,
            files: vec![],
            cache_manager: None,
            ignore_file_not_found: false,
            parallelism: ScanParallism::default(),
        }
    }

    /// Attaches the projection.
    pub fn with_projection(mut self, projection: Vec<ColumnId>) -> Self {
        self.projection = projection;
        self
    }

    /// Attaches the time range filter.
    pub fn with_time_range(mut self, range: Option<TimestampRange>) -> Self {
        self.time_range = range;
        self
    }

    /// Attaches the predicate.
    pub fn with_predicate(mut self, predicate: Option<Predicate>) -> Self {
        self.predicate = predicate;
        self
    }

    /// Attaches files to scan.
    pub fn with_files(mut self, files: Vec<FileHandle>) -> Self {
        self.files = files;
        self
    }

    /// Attaches the cache.
    pub fn with_cache(mut self, cache: Option<CacheManagerRef>) -> Self {
        self.cache_manager = cache;
        self
    }

    /// Ignores file not found error.
    pub fn with_ignore_file_not_found(mut self, ignore: bool) -> Self {
        self.ignore_file_not_found = ignore;
        self
    }

    /// Attaches scan parallelism.
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism.parallelism = parallelism;
        self
    }

    /// Attaches scan channel size.
    pub fn with_parallelism_channel_size(mut self, channel_size: usize) -> Self {
        self.parallelism.channel_size = channel_size;
        self
    }

    // For simplicity and performance, we use datafusion's stream.
    /// Builds a stream for the query.
    pub async fn build_streams(&self) -> Result<Vec<DfSendableRecordBatchStream>> {
        let num_tasks = if self.parallelism.parallelism == 0 {
            1
        } else {
            self.parallelism.parallelism
        };
        // TODO(yingwen): For test we create a read format here.
        let read_format = if self.projection.is_empty() {
            AppendReadFormat::new(self.metadata.clone(), None)
        } else {
            AppendReadFormat::new(self.metadata.clone(), Some(&self.projection))
        };
        let partitions = self.build_parquet_partitions().await?;
        let partitions = Arc::new(PartitionQueue::new(partitions));
        let record_batch_schema = if !self.projection.is_empty() {
            let project_indices = read_format.projection_indices();
            Arc::new(
                read_format
                    .sst_arrow_schema()
                    .project(&project_indices)
                    .unwrap(),
            )
        } else {
            read_format.sst_arrow_schema().clone()
        };
        if num_tasks > 0 {
            let streams: Vec<_> = (0..num_tasks)
                .map(|_| {
                    self.build_single_thread_stream(partitions.clone(), record_batch_schema.clone())
                })
                .collect();
            Ok(streams)
        } else {
            let stream = self.build_single_thread_stream(partitions, record_batch_schema);
            Ok(vec![stream])
        }
    }

    /// Builds a stream to read in single thread.
    fn build_single_thread_stream(
        &self,
        partitions: Arc<PartitionQueue>,
        record_batch_schema: arrow::datatypes::SchemaRef,
    ) -> DfSendableRecordBatchStream {
        let stream = stream! {
            while let Some(partition) = partitions.pop() {
                let reader = partition.reader().await?;
                for batch in reader {
                    let batch = batch.context(ArrowReaderSnafu {
                        path: partition.file_path(),
                    })?;

                    yield Ok(batch);
                }
            }
        };

        let stream =
            stream.map_err(|e: Error| datafusion_common::DataFusionError::External(Box::new(e)));
        let stream = RecordBatchStreamAdapter::new(record_batch_schema, stream);
        Box::pin(stream)
    }

    /// Builds and returns partitions to read.
    async fn build_parquet_partitions(&self) -> Result<VecDeque<ParquetPartition>> {
        let projection = if self.projection.is_empty() {
            None
        } else {
            Some(self.projection.clone())
        };
        let mut partitions = VecDeque::with_capacity(self.files.len());
        for file in &self.files {
            // TODO(yingwen); Read and prune in parallel.

            let maybe_parts = ParquetReaderBuilder::new(
                self.file_dir.clone(),
                file.clone(),
                self.object_store.clone(),
            )
            .predicate(self.predicate.clone())
            .time_range(self.time_range)
            .projection(projection.clone())
            .cache(self.cache_manager.clone())
            // TODO(yingwen): Index applier.
            .latest_metadata(Some(self.metadata.clone()))
            .build_partitions()
            .await;
            let file_parts = match maybe_parts {
                Ok(file_parts) => file_parts,
                Err(e) => {
                    if e.is_object_not_found() && self.ignore_file_not_found {
                        error!(e; "File to scan does not exist, region_id: {}, file: {}", file.region_id(), file.file_id());
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };
            // TODO(yingwen): Compat schema.

            partitions.extend(file_parts.into_iter());
        }

        READ_SST_COUNT.observe(self.files.len() as f64);

        Ok(partitions)
    }

    #[allow(dead_code)]
    fn spawn_scan_task(
        &self,
        partitions: Arc<PartitionQueue>,
        sender: mpsc::Sender<Result<RecordBatch>>,
    ) {
        tokio::spawn(async move {
            if let Err(e) = Self::scan_partition(partitions, &sender).await {
                let _ = sender.send(Err(e)).await;
            }
        });
    }

    async fn scan_partition(
        partitions: Arc<PartitionQueue>,
        sender: &mpsc::Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        while let Some(partition) = partitions.pop() {
            let reader = partition.reader().await?;
            for batch in reader {
                let batch = batch.context(ArrowReaderSnafu {
                    path: partition.file_path(),
                })?;

                if sender.send(Ok(batch)).await.is_err() {
                    return Ok(());
                }
            }
        }

        Ok(())
    }
}

struct PartitionQueue {
    partitions: Mutex<VecDeque<ParquetPartition>>,
}

impl PartitionQueue {
    fn new(partitions: VecDeque<ParquetPartition>) -> Self {
        Self {
            partitions: Mutex::new(partitions),
        }
    }

    fn pop(&self) -> Option<ParquetPartition> {
        self.partitions.lock().unwrap().pop_front()
    }
}

// ---------------- Functions for benchmark. -----------------------------

/// Metrics for scanning the file.
#[derive(Debug, Default)]
pub struct ScanMetrics {
    /// Scan cost.
    pub scan_cost: Duration,
    /// Number of batches.
    pub num_batches: usize,
    /// Number of rows.
    pub num_rows: usize,
    /// Number of columns.
    pub num_columns: usize,
}

/// Infers the metadata of the region from a file.
pub(crate) fn infer_region_metadata(file: File, region_id: RegionId) -> RegionMetadataRef {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = reader.schema();

    let mut builder = RegionMetadataBuilder::new(region_id);
    let mut column_id = 0;
    let mut primary_key = Vec::new();
    for field in schema.fields() {
        if is_internal_column(field.name()) {
            continue;
        }

        let semantic_type = infer_semantic_type(field);
        if semantic_type == SemanticType::Tag {
            primary_key.push(column_id);
        }
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                field.name(),
                infer_data_type(field),
                field.is_nullable(),
            ),
            semantic_type,
            column_id,
        });
        column_id += 1;
    }
    builder.primary_key(primary_key);

    let metadata = builder.build().unwrap();
    Arc::new(metadata)
}

fn infer_file_size(file: &File) -> u64 {
    let meta = file.metadata().unwrap();
    meta.len()
}

fn infer_data_type(field: &Field) -> ConcreteDataType {
    ConcreteDataType::try_from(field.data_type()).unwrap()
}

fn infer_semantic_type(field: &Field) -> SemanticType {
    if matches!(field.data_type(), ArrowDataType::Timestamp(_, _)) {
        return SemanticType::Timestamp;
    }

    if matches!(
        field.data_type(),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8
    ) {
        return SemanticType::Tag;
    }

    SemanticType::Field
}

/// Creates a mock file handle to converting files.
fn new_file_handle(region_id: RegionId, file_size: u64) -> FileHandle {
    use common_time::Timestamp;

    use crate::sst::file::FileMeta;
    use crate::sst::file_purger::{FilePurger, PurgeRequest};

    #[derive(Debug)]
    struct NoopFilePurger;

    impl FilePurger for NoopFilePurger {
        fn send_request(&self, _request: PurgeRequest) {}
    }

    let file_purger = Arc::new(NoopFilePurger {});
    let file_id = FileId::random();

    FileHandle::new(
        FileMeta {
            region_id,
            file_id,
            time_range: (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(3600000),
            ),
            level: 0,
            file_size,
            available_indexes: Default::default(),
            index_file_size: 0,
        },
        file_purger,
    )
}

/// Scans the file in parallel.
pub async fn parallel_scan_file(
    file_path: &str,
    object_store: &ObjectStore,
    parallelism: usize,
    channel_size: Option<usize>,
) -> Result<ScanMetrics> {
    // Infer metadata and file size.
    let file = File::open(file_path).unwrap();
    let region_id = RegionId::new(1, 1);
    let file_size = infer_file_size(&file);
    let metadata = infer_region_metadata(file, region_id);

    let now = Instant::now();
    let file_handle = new_file_handle(region_id, file_size);
    let scan = RowGroupScan::new(file_path.to_string(), object_store.clone(), metadata)
        .with_files(vec![file_handle])
        .with_parallelism(parallelism)
        .with_parallelism_channel_size(channel_size.unwrap_or(DEFAULT_CHANNEL_SIZE));
    let streams = scan.build_streams().await?;
    let final_metrics = scan_streams(streams, now).await;

    Ok(final_metrics)
}

/// Scans directory in parallel.
pub async fn parallel_scan_dir(
    file_dir: &str,
    object_store: &ObjectStore,
    parallelism: usize,
) -> Result<ScanMetrics> {
    let (metadata, file_handles) =
        create_file_handles_and_infer_metadata(file_dir, RegionId::new(1, 1));
    let Some(metadata) = metadata else {
        return Ok(ScanMetrics::default());
    };

    let now = Instant::now();
    let scan = RowGroupScan::new(file_dir.to_string(), object_store.clone(), metadata)
        .with_files(file_handles)
        .with_parallelism(parallelism);
    let streams = scan.build_streams().await?;
    let final_metrics = scan_streams(streams, now).await;

    Ok(final_metrics)
}

async fn scan_streams(streams: Vec<DfSendableRecordBatchStream>, now: Instant) -> ScanMetrics {
    let mut futures = Vec::with_capacity(streams.len());
    for mut stream in streams {
        let future = tokio::spawn(async move {
            let mut metrics = ScanMetrics::default();
            while let Some(batch) = stream.try_next().await.unwrap() {
                metrics.num_batches += 1;
                metrics.num_rows += batch.num_rows();
                metrics.num_columns = batch.num_columns();
            }

            metrics
        });
        futures.push(future);
    }
    let mut final_metrics = ScanMetrics::default();
    let task_metrics = try_join_all(futures).await.unwrap();
    for metrics in task_metrics {
        final_metrics.num_batches += metrics.num_batches;
        final_metrics.num_rows += metrics.num_rows;
        final_metrics.num_columns = metrics.num_columns;
    }
    final_metrics.scan_cost = now.elapsed();

    final_metrics
}

/// Iterates files under the directory, infer the metadata of the first file
/// and files sizes for all files. Creates file handles for all files based
/// on their file sizes.
/// Returns the file handles and the inferred metadata.
pub fn create_file_handles_and_infer_metadata(
    file_dir: &str,
    region_id: RegionId,
) -> (Option<RegionMetadataRef>, Vec<FileHandle>) {
    // Gets file metadata and file sizes from the file directory.
    let file_paths = fs::read_dir(file_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();

    let mut file_handles = Vec::with_capacity(file_paths.len());
    let mut metadata = None;
    for file_path in file_paths {
        let file = File::open(file_path).unwrap();
        let file_size = infer_file_size(&file);
        let file_handle = new_file_handle(region_id, file_size);
        file_handles.push(file_handle);

        if metadata.is_none() {
            metadata = Some(infer_region_metadata(file, region_id));
        }
    }

    (metadata, file_handles)
}
