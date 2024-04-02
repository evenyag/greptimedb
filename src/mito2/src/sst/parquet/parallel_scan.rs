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
use std::sync::{Arc, Mutex};

use common_recordbatch::DfSendableRecordBatchStream;
use common_telemetry::error;
use common_time::range::TimestampRange;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datatypes::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::error::{ArrowReaderSnafu, Result};
use crate::metrics::READ_SST_COUNT;
use crate::read::scan_region::ScanParallism;
use crate::sst::file::FileHandle;
use crate::sst::parquet::reader::ParquetPartition;

// TODO(yingwen): Read memtables.
/// Parallel row group scanner.
pub struct RowGroupScan {
    /// Region SST access layer.
    access_layer: AccessLayerRef,
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
    pub fn new(access_layer: AccessLayerRef, metadata: RegionMetadataRef) -> Self {
        Self {
            access_layer,
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
    pub async fn build_stream(&self) -> Result<DfSendableRecordBatchStream> {
        let partitions = self.build_parquet_partitions().await?;
        let partitions = Arc::new(PartitionQueue::new(partitions));
        let (sender, receiver) = mpsc::channel(self.parallelism.channel_size);
        let num_tasks = if self.parallelism.parallelism == 0 {
            1
        } else {
            self.parallelism.parallelism
        };
        for _ in 0..num_tasks {
            self.spawn_scan_task(partitions.clone(), sender.clone());
        }

        // TODO(yingwen): Error handling: id not exists, duplicate ids.
        let mut project_indices: Vec<_> = if !self.projection.is_empty() {
            self.projection
                .iter()
                .map(|column_id| self.metadata.column_index_by_id(*column_id).unwrap())
                .collect()
        } else {
            (0..self.metadata.column_metadatas.len()).collect()
        };
        project_indices.sort_unstable();
        let record_batch_schema = self
            .metadata
            .schema
            .arrow_schema()
            .project(&project_indices)
            .unwrap();
        let stream = ReceiverStream::new(receiver)
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)));
        let stream = RecordBatchStreamAdapter::new(Arc::new(record_batch_schema), stream);

        Ok(Box::pin(stream))
    }

    /// Builds and returns partitions to read.
    async fn build_parquet_partitions(&self) -> Result<VecDeque<ParquetPartition>> {
        let mut partitions = VecDeque::with_capacity(self.files.len());
        for file in &self.files {
            // TODO(yingwen); Read and prune in parallel.
            let maybe_parts = self
                .access_layer
                .read_sst(file.clone())
                .predicate(self.predicate.clone())
                .time_range(self.time_range)
                .projection(Some(self.projection.clone()))
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
