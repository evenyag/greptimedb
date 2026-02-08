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

//! Scans file row groups once and caches results, sharing cached data across
//! key ranges that need the same row group within a single SeqScan instance.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use datatypes::arrow::record_batch::RecordBatch;
use moka::sync::Cache;
use snafu::ResultExt;
use store_api::storage::FileId;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::error::Result;
use crate::memtable::PrimaryKeyRange;
use crate::read::Batch;
use crate::read::scan_util::PartitionMetrics;
use crate::sst::parquet::file_range::FileRange;
use crate::sst::parquet::row_group::ParquetFetchMetrics;

/// Cache key: identifies a single row group within a single SST file.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RowGroupKey {
    file_id: FileId,
    row_group_idx: usize,
}

impl fmt::Display for RowGroupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.file_id, self.row_group_idx)
    }
}

/// Cached row group scan result stored in the LRU cache.
#[derive(Clone)]
pub(crate) enum CachedRowGroupData {
    Batches(Arc<Vec<Batch>>),
    RecordBatches(Arc<Vec<RecordBatch>>),
}

impl CachedRowGroupData {
    /// Estimates the memory size of the cached data.
    fn memory_size(&self) -> usize {
        match self {
            CachedRowGroupData::Batches(batches) => {
                batches.iter().map(|b| b.memory_size()).sum::<usize>()
            }
            CachedRowGroupData::RecordBatches(batches) => batches
                .iter()
                .map(|b| b.get_array_memory_size())
                .sum::<usize>(),
        }
    }
}

/// LRU cache for row group scan results (moka is internally sharded/thread-safe).
type RowGroupCache = Cache<RowGroupKey, CachedRowGroupData>;

/// Map of in-flight waiters per row group key.
type WaiterMap = HashMap<RowGroupKey, Vec<oneshot::Sender<Result<CachedRowGroupData>>>>;

/// Scans file row groups once and caches results per (file_id, row_group_idx).
/// Lifetime: one per SeqScan instance, shared across partition ranges.
pub(crate) struct RowGroupScanner {
    worker_senders: Vec<mpsc::Sender<RowGroupScanRequest>>,
    inner: Arc<RowGroupScannerInner>,
}

struct RowGroupScannerInner {
    num_workers: usize,
    /// Single shared LRU cache (moka handles internal sharding).
    cache: RowGroupCache,
    /// Waiters for in-flight dedup, sharded by worker to avoid lock contention.
    waiter_shards: Vec<Mutex<WaiterMap>>,
}

struct RowGroupScanRequest {
    /// The FileRange to read (one specific row group in one file).
    file_range: FileRange,
    /// The file_id of the file containing this row group.
    file_id: FileId,
    /// The row group index within the file.
    row_group_idx: usize,
    /// Whether to use flat format.
    flat: bool,
    /// Fetch metrics for verbose mode.
    fetch_metrics: Option<Arc<ParquetFetchMetrics>>,
    /// Partition metrics for tracking cache hits/misses.
    part_metrics: PartitionMetrics,
    /// Response channel.
    response_tx: oneshot::Sender<Result<CachedRowGroupData>>,
}

/// Default cache capacity in bytes (256 MB).
const DEFAULT_CACHE_CAPACITY: u64 = 256 * 1024 * 1024;

impl RowGroupScanner {
    /// Creates a new RowGroupScanner with the given number of workers.
    pub(crate) fn new(num_workers: usize) -> Self {
        let cache: RowGroupCache = Cache::builder()
            .max_capacity(DEFAULT_CACHE_CAPACITY)
            .weigher(|_key: &RowGroupKey, value: &CachedRowGroupData| -> u32 {
                // Approximate weight in bytes, capped at u32::MAX.
                let size = value.memory_size();
                size.min(u32::MAX as usize) as u32
            })
            .build();

        let mut worker_senders = Vec::with_capacity(num_workers);
        let mut receivers = Vec::with_capacity(num_workers);
        let mut waiter_shards = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let (tx, rx) = mpsc::channel::<RowGroupScanRequest>(64);
            worker_senders.push(tx);
            receivers.push(rx);
            waiter_shards.push(Mutex::new(HashMap::new()));
        }

        let inner = Arc::new(RowGroupScannerInner {
            num_workers,
            cache,
            waiter_shards,
        });

        // Spawn worker tasks.
        for (worker_id, rx) in receivers.into_iter().enumerate() {
            let inner_clone = inner.clone();
            common_runtime::spawn_global(async move {
                Self::worker_loop(worker_id, rx, inner_clone).await;
            });
        }

        Self {
            worker_senders,
            inner,
        }
    }

    /// Requests the cached data for a row group. If not cached, the worker will
    /// read it from the file range and cache it.
    ///
    /// Returns the cached data for the row group.
    pub(crate) async fn get_or_scan(
        &self,
        file_range: &FileRange,
        file_id: FileId,
        row_group_idx: usize,
        flat: bool,
        fetch_metrics: Option<Arc<ParquetFetchMetrics>>,
        part_metrics: &PartitionMetrics,
    ) -> Result<CachedRowGroupData> {
        let key = RowGroupKey {
            file_id,
            row_group_idx,
        };

        // Fast path: check cache.
        if let Some(cached) = self.inner.cache.get(&key) {
            part_metrics.inc_rg_cache_hit(1);
            return Ok(cached);
        }

        let worker_idx = self.get_worker_idx(file_id);

        let (response_tx, response_rx) = oneshot::channel();
        let request = RowGroupScanRequest {
            file_range: file_range.clone(),
            file_id,
            row_group_idx,
            flat,
            fetch_metrics,
            part_metrics: part_metrics.clone(),
            response_tx,
        };

        if self.worker_senders[worker_idx].send(request).await.is_err() {
            // Worker channel closed, fall back to direct scan.
            part_metrics.inc_rg_cache_miss(1);
            return self
                .scan_directly(file_range, file_id, row_group_idx, flat, None)
                .await;
        }

        match response_rx.await {
            Ok(result) => result,
            Err(_) => {
                // Channel closed, fall back to direct scan.
                part_metrics.inc_rg_cache_miss(1);
                self.scan_directly(file_range, file_id, row_group_idx, flat, None)
                    .await
            }
        }
    }

    /// Directly scans a row group without going through a worker.
    async fn scan_directly(
        &self,
        file_range: &FileRange,
        file_id: FileId,
        row_group_idx: usize,
        flat: bool,
        fetch_metrics: Option<Arc<ParquetFetchMetrics>>,
    ) -> Result<CachedRowGroupData> {
        let key = RowGroupKey {
            file_id,
            row_group_idx,
        };

        let data = Self::read_row_group(file_range, flat, fetch_metrics.as_deref()).await?;

        // Cache the result.
        self.inner.cache.insert(key, data.clone());

        Ok(data)
    }

    /// Reads all data from a row group (no key_range filtering).
    async fn read_row_group(
        file_range: &FileRange,
        flat: bool,
        fetch_metrics: Option<&ParquetFetchMetrics>,
    ) -> Result<CachedRowGroupData> {
        if flat {
            let reader = file_range
                .flat_reader(PrimaryKeyRange::unbounded(), fetch_metrics)
                .await?;
            let mut batches = Vec::new();
            if let Some(mut reader) = reader {
                while let Some(batch) = reader.next_batch()? {
                    batches.push(batch);
                }
            }
            Ok(CachedRowGroupData::RecordBatches(Arc::new(batches)))
        } else {
            let reader = file_range
                .reader(None, PrimaryKeyRange::unbounded(), fetch_metrics)
                .await?;
            let mut batches = Vec::new();
            if let Some(mut reader) = reader {
                while let Some(batch) = reader.next_batch().await? {
                    batches.push(batch);
                }
            }
            Ok(CachedRowGroupData::Batches(Arc::new(batches)))
        }
    }

    fn get_worker_idx(&self, file_id: FileId) -> usize {
        let file_id_hash = Uuid::from(file_id).as_u128() as usize;
        file_id_hash % self.inner.num_workers
    }

    /// Worker loop that processes row group scan requests.
    async fn worker_loop(
        worker_id: usize,
        mut rx: mpsc::Receiver<RowGroupScanRequest>,
        inner: Arc<RowGroupScannerInner>,
    ) {
        let mut miss_counts: HashMap<RowGroupKey, usize> = HashMap::new();
        while let Some(request) = rx.recv().await {
            let RowGroupScanRequest {
                file_range,
                file_id,
                row_group_idx,
                flat,
                fetch_metrics,
                part_metrics,
                response_tx,
            } = request;

            let key = RowGroupKey {
                file_id,
                row_group_idx,
            };

            // Check cache first.
            if let Some(cached) = inner.cache.get(&key) {
                part_metrics.inc_rg_cache_hit(1);
                let _ = response_tx.send(Ok(cached));
                continue;
            }

            // Check if there's already an in-flight scan for this key.
            {
                let mut waiters = inner.waiter_shards[worker_id].lock().unwrap();
                if let Some(waiter_list) = waiters.get_mut(&key) {
                    // Another scan is in-flight, just add ourselves as a waiter.
                    waiter_list.push(response_tx);
                    continue;
                }
                // First request for this key - register ourselves and proceed to scan.
                waiters.insert(key.clone(), vec![response_tx]);
            }

            // Perform the actual scan (outside any lock).
            part_metrics.inc_rg_cache_miss(1);
            *miss_counts.entry(key.clone()).or_default() += 1;
            let result = Self::read_row_group(&file_range, flat, fetch_metrics.as_deref()).await;

            // Insert into cache and notify all waiters.
            let mut waiters = inner.waiter_shards[worker_id].lock().unwrap();
            let waiter_list = waiters.remove(&key).unwrap_or_default();

            match result {
                Ok(data) => {
                    inner.cache.insert(key, data.clone());
                    for waiter in waiter_list {
                        let _ = waiter.send(Ok(data.clone()));
                    }
                }
                Err(e) => {
                    let arc_error = Arc::new(e);
                    for waiter in waiter_list {
                        let _ = waiter
                            .send(Err(arc_error.clone()).context(crate::error::ScanRowGroupSnafu));
                    }
                }
            }
        }

        if miss_counts.is_empty() {
            common_telemetry::debug!("RowGroupScanner worker {} finished, no cache misses", worker_id);
        } else {
            common_telemetry::debug!(
                "RowGroupScanner worker {} finished, cache_misses: {:?}",
                worker_id,
                miss_counts,
            );
        }
    }
}
