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

//! Shared admission, measurements, and ownership-based scratch cleanup.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::array::Array;
use serde::Serialize;
use snafu::ResultExt;
use tokio::sync::Notify;

use crate::error::{JoinSnafu, Result};
use crate::read::series_compact::OperationMetrics;
use crate::read::series_result::{StoreOptions, checked_add, fail};

#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Payload,
    Metadata,
    Workspace,
}

/// A lease is shared with its allocation, never with cursor position.
pub(crate) struct Charge {
    reservation: MemoryReservation,
    pool: Arc<dyn MemoryPool>,
    resources: Arc<StoreResources>,
    kind: Kind,
}

impl Charge {
    pub(crate) fn bytes(&self) -> usize {
        self.reservation.size()
    }

    pub(crate) fn grow(&mut self, bytes: usize) -> Result<()> {
        if matches!(self.kind, Kind::Metadata) {
            self.resources
                .metadata
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                    n.checked_add(bytes)
                        .filter(|n| *n <= self.resources.options.metadata_bytes)
                })
                .map_err(|_| {
                    fail(format!(
                        "metadata admission: required {bytes}, used {}, limit {}",
                        self.resources.metadata.load(Ordering::SeqCst),
                        self.resources.options.metadata_bytes
                    ))
                })?;
        }
        if let Err(error) = self.reservation.try_grow(bytes) {
            if matches!(self.kind, Kind::Metadata) {
                self.resources.metadata.fetch_sub(bytes, Ordering::SeqCst);
            }
            let limit = match self.pool.memory_limit() {
                datafusion::execution::memory_pool::MemoryLimit::Finite(limit) => limit,
                _ => self.resources.options.memory_bytes,
            };
            return Err(crate::error::BufferedScanMemorySnafu {
                stage: self.reservation.consumer().name().to_owned(),
                required: bytes,
                available: limit.saturating_sub(self.pool.reserved()),
                limit,
                reason: error.to_string(),
            }
            .build());
        }
        let (current, name) = match self.kind {
            Kind::Metadata => (&self.resources.metadata, "metadata_bytes"),
            Kind::Payload => (&self.resources.payload, "payload_bytes"),
            Kind::Workspace => (&self.resources.workspace, "workspace_bytes"),
        };
        if !matches!(self.kind, Kind::Metadata) {
            current.fetch_add(bytes, Ordering::SeqCst);
        }
        self.resources.peak(name, current.load(Ordering::SeqCst));
        self.resources
            .peak("memory_bytes", self.resources.pool.reserved());
        if matches!(self.kind, Kind::Metadata) {
            self.resources.peak(
                "metadata_bytes",
                self.resources.metadata.load(Ordering::SeqCst),
            );
        }
        Ok(())
    }

    pub(crate) fn resize(&mut self, bytes: usize) -> Result<()> {
        if bytes >= self.bytes() {
            return self.grow(bytes - self.bytes());
        }
        let released = self.bytes() - bytes;
        self.reservation
            .try_shrink(released)
            .map_err(|e| fail(e.to_string()))?;
        let current = match self.kind {
            Kind::Metadata => &self.resources.metadata,
            Kind::Payload => &self.resources.payload,
            Kind::Workspace => &self.resources.workspace,
        };
        current.fetch_sub(released, Ordering::SeqCst);
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        let bytes = self.reservation.free();
        match self.kind {
            Kind::Metadata => {
                self.resources.metadata.fetch_sub(bytes, Ordering::SeqCst);
            }
            Kind::Payload => {
                self.resources.payload.fetch_sub(bytes, Ordering::SeqCst);
            }
            Kind::Workspace => {
                self.resources.workspace.fetch_sub(bytes, Ordering::SeqCst);
            }
        }
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.clear();
    }
}

/// Measurements are bounded aggregates, not per-row/per-operation event logs.
#[derive(Debug, Serialize)]
pub struct Snapshot {
    /// Bounded control/metric state and optional fixed dictionary retained by resources.
    pub resource_bytes: usize,
    pub memory_bytes: usize,
    pub metadata_bytes: usize,
    pub payload_bytes: usize,
    pub workspace_bytes: usize,
    pub disk_bytes: usize,
    pub pending_cleanup: usize,
    pub active_operations: usize,
    pub failed_cleanup: usize,
    pub counts: BTreeMap<String, usize>,
    pub peaks: BTreeMap<String, usize>,
}

/// One budget shared by all builders, handles and cursors of a query.
pub struct StoreResources {
    pub(crate) options: StoreOptions,
    pub(crate) root: PathBuf,
    pool: Arc<dyn MemoryPool>,
    metadata: AtomicUsize,
    payload: AtomicUsize,
    workspace: AtomicUsize,
    disk: AtomicUsize,
    pending: AtomicUsize,
    active: AtomicUsize,
    live_readers: AtomicUsize,
    failed: Mutex<Vec<FailedDelete>>,
    notify: Notify,
    counts: Mutex<BTreeMap<String, usize>>,
    peaks: Mutex<BTreeMap<String, usize>>,
    pub(crate) operations: ExecutionPlanMetricsSet,
    pub(crate) conversion: OperationMetrics,
    pub(crate) lookup: OperationMetrics,
    pub(crate) serialization: OperationMetrics,
    pub(crate) initialization: OperationMetrics,
    pub(crate) decoding: OperationMetrics,
    pub(crate) reconstruction: OperationMetrics,
    cleanup: OperationMetrics,
    // Fixed dictionary is caller-owned immutable input, charged once for this budget.
    _fixed_reservation: MemoryReservation,
    #[cfg(test)]
    pub(crate) faults: Faults,
}

impl StoreResources {
    pub(crate) fn new(parent: &Path, options: StoreOptions) -> Result<Arc<Self>> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(options.memory_bytes));
        Self::with_pool(parent, options, pool)
    }

    pub(crate) fn with_pool(
        parent: &Path,
        options: StoreOptions,
        pool: Arc<dyn MemoryPool>,
    ) -> Result<Arc<Self>> {
        if options.batch_rows == 0
            || options.batch_rows > u32::MAX as usize
            || options.batch_bytes == 0
            || options.file_batches == 0
            || options.file_bytes == 0
            || options.file_metadata_bytes == 0
            || options.memory_bytes == 0
            || options.metadata_bytes == 0
            || options.disk_bytes == 0
        {
            return Err(fail(
                "all capacity limits must be positive and batch rows must fit u32",
            ));
        }
        if let Some(keys) = &options.fixed_keys
            && (keys.null_count() != 0
                || keys.iter().flatten().any(|k| k.len() != 22)
                || keys
                    .iter()
                    .flatten()
                    .zip(keys.iter().flatten().skip(1))
                    .any(|(a, b)| a >= b))
        {
            return Err(fail(
                "fixed dictionary must contain sorted unique nonnull compact keys",
            ));
        }
        let fixed = MemoryConsumer::new("SeriesResult::fixed_dictionary").register(&pool);
        let fixed_bytes = checked_add(
            16 * 1024,
            options
                .fixed_keys
                .as_ref()
                .map_or(0, |a| a.get_array_memory_size()),
        )?;
        if fixed_bytes > options.metadata_bytes {
            return Err(fail("fixed dictionary exceeds metadata budget"));
        }
        fixed
            .try_grow(fixed_bytes)
            .map_err(|e| fail(e.to_string()))?;
        let root = parent.join(format!("series-result-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root)
            .map_err(|e| fail(format!("create scratch {}: {e}", root.display())))?;
        let operations = ExecutionPlanMetricsSet::default();
        let op = |name| OperationMetrics::new(&operations, 0, name);
        Ok(Arc::new(Self {
            root,
            pool,
            metadata: AtomicUsize::new(fixed_bytes),
            payload: AtomicUsize::new(0),
            workspace: AtomicUsize::new(0),
            disk: AtomicUsize::new(0),
            pending: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            live_readers: AtomicUsize::new(0),
            failed: Mutex::new(vec![]),
            notify: Notify::new(),
            counts: Mutex::new(BTreeMap::new()),
            peaks: Mutex::new(BTreeMap::new()),
            conversion: op("normalization"),
            lookup: op("index_lookup"),
            serialization: op("serialization"),
            initialization: op("metadata_initialization"),
            decoding: op("ipc_decoding"),
            reconstruction: op("reconstruction"),
            cleanup: op("cleanup"),
            operations,
            options,
            _fixed_reservation: fixed,
            #[cfg(test)]
            faults: Faults::default(),
        }))
    }

    pub(crate) async fn blocking<T: Send + 'static>(
        self: &Arc<Self>,
        operation: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let active = ActiveOperation(self.clone());
        // Tuple field order releases abandoned results before announcing quiescence.
        let (result, active) = common_runtime::spawn_blocking_query(move || (operation(), active))
            .await
            .context(JoinSnafu)?;
        drop(active);
        result
    }

    pub(crate) fn drop_blocking<T: Send + 'static>(self: &Arc<Self>, value: T) {
        self.active.fetch_add(1, Ordering::SeqCst);
        let active = ActiveOperation(self.clone());
        common_runtime::spawn_blocking_query(move || {
            drop(value);
            drop(active);
        });
    }

    pub(crate) fn reader_lease(self: &Arc<Self>) -> ReaderLease {
        let live = self.live_readers.fetch_add(1, Ordering::SeqCst) + 1;
        self.count("readers_started", 1);
        self.peak("live_readers", live);
        ReaderLease(self.clone())
    }

    pub(crate) fn live_readers(&self) -> usize {
        self.live_readers.load(Ordering::SeqCst)
    }

    pub(crate) fn pool(&self) -> Arc<dyn MemoryPool> {
        self.pool.clone()
    }

    pub(crate) fn available(&self) -> usize {
        self.options
            .memory_bytes
            .saturating_sub(self.pool.reserved())
    }

    pub(crate) fn reserve(self: &Arc<Self>, kind: Kind, bytes: usize) -> Result<Charge> {
        self.reserve_in(kind, bytes, &self.pool)
    }

    pub(crate) fn reserve_in(
        self: &Arc<Self>,
        kind: Kind,
        bytes: usize,
        pool: &Arc<dyn MemoryPool>,
    ) -> Result<Charge> {
        let reservation = MemoryConsumer::new(match kind {
            Kind::Payload => "SeriesResult::payload",
            Kind::Metadata => "SeriesResult::metadata",
            Kind::Workspace => "SeriesResult::workspace",
        })
        .register(pool);
        let mut charge = Charge {
            reservation,
            pool: pool.clone(),
            resources: self.clone(),
            kind,
        };
        charge.grow(bytes)?;
        Ok(charge)
    }

    pub(crate) fn disk_grow(&self, bytes: usize) -> std::io::Result<()> {
        self.disk
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                n.checked_add(bytes)
                    .filter(|n| *n <= self.options.disk_bytes)
            })
            .map_err(|_| {
                std::io::Error::other(format!(
                    "series result disk quota: required {bytes}, used {}, limit {}",
                    self.disk.load(Ordering::SeqCst),
                    self.options.disk_bytes
                ))
            })?;
        self.peak("disk_bytes", self.disk.load(Ordering::SeqCst));
        Ok(())
    }

    pub(crate) fn disk_shrink(&self, bytes: usize) {
        self.disk.fetch_sub(bytes, Ordering::SeqCst);
    }

    pub(crate) fn count(&self, name: &str, n: usize) {
        let mut values = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        let value = values.entry(name.to_owned()).or_default();
        *value = value.saturating_add(n);
    }

    pub(crate) fn peak(&self, name: &str, n: usize) {
        let mut values = self.peaks.lock().unwrap_or_else(|e| e.into_inner());
        let value = values.entry(name.to_owned()).or_default();
        *value = (*value).max(n);
    }

    pub fn snapshot(&self) -> Snapshot {
        if let Some(pool) = self
            .pool
            .downcast_ref::<crate::read::series_result::budget::BudgetPool>()
        {
            self.peak("memory_bytes", pool.peak());
        }
        Snapshot {
            resource_bytes: self._fixed_reservation.size(),
            memory_bytes: self.pool.reserved(),
            metadata_bytes: self.metadata.load(Ordering::SeqCst),
            payload_bytes: self.payload.load(Ordering::SeqCst),
            workspace_bytes: self.workspace.load(Ordering::SeqCst),
            disk_bytes: self.disk.load(Ordering::SeqCst),
            pending_cleanup: self.pending.load(Ordering::SeqCst),
            active_operations: self.active.load(Ordering::SeqCst),
            failed_cleanup: self.failed.lock().unwrap_or_else(|e| e.into_inner()).len(),
            counts: self
                .counts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            peaks: self.peaks.lock().unwrap_or_else(|e| e.into_inner()).clone(),
        }
    }

    /// Waits for scheduled deletions and retries failed owned paths once. Call after
    /// dropping builders/handles/cursors; live owners intentionally keep files charged.
    pub async fn drain_cleanup(self: &Arc<Self>) -> Result<()> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.pending.load(Ordering::SeqCst) == 0 && self.active.load(Ordering::SeqCst) == 0 {
                break;
            }
            notified.await;
        }
        let resources = self.clone();
        common_runtime::spawn_blocking_query(move || {
            let failures =
                std::mem::take(&mut *resources.failed.lock().unwrap_or_else(|e| e.into_inner()));
            for entry in failures {
                resources.remove_owned(entry.paths, entry.bytes, entry.metadata);
            }
            if !resources
                .failed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
            {
                return Err(fail("owned file cleanup failed; disk charge retained"));
            }
            Ok(())
        })
        .await
        .context(JoinSnafu)?
    }

    fn remove_owned(&self, mut paths: Vec<PathBuf>, bytes: usize, metadata: Arc<Mutex<Charge>>) {
        paths.retain(|path| {
            let outcome = self.cleanup.measure(|| {
                #[cfg(test)]
                if self.faults.remove.load(Ordering::SeqCst) {
                    return Err(std::io::Error::other("injected remove failure"));
                }
                std::fs::remove_file(path)
            });
            match outcome {
                Ok(()) => false,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                Err(e) => {
                    common_telemetry::warn!(
                        "Failed to remove owned series result {}: {e}",
                        path.display()
                    );
                    true
                }
            }
        });
        if paths.is_empty() {
            self.disk_shrink(bytes);
        } else {
            self.failed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(FailedDelete {
                    paths,
                    bytes,
                    metadata,
                });
        }
    }
}

impl Drop for StoreResources {
    fn drop(&mut self) {
        let root = self.root.clone();
        // remove_dir is deliberately nonrecursive: never delete unknown artifacts.
        common_runtime::spawn_blocking_query(move || {
            if let Err(e) = std::fs::remove_dir(&root) {
                common_telemetry::warn!(
                    "Cannot remove series scratch directory {}: {e}",
                    root.display()
                );
            }
        });
    }
}

pub(crate) struct OwnedFile {
    pub(crate) metadata: Arc<Mutex<Charge>>,
    pub(crate) path: PathBuf,
    pub(crate) partial_link: Option<PathBuf>,
    pub(crate) bytes: AtomicUsize,
    pub(crate) resources: Arc<StoreResources>,
}

impl OwnedFile {
    pub(crate) fn add_bytes(&self, bytes: usize) -> std::io::Result<()> {
        let old = self.bytes.load(Ordering::SeqCst);
        let total = checked_add(old, bytes).map_err(|e| std::io::Error::other(e.to_string()))?;
        if total > self.resources.options.file_bytes {
            return Err(std::io::Error::other("IPC file byte limit"));
        }
        self.resources.disk_grow(bytes)?;
        self.bytes.store(total, Ordering::SeqCst);
        Ok(())
    }
}

impl Drop for OwnedFile {
    fn drop(&mut self) {
        let resources = self.resources.clone();
        let mut paths = vec![self.path.clone()];
        paths.extend(self.partial_link.clone());
        let bytes = self.bytes.load(Ordering::SeqCst);
        let metadata = self.metadata.clone();
        resources.pending.fetch_add(1, Ordering::SeqCst);
        common_runtime::spawn_blocking_query(move || {
            resources.remove_owned(paths, bytes, metadata);
            resources.pending.fetch_sub(1, Ordering::SeqCst);
            resources.notify.notify_waiters();
        });
    }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct Faults {
    pub(crate) write_after: Mutex<Option<usize>>,
    pub(crate) finish: std::sync::atomic::AtomicBool,
    pub(crate) read: std::sync::atomic::AtomicBool,
    pub(crate) remove: std::sync::atomic::AtomicBool,
    pub(crate) write_gate:
        Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
}

struct FailedDelete {
    paths: Vec<PathBuf>,
    bytes: usize,
    metadata: Arc<Mutex<Charge>>,
}

struct ActiveOperation(Arc<StoreResources>);
impl Drop for ActiveOperation {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.notify.notify_waiters();
    }
}

/// Declared before the reader in its scope, so decoder destruction precedes release.
pub(crate) struct ReaderLease(Arc<StoreResources>);
impl Drop for ReaderLease {
    fn drop(&mut self) {
        self.0.live_readers.fetch_sub(1, Ordering::SeqCst);
        self.0.count("readers_destroyed", 1);
    }
}
