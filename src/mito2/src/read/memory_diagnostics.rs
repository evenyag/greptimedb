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

//! Debug-only observations of retained scan buffers, separate from memory budgets.
//! Footprints may overlap across owners and exclude allocator/codec overhead.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, Instant};

use datatypes::arrow::array::{Array, DictionaryArray};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt, TryStreamExt};
use prometheus::core::Collector;
use prometheus::{IntCounterVec, IntGaugeVec, register_int_counter_vec, register_int_gauge_vec};
use store_api::storage::RegionId;

static ENABLED: LazyLock<bool> = LazyLock::new(|| {
    !std::env::var("GREPTIME_MITO_SCAN_MEMORY_DIAGNOSTICS")
        .is_ok_and(|value| value.eq_ignore_ascii_case("false"))
});
tokio::task_local! {
    static PARTITION: Option<usize>;
}

pub(crate) fn current_partition() -> Option<usize> {
    PARTITION.try_with(|partition| *partition).ok().flatten()
}

fn partition_label() -> String {
    current_partition().map_or_else(|| "shared".to_string(), |p| p.to_string())
}

/// Re-establishes diagnostic context on every poll, including after task migration.
pub(crate) fn partition_stream<S: Stream + Send + 'static>(
    stream: S,
    partition: Option<usize>,
) -> futures::stream::BoxStream<'static, S::Item> {
    let mut stream = Box::pin(stream);
    futures::stream::poll_fn(move |cx| {
        PARTITION.sync_scope(partition, || stream.as_mut().poll_next(cx))
    })
    .boxed()
}

/// Observes the current batch while its producer is suspended. Downstream owners may overlap.
pub(crate) fn observe_stream(
    mut stream: crate::read::BoxedRecordBatchStream,
    phase: &'static str,
) -> crate::read::BoxedRecordBatchStream {
    Box::pin(async_stream::try_stream! {
        while let Some(batch) = stream.try_next().await? {
            let mut usage = MemoryUsage::new(phase, "suspended_output");
            usage.batch(&batch, 0);
            yield batch;
        }
    })
}

pub(crate) async fn partition_future<F: std::future::Future>(
    future: F,
    partition: Option<usize>,
) -> F::Output {
    PARTITION.scope(partition, future).await
}

pub(crate) fn partition_sync<T>(partition: Option<usize>, f: impl FnOnce() -> T) -> T {
    PARTITION.sync_scope(partition, f)
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

lazy_static::lazy_static! {
    static ref MEMORY: IntGaugeVec = register_int_gauge_vec!(
        "greptime_mito_scan_debug_memory_bytes",
        "Estimated retained scan buffer bytes; components may share allocations",
        &["phase", "component", "partition"]
    ).unwrap();
    static ref OBJECTS: IntGaugeVec = register_int_gauge_vec!(
        "greptime_mito_scan_debug_objects",
        "Live diagnostic owners, including readers suspended in merge initialization",
        &["phase", "kind", "partition"]
    ).unwrap();
    static ref ROWS: IntGaugeVec = register_int_gauge_vec!(
        "greptime_mito_scan_debug_retained_rows",
        "Rows retained by diagnostic owners; stages may overlap",
        &["phase", "partition"]
    ).unwrap();
    static ref PRODUCED: IntCounterVec = register_int_counter_vec!(
        "greptime_mito_scan_debug_produced_bytes_total",
        "Cumulative observed output or fetched bytes, not retained heap bytes",
        &["phase", "component", "partition"]
    ).unwrap();
}

#[derive(Debug, Default, Clone)]
struct Component {
    bytes: u64,
    peak_bytes: u64,
    produced_bytes: u64,
}

type ComponentKey = (&'static str, &'static str, String);

static COMPONENTS: LazyLock<Mutex<BTreeMap<ComponentKey, Component>>> =
    LazyLock::new(Mutex::default);

/// Observes one owner's memory without retaining any of its buffers.
#[derive(Debug)]
pub(crate) struct MemoryUsage {
    phase: &'static str,
    partition: String,
    kind: &'static str,
    sizes: BTreeMap<&'static str, u64>,
    enabled: bool,
    rows: u64,
}

impl MemoryUsage {
    pub(crate) fn new(phase: &'static str, kind: &'static str) -> Self {
        let enabled = *ENABLED;
        let partition = partition_label();
        if enabled {
            OBJECTS.with_label_values(&[phase, kind, &partition]).inc();
        }
        Self {
            phase,
            partition,
            kind,
            sizes: BTreeMap::new(),
            enabled,
            rows: 0,
        }
    }

    pub(crate) fn set(&mut self, component: &'static str, bytes: usize) {
        if !self.enabled {
            return;
        }
        let bytes = bytes as u64;
        let old = self.sizes.insert(component, bytes).unwrap_or(0);
        if old == bytes {
            return;
        }
        let delta = bytes as i64 - old as i64;
        let mut components = COMPONENTS.lock().unwrap_or_else(|e| e.into_inner());
        let entry = components
            .entry((self.phase, component, self.partition.clone()))
            .or_default();
        entry.bytes = entry.bytes.saturating_add(bytes).saturating_sub(old);
        entry.peak_bytes = entry.peak_bytes.max(entry.bytes);
        MEMORY
            .with_label_values(&[self.phase, component, &self.partition])
            .add(delta);
    }

    pub(crate) fn rows(&mut self, rows: usize) {
        if !self.enabled {
            return;
        }
        ROWS.with_label_values(&[self.phase, &self.partition])
            .add(rows as i64 - self.rows as i64);
        self.rows = rows as u64;
    }

    pub(crate) fn batch(&mut self, batch: &RecordBatch, tag_count: usize) {
        if !self.enabled {
            return;
        }
        self.rows(batch.num_rows());
        for (component, bytes) in batch_components(batch, tag_count) {
            self.set(component, bytes);
        }
    }
}

impl Drop for MemoryUsage {
    fn drop(&mut self) {
        if !self.enabled {
            return;
        }
        let mut components = COMPONENTS.lock().unwrap_or_else(|e| e.into_inner());
        for (component, bytes) in &self.sizes {
            if let Some(entry) =
                components.get_mut(&(self.phase, *component, self.partition.clone()))
            {
                entry.bytes = entry.bytes.saturating_sub(*bytes);
            }
            MEMORY
                .with_label_values(&[self.phase, component, &self.partition])
                .sub(*bytes as i64);
        }
        OBJECTS
            .with_label_values(&[self.phase, self.kind, &self.partition])
            .dec();
        ROWS.with_label_values(&[self.phase, &self.partition])
            .sub(self.rows as i64);
    }
}

pub(crate) fn produced(phase: &'static str, component: &'static str, bytes: usize) {
    if !*ENABLED {
        return;
    }
    let partition = partition_label();
    COMPONENTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry((phase, component, partition.clone()))
        .or_default()
        .produced_bytes += bytes as u64;
    PRODUCED
        .with_label_values(&[phase, component, &partition])
        .inc_by(bytes as u64);
}

/// Splits dictionaries into indices and values. Deduplicates allocations within a batch.
pub(crate) fn batch_components(
    batch: &RecordBatch,
    tag_count: usize,
) -> BTreeMap<&'static str, usize> {
    let mut result = BTreeMap::from([
        ("pk_values", 0),
        ("pk_indices", 0),
        ("tag_values", 0),
        ("tag_indices", 0),
        ("primitive_tags", 0),
        ("data", 0),
    ]);
    if !*ENABLED {
        return result;
    }
    let mut seen = HashSet::new();
    for (index, (field, array)) in batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .enumerate()
    {
        let pk = field.name() == "__primary_key";
        if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
            let (keys, values) = if pk {
                ("pk_indices", "pk_values")
            } else if index < tag_count {
                ("tag_indices", "tag_values")
            } else {
                ("data", "data")
            };
            *result.entry(keys).or_default() += array_bytes(dict.keys(), &mut seen);
            *result.entry(values).or_default() += array_bytes(dict.values().as_ref(), &mut seen);
        } else {
            let component = if pk {
                "pk_values"
            } else if index < tag_count {
                "primitive_tags"
            } else {
                "data"
            };
            *result.entry(component).or_default() += array_bytes(array.as_ref(), &mut seen);
        }
    }
    result
}

fn array_bytes(array: &dyn Array, seen: &mut HashSet<usize>) -> usize {
    fn visit(data: &datatypes::arrow::array::ArrayData, seen: &mut HashSet<usize>) -> usize {
        let mut size = 0;
        for buffer in data
            .buffers()
            .iter()
            .chain(data.nulls().map(|n| n.buffer()))
        {
            let base = (buffer.as_ptr() as usize).wrapping_sub(buffer.ptr_offset());
            if seen.insert(base) {
                size += buffer.capacity().max(buffer.len());
            }
        }
        for child in data.child_data() {
            size += visit(child, seen);
        }
        size
    }
    visit(&array.to_data(), seen)
}

#[derive(Default)]
struct Registry {
    scans: Vec<Weak<ScanDiagnostics>>,
    running: bool,
}
static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(Mutex::default);

/// Progress retained separately from query output, so stalled initialization is observable.
pub(crate) struct ScanDiagnostics {
    id: u64,
    region: RegionId,
    start: Instant,
    ranges_ready: AtomicU64,
    snapshot_sequence: AtomicU64,
    first_output: Mutex<HashSet<usize>>,
    stages: Mutex<BTreeMap<String, &'static str>>,
    ready_by_partition: Mutex<BTreeMap<String, u64>>,
    memory_pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
}

impl ScanDiagnostics {
    pub(crate) fn new(
        region: RegionId,
        memory_pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    ) -> Option<Arc<Self>> {
        if !*ENABLED {
            return None;
        }
        let scan = Arc::new(Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            region,
            start: Instant::now(),
            ranges_ready: AtomicU64::new(0),
            snapshot_sequence: AtomicU64::new(0),
            first_output: Mutex::new(HashSet::new()),
            stages: Mutex::new(BTreeMap::new()),
            ready_by_partition: Mutex::new(BTreeMap::new()),
            memory_pool,
        });
        scan.event("scan_created", None);
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        registry.scans.push(Arc::downgrade(&scan));
        if !registry.running {
            registry.running = true;
            common_runtime::spawn_global(report());
        }
        Some(scan)
    }

    pub(crate) fn event(&self, stage: &'static str, partition: Option<usize>) {
        let partition = partition.or_else(current_partition);
        let label = partition.map_or_else(|| "shared".to_string(), |p| p.to_string());
        self.stages
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(label, stage);
        let components = COMPONENTS.lock().unwrap_or_else(|e| e.into_inner()).clone();
        common_telemetry::info!(scan_id = self.id, region_id = %self.region,
            elapsed_ms = self.start.elapsed().as_millis() as u64, stage, partition,
            ranges_ready = self.ranges_ready.load(Ordering::Relaxed),
            pool_reserved_bytes = self.memory_pool.reserved(), process_components = ?components,
            "Scan memory progress (estimated footprints overlap)");
    }

    pub(crate) fn range_ready(&self) {
        self.ranges_ready.fetch_add(1, Ordering::Relaxed);
        *self
            .ready_by_partition
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(partition_label())
            .or_default() += 1;
    }

    pub(crate) fn first_output(&self, partition: usize) {
        let first = self
            .first_output
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(partition);
        if first {
            self.event("first_output", Some(partition));
        }
    }
}

impl Drop for ScanDiagnostics {
    fn drop(&mut self) {
        self.event("scan_context_dropped", None);
    }
}

async fn report() {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let rss = process_rss().await;
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        registry.scans.retain(|scan| scan.strong_count() > 0);
        if registry.scans.is_empty() {
            registry.running = false;
            return;
        }
        let components = COMPONENTS.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let objects = gauge_snapshot(&OBJECTS);
        let rows = gauge_snapshot(&ROWS);
        for scan in registry.scans.iter().filter_map(Weak::upgrade) {
            let snapshot_sequence = scan.snapshot_sequence.fetch_add(1, Ordering::Relaxed);
            common_telemetry::info!(scan_id = scan.id, region_id = %scan.region,
                elapsed_ms = scan.start.elapsed().as_millis() as u64,
                snapshot_sequence,
                ranges_ready = scan.ranges_ready.load(Ordering::Relaxed), rss_bytes = rss,
                partition_stages = ?*scan.stages.lock().unwrap_or_else(|e| e.into_inner()),
                ranges_ready_by_partition = ?*scan.ready_by_partition.lock().unwrap_or_else(|e| e.into_inner()),
                pool_reserved_bytes = scan.memory_pool.reserved(),
                process_components = ?components, process_objects = ?objects, process_rows = ?rows,
                "Scan memory snapshot (estimated footprints overlap; peaks are process lifetime)");
        }
    }
}

fn gauge_snapshot(gauge: &IntGaugeVec) -> Vec<(Vec<String>, u64)> {
    gauge
        .collect()
        .into_iter()
        .flat_map(|family| {
            family
                .get_metric()
                .iter()
                .map(|metric| {
                    let labels = metric
                        .get_label()
                        .iter()
                        .map(|label| format!("{}={}", label.name(), label.value()))
                        .collect();
                    (
                        labels,
                        metric.get_gauge().as_ref().map_or(0.0, |g| g.value()) as u64,
                    )
                })
                .collect::<Vec<_>>()
        })
        .filter(|(_, value)| *value > 0)
        .collect()
}

async fn process_rss() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = tokio::fs::read_to_string("/proc/self/status").await.ok()?;
        let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
        return line
            .split_whitespace()
            .nth(1)?
            .parse::<u64>()
            .ok()?
            .checked_mul(1024);
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_owner_releases_memory() {
        if !*ENABLED {
            return;
        }
        let gauge = MEMORY.with_label_values(&["test", "buffer", "shared"]);
        let before = gauge.get();
        {
            let mut usage = MemoryUsage::new("test", "owner");
            usage.set("buffer", 100);
            usage.set("buffer", 40);
            assert_eq!(before + 40, gauge.get());
        }
        assert_eq!(before, gauge.get());
        let partition_zero = partition_sync(Some(0), || {
            let mut usage = MemoryUsage::new("test", "owner");
            usage.set("partition_buffer", 17);
            usage
        });
        let partition_one = partition_sync(Some(1), || {
            let mut usage = MemoryUsage::new("test", "owner");
            usage.set("partition_buffer", 31);
            usage
        });
        assert_eq!(
            17,
            MEMORY
                .with_label_values(&["test", "partition_buffer", "0"])
                .get()
        );
        assert_eq!(
            31,
            MEMORY
                .with_label_values(&["test", "partition_buffer", "1"])
                .get()
        );
        drop((partition_zero, partition_one));
        assert_eq!(
            0,
            MEMORY
                .with_label_values(&["test", "partition_buffer", "0"])
                .get()
        );
        assert_eq!(
            0,
            MEMORY
                .with_label_values(&["test", "partition_buffer", "1"])
                .get()
        );

        // The scanner has produced nothing; snapshots must not depend on output progress.
        let scan = ScanDiagnostics::new(
            RegionId::new(1, 2),
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default()),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while scan.snapshot_sequence.load(Ordering::Relaxed) == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(scan.first_output.lock().unwrap().is_empty());
    }
}
