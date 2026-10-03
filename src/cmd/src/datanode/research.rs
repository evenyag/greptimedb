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

//! Opt-in, bounded diagnostic traces for storage research tools.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;

use datatypes::arrow::record_batch::RecordBatch;
use serde_json::{Value, json};

use crate::error;

pub(crate) type Trace = Option<Arc<ResearchTrace>>;

/// Bounded observations drained by a dedicated writer, outside async runtimes.
pub(crate) struct ResearchTrace {
    sender: Mutex<Option<SyncSender<Value>>>,
    writer: Mutex<Option<JoinHandle<std::io::Result<()>>>>,
    dropped: AtomicU64,
    start: Instant,
    batches: Mutex<BatchStats>,
}

#[derive(Default)]
struct BatchStats {
    rows: u64,
    batches: u64,
    cumulative_logical_bytes: u64,
    max_single_batch_retained_bytes: usize,
    max_rows: usize,
}

impl ResearchTrace {
    pub(crate) fn open(path: Option<&Path>, identity: Value) -> error::Result<Trace> {
        let Some(path) = path else {
            return Ok(None);
        };
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .map_err(|e| config_error(format!("create research trace {}: {e}", path.display())))?;
        let (sender, receiver) = sync_channel::<Value>(128);
        let writer = std::thread::spawn(move || {
            let mut file = BufWriter::new(file);
            for event in receiver {
                serde_json::to_writer(&mut file, &event)?;
                file.write_all(b"\n")?;
                file.flush()?;
            }
            file.get_ref().sync_all()
        });
        let trace = Arc::new(Self {
            sender: Mutex::new(Some(sender)),
            writer: Mutex::new(Some(writer)),
            dropped: AtomicU64::new(0),
            start: Instant::now(),
            batches: Mutex::default(),
        });
        trace.observe("started", json!({"identity": identity, "complete": false,
            "format_version": 1, "classification": "local_smoke_only",
            "build": common_version::build_info().to_string(),
            "process": process_memory(),
            "environment": (["GREPTIME_MITO_READ_BATCH_SIZE", "GREPTIME_MITO_SCAN_MEMORY_DIAGNOSTICS", "MALLOC_CONF"]
                .into_iter().map(|key| (key, std::env::var(key).ok())).collect::<HashMap<_, _>>()) }));
        Ok(Some(trace))
    }

    pub(crate) fn observe(&self, phase: &str, details: Value) {
        let event = json!({"format_version": 1, "elapsed_ns": self.start.elapsed().as_nanos() as u64,
            "phase": phase, "details": details, "dropped_records": self.dropped.load(Ordering::Relaxed)});
        if let Ok(sender) = self.sender.lock()
            && let Some(sender) = sender.as_ref()
            && sender.try_send(event).is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Used only from blocking inventory tasks: unique-key records must be lossless.
    pub(crate) fn observe_required(&self, phase: &str, details: Value) -> error::Result<()> {
        let event = json!({"format_version": 1, "elapsed_ns": self.start.elapsed().as_nanos() as u64,
            "phase": phase, "details": details});
        if let Some(sender) = self
            .sender
            .lock()
            .map_err(|e| config_error(e.to_string()))?
            .as_ref()
        {
            sender
                .send(event)
                .map_err(|e| config_error(e.to_string()))?;
        }
        Ok(())
    }

    pub(crate) fn batch(&self, batch: &RecordBatch, owner: Value) {
        let retained = mito2::read::retained_batch_buffer_size(std::slice::from_ref(batch));
        let logical = mito2::memtable::record_batch_estimated_size(batch) as u64;
        let Ok(mut stats) = self.batches.lock() else {
            return;
        };
        stats.rows += batch.num_rows() as u64;
        stats.batches += 1;
        stats.cumulative_logical_bytes += logical;
        stats.max_single_batch_retained_bytes = stats.max_single_batch_retained_bytes.max(retained);
        stats.max_rows = stats.max_rows.max(batch.num_rows());
        self.observe(
            "batch",
            json!({"owner": owner, "rows": batch.num_rows(),
            "logical_bytes": logical, "retained_buffer_capacity_bytes": retained,
            "measurement_kind": "single_emitted_batch_buffers", "ordinal": stats.batches}),
        );
        if stats.batches == 1 || stats.batches.is_multiple_of(128) {
            self.observe("process_sample", process_memory());
        }
    }

    pub(crate) fn finish(&self) -> error::Result<()> {
        let stats = self
            .batches
            .lock()
            .map_err(|e| config_error(e.to_string()))?;
        let event = json!({"format_version": 1, "phase": "completed", "elapsed_ns": self.start.elapsed().as_nanos() as u64,
            "complete": true, "dropped_records": self.dropped.load(Ordering::Relaxed),
            "rows": stats.rows, "batches": stats.batches,
            "cumulative_logical_bytes": stats.cumulative_logical_bytes,
            "max_single_batch_retained_bytes": stats.max_single_batch_retained_bytes,
            "max_batch_rows": stats.max_rows, "process": process_memory()});
        if let Some(sender) = self
            .sender
            .lock()
            .map_err(|e| config_error(e.to_string()))?
            .take()
        {
            sender
                .send(event)
                .map_err(|e| config_error(e.to_string()))?;
        }
        if let Some(writer) = self
            .writer
            .lock()
            .map_err(|e| config_error(e.to_string()))?
            .take()
        {
            writer
                .join()
                .map_err(|_| config_error("research writer panicked".into()))?
                .map_err(|e| config_error(e.to_string()))?;
        }
        Ok(())
    }
}

fn config_error(msg: String) -> error::Error {
    error::IllegalConfigSnafu { msg }.build()
}

pub(crate) fn observe(trace: &Trace, phase: &str, details: Value) {
    if let Some(trace) = trace {
        trace.observe(phase, details);
    }
}

pub(crate) fn process_memory() -> Value {
    let allocator = common_mem_prof::allocator_stats().map(|stats| {
        json!({
        "allocated": stats.allocated, "active": stats.active, "resident": stats.resident,
        "mapped": stats.mapped, "retained": stats.retained })
    });
    let rss = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with("VmRSS:"))
                .and_then(|line| line.split_whitespace().nth(1)?.parse::<u64>().ok())
                .map(|kb| kb * 1024)
        });
    json!({"measurement_kind": "process_global", "allocator": allocator, "rss_bytes": rss,
        "cgroup_usage_bytes": common_stat::get_memory_usage_from_cgroups(),
        "cgroup_hard_limit_bytes": common_stat::get_memory_limit_from_cgroups(),
        "cgroup_pressure_threshold_bytes": common_stat::get_memory_high_from_cgroups(),
        "sizing_memory_bytes": common_stat::get_total_memory_bytes(),
        "observed_live_batch_buffers_current_peak": mito2::read::retained_batch_buffer_snapshot()})
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::arrow::array::Int64Array;
    use datatypes::arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn shared_slices_count_once() {
        let array = Arc::new(Int64Array::from_iter_values(0..1024));
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let size = mito2::read::retained_batch_buffer_size(std::slice::from_ref(&batch));
        assert_eq!(
            mito2::read::retained_batch_buffer_size(&[batch.slice(0, 10), batch.slice(10, 20)]),
            size
        );
    }

    #[test]
    fn incremental_output_without_completion() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace.jsonl");
        let trace = ResearchTrace::open(Some(&path), json!({"test": true}))
            .unwrap()
            .unwrap();
        trace.observe("first_batch", json!({"rows": 3}));
        // Closing the channel simulates leaving an unfinished scan. Existing
        // observations are flushed without adding a successful completion marker.
        trace.sender.lock().unwrap().take();
        trace
            .writer
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
        let contents = std::fs::read_to_string(path).unwrap();
        assert!(contents.contains("first_batch"));
        assert!(!contents.contains("completed"));
    }
}
