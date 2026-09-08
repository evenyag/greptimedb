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

//! Worker-owned background maintenance for series indexes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common_telemetry::{info, warn};
use object_store::ObjectStore;
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::metrics::SERIES_INDEX_RECONCILE_TOTAL;
use crate::region::RegionMapRef;
use crate::series_index::maintenance::reconcile_series_indexes;
use crate::series_index::purger::{IndexFilePurger, PurgeRequest, run_index_purge_task};

/// Shared lifecycle state for a worker's series-index task.
#[derive(Debug)]
pub(crate) struct SeriesIndexTaskState {
    running: AtomicBool,
    notify: Notify,
}

impl SeriesIndexTaskState {
    pub(crate) fn new() -> Self {
        Self {
            running: AtomicBool::new(true),
            notify: Notify::new(),
        }
    }

    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub(crate) fn wake(&self) {
        self.notify.notify_one();
    }

    pub(crate) fn stop(&self) {
        self.running.store(false, Ordering::Release);
        // Retain a permit if maintenance has not started waiting yet.
        self.notify.notify_one();
    }

    pub(crate) async fn notified(&self) {
        self.notify.notified().await;
    }
}

/// Starts both tasks, detaching purge and returning the maintenance handle.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_series_index_tasks(
    worker_id: u32,
    store: ObjectStore,
    regions: RegionMapRef,
    state: Arc<SeriesIndexTaskState>,
    bucket_width: Duration,
    purger: IndexFilePurger,
    purge_receiver: UnboundedReceiver<PurgeRequest>,
    interval: Duration,
) -> JoinHandle<()> {
    // Snapshots may retain senders after the worker stops; purge until all senders drop.
    common_runtime::spawn_global(run_index_purge_task(
        worker_id,
        store.clone(),
        purge_receiver,
    ));
    common_runtime::spawn_global(async move {
        SeriesIndexTask {
            worker_id,
            store,
            regions,
            bucket_width,
            purger,
            state,
            interval,
        }
        .run()
        .await;
    })
}

/// Periodic series-index maintenance for one region worker.
struct SeriesIndexTask {
    store: ObjectStore,
    regions: RegionMapRef,
    bucket_width: Duration,
    purger: IndexFilePurger,
    worker_id: u32,
    state: Arc<SeriesIndexTaskState>,
    interval: Duration,
}

impl SeriesIndexTask {
    /// Runs periodic maintenance until the worker stops.
    async fn run(mut self) {
        let worker_id = self.worker_id;
        info!("Start series-index background task, worker: {worker_id}");
        let interval = self.interval;
        let mut timer = tokio::time::interval_at(Instant::now() + interval, interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        while self.state.is_running() {
            tokio::select! {
                _ = self.state.notified() => {}
                _ = timer.tick() => {}
            }
            if self.state.is_running() {
                self.maintain().await;
            }
        }
        info!("Stop series-index background task, worker: {worker_id}");
    }

    /// Runs periodic maintenance independently of incoming deletion requests.
    async fn maintain(&mut self) {
        for region in self.regions.list_regions() {
            if !self.state.is_running() {
                break;
            }
            if let Err(error) = reconcile_series_indexes(
                self.worker_id,
                self.store.clone(),
                region.clone(),
                self.bucket_width,
                common_time::util::current_time_millis(),
                self.purger.clone(),
            )
            .await
            {
                SERIES_INDEX_RECONCILE_TOTAL
                    .with_label_values(&["failure"])
                    .inc();
                warn!(error; "Failed to reconcile series indexes, worker: {}, region: {}", self.worker_id, region.region_id);
            }
        }
    }
}
