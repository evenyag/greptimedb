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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use common_telemetry::{info, warn};
use object_store::ObjectStore;
use store_api::storage::RegionId;
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::metrics::SERIES_INDEX_RECONCILE_TOTAL;
use crate::region::{MitoRegion, RegionLeaderState, RegionMapRef, RegionRoleState};
use crate::series_index::bucket::SeriesIndexBuildState;
use crate::series_index::maintenance::reconcile_series_indexes;
use crate::series_index::purger::{IndexFilePurger, PurgeRequest, run_index_purge_task};
use crate::time_provider::TimeProviderRef;

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

/// Starts both tasks on the compaction runtime, detaching purge and returning the maintenance handle.
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
    time_provider: TimeProviderRef,
    enable_range_index: bool,
    idle_timeout: Duration,
) -> JoinHandle<()> {
    // Snapshots may retain senders after the worker stops; purge until all senders drop.
    common_runtime::spawn_compact(run_index_purge_task(
        worker_id,
        store.clone(),
        purge_receiver,
    ));
    common_runtime::spawn_compact(async move {
        SeriesIndexTask {
            worker_id,
            store,
            regions,
            bucket_width,
            purger,
            state,
            interval,
            time_provider,
            enable_range_index,
            idle_timeout,
            build_states: HashMap::new(),
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
    time_provider: TimeProviderRef,
    enable_range_index: bool,
    idle_timeout: Duration,
    build_states: HashMap<RegionId, RegionBuildState>,
}

/// Weak identity prevents an observation from outliving its region instance.
struct RegionBuildState {
    region: Weak<MitoRegion>,
    buckets: SeriesIndexBuildState,
}

impl SeriesIndexTask {
    /// Runs periodic maintenance until the worker stops.
    async fn run(mut self) {
        let worker_id = self.worker_id;
        info!("Start series-index background task, worker: {worker_id}");
        let interval = self.time_provider.wait_duration(self.interval);
        let mut timer = tokio::time::interval_at(Instant::now() + interval, interval);
        // Schedule future ticks from a late tick rather than the original cadence.
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
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
        self.maintain_with_clock(std::time::Instant::now).await;
    }

    async fn maintain_with_clock(&mut self, now: impl Fn() -> std::time::Instant) {
        self.build_states.retain(|id, state| {
            self.regions
                .get_region(*id)
                .is_some_and(|region| state.region.ptr_eq(&Arc::downgrade(&region)))
        });
        for region in self.regions.list_regions() {
            if !self.state.is_running() {
                break;
            }
            // Best effort: the region can still change state during reconciliation.
            // Local indexes can be built on followers as well as writable leaders.
            if !matches!(
                region.state(),
                RegionRoleState::Follower | RegionRoleState::Leader(RegionLeaderState::Writable)
            ) {
                self.build_states.remove(&region.region_id);
                continue;
            }
            let build_state = self
                .build_states
                .entry(region.region_id)
                .or_insert_with(|| RegionBuildState {
                    region: Arc::downgrade(&region),
                    buckets: SeriesIndexBuildState::new(self.idle_timeout),
                });
            // A reopen can race with the cleanup above and region enumeration.
            if !build_state.region.ptr_eq(&Arc::downgrade(&region)) {
                build_state.region = Arc::downgrade(&region);
                build_state.buckets.clear();
            }
            if let Err(error) = reconcile_series_indexes(
                self.worker_id,
                self.store.clone(),
                region.clone(),
                self.bucket_width,
                self.time_provider.current_time_millis(),
                self.purger.clone(),
                self.enable_range_index,
                &mut build_state.buckets,
                now(),
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

#[cfg(test)]
mod tests {
    use object_store::services::Memory;
    use store_api::region_engine::RegionEngine;

    use super::*;
    use crate::region::RegionMap;
    use crate::series_index::catalog::{range_catalog_path, series_catalog_path};
    use crate::series_index::purger::series_index_channel;
    use crate::series_index::tests::prepare_region_with_timestamps;
    use crate::test_util::{TestEnv, reopen_region};

    #[rstest::rstest]
    #[case::follower(RegionRoleState::Follower, true)]
    #[case::writable(RegionRoleState::Leader(RegionLeaderState::Writable), true)]
    #[case::staging(RegionRoleState::Leader(RegionLeaderState::Staging), false)]
    #[case::entering_staging(RegionRoleState::Leader(RegionLeaderState::EnteringStaging), false)]
    #[case::altering(RegionRoleState::Leader(RegionLeaderState::Altering), false)]
    #[case::dropping(RegionRoleState::Leader(RegionLeaderState::Dropping), false)]
    #[case::truncating(RegionRoleState::Leader(RegionLeaderState::Truncating), false)]
    #[case::editing(RegionRoleState::Leader(RegionLeaderState::Editing), false)]
    #[case::downgrading(RegionRoleState::Leader(RegionLeaderState::Downgrading), false)]
    #[tokio::test]
    async fn test_maintenance_region_states(#[case] role: RegionRoleState, #[case] builds: bool) {
        let mut env = TestEnv::with_prefix("series-maintenance-state").await;
        let (engine, mut region) = prepare_region_with_timestamps(&mut env, &[1000]).await;
        // Install the desired state without triggering the corresponding DDL.
        region.switch_state_to_staging(RegionLeaderState::Writable);
        region
            .manifest_ctx
            .exit_staging(region.region_id, role)
            .unwrap();
        let store = ObjectStore::new(Memory::default()).unwrap();
        let (purger, _receiver) = series_index_channel(store.clone());
        let regions = Arc::new(RegionMap::default());
        regions.insert_region(region.clone());
        let mut task = SeriesIndexTask {
            store: store.clone(),
            regions: regions.clone(),
            bucket_width: Duration::from_secs(100),
            purger,
            worker_id: 0,
            state: Arc::new(SeriesIndexTaskState::new()),
            interval: Duration::from_secs(3600),
            time_provider: Arc::new(crate::time_provider::StdTimeProvider),
            enable_range_index: true,
            idle_timeout: Duration::from_secs(600),
            build_states: HashMap::new(),
        };
        let start = std::time::Instant::now();
        task.maintain_with_clock(|| start).await;
        assert!(region.series_index_version().series_indexes.is_empty());
        if builds {
            // Keep the old instance alive: reopening the same ID must still reset
            // its observation rather than inheriting the elapsed idle timeout.
            let old_region = region.clone();
            reopen_region(
                &engine,
                region.region_id,
                region.table_dir().to_string(),
                role == RegionRoleState::Leader(RegionLeaderState::Writable),
                HashMap::from([
                    ("compaction.type".to_string(), "twcs".to_string()),
                    (
                        "compaction.twcs.time_window".to_string(),
                        "100s".to_string(),
                    ),
                ]),
            )
            .await;
            region = engine.get_region(region.region_id).unwrap();
            regions.insert_region(region.clone());
            task.maintain_with_clock(|| start + Duration::from_secs(600))
                .await;
            assert!(region.series_index_version().series_indexes.is_empty());
            assert!(old_region.series_index_version().series_indexes.is_empty());
        }
        task.maintain_with_clock(|| start + Duration::from_secs(1200))
            .await;
        assert_eq!(
            builds,
            !region.series_index_version().series_indexes.is_empty()
        );
        for path in [
            range_catalog_path(region.region_id),
            series_catalog_path(region.region_id),
        ] {
            assert_eq!(builds, store.exists(&path).await.unwrap());
        }
        regions.remove_region(region.region_id);
        task.maintain_with_clock(|| start + Duration::from_secs(1800))
            .await;
        assert!(task.build_states.is_empty());
        engine.stop().await.unwrap();
    }
}
