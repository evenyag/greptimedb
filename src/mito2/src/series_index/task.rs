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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use common_telemetry::{info, warn};
use snafu::ensure;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::error::{InvalidRequestSnafu, RegionBusySnafu, Result, WorkerStoppedSnafu};
use crate::metrics::{
    SERIES_INDEX_CAPACITY_DEFERRED, SERIES_INDEX_DISK_BYTES, SERIES_INDEX_RECONCILE_TOTAL,
};
use crate::read::series_candidate::is_sparse_metric_metadata;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionMapRef, RegionRoleState};
use crate::time_provider::TimeProviderRef;

/// Commands serialized with periodic series-index maintenance.
#[derive(Debug)]
pub(crate) enum SeriesIndexCommand {
    Wake,
    Reconcile {
        region: MitoRegionRef,
        sender: oneshot::Sender<Result<bool>>,
    },
    Stop,
}

/// Shared lifecycle state and command sender for a worker's series-index task.
#[derive(Debug)]
pub(crate) struct SeriesIndexTaskState {
    worker_id: u32,
    running: AtomicBool,
    sender: Sender<SeriesIndexCommand>,
    pub(crate) delta: bool,
    pub(crate) worker: Option<Sender<crate::request::WorkerRequestWithTime>>,
}

impl SeriesIndexTaskState {
    pub(crate) fn new(worker_id: u32, channel_size: usize) -> (Self, Receiver<SeriesIndexCommand>) {
        let (sender, receiver) = mpsc::channel(channel_size);
        (
            Self {
                worker_id,
                running: AtomicBool::new(true),
                sender,
                delta: false,
                worker: None,
            },
            receiver,
        )
    }

    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub(crate) fn wake(&self) {
        // Queued work already wakes the task. Periodic maintenance still runs even
        // if the queued command only reconciles one region.
        if self.is_running() && self.sender.capacity() == self.sender.max_capacity() {
            let _ = self.sender.try_send(SeriesIndexCommand::Wake);
        }
    }

    /// Returns a receiver for build completion, or RegionBusy if the queue is full.
    pub(crate) fn try_reconcile(
        &self,
        region: MitoRegionRef,
    ) -> Result<oneshot::Receiver<Result<bool>>> {
        ensure!(self.is_running(), WorkerStoppedSnafu { id: self.worker_id });
        let region_id = region.region_id;
        let (sender, receiver) = oneshot::channel();
        self.sender
            .try_send(SeriesIndexCommand::Reconcile { region, sender })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => RegionBusySnafu { region_id }.build(),
                mpsc::error::TrySendError::Closed(_) => {
                    WorkerStoppedSnafu { id: self.worker_id }.build()
                }
            })?;
        Ok(receiver)
    }

    pub(crate) fn stop(&self) {
        if self.running.swap(false, Ordering::AcqRel) {
            // A full queue already wakes the task, which checks the running flag.
            let _ = self.sender.try_send(SeriesIndexCommand::Stop);
        }
    }
}

/// Runs native index maintenance on the compaction runtime.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_series_index_tasks(
    worker_id: u32,
    regions: RegionMapRef,
    state: Arc<SeriesIndexTaskState>,
    receiver: Receiver<SeriesIndexCommand>,
    bucket_width: Duration,
    disk_usage: Arc<AtomicU64>,
    max_size: u64,
    interval: Duration,
    time_provider: TimeProviderRef,
) -> JoinHandle<()> {
    common_runtime::spawn_compact(async move {
        SeriesIndexTask {
            worker_id,
            regions,
            bucket_width,
            disk_usage,
            max_size,
            reported_usage: 0,
            state,
            receiver,
            interval,
            time_provider,
        }
        .run()
        .await;
    })
}

/// Periodic series-index maintenance for one region worker.
struct SeriesIndexTask {
    regions: RegionMapRef,
    bucket_width: Duration,
    disk_usage: Arc<AtomicU64>,
    max_size: u64,
    reported_usage: u64,
    worker_id: u32,
    state: Arc<SeriesIndexTaskState>,
    receiver: Receiver<SeriesIndexCommand>,
    interval: Duration,
    time_provider: TimeProviderRef,
}

impl SeriesIndexTask {
    /// Runs periodic maintenance until the worker stops.
    async fn run(mut self) {
        let worker_id = self.worker_id;
        info!("Start series-index background task, worker: {worker_id}");
        let interval = self.time_provider.wait_duration(self.interval);
        let mut deadline = Instant::now() + interval;
        while self.state.is_running() {
            // Check before receiving so a busy channel cannot starve periodic maintenance.
            if Instant::now() >= deadline {
                self.maintain().await;
                deadline = Instant::now() + interval;
                continue;
            }
            match tokio::time::timeout_at(deadline, self.receiver.recv()).await {
                Err(_) => continue,
                Ok(None | Some(SeriesIndexCommand::Stop)) => break,
                Ok(Some(SeriesIndexCommand::Wake)) => {
                    if self.state.is_running() {
                        self.maintain().await;
                        deadline = Instant::now() + interval;
                    }
                }
                Ok(Some(SeriesIndexCommand::Reconcile { region, sender })) => {
                    let result = self.reconcile_manual(region).await;
                    let _ = sender.send(result);
                }
            }
        }
        self.state.stop();
        self.receiver.close();
        while let Some(command) = self.receiver.recv().await {
            if let SeriesIndexCommand::Reconcile { sender, .. } = command {
                let _ = sender.send(WorkerStoppedSnafu { id: worker_id }.fail());
            }
        }
        info!("Stop series-index background task, worker: {worker_id}");
    }

    /// A manual request must still refer to the same writable region instance.
    fn validate_manual_region(&self, region: &MitoRegionRef) -> Result<()> {
        ensure!(
            self.state.is_running(),
            WorkerStoppedSnafu { id: self.worker_id }
        );
        let current = self.regions.writable_non_staging_region(region.region_id)?;
        ensure!(
            Arc::ptr_eq(&current, region),
            InvalidRequestSnafu {
                region_id: region.region_id,
                reason: "region was replaced during series-index reconciliation",
            }
        );
        ensure!(
            is_sparse_metric_metadata(&region.version().metadata),
            InvalidRequestSnafu {
                region_id: region.region_id,
                reason: "series indexes require sparse metric metadata",
            }
        );
        Ok(())
    }

    async fn reconcile_manual(&mut self, region: MitoRegionRef) -> Result<bool> {
        self.validate_manual_region(&region)?;
        self.refresh_usage();
        let mut changed = false;
        while self
            .reconcile_region_with_mode(region.clone(), true)
            .await?
        {
            changed = true;
            self.refresh_usage();
            self.validate_manual_region(&region)?;
        }
        Ok(changed)
    }

    async fn reconcile_region(&self, region: MitoRegionRef) -> Result<bool> {
        self.reconcile_region_with_mode(region, false).await
    }

    async fn reconcile_region_with_mode(
        &self,
        region: MitoRegionRef,
        manual: bool,
    ) -> Result<bool> {
        // Full capacity defers builds, but cleanup must still reclaim published usage.
        let allow_builds = self.disk_usage.load(Ordering::Relaxed) < self.max_size;
        if !allow_builds {
            SERIES_INDEX_CAPACITY_DEFERRED.inc();
        }
        if !allow_builds
            || region.version().options.sst_format != Some(crate::sst::FormatType::Narthex)
            || region.state() != RegionRoleState::Leader(RegionLeaderState::Writable)
        {
            return Ok(false);
        }
        let result = self.reconcile_native(region, manual).await;
        if result.is_err() {
            SERIES_INDEX_RECONCILE_TOTAL
                .with_label_values(&["failure"])
                .inc();
        }
        result
    }

    async fn reconcile_native(&self, region: MitoRegionRef, manual: bool) -> Result<bool> {
        use crate::request::{
            RegionEditKind, RegionEditRequest, WorkerRequest, WorkerRequestWithTime,
        };
        use crate::series_index::bucket::{group_files_into_series_buckets, rounded_bucket_width};
        use crate::sst::file::{CertifiedWindow, FileFormat, NarthexCoverage};

        let current = region.version_control.current();
        let version = current.version;
        // A completed flush published its inputs and committed boundary together.
        // Defer during in-flight admissions, unflushed writes and WAL recovery.
        if current.last_sequence != version.flushed_sequence
            || current.committed_sequence != version.flushed_sequence
            || !version.memtables.is_empty()
            || !version.pending_tails.is_empty()
        {
            return Ok(false);
        }
        let Some(window) = version
            .compaction_time_window
            .or(manual.then_some(self.bucket_width))
        else {
            return Ok(false);
        };
        let Some(width) = rounded_bucket_width(self.bucket_width, window) else {
            return Ok(false);
        };
        let files: Vec<_> = version
            .ssts
            .levels()
            .iter()
            .flat_map(|l| l.files.values().cloned())
            .collect();
        let buckets =
            group_files_into_series_buckets(&files, width, (window.as_secs() as i64).max(1));
        let indexes = version.ssts.series_indexes();
        let worker = self
            .state
            .worker
            .as_ref()
            .ok_or_else(|| WorkerStoppedSnafu { id: self.worker_id }.build())?;
        let mut job = None;
        for bucket in buckets {
            if bucket.files.iter().all(|f| f.meta_ref().sequence.is_none()) {
                continue;
            }
            let existing: Vec<_> = indexes
                .values()
                .filter_map(|file| {
                    let FileFormat::NarthexSeriesIndex(index) = &file.meta_ref().format else {
                        return None;
                    };
                    let proof = index.coverage.as_ref()?;
                    (proof.region_id == region.region_id
                        && proof.truncated_entry_id == version.truncated_entry_id)
                        .then_some((file, proof))
                })
                .collect();
            if bucket
                .files
                .iter()
                .filter(|f| f.meta_ref().sequence.is_some())
                .all(|f| {
                    existing.iter().any(|(_, p)| {
                        p.schema_version == version.metadata.schema_version
                            && p.covers(f.meta_ref())
                    })
                })
            {
                continue;
            }

            let coverage = NarthexCoverage {
                required_files: Vec::new(),
                region_id: region.region_id,
                schema_version: version.metadata.schema_version,
                truncated_entry_id: version.truncated_entry_id,
                windows: vec![CertifiedWindow {
                    start: bucket.start,
                    end: bucket.end,
                    frontier: version.flushed_sequence,
                }],
            };
            let mut bases = Vec::new();
            let mut remove = Vec::new();
            for (file, proof) in &existing {
                if proof
                    .windows
                    .iter()
                    .all(|w| bucket.start <= w.start && w.end <= bucket.end)
                {
                    if self.state.delta && proof.schema_version == version.metadata.schema_version {
                        bases.push((*file).clone());
                    } else {
                        remove.push(file.file_id());
                    }
                }
            }
            let mut sources = bucket.files.into_vec();
            if !bases.is_empty() {
                sources.retain(|f| {
                    !existing.iter().any(|(index, p)| {
                        bases.iter().any(|b| b.file_id() == index.file_id())
                            && p.required_files
                                .iter()
                                .all(|id| bases.iter().any(|b| b.file_id() == *id))
                            && p.covers(f.meta_ref())
                    })
                });
            }
            job = Some((sources, bases, remove, coverage));
            break;
        }
        if job.is_none() {
            // Reuse the four-file maintenance trigger. Union only certified peers
            // in this schema and domain; retain their individual window frontiers.
            let mut inputs = Vec::new();
            let mut windows = Vec::new();
            let mut peers: Vec<_> = indexes.values().collect();
            peers.sort_unstable_by_key(|file| file.time_range().0);
            for file in peers {
                let FileFormat::NarthexSeriesIndex(meta) = &file.meta_ref().format else {
                    continue;
                };
                let Some(proof) = &meta.coverage else {
                    continue;
                };
                if proof.region_id != region.region_id
                    || proof.truncated_entry_id != version.truncated_entry_id
                    || proof.schema_version != version.metadata.schema_version
                {
                    continue;
                }
                if inputs
                    .iter()
                    .any(|f: &crate::sst::file::FileHandle| f.file_id() == file.file_id())
                {
                    continue;
                }
                let members = proof.required_files.iter().map(|id|
                    indexes.get(&id.file_id()).filter(|f| f.file_id() == *id && matches!(&f.meta_ref().format,
                        FileFormat::NarthexSeriesIndex(meta) if meta.coverage.as_ref() == Some(proof)))
                ).collect::<Option<Vec<_>>>();
                let Some(members) = members else { continue };
                if members.is_empty() {
                    inputs.push(file.clone());
                } else {
                    inputs.extend(members.into_iter().cloned());
                }
                windows.extend(proof.windows.iter().cloned());
                if inputs.len() >= 4 {
                    break;
                }
            }
            if inputs.len() >= 4 {
                windows.sort_by_key(|w| w.start);
                let remove = inputs.iter().map(|f| f.file_id()).collect();
                job = Some((
                    inputs,
                    Vec::new(),
                    remove,
                    NarthexCoverage {
                        required_files: Vec::new(),
                        region_id: region.region_id,
                        schema_version: version.metadata.schema_version,
                        truncated_entry_id: version.truncated_entry_id,
                        windows,
                    },
                ));
            }
        }
        if let Some((inputs, bases, remove, mut coverage)) = job {
            let output = if bases.is_empty() {
                Some(
                    crate::sst::narthex::write_series(
                        &region.access_layer,
                        version.metadata.clone(),
                        &inputs,
                        Some(coverage.clone()),
                    )
                    .await?,
                )
            } else {
                crate::sst::narthex::write_series_difference(
                    &region.access_layer,
                    version.metadata.clone(),
                    &inputs,
                    &bases,
                    Some(coverage.clone()),
                )
                .await?
            };
            let handle = output.as_ref().map(|output| {
                crate::sst::file::FileHandle::new(output.clone(), region.file_purger.clone())
            });
            let mut additions = bases
                .iter()
                .map(|f| f.meta_ref().clone())
                .chain(output)
                .collect::<Vec<_>>();
            if !bases.is_empty() {
                coverage.required_files = additions.iter().map(|f| f.file_id()).collect();
                for file in &mut additions {
                    if let FileFormat::NarthexSeriesIndex(meta) = &mut file.format {
                        meta.coverage = Some(coverage.clone());
                    }
                }
            }
            let edit = crate::manifest::action::RegionEdit {
                series_indexes_to_add: additions,
                series_indexes_to_remove: remove.clone(),
                ..Default::default()
            };
            let (tx, rx) = oneshot::channel();
            let mut request = RegionEditRequest::new(region.region_id, edit, false, tx);
            request.kind = RegionEditKind::NativeSeries(region.clone(), version.clone());
            if worker
                .send(WorkerRequestWithTime::new(WorkerRequest::EditRegion(
                    request,
                )))
                .await
                .is_err()
            {
                if let Some(handle) = handle {
                    handle.mark_deleted();
                }
                return WorkerStoppedSnafu { id: self.worker_id }.fail();
            }
            if let Err(error) = rx
                .await
                .map_err(|_| WorkerStoppedSnafu { id: self.worker_id }.build())?
            {
                if !error.may_have_persisted_manifest_update()
                    && let Some(handle) = handle
                {
                    handle.mark_deleted();
                }
                return Err(error);
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// Each worker contributes only its open regions, refreshed at maintenance boundaries.
    fn refresh_usage(&mut self) {
        let usage = self
            .regions
            .list_regions()
            .iter()
            .map(|region| {
                region
                    .version()
                    .ssts
                    .series_indexes()
                    .values()
                    .map(|file| file.meta_ref().file_size)
                    .sum::<u64>()
            })
            .sum();
        self.report_usage(usage);
    }

    fn report_usage(&mut self, usage: u64) {
        if usage >= self.reported_usage {
            let delta = usage - self.reported_usage;
            self.disk_usage.fetch_add(delta, Ordering::Relaxed);
            SERIES_INDEX_DISK_BYTES.add(delta as i64);
        } else {
            let delta = self.reported_usage - usage;
            self.disk_usage.fetch_sub(delta, Ordering::Relaxed);
            SERIES_INDEX_DISK_BYTES.sub(delta as i64);
        }
        self.reported_usage = usage;
    }

    /// Runs periodic maintenance independently of incoming deletion requests.
    async fn maintain(&mut self) {
        self.refresh_usage();
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
                continue;
            }
            if let Err(error) = self.reconcile_region(region.clone()).await {
                warn!(error; "Failed to reconcile series indexes, worker: {}, region: {}", self.worker_id, region.region_id);
            }
            self.refresh_usage();
        }
    }
}

impl Drop for SeriesIndexTask {
    fn drop(&mut self) {
        self.report_usage(0);
    }
}
