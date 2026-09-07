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

//! Worker-owned series-index reconciliation and publication.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common_telemetry::{debug, info, warn};
use common_time::{TimeToLive, Timestamp};
use object_store::ObjectStore;
use store_api::storage::FileId;
use tokio::sync::{Notify, mpsc};

use super::bucket::{plan_series_buckets, rounded_bucket_width, series_entry};
use super::builder::{build_range_index, build_series_index};
use super::catalog::{
    RangeIndexCatalog, SeriesIndexCatalog, SeriesIndexEntry, load_catalog, load_range_indexes,
    load_series_indexes, range_catalog_path, same_series_coverage, series_catalog_path,
    store_catalog,
};
use super::purger::{IndexFilePurger, PurgeRequest, purge_file};
use super::version::{SeriesIndexFileHandle, SeriesIndexVersion};
use crate::error::Result;
use crate::metrics::{SERIES_INDEX_RECONCILE_ELAPSED, SERIES_INDEX_RECONCILE_TOTAL};
use crate::read::series_candidate::is_sparse_metric_metadata;
use crate::region::version::VersionRef;
use crate::region::{MitoRegionRef, RegionMapRef};

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
        // notify_one() retains a permit if the task has not started waiting yet.
        self.notify.notify_one();
    }

    pub(crate) async fn notified(&self) {
        self.notify.notified().await;
    }
}

#[derive(Debug, Default)]
pub(crate) struct ReconcileStats {
    pub(crate) source_files: usize,
    pub(crate) loaded_range: usize,
    pub(crate) loaded_series: usize,
    pub(crate) built_range: usize,
    pub(crate) built_series: usize,
    pub(crate) removed_range: usize,
    pub(crate) removed_series: usize,
    pub(crate) repaired_catalogs: usize,
    pub(crate) computed_buckets: usize,
    pub(crate) skipped_buckets: usize,
}

impl ReconcileStats {
    fn changed(&self) -> bool {
        self.built_range
            + self.built_series
            + self.removed_range
            + self.removed_series
            + self.repaired_catalogs
            > 0
    }
}

fn is_current_region_version(
    regions: &RegionMapRef,
    region: &MitoRegionRef,
    version: &VersionRef,
) -> bool {
    regions.get_region(region.region_id).is_some_and(|current| {
        Arc::ptr_eq(&current, region)
            && Arc::ptr_eq(&current.version_control.current().version, version)
    })
}

fn complete_bucket_expired(entry: &SeriesIndexEntry, ttl: Option<TimeToLive>, now_ms: i64) -> bool {
    let Some(ttl) = ttl else { return false };
    ttl.is_expired(&entry.bucket_end, &Timestamp::new_millisecond(now_ms))
        .unwrap_or(false)
}

/// Reconciles and atomically publishes all series indexes for one region snapshot.
pub(crate) async fn reconcile_series_indexes(
    worker_id: u32,
    store: ObjectStore,
    regions: RegionMapRef,
    region: MitoRegionRef,
    requested_bucket_width: Duration,
    now_ms: i64,
    purger: IndexFilePurger,
) -> Result<ReconcileStats> {
    let total_start = Instant::now();
    let version = region.version_control.current().version;
    let mut stats = ReconcileStats::default();
    if !is_sparse_metric_metadata(&version.metadata) {
        SERIES_INDEX_RECONCILE_TOTAL
            .with_label_values(&["noop"])
            .inc();
        return Ok(stats);
    }
    let files = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .cloned()
        .collect::<Vec<_>>();
    stats.source_files = files.len();
    let visible = files
        .iter()
        .map(|file| file.file_id().file_id())
        .collect::<HashSet<_>>();
    let current = region.series_index_version();
    let known = current
        .series_indexes
        .values()
        .map(|handle| (handle.identity(), handle.clone()))
        .collect::<HashMap<_, _>>();
    let load_start = Instant::now();
    let (range_catalog, repair_range_catalog) =
        load_catalog::<RangeIndexCatalog>(&store, &range_catalog_path(region.region_id)).await?;
    let (series_catalog, repair_series_catalog) =
        load_catalog::<SeriesIndexCatalog>(&store, &series_catalog_path(region.region_id)).await?;
    stats.repaired_catalogs =
        usize::from(repair_range_catalog) + usize::from(repair_series_catalog);
    let mut range_indexes = load_range_indexes(
        &store,
        region.region_id,
        range_catalog,
        &visible,
        &mut stats,
    )
    .await?;
    let mut retired_handles = Vec::new();
    let mut loaded_series = load_series_indexes(
        series_catalog,
        region.region_id,
        &purger,
        &known,
        &mut stats,
    );
    SERIES_INDEX_RECONCILE_ELAPSED
        .with_label_values(&["load"])
        .observe(load_start.elapsed().as_secs_f64());
    let buckets = match version.compaction_time_window {
        Some(window) => rounded_bucket_width(requested_bucket_width, window)
            .map(|width| plan_series_buckets(&files, width))
            .unwrap_or_default(),
        None => {
            debug!(
                "Deferring series indexes without compaction window, worker: {worker_id}, region: {}",
                region.region_id
            );
            Vec::new()
        }
    };
    stats.computed_buckets = buckets.len();
    let build_start = Instant::now();
    let mut series_indexes = HashMap::new();
    let mut series_entries = Vec::new();
    let mut newly_built = Vec::new();
    for bucket in buckets {
        let Some(mut expected) = series_entry(&bucket) else {
            stats.skipped_buckets += 1;
            if bucket.has_unknown_sequence {
                debug!(
                    "Deferring series-index bucket with unknown sequence, worker: {worker_id}, region: {}, bucket_start: {:?}, bucket_end: {:?}",
                    region.region_id, bucket.start, bucket.end
                );
            }
            continue;
        };
        if complete_bucket_expired(&expected, version.options.ttl, now_ms) {
            stats.skipped_buckets += 1;
            continue;
        }
        if let Some(position) = loaded_series
            .iter()
            .position(|(entry, _)| same_series_coverage(entry, &expected))
        {
            let (entry, handle) = loaded_series.swap_remove(position);
            if !complete_bucket_expired(&entry, version.options.ttl, now_ms) {
                series_entries.push(entry.clone());
                series_indexes.insert(entry.index_uuid, handle);
                continue;
            }
            retired_handles.push(handle);
            stats.removed_series += 1;
        }
        let missing_range_ids = bucket
            .files
            .iter()
            .map(|file| file.file_id().file_id())
            .filter(|file_id| !range_indexes.contains(file_id))
            .collect::<HashSet<_>>();
        expected.index_uuid = FileId::random();
        let (series_handle, built_ranges) = build_series_index(
            &store,
            &region,
            &version,
            &bucket,
            &expected,
            &missing_range_ids,
            &purger,
        )
        .await?;
        stats.built_series += 1;
        stats.built_range += built_ranges.len();
        range_indexes.extend(built_ranges);
        series_entries.push(expected.clone());
        newly_built.push(series_handle.clone());
        series_indexes.insert(expected.index_uuid, series_handle);
    }
    for file in files {
        let file_id = file.file_id().file_id();
        if range_indexes.contains(&file_id) {
            continue;
        }
        if let Some(file_id) = build_range_index(&store, &region, &version, file).await? {
            stats.built_range += 1;
            range_indexes.insert(file_id);
        }
    }
    SERIES_INDEX_RECONCILE_ELAPSED
        .with_label_values(&["build"])
        .observe(build_start.elapsed().as_secs_f64());
    stats.removed_series += loaded_series.len();
    retired_handles.extend(loaded_series.into_iter().map(|(_, handle)| handle));
    let next = Arc::new(SeriesIndexVersion {
        range_indexes,
        series_indexes,
    });
    if !is_current_region_version(&regions, &region, &version) {
        for handle in newly_built {
            handle.mark_deleted();
        }
        SERIES_INDEX_RECONCILE_TOTAL
            .with_label_values(&["stale"])
            .inc();
        debug!(
            "Skipped stale series-index publication, worker: {worker_id}, region: {}",
            region.region_id
        );
        return Ok(stats);
    }
    if stats.changed() {
        let mut range_entries = next.range_indexes.iter().copied().collect::<Vec<_>>();
        range_entries.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        series_entries.sort_unstable_by_key(|entry| {
            (
                entry.bucket_start,
                entry.bucket_end,
                entry.min_file_sequence,
                entry.max_file_sequence,
            )
        });
        store_catalog(
            &store,
            &range_catalog_path(region.region_id),
            &RangeIndexCatalog {
                indexes: range_entries,
            },
        )
        .await?;
        store_catalog(
            &store,
            &series_catalog_path(region.region_id),
            &SeriesIndexCatalog {
                indexes: series_entries,
            },
        )
        .await?;
    }
    if !is_current_region_version(&regions, &region, &version) {
        SERIES_INDEX_RECONCILE_TOTAL
            .with_label_values(&["stale"])
            .inc();
        debug!(
            "Skipped stale series-index publication after catalog update, worker: {worker_id}, region: {}",
            region.region_id
        );
        return Ok(stats);
    }
    let previous = region.series_index_version_control.publish(next.clone());
    let current_identities = next
        .series_indexes
        .values()
        .map(SeriesIndexFileHandle::identity)
        .collect::<HashSet<_>>();
    for handle in previous.series_indexes.values() {
        if !current_identities.contains(&handle.identity()) {
            handle.mark_deleted();
        }
    }
    for handle in retired_handles {
        handle.mark_deleted();
    }
    let result = if stats.changed() { "changed" } else { "noop" };
    SERIES_INDEX_RECONCILE_TOTAL
        .with_label_values(&[result])
        .inc();
    SERIES_INDEX_RECONCILE_ELAPSED
        .with_label_values(&["total"])
        .observe(total_start.elapsed().as_secs_f64());
    if stats.changed() {
        info!(
            "Reconciled series-index snapshot, worker: {worker_id}, region: {}, elapsed: {:?}, stats: {:?}",
            region.region_id,
            total_start.elapsed(),
            stats
        );
    } else {
        debug!(
            "Series-index reconciliation made no changes, worker: {worker_id}, region: {}",
            region.region_id
        );
    }
    Ok(stats)
}

/// Runs one sequential reconciliation task for a region worker.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_series_index_task(
    worker_id: u32,
    store: ObjectStore,
    regions: RegionMapRef,
    state: Arc<SeriesIndexTaskState>,
    interval: Duration,
    bucket_width: Duration,
    mut purge_receiver: mpsc::UnboundedReceiver<PurgeRequest>,
    purger: IndexFilePurger,
) {
    info!("Start series-index reconciliation task, worker: {worker_id}");
    let mut retry_purges = Vec::new();
    while state.is_running() {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = state.notified() => {}
            Some(request) = purge_receiver.recv() => {
                if !purge_file(&store, request).await {
                    retry_purges.push(request);
                }
                continue;
            }
        }
        if !state.is_running() {
            break;
        }
        for request in std::mem::take(&mut retry_purges) {
            if !purge_file(&store, request).await {
                retry_purges.push(request);
            }
        }
        for region in regions.list_regions() {
            if let Err(error) = reconcile_series_indexes(
                worker_id,
                store.clone(),
                regions.clone(),
                region.clone(),
                bucket_width,
                common_time::util::current_time_millis(),
                purger.clone(),
            )
            .await
            {
                SERIES_INDEX_RECONCILE_TOTAL
                    .with_label_values(&["failure"])
                    .inc();
                warn!(error; "Failed to reconcile series indexes, worker: {worker_id}, region: {}, phase: reconcile, retry: true", region.region_id);
            }
        }
    }
    while let Ok(request) = purge_receiver.try_recv() {
        retry_purges.push(request);
    }
    for request in retry_purges {
        let _ = purge_file(&store, request).await;
    }
    info!("Stop series-index reconciliation task, worker: {worker_id}");
}
