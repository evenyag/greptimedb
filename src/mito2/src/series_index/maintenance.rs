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
use std::time::{Duration, Instant};

use common_telemetry::{debug, info};
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::storage::{FileId, RegionId};

use crate::error::Result;
use crate::metrics::{SERIES_INDEX_RECONCILE_ELAPSED, SERIES_INDEX_RECONCILE_TOTAL};
use crate::read::series_candidate::is_sparse_metric_metadata;
use crate::region::version::VersionRef;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::series_index::artifact::ArtifactFormat;
use crate::series_index::bucket::{
    group_files_into_series_buckets, plan_series_indexes_with_limit, rounded_bucket_width,
};
use crate::series_index::builder::{build_range_index, build_series_index};
use crate::series_index::catalog::{
    RangeIndexCatalog, SeriesIndexCatalog, delete_catalogs, range_catalog_path,
    series_catalog_path, store_catalog,
};
use crate::series_index::format;
use crate::series_index::purger::IndexFilePurger;
use crate::series_index::version::{SeriesIndexFileHandle, SeriesIndexVersion};

/// Retires newly completed series files unless their snapshot is published.
#[derive(Default)]
struct UnpublishedSeriesFiles(
    Vec<SeriesIndexFileHandle>,
    Vec<crate::series_index::artifact::SamplesFileHandle>,
);

impl UnpublishedSeriesFiles {
    fn disarm(&mut self) {
        self.0.clear();
        self.1.clear();
    }
}

impl Drop for UnpublishedSeriesFiles {
    fn drop(&mut self) {
        for handle in &self.1 {
            handle.mark_deleted();
        }
        for handle in &self.0 {
            handle.mark_deleted();
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ReconcileStats {
    /// More eligible work remains after this bounded turn.
    pub(crate) pending: bool,
    pub(crate) sample_groups: usize,
    pub(crate) consolidated_series: usize,
    pub(crate) input_bytes: u64,
    pub(crate) output_bytes: u64,
    pub(crate) consolidated_samples: usize,
    pub(crate) built_samples: usize,
    pub(crate) removed_samples: usize,
    pub(crate) source_files: usize,
    pub(crate) built_range: usize,
    pub(crate) built_series: usize,
    pub(crate) removed_range: usize,
    pub(crate) removed_series: usize,
    pub(crate) computed_buckets: usize,
    pub(crate) skipped_buckets: usize,
}

impl ReconcileStats {
    pub(crate) fn changed(&self) -> bool {
        self.built_samples
            + self.removed_samples
            + self.built_range
            + self.built_series
            + self.removed_range
            + self.removed_series
            > 0
    }
}

/// Reconciles indexes for one region snapshot, persists catalogs, then atomically publishes it.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn reconcile_series_indexes(
    worker_id: u32,
    store: ObjectStore,
    region: MitoRegionRef,
    requested_bucket_width: Duration,
    now_ms: i64,
    purger: IndexFilePurger,
    enable_range_index: bool,
    allow_builds: bool,
) -> Result<ReconcileStats> {
    reconcile_series_indexes_with_mode(
        worker_id,
        store,
        region,
        requested_bucket_width,
        now_ms,
        purger,
        enable_range_index,
        allow_builds,
        false,
    )
    .await
}

/// Manual reconciliation bypasses maintenance eligibility, but not capacity limits.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn reconcile_series_indexes_with_mode(
    worker_id: u32,
    store: ObjectStore,
    region: MitoRegionRef,
    requested_bucket_width: Duration,
    now_ms: i64,
    purger: IndexFilePurger,
    enable_range_index: bool,
    allow_builds: bool,
    manual: bool,
) -> Result<ReconcileStats> {
    reconcile_series_indexes_with_budget(
        worker_id,
        store,
        region,
        requested_bucket_width,
        now_ms,
        purger,
        enable_range_index,
        allow_builds,
        manual,
        usize::MAX,
    )
    .await
}

/// Automatic work is limited to one source group or series bucket per turn.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn reconcile_series_indexes_with_budget(
    worker_id: u32,
    store: ObjectStore,
    region: MitoRegionRef,
    requested_bucket_width: Duration,
    now_ms: i64,
    purger: IndexFilePurger,
    enable_range_index: bool,
    allow_builds: bool,
    manual: bool,
    mut budget: usize,
) -> Result<ReconcileStats> {
    let total_start = Instant::now();
    let original_indexes = region.series_index_version();
    // Use this snapshot throughout reconciliation, even if the region version advances.
    let version = region.version_control.current().version;
    if !is_sparse_metric_metadata(&version.metadata) {
        SERIES_INDEX_RECONCILE_TOTAL
            .with_label_values(&["noop"])
            .inc();
        return Ok(ReconcileStats::default());
    }
    let build_start = Instant::now();
    let mut unpublished = UnpublishedSeriesFiles::default();
    let (next, stats) = build_index_version(
        &store,
        &region,
        &version,
        requested_bucket_width,
        now_ms,
        &purger,
        &mut unpublished,
        enable_range_index,
        allow_builds,
        manual,
        &mut budget,
    )
    .await?;
    SERIES_INDEX_RECONCILE_ELAPSED
        .with_label_values(&["build"])
        .observe(build_start.elapsed().as_secs_f64());
    // Once catalog I/O begins, cancellation must not retire outputs after a write
    // has committed. The detached commit owns the output guard until it publishes
    // or rejects the complete transaction; build cancellation remains speculative.
    let stats = if let Some(next) = next {
        let publication = PendingSeriesIndexVersion {
            version,
            previous: original_indexes,
            next,
            unpublished,
            stats,
            manual,
        };
        common_runtime::spawn_compact(publication.publish(store.clone(), region.clone()))
            .await
            .context(crate::error::JoinSnafu)??
    } else {
        stats
    };
    for (kind, count) in [
        ("sample_coverage", stats.sample_groups),
        ("sample_consolidation", stats.consolidated_samples),
        (
            "series_coverage",
            stats.built_series.saturating_sub(stats.consolidated_series),
        ),
        ("series_consolidation", stats.consolidated_series),
    ] {
        crate::metrics::SERIES_INDEX_WORK_TOTAL
            .with_label_values(&[kind])
            .inc_by(count as u64);
    }
    for (direction, bytes) in [("input", stats.input_bytes), ("output", stats.output_bytes)] {
        crate::metrics::SERIES_INDEX_WORK_BYTES_TOTAL
            .with_label_values(&[direction])
            .inc_by(bytes);
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

/// Complete speculative outputs and their source fence, owned through catalog commit.
struct PendingSeriesIndexVersion {
    version: VersionRef,
    previous: Arc<SeriesIndexVersion>,
    next: SeriesIndexVersion,
    unpublished: UnpublishedSeriesFiles,
    stats: ReconcileStats,
    manual: bool,
}

impl PendingSeriesIndexVersion {
    async fn publish(self, store: ObjectStore, region: MitoRegionRef) -> Result<ReconcileStats> {
        let Self {
            version,
            previous,
            next,
            mut unpublished,
            mut stats,
            manual,
        } = self;
        // Builds never hold this lock. Only the small catalog commit is serialized
        // against durable source removal, schema updates, stop, and staging promotion.
        let manager = region.manifest_ctx.manifest_manager.write().await;
        let manifest = manager.manifest();
        let live = region.version();
        let live_files: HashMap<_, _> = live
            .ssts
            .levels()
            .iter()
            .flat_map(|level| level.files())
            .map(|file| (file.file_id().file_id(), file.meta_ref()))
            .collect();
        let valid = Arc::ptr_eq(&previous, &region.series_index_version())
            && !manager.is_stopped()
            && matches!(
                region.state(),
                RegionRoleState::Follower | RegionRoleState::Leader(RegionLeaderState::Writable)
            )
            && live.options.ttl == version.options.ttl
            && live.metadata.as_ref() == version.metadata.as_ref()
            && manifest.metadata.as_ref() == version.metadata.as_ref()
            && version
                .ssts
                .levels()
                .iter()
                .flat_map(|level| level.files())
                .all(|file| {
                    let source = file.meta_ref();
                    live_files
                        .get(&source.file_id)
                        .is_some_and(|meta| same_source(source, meta))
                        && manifest
                            .files
                            .get(&source.file_id)
                            .is_some_and(|meta| same_source(source, meta))
                });
        if !valid {
            crate::metrics::SERIES_INDEX_RECONCILE_TOTAL
                .with_label_values(&["stale"])
                .inc();
            if manual {
                return crate::error::InvalidRequestSnafu {
                    region_id: region.region_id,
                    reason: "series-index sources or region lifecycle changed during construction",
                }
                .fail();
            }
            // A subsequent publication notification (or periodic recovery) retries
            // the current snapshot. Staleness is not an I/O failure with backoff.
            stats = ReconcileStats {
                pending: true,
                ..Default::default()
            };
            return Ok(stats);
        }
        persist_index_catalogs(&store, region.region_id, &previous, &next, &stats).await?;
        if region.state() == RegionRoleState::Leader(RegionLeaderState::Dropping) {
            // Stop/reopen cannot overtake cleanup while this manifest guard is held.
            delete_catalogs(&store, region.region_id).await;
            region.series_index_version_control.mark_dropped();
            stats = ReconcileStats::default();
            return Ok(stats);
        }
        publish_index_version(&region, Arc::new(next));
        unpublished.disarm();
        Ok(stats)
    }
}

/// Builds a changed snapshot, retaining reusable indexes and removing obsolete coverage.
/// Returns `None` when no range or series indexes were added or removed.
#[allow(clippy::too_many_arguments)]
async fn build_index_version(
    store: &ObjectStore,
    region: &MitoRegionRef,
    version: &VersionRef,
    requested_bucket_width: Duration,
    now_ms: i64,
    purger: &IndexFilePurger,
    unpublished: &mut UnpublishedSeriesFiles,
    enable_range_index: bool,
    allow_builds: bool,
    manual: bool,
    budget: &mut usize,
) -> Result<(Option<SeriesIndexVersion>, ReconcileStats)> {
    let mut stats = ReconcileStats::default();
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
    // Empty build inputs still let the planner expire established bucket coverage.
    let mut buckets = match (allow_builds, version.compaction_time_window) {
        (true, Some(window)) => rounded_bucket_width(requested_bucket_width, window)
            // Successful rounding guarantees the window fits in i64; subsecond windows
            // use the same one-second minimum as rounded_bucket_width.
            .map(|width| {
                group_files_into_series_buckets(&files, width, (window.as_secs() as i64).max(1))
            })
            .unwrap_or_default(),
        (true, None) => {
            let width = i64::try_from(requested_bucket_width.as_secs())
                .unwrap_or(i64::MAX)
                .max(1);
            group_files_into_series_buckets(&files, width, width)
        }
        (false, _) => Vec::new(),
    };
    for bucket in &mut buckets {
        bucket.requires_exact_coverage |= bucket.files.iter().any(|file| {
            file.region_id() != region.region_id || !file.meta_ref().preserve_row_sequence
        });
    }
    let formats = region
        .series_index_version_control
        .formats
        .read()
        .unwrap()
        .clone();
    let selected_format = formats.index.as_ref().map_or(
        crate::series_index::artifact::ArtifactFormat::Parquet,
        |backend| backend.format(),
    );
    let mut unusable = HashSet::new();
    for handle in current.series_indexes.values() {
        let entry = handle.entry();
        let mut reusable = entry.format() == selected_format
            && entry.target_partition_expr == version.metadata.partition_expr
            && (!entry.exact_source_coverage
                || entry.source_metadata.iter().all(|(id, source)| {
                    files
                        .iter()
                        .find(|file| file.file_id().file_id() == *id)
                        .is_some_and(|file| {
                            source
                                == &crate::series_index::artifact::SourceSst::from_file(
                                    file.meta_ref(),
                                )
                        })
                }));
        // Legacy Parquet indexes retain their existing maintenance behavior.
        if reusable && let Some(artifact) = &entry.artifact {
            for object in &artifact.objects {
                reusable &= store
                    .exists(&object.path)
                    .await
                    .context(crate::error::OpenDalSnafu)?;
            }
        }
        if manual && reusable && entry.artifact.is_none() {
            reusable = store
                .exists(&crate::series_index::catalog::series_index_path(
                    region.region_id,
                    entry.index_uuid,
                ))
                .await
                .context(crate::error::OpenDalSnafu)?;
        }
        // Repair schema changes even when SST coverage is unchanged.
        if reusable
            && selected_format == ArtifactFormat::ColumnarSeries
            && let Some(backend) = &formats.index
        {
            let reader = backend.reader(store.clone(), handle.clone()).await?;
            reusable = format::validate_series_schema(&reader.schema(), &version.metadata).is_ok();
        }
        if !reusable {
            unusable.insert(entry.index_uuid);
        }
    }
    for bucket in &mut buckets {
        bucket.needs_coverage = bucket.files.iter().any(|file| {
            !current.series_indexes.values().any(|handle| {
                !unusable.contains(&handle.entry().index_uuid)
                    && handle
                        .entry()
                        .covers_file(file.meta_ref(), region.region_id)
            })
        });
    }
    let mut reusable_samples = HashSet::new();
    if let Some(backend) = &formats.samples {
        for (id, handle) in &current.samples {
            if handle.entry().target_partition_expr != version.metadata.partition_expr
                || !handle.entry().sources_visible(&visible)
                || handle.entry().artifact.format != backend.format()
            {
                continue;
            }
            let mut reusable = handle.entry().source_metadata.iter().all(|(id, source)| {
                files
                    .iter()
                    .find(|file| file.file_id().file_id() == *id)
                    .is_some_and(|file| {
                        source
                            == &crate::series_index::artifact::SourceSst::from_file(file.meta_ref())
                    })
            });
            for object in &handle.entry().artifact.objects {
                reusable &= store
                    .exists(&object.path)
                    .await
                    .context(crate::error::OpenDalSnafu)?;
            }
            if reusable && backend.format() == ArtifactFormat::ColumnarSamples {
                let reader = backend.reader(store.clone(), handle.clone()).await?;
                reusable =
                    format::validate_sample_schema(&reader.schema(), &version.metadata).is_ok();
            }
            if reusable {
                reusable_samples.insert(*id);
            }
        }
    }
    let mut index_buckets = current.index_buckets.clone();
    for bucket in index_buckets.values_mut() {
        if bucket.index_ids.iter().any(|id| unusable.contains(id)) {
            // Force replacement only for the affected bucket. Retain its geometry and
            // handles until a replacement is built and its catalog is published.
            bucket.compaction_window_secs = 0;
            bucket.window_sequences.clear();
        }
    }
    let obsolete_exact: Vec<_> = current
        .series_indexes
        .iter()
        .filter(|(_, handle)| {
            handle.entry().exact_source_coverage
                && handle
                    .entry()
                    .source_file_ids
                    .iter()
                    .any(|id| !visible.contains(id))
        })
        .map(|(id, _)| *id)
        .collect();
    for bucket in index_buckets.values_mut() {
        bucket.index_ids.retain(|id| !obsolete_exact.contains(id));
    }
    index_buckets.retain(|_, bucket| !bucket.index_ids.is_empty());
    let mut plan = plan_series_indexes_with_limit(
        Vec::new(),
        index_buckets,
        version.options.ttl,
        now_ms,
        manual,
        0,
    );
    stats.computed_buckets = plan.computed_buckets;
    stats.skipped_buckets = plan.skipped_buckets;
    plan.expired_index_ids.extend(obsolete_exact);
    let next = prune_index_version(&current, &visible, &plan.expired_index_ids, &mut stats);
    if !allow_builds {
        // If cleanup releases capacity, the scheduler can promptly resume this region.
        stats.pending = !files.is_empty();
        let reusable_coverage = current
            .samples
            .iter()
            .filter(|(id, _)| reusable_samples.contains(id))
            .map(|(id, h)| (*id, h.clone()))
            .collect();
        if manual
            && files.iter().any(|file| {
                !current.series_indexes.values().any(|handle| {
                    handle.entry().format() == selected_format
                        && !unusable.contains(&handle.entry().index_uuid)
                        && handle
                            .entry()
                            .covers_file(file.meta_ref(), region.region_id)
                }) || (formats.samples.is_some()
                    && !crate::series_index::artifact::samples_cover_file(&reusable_coverage, file))
            })
        {
            return crate::error::InvalidRequestSnafu {
                region_id: region.region_id,
                reason: "series-index capacity limit prevents complete manual coverage",
            }
            .fail();
        }
        // Invalid format/schema/provenance cannot serve current readers. Retiring it
        // under pressure lets the next bounded turn rebuild instead of deadlocking
        // on space occupied by unusable generations. Pinned snapshots keep their files.
        let reusable: HashMap<_, _> = current
            .samples
            .iter()
            .filter(|(id, _)| reusable_samples.contains(id))
            .map(|(id, handle)| (*id, handle.clone()))
            .collect();
        let covered: HashSet<_> = files
            .iter()
            .filter(|file| crate::series_index::artifact::samples_cover_file(&reusable, file))
            .map(|file| file.file_id().file_id())
            .collect();
        let mut cleaned = next.unwrap_or_else(|| SeriesIndexVersion {
            samples: current.samples.clone(),
            range_indexes: current.range_indexes.clone(),
            series_indexes: current.series_indexes.clone(),
            index_buckets: plan.index_buckets.clone(),
        });
        cleaned.samples.retain(|id, handle| {
            reusable_samples.contains(id) && handle.entry().sources_visible(&covered)
        });
        cleaned
            .series_indexes
            .retain(|id, _| !unusable.contains(id));
        cleaned.index_buckets = plan.index_buckets;
        for bucket in cleaned.index_buckets.values_mut() {
            bucket
                .index_ids
                .retain(|id| cleaned.series_indexes.contains_key(id));
        }
        cleaned
            .index_buckets
            .retain(|_, bucket| !bucket.index_ids.is_empty());
        stats.removed_samples = current.samples.len() - cleaned.samples.len();
        stats.removed_series = current.series_indexes.len() - cleaned.series_indexes.len();
        return Ok((stats.changed().then_some(cleaned), stats));
    }
    let SeriesIndexVersion {
        mut range_indexes,
        mut series_indexes,
        mut samples,
        ..
    } = next.unwrap_or_else(|| SeriesIndexVersion {
        samples: current.samples.clone(),
        range_indexes: current.range_indexes.clone(),
        series_indexes: current.series_indexes.clone(),
        index_buckets: Default::default(),
    });
    if formats.samples.is_none() {
        stats.removed_samples += samples.len();
        samples.clear();
    }
    if let Some(backend) = &formats.samples {
        if backend.format() == ArtifactFormat::ColumnarSamples {
            let reusable: std::collections::HashMap<_, _> = samples
                .iter()
                .filter(|(id, _)| reusable_samples.contains(id))
                .map(|(id, handle)| (*id, handle.clone()))
                .collect();
            // Retain complete existing groups. Adding a source must not shift their boundaries.
            let covered: HashSet<_> = files
                .iter()
                .filter(|file| crate::series_index::artifact::samples_cover_file(&reusable, file))
                .map(|file| file.file_id().file_id())
                .collect();
            let mut daily: std::collections::HashMap<_, _> = reusable
                .into_iter()
                .filter(|(_, h)| {
                    !h.entry().source_file_ids.is_empty() && h.entry().sources_visible(&covered)
                })
                .collect();
            let covered: HashSet<_> = daily
                .values()
                .flat_map(|handle| handle.entry().source_file_ids.iter().copied())
                .collect();
            let mut uncovered: Vec<_> = files
                .iter()
                .filter(|file| !covered.contains(&file.file_id().file_id()))
                .cloned()
                .collect();
            uncovered.sort_unstable_by_key(|file| file.file_id().file_id().to_string());
            stats.pending |= uncovered.len().div_ceil(64) > *budget;
            for sources in uncovered.chunks(64).take(*budget) {
                let handles = crate::series_index::builder::build_sample_windows(
                    store,
                    region,
                    version,
                    sources,
                    backend.as_ref(),
                    purger,
                )
                .await?;
                // Companion range construction can fail after these outputs complete.
                unpublished.1.extend(handles.iter().cloned());
                *budget = budget.saturating_sub(1);
                stats.sample_groups += 1;
                stats.input_bytes += sources
                    .iter()
                    .map(|file| file.meta_ref().file_size)
                    .sum::<u64>();
                for file in sources {
                    let id = file.file_id().file_id();
                    if enable_range_index
                        && !range_indexes.contains_key(&id)
                        && let Some(entry) =
                            build_range_index(store, region, version, file.clone()).await?
                    {
                        range_indexes.insert(id, entry);
                        stats.built_range += 1;
                    }
                }
                for handle in handles {
                    stats.built_samples += 1;
                    daily.insert(handle.entry().file_id, handle);
                }
            }
            stats.removed_samples += samples.keys().filter(|id| !daily.contains_key(id)).count();
            samples = daily;
        } else {
            for file in &files {
                let id = file.file_id().file_id();
                let reusable = reusable_samples.contains(&id);
                if reusable {
                    continue;
                }
                if *budget == 0 {
                    stats.pending = true;
                    continue;
                }
                *budget = budget.saturating_sub(1);
                stats.sample_groups += 1;
                stats.input_bytes += file.meta_ref().file_size;
                if let Some(handle) = crate::series_index::builder::build_samples(
                    store,
                    region,
                    version,
                    file.clone(),
                    backend.as_ref(),
                    purger,
                )
                .await?
                {
                    unpublished.1.push(handle.clone());
                    samples.insert(id, handle);
                    stats.built_samples += 1;
                } else if manual {
                    return crate::error::InvalidRequestSnafu {
                        region_id: region.region_id,
                        reason: format!("SST {id} has no readable input for samples"),
                    }
                    .fail();
                }
            }
        }
    }
    let sample_gaps = formats.samples.is_some()
        && files
            .iter()
            .any(|file| !crate::series_index::artifact::samples_cover_file(&samples, file));
    plan = plan_series_indexes_with_limit(
        buckets,
        plan.index_buckets,
        version.options.ttl,
        now_ms,
        manual,
        if sample_gaps { 0 } else { *budget },
    );
    stats.pending |= plan.pending || sample_gaps;
    stats.computed_buckets = plan.computed_buckets;
    stats.skipped_buckets = plan.skipped_buckets;
    *budget = budget.saturating_sub(plan.builds.len());
    let index_buckets = plan.index_buckets;
    for id in &plan.superseded_index_ids {
        series_indexes.remove(id);
    }
    for (bucket, expected) in plan.builds {
        stats.input_bytes += bucket
            .files
            .iter()
            .map(|file| file.meta_ref().file_size)
            .sum::<u64>();
        stats.consolidated_series += usize::from(!bucket.needs_coverage);
        // Complete companion indexes independently so a failed series build preserves them.
        for file in &bucket.files {
            if enable_range_index
                && !range_indexes.contains_key(&file.file_id().file_id())
                && let Some(entry) = build_range_index(store, region, version, file.clone()).await?
            {
                stats.built_range += 1;
                range_indexes.insert(entry.file_id, entry);
            }
        }
        let series_handle = build_series_index(
            store,
            region,
            version,
            &bucket,
            &expected,
            formats.index.as_deref(),
            purger,
        )
        .await?;
        unpublished.0.push(series_handle.clone());
        stats.built_series += 1;
        series_indexes.insert(expected.index_uuid, series_handle);
    }
    // Cover SSTs outside planned aggregate builds, including skipped buckets.
    if enable_range_index {
        for file in &files {
            let file_id = file.file_id().file_id();
            if range_indexes.contains_key(&file_id) {
                continue;
            }
            if *budget == 0 {
                stats.pending = true;
                continue;
            }
            *budget = budget.saturating_sub(1);
            if let Some(entry) = build_range_index(store, region, version, file.clone()).await? {
                stats.built_range += 1;
                range_indexes.insert(entry.file_id, entry);
            }
        }
    }
    if !stats.pending
        && let Some(backend) = &formats.samples
        && backend.format() == ArtifactFormat::ColumnarSamples
        && let Some((sources, replaced)) = sample_consolidation(&files, &samples)
    {
        if *budget == 0 {
            stats.pending = true;
        } else {
            let handles = crate::series_index::builder::build_sample_windows(
                store,
                region,
                version,
                &sources,
                backend.as_ref(),
                purger,
            )
            .await?;
            // All populated days replace the entire selected source groups together.
            let old_count = replaced.len();
            if handles.len() < old_count {
                for id in replaced {
                    samples.remove(&id);
                }
                stats.removed_samples += old_count;
                stats.consolidated_samples += 1;
                stats.input_bytes += sources
                    .iter()
                    .map(|file| file.meta_ref().file_size)
                    .sum::<u64>();
                for handle in handles {
                    unpublished.1.push(handle.clone());
                    stats.built_samples += 1;
                    samples.insert(handle.entry().file_id, handle);
                }
                stats.pending = sample_consolidation(&files, &samples).is_some();
            } else {
                // Defensive: planning must prove fragmentation decreases before doing I/O.
                for handle in handles {
                    handle.mark_deleted();
                }
            }
        }
    }
    if manual
        && files.iter().any(|file| {
            !series_indexes.values().any(|handle| {
                handle.entry().format() == selected_format
                    && handle
                        .entry()
                        .covers_file(file.meta_ref(), region.region_id)
            }) || (formats.samples.is_some()
                && !crate::series_index::artifact::samples_cover_file(&samples, file))
        })
    {
        return crate::error::InvalidRequestSnafu {
            region_id: region.region_id,
            reason: "manual series-index build did not cover every SST",
        }
        .fail();
    }
    stats.removed_series = current
        .series_indexes
        .keys()
        .filter(|id| !series_indexes.contains_key(id))
        .count();
    // Bucket reconciliation is speculative until indexes change. Recompute its coverage
    // next time rather than replacing the published snapshot on a no-op pass.
    if !stats.changed() {
        return Ok((None, stats));
    }
    stats.output_bytes = unpublished
        .0
        .iter()
        .map(|handle| handle.entry().file_size)
        .sum::<u64>()
        + unpublished
            .1
            .iter()
            .map(|handle| handle.entry().artifact.file_size)
            .sum::<u64>();
    let next = SeriesIndexVersion {
        samples,
        range_indexes,
        series_indexes,
        index_buckets,
    };
    Ok((Some(next), stats))
}

fn same_source(left: &crate::sst::file::FileMeta, right: &crate::sst::file::FileMeta) -> bool {
    left.file_id == right.file_id
        && left.region_id == right.region_id
        && left.sequence == right.sequence
        && left.preserve_row_sequence == right.preserve_row_sequence
        && left.time_range == right.time_range
        && left.partition_expr == right.partition_expr
        && left.file_size == right.file_size
}

/// Selects complete source groups whose replacement strictly reduces daily generations.
/// Groups are indivisible: rebuilding a subset must not discard another source's coverage.
fn sample_consolidation(
    files: &[crate::sst::file::FileHandle],
    samples: &HashMap<FileId, crate::series_index::artifact::SamplesFileHandle>,
) -> Option<(Vec<crate::sst::file::FileHandle>, Vec<FileId>)> {
    let visible: HashMap<_, _> = files
        .iter()
        .map(|file| (file.file_id().file_id(), file))
        .collect();
    let mut groups = HashMap::<Vec<FileId>, (HashSet<Option<i64>>, Vec<FileId>)>::new();
    for (id, handle) in samples {
        let entry = handle.entry();
        let day = entry.window;
        if entry.source_file_ids.is_empty() {
            continue;
        }
        if entry.source_file_ids.iter().any(|id| {
            visible.get(id).is_none_or(|file| {
                !crate::series_index::artifact::samples_cover_file(samples, file)
            })
        }) {
            continue;
        }
        let group = groups.entry(entry.source_file_ids.clone()).or_default();
        group.0.insert(day);
        group.1.push(*id);
    }
    let mut groups: Vec<_> = groups.into_iter().collect();
    groups.sort_unstable_by(|a, b| {
        a.0.len().cmp(&b.0.len()).then_with(|| {
            a.0.iter()
                .map(|id| id.as_bytes())
                .cmp(b.0.iter().map(|id| id.as_bytes()))
        })
    });
    for (position, (left, (left_days, left_ids))) in groups.iter().enumerate() {
        for (right, (right_days, right_ids)) in &groups[position + 1..] {
            if left.len() + right.len() > 64
                || (left_days.is_disjoint(right_days)
                    && !left_days.contains(&None)
                    && !right_days.contains(&None))
                || left.iter().any(|id| right.contains(id))
            {
                continue;
            }
            let mut sources: Vec<_> = left.iter().chain(right).copied().collect();
            sources.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            return Some((
                sources
                    .iter()
                    .map(|id| visible.get(id).map(|file| (*file).clone()))
                    .collect::<Option<Vec<_>>>()?,
                left_ids.iter().chain(right_ids).copied().collect(),
            ));
        }
    }
    None
}

/// Prunes obsolete metadata without building indexes or retiring published handles.
/// Returns `None` without cloning the current version when no entries need removal.
/// Physical range deletion belongs to the SST purger; series handles are retired only
/// after the cleaned catalogs and snapshot have been published successfully.
fn prune_index_version(
    current: &SeriesIndexVersion,
    visible: &HashSet<FileId>,
    expired_index_ids: &[FileId],
    stats: &mut ReconcileStats,
) -> Option<SeriesIndexVersion> {
    if current
        .samples
        .values()
        .all(|h| h.entry().sources_visible(visible))
        && current.range_indexes.keys().all(|id| visible.contains(id))
        && expired_index_ids
            .iter()
            .all(|id| !current.series_indexes.contains_key(id))
    {
        return None;
    }
    let mut next = SeriesIndexVersion {
        samples: current.samples.clone(),
        range_indexes: current.range_indexes.clone(),
        series_indexes: current.series_indexes.clone(),
        index_buckets: current.index_buckets.clone(),
    };
    next.samples
        .retain(|_, h| h.entry().sources_visible(visible));
    stats.removed_samples = current.samples.len() - next.samples.len();
    next.range_indexes
        .retain(|file_id, _| visible.contains(file_id));
    for id in expired_index_ids {
        next.series_indexes.remove(id);
    }
    stats.removed_range = current.range_indexes.len() - next.range_indexes.len();
    stats.removed_series = current.series_indexes.len() - next.series_indexes.len();
    Some(next)
}

/// Commits one authoritative catalog; legacy mirrors remain available for diagnostics.
async fn persist_index_catalogs(
    store: &ObjectStore,
    region_id: RegionId,
    previous: &SeriesIndexVersion,
    next: &SeriesIndexVersion,
    stats: &ReconcileStats,
) -> Result<()> {
    let samples_path = crate::series_index::catalog::samples_catalog_path(region_id);
    if !next.samples.is_empty() || stats.removed_samples > 0 {
        return store_combined_catalog(store, &samples_path, next).await;
    }
    // Install the prior snapshot before touching legacy mirrors. On the first
    // publication, a failure after either mirror write must not make readers fall
    // back to references whose speculative outputs will be deleted.
    if !store
        .exists(&samples_path)
        .await
        .context(crate::error::OpenDalSnafu)?
    {
        store_combined_catalog(store, &samples_path, previous).await?;
    }
    if stats.built_range + stats.removed_range > 0 {
        let mut range_entries = next.range_indexes.values().copied().collect::<Vec<_>>();
        range_entries
            .sort_unstable_by(|left, right| left.file_id.as_bytes().cmp(right.file_id.as_bytes()));
        store_catalog(
            store,
            &range_catalog_path(region_id),
            &RangeIndexCatalog {
                indexes: range_entries,
            },
        )
        .await?;
    }
    if stats.built_series + stats.removed_series > 0 {
        let mut series_entries = next
            .series_indexes
            .values()
            .map(|handle| handle.entry().clone())
            .collect::<Vec<_>>();
        series_entries.sort_unstable_by_key(|entry| {
            (
                entry.bucket_start,
                entry.bucket_end,
                entry.min_file_sequence,
                entry.max_file_sequence,
            )
        });
        store_catalog(
            store,
            &series_catalog_path(region_id),
            &SeriesIndexCatalog {
                indexes: series_entries,
            },
        )
        .await?;
    }
    store_combined_catalog(store, &samples_path, next).await
}

async fn store_combined_catalog(
    store: &ObjectStore,
    path: &str,
    version: &SeriesIndexVersion,
) -> Result<()> {
    store_catalog(
        store,
        path,
        &crate::series_index::catalog::SamplesCatalog {
            entries: version
                .samples
                .values()
                .map(|handle| handle.entry().clone())
                .collect(),
            series: version
                .series_indexes
                .values()
                .map(|handle| handle.entry().clone())
                .collect(),
            ranges: version.range_indexes.values().copied().collect(),
        },
    )
    .await
}

/// Publishes the snapshot and retires series files absent from the new version.
fn publish_index_version(region: &MitoRegionRef, next: Arc<SeriesIndexVersion>) {
    let previous = region.series_index_version_control.publish(next.clone());
    for (id, handle) in &previous.samples {
        if next.samples.get(id).is_none_or(|next| {
            next.entry().artifact.generation != handle.entry().artifact.generation
        }) {
            handle.mark_deleted();
        }
    }
    for (id, handle) in &previous.series_indexes {
        if !next.series_indexes.contains_key(id) {
            // Purge only after readers release their retained handles.
            handle.mark_deleted();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_time::Timestamp;
    use object_store::services::Memory;

    use super::*;
    use crate::series_index::catalog::{RangeIndexEntry, SeriesIndexEntry};
    use crate::series_index::purger::series_index_channel;

    #[rstest::rstest]
    fn test_prune_index_version(
        #[values(false, true)] remove_range: bool,
        #[values(false, true)] remove_series: bool,
    ) {
        let range_id = FileId::random();
        let series_id = FileId::random();
        let store = ObjectStore::new(Memory::default()).unwrap();
        let (purger, mut receiver) = series_index_channel(store);
        let current = SeriesIndexVersion::new(
            HashMap::from([(
                range_id,
                RangeIndexEntry {
                    file_id: range_id,
                    file_size: 10,
                },
            )]),
            HashMap::from([(
                series_id,
                SeriesIndexFileHandle::new(
                    RegionId::new(1, 1),
                    SeriesIndexEntry {
                        artifact: None,
                        index_uuid: series_id,
                        file_size: 20,
                        bucket_start: Timestamp::new_second(0),
                        bucket_end: Timestamp::new_second(100),
                        exact_source_coverage: false,
                        target_partition_expr: None,
                        source_metadata: Default::default(),
                        source_file_ids: vec![range_id],
                        min_file_sequence: 1,
                        max_file_sequence: 1,
                        compaction_window_secs: 100,
                        window_sequences: Default::default(),
                    },
                    purger,
                ),
            )]),
        );
        let visible = if remove_range {
            HashSet::new()
        } else {
            HashSet::from([range_id])
        };
        // Unknown and repeated IDs must not inflate removal counts.
        let mut expired = vec![FileId::random()];
        if remove_series {
            expired.extend([series_id, series_id]);
        }
        let mut stats = ReconcileStats::default();
        let next = prune_index_version(&current, &visible, &expired, &mut stats);
        assert_eq!(remove_range || remove_series, next.is_some());
        assert_eq!(usize::from(remove_range), stats.removed_range);
        assert_eq!(usize::from(remove_series), stats.removed_series);
        if let Some(next) = next {
            assert_eq!(!remove_range, next.range_indexes.contains_key(&range_id));
            assert_eq!(!remove_series, next.series_indexes.contains_key(&series_id));
        }
        assert_eq!(1, current.range_indexes.len());
        assert_eq!(1, current.series_indexes.len());
        drop(current);
        // Pruning alone must never retire published files.
        assert!(receiver.try_recv().is_err());
    }
}
