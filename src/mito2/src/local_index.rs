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

//! Disposable machine-local indexes maintained from visible SSTs.

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_stream::try_stream;
use common_telemetry::{debug, info, warn};
use common_time::timestamp::TimeUnit;
use common_time::{TimeToLive, Timestamp};
use futures::TryStreamExt;
use object_store::{ErrorKind, ObjectStore};
use parquet::file::metadata::KeyValue;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{FileId, RegionId};
use tokio::sync::{Notify, mpsc};

use crate::error::{OpenDalSnafu, Result, SerdeJsonSnafu, UnexpectedSnafu};
use crate::metrics::{
    LOCAL_INDEX_FILE_OPERATION_TOTAL, LOCAL_INDEX_RECONCILE_ELAPSED, LOCAL_INDEX_RECONCILE_TOTAL,
};
use crate::read::BoxedRecordBatchStream;
use crate::read::flat_merge::FlatMergeReader;
use crate::read::prune::FlatPruneReader;
use crate::read::read_columns::ReadColumns;
use crate::read::series_candidate::is_sparse_metric_metadata;
use crate::region::version::VersionRef;
use crate::region::{MitoRegionRef, RegionMapRef};
use crate::series_index::{SeriesIndexWriter, SeriesIndexWriterOptions};
use crate::sst::file::{FileHandle, RegionFileId};
use crate::sst::parquet::reader::{FlatRowGroupReader, ReaderMetrics};
use crate::sst::parquet::row_group::ParquetFetchMetrics;
use crate::sst::range_index::{SstRangeIndexWriter, SstRangeIndexWriterOptions};

pub(crate) const RANGE_DIR: &str = "range";
pub(crate) const SERIES_DIR: &str = "series";
const RANGE_CATALOG: &str = "range-index.json";
const SERIES_CATALOG: &str = "series-index.json";
const SERIES_METADATA_KEY: &str = "greptime.local_series_index";

/// Shared lifecycle state for a worker's local-index task.
#[derive(Debug)]
pub(crate) struct LocalIndexTaskState {
    running: AtomicBool,
    notify: Notify,
}

impl LocalIndexTaskState {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum LocalIndexType {
    Range,
    Series,
}

impl LocalIndexType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Range => "range",
            Self::Series => "series",
        }
    }
}

#[derive(Debug)]
pub(crate) struct PurgeRequest {
    index_type: LocalIndexType,
    file_id: RegionFileId,
}

#[derive(Clone)]
pub(crate) struct LocalIndexFilePurger {
    store: ObjectStore,
    sender: mpsc::UnboundedSender<PurgeRequest>,
}

impl Debug for LocalIndexFilePurger {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalIndexFilePurger")
            .finish_non_exhaustive()
    }
}

impl LocalIndexFilePurger {
    fn purge(&self, request: PurgeRequest) {
        if let Err(error) = self.sender.send(request) {
            let store = self.store.clone();
            common_runtime::spawn_global(async move {
                let _ = purge_file(&store, error.0).await;
            });
        }
    }
}

/// A reference-counted local-index file with deferred deletion semantics.
#[derive(Clone)]
pub(crate) struct LocalIndexFileHandle {
    inner: Arc<LocalIndexFileHandleInner>,
}

impl Debug for LocalIndexFileHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalIndexFileHandle")
            .field("file_id", &self.inner.file_id)
            .field("index_type", &self.inner.index_type)
            .field("deleted", &self.inner.deleted.load(Ordering::Relaxed))
            .finish()
    }
}

impl LocalIndexFileHandle {
    fn new(
        file_id: RegionFileId,
        index_type: LocalIndexType,
        purger: LocalIndexFilePurger,
    ) -> Self {
        Self {
            inner: Arc::new(LocalIndexFileHandleInner {
                file_id,
                index_type,
                deleted: AtomicBool::new(false),
                purger,
            }),
        }
    }

    fn identity(&self) -> (LocalIndexType, RegionFileId) {
        (self.inner.index_type, self.inner.file_id)
    }

    pub(crate) fn mark_deleted(&self) {
        self.inner.deleted.store(true, Ordering::Release);
    }
}

struct LocalIndexFileHandleInner {
    file_id: RegionFileId,
    index_type: LocalIndexType,
    deleted: AtomicBool,
    purger: LocalIndexFilePurger,
}

impl Drop for LocalIndexFileHandleInner {
    fn drop(&mut self) {
        if self.deleted.load(Ordering::Acquire) {
            self.purger.purge(PurgeRequest {
                index_type: self.index_type,
                file_id: self.file_id,
            });
        }
    }
}

/// Immutable local-index snapshot for one region.
#[derive(Debug, Default)]
pub(crate) struct LocalIndexVersion {
    pub(crate) range_indexes: HashMap<FileId, LocalIndexFileHandle>,
    pub(crate) series_indexes: HashMap<FileId, LocalIndexFileHandle>,
}

impl LocalIndexVersion {
    fn mark_all_deleted(&self) {
        self.range_indexes
            .values()
            .chain(self.series_indexes.values())
            .for_each(LocalIndexFileHandle::mark_deleted);
    }
}

/// Copy-on-write local-index snapshots owned by a region.
#[derive(Debug, Default)]
pub(crate) struct LocalIndexVersionControl {
    current: RwLock<Arc<LocalIndexVersion>>,
}

impl LocalIndexVersionControl {
    pub(crate) fn current(&self) -> Arc<LocalIndexVersion> {
        self.current.read().unwrap().clone()
    }

    fn publish(&self, next: Arc<LocalIndexVersion>) -> Arc<LocalIndexVersion> {
        std::mem::replace(&mut *self.current.write().unwrap(), next)
    }

    pub(crate) fn mark_dropped(&self) {
        self.publish(Arc::new(LocalIndexVersion::default()))
            .mark_all_deleted();
    }
}

/// Self-describing coverage stored in a series-index Parquet footer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SeriesIndexEntry {
    index_uuid: FileId,
    /// Inclusive bucket start in Unix milliseconds.
    bucket_start_ms: i64,
    /// Exclusive bucket end in Unix milliseconds.
    bucket_end_ms: i64,
    source_file_ids: Vec<FileId>,
    min_file_sequence: u64,
    max_file_sequence: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct SeriesIndexCatalog {
    indexes: Vec<SeriesIndexEntry>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RangeIndexCatalog {
    indexes: Vec<FileId>,
}

#[derive(Debug, Clone)]
struct SeriesBucket {
    start_ms: i64,
    end_ms: i64,
    files: Vec<FileHandle>,
    has_unknown_sequence: bool,
}

#[derive(Debug, Default)]
pub(crate) struct ReconcileStats {
    source_files: usize,
    loaded_range: usize,
    loaded_series: usize,
    built_range: usize,
    built_series: usize,
    removed_range: usize,
    removed_series: usize,
    repaired_catalogs: usize,
    computed_buckets: usize,
    skipped_buckets: usize,
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

pub(crate) fn range_index_path(region_id: RegionId, file_id: FileId) -> String {
    format!("{}/{RANGE_DIR}/{file_id}.parquet", region_id.as_u64())
}

fn range_catalog_path(region_id: RegionId) -> String {
    format!("{}/{RANGE_CATALOG}", region_id.as_u64())
}

fn series_index_path(region_id: RegionId, index_uuid: FileId) -> String {
    format!("{}/{SERIES_DIR}/{index_uuid}.parquet", region_id.as_u64())
}

fn series_catalog_path(region_id: RegionId) -> String {
    format!("{}/{SERIES_CATALOG}", region_id.as_u64())
}

fn local_index_path(index_type: LocalIndexType, file_id: RegionFileId) -> String {
    match index_type {
        LocalIndexType::Range => range_index_path(file_id.region_id(), file_id.file_id()),
        LocalIndexType::Series => series_index_path(file_id.region_id(), file_id.file_id()),
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

fn timestamp_millis(timestamp: Timestamp) -> Option<i64> {
    timestamp
        .convert_to(TimeUnit::Millisecond)
        .map(|timestamp| timestamp.value())
}

fn rounded_bucket_width(requested: Duration, compaction_window: Duration) -> Option<i64> {
    let window_ms = i64::try_from(compaction_window.as_millis()).ok()?.max(1);
    let requested_ms = i64::try_from(requested.as_millis())
        .unwrap_or(i64::MAX)
        .max(1);
    let multiples = requested_ms.saturating_add(window_ms - 1) / window_ms;
    Some(multiples.saturating_mul(window_ms))
}

fn plan_series_buckets(files: &[FileHandle], width_ms: i64) -> Vec<SeriesBucket> {
    let mut spans = files
        .iter()
        .filter_map(|file| {
            let start = timestamp_millis(file.time_range().0)?;
            let end = timestamp_millis(file.time_range().1)?;
            Some(SeriesBucket {
                start_ms: start.div_euclid(width_ms).saturating_mul(width_ms),
                end_ms: end
                    .div_euclid(width_ms)
                    .saturating_add(1)
                    .saturating_mul(width_ms),
                files: vec![file.clone()],
                has_unknown_sequence: file.meta_ref().sequence.is_none(),
            })
        })
        .collect::<Vec<_>>();
    spans.sort_by_key(|span| (span.start_ms, span.end_ms));
    let mut buckets: Vec<SeriesBucket> = Vec::new();
    for mut span in spans {
        if let Some(last) = buckets.last_mut()
            && span.start_ms < last.end_ms
        {
            last.end_ms = last.end_ms.max(span.end_ms);
            last.files.append(&mut span.files);
            last.has_unknown_sequence |= span.has_unknown_sequence;
        } else {
            buckets.push(span);
        }
    }
    buckets
}

fn series_entry(bucket: &SeriesBucket) -> Option<SeriesIndexEntry> {
    if bucket.has_unknown_sequence || bucket.files.len() < 2 {
        return None;
    }
    let mut source_file_ids = bucket
        .files
        .iter()
        .map(|file| file.file_id().file_id())
        .collect::<Vec<_>>();
    source_file_ids.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    let mut sequences = bucket
        .files
        .iter()
        .filter_map(|file| file.meta_ref().sequence.map(|sequence| sequence.get()));
    let first = sequences.next()?;
    let (mut min_file_sequence, mut max_file_sequence) = (first, first);
    for sequence in sequences {
        min_file_sequence = min_file_sequence.min(sequence);
        max_file_sequence = max_file_sequence.max(sequence);
    }
    Some(SeriesIndexEntry {
        index_uuid: FileId::random(),
        bucket_start_ms: bucket.start_ms,
        bucket_end_ms: bucket.end_ms,
        source_file_ids,
        min_file_sequence,
        max_file_sequence,
    })
}

fn same_series_coverage(left: &SeriesIndexEntry, right: &SeriesIndexEntry) -> bool {
    left.bucket_start_ms == right.bucket_start_ms
        && left.bucket_end_ms == right.bucket_end_ms
        && left.source_file_ids == right.source_file_ids
        && left.min_file_sequence == right.min_file_sequence
        && left.max_file_sequence == right.max_file_sequence
}

fn series_metadata(entry: &SeriesIndexEntry) -> Result<Vec<KeyValue>> {
    Ok(vec![KeyValue::new(
        SERIES_METADATA_KEY.to_string(),
        Some(serde_json::to_string(entry).context(SerdeJsonSnafu)?),
    )])
}

async fn load_catalog<T>(store: &ObjectStore, path: &str) -> Result<(T, bool)>
where
    T: Default + DeserializeOwned,
{
    let bytes = match store.read(path).await {
        Ok(bytes) => bytes.to_bytes(),
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok((T::default(), true)),
        Err(error) => return Err(error).context(OpenDalSnafu),
    };
    match serde_json::from_slice(&bytes) {
        Ok(catalog) => Ok((catalog, false)),
        Err(error) => {
            warn!(error; "Invalid local-index catalog, path: {path}, phase: load, retry: true");
            Ok((T::default(), true))
        }
    }
}

async fn store_catalog<T>(store: &ObjectStore, path: &str, catalog: &T) -> Result<()>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec_pretty(catalog).context(SerdeJsonSnafu)?;
    store
        .write(path, bytes)
        .await
        .map(|_| ())
        .context(OpenDalSnafu)
}

fn load_range_indexes(
    catalog: RangeIndexCatalog,
    region_id: RegionId,
    visible: &HashSet<FileId>,
    purger: &LocalIndexFilePurger,
    known: &HashMap<(LocalIndexType, RegionFileId), LocalIndexFileHandle>,
    stats: &mut ReconcileStats,
) -> (
    HashMap<FileId, LocalIndexFileHandle>,
    Vec<LocalIndexFileHandle>,
) {
    let mut indexes = HashMap::new();
    let mut retired = Vec::new();
    for file_id in catalog.indexes {
        let region_file_id = RegionFileId::new(region_id, file_id);
        let identity = (LocalIndexType::Range, region_file_id);
        let handle = known.get(&identity).cloned().unwrap_or_else(|| {
            LocalIndexFileHandle::new(region_file_id, LocalIndexType::Range, purger.clone())
        });
        if !visible.contains(&file_id) {
            stats.removed_range += 1;
            retired.push(handle);
            continue;
        }
        stats.loaded_range += 1;
        file_operation(LocalIndexType::Range, "load", "success");
        indexes.insert(file_id, handle);
    }
    (indexes, retired)
}

fn load_series_indexes(
    catalog: SeriesIndexCatalog,
    region_id: RegionId,
    purger: &LocalIndexFilePurger,
    known: &HashMap<(LocalIndexType, RegionFileId), LocalIndexFileHandle>,
    stats: &mut ReconcileStats,
) -> Vec<(SeriesIndexEntry, LocalIndexFileHandle)> {
    let mut indexes = Vec::new();
    for entry in catalog.indexes {
        let region_file_id = RegionFileId::new(region_id, entry.index_uuid);
        let identity = (LocalIndexType::Series, region_file_id);
        let handle = known.get(&identity).cloned().unwrap_or_else(|| {
            LocalIndexFileHandle::new(region_file_id, LocalIndexType::Series, purger.clone())
        });
        stats.loaded_series += 1;
        file_operation(LocalIndexType::Series, "load", "success");
        indexes.push((entry, handle));
    }
    indexes
}

async fn reader_input(
    region: &MitoRegionRef,
    file: FileHandle,
) -> Result<
    Option<(
        Arc<crate::sst::parquet::file_range::FileRangeContext>,
        crate::sst::parquet::row_selection::RowGroupSelection,
    )>,
> {
    Ok(region
        .access_layer
        .read_sst(file)
        .projection(Some(ReadColumns::new([])))
        .build_reader_input(&mut ReaderMetrics::default())
        .await?
        .map(|(context, selection)| (Arc::new(context), selection)))
}

async fn build_range_index(
    store: &ObjectStore,
    region: &MitoRegionRef,
    version: &VersionRef,
    file: FileHandle,
    purger: &LocalIndexFilePurger,
) -> Result<Option<LocalIndexFileHandle>> {
    let file_id = file.file_id().file_id();
    let path = range_index_path(region.region_id, file_id);
    let mut writer = SstRangeIndexWriter::try_new(
        version.metadata.clone(),
        store.clone(),
        &path,
        SstRangeIndexWriterOptions::default(),
    )
    .await?;
    let Some((context, mut selection)) = reader_input(region, file).await? else {
        writer.abort().await?;
        return Ok(None);
    };
    let fetch_metrics = ParquetFetchMetrics::default();
    while let Some((row_group_id, row_selection)) = selection.pop_first() {
        let parquet_reader = context
            .reader_builder()
            .build(context.build_context(row_group_id, Some(row_selection), Some(&fetch_metrics)))
            .await?;
        let mut reader = FlatPruneReader::new_with_row_group_reader(
            context.clone(),
            FlatRowGroupReader::new(context.clone(), parquet_reader),
            context.pre_filter_mode().skip_fields(),
        );
        while let Some(batch) = reader.next_batch().await? {
            writer.write(row_group_id as u32, &batch).await?;
        }
    }
    writer.finish().await?;
    file_operation(LocalIndexType::Range, "build", "success");
    Ok(Some(LocalIndexFileHandle::new(
        RegionFileId::new(region.region_id, file_id),
        LocalIndexType::Range,
        purger.clone(),
    )))
}

async fn build_series_index(
    store: &ObjectStore,
    region: &MitoRegionRef,
    version: &VersionRef,
    bucket: &SeriesBucket,
    entry: &SeriesIndexEntry,
    missing_range_ids: &HashSet<FileId>,
    purger: &LocalIndexFilePurger,
) -> Result<(LocalIndexFileHandle, HashMap<FileId, LocalIndexFileHandle>)> {
    struct UnpublishedRangeIndexes(Arc<Mutex<HashMap<FileId, LocalIndexFileHandle>>>);

    impl Drop for UnpublishedRangeIndexes {
        fn drop(&mut self) {
            for handle in self.0.lock().unwrap().values() {
                handle.mark_deleted();
            }
        }
    }

    let completed_ranges = Arc::new(Mutex::new(HashMap::new()));
    let unpublished_ranges = UnpublishedRangeIndexes(completed_ranges.clone());
    let mut sources = Vec::<BoxedRecordBatchStream>::new();
    let mut schema = None;
    let mut expected_ranges = 0;
    for file in &bucket.files {
        let file_id = file.file_id().file_id();
        let Some((context, mut selection)) = reader_input(region, file.clone()).await? else {
            continue;
        };
        schema.get_or_insert(context.read_format().output_arrow_schema()?);
        let range_writer = if missing_range_ids.contains(&file_id) {
            expected_ranges += 1;
            let path = range_index_path(region.region_id, file_id);
            Some(
                SstRangeIndexWriter::try_new(
                    version.metadata.clone(),
                    store.clone(),
                    &path,
                    SstRangeIndexWriterOptions::default(),
                )
                .await?,
            )
        } else {
            None
        };
        let completed_ranges = completed_ranges.clone();
        let range_purger = purger.clone();
        let region_id = region.region_id;
        sources.push(Box::pin(try_stream! {
            let fetch_metrics = ParquetFetchMetrics::default();
            let mut range_writer = range_writer;
            while let Some((row_group_id, row_selection)) = selection.pop_first() {
                let parquet_reader = context.reader_builder().build(context.build_context(
                    row_group_id,
                    Some(row_selection),
                    Some(&fetch_metrics),
                )).await?;
                let mut reader = FlatPruneReader::new_with_row_group_reader(
                    context.clone(),
                    FlatRowGroupReader::new(context.clone(), parquet_reader),
                    context.pre_filter_mode().skip_fields(),
                );
                while let Some(batch) = reader.next_batch().await? {
                    if let Some(writer) = &mut range_writer {
                        writer.write(row_group_id as u32, &batch).await?;
                    }
                    yield batch;
                }
            }
            if let Some(writer) = range_writer {
                writer.finish().await?;
                file_operation(LocalIndexType::Range, "build", "success");
                completed_ranges.lock().unwrap().insert(
                    file_id,
                    LocalIndexFileHandle::new(
                        RegionFileId::new(region_id, file_id),
                        LocalIndexType::Range,
                        range_purger,
                    ),
                );
            }
        }));
    }
    let schema = schema.context(UnexpectedSnafu {
        reason: "local series-index bucket has no readable SST",
    })?;
    let merged: BoxedRecordBatchStream = if sources.len() == 1 {
        sources.pop().context(UnexpectedSnafu {
            reason: "local series-index source disappeared",
        })?
    } else {
        Box::pin(
            FlatMergeReader::new(schema, sources, 8192, None)
                .await?
                .into_stream(),
        )
    };
    // TODO: Deduplicate update-mode rows before local series indexes are used by queries.
    let mut visible = merged;
    let path = series_index_path(region.region_id, entry.index_uuid);
    let mut writer = SeriesIndexWriter::try_new(
        region.metadata(),
        store.clone(),
        &path,
        SeriesIndexWriterOptions::default(),
        Some(series_metadata(entry)?),
    )
    .await?;
    while let Some(batch) = visible.try_next().await? {
        writer.write(&batch).await?;
    }
    writer.finish().await?;
    file_operation(LocalIndexType::Series, "build", "success");
    let range_indexes = std::mem::take(&mut *completed_ranges.lock().unwrap());
    if range_indexes.len() != expected_ranges {
        return UnexpectedSnafu {
            reason: "not all combined range-index builds completed",
        }
        .fail();
    }
    drop(unpublished_ranges);
    Ok((
        LocalIndexFileHandle::new(
            RegionFileId::new(region.region_id, entry.index_uuid),
            LocalIndexType::Series,
            purger.clone(),
        ),
        range_indexes,
    ))
}

fn complete_bucket_expired(entry: &SeriesIndexEntry, ttl: Option<TimeToLive>, now_ms: i64) -> bool {
    let Some(ttl) = ttl else { return false };
    ttl.is_expired(
        &Timestamp::new_millisecond(entry.bucket_end_ms),
        &Timestamp::new_millisecond(now_ms),
    )
    .unwrap_or(false)
}

/// Reconciles and atomically publishes all local indexes for one region snapshot.
pub(crate) async fn reconcile_local_indexes(
    worker_id: u32,
    store: ObjectStore,
    regions: RegionMapRef,
    region: MitoRegionRef,
    requested_bucket_width: Duration,
    now_ms: i64,
    purger: LocalIndexFilePurger,
) -> Result<ReconcileStats> {
    let total_start = Instant::now();
    let version = region.version_control.current().version;
    let mut stats = ReconcileStats::default();
    if !is_sparse_metric_metadata(&version.metadata) {
        LOCAL_INDEX_RECONCILE_TOTAL
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
    let current = region.local_index_version();
    let known = current
        .range_indexes
        .values()
        .chain(current.series_indexes.values())
        .map(|handle| (handle.identity(), handle.clone()))
        .collect::<HashMap<_, _>>();
    let load_start = Instant::now();
    let (range_catalog, repair_range_catalog) =
        load_catalog::<RangeIndexCatalog>(&store, &range_catalog_path(region.region_id)).await?;
    let (series_catalog, repair_series_catalog) =
        load_catalog::<SeriesIndexCatalog>(&store, &series_catalog_path(region.region_id)).await?;
    stats.repaired_catalogs =
        usize::from(repair_range_catalog) + usize::from(repair_series_catalog);
    let (mut range_indexes, mut retired_handles) = load_range_indexes(
        range_catalog,
        region.region_id,
        &visible,
        &purger,
        &known,
        &mut stats,
    );
    let mut loaded_series = load_series_indexes(
        series_catalog,
        region.region_id,
        &purger,
        &known,
        &mut stats,
    );
    LOCAL_INDEX_RECONCILE_ELAPSED
        .with_label_values(&["load"])
        .observe(load_start.elapsed().as_secs_f64());
    let buckets = match version.compaction_time_window {
        Some(window) => rounded_bucket_width(requested_bucket_width, window)
            .map(|width| plan_series_buckets(&files, width))
            .unwrap_or_default(),
        None => {
            debug!(
                "Deferring local series indexes without compaction window, worker: {worker_id}, region: {}",
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
                    "Deferring local series-index bucket with unknown sequence, worker: {worker_id}, region: {}, bucket_start_ms: {}, bucket_end_ms: {}",
                    region.region_id, bucket.start_ms, bucket.end_ms
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
            .filter(|file_id| !range_indexes.contains_key(file_id))
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
        newly_built.extend(built_ranges.values().cloned());
        range_indexes.extend(built_ranges);
        series_entries.push(expected.clone());
        newly_built.push(series_handle.clone());
        series_indexes.insert(expected.index_uuid, series_handle);
    }
    for file in files {
        let file_id = file.file_id().file_id();
        if range_indexes.contains_key(&file_id) {
            continue;
        }
        if let Some(handle) = build_range_index(&store, &region, &version, file, &purger).await? {
            stats.built_range += 1;
            newly_built.push(handle.clone());
            range_indexes.insert(file_id, handle);
        }
    }
    LOCAL_INDEX_RECONCILE_ELAPSED
        .with_label_values(&["build"])
        .observe(build_start.elapsed().as_secs_f64());
    stats.removed_series += loaded_series.len();
    retired_handles.extend(loaded_series.into_iter().map(|(_, handle)| handle));
    let next = Arc::new(LocalIndexVersion {
        range_indexes,
        series_indexes,
    });
    if !is_current_region_version(&regions, &region, &version) {
        for handle in newly_built {
            handle.mark_deleted();
        }
        LOCAL_INDEX_RECONCILE_TOTAL
            .with_label_values(&["stale"])
            .inc();
        debug!(
            "Skipped stale local-index publication, worker: {worker_id}, region: {}",
            region.region_id
        );
        return Ok(stats);
    }
    if stats.changed() {
        let mut range_entries = next.range_indexes.keys().copied().collect::<Vec<_>>();
        range_entries.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        series_entries.sort_unstable_by_key(|entry| {
            (
                entry.bucket_start_ms,
                entry.bucket_end_ms,
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
        LOCAL_INDEX_RECONCILE_TOTAL
            .with_label_values(&["stale"])
            .inc();
        debug!(
            "Skipped stale local-index publication after catalog update, worker: {worker_id}, region: {}",
            region.region_id
        );
        return Ok(stats);
    }
    let previous = region.local_index_version_control.publish(next.clone());
    let current_identities = next
        .range_indexes
        .values()
        .chain(next.series_indexes.values())
        .map(LocalIndexFileHandle::identity)
        .collect::<HashSet<_>>();
    for handle in previous
        .range_indexes
        .values()
        .chain(previous.series_indexes.values())
    {
        if !current_identities.contains(&handle.identity()) {
            handle.mark_deleted();
        }
    }
    for handle in retired_handles {
        handle.mark_deleted();
    }
    let result = if stats.changed() { "changed" } else { "noop" };
    LOCAL_INDEX_RECONCILE_TOTAL
        .with_label_values(&[result])
        .inc();
    LOCAL_INDEX_RECONCILE_ELAPSED
        .with_label_values(&["total"])
        .observe(total_start.elapsed().as_secs_f64());
    if stats.changed() {
        info!(
            "Reconciled local-index snapshot, worker: {worker_id}, region: {}, elapsed: {:?}, stats: {:?}",
            region.region_id,
            total_start.elapsed(),
            stats
        );
    } else {
        debug!(
            "Local-index reconciliation made no changes, worker: {worker_id}, region: {}",
            region.region_id
        );
    }
    Ok(stats)
}

fn file_operation(index_type: LocalIndexType, operation: &str, result: &str) {
    LOCAL_INDEX_FILE_OPERATION_TOTAL
        .with_label_values(&[index_type.as_str(), operation, result])
        .inc();
}

async fn purge_file(store: &ObjectStore, request: PurgeRequest) -> bool {
    let path = local_index_path(request.index_type, request.file_id);
    match store.delete(&path).await {
        Ok(()) => {
            file_operation(request.index_type, "delete", "success");
            true
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            file_operation(request.index_type, "delete", "success");
            true
        }
        Err(error) => {
            file_operation(request.index_type, "delete", "failure");
            warn!(error; "Failed to delete local index, index_type: {}, path: {}, phase: deletion, retry: true", request.index_type.as_str(), path);
            false
        }
    }
}

/// Runs one sequential reconciliation task for a region worker.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_local_index_task(
    worker_id: u32,
    store: ObjectStore,
    regions: RegionMapRef,
    state: Arc<LocalIndexTaskState>,
    interval: Duration,
    bucket_width: Duration,
    mut purge_receiver: mpsc::UnboundedReceiver<PurgeRequest>,
    purger: LocalIndexFilePurger,
) {
    info!("Start local-index reconciliation task, worker: {worker_id}");
    let mut retry_purges = Vec::new();
    while state.is_running() {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = state.notified() => {}
            Some(request) = purge_receiver.recv() => {
                if !purge_file(&store, PurgeRequest {
                    index_type: request.index_type,
                    file_id: request.file_id,
                }).await {
                    retry_purges.push(request);
                }
                continue;
            }
        }
        if !state.is_running() {
            break;
        }
        for request in std::mem::take(&mut retry_purges) {
            if !purge_file(
                &store,
                PurgeRequest {
                    index_type: request.index_type,
                    file_id: request.file_id,
                },
            )
            .await
            {
                retry_purges.push(request);
            }
        }
        for region in regions.list_regions() {
            if let Err(error) = reconcile_local_indexes(
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
                LOCAL_INDEX_RECONCILE_TOTAL
                    .with_label_values(&["failure"])
                    .inc();
                warn!(error; "Failed to reconcile local indexes, worker: {worker_id}, region: {}, phase: reconcile, retry: true", region.region_id);
            }
        }
    }
    while let Ok(request) = purge_receiver.try_recv() {
        retry_purges.push(request);
    }
    for request in retry_purges {
        let _ = purge_file(&store, request).await;
    }
    info!("Stop local-index reconciliation task, worker: {worker_id}");
}

pub(crate) fn local_index_channel(
    store: ObjectStore,
) -> (LocalIndexFilePurger, mpsc::UnboundedReceiver<PurgeRequest>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (LocalIndexFilePurger { store, sender }, receiver)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use bytes::Bytes;
    use object_store::ObjectStore;
    use object_store::services::Memory;

    use super::*;
    use crate::sst::file::FileMeta;
    use crate::test_util::new_noop_file_purger;

    fn file(sequence: Option<u64>, level: u8, start_ms: i64, end_ms: i64) -> FileHandle {
        FileHandle::new(
            FileMeta {
                file_id: FileId::random(),
                level,
                sequence: sequence.and_then(NonZeroU64::new),
                time_range: (
                    Timestamp::new_millisecond(start_ms),
                    Timestamp::new_millisecond(end_ms),
                ),
                ..Default::default()
            },
            new_noop_file_purger(),
        )
    }

    #[test]
    fn test_bucket_width_is_rounded_to_compaction_window() {
        assert_eq!(
            Some(300),
            rounded_bucket_width(Duration::from_millis(201), Duration::from_millis(100))
        );
        assert_eq!(
            Some(100),
            rounded_bucket_width(Duration::ZERO, Duration::from_millis(100))
        );
    }

    #[test]
    fn test_bucket_planning_merges_cross_boundary_spans_transitively() {
        let files = vec![
            file(Some(1), 1, 10, 20),
            file(Some(2), 0, 90, 110),
            file(Some(3), 1, 190, 210),
        ];
        let buckets = plan_series_buckets(&files, 100);
        assert_eq!(1, buckets.len());
        assert_eq!(
            (0, 300, 3),
            (
                buckets[0].start_ms,
                buckets[0].end_ms,
                buckets[0].files.len()
            )
        );
    }

    #[test]
    fn test_bucket_planning_keeps_exact_boundary_in_next_bucket() {
        let buckets = plan_series_buckets(&[file(Some(1), 1, 0, 100)], 100);
        assert_eq!((0, 200), (buckets[0].start_ms, buckets[0].end_ms));
    }

    #[test]
    fn test_bucket_planning_includes_levels_and_defers_unknown_sequences() {
        let buckets = plan_series_buckets(
            &[
                file(Some(1), 0, 1, 2),
                file(None, 1, 3, 4),
                file(Some(2), 2, 5, 6),
            ],
            100,
        );
        assert_eq!(1, buckets.len());
        assert!(buckets[0].has_unknown_sequence);
        assert!(series_entry(&buckets[0]).is_none());
    }

    #[test]
    fn test_changed_bucket_layout_does_not_match_existing_coverage() {
        let bucket = SeriesBucket {
            start_ms: 0,
            end_ms: 200,
            files: vec![file(Some(1), 1, 1, 101), file(Some(2), 1, 2, 3)],
            has_unknown_sequence: false,
        };
        let current = series_entry(&bucket).unwrap();
        let mut old = current.clone();
        old.bucket_end_ms = 100;
        assert!(!same_series_coverage(&old, &current));
    }

    #[tokio::test]
    async fn test_old_snapshot_defers_removed_file_deletion() {
        let store = ObjectStore::new(Memory::default()).unwrap().finish();
        let region_file_id = RegionFileId::new(RegionId::new(1, 1), FileId::random());
        let path = local_index_path(LocalIndexType::Range, region_file_id);
        store
            .write(&path, Bytes::from_static(b"index"))
            .await
            .unwrap();
        let (purger, mut receiver) = local_index_channel(store.clone());
        let handle = LocalIndexFileHandle::new(region_file_id, LocalIndexType::Range, purger);
        let control = LocalIndexVersionControl::default();
        control.publish(Arc::new(LocalIndexVersion {
            range_indexes: HashMap::from([(FileId::random(), handle)]),
            series_indexes: HashMap::new(),
        }));
        let held = control.current();
        let removed = control.publish(Arc::new(LocalIndexVersion::default()));
        removed.mark_all_deleted();
        drop(removed);
        assert!(store.stat(&path).await.is_ok());

        drop(held);
        assert!(purge_file(&store, receiver.recv().await.unwrap()).await);
        assert_eq!(
            ErrorKind::NotFound,
            store.stat(&path).await.unwrap_err().kind()
        );
    }
}
