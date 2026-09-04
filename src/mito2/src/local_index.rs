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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_stream::try_stream;
use common_telemetry::warn;
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit};
use futures::TryStreamExt;
use object_store::{ErrorKind, ObjectStore};
use parquet::file::metadata::KeyValue;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{FileId, RegionId};
use table::predicate::Predicate;

use crate::error::{OpenDalSnafu, Result, UnexpectedSnafu};
use crate::read::BoxedRecordBatchStream;
use crate::read::flat_dedup::{FlatDedupReader, FlatLastRow};
use crate::read::flat_merge::FlatMergeReader;
use crate::read::read_columns::ReadColumns;
use crate::read::series_candidate::is_sparse_metric_metadata;
use crate::region::version::VersionRef;
use crate::region::{MitoRegionRef, RegionMapRef};
use crate::series_index::{SeriesIndexSearcher, SeriesIndexWriter, SeriesIndexWriterOptions};
use crate::sst::file::FileHandle;
use crate::sst::range_index::{
    SstRangeIndexSearcher, SstRangeIndexWriter, SstRangeIndexWriterOptions,
};

pub(crate) const RANGE_DIR: &str = "range";
pub(crate) const SERIES_DIR: &str = "series";
pub(crate) const SERIES_CATALOG: &str = "series-index.json";
const SERIES_FORMAT_VERSION: u32 = 1;
const SERIES_SCHEMA_VERSION: u32 = 1;
const META_PREFIX: &str = "greptime.local_series_index.";

/// Complete catalog of disposable series indexes for one region.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SeriesIndexCatalog {
    #[serde(default = "catalog_version")]
    pub(crate) version: u32,
    #[serde(default)]
    pub(crate) indexes: Vec<SeriesIndexEntry>,
}

fn catalog_version() -> u32 {
    1
}

impl Default for SeriesIndexCatalog {
    fn default() -> Self {
        Self {
            version: catalog_version(),
            indexes: Vec::new(),
        }
    }
}

/// Coverage of one local series-index file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SeriesIndexEntry {
    pub(crate) index_uuid: FileId,
    /// Inclusive bucket start in Unix milliseconds.
    pub(crate) bucket_start_ms: i64,
    /// Exclusive bucket end in Unix milliseconds.
    pub(crate) bucket_end_ms: i64,
    /// Inclusive source-file sequence interval.
    pub(crate) min_file_sequence: u64,
    /// Inclusive source-file sequence interval.
    pub(crate) max_file_sequence: u64,
    pub(crate) format_version: u32,
    pub(crate) schema_version: u32,
}

#[derive(Debug, Clone)]
struct SeriesBuildPlan {
    entry: SeriesIndexEntry,
    files: Vec<FileHandle>,
}

/// Result sent back to the owning region worker.
#[derive(Debug)]
pub(crate) struct LocalIndexReconcileFinished {
    pub(crate) generation: u64,
}

pub(crate) fn range_index_path(region_id: RegionId, file_id: FileId) -> String {
    format!("{region_id}/{RANGE_DIR}/{file_id}.parquet")
}

fn range_index_dir(region_id: RegionId) -> String {
    format!("{region_id}/{RANGE_DIR}/")
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

/// Reconciles all range indexes for one immutable region version.
pub(crate) async fn reconcile_range_indexes(
    store: ObjectStore,
    regions: RegionMapRef,
    region: MitoRegionRef,
    version: VersionRef,
) -> Result<()> {
    if !is_sparse_metric_metadata(&version.metadata) {
        return Ok(());
    }

    let files = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .cloned()
        .collect::<Vec<_>>();
    let visible = files
        .iter()
        .map(|file| file.file_id().file_id())
        .collect::<HashSet<_>>();

    for file in files {
        let file_id = file.file_id().file_id();
        let target = range_index_path(region.region_id, file_id);
        if SstRangeIndexSearcher::open(store.clone(), &target)
            .await
            .is_ok()
        {
            continue;
        }

        let temporary = format!("{}.building-{}", target, FileId::random());
        let mut writer = SstRangeIndexWriter::try_new(
            version.metadata.clone(),
            store.clone(),
            &temporary,
            SstRangeIndexWriterOptions::default(),
        )
        .await?;
        let mut reader = match region
            .access_layer
            .read_sst(file)
            .projection(Some(ReadColumns::new([])))
            .build()
            .await?
        {
            Some(reader) => reader,
            None => {
                writer.abort().await?;
                continue;
            }
        };

        while let Some((row_group_id, batch)) = reader.next_record_batch_with_row_group().await? {
            if let Err(error) = writer.write(row_group_id as u32, &batch).await {
                let _ = writer.abort().await;
                return Err(error);
            }
        }
        writer.finish().await?;

        if !is_current_region_version(&regions, &region, &version) {
            let _ = store.delete(&temporary).await;
            return Ok(());
        }
        store
            .rename(&temporary, &target)
            .await
            .context(OpenDalSnafu)?;
        SstRangeIndexSearcher::open(store.clone(), &target).await?;
    }

    if !is_current_region_version(&regions, &region, &version) {
        return Ok(());
    }
    let dir = range_index_dir(region.region_id);
    let entries = match store.list(&dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context(OpenDalSnafu),
    };
    for entry in entries {
        if entry.metadata().is_dir() {
            continue;
        }
        let path = entry.path();
        let Some(name) = path.rsplit('/').next() else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".parquet") else {
            continue;
        };
        let Ok(file_id) = FileId::parse_str(stem) else {
            continue;
        };
        if !visible.contains(&file_id)
            && let Err(error) = store.delete(path).await
        {
            warn!(error; "Failed to remove stale local range index {path}");
        }
    }

    Ok(())
}

fn series_catalog_path(region_id: RegionId) -> String {
    format!("{region_id}/{SERIES_CATALOG}")
}

fn series_index_path(region_id: RegionId, index_uuid: FileId) -> String {
    format!("{region_id}/{SERIES_DIR}/{index_uuid}.parquet")
}

fn timestamp_millis(timestamp: Timestamp) -> Option<i64> {
    timestamp
        .convert_to(TimeUnit::Millisecond)
        .map(|timestamp| timestamp.value())
}

fn plan_series_indexes(
    files: &[FileHandle],
    existing: &[SeriesIndexEntry],
    bucket_width: Duration,
    now_ms: i64,
) -> Vec<SeriesBuildPlan> {
    let width_ms = i64::try_from(bucket_width.as_millis())
        .unwrap_or(i64::MAX)
        .max(1);
    let active_start = now_ms.div_euclid(width_ms) * width_ms;
    let mut buckets = BTreeMap::<i64, Vec<FileHandle>>::new();

    for file in files {
        if file.meta_ref().sequence.is_none() {
            continue;
        }
        let Some(start_ms) = timestamp_millis(file.time_range().0) else {
            continue;
        };
        let Some(end_ms) = timestamp_millis(file.time_range().1) else {
            continue;
        };
        let first = start_ms.div_euclid(width_ms) * width_ms;
        let last = end_ms.div_euclid(width_ms) * width_ms;
        let mut bucket = first;
        loop {
            buckets.entry(bucket).or_default().push(file.clone());
            if bucket >= last {
                break;
            }
            let Some(next) = bucket.checked_add(width_ms) else {
                break;
            };
            bucket = next;
        }
    }

    let mut plans = Vec::new();
    for (bucket_start_ms, files) in buckets {
        let mut groups = BTreeMap::<u64, Vec<FileHandle>>::new();
        for file in files {
            let Some(sequence) = file.meta_ref().sequence else {
                continue;
            };
            groups.entry(sequence.get()).or_default().push(file);
        }

        let mut eligible = Vec::new();
        for (sequence, group) in groups {
            // The current/future bucket only consumes the contiguous L1 prefix.
            if bucket_start_ms >= active_start && group.iter().any(|file| file.level() == 0) {
                break;
            }
            eligible.push((sequence, group));
        }

        let covered = existing
            .iter()
            .filter(|entry| entry.bucket_start_ms == bucket_start_ms)
            .map(|entry| (entry.min_file_sequence, entry.max_file_sequence))
            .collect::<Vec<_>>();
        let mut run = Vec::new();
        for (sequence, group) in eligible {
            if covered
                .iter()
                .any(|(min, max)| sequence >= *min && sequence <= *max)
            {
                append_series_plan(&mut plans, bucket_start_ms, width_ms, &mut run);
            } else {
                run.push((sequence, group));
            }
        }
        append_series_plan(&mut plans, bucket_start_ms, width_ms, &mut run);
    }
    plans
}

fn append_series_plan(
    plans: &mut Vec<SeriesBuildPlan>,
    bucket_start_ms: i64,
    width_ms: i64,
    run: &mut Vec<(u64, Vec<FileHandle>)>,
) {
    let file_count = run.iter().map(|(_, files)| files.len()).sum::<usize>();
    if file_count < 2 {
        run.clear();
        return;
    }
    let Some(min_file_sequence) = run.first().map(|group| group.0) else {
        return;
    };
    let Some(max_file_sequence) = run.last().map(|group| group.0) else {
        return;
    };
    let files = run.drain(..).flat_map(|(_, files)| files).collect();
    plans.push(SeriesBuildPlan {
        entry: SeriesIndexEntry {
            index_uuid: FileId::random(),
            bucket_start_ms,
            bucket_end_ms: bucket_start_ms.saturating_add(width_ms),
            min_file_sequence,
            max_file_sequence,
            format_version: SERIES_FORMAT_VERSION,
            schema_version: SERIES_SCHEMA_VERSION,
        },
        files,
    });
}

fn timestamp_literal(timestamp_ms: i64, unit: TimeUnit) -> Option<ScalarValue> {
    let timestamp = Timestamp::new_millisecond(timestamp_ms).convert_to(unit)?;
    Some(match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(timestamp.value()), None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(timestamp.value()), None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(timestamp.value()), None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(timestamp.value()), None),
    })
}

fn series_metadata(entry: &SeriesIndexEntry) -> Vec<KeyValue> {
    [
        ("index_uuid", entry.index_uuid.to_string()),
        ("bucket_start_ms", entry.bucket_start_ms.to_string()),
        ("bucket_end_ms", entry.bucket_end_ms.to_string()),
        ("min_file_sequence", entry.min_file_sequence.to_string()),
        ("max_file_sequence", entry.max_file_sequence.to_string()),
        ("format_version", entry.format_version.to_string()),
        ("schema_version", entry.schema_version.to_string()),
    ]
    .into_iter()
    .map(|(key, value)| KeyValue::new(format!("{META_PREFIX}{key}"), Some(value)))
    .collect()
}

async fn build_series_index(
    store: &ObjectStore,
    regions: &RegionMapRef,
    region: &MitoRegionRef,
    version: &VersionRef,
    plan: &SeriesBuildPlan,
) -> Result<bool> {
    let metadata = region.metadata();
    let time_index = metadata.time_index_column();
    let unit = time_index
        .column_schema
        .data_type
        .as_timestamp()
        .map(|timestamp| timestamp.unit())
        .unwrap_or(TimeUnit::Millisecond);
    let start = timestamp_literal(plan.entry.bucket_start_ms, unit).context(UnexpectedSnafu {
        reason: "local series-index bucket start does not fit the time-index unit",
    })?;
    let end = timestamp_literal(plan.entry.bucket_end_ms, unit).context(UnexpectedSnafu {
        reason: "local series-index bucket end does not fit the time-index unit",
    })?;
    let predicate = Predicate::new(vec![
        col(&time_index.column_schema.name).gt_eq(lit(start)),
        col(&time_index.column_schema.name).lt(lit(end)),
    ]);

    let mut sources = Vec::<BoxedRecordBatchStream>::new();
    let mut schema = None;
    for file in &plan.files {
        let Some(mut reader) = region
            .access_layer
            .read_sst(file.clone())
            .projection(Some(ReadColumns::new([])))
            .predicate(Some(predicate.clone()))
            .build()
            .await?
        else {
            continue;
        };
        let Some(first) = reader.next_record_batch().await? else {
            continue;
        };
        schema.get_or_insert_with(|| first.schema());
        sources.push(Box::pin(try_stream! {
            yield first;
            while let Some(batch) = reader.next_record_batch().await? {
                yield batch;
            }
        }));
    }
    let Some(schema) = schema else {
        return Ok(false);
    };

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
    let mut visible: BoxedRecordBatchStream = if version.options.append_mode {
        merged
    } else {
        Box::pin(FlatDedupReader::new(merged, FlatLastRow::new(true), None).into_stream())
    };

    let target = series_index_path(region.region_id, plan.entry.index_uuid);
    let temporary = format!("{}.building-{}", target, FileId::random());
    let mut writer = SeriesIndexWriter::try_new_with_key_value_metadata(
        metadata.clone(),
        store.clone(),
        &temporary,
        SeriesIndexWriterOptions::default(),
        Some(series_metadata(&plan.entry)),
    )
    .await?;
    while let Some(batch) = visible.try_next().await? {
        if let Err(error) = writer.write(&batch).await {
            let _ = writer.abort().await;
            return Err(error);
        }
    }
    writer.finish().await?;
    if !is_current_region_version(regions, region, version) {
        let _ = store.delete(&temporary).await;
        return Ok(false);
    }
    store
        .rename(&temporary, &target)
        .await
        .context(OpenDalSnafu)?;
    let searcher = SeriesIndexSearcher::try_new(metadata, store.clone(), None, None)?;
    let mut result = searcher.search(&target).await?;
    let _ = result.try_next().await?;
    Ok(true)
}

async fn load_series_catalog(
    store: &ObjectStore,
    region_id: RegionId,
) -> Result<SeriesIndexCatalog> {
    let path = series_catalog_path(region_id);
    let bytes = match store.read(&path).await {
        Ok(bytes) => bytes.to_bytes(),
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Default::default()),
        Err(error) => return Err(error).context(OpenDalSnafu),
    };
    serde_json::from_slice(&bytes).context(crate::error::SerdeJsonSnafu)
}

async fn store_series_catalog(
    store: &ObjectStore,
    region_id: RegionId,
    catalog: &SeriesIndexCatalog,
) -> Result<()> {
    let path = series_catalog_path(region_id);
    let temporary = format!("{}.building-{}", path, FileId::random());
    let bytes = serde_json::to_vec_pretty(catalog).context(crate::error::SerdeJsonSnafu)?;
    store.write(&temporary, bytes).await.context(OpenDalSnafu)?;
    store.rename(&temporary, &path).await.context(OpenDalSnafu)
}

/// Adds missing series coverage after range-index reconciliation.
pub(crate) async fn reconcile_series_indexes(
    store: ObjectStore,
    regions: RegionMapRef,
    region: MitoRegionRef,
    version: VersionRef,
    bucket_width: Duration,
    now_ms: i64,
) -> Result<()> {
    if !is_sparse_metric_metadata(&version.metadata) {
        return Ok(());
    }
    let mut catalog = load_series_catalog(&store, region.region_id).await?;
    let files = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .cloned()
        .collect::<Vec<_>>();
    let plans = plan_series_indexes(&files, &catalog.indexes, bucket_width, now_ms);
    for plan in plans {
        if !build_series_index(&store, &regions, &region, &version, &plan).await? {
            return Ok(());
        }
        catalog.indexes.push(plan.entry);
    }
    catalog.indexes.sort_by_key(|entry| {
        (
            entry.bucket_start_ms,
            entry.min_file_sequence,
            entry.max_file_sequence,
        )
    });
    if is_current_region_version(&regions, &region, &version) {
        store_series_catalog(&store, region.region_id, &catalog).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::sst::file::FileMeta;
    use crate::test_util::new_noop_file_purger;

    #[test]
    fn test_range_index_layout() {
        let region_id = RegionId::new(42, 7);
        let file_id = FileId::random();
        assert_eq!(
            format!("42_0000000007/range/{file_id}.parquet"),
            range_index_path(region_id, file_id)
        );
    }

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

    fn entry(min: u64, max: u64) -> SeriesIndexEntry {
        SeriesIndexEntry {
            index_uuid: FileId::random(),
            bucket_start_ms: 0,
            bucket_end_ms: 100,
            min_file_sequence: min,
            max_file_sequence: max,
            format_version: 1,
            schema_version: 1,
        }
    }

    #[test]
    fn test_plan_closed_bucket_includes_all_levels_and_skips_unknown_sequence() {
        let files = vec![
            file(Some(1), 0, 1, 2),
            file(None, 0, 1, 2),
            file(Some(2), 1, 1, 2),
        ];
        let plans = plan_series_indexes(&files, &[], Duration::from_millis(100), 200);
        assert_eq!(1, plans.len());
        assert_eq!(2, plans[0].files.len());
        assert_eq!(1, plans[0].entry.min_file_sequence);
        assert_eq!(2, plans[0].entry.max_file_sequence);
    }

    #[test]
    fn test_plan_active_bucket_stops_before_indivisible_l0_group() {
        let files = vec![
            file(Some(1), 1, 1, 2),
            file(Some(1), 1, 1, 2),
            file(Some(2), 1, 1, 2),
            file(Some(3), 0, 1, 2),
            file(Some(3), 1, 1, 2),
            file(Some(4), 1, 1, 2),
        ];
        let plans = plan_series_indexes(&files, &[], Duration::from_millis(100), 50);
        assert_eq!(1, plans.len());
        assert_eq!(3, plans[0].files.len());
        assert_eq!(1, plans[0].entry.min_file_sequence);
        assert_eq!(2, plans[0].entry.max_file_sequence);
    }

    #[test]
    fn test_plan_keeps_non_overlapping_indexes_in_one_bucket() {
        let files = (1..=5)
            .map(|sequence| file(Some(sequence), 1, 1, 2))
            .collect::<Vec<_>>();
        let plans = plan_series_indexes(&files, &[entry(3, 3)], Duration::from_millis(100), 200);
        assert_eq!(2, plans.len());
        assert_eq!(
            (1, 2),
            (
                plans[0].entry.min_file_sequence,
                plans[0].entry.max_file_sequence
            )
        );
        assert_eq!(
            (4, 5),
            (
                plans[1].entry.min_file_sequence,
                plans[1].entry.max_file_sequence
            )
        );
    }
}
