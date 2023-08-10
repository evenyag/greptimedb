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

//! Repair command for manifests.

use std::collections::HashSet;
use std::str::FromStr;

use async_compat::CompatExt;
use common_datasource::compression::CompressionType;
use common_telemetry::{debug, info};
use common_time::timestamp::TimeUnit;
use common_time::util::current_time_millis;
use common_time::Timestamp;
use datatypes::prelude::ConcreteDataType;
use futures::TryStreamExt;
use object_store::util::join_dir;
use object_store::{util, ObjectStore};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::ParquetMetaData;
use snafu::{whatever, OptionExt, ResultExt};
use storage::manifest::action::{RegionCheckpoint, RegionMetaAction, RegionMetaActionList};
use storage::manifest::storage::ManifestObjectStore;
use storage::schema::StoreSchema;
use storage::sst::{FileId, FileMeta};
use store_api::manifest::{
    Checkpoint, LogIterator, ManifestLogStorage, ManifestVersion, MAX_VERSION,
};
use store_api::storage::RegionId;
use tokio::io::BufReader;

use crate::Result;

/// Request to rebuild one region.
#[derive(Debug)]
pub struct RebuildOneRegion {
    /// Relative dir of the region.
    pub region_dir: String,
}

/// Rebuilder to rebuild manifests from existing SSTs.
#[derive(Debug)]
pub struct ManifestRebuilder {
    object_store: ObjectStore,
    /// Dry run mode.
    dry_run: bool,
}

impl ManifestRebuilder {
    /// Returns a new rebuilder.
    pub fn new(object_store: ObjectStore) -> Self {
        Self {
            object_store,
            dry_run: false,
        }
    }

    /// Set dry run.
    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Rebuild one region's checkpoint.
    pub async fn rebuild_one(&self, req: &RebuildOneRegion) -> Result<()> {
        let manifest_dir = join_dir(&req.region_dir, "manifest");
        let store = ManifestObjectStore::new(
            &manifest_dir,
            self.object_store.clone(),
            CompressionType::Uncompressed,
        );

        let operator = RegionManifestOperator {
            path: manifest_dir.clone(),
            store,
            dry_run: self.dry_run,
        };

        let Some((checkpoint_version, checkpoint)) = operator.load_last_checkpoint().await? else {
            // No checkpoint, don't need to rebuild.
            info!("no checkpoint to rebuild, manifest_dir: {}", manifest_dir);
            return Ok(());
        };

        debug!("load checkpoint to rebuild, manifest_dir: {}, checkpoint_version: {}, checkpoint: {:?}", manifest_dir, checkpoint_version, checkpoint);
        assert_eq!(checkpoint_version, checkpoint.last_version);

        let start_version = checkpoint.last_version + 1;
        let action_lists = operator.scan_actions(start_version).await?;

        let Some(region_id) = region_id_from_actions(&checkpoint, &action_lists) else {
            whatever!("No region id in {manifest_dir}");
        };

        let file_names = list_ssts(&self.object_store, &req.region_dir).await?;
        let file_ids = parse_file_names(&file_names);

        let Some(diff) = diff_checkpoint(&checkpoint, &file_ids, &action_lists) else {
            info!("checkpoint of region {} has no data", region_id);
            return Ok(());
        };

        info!("Region {} checkpoint diff is {:?}", region_id, diff);

        let meta_to_add = diff
            .detect_file_meta_to_add(&self.object_store, &req.region_dir, region_id)
            .await?;

        info!("Region {} meta to add is {:?}", region_id, meta_to_add);

        // We have diff so checkpoint is not None.
        let mut checkpoint = checkpoint;
        let version = checkpoint
            .checkpoint
            .as_mut()
            .unwrap()
            .version
            .as_mut()
            .unwrap();
        // Remove files.
        for file_id in &diff.files_to_remove {
            version.files.remove(file_id);
        }
        for meta in meta_to_add {
            version.files.insert(meta.file_id, meta);
        }

        // Backup old checkpoint.
        let checkpoint_path = operator.checkpoint_path(checkpoint_version);
        self.backup_file(&checkpoint_path).await?;

        // Overwrite checkpoint.
        operator.save_checkpoint(&checkpoint).await?;

        Ok(())
    }

    /// Backup a file.
    async fn backup_file(&self, path: &str) -> Result<()> {
        let current_millis = current_time_millis();
        let to_path = format!("{path}.{current_millis}.backup");

        info!("Copy {} to {}", path, to_path);

        if self.dry_run {
            return Ok(());
        }

        self.object_store
            .copy(path, &to_path)
            .await
            .whatever_context("copy")
    }
}

/// Struct to operate region manifest.
struct RegionManifestOperator {
    /// Manifest path.
    path: String,
    /// Manifest store.
    ///
    /// We use store directly to avoid starting background tasks.
    store: ManifestObjectStore,
    /// Dry run mode.
    dry_run: bool,
}

impl RegionManifestOperator {
    /// Checkpoint path
    fn checkpoint_path(&self, version: ManifestVersion) -> String {
        self.store.checkpoint_file_path(version)
    }

    /// Load last checkpoint from store.
    async fn load_last_checkpoint(&self) -> Result<Option<(ManifestVersion, RegionCheckpoint)>> {
        let last_checkpoint = self
            .store
            .load_last_checkpoint()
            .await
            .whatever_context("load last checkpoint")?;

        if let Some((version, bytes)) = last_checkpoint {
            let checkpoint = decode_checkpoint(&bytes)?;
            assert!(checkpoint.last_version() >= version);
            Ok(Some((version, checkpoint)))
        } else {
            Ok(None)
        }
    }

    /// Scan meta actions.
    async fn scan_actions(&self, start: ManifestVersion) -> Result<Vec<RegionMetaActionList>> {
        let mut action_lists = Vec::new();

        let mut entries = self
            .store
            .scan(start, MAX_VERSION)
            .await
            .whatever_context("scan actions")?;
        while let Some((_version, bytes)) = entries.next_log().await.whatever_context("next log")? {
            let action_list = decode_action_list(&bytes)?;
            action_lists.push(action_list);
        }

        Ok(action_lists)
    }

    /// Save checkpoint to path of given version.
    async fn save_checkpoint(&self, checkpoint: &RegionCheckpoint) -> Result<()> {
        info!("Save checkpoint {:?} under {}", checkpoint, self.path);

        if self.dry_run {
            return Ok(());
        }

        let bytes = checkpoint.encode().whatever_context("encode checkpoint")?;
        self.store
            .save_checkpoint(checkpoint.last_version, &bytes)
            .await
            .whatever_context("save checkpoint")
    }
}

/// Decode checkpoint directly.
fn decode_checkpoint(bs: &[u8]) -> Result<RegionCheckpoint> {
    let s = std::str::from_utf8(bs).whatever_context("not a valid UTF-8")?;
    let checkpoint: RegionCheckpoint = serde_json::from_str(s).whatever_context("decode json")?;

    Ok(checkpoint)
}

/// Decode action list directly.
fn decode_action_list(bs: &[u8]) -> Result<RegionMetaActionList> {
    use std::io::{BufRead, BufReader};

    let mut lines = BufReader::new(bs).lines();
    // Skip header.
    lines
        .next()
        .whatever_context("empty header")?
        .whatever_context("invalid header")?;
    let mut actions = Vec::new();
    for line in lines {
        let line = line.whatever_context("invalid line")?;
        let action: RegionMetaAction =
            serde_json::from_str(&line).whatever_context("parse json action")?;
        if let RegionMetaAction::Protocol(_) = &action {
            continue;
        }

        actions.push(action);
    }

    Ok(RegionMetaActionList {
        actions,
        prev_version: 0,
    })
}

/// List parquet SSTs under `sst_dir`.
async fn list_ssts(object_store: &ObjectStore, sst_dir: &str) -> Result<Vec<String>> {
    let mut entries = object_store.list(sst_dir).await.whatever_context("list")?;
    let mut file_names = Vec::new();
    while let Some(entry) = entries.try_next().await.whatever_context("next entry")? {
        if entry.name().ends_with(".parquet") {
            file_names.push(entry.name().to_string());
        }
    }

    Ok(file_names)
}

/// Parse file names into file ids.
fn parse_file_names(file_names: &[String]) -> Vec<FileId> {
    file_names
        .iter()
        .filter_map(|name| name.split('.').next())
        .filter_map(|name| FileId::from_str(name).ok())
        .collect()
}

/// Diff of checkpoint and SSTs.
#[derive(Debug, Default)]
struct CheckpointDiff {
    /// Files to remove from checkpoint.
    files_to_remove: HashSet<FileId>,
    /// Files both in checkpoint and data directory.
    files_to_keep: HashSet<FileId>,
    /// Files in data directory but not in checkpoint.
    files_to_add: HashSet<FileId>,
}

impl CheckpointDiff {
    /// Detect file meta from SSTs to add.
    async fn detect_file_meta_to_add(
        &self,
        object_store: &ObjectStore,
        sst_dir: &str,
        region_id: RegionId,
    ) -> Result<Vec<FileMeta>> {
        let mut file_metas = Vec::with_capacity(self.files_to_add.len());
        for file_id in &self.files_to_add {
            let file_path = util::join_path(sst_dir, &format!("{file_id}.parquet"));
            // Get file size.
            let object_meta = object_store
                .stat(&file_path)
                .await
                .whatever_context("stat")?;
            let file_size = object_meta.content_length();

            let reader = object_store
                .reader(&file_path)
                .await
                .whatever_context("read file")?
                .compat();
            let buf_reader = BufReader::new(reader);
            let builder = ParquetRecordBatchStreamBuilder::new(buf_reader)
                .await
                .whatever_context("build parquet reader")?;
            let parquet_meta = builder.metadata();
            let arrow_schema = builder.schema().clone();
            let store_schema =
                StoreSchema::try_from(arrow_schema).whatever_context("to store schema")?;

            let time_range = decode_timestamp_range(&parquet_meta, store_schema.schema())
                .whatever_context("decode time range")?;
            let file_meta = FileMeta {
                region_id,
                file_id: *file_id,
                time_range,
                level: 0,
                file_size,
            };
            file_metas.push(file_meta);
        }

        Ok(file_metas)
    }
}

/// Get region id from meta actions and checkpoint.
fn region_id_from_actions(
    checkpoint: &RegionCheckpoint,
    action_lists: &[RegionMetaActionList],
) -> Option<RegionId> {
    if let Some(checkpoint_data) = &checkpoint.checkpoint {
        return Some(checkpoint_data.metadata.id);
    }

    for action_list in action_lists {
        for action in &action_list.actions {
            if let RegionMetaAction::Change(change) = action {
                return Some(change.metadata.id);
            }
        }
    }

    None
}

/// Diff checkpoint and SSTs in data directory.
fn diff_checkpoint(
    checkpoint: &RegionCheckpoint,
    file_ids: &[FileId],
    remaining_actions: &[RegionMetaActionList],
) -> Option<CheckpointDiff> {
    let mut sst_files: HashSet<_> = HashSet::from_iter(file_ids.iter().copied());
    // Filter sst files with remaining actions.
    for action_list in remaining_actions {
        for action in &action_list.actions {
            if let RegionMetaAction::Edit(edit) = action {
                for meta in &edit.files_to_remove {
                    sst_files.remove(&meta.file_id);
                }
            }
        }
    }

    let Some(checkpoint_data) = &checkpoint.checkpoint else {
        return None;
    };

    let Some(version) = &checkpoint_data.version else {
        return None;
    };

    let mut diff = CheckpointDiff::default();
    for file_id in version.files.keys() {
        if sst_files.contains(&file_id) {
            // SST exists.
            diff.files_to_keep.insert(*file_id);
        } else {
            diff.files_to_remove.insert(*file_id);
        }
    }
    for file_id in &sst_files {
        if !version.files.contains_key(file_id) {
            diff.files_to_add.insert(*file_id);
        }
    }

    Some(diff)
}

fn decode_timestamp_range(
    file_meta: &ParquetMetaData,
    schema: &datatypes::schema::SchemaRef,
) -> Result<Option<(Timestamp, Timestamp)>> {
    let (Some(ts_col_idx), Some(ts_col)) = (schema.timestamp_index(), schema.timestamp_column())
    else {
        return Ok(None);
    };
    let ts_datatype = &ts_col.data_type;
    decode_timestamp_range_inner(file_meta, ts_col_idx, ts_datatype)
}

fn decode_timestamp_range_inner(
    file_meta: &ParquetMetaData,
    ts_index: usize,
    ts_datatype: &ConcreteDataType,
) -> Result<Option<(Timestamp, Timestamp)>> {
    let mut start = i64::MAX;
    let mut end = i64::MIN;

    let unit = match ts_datatype {
        ConcreteDataType::Int64(_) => TimeUnit::Millisecond,
        ConcreteDataType::Timestamp(type_) => type_.unit(),
        _ => {
            whatever!("Unexpected timestamp column datatype: {ts_datatype:?}");
        }
    };

    for rg in file_meta.row_groups() {
        let Some(stats) = rg
            .columns()
            .get(ts_index)
            .whatever_context("get ts column")?
            .statistics()
        else {
            return Ok(None);
        };
        if !stats.has_min_max_set() {
            return Ok(None);
        }
        let (min_value, max_value) = (stats.min_bytes(), stats.max_bytes());

        // according to [parquet's spec](https://parquet.apache.org/docs/file-format/data-pages/encodings/), min/max value in stats uses plain encoding with little endian.
        // also see https://github.com/apache/arrow-rs/blob/5fb337db04a1a19f7d40da46f19b7b5fd4051593/parquet/src/file/statistics.rs#L172
        let min = i64::from_le_bytes(
            min_value[..8]
                .try_into()
                .whatever_context("Failed to decode min value from stats")?,
        );
        let max = i64::from_le_bytes(
            max_value[..8]
                .try_into()
                .whatever_context("Failed to decode max value from stats")?,
        );
        start = start.min(min);
        end = end.max(max);
    }

    assert!(
        start <= end,
        "Illegal timestamp range decoded from SST file {:?}, start: {}, end: {}",
        file_meta,
        start,
        end
    );
    Ok(Some((
        Timestamp::new(start, unit),
        Timestamp::new(end, unit),
    )))
}
