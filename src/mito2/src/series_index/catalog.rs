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

//! Index catalog persistence, coverage metadata, and file paths.

use std::collections::{HashMap, HashSet};

use common_telemetry::warn;
use common_time::Timestamp;
use object_store::{ErrorKind, ObjectStore};
use parquet::file::metadata::KeyValue;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::{FileId, RegionId};

use super::maintenance::ReconcileStats;
use super::purger::{IndexFilePurger, IndexFileType, file_operation};
use super::version::SeriesIndexFileHandle;
use crate::error::{OpenDalSnafu, Result, SerdeJsonSnafu};
use crate::sst::file::RegionFileId;
pub(crate) use crate::sst::range_index::range_index_path;
const SERIES_DIR: &str = "series";
const RANGE_CATALOG: &str = "range-index.json";
const SERIES_CATALOG: &str = "series-index.json";
const SERIES_METADATA_KEY: &str = "greptime.series_index";

/// Self-describing coverage stored in a series-index Parquet footer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SeriesIndexEntry {
    pub(crate) index_uuid: FileId,
    /// Inclusive bucket start.
    pub(crate) bucket_start: Timestamp,
    /// Exclusive bucket end.
    pub(crate) bucket_end: Timestamp,
    pub(crate) source_file_ids: Vec<FileId>,
    pub(crate) min_file_sequence: u64,
    pub(crate) max_file_sequence: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct SeriesIndexCatalog {
    pub(crate) indexes: Vec<SeriesIndexEntry>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct RangeIndexCatalog {
    pub(crate) indexes: Vec<FileId>,
}

pub(crate) fn range_catalog_path(region_id: RegionId) -> String {
    format!("{}/{RANGE_CATALOG}", region_id.as_u64())
}

pub(crate) fn series_index_path(region_id: RegionId, index_uuid: FileId) -> String {
    format!("{}/{SERIES_DIR}/{index_uuid}.parquet", region_id.as_u64())
}

pub(crate) fn series_catalog_path(region_id: RegionId) -> String {
    format!("{}/{SERIES_CATALOG}", region_id.as_u64())
}

pub(crate) fn same_series_coverage(left: &SeriesIndexEntry, right: &SeriesIndexEntry) -> bool {
    left.bucket_start == right.bucket_start
        && left.bucket_end == right.bucket_end
        && left.source_file_ids == right.source_file_ids
        && left.min_file_sequence == right.min_file_sequence
        && left.max_file_sequence == right.max_file_sequence
}

pub(crate) fn series_metadata(entry: &SeriesIndexEntry) -> Result<Vec<KeyValue>> {
    Ok(vec![KeyValue::new(
        SERIES_METADATA_KEY.to_string(),
        Some(serde_json::to_string(entry).context(SerdeJsonSnafu)?),
    )])
}

pub(crate) async fn load_catalog<T>(store: &ObjectStore, path: &str) -> Result<(T, bool)>
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
            warn!(error; "Invalid series-index catalog, path: {path}, phase: load, retry: true");
            Ok((T::default(), true))
        }
    }
}

pub(crate) async fn store_catalog<T>(store: &ObjectStore, path: &str, catalog: &T) -> Result<()>
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

/// Catalog entries can outlive range files deleted on final SST handle release.
pub(crate) async fn load_range_indexes(
    store: &ObjectStore,
    region_id: RegionId,
    catalog: RangeIndexCatalog,
    visible: &HashSet<FileId>,
    stats: &mut ReconcileStats,
) -> Result<HashSet<FileId>> {
    let mut indexes = HashSet::new();
    for file_id in catalog.indexes {
        if visible.contains(&file_id)
            && store
                .exists(&range_index_path(region_id, file_id))
                .await
                .context(OpenDalSnafu)?
        {
            stats.loaded_range += 1;
            file_operation(IndexFileType::Range, "load", "success");
            indexes.insert(file_id);
        } else {
            stats.removed_range += 1;
        }
    }
    Ok(indexes)
}

pub(crate) fn load_series_indexes(
    catalog: SeriesIndexCatalog,
    region_id: RegionId,
    purger: &IndexFilePurger,
    known: &HashMap<RegionFileId, SeriesIndexFileHandle>,
    stats: &mut ReconcileStats,
) -> Vec<(SeriesIndexEntry, SeriesIndexFileHandle)> {
    let mut indexes = Vec::new();
    for entry in catalog.indexes {
        let region_file_id = RegionFileId::new(region_id, entry.index_uuid);
        let handle = known
            .get(&region_file_id)
            .cloned()
            .unwrap_or_else(|| SeriesIndexFileHandle::new(region_file_id, purger.clone()));
        stats.loaded_series += 1;
        file_operation(IndexFileType::Series, "load", "success");
        indexes.push((entry, handle));
    }
    indexes
}
