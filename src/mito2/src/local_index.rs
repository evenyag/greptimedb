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

use std::collections::HashSet;
use std::sync::Arc;

use common_telemetry::warn;
use object_store::{ErrorKind, ObjectStore};
use snafu::ResultExt;
use store_api::storage::{FileId, RegionId};

use crate::error::{OpenDalSnafu, Result};
use crate::read::read_columns::ReadColumns;
use crate::read::series_candidate::is_sparse_metric_metadata;
use crate::region::version::VersionRef;
use crate::region::{MitoRegionRef, RegionMapRef};
use crate::sst::range_index::{
    SstRangeIndexSearcher, SstRangeIndexWriter, SstRangeIndexWriterOptions,
};

pub(crate) const RANGE_DIR: &str = "range";

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_index_layout() {
        let region_id = RegionId::new(42, 7);
        let file_id = FileId::random();
        assert_eq!(
            format!("42_0000000007/range/{file_id}.parquet"),
            range_index_path(region_id, file_id)
        );
    }
}
