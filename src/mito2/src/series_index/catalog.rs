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

use common_telemetry::warn;
use common_time::Timestamp;
use object_store::{ErrorKind, ObjectStore};
use parquet::file::metadata::KeyValue;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::{FileId, RegionId};

use crate::error::{OpenDalSnafu, Result, SerdeJsonSnafu};
use crate::series_index::purger::IndexFilePurger;
use crate::series_index::version::{
    SeriesIndexFileHandle, SeriesIndexVersion, SeriesIndexVersionControl,
};
const SERIES_DIR: &str = "series";
const RANGE_CATALOG: &str = "range-index.json";
const SERIES_CATALOG: &str = "series-index.json";
const SERIES_METADATA_KEY: &str = "greptime.series_index";

/// Self-describing coverage stored in a series-index Parquet footer.
///
/// A published entry must include every series from every SST in the region
/// contained by its bucket and inclusive file-sequence interval. Query planning
/// relies on this complete-coverage contract, not on `source_file_ids`.
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

impl SeriesIndexEntry {
    /// Whether this index completely covers an SST in the query's sequence domain.
    pub(crate) fn covers_file(
        &self,
        file: &crate::sst::file::FileMeta,
        region_id: RegionId,
    ) -> bool {
        file.region_id == region_id
            && file.time_range.0 <= file.time_range.1
            && self.bucket_start <= file.time_range.0
            && file.time_range.1 < self.bucket_end
            && file.sequence.is_some_and(|sequence| {
                self.min_file_sequence <= sequence.get() && sequence.get() <= self.max_file_sequence
            })
    }
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

pub(crate) fn series_metadata(entry: &SeriesIndexEntry) -> Result<Vec<KeyValue>> {
    Ok(vec![KeyValue::new(
        SERIES_METADATA_KEY.to_string(),
        Some(serde_json::to_string(entry).context(SerdeJsonSnafu)?),
    )])
}

pub(crate) async fn load_catalog<T>(store: &ObjectStore, path: &str) -> Option<T>
where
    T: DeserializeOwned,
{
    let bytes = match store.read(path).await {
        Ok(bytes) => bytes.to_bytes(),
        Err(error) if error.kind() == ErrorKind::NotFound => return None,
        Err(error) => {
            warn!(error; "Failed to load series-index catalog, path: {path}");
            return None;
        }
    };
    match serde_json::from_slice(&bytes) {
        Ok(catalog) => Some(catalog),
        Err(error) => {
            warn!(error; "Invalid series-index catalog, path: {path}, phase: load");
            None
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

/// Best-effort removal of both catalogs when dropping a region.
pub(crate) async fn delete_catalogs(store: &ObjectStore, region_id: RegionId) {
    for path in [
        series_catalog_path(region_id),
        range_catalog_path(region_id),
    ] {
        if let Err(error) = store.delete(&path).await
            && error.kind() != ErrorKind::NotFound
        {
            warn!(error; "Failed to delete index catalog, path: {path}");
        }
    }
}

/// Restores the in-memory snapshot once when opening a region.
pub(crate) async fn load_version_control(
    store: &ObjectStore,
    region_id: RegionId,
    purger: &IndexFilePurger,
) -> SeriesIndexVersionControl {
    let range = load_catalog::<RangeIndexCatalog>(store, &range_catalog_path(region_id))
        .await
        .unwrap_or_default();
    let series = load_catalog::<SeriesIndexCatalog>(store, &series_catalog_path(region_id))
        .await
        .unwrap_or_default();
    // TODO: Handle catalog entries whose index files are missing from storage.
    let version = SeriesIndexVersion {
        range_indexes: range.indexes.into_iter().collect(),
        series_indexes: series
            .indexes
            .into_iter()
            .map(|entry| {
                (
                    entry.index_uuid,
                    SeriesIndexFileHandle::new(region_id, entry, purger.clone()),
                )
            })
            .collect(),
    };
    let control = SeriesIndexVersionControl::default();
    control.publish(std::sync::Arc::new(version));
    control
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_time::Timestamp;
    use object_store::ObjectStore;
    use object_store::layers::mock::{self, MockLayerBuilder};
    use object_store::services::Memory;
    use store_api::storage::{FileId, RegionId};

    use crate::series_index::catalog::{
        SeriesIndexCatalog, SeriesIndexEntry, load_catalog, load_version_control,
        series_catalog_path, series_metadata, store_catalog,
    };
    use crate::series_index::purger::series_index_channel;

    struct FailingCatalogReader;

    impl mock::Read for FailingCatalogReader {
        async fn read(
            &self,
            _range: mock::BytesRange,
        ) -> mock::Result<(mock::RpRead, mock::Buffer)> {
            Err(mock::Error::new(
                mock::ErrorKind::Unexpected,
                "injected catalog read failure",
            ))
        }

        async fn open(
            &self,
            _range: mock::BytesRange,
        ) -> mock::Result<(mock::RpRead, Box<dyn mock::ReadStreamDyn>)> {
            Err(mock::Error::new(
                mock::ErrorKind::Unexpected,
                "injected catalog read failure",
            ))
        }
    }

    #[test]
    fn coverage_uses_exclusive_time_end_and_inclusive_file_sequences() {
        let region_id = RegionId::new(1, 1);
        let entry = SeriesIndexEntry {
            index_uuid: FileId::random(),
            bucket_start: Timestamp::new_second(1),
            bucket_end: Timestamp::new_second(2),
            source_file_ids: Vec::new(),
            min_file_sequence: 2,
            max_file_sequence: 4,
        };
        for (start, end, sequence, own_region, covered) in [
            (1000, 1999, 2, true, true),
            (1000, 1999, 4, true, true),
            (999, 1999, 3, true, false),
            (1000, 2000, 3, true, false),
            (1000, 1999, 1, true, false),
            (1000, 1999, 5, true, false),
            (1000, 1999, 0, true, false),
            (1000, 1999, 3, false, false),
        ] {
            let file = crate::sst::file::FileMeta {
                region_id: if own_region {
                    region_id
                } else {
                    RegionId::new(2, 1)
                },
                time_range: (
                    Timestamp::new_millisecond(start),
                    Timestamp::new_millisecond(end),
                ),
                sequence: std::num::NonZeroU64::new(sequence),
                ..Default::default()
            };
            assert_eq!(covered, entry.covers_file(&file, region_id), "{file:?}");
        }
    }

    #[tokio::test]
    async fn test_load_catalog_returns_none_on_error() {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let path = series_catalog_path(RegionId::new(1, 1));
        // Missing catalog.
        assert!(
            load_catalog::<SeriesIndexCatalog>(&store, &path)
                .await
                .is_none()
        );
        store.write(&path, "invalid").await.unwrap();
        assert!(
            load_catalog::<SeriesIndexCatalog>(&store, &path)
                .await
                .is_none()
        );
        let layer = MockLayerBuilder::default()
            .reader_factory(Arc::new(|_, _, _| Box::new(FailingCatalogReader)))
            .build()
            .unwrap();
        let store = store.layer(layer);
        assert!(
            load_catalog::<SeriesIndexCatalog>(&store, &path)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_catalog_roundtrip() {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let region_id = RegionId::new(1, 1);
        let entry = SeriesIndexEntry {
            index_uuid: FileId::random(),
            bucket_start: Timestamp::new_second(0),
            bucket_end: Timestamp::new_second(100),
            source_file_ids: vec![FileId::random()],
            min_file_sequence: 1,
            max_file_sequence: 2,
        };
        store_catalog(
            &store,
            &series_catalog_path(region_id),
            &SeriesIndexCatalog {
                indexes: vec![entry.clone()],
            },
        )
        .await
        .unwrap();
        let (purger, _receiver) = series_index_channel(store.clone());
        let current = load_version_control(&store, region_id, &purger)
            .await
            .current();
        assert!(current.range_indexes.is_empty());
        assert_eq!(&entry, current.series_indexes[&entry.index_uuid].entry());

        let metadata = series_metadata(&entry).unwrap();
        let decoded: SeriesIndexEntry =
            serde_json::from_str(metadata[0].value.as_ref().unwrap()).unwrap();
        assert_eq!(entry, decoded);
    }
}
