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

//! Immutable index snapshots and aggregate series-file handles.

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use store_api::storage::FileId;

use super::purger::{IndexFilePurger, IndexFileType, PurgeRequest};
use crate::sst::file::RegionFileId;

/// A reference-counted series-index file with deferred deletion semantics.
#[derive(Clone)]
pub(crate) struct SeriesIndexFileHandle {
    inner: Arc<SeriesIndexFileHandleInner>,
}

impl Debug for SeriesIndexFileHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeriesIndexFileHandle")
            .field("file_id", &self.inner.file_id)
            .field("deleted", &self.inner.deleted.load(Ordering::Relaxed))
            .finish()
    }
}

impl SeriesIndexFileHandle {
    pub(super) fn new(file_id: RegionFileId, purger: IndexFilePurger) -> Self {
        Self {
            inner: Arc::new(SeriesIndexFileHandleInner {
                file_id,
                deleted: AtomicBool::new(false),
                purger,
            }),
        }
    }

    pub(super) fn identity(&self) -> RegionFileId {
        self.inner.file_id
    }

    pub(crate) fn mark_deleted(&self) {
        self.inner.deleted.store(true, Ordering::Release);
    }
}

struct SeriesIndexFileHandleInner {
    file_id: RegionFileId,
    deleted: AtomicBool,
    purger: IndexFilePurger,
}

impl Drop for SeriesIndexFileHandleInner {
    fn drop(&mut self) {
        if self.deleted.load(Ordering::Acquire) {
            self.purger.purge(PurgeRequest {
                index_type: IndexFileType::Series,
                file_id: self.file_id,
            });
        }
    }
}

/// Immutable series-index snapshot for one region.
#[derive(Debug, Default)]
pub(crate) struct SeriesIndexVersion {
    pub(crate) range_indexes: HashSet<FileId>,
    pub(crate) series_indexes: HashMap<FileId, SeriesIndexFileHandle>,
}

impl SeriesIndexVersion {
    fn mark_all_deleted(&self) {
        self.series_indexes
            .values()
            .for_each(SeriesIndexFileHandle::mark_deleted);
    }
}

/// Copy-on-write series-index snapshots owned by a region.
#[derive(Debug, Default)]
pub(crate) struct SeriesIndexVersionControl {
    current: RwLock<Arc<SeriesIndexVersion>>,
}

impl SeriesIndexVersionControl {
    pub(crate) fn current(&self) -> Arc<SeriesIndexVersion> {
        self.current.read().unwrap().clone()
    }

    pub(super) fn publish(&self, next: Arc<SeriesIndexVersion>) -> Arc<SeriesIndexVersion> {
        std::mem::replace(&mut *self.current.write().unwrap(), next)
    }

    pub(crate) fn mark_dropped(&self) {
        self.publish(Arc::new(SeriesIndexVersion::default()))
            .mark_all_deleted();
    }
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStore;
    use object_store::services::Memory;
    use store_api::storage::RegionId;

    use super::super::catalog::series_index_path;
    use super::super::purger::{purge_file, series_index_channel};
    use super::*;

    #[tokio::test]
    async fn test_snapshot_pins_retired_series_index() {
        let store = ObjectStore::new(Memory::default()).unwrap().finish();
        let id = RegionFileId::new(RegionId::new(1, 1), FileId::random());
        let path = series_index_path(id.region_id(), id.file_id());
        store.write(&path, "index").await.unwrap();
        let (purger, mut receiver) = series_index_channel(store.clone());
        let control = SeriesIndexVersionControl::default();
        control.publish(Arc::new(SeriesIndexVersion {
            series_indexes: HashMap::from([(id.file_id(), SeriesIndexFileHandle::new(id, purger))]),
            ..Default::default()
        }));
        let held = control.current();
        control.mark_dropped();
        assert!(receiver.try_recv().is_err());
        assert!(store.exists(&path).await.unwrap());
        drop(held);
        assert!(purge_file(&store, receiver.try_recv().unwrap()).await);
        assert!(!store.exists(&path).await.unwrap());
    }
}
