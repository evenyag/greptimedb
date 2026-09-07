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

//! Deferred index deletion and SST range-index lifecycle integration.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use common_telemetry::warn;
use object_store::{ErrorKind, ObjectStore};
use store_api::storage::RegionId;
use tokio::sync::mpsc;

use super::catalog::index_file_path;
use crate::metrics::SERIES_INDEX_FILE_OPERATION_TOTAL;
use crate::sst::file::{FileMeta, RegionFileId};
use crate::sst::file_purger::{FilePurger, FilePurgerRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum IndexFileType {
    Range,
    Series,
}

impl IndexFileType {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Range => "range",
            Self::Series => "series",
        }
    }
}

#[derive(Debug)]
pub(crate) struct PurgeRequest {
    pub(super) index_type: IndexFileType,
    pub(super) file_id: RegionFileId,
}

#[derive(Clone)]
pub(crate) struct IndexFilePurger {
    store: ObjectStore,
    sender: mpsc::UnboundedSender<PurgeRequest>,
}

impl Debug for IndexFilePurger {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexFilePurger").finish_non_exhaustive()
    }
}

impl IndexFilePurger {
    /// Couples range-index cleanup to the owning region's SST handles.
    pub(crate) fn wrap_sst_purger(
        &self,
        region_id: RegionId,
        inner: FilePurgerRef,
    ) -> FilePurgerRef {
        Arc::new(RangeIndexFilePurger {
            region_id,
            inner,
            index_purger: self.clone(),
        })
    }

    pub(super) fn purge(&self, request: PurgeRequest) {
        if let Err(error) = self.sender.send(request) {
            let store = self.store.clone();
            common_runtime::spawn_global(async move {
                let _ = purge_file(&store, error.0).await;
            });
        }
    }
}

pub(super) fn file_operation(index_type: IndexFileType, operation: &str, result: &str) {
    SERIES_INDEX_FILE_OPERATION_TOTAL
        .with_label_values(&[index_type.as_str(), operation, result])
        .inc();
}

pub(crate) async fn purge_file(store: &ObjectStore, request: PurgeRequest) -> bool {
    let path = index_file_path(request.index_type, request.file_id);
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
            warn!(error; "Failed to delete series index, index_type: {}, path: {}, phase: deletion, retry: true", request.index_type.as_str(), path);
            false
        }
    }
}

pub(crate) fn series_index_channel(
    store: ObjectStore,
) -> (IndexFilePurger, mpsc::UnboundedReceiver<PurgeRequest>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (IndexFilePurger { store, sender }, receiver)
}

/// Delegates SST purging and deletes its companion range index on SST deletion.
#[derive(Debug)]
struct RangeIndexFilePurger {
    region_id: RegionId,
    inner: FilePurgerRef,
    index_purger: IndexFilePurger,
}

impl FilePurger for RangeIndexFilePurger {
    fn remove_file(&self, file_meta: FileMeta, is_delete: bool, index_outdated: bool) {
        if is_delete {
            self.index_purger.purge(PurgeRequest {
                index_type: IndexFileType::Range,
                // Imported SSTs may have a different source region ID.
                file_id: RegionFileId::new(self.region_id, file_meta.file_id),
            });
        }
        self.inner.remove_file(file_meta, is_delete, index_outdated);
    }

    fn new_file(&self, file_meta: &FileMeta) {
        self.inner.new_file(file_meta);
    }
}
