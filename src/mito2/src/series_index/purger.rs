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

//! Deferred deletion of aggregate series-index files.

use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use store_api::storage::FileId;

use common_telemetry::{info, warn};
use object_store::{ErrorKind, ObjectStore};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::metrics::SERIES_INDEX_FILE_OPERATION_TOTAL;
use crate::series_index::catalog::series_index_path;
use crate::sst::file::RegionFileId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum IndexFileType {
    Range,
    Series,
}

impl IndexFileType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Range => "range",
            Self::Series => "series",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PurgeRequest {
    pub(crate) file_id: RegionFileId,
    pub(crate) objects: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ArtifactKey {
    file_id: RegionFileId,
    generation: Option<FileId>,
    sample: bool,
}

type ArtifactLeases = HashMap<ArtifactKey, Weak<ArtifactLeaseInner>>;

#[derive(Clone)]
pub(crate) struct IndexFilePurger {
    store: ObjectStore,
    sender: UnboundedSender<PurgeRequest>,
    leases: Arc<Mutex<ArtifactLeases>>,
}

impl Debug for IndexFilePurger {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexFilePurger").finish_non_exhaustive()
    }
}

/// Shares retirement across handles restored by close/reopen while old readers remain.
#[derive(Debug)]
pub(crate) struct ArtifactLease(Arc<ArtifactLeaseInner>);

#[derive(Debug)]
struct ArtifactLeaseInner {
    key: ArtifactKey,
    retired: AtomicBool,
    request: PurgeRequest,
    purger: IndexFilePurger,
}

impl ArtifactLease {
    pub(crate) fn mark_deleted(&self) {
        self.0.retired.store(true, Ordering::Release);
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.0.retired.load(Ordering::Acquire)
    }
}

impl Drop for ArtifactLeaseInner {
    fn drop(&mut self) {
        {
            let mut leases = self
                .purger
                .leases
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            // A concurrent reload may already have replaced this expired weak
            // reference. Remove only our dead entry, without scanning all pins.
            if leases
                .get(&self.key)
                .is_some_and(|lease| lease.strong_count() == 0)
            {
                leases.remove(&self.key);
            }
        }
        if self.retired.load(Ordering::Acquire) {
            self.purger.purge(self.request.clone());
        }
    }
}

impl IndexFilePurger {
    pub(crate) fn pin(
        &self,
        request: PurgeRequest,
        generation: Option<FileId>,
        sample: bool,
    ) -> ArtifactLease {
        let mut leases = self
            .leases
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let key = ArtifactKey {
            file_id: request.file_id,
            generation,
            sample,
        };
        if let Some(lease) = leases.get(&key).and_then(Weak::upgrade) {
            return ArtifactLease(lease);
        }
        let lease = Arc::new(ArtifactLeaseInner {
            key,
            retired: AtomicBool::new(false),
            request,
            purger: self.clone(),
        });
        leases.insert(key, Arc::downgrade(&lease));
        ArtifactLease(lease)
    }

    pub(crate) fn purge(&self, request: PurgeRequest) {
        if let Err(error) = self.sender.send(request) {
            let store = self.store.clone();
            common_runtime::spawn_compact(async move {
                purge_file(&store, error.0).await;
            });
        }
    }
}

pub(crate) fn file_operation(index_type: IndexFileType, operation: &str, result: &str) {
    SERIES_INDEX_FILE_OPERATION_TOTAL
        .with_label_values(&[index_type.as_str(), operation, result])
        .inc();
}

/// Processes queued deletions once each, independently of periodic maintenance.
pub(crate) async fn run_index_purge_task(
    worker_id: u32,
    store: ObjectStore,
    mut receiver: UnboundedReceiver<PurgeRequest>,
) {
    info!("Start series-index purge task, worker: {worker_id}");
    while let Some(request) = receiver.recv().await {
        purge_file(&store, request).await;
    }
    info!("Stop series-index purge task, worker: {worker_id}");
}

async fn purge_file(store: &ObjectStore, request: PurgeRequest) {
    let objects = request.objects.unwrap_or_else(|| {
        vec![series_index_path(
            request.file_id.region_id(),
            request.file_id.file_id(),
        )]
    });
    for path in objects {
        match store.delete(&path).await {
            Ok(()) => {
                file_operation(IndexFileType::Series, "delete", "success");
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                file_operation(IndexFileType::Series, "delete", "success");
            }
            Err(error) => {
                file_operation(IndexFileType::Series, "delete", "failure");
                warn!(error; "Failed to delete series index, index_type: {}, path: {}, phase: deletion", IndexFileType::Series.as_str(), path);
            }
        }
    }
}

pub(crate) fn series_index_channel(
    store: ObjectStore,
) -> (IndexFilePurger, UnboundedReceiver<PurgeRequest>) {
    let (sender, receiver) = unbounded_channel();
    (
        IndexFilePurger {
            store,
            sender,
            leases: Arc::default(),
        },
        receiver,
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use object_store::ObjectStore;
    use object_store::services::Memory;
    use store_api::storage::{FileId, RegionId};

    use super::*;

    #[tokio::test]
    async fn test_purge_drains_queue_after_last_sender_drops() {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let (purger, receiver) = series_index_channel(store.clone());
        let mut paths = Vec::new();
        for _ in 0..3 {
            let file_id = RegionFileId::new(RegionId::new(1, 1), FileId::random());
            let path = series_index_path(file_id.region_id(), file_id.file_id());
            store.write(&path, "index").await.unwrap();
            paths.push(path);
            purger.purge(PurgeRequest {
                file_id,
                objects: None,
            });
        }
        drop(purger);
        tokio::time::timeout(
            Duration::from_secs(10),
            run_index_purge_task(0, store.clone(), receiver),
        )
        .await
        .unwrap();
        for path in paths {
            assert!(!store.exists(&path).await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_purge_falls_back_after_receiver_drops() {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let (purger, receiver) = series_index_channel(store.clone());
        drop(receiver);
        let file_id = RegionFileId::new(RegionId::new(1, 1), FileId::random());
        let path = series_index_path(file_id.region_id(), file_id.file_id());
        store.write(&path, "index").await.unwrap();
        purger.purge(PurgeRequest {
            file_id,
            objects: None,
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            while store.exists(&path).await.unwrap() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
