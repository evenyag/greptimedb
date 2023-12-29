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

//! A write-through cache for remote object stores.

use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use object_store::manager::ObjectStoreManagerRef;
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{RegionId, SequenceNumber};
use tokio::sync::mpsc::Sender;

use crate::access_layer::sst_file_path;
use crate::cache::file_cache::{FileCache, FileCacheRef, IndexValue};
use crate::error::{CopySstSnafu, ObjectStoreNotFoundSnafu, OpenDalSnafu, Result};
use crate::read::Source;
use crate::request::WorkerRequest;
use crate::sst::file::{FileId, FileMeta, Level};
use crate::sst::parquet::writer::ParquetWriter;
use crate::sst::parquet::WriteOptions;
use crate::wal::EntryId;

/// A cache for uploading files to remote object stores.
///
/// It keeps files in local disk and then sends files to object stores.
pub(crate) struct WriteCache {
    /// Local file cache.
    file_cache: FileCacheRef,
    /// Object store manager.
    object_store_manager: ObjectStoreManagerRef,
}

pub(crate) type WriteCacheRef = Arc<WriteCache>;

impl WriteCache {
    // TODO(yingwen): Maybe pass cache path instead of local store.
    /// Create the cache with a `local_store` to cache files and a
    /// `object_store_manager` for all object stores.
    pub(crate) fn new(
        local_store: ObjectStore,
        object_store_manager: ObjectStoreManagerRef,
    ) -> Self {
        // TODO(yingwen): Expose cache capacity and cache path config.
        Self {
            file_cache: Arc::new(FileCache::new(
                local_store,
                "cache".to_string(),
                ReadableSize::mb(512),
            )),
            object_store_manager,
        }
    }

    /// Adds files to the cache.
    pub(crate) async fn upload(&self, upload: Upload) -> Result<()> {
        // Uploads each parts.
        for part in &upload.parts {
            self.upload_part(part).await?;
        }

        // Add files to the file cache.
        for part in &upload.parts {
            for meta in &part.file_metas {
                self.file_cache
                    .put(
                        (part.region_id, meta.file_id),
                        IndexValue {
                            file_size: meta.file_size as u32,
                        },
                    )
                    .await;
            }
        }

        Ok(())
    }

    /// Returns the file cache of the write cache.
    pub(crate) fn file_cache(&self) -> FileCacheRef {
        self.file_cache.clone()
    }

    /// Uploads a part to remote.
    async fn upload_part(&self, part: &UploadPart) -> Result<()> {
        for meta in &part.file_metas {
            let remote = self.remote_store(part.storage.as_ref())?;
            let local = self.file_cache.local_store();

            let path = self
                .file_cache
                .cache_file_path((part.region_id, meta.file_id));
            let reader = local.reader(&path).await.context(OpenDalSnafu)?;

            // TODO(yingwen): Reuse DEFAULT_WRITE_BUFFER_SIZE.
            let mut writer = remote
                .writer_with(&sst_file_path(&part.region_dir, meta.file_id))
                .buffer(5 * 1024 * 1024)
                .await
                .context(OpenDalSnafu)?;

            futures::io::copy(reader, &mut writer)
                .await
                .context(CopySstSnafu {
                    region_id: meta.region_id,
                    file_id: meta.file_id,
                })?;
        }

        Ok(())
    }

    fn remote_store(&self, storage: Option<&String>) -> Result<ObjectStore> {
        match storage {
            Some(name) => self
                .object_store_manager
                .find(name)
                .cloned()
                .context(ObjectStoreNotFoundSnafu { object_store: name }),
            None => Ok(self.object_store_manager.default_object_store().clone()),
        }
    }
}

/// A remote write request to upload files.
pub(crate) struct Upload {
    /// Parts to upload.
    pub(crate) parts: Vec<UploadPart>,
}

impl Upload {
    /// Creates a new upload from parts.
    pub(crate) fn new(parts: Vec<UploadPart>) -> Upload {
        Upload { parts }
    }
}

/// Metadata of SSTs to upload together.
pub(crate) struct UploadPart {
    /// Region id.
    region_id: RegionId,
    /// Directory of the region data.
    region_dir: String,
    /// Meta of files created.
    pub(crate) file_metas: Vec<FileMeta>,
    /// Target storage of SSTs.
    storage: Option<String>,
}

/// Writer to build a upload part.
pub(crate) struct UploadPartWriter {
    /// Remote object store to write.
    remote_store: ObjectStore,
    /// Metadata of the region.
    metadata: RegionMetadataRef,
    /// Directory of the region.
    region_dir: String,
    /// Meta of files created.
    file_metas: Vec<FileMeta>,
    /// Target storage of SSTs.
    storage: Option<String>,
    /// Local file cache.
    file_cache: Option<FileCacheRef>,
}

impl UploadPartWriter {
    /// Creates a new writer.
    pub(crate) fn new(remote_store: ObjectStore, metadata: RegionMetadataRef) -> Self {
        Self {
            remote_store,
            metadata,
            region_dir: String::new(),
            file_metas: Vec::new(),
            storage: None,
            file_cache: None,
        }
    }

    /// Sets region directory for the part.
    #[must_use]
    pub(crate) fn with_region_dir(mut self, region_dir: String) -> Self {
        self.region_dir = region_dir;
        self
    }

    /// Sets target storage for the part.
    #[must_use]
    pub(crate) fn with_storage(mut self, storage: Option<String>) -> Self {
        self.storage = storage;
        self
    }

    /// Sets the file cache for the part.
    #[must_use]
    pub(crate) fn with_file_cache(mut self, cache: Option<FileCacheRef>) -> Self {
        self.file_cache = cache;
        self
    }

    /// Reserve capacity for `additional` files.
    pub(crate) fn reserve_capacity(&mut self, additional: usize) {
        self.file_metas.reserve(additional);
    }

    /// Builds a new parquet writer to write to this part.
    ///
    /// If the file cache is enabled, it writes to the local store of the file cache.
    /// It doesn't add the file to the index of the cache to avoid the cache removing
    /// it before we uploading the file.
    pub(crate) fn new_sst_writer(&self, file_id: FileId) -> ParquetWriter {
        match self.file_cache.as_ref() {
            Some(cache) => {
                // File cache is enabled, write to the local store.
                let path = cache.cache_file_path((self.metadata.region_id, file_id));
                ParquetWriter::new(path, self.metadata.clone(), cache.local_store())
            }
            None => {
                // File cache is disabled, write to the remote store.
                let path = sst_file_path(&self.region_dir, file_id);
                ParquetWriter::new(path, self.metadata.clone(), self.remote_store.clone())
            }
        }
    }

    /// Adds a SST to this part.
    pub(crate) fn add_sst(&mut self, file_meta: FileMeta) {
        self.file_metas.push(file_meta);
    }

    /// Adds multiple SSTs to this part.
    pub(crate) fn extend_ssts(&mut self, iter: impl IntoIterator<Item = FileMeta>) {
        self.file_metas.extend(iter)
    }

    /// Returns [FileMeta] of written files.
    pub(crate) fn written_file_metas(&self) -> &[FileMeta] {
        &self.file_metas
    }

    /// Finishes the writer and builds a part.
    pub(crate) fn finish(self) -> UploadPart {
        UploadPart {
            region_id: self.metadata.region_id,
            region_dir: self.region_dir,
            file_metas: self.file_metas,
            storage: self.storage,
        }
    }

    /// Returns the path to store the file.
    fn file_path(&self, file_id: FileId) -> String {
        match self.file_cache.as_ref() {
            Some(cache) => cache.cache_file_path((self.metadata.region_id, file_id)),
            None => sst_file_path(&self.region_dir, file_id),
        }
    }
}
