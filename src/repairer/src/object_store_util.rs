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

//! object storage utilities.

use datanode::datanode::{FileConfig, ObjectStoreConfig};
use datanode::store;
use datanode::store::fs::new_fs_with_atomic_dir_suffix;
use object_store::layers::RetryLayer;
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::Result;

/// Dir for atomic write in repair mode.
///
/// We choose another dir to avoid collision with exisitng atomic dir.
const REPAIR_ATOMIC_WRITE_DIR: &str = ".repair";

/// Creates a new fs object store with a atomic dir for repairer.
pub(crate) async fn new_fs_object_store(file_config: &FileConfig) -> Result<ObjectStore> {
    new_fs_with_atomic_dir_suffix(file_config, REPAIR_ATOMIC_WRITE_DIR)
        .await
        .whatever_context("new fs object store")
}

/// Creates a new object store.
pub(crate) async fn new_object_store(store_config: &ObjectStoreConfig) -> Result<ObjectStore> {
    let object_store = match store_config {
        ObjectStoreConfig::File(file_config) => new_fs_object_store(file_config).await,
        ObjectStoreConfig::S3(s3_config) => store::s3::new_s3_object_store(s3_config)
            .await
            .whatever_context("new s3 object store"),
        ObjectStoreConfig::Oss(oss_config) => store::oss::new_oss_object_store(oss_config)
            .await
            .whatever_context("new oss object store"),
        ObjectStoreConfig::Azblob(azblob_config) => {
            store::azblob::new_azblob_object_store(azblob_config)
                .await
                .whatever_context("new azblob object store")
        }
        ObjectStoreConfig::Gcs(gcs_config) => store::gcs::new_gcs_object_store(gcs_config)
            .await
            .whatever_context("new gcs object store"),
    }?;

    // Enable retry layer and cache layer for non-fs object storages
    let object_store = if !matches!(store_config, ObjectStoreConfig::File(..)) {
        object_store.layer(RetryLayer::new().with_jitter())
    } else {
        object_store
    };

    Ok(object_store)
}
