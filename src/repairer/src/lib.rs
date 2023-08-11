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

mod manifest;
mod object_store_util;

use datanode::datanode::StorageConfig;
use object_store::ObjectStore;

use crate::manifest::{ManifestRebuilder, RebuildCatalog, RebuildDb, RebuildRegion, RebuildSchema};

pub type Result<T, E = snafu::Whatever> = std::result::Result<T, E>;

/// DB repairer.
#[derive(Debug)]
pub struct Repairer {
    /// Object store from storage config.
    object_store: ObjectStore,
    /// Dry run mode
    dry_run: bool,
}

impl Repairer {
    /// Creates a new repairer.
    pub async fn new(storage_config: StorageConfig) -> Result<Repairer> {
        let object_store = object_store_util::new_object_store(&storage_config.store).await?;

        Ok(Repairer {
            object_store,
            dry_run: false,
        })
    }

    /// Set dry run mode.
    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Repair a region manifest.
    pub async fn repair_region_manifest(&self, req: RebuildRegion) -> Result<()> {
        let rebuilder =
            ManifestRebuilder::new(self.object_store.clone()).with_dry_run(self.dry_run);
        rebuilder.rebuild_region(&req).await
    }

    /// Repair db manifest.
    pub async fn repair_db_manifest(&self, req: RebuildDb) -> Result<()> {
        let rebuilder =
            ManifestRebuilder::new(self.object_store.clone()).with_dry_run(self.dry_run);
        rebuilder.rebuild_db(&req).await
    }

    /// Repair catalog manifest.
    pub async fn repair_catalog_manifest(&self, req: RebuildCatalog) -> Result<()> {
        let rebuilder =
            ManifestRebuilder::new(self.object_store.clone()).with_dry_run(self.dry_run);
        rebuilder.rebuild_catalog(&req).await
    }

    /// Repair schema manifest.
    pub async fn repair_schema_manifest(&self, req: RebuildSchema) -> Result<()> {
        let rebuilder =
            ManifestRebuilder::new(self.object_store.clone()).with_dry_run(self.dry_run);
        rebuilder.rebuild_schema(&req).await
    }
}
