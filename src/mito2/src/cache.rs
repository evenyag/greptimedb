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

//! Cache for the engine.

use std::sync::Arc;

use moka::sync::Cache;
use parquet::file::metadata::ParquetMetaData;
use store_api::storage::RegionId;

use crate::sst::file::FileId;

/// Manages cached data for the engine.
pub struct CacheManager {
    cache: Cache<String, CacheValue>,
}

pub type CacheManagerRef = Arc<CacheManager>;

impl CacheManager {
    /// Creates a new manager with specific cache capacity in bytes.
    /// Returns `None` if `capacity` is 0.
    pub fn new(capacity: u64) -> Option<CacheManager> {
        if capacity == 0 {
            None
        } else {
            let cache = Cache::builder()
                .max_capacity(capacity)
                .build();
            Some(CacheManager {
                cache,
            })
        }
    }

    /// Gets cached [ParquetMetaData].
    pub fn get_parquet_meta_data(
        &self,
        _region_id: RegionId,
        _file_id: FileId,
    ) -> Option<Arc<ParquetMetaData>> {
        // TODO(yingwen): Implements it.
        None
    }

    /// Puts [ParquetMetaData] into the cache.
    pub fn put_parquet_meta_data(
        &self,
        _region_id: RegionId,
        _file_id: FileId,
        _metadata: Arc<ParquetMetaData>,
    ) {
        // TODO(yingwen): Implements it.
    }
}

/// Cached value.
/// It can hold different kinds of data.
#[derive(Clone)]
enum CacheValue {
    /// Parquet meta data.
    ParquetMeta(Arc<ParquetMetaData>),
}
