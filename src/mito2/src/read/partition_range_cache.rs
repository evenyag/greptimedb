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

//! Cache key types for partition-range scan outputs.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use store_api::storage::{ColumnId, FileId, RegionId};

use crate::cache::CacheStrategy;

/// Fingerprint of request-relevant scan options.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ScanRequestFingerprint {
    pub(crate) read_column_ids: Vec<ColumnId>,
    pub(crate) filters: Vec<String>,
    pub(crate) series_row_selector: Option<String>,
    pub(crate) distribution: Option<String>,
    pub(crate) append_mode: bool,
    pub(crate) filter_deleted: bool,
    pub(crate) merge_mode: &'static str,
    pub(crate) flat_format: bool,
    pub(crate) compaction: bool,
}

/// Cache key for partition-range scan outputs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PartitionRangeScanCacheKey {
    pub(crate) region_id: RegionId,
    pub(crate) file_ids: Vec<FileId>,
    pub(crate) scan: ScanRequestFingerprint,
}

impl PartitionRangeScanCacheKey {
    pub(crate) fn digest(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

/// Cache plan for a flat partition range.
#[derive(Debug, Clone)]
pub(crate) struct FlatPartitionRangeCachePlan {
    pub(crate) key: Option<PartitionRangeScanCacheKey>,
}

/// Returns true if cache is enabled by strategy.
pub(crate) fn cache_enabled(cache_strategy: &CacheStrategy) -> bool {
    !matches!(cache_strategy, CacheStrategy::Disabled)
}
