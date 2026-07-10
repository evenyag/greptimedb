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

//! Sparse series-key filters for SeriesScan-by-key.

use std::collections::HashSet;
use std::sync::Arc;

use mito_codec::row_converter::{PrimaryKeyFilter, SparsePrimaryKeyCodec};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::FileId;

/// Sparse `(table_id, tsid)` allow-list used by SeriesScan-by-key partitions.
#[derive(Clone, Debug)]
pub(crate) struct SeriesKeyFilter {
    keys: Arc<HashSet<(u32, u64)>>,
    covered_files: Arc<HashSet<FileId>>,
}

impl SeriesKeyFilter {
    pub(crate) fn new(
        keys: HashSet<(u32, u64)>,
        covered_files: HashSet<FileId>,
    ) -> SeriesKeyFilter {
        SeriesKeyFilter {
            keys: Arc::new(keys),
            covered_files: Arc::new(covered_files),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub(crate) fn covers(&self, file_id: FileId) -> bool {
        self.covered_files.contains(&file_id)
    }

    pub(crate) fn make_primary_key_filter(
        &self,
        metadata: &RegionMetadataRef,
    ) -> Box<dyn PrimaryKeyFilter> {
        Box::new(SparseSeriesKeyPrimaryKeyFilter {
            codec: SparsePrimaryKeyCodec::new(metadata),
            keys: Arc::clone(&self.keys),
            last_primary_key: Vec::new(),
            last_match: None,
        })
    }
}

struct SparseSeriesKeyPrimaryKeyFilter {
    codec: SparsePrimaryKeyCodec,
    keys: Arc<HashSet<(u32, u64)>>,
    last_primary_key: Vec<u8>,
    last_match: Option<bool>,
}

impl PrimaryKeyFilter for SparseSeriesKeyPrimaryKeyFilter {
    fn matches(&mut self, pk: &[u8]) -> mito_codec::error::Result<bool> {
        if let Some(last_match) = self.last_match
            && self.last_primary_key == pk
        {
            return Ok(last_match);
        }

        let key = self.codec.read_table_id_tsid(pk)?;
        let matched = self.keys.contains(&key);
        self.last_primary_key.clear();
        self.last_primary_key.extend_from_slice(pk);
        self.last_match = Some(matched);
        Ok(matched)
    }
}
