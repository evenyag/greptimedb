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

//! Immutable, potentially multi-object format outputs and reader-owned lifetimes.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use store_api::storage::FileId;

use crate::series_index::purger::{ArtifactLease, IndexFilePurger, PurgeRequest};
use crate::sst::file::RegionFileId;

/// Persisted artifact identifiers. Concrete codecs are added with their implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ArtifactFormat {
    #[serde(rename = "parquet")]
    Parquet,
    #[serde(rename = "columnar-series")]
    ColumnarSeries,
    #[serde(rename = "columnar-samples")]
    ColumnarSamples,
    #[cfg(test)]
    #[serde(rename = "fake-samples")]
    FakeSamples,
    #[cfg(test)]
    #[serde(rename = "fake-index")]
    FakeIndex,
}

/// One immutable object produced by a backend build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ArtifactObject {
    pub(crate) path: String,
    pub(crate) file_size: u64,
}

/// Identifies the exact bytes produced by one backend build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ArtifactMetadata {
    pub(crate) format: ArtifactFormat,
    pub(crate) generation: FileId,
    /// Every object owned by this build, including metadata and auxiliary files.
    pub(crate) objects: Vec<ArtifactObject>,
    pub(crate) file_size: u64,
}

/// Source admission semantics captured before reading authoritative Parquet rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SourceSst {
    region_id: store_api::storage::RegionId,
    sequence: Option<std::num::NonZeroU64>,
    preserve_row_sequence: bool,
    time_range: (common_time::Timestamp, common_time::Timestamp),
    file_size: u64,
    partition_expr: Option<partition::expr::PartitionExpr>,
}

impl SourceSst {
    pub(crate) fn from_file(file: &crate::sst::file::FileMeta) -> Self {
        Self {
            region_id: file.region_id,
            sequence: file.sequence,
            preserve_row_sequence: file.preserve_row_sequence,
            time_range: file.time_range,
            file_size: file.file_size,
            partition_expr: file.partition_expr.clone(),
        }
    }
}

/// One samples artifact derived from immutable Parquet SSTs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SamplesEntry {
    pub(crate) file_id: FileId,
    pub(crate) artifact: ArtifactMetadata,
    /// Empty for legacy per-SST artifacts. Otherwise the complete source set for a UTC day.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) source_file_ids: Vec<FileId>,
    /// UTC day number since the Unix epoch; absent for legacy per-SST artifacts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) window: Option<i64>,
    /// Complete nonempty UTC-day coverage, stored on one anchor entry per source group.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub(crate) source_windows: std::collections::HashMap<FileId, Vec<i64>>,
    /// Effective sequence provenance, on the same anchor as populated-day coverage.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub(crate) source_metadata: std::collections::HashMap<FileId, SourceSst>,
    /// Destination visibility predicate used while reading the source Parquet files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) target_partition_expr: Option<String>,
}

impl SamplesEntry {
    pub(crate) fn covers(&self, id: FileId) -> bool {
        if !self.source_file_ids.is_empty() {
            self.source_file_ids.contains(&id)
        } else {
            self.file_id == id
        }
    }

    pub(crate) fn sources_visible(&self, visible: &std::collections::HashSet<FileId>) -> bool {
        if self.window.is_some() || !self.source_file_ids.is_empty() {
            !self.source_file_ids.is_empty()
                && self.source_file_ids.iter().all(|id| visible.contains(id))
        } else {
            visible.contains(&self.file_id)
        }
    }
}

/// Requires either a legacy whole-SST artifact or complete daily coverage.
pub(crate) fn samples_cover_file(
    samples: &std::collections::HashMap<FileId, SamplesFileHandle>,
    file: &crate::sst::file::FileHandle,
) -> bool {
    let id = file.file_id().file_id();
    if samples.values().any(|h| {
        h.entry().window.is_none() && h.entry().source_file_ids.is_empty() && h.entry().covers(id)
    }) {
        return true;
    }
    samples.values().any(|handle| {
        let Some(days) = handle.entry().source_windows.get(&id) else {
            // Earlier daily catalogs require rebuilding to establish actual row coverage.
            return false;
        };
        handle.entry().source_metadata.get(&id) == Some(&SourceSst::from_file(file.meta_ref()))
            && days.iter().all(|day| {
                samples.values().any(|h| {
                    h.entry().window == Some(*day)
                        && h.entry().covers(id)
                        && h.entry().source_file_ids == handle.entry().source_file_ids
                })
            })
    })
}

#[derive(Debug, Clone)]
pub(crate) struct SamplesFileHandle(Arc<SamplesFileInner>);

#[derive(Debug)]
struct SamplesFileInner {
    file_id: RegionFileId,
    entry: SamplesEntry,
    lease: ArtifactLease,
}

impl SamplesFileHandle {
    pub(crate) fn new(file_id: RegionFileId, entry: SamplesEntry, purger: IndexFilePurger) -> Self {
        let lease = purger.pin(
            PurgeRequest {
                file_id,
                objects: Some(
                    entry
                        .artifact
                        .objects
                        .iter()
                        .map(|object| object.path.clone())
                        .collect(),
                ),
            },
            Some(entry.artifact.generation),
            true,
        );
        Self(Arc::new(SamplesFileInner {
            file_id,
            entry,
            lease,
        }))
    }

    pub(crate) fn file_id(&self) -> RegionFileId {
        self.0.file_id
    }

    pub(crate) fn entry(&self) -> &SamplesEntry {
        &self.0.entry
    }

    pub(crate) fn mark_deleted(&self) {
        self.0.lease.mark_deleted();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn frozen_sample_catalog_coverage() {
        let legacy: SamplesEntry = serde_json::from_str(include_str!(
            "../../../../tests/compatibility/wire/mito_columnar_samples/legacy-catalog-entry.json"
        ))
        .unwrap();
        assert!(legacy.window.is_none());
        assert!(legacy.source_file_ids.is_empty());
        assert!(legacy.covers(legacy.file_id));
        let daily: SamplesEntry = serde_json::from_str(include_str!(
            "../../../../tests/compatibility/wire/mito_columnar_samples/daily-catalog-entry.json"
        ))
        .unwrap();
        assert!(daily.source_windows.is_empty());
        let sparse: SamplesEntry = serde_json::from_str(include_str!(
            "../../../../tests/compatibility/wire/mito_columnar_samples/sparse-daily-catalog-entry.json"
        )).unwrap();
        assert_eq!(sparse.source_windows[&legacy.file_id], vec![0, 3]);
        assert_eq!(
            sparse,
            serde_json::from_str(&serde_json::to_string(&sparse).unwrap()).unwrap()
        );
        let mut visible: HashSet<_> = daily.source_file_ids.iter().copied().collect();
        assert!(daily.sources_visible(&visible));
        assert!(daily.covers(legacy.file_id));
        assert!(!daily.covers(daily.file_id));
        visible.remove(&legacy.file_id);
        assert!(!daily.sources_visible(&visible));
        assert_eq!(
            daily,
            serde_json::from_str(&serde_json::to_string(&daily).unwrap()).unwrap()
        );
        assert!(
            !serde_json::to_string(&legacy)
                .unwrap()
                .contains("source_file_ids")
        );
    }
}
