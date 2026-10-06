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

//! Ephemeral engine-owned complete results. Admission is preparation-only.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use datafusion::execution::memory_pool::MemoryPool;
use serde::{Deserialize, Serialize};
use store_api::region_engine::PartitionRange;

use crate::error::Result;
use crate::read::range_cache::{RangeScanCacheKey, build_buffered_range_cache_key};
use crate::read::scan_region::StreamContext;
use crate::read::series_compact::{CachedTags, TagCatalog};
use crate::read::series_result::resources::{Charge, Kind, open_namespace};
use crate::read::series_result::{
    ResultHandle, StoreOptions, StoreResources, checked_add, checked_mul, fail,
};
use crate::series_index::MetricSeriesId;

/// Explicit development capacities, independent of query spill quotas.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Options {
    pub(crate) directory: PathBuf,
    pub(crate) disk_bytes: usize,
    pub(crate) metadata_bytes: usize,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    range: RangeScanCacheKey,
    /// Includes schema/defaults and effective file sequence metadata.
    schema_sources: String,
    /// Versioned compact IPC representation, including effective batch bounds.
    representation: String,
    ids: Vec<MetricSeriesId>,
}

impl Key {
    pub(crate) fn new(
        ctx: &StreamContext,
        range: &PartitionRange,
        ids: &[MetricSeriesId],
        options: &StoreOptions,
    ) -> Option<Self> {
        let key = build_buffered_range_cache_key(ctx, range)?;
        let mut sources = ctx.ranges[range.identifier]
            .row_group_indices
            .iter()
            .map(|index| serde_json::to_string(ctx.input.file_from_index(*index).meta_ref()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .ok()?;
        sources.sort_unstable();
        sources.dedup();
        let schema_sources = serde_json::to_string(&(ctx.input.region_metadata(), sources)).ok()?;
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.dedup();
        Some(Self {
            range: key,
            schema_sources,
            representation: representation(options),
            ids,
        })
    }

    pub(crate) fn bytes(&self) -> Result<usize> {
        checked_add(
            self.range.estimated_size(),
            checked_add(
                checked_mul(self.ids.capacity(), std::mem::size_of::<MetricSeriesId>())?,
                checked_add(
                    self.schema_sources.capacity(),
                    self.representation.capacity(),
                )?,
            )?,
        )
    }
}

fn representation(options: &StoreOptions) -> String {
    format!(
        "compact-ipc-v1/layout-v1/{:?}/{:?}/{}/{}/{}/{}/{}/{:?}/{}",
        options.layout,
        options.compression,
        options.batch_rows,
        options.batch_bytes,
        options.file_bytes,
        options.file_batches,
        options.file_metadata_bytes,
        options.key_encoding,
        options.fixed_keys.is_some()
    )
}

struct Entry {
    result: ResultHandle,
    tags: CachedTags,
    _charge: Charge,
}

#[derive(Default)]
struct State {
    clock: u64,
    entries: HashMap<Key, (u64, Arc<Entry>)>,
}

/// Lookup visibility and ownership are separate: result handles pin evicted files.
pub(crate) struct BufferedDataCache {
    pub(crate) options: Options,
    pub(crate) resources: Arc<StoreResources>,
    state: Mutex<State>,
    admission: tokio::sync::Mutex<()>,
}

impl BufferedDataCache {
    pub(crate) fn new(options: Options, mut store: StoreOptions) -> Result<Arc<Self>> {
        if options.disk_bytes == 0 || options.metadata_bytes == 0 {
            return Err(fail("cache capacities must be positive"));
        }
        let root = options.directory.join("buffered-data-cache-v1");
        let lock = open_namespace(&root, "buffered-cache.lock")?;
        store.disk_bytes = options.disk_bytes;
        store.metadata_bytes = options.metadata_bytes;
        store.memory_bytes = options.metadata_bytes;
        let resources = StoreResources::new(&root, store)?;
        resources.retain_namespace(lock);
        Ok(Arc::new(Self {
            options,
            resources,
            state: Mutex::new(State::default()),
            admission: tokio::sync::Mutex::new(()),
        }))
    }

    pub(crate) fn snapshot(&self) -> serde_json::Value {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut visible = 0usize;
        let mut pinned = 0usize;
        for (_, entry) in state.entries.values() {
            let bytes = entry.result.cache_disk_bytes();
            visible = visible.saturating_add(bytes);
            if entry.result.externally_pinned() {
                pinned = pinned.saturating_add(bytes);
            }
        }
        let resources = self.resources.snapshot();
        serde_json::json!({
            "visible_entries": state.entries.len(),
            "visible_disk_bytes": visible,
            "visible_pinned_disk_bytes": pinned,
            "retired_or_staging_disk_bytes": resources.disk_bytes.saturating_sub(visible),
            "resources": resources,
            "operations": crate::read::series_result::operation_values(&self.resources),
        })
    }

    pub(crate) fn invalidate_all(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.entries.clear();
        state.entries.shrink_to_fit();
    }

    pub(crate) fn supports(&self, options: &StoreOptions) -> bool {
        representation(&self.resources.options) == representation(options)
    }

    pub(crate) fn get(
        &self,
        key: &Key,
        catalog: &TagCatalog,
        query: &StoreResources,
    ) -> Result<Option<ResultHandle>> {
        let entry = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.clock = state.clock.wrapping_add(1);
            let clock = state.clock;
            state.entries.get_mut(key).map(|(age, entry)| {
                *age = clock;
                entry.clone()
            })
        };
        let Some(entry) = entry else {
            query.count("buffered_cache_misses", 1);
            return Ok(None);
        };
        catalog.import_tags(&entry.tags)?;
        query.count("buffered_cache_hits", 1);
        Ok(Some(entry.result.clone()))
    }

    /// Failure is optional. Never consume or mutate the query's only usable result.
    pub(crate) async fn admit(
        &self,
        key: Key,
        result: &ResultHandle,
        catalog: &TagCatalog,
        query: Arc<StoreResources>,
        pool: Arc<dyn MemoryPool>,
    ) -> Option<ResultHandle> {
        let _admission = self.admission.lock().await;
        if let Some(hit) = self.get_existing(&key) {
            return Some(hit);
        }
        let started = std::time::Instant::now();
        loop {
            let rejections = self
                .resources
                .snapshot()
                .counts
                .get("capacity_rejections")
                .copied()
                .unwrap_or(0);
            let attempt = async {
                let charge = self.resources.reserve(
                    Kind::Metadata,
                    checked_add(1024, checked_mul(key.bytes()?, 2)?)?,
                )?;
                let tags = catalog.snapshot_tags(&result.stored_identities(), &self.resources)?;
                let copy = result
                    .copy_to_cache(self.resources.clone(), query.clone(), pool.clone())
                    .await?;
                Ok::<_, crate::error::Error>(Arc::new(Entry {
                    result: copy,
                    tags,
                    _charge: charge,
                }))
            }
            .await;
            match attempt {
                Ok(entry) => {
                    let copy = entry.result.clone();
                    let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    state.clock = state.clock.wrapping_add(1);
                    let clock = state.clock;
                    state.entries.insert(key, (clock, entry));
                    query.count("buffered_cache_admissions", 1);
                    query.count("buffered_cache_admission_ns", super::nanos(started));
                    return Some(copy);
                }
                Err(error) => {
                    if self
                        .resources
                        .snapshot()
                        .counts
                        .get("capacity_rejections")
                        .copied()
                        .unwrap_or(0)
                        == rejections
                    {
                        query.count("buffered_cache_admission_skips", 1);
                        let _ = self.resources.drain_cleanup().await;
                        common_telemetry::debug!("Skipping buffered cache admission: {error}");
                        return None;
                    }
                    // Reclaim only lookup ownership; active query handles continue to pin.
                    let victim = {
                        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                        let oldest = state
                            .entries
                            .iter()
                            .min_by_key(|(_, (age, _))| *age)
                            .map(|(key, _)| key.clone());
                        let victim = oldest.and_then(|key| state.entries.remove(&key));
                        state.entries.shrink_to_fit();
                        victim
                    };
                    let evicted = victim.is_some();
                    drop(victim);
                    if evicted {
                        query.count("buffered_cache_evictions", 1);
                    }
                    if self.resources.drain_cleanup().await.is_err() || !evicted {
                        query.count("buffered_cache_admission_skips", 1);
                        common_telemetry::debug!("Skipping buffered cache admission: {error}");
                        return None;
                    }
                }
            }
        }
    }

    fn get_existing(&self, key: &Key) -> Option<ResultHandle> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .get(key)
            .map(|(_, e)| e.result.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pinned_eviction_and_failed_deletion_retain_capacity() {
        use crate::read::range_cache::ScanRequestFingerprintBuilder;
        use crate::read::read_columns::ReadColumns;
        use crate::read::series_compact::CompactMetrics;
        use crate::read::series_result::{Placement, ResultBuilder};
        use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
        use mito_codec::row_converter::SparsePrimaryKeyCodec;
        use store_api::codec::PrimaryKeyEncoding;
        use store_api::storage::RegionId;
        let dir = common_test_util::temp_dir::create_temp_dir("cache-pinned-eviction");
        let store = StoreOptions {
            batch_rows: 4,
            ..Default::default()
        };
        let input = crate::read::series_result::tests::fixture(&[(1, 3, 20)]);
        let builder = ResultBuilder::new(dir.path(), input.schema(), store.clone())
            .await
            .unwrap();
        let query = builder.resources();
        let original = builder
            .append(input, Placement::File)
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let cache = BufferedDataCache::new(
            Options {
                directory: dir.path().to_owned(),
                disk_bytes: original.cache_disk_bytes(),
                metadata_bytes: 1024 * 1024,
            },
            store,
        )
        .unwrap();
        let metadata = Arc::new(
            crate::test_util::sst_util::sst_region_metadata_with_encoding(
                PrimaryKeyEncoding::Sparse,
            ),
        );
        let catalog = TagCatalog::new(
            metadata,
            &query.pool(),
            CompactMetrics::new(&ExecutionPlanMetricsSet::default(), 0),
        );
        let mut encoded = vec![];
        SparsePrimaryKeyCodec::schemaless()
            .encode_internal(1, 3, &mut encoded)
            .unwrap();
        catalog.insert(&encoded).unwrap();
        let key = Key {
            range: RangeScanCacheKey {
                region_id: RegionId::new(1, 1),
                row_groups: vec![],
                scan: ScanRequestFingerprintBuilder {
                    read_columns: ReadColumns::new([]),
                    read_column_types: vec![],
                    filters: vec![],
                    time_filters: vec![],
                    series_row_selector: None,
                    append_mode: false,
                    filter_deleted: true,
                    merge_mode: crate::region::options::MergeMode::LastRow,
                    sequence_range: None,
                    partition_expr_version: 0,
                }
                .build(),
            },
            schema_sources: "fixture".to_owned(),
            representation: "fixture".to_owned(),
            ids: original.stored_identities(),
        };
        let pinned = cache
            .admit(
                key.clone(),
                &original,
                &catalog,
                query.clone(),
                query.pool(),
            )
            .await
            .unwrap();
        let disk = cache.resources.snapshot().disk_bytes;
        assert!(disk > 0);
        assert!(
            cache.snapshot()["visible_pinned_disk_bytes"]
                .as_u64()
                .unwrap()
                > 0
        );
        let mut other = key.clone();
        other.schema_sources = "another schema".to_owned();
        assert!(
            cache
                .admit(other, &original, &catalog, query.clone(), query.pool())
                .await
                .is_none()
        );
        assert!(query.snapshot().counts["buffered_cache_evictions"] > 0);
        assert!(cache.get(&key, &catalog, &query).unwrap().is_none());
        cache.resources.drain_cleanup().await.unwrap();
        assert_eq!(disk, cache.resources.snapshot().disk_bytes);
        cache
            .resources
            .faults
            .remove
            .store(true, std::sync::atomic::Ordering::SeqCst);
        drop(pinned);
        assert!(cache.resources.drain_cleanup().await.is_err());
        assert_eq!(disk, cache.resources.snapshot().disk_bytes);
        assert!(
            cache.resources.snapshot().metadata_bytes > cache.resources.snapshot().resource_bytes
        );
        cache
            .resources
            .faults
            .remove
            .store(false, std::sync::atomic::Ordering::SeqCst);
        cache.resources.drain_cleanup().await.unwrap();
        assert_eq!(0, cache.resources.snapshot().disk_bytes);
        assert_eq!(
            cache.resources.snapshot().metadata_bytes,
            cache.resources.snapshot().resource_bytes
        );
    }

    #[tokio::test]
    async fn startup_cleanup_preserves_unknown_files_and_live_namespaces() {
        let dir = common_test_util::temp_dir::create_temp_dir("buffered-cache-startup");
        let root = dir.path().join("buffered-data-cache-v1");
        let abandoned = root.join(format!("series-result-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&abandoned).unwrap();
        let partial = abandoned.join(format!("{}.partial", uuid::Uuid::new_v4()));
        std::fs::write(&partial, b"partial").unwrap();
        let sentinel = abandoned.join("unrelated");
        std::fs::write(&sentinel, b"keep").unwrap();
        let options = Options {
            directory: dir.path().to_owned(),
            disk_bytes: 1024 * 1024,
            metadata_bytes: 1024 * 1024,
        };
        let cache = BufferedDataCache::new(options.clone(), StoreOptions::default()).unwrap();
        assert!(!partial.exists());
        assert_eq!(std::fs::read(sentinel).unwrap(), b"keep");
        assert!(BufferedDataCache::new(options, StoreOptions::default()).is_err());
        cache.resources.drain_cleanup().await.unwrap();
    }
}
