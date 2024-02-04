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

//! Implementation of the memtable merge tree.

use std::sync::{Arc, RwLock};

use api::v1::OpType;
use common_time::Timestamp;
use moka::sync::Cache;
use snafu::ensure;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::cache::PK_ID_TYPE;
use crate::error::{PrimaryKeyLengthMismatchSnafu, Result};
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::data::DataBuffer;
use crate::memtable::merge_tree::index::{IndexConfig, KeyIndex, KeyIndexRef};
use crate::memtable::merge_tree::mutable::WriteMetrics;
use crate::memtable::merge_tree::{MergeTreeConfig, PkId};
use crate::memtable::{BoxedBatchIterator, KeyValues};
use crate::metrics::CACHE_BYTES;
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};

/// Initial capacity for the data buffer.
const DATA_INIT_CAP: usize = 8;

/// The merge tree.
pub(crate) struct MergeTree {
    /// Config of the tree.
    config: MergeTreeConfig,
    /// Metadata of the region.
    pub(crate) metadata: RegionMetadataRef,
    /// Primary key codec.
    row_codec: Arc<McmpRowCodec>,
    // TODO(yingwen): The pk id cache allocates many small objects. We might need some benchmarks to see whether
    // it is necessary to use another way to get the id from pk.
    pk_id_cache: Option<PkIdCache>,
    // TODO(yingwen): Freeze parts.
    parts: RwLock<TreeParts>,
}

pub(crate) type MergeTreeRef = Arc<MergeTree>;

impl MergeTree {
    /// Creates a new merge tree.
    pub(crate) fn new(metadata: RegionMetadataRef, config: &MergeTreeConfig) -> MergeTree {
        let row_codec = McmpRowCodec::new(
            metadata
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        );
        let pk_id_cache = (!metadata.primary_key.is_empty()
            && config.pk_cache_size.as_bytes() != 0)
            .then(|| new_cache(config.pk_cache_size.as_bytes()));

        let index = (!metadata.primary_key.is_empty()).then(|| {
            Arc::new(KeyIndex::new(IndexConfig {
                max_keys_per_shard: config.index_max_keys_per_shard,
            }))
        });
        let data_buffer = DataBuffer::with_capacity(metadata.clone(), DATA_INIT_CAP);
        let parts = TreeParts {
            immutable: false,
            index,
            data_buffer,
        };

        MergeTree {
            config: config.clone(),
            metadata,
            row_codec: Arc::new(row_codec),
            pk_id_cache,
            parts: RwLock::new(parts),
        }
    }

    // FIXME(yingwen): We should use actual size of parts.
    /// Write key-values into the tree.
    ///
    /// # Panics
    /// Panics if the tree is immutable.
    pub(crate) fn write(&self, kvs: &KeyValues, metrics: &mut WriteMetrics) -> Result<()> {
        let mut primary_key = Vec::new();
        let has_pk = !self.metadata.primary_key.is_empty();

        for kv in kvs.iter() {
            ensure!(
                kv.num_primary_keys() == self.row_codec.num_fields(),
                PrimaryKeyLengthMismatchSnafu {
                    expect: self.row_codec.num_fields(),
                    actual: kv.num_primary_keys(),
                }
            );
            // Safety: timestamp of kv must be both present and a valid timestamp value.
            let ts = kv.timestamp().as_timestamp().unwrap().unwrap().value();
            metrics.min_ts = metrics.min_ts.min(ts);
            metrics.max_ts = metrics.max_ts.max(ts);
            metrics.value_bytes += kv.fields().map(|v| v.data_size()).sum::<usize>();

            if !has_pk {
                // No primary key.
                // Now we always assign the first shard and the first pk index to the id.
                let pk_id = PkId {
                    shard_id: 0,
                    pk_index: 0,
                };
                self.write_with_id(pk_id, kv);
                continue;
            }

            // Encode primary key.
            primary_key.clear();
            self.row_codec
                .encode_to_vec(kv.primary_keys(), &mut primary_key)?;

            // Add bytes used by the primary key.
            metrics.key_bytes += primary_key.len();

            // Write rows with primary keys.
            self.write_with_key(&primary_key, kv)?;
        }

        metrics.value_bytes +=
            kvs.num_rows() * (std::mem::size_of::<Timestamp>() + std::mem::size_of::<OpType>());

        Ok(())
    }

    /// Scans the tree.
    pub(crate) fn scan(
        &self,
        _projection: Option<&[ColumnId]>,
        _predicate: Option<Predicate>,
    ) -> Result<BoxedBatchIterator> {
        todo!()
    }

    /// Returns true if the tree is empty.
    pub(crate) fn is_empty(&self) -> bool {
        // Gets whether the memtable is empty from the data part.
        unimplemented!()
    }

    /// Marks the tree as immutable.
    ///
    /// Once the tree becomes immutable, callers should not write to it again.
    pub(crate) fn freeze(&self) -> Result<()> {
        let mut parts = self.parts.write().unwrap();
        parts.immutable = true;
        // Freezes the index.
        if let Some(index) = &parts.index {
            index.freeze()?;
        }

        Ok(())
    }

    /// Forks an immutable tree. Returns a mutable tree that inherits the index
    /// and cache of this tree.
    pub(crate) fn fork(&self, metadata: RegionMetadataRef) -> MergeTree {
        if metadata.primary_key != self.metadata.primary_key {
            // The priamry key is changed. We can't reuse fields.
            return MergeTree::new(metadata, &self.config);
        }

        let current_parts = self.parts.read().unwrap();
        let index = current_parts
            .index
            .as_ref()
            .map(|index| Arc::new(index.fork()));
        // New parts.
        let parts = TreeParts {
            immutable: false,
            index,
            data_buffer: DataBuffer::with_capacity(metadata.clone(), DATA_INIT_CAP),
        };

        MergeTree {
            config: self.config.clone(),
            metadata,
            // We can reuse row codec.
            row_codec: self.row_codec.clone(),
            pk_id_cache: self.pk_id_cache.clone(),
            parts: RwLock::new(parts),
        }
    }

    fn write_with_key(&self, primary_key: &[u8], kv: KeyValue) -> Result<()> {
        // Safety: `write()` ensures this is not None.
        let cache = self.pk_id_cache.as_ref().unwrap();
        if let Some(pk_id) = cache.get(primary_key) {
            // The pk is in the cache.
            self.write_with_id(pk_id, kv);
            return Ok(());
        }

        // The pk is not in the cache, we need to write the pk to the index.
        let pk_id = self.write_primary_key(primary_key)?;
        // Also writes the pk to the cache.
        self.add_pk_to_cache(primary_key, pk_id);
        // Writes data.
        self.write_with_id(pk_id, kv);

        Ok(())
    }

    fn write_with_id(&self, pk_id: PkId, kv: KeyValue) {
        let mut parts = self.parts.write().unwrap();
        assert!(!parts.immutable);
        parts.data_buffer.write_row(pk_id, kv)
    }

    fn write_primary_key(&self, key: &[u8]) -> Result<PkId> {
        let index = {
            let parts = self.parts.read().unwrap();
            assert!(!parts.immutable);
            // Safety: The region has primary keys.
            parts.index.clone().unwrap()
        };

        index.write_primary_key(key)
    }

    fn add_pk_to_cache(&self, primary_key: &[u8], pk_id: PkId) {
        let Some(pk_id_cache) = &self.pk_id_cache else {
            return;
        };
        pk_id_cache.insert(primary_key.to_vec(), pk_id);
        CACHE_BYTES
            .with_label_values(&[PK_ID_TYPE])
            .add(pk_id_cache_weight(primary_key, &pk_id).into())
    }
}

struct TreeParts {
    /// Whether the tree is immutable.
    immutable: bool,
    /// Index part of the tree. If the region doesn't have a primary key, this field
    /// is `None`.
    // TODO(yingwen): Support freezing the index.
    index: Option<KeyIndexRef>,
    /// Data buffer of the tree.
    data_buffer: DataBuffer,
}

/// Maps primary key to [PkId].
type PkIdCache = Cache<Vec<u8>, PkId>;

fn pk_id_cache_weight(k: &[u8], _v: &PkId) -> u32 {
    (k.len() + std::mem::size_of::<PkId>()) as u32
}

fn new_cache(cache_size: u64) -> PkIdCache {
    PkIdCache::builder()
        .max_capacity(cache_size)
        .weigher(|k, v| pk_id_cache_weight(k.as_slice(), v))
        .eviction_listener(|k, v, _cause| {
            let size = pk_id_cache_weight(&k, &v);
            CACHE_BYTES
                .with_label_values(&[PK_ID_TYPE])
                .sub(size.into());
        })
        .build()
}
