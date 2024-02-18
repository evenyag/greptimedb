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

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use api::v1::OpType;
use common_recordbatch::filter::{ScalarValue, SimpleFilterEvaluator};
use common_time::Timestamp;
use datatypes::arrow;
use datatypes::data_type::ConcreteDataType;
use datatypes::value::ValueRef;
use snafu::ensure;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::{PrimaryKeyLengthMismatchSnafu, Result};
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::data::{self, DataBatch, DataParts};
use crate::memtable::merge_tree::index::{
    compute_pk_weights, IndexConfig, IndexReader, KeyIndex, KeyIndexRef, ShardReader,
};
use crate::memtable::merge_tree::mutable::{ReadMetrics, WriteMetrics};
use crate::memtable::merge_tree::{MergeTreeConfig, PkId, PkIndex, ShardId};
use crate::memtable::time_series::primary_key_schema;
use crate::memtable::{BoxedBatchIterator, KeyValues};
use crate::read::{Batch, BatchBuilder};
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};

/// Initial capacity for the data buffer.
const DATA_INIT_CAP: usize = 8;

type PartitionKey = u64;

/// The merge tree.
pub(crate) struct MergeTree {
    /// Config of the tree.
    config: MergeTreeConfig,
    /// Metadata of the region.
    pub(crate) metadata: RegionMetadataRef,
    /// Primary key codec.
    row_codec: Arc<McmpRowCodec>,
    parts: RwLock<PartitionTreeParts>,
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

        MergeTree {
            config: config.clone(),
            metadata,
            row_codec: Arc::new(row_codec),
            parts: RwLock::new(PartitionTreeParts::default()),
        }
    }

    // TODO(yingwen): The size computed from values is inaccurate.
    /// Write key-values into the tree.
    ///
    /// # Panics
    /// Panics if the tree is immutable (frozen).
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
                self.write_no_key(0, kv)?;
                continue;
            }

            // Encode primary key.
            primary_key.clear();
            self.row_codec
                .encode_to_vec(kv.primary_keys(), &mut primary_key)?;

            // Write rows with primary keys.
            self.write_with_key(&primary_key, kv, metrics)?;
        }

        metrics.value_bytes +=
            kvs.num_rows() * (std::mem::size_of::<Timestamp>() + std::mem::size_of::<OpType>());

        Ok(())
    }

    /// Scans the tree.
    pub(crate) fn scan(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: Option<Predicate>,
    ) -> Result<BoxedBatchIterator> {
        let mut metrics = ReadMetrics::default();
        let init_start = Instant::now();

        // Creates the projection set.
        let projection: HashSet<_> = if let Some(projection) = projection {
            projection.iter().copied().collect()
        } else {
            self.metadata.field_columns().map(|c| c.column_id).collect()
        };

        let simple_filters = predicate
            .map(|p| {
                p.exprs()
                    .iter()
                    .filter_map(|f| SimpleFilterEvaluator::try_new(f.df_expr()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let partition_keys = self.partition_keys(&simple_filters);
        metrics.num_partitions_before_prune = partition_keys.len();
        let mut partitions = VecDeque::with_capacity(partition_keys.len());
        for partition in partition_keys {
            let num_shards = {
                let parts = self.parts.read().unwrap();
                let parts_vec = parts.parts.get(&partition).unwrap();
                parts_vec.len()
            };

            for i in 0..num_shards {
                let iter = self.scan_part_shard(partition, i)?;
                partitions.push_back(iter);
            }
        }

        let pk_schema = primary_key_schema(&self.metadata);
        let pk_datatypes = self
            .metadata
            .primary_key_columns()
            .map(|pk| pk.column_schema.data_type.clone())
            .collect();

        metrics.init_cost = init_start.elapsed();
        metrics.num_partitions = partitions.len();
        let iter = PartitionIter {
            metadata: self.metadata.clone(),
            pk_schema,
            pk_datatypes,
            simple_filters,
            projection,
            row_codec: self.row_codec.clone(),
            partitions,
            metrics,
        };

        Ok(Box::new(iter))
    }

    fn partition_keys(&self, filters: &[SimpleFilterEvaluator]) -> Vec<PartitionKey> {
        let parts = self.parts.read().unwrap();
        // Prune partition keys.
        if let Some(partition_col) = self.metadata.primary_key_columns().next() {
            for filter in filters {
                // Only the first filter takes effect.
                if filter.column_name() == partition_col.column_schema.name {
                    let mut partition_keys = Vec::new();
                    for key in parts.parts.keys() {
                        match partition_col.column_schema.data_type {
                            ConcreteDataType::UInt32(_) => {
                                if filter
                                    .evaluate_scalar(&ScalarValue::UInt32(Some(*key as u32)))
                                    .unwrap_or(true)
                                {
                                    partition_keys.push(*key);
                                }
                            }
                            ConcreteDataType::UInt64(_) => {
                                if filter
                                    .evaluate_scalar(&ScalarValue::UInt64(Some(*key)))
                                    .unwrap_or(true)
                                {
                                    partition_keys.push(*key);
                                }
                            }
                            _ => partition_keys.push(*key),
                        }
                    }

                    return partition_keys;
                }
            }
        }

        parts.parts.keys().copied().collect()
    }

    fn scan_part_shard(&self, partition: PartitionKey, shard_index: usize) -> Result<ShardIter> {
        let index = {
            let parts = self.parts.read().unwrap();
            let parts_vec = parts.parts.get(&partition).unwrap();
            parts_vec[shard_index].index.clone()
        };
        let index_reader = index
            .as_ref()
            .map(|index| index.scan_shard(0))
            .transpose()?;
        // Compute pk weights.
        let mut pk_weights = Vec::new();
        if let Some(reader) = &index_reader {
            compute_pk_weights(reader.sorted_pk_index(), &mut pk_weights);
        } else {
            // Push weight for the only key.
            // TODO(yingwen): Allow passing empty weights if there is no primary key.
            pk_weights.push(0);
        }

        let data_iter = {
            let mut parts = self.parts.write().unwrap();
            parts.parts.get_mut(&partition).unwrap()[shard_index]
                .data
                .iter(pk_weights)?
        };

        let iter = ShardIter {
            metadata: self.metadata.clone(),
            index_reader,
            data_reader: DataReader::new(data_iter)?,
        };

        Ok(iter)
    }

    /// Returns true if the tree is empty.
    pub(crate) fn is_empty(&self) -> bool {
        let parts = self.parts.read().unwrap();
        // Gets whether the memtable is empty from the data part.
        parts.is_empty()
        // TODO(yingwen): Also consider other parts if we freeze the data buffer.
    }

    /// Marks the tree as immutable.
    ///
    /// Once the tree becomes immutable, callers should not write to it again.
    pub(crate) fn freeze(&self) -> Result<()> {
        let mut parts = self.parts.write().unwrap();
        parts.freeze()?;

        Ok(())
    }

    /// Forks an immutable tree. Returns a mutable tree that inherits the index
    /// of this tree.
    pub(crate) fn fork(&self, metadata: RegionMetadataRef) -> MergeTree {
        if metadata.primary_key != self.metadata.primary_key {
            // The priamry key is changed. We can't reuse fields.
            return MergeTree::new(metadata, &self.config);
        }

        let current_parts = self.parts.read().unwrap();
        let parts = current_parts.fork(&metadata, &self.config);

        MergeTree {
            config: self.config.clone(),
            metadata,
            // We can reuse row codec.
            row_codec: self.row_codec.clone(),
            parts: RwLock::new(parts),
        }
    }

    /// Returns the memory size of shared parts.
    pub(crate) fn shared_memory_size(&self) -> usize {
        let parts = self.parts.read().unwrap();
        parts.shared_memory_size()
    }

    fn write_with_key(
        &self,
        primary_key: &[u8],
        kv: KeyValue,
        metrics: &mut WriteMetrics,
    ) -> Result<()> {
        let partition = compute_partition_key(kv.primary_keys().next().unwrap());
        // Write the pk to the index.
        let pk_id = self.write_primary_key(partition, primary_key, metrics)?;
        // Writes data.
        self.write_with_id(partition, pk_id, kv)
    }

    fn write_no_key(&self, partition: PartitionKey, kv: KeyValue) -> Result<()> {
        let mut parts = self.parts.write().unwrap();
        let tree_parts = parts.get_or_create_parts(partition, &self.metadata, &self.config);
        // Now we always assign the first shard and the first pk index to the id.
        let pk_id = PkId {
            shard_id: 0,
            pk_index: 0,
        };
        tree_parts.last_mut().unwrap().write_with_id(pk_id, kv)?;
        parts.num_rows += 1;
        Ok(())
    }

    fn write_with_id(&self, partition: PartitionKey, pk_id: PkId, kv: KeyValue) -> Result<()> {
        let mut parts = self.parts.write().unwrap();
        let tree_parts = parts.get_parts_by_pkid(partition, pk_id);
        tree_parts.write_with_id(pk_id, kv)?;
        parts.num_rows += 1;
        Ok(())
    }

    fn write_primary_key(
        &self,
        partition: PartitionKey,
        key: &[u8],
        metrics: &mut WriteMetrics,
    ) -> Result<PkId> {
        {
            let parts = self.parts.read().unwrap();
            if let Some(parts_vec) = parts.parts.get(&partition) {
                for parts in parts_vec {
                    // Safety: The region has primary key.
                    if let Some(pk_id) = parts.index.as_ref().unwrap().get_pk_id(key) {
                        return Ok(pk_id);
                    }
                }
            }
        }

        let index = {
            let mut parts = self.parts.write().unwrap();
            let tree_parts = parts.get_or_create_parts(partition, &self.metadata, &self.config);
            // We don't check whether the key is already added in previous shards now. It is possible
            // to write a key into multiple shards.
            tree_parts.last().unwrap().index.clone().unwrap()
        };

        let pk_opt = index.write_primary_key(key, metrics)?;
        match pk_opt {
            Some(v) => Ok(v),
            None => {
                panic!(
                    "region {} partition {} shard is full",
                    self.metadata.region_id, partition
                );
            }
        }
    }
}

fn compute_partition_key(value: ValueRef) -> PartitionKey {
    match value {
        ValueRef::UInt32(v) => v.into(),
        ValueRef::UInt64(v) => v,
        _ => 0,
    }
}

#[derive(Default)]
struct PartitionTreeParts {
    // FIXME(yingwen): We should merge parts under the same partition.
    parts: BTreeMap<PartitionKey, Vec<TreeParts>>,
    num_rows: usize,
    next_shard_id: ShardId,
}

impl PartitionTreeParts {
    fn get_or_create_parts(
        &mut self,
        partition: PartitionKey,
        metadata: &RegionMetadataRef,
        config: &MergeTreeConfig,
    ) -> &mut Vec<TreeParts> {
        let parts_vec = self.parts.entry(partition).or_insert_with(|| {
            let shard_id = self.next_shard_id;
            self.next_shard_id += 1;
            vec![TreeParts::new(metadata, config, shard_id)]
        });
        assert!(!parts_vec.last().unwrap().immutable);
        // Safety: The region has primary keys.
        if parts_vec.last().unwrap().index.as_ref().unwrap().is_full() {
            common_telemetry::info!(
                "Adds a new shard {} to region {} partition {}",
                parts_vec.len(),
                metadata.region_id,
                partition
            );
            parts_vec.push(TreeParts::new(metadata, config, self.next_shard_id));
            self.next_shard_id += 1;
        }
        parts_vec
    }

    fn get_parts_by_pkid(&mut self, partition: PartitionKey, pk_id: PkId) -> &mut TreeParts {
        let parts_vec = self.parts.get_mut(&partition).unwrap();
        assert!(!parts_vec.last().unwrap().immutable);
        for parts in parts_vec {
            if parts.shard_id == pk_id.shard_id {
                return parts;
            }
        }

        panic!(
            "partition {} shard for pk id {:?} not found",
            partition, pk_id
        )
    }

    fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    fn freeze(&mut self) -> Result<()> {
        for parts_vec in self.parts.values_mut() {
            for p in parts_vec {
                p.freeze()?;
            }
        }

        Ok(())
    }

    fn fork(&self, metadata: &RegionMetadataRef, config: &MergeTreeConfig) -> PartitionTreeParts {
        let mut parts = BTreeMap::new();
        for (k, parts_vec) in &self.parts {
            let new_parts_vec = parts_vec
                .iter()
                .map(|v| v.fork(metadata.clone(), config))
                .collect();
            parts.insert(*k, new_parts_vec);
        }

        PartitionTreeParts {
            parts,
            num_rows: 0,
            next_shard_id: self.next_shard_id,
        }
    }

    fn shared_memory_size(&self) -> usize {
        self.parts
            .values()
            .map(|parts_vec| {
                parts_vec
                    .iter()
                    .map(|parts| parts.shared_memory_size())
                    .sum::<usize>()
            })
            .sum()
    }
}

pub(crate) struct TreeParts {
    shard_id: ShardId,
    /// Whether the tree is immutable.
    immutable: bool,
    /// Index part of the tree. If the region doesn't have a primary key, this field
    /// is `None`.
    index: Option<KeyIndexRef>,
    /// Data part of the tree.
    pub(crate) data: DataParts,
}

impl TreeParts {
    fn new(metadata: &RegionMetadataRef, config: &MergeTreeConfig, shard_id: ShardId) -> Self {
        let index = (!metadata.primary_key.is_empty()).then(|| {
            Arc::new(KeyIndex::new(
                IndexConfig {
                    max_keys_per_shard: config.index_max_keys_per_shard,
                },
                shard_id,
            ))
        });
        let data =
            DataParts::with_capacity(metadata.clone(), DATA_INIT_CAP, config.freeze_threshold);
        TreeParts {
            shard_id,
            immutable: false,
            index,
            data,
        }
    }

    fn write_with_id(&mut self, pk_id: PkId, kv: KeyValue) -> Result<()> {
        assert!(!self.immutable);
        if self.data.write_row(pk_id, kv) {
            // should trigger freeze
            let weights = if let Some(index) = self.index.as_ref() {
                let pk_indices = index.sorted_pk_indices();
                let mut weights = Vec::with_capacity(pk_indices.len());
                compute_pk_weights(&pk_indices, &mut weights);
                weights
            } else {
                vec![0]
            };
            self.data.freeze(&weights)
        } else {
            Ok(())
        }
    }

    fn freeze(&mut self) -> Result<()> {
        self.immutable = true;
        // Freezes the index.
        if let Some(index) = &self.index {
            index.freeze()?;
        }

        Ok(())
    }

    fn fork(&self, metadata: RegionMetadataRef, config: &MergeTreeConfig) -> TreeParts {
        let index = self.index.as_ref().map(|index| Arc::new(index.fork()));
        // New parts.
        TreeParts {
            shard_id: self.shard_id,
            immutable: false,
            index,
            data: DataParts::new(metadata, DATA_INIT_CAP, config.freeze_threshold),
        }
    }

    fn shared_memory_size(&self) -> usize {
        self.index
            .as_ref()
            .map(|index| index.memory_size())
            .unwrap_or(0)
    }
}

struct PartitionIter {
    metadata: RegionMetadataRef,
    pk_schema: arrow::datatypes::SchemaRef,
    pk_datatypes: Vec<ConcreteDataType>,
    simple_filters: Vec<SimpleFilterEvaluator>,
    projection: HashSet<ColumnId>,
    row_codec: Arc<McmpRowCodec>,
    partitions: VecDeque<ShardIter>,
    metrics: ReadMetrics,
}

impl Iterator for PartitionIter {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        let start = Instant::now();
        let ret = self.next_batch().transpose();
        self.metrics.next_cost += start.elapsed();
        ret
    }
}

impl PartitionIter {
    fn next_batch(&mut self) -> Result<Option<Batch>> {
        while let Some(iter) = self.partitions.front_mut() {
            while let Some(batch) = iter.next_batch(&self.projection)? {
                self.metrics.num_batches_before_prune += 1;
                self.metrics.num_rows_before_prune += batch.num_rows();

                // Prune primary key.
                if !prune_primary_key(
                    &self.row_codec,
                    batch.primary_key(),
                    &self.pk_datatypes,
                    &self.pk_schema,
                    &self.simple_filters,
                ) {
                    continue;
                }

                self.metrics.num_batches += 1;
                self.metrics.num_rows_returned += batch.num_rows();
                return Ok(Some(batch));
            }
            self.partitions.pop_front();
        }

        Ok(None)
    }
}

impl Drop for PartitionIter {
    fn drop(&mut self) {
        common_telemetry::info!("PartitionIter drop, metrics: {:?}", self.metrics);
    }
}

fn prune_primary_key(
    codec: &Arc<McmpRowCodec>,
    pk: &[u8],
    datatypes: &[ConcreteDataType],
    pk_schema: &arrow::datatypes::SchemaRef,
    predicates: &[SimpleFilterEvaluator],
) -> bool {
    // no primary key, we simply return true.
    if pk_schema.fields().is_empty() {
        return true;
    }

    let pk_values = codec.decode(pk);
    if let Err(e) = pk_values {
        common_telemetry::error!(e; "Failed to decode primary key");
        return true;
    }
    let pk_values = pk_values.unwrap();

    // evaluate predicates against primary key values
    let mut result = true;
    for predicate in predicates {
        // ignore predicates that are not referencing primary key columns
        let Ok(index) = pk_schema.index_of(predicate.column_name()) else {
            continue;
        };
        // Safety: arrow schema and datatypes are constructed from the same source.
        let scalar_value = pk_values[index]
            .try_to_scalar_value(&datatypes[index])
            .unwrap();
        result &= predicate.evaluate_scalar(&scalar_value).unwrap_or(true);
    }

    result
}

struct ShardIter {
    metadata: RegionMetadataRef,
    index_reader: Option<ShardReader>,
    data_reader: DataReader,
}

impl ShardIter {
    /// Fetches next batch and advances the iter.
    fn next_batch(&mut self, projection: &HashSet<ColumnId>) -> Result<Option<Batch>> {
        if !self.data_reader.is_valid() {
            return Ok(None);
        }

        let Some(index_reader) = &mut self.index_reader else {
            // No primary key to read.
            // Safety: `next()` ensures the data reader is valid.
            let batch =
                self.data_reader
                    .convert_current_record_batch(&self.metadata, projection, &[])?;
            // Advances the data reader.
            self.data_reader.next()?;
            return Ok(Some(batch));
        };

        // Iterate the index reader until we see the same pk index of the data batch.
        while index_reader.is_valid()
            && index_reader.current_pk_index() != self.data_reader.current_pk_index()
        {
            index_reader.next();
        }
        assert!(
            index_reader.is_valid(),
            "Data contains pk_index {} not in the index",
            self.data_reader.current_pk_index()
        );

        let batch = self.data_reader.convert_current_record_batch(
            &self.metadata,
            projection,
            index_reader.current_key(),
        )?;
        // Advances the data reader.
        self.data_reader.next()?;
        Ok(Some(batch))
    }
}

struct DataReader {
    current: Option<DataBatch>,
    iter: data::Iter,
}

impl DataReader {
    fn new(mut iter: data::Iter) -> Result<Self> {
        let current = iter.next().transpose()?;

        Ok(Self { current, iter })
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn current_pk_index(&self) -> PkIndex {
        self.current.as_ref().unwrap().pk_index()
    }

    /// Converts current [RecordBatch] to [Batch].
    fn convert_current_record_batch(
        &self,
        metadata: &RegionMetadataRef,
        projection: &HashSet<ColumnId>,
        primary_key: &[u8],
    ) -> Result<Batch> {
        let data_batch = self.current.as_ref().unwrap();
        let offset = data_batch.range().start;
        let length = data_batch.range().len();
        let record_batch = data_batch.record_batch();

        let mut builder = BatchBuilder::new(primary_key.to_vec());
        builder
            .timestamps_array(record_batch.column(1).slice(offset, length))?
            .sequences_array(record_batch.column(2).slice(offset, length))?
            .op_types_array(record_batch.column(3).slice(offset, length))?;

        // TODO(yingwen): Pushdown projection to data parts.
        if record_batch.num_columns() <= 4 {
            // No fields.
            return builder.build();
        }

        // Iterate all field columns.
        for (array, field) in record_batch
            .columns()
            .iter()
            .zip(record_batch.schema().fields().iter())
            .skip(4)
        {
            // Safety: metadata should contain all fields.
            let column_id = metadata.column_by_name(field.name()).unwrap().column_id;
            if !projection.contains(&column_id) {
                continue;
            }
            builder.push_field_array(column_id, array.slice(offset, length))?;
        }

        builder.build()
    }

    fn next(&mut self) -> Result<()> {
        self.current = self.iter.next().transpose()?;
        Ok(())
    }
}
