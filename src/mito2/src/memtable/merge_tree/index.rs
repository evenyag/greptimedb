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

//! Primary key index of the merge tree.

use datatypes::arrow::array::{
    ArrayBuilder, BinaryArray, BinaryBuilder, UInt16Array, UInt16Builder,
};
use datatypes::arrow::compute;
use snafu::ResultExt;

use crate::error::{ComputeArrowSnafu, Result};

// TODO(yingwen): Consider using byte size to manage block.
/// Maximum keys in a block. Should be power of 2.
const MAX_KEYS_PER_BLOCK: usize = 256;

/// Id of a shard.
type ShardId = u32;
/// Index of a primary key in a shard.
type PkIndex = u16;
/// Id of a primary key.
struct PkId {
    shard_id: ShardId,
    pk_index: PkIndex,
}

/// Config for the index.
struct IndexConfig {
    /// Max keys in an index shard.
    max_keys_per_shard: usize,
}

/// Primary key index.
struct KeyIndex {
    //
}

// TODO(yingwen): Support partition index (partition by a column, e.g. table_id) to
// reduce null columns and eliminate lock contention. We only need to partition the
// write buffer but modify dicts with partition lock held.
/// Mutable shard for the index.
struct MutableShard {
    shard_id: ShardId,
    write_buffer: WriteBuffer,
    dict_blocks: Vec<DictBlock>,
    num_keys: usize,
}

impl MutableShard {
    fn try_add_primary_key(&mut self, config: &IndexConfig, key: &[u8]) -> Result<Option<PkId>> {
        // The shard is full.
        if self.num_keys >= config.max_keys_per_shard {
            return Ok(None);
        }

        if self.write_buffer.len() >= MAX_KEYS_PER_BLOCK {
            // The write buffer is full.
            let dict_block = self.write_buffer.finish_dict_block()?;
            self.dict_blocks.push(dict_block);
        }

        let pk_index = self.write_buffer.push_key(key);

        Ok(Some(PkId {
            shard_id: self.shard_id,
            pk_index,
        }))
    }
}

// TODO(yingwen): Bench using custom container for binary and ids so we can
// sort the buffer in place and reuse memory.
struct DictBlockBuilder {
    // We use arrow's binary builder as out default binary builder
    // is LargeBinaryBuilder
    primary_key: BinaryBuilder,
    // TODO(yingwen): We don't need to store index in the builder, we only need
    // to store start index.
    pk_index: UInt16Builder,
}

impl DictBlockBuilder {
    fn push_key(&mut self, key: &[u8], index: PkIndex) {
        self.primary_key.append_value(key);
        self.pk_index.append_value(index);
    }

    /// Builds and sorts the key dict.
    fn finish(&mut self) -> Result<DictBlock> {
        // TODO(yingwen): We can check whether keys are already sorted first. But
        // we might need some benchmarks.
        let primary_key = self.primary_key.finish();
        let pk_index = self.pk_index.finish();

        DictBlock::try_new(primary_key, pk_index)
    }

    fn finish_cloned(&self) -> Result<DictBlock> {
        let primary_key = self.primary_key.finish_cloned();
        let pk_index = self.pk_index.finish_cloned();

        DictBlock::try_new(primary_key, pk_index)
    }

    fn len(&self) -> usize {
        self.primary_key.len()
    }
}

struct WriteBuffer {
    builder: DictBlockBuilder,
    next_index: usize,
}

impl WriteBuffer {
    /// Push a new key.
    ///
    /// # Panics
    /// Panics if the index will overflow.
    fn push_key(&mut self, key: &[u8]) -> PkIndex {
        let pk_index = self.next_index.try_into().unwrap();
        self.next_index += 1;
        self.builder.push_key(key, pk_index);

        pk_index
    }

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn finish_dict_block(&mut self) -> Result<DictBlock> {
        self.builder.finish()
    }
}

struct DictBlock {
    primary_key: BinaryArray,
    pk_index: UInt16Array,
}

impl DictBlock {
    fn try_new(primary_key: BinaryArray, pk_index: UInt16Array) -> Result<Self> {
        // Sort by primary key.
        let indices =
            compute::sort_to_indices(&primary_key, None, None).context(ComputeArrowSnafu)?;
        let primary_key = compute::take(&primary_key, &indices, None).context(ComputeArrowSnafu)?;
        let pk_index = compute::take(&pk_index, &indices, None).context(ComputeArrowSnafu)?;

        let dict = DictBlock {
            primary_key: primary_key
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .clone(),
            pk_index: pk_index
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .clone(),
        };
        Ok(dict)
    }
}
