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
    ArrayBuilder, BinaryArray, BinaryBuilder, UInt64Array, UInt64Builder,
};
use datatypes::arrow::compute;
use snafu::ResultExt;

use crate::error::{ComputeArrowSnafu, Result};

/// Id of a primary key.
type PkId = u64;

/// Config for the index.
struct IndexConfig {
    /// Max keys in a dictionary block.
    max_keys_per_dict: usize,
    // TODO(yingwen): Also consider byte size.
}

// TODO(yingwen): Support partition index (partition by a column, e.g. table_id) to
// reduce null columns and eliminate lock contention. We only need to partition the
// write buffer but modify dicts with partition lock held.
/// Index for primary keys.
struct KeyIndex {
    write_buffer: WriteBuffer,
    dicts: Vec<DictBlock>,
}

impl KeyIndex {
    fn add_primary_key(&mut self, config: &IndexConfig, key: &[u8]) -> Result<PkId> {
        let pkid = self.write_buffer.push_key(key);

        if self.write_buffer.len() < config.max_keys_per_dict {
            return Ok(pkid);
        }

        // TODO(yingwen): Freeze buffer to dicts.
        // The write buffer is full.
        let dict_block = self.write_buffer.finish_dict_block()?;
        self.dicts.push(dict_block);

        Ok(pkid)
    }
}

// TODO(yingwen): Bench using custom container for binary and ids so we can
// sort the buffer in place and reuse memory.
struct DictBlockBuilder {
    // We use arrow's binary builder as out default binary builder
    // is LargeBinaryBuilder
    primary_key: BinaryBuilder,
    pkid: UInt64Builder,
}

impl DictBlockBuilder {
    fn push_key(&mut self, key: &[u8], pkid: PkId) {
        self.primary_key.append_value(key);
        self.pkid.append_value(pkid);
    }

    /// Builds and sorts the key dict.
    fn finish(&mut self) -> Result<DictBlock> {
        // TODO(yingwen): We can check whether keys are already sorted first. But
        // we might need some benchmarks.
        let primary_key = self.primary_key.finish();
        let pkid = self.pkid.finish();

        DictBlock::try_new(primary_key, pkid)
    }

    fn finish_cloned(&self) -> Result<DictBlock> {
        let primary_key = self.primary_key.finish_cloned();
        let pkid = self.pkid.finish_cloned();

        DictBlock::try_new(primary_key, pkid)
    }

    fn len(&self) -> usize {
        self.primary_key.len()
    }
}

struct WriteBuffer {
    builder: DictBlockBuilder,
    next_pkid: PkId,
}

impl WriteBuffer {
    fn push_key(&mut self, key: &[u8]) -> PkId {
        let pkid = self.next_pkid;
        self.next_pkid += 1;
        self.builder.push_key(key, pkid);
        pkid
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
    pkid: UInt64Array,
}

impl DictBlock {
    fn try_new(primary_key: BinaryArray, pkid: UInt64Array) -> Result<Self> {
        // Sort by primary key.
        let indices =
            compute::sort_to_indices(&primary_key, None, None).context(ComputeArrowSnafu)?;
        let primary_key = compute::take(&primary_key, &indices, None).context(ComputeArrowSnafu)?;
        let pkid = compute::take(&pkid, &indices, None).context(ComputeArrowSnafu)?;

        let dict = DictBlock {
            primary_key: primary_key
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .clone(),
            pkid: pkid.as_any().downcast_ref::<UInt64Array>().unwrap().clone(),
        };
        Ok(dict)
    }
}
