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

//! Key dictionary of a shard.

use std::collections::HashMap;
use std::sync::Arc;

use datatypes::arrow::array::{Array, ArrayBuilder, BinaryArray, BinaryBuilder};

use crate::memtable::merge_tree::metrics::WriteMetrics;
use crate::memtable::merge_tree::PkIndex;

/// Maximum keys in a [DictBlock].
const MAX_KEYS_PER_BLOCK: u16 = 256;

type PkIndexMap = HashMap<Vec<u8>, PkIndex>;

/// Builder to build a key dictionary.
pub struct KeyDictBuilder {
    /// Max keys of the dictionary.
    capacity: usize,
    /// Number of keys in the builder.
    num_keys: usize,
    /// Maps primary key to pk index.
    pk_to_index: PkIndexMap,
    /// Buffer for active dict block.
    key_buffer: KeyBuffer,
    /// Dictionary blocks.
    dict_blocks: Vec<DictBlock>,
    /// Bytes allocated by keys in the [pk_to_index](Self::pk_to_index).
    key_bytes_in_index: usize,
}

impl KeyDictBuilder {
    /// Creates a new builder that can hold up to `capacity` keys.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            num_keys: 0,
            pk_to_index: HashMap::new(),
            key_buffer: KeyBuffer::new(MAX_KEYS_PER_BLOCK.into()),
            dict_blocks: Vec::with_capacity(capacity / MAX_KEYS_PER_BLOCK as usize + 1),
            key_bytes_in_index: 0,
        }
    }

    /// Returns true if the builder is full.
    pub fn is_full(&self) -> bool {
        self.num_keys >= self.capacity
    }

    /// Adds the key to the builder and returns its index if the builder is not full.
    ///
    /// # Panics
    /// Panics if the builder is full.
    pub fn insert_key(&mut self, key: &[u8], metrics: &mut WriteMetrics) -> PkIndex {
        assert!(!self.is_full());

        if let Some(pk_index) = self.pk_to_index.get(key).copied() {
            // Already in the builder.
            return pk_index;
        }

        if self.key_buffer.len() >= MAX_KEYS_PER_BLOCK.into() {
            // The write buffer is full. Freeze a dict block.
            let dict_block = self.key_buffer.finish(false);
            self.dict_blocks.push(dict_block);
        }

        // Safety: we have checked the buffer length.
        let pk_index = self.key_buffer.push_key(key);
        self.pk_to_index.insert(key.to_vec(), pk_index);
        self.num_keys += 1;

        // Since we store the key twice so the bytes usage doubled.
        metrics.key_bytes += key.len() * 2;
        self.key_bytes_in_index += key.len();

        pk_index
    }

    /// Memory size of the builder.
    #[cfg(test)]
    pub fn memory_size(&self) -> usize {
        self.key_bytes_in_index
            + self.key_buffer.buffer_memory_size()
            + self
                .dict_blocks
                .iter()
                .map(|block| block.buffer_memory_size())
                .sum::<usize>()
    }

    /// Finishes the builder.
    pub fn finish(&mut self) -> Option<KeyDict> {
        if self.key_buffer.is_empty() {
            return None;
        }

        // Finishes current dict block and resets the pk index.
        let dict_block = self.key_buffer.finish(true);
        self.dict_blocks.push(dict_block);
        // Takes the pk to index map.
        let pk_to_index = std::mem::take(&mut self.pk_to_index);
        // Computes key position and then alter pk index.
        let mut key_positions = vec![0; pk_to_index.len()];
        let mut key_pk_indices = pk_to_index.into_iter().collect::<Vec<_>>();
        key_pk_indices.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut pk_to_index = HashMap::with_capacity(key_pk_indices.len());
        for (i, (key, pk_index)) in key_pk_indices.into_iter().enumerate() {
            // The position of the i-th key is the old pk index.
            key_positions[i] = pk_index;
            // Overwrites the pk index.
            pk_to_index.insert(key, i as PkIndex);
        }
        self.num_keys = 0;

        Some(KeyDict {
            pk_to_index,
            dict_blocks: std::mem::take(&mut self.dict_blocks),
            key_positions,
            key_bytes_in_index: self.key_bytes_in_index,
        })
    }

    /// Reads the builder.
    pub fn read(&self) -> DictBuilderReader {
        let mut key_pk_indices = self
            .pk_to_index
            .iter()
            .map(|(k, v)| (k, *v))
            .collect::<Vec<_>>();
        key_pk_indices.sort_unstable_by_key(|kv| kv.0);
        let sorted_pk_indices = key_pk_indices
            .iter()
            .map(|(_key, pk_index)| *pk_index)
            .collect();
        let block = self.key_buffer.finish_cloned();
        let mut blocks = Vec::with_capacity(self.dict_blocks.len() + 1);
        blocks.extend_from_slice(&self.dict_blocks);
        blocks.push(block);

        DictBuilderReader::new(blocks, sorted_pk_indices)
    }
}

/// Reader to scan the [KeyDictBuilder].
#[derive(Default)]
pub struct DictBuilderReader {
    blocks: Vec<DictBlock>,
    sorted_pk_indices: Vec<PkIndex>,
}

impl DictBuilderReader {
    fn new(blocks: Vec<DictBlock>, sorted_pk_indices: Vec<PkIndex>) -> Self {
        Self {
            blocks,
            sorted_pk_indices,
        }
    }

    /// Returns the number of keys.
    #[cfg(test)]
    pub fn num_keys(&self) -> usize {
        self.sorted_pk_indices.len()
    }

    /// Gets the i-th pk index.
    #[cfg(test)]
    pub fn pk_index(&self, offset: usize) -> PkIndex {
        self.sorted_pk_indices[offset]
    }

    /// Gets the i-th key.
    #[cfg(test)]
    pub fn key(&self, offset: usize) -> &[u8] {
        let pk_index = self.pk_index(offset);
        self.key_by_pk_index(pk_index)
    }

    /// Gets the key by the pk index.
    pub fn key_by_pk_index(&self, pk_index: PkIndex) -> &[u8] {
        let block_idx = pk_index / MAX_KEYS_PER_BLOCK;
        self.blocks[block_idx as usize].key_by_pk_index(pk_index)
    }

    /// Returns pk weights to sort a data part and replaces pk indices.
    pub(crate) fn pk_weights_to_sort_data(&self, pk_weights: &mut Vec<u16>) {
        compute_pk_weights(&self.sorted_pk_indices, pk_weights)
    }
}

/// Returns pk weights to sort a data part and replaces pk indices.
fn compute_pk_weights(sorted_pk_indices: &[PkIndex], pk_weights: &mut Vec<u16>) {
    pk_weights.resize(sorted_pk_indices.len(), 0);
    for (weight, pk_index) in sorted_pk_indices.iter().enumerate() {
        pk_weights[*pk_index as usize] = weight as u16;
    }
}

/// A key dictionary.
#[derive(Default)]
pub struct KeyDict {
    // TODO(yingwen): We can use key_positions to do a binary search.
    /// Key map to find a key in the dict.
    pk_to_index: PkIndexMap,
    /// Unsorted key blocks.
    dict_blocks: Vec<DictBlock>,
    /// Maps pk index to position of the key in [Self::dict_blocks].
    key_positions: Vec<PkIndex>,
    key_bytes_in_index: usize,
}

pub type KeyDictRef = Arc<KeyDict>;

impl KeyDict {
    /// Gets the primary key by its index.
    ///
    /// # Panics
    /// Panics if the index is invalid.
    pub fn key_by_pk_index(&self, index: PkIndex) -> &[u8] {
        let position = self.key_positions[index as usize];
        let block_index = position / MAX_KEYS_PER_BLOCK;
        self.dict_blocks[block_index as usize].key_by_pk_index(position)
    }

    /// Gets the pk index by the key.
    pub fn get_pk_index(&self, key: &[u8]) -> Option<PkIndex> {
        self.pk_to_index.get(key).copied()
    }

    /// Returns pk weights to sort a data part and replaces pk indices.
    pub(crate) fn pk_weights_to_sort_data(&self) -> Vec<u16> {
        let mut pk_weights = Vec::with_capacity(self.key_positions.len());
        compute_pk_weights(&self.key_positions, &mut pk_weights);
        pk_weights
    }

    /// Returns the shared memory size.
    pub(crate) fn shared_memory_size(&self) -> usize {
        self.key_bytes_in_index
    }
}

/// Buffer to store unsorted primary keys.
struct KeyBuffer {
    // We use arrow's binary builder as out default binary builder
    // is LargeBinaryBuilder
    // TODO(yingwen): Change the type binary vector to Binary instead of LargeBinary.
    /// Builder for binary key array.
    key_builder: BinaryBuilder,
    next_pk_index: usize,
}

impl KeyBuffer {
    fn new(item_capacity: usize) -> Self {
        Self {
            key_builder: BinaryBuilder::with_capacity(item_capacity, 0),
            next_pk_index: 0,
        }
    }

    /// Pushes a new key and returns its pk index.
    ///
    /// # Panics
    /// Panics if the [PkIndex] type cannot represent the index.
    fn push_key(&mut self, key: &[u8]) -> PkIndex {
        let pk_index = self.next_pk_index.try_into().unwrap();
        self.next_pk_index += 1;
        self.key_builder.append_value(key);

        pk_index
    }

    /// Returns number of items in the buffer.
    fn len(&self) -> usize {
        self.key_builder.len()
    }

    /// Returns whether the buffer is empty.
    fn is_empty(&self) -> bool {
        self.key_builder.is_empty()
    }

    /// Returns the buffer size of the builder.
    #[cfg(test)]
    fn buffer_memory_size(&self) -> usize {
        self.key_builder.values_slice().len()
            + std::mem::size_of_val(self.key_builder.offsets_slice())
            + self
                .key_builder
                .validity_slice()
                .map(|v| v.len())
                .unwrap_or(0)
    }

    fn finish(&mut self, reset_index: bool) -> DictBlock {
        let primary_key = self.key_builder.finish();
        // Reserve capacity for the new builder. `finish()` the builder will leave the builder
        // empty with capacity 0.
        // TODO(yingwen): Do we need to reserve capacity for data?
        self.key_builder = BinaryBuilder::with_capacity(primary_key.len(), 0);
        if reset_index {
            self.next_pk_index = 0;
        }

        DictBlock::new(primary_key)
    }

    fn finish_cloned(&self) -> DictBlock {
        let primary_key = self.key_builder.finish_cloned();

        DictBlock::new(primary_key)
    }
}

/// A block in the key dictionary.
///
/// The block is cheap to clone. Keys in the block are unsorted.
#[derive(Clone)]
struct DictBlock {
    /// Container of keys in the block.
    keys: BinaryArray,
}

impl DictBlock {
    fn new(keys: BinaryArray) -> Self {
        Self { keys }
    }

    fn key_by_pk_index(&self, index: PkIndex) -> &[u8] {
        let pos = index % MAX_KEYS_PER_BLOCK;
        self.keys.value(pos as usize)
    }

    #[cfg(test)]
    fn buffer_memory_size(&self) -> usize {
        self.keys.get_buffer_memory_size()
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    fn prepare_input_keys(num_keys: usize) -> Vec<Vec<u8>> {
        let prefix = ["a", "b", "c", "d", "e", "f"];
        let mut rng = rand::thread_rng();
        let mut keys = Vec::with_capacity(num_keys);
        for i in 0..num_keys {
            let prefix_idx = rng.gen_range(0..prefix.len());
            // We don't need to decode the primary key in index's test so we format the string
            // into the key.
            let key = format!("{}{}", prefix[prefix_idx], i);
            keys.push(key.into_bytes());
        }

        keys
    }

    #[test]
    fn test_write_scan_builder() {
        let num_keys = MAX_KEYS_PER_BLOCK * 2 + MAX_KEYS_PER_BLOCK / 2;
        let keys = prepare_input_keys(num_keys.into());

        let mut builder = KeyDictBuilder::new((MAX_KEYS_PER_BLOCK * 3).into());
        let mut last_pk_index = None;
        let mut metrics = WriteMetrics::default();
        for key in &keys {
            assert!(!builder.is_full());
            let pk_index = builder.insert_key(key, &mut metrics);
            last_pk_index = Some(pk_index);
        }
        assert_eq!(num_keys - 1, last_pk_index.unwrap());
        let key_bytes: usize = keys.iter().map(|key| key.len() * 2).sum();
        assert_eq!(key_bytes, metrics.key_bytes);

        let mut expect: Vec<_> = keys
            .into_iter()
            .enumerate()
            .map(|(i, key)| (key, i as PkIndex))
            .collect();
        expect.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let mut result = Vec::with_capacity(expect.len());
        let reader = builder.read();
        for i in 0..reader.num_keys() {
            result.push((reader.key(i).to_vec(), reader.pk_index(i)));
        }
        assert_eq!(expect, result);
    }

    #[test]
    fn test_builder_memory_size() {
        let mut builder = KeyDictBuilder::new((MAX_KEYS_PER_BLOCK * 3).into());
        let mut metrics = WriteMetrics::default();
        // 513 keys
        let num_keys = MAX_KEYS_PER_BLOCK * 2 + 1;
        // Writes 2 blocks
        for i in 0..num_keys {
            // Each key is 5 bytes.
            let key = format!("{i:05}");
            builder.insert_key(key.as_bytes(), &mut metrics);
        }
        // num_keys * 5 * 2
        assert_eq!(5130, metrics.key_bytes);
        assert_eq!(8850, builder.memory_size());
    }

    #[test]
    fn test_builder_finish() {
        let mut builder = KeyDictBuilder::new((MAX_KEYS_PER_BLOCK * 2).into());
        let mut metrics = WriteMetrics::default();
        for i in 0..MAX_KEYS_PER_BLOCK * 2 {
            let key = format!("{i:010}");
            assert!(!builder.is_full());
            builder.insert_key(key.as_bytes(), &mut metrics);
        }
        assert!(builder.is_full());
        builder.finish();

        assert!(!builder.is_full());
        assert_eq!(0, builder.insert_key(b"a0", &mut metrics));
    }
}
