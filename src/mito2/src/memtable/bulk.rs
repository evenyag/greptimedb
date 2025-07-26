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

//! Memtable implementation for bulk load

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use mito_codec::key_values::KeyValue;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceNumber};

use crate::error::Result;
use crate::memtable::bulk::context::{BulkIterContext, BulkIterContextRef};
use crate::memtable::bulk::part::{BulkPart, BulkPartWithId, EncodedBulkPart};
use crate::memtable::bulk::part_reader::RecordBatchIter;
use crate::memtable::{
    BoxedBatchIterator, IterBuilder, KeyValues, Memtable, MemtableId, MemtableRange,
    MemtableRangeContext, MemtableRanges, MemtableRef, MemtableStats, PredicateGroup,
};
use crate::read::dedup::LastNonNullIter;
use crate::region::options::MergeMode;

#[allow(unused)]
mod context;
#[allow(unused)]
pub mod part;
mod part_reader;
mod row_group_reader;

pub struct BulkMemtable {
    id: MemtableId,
    metadata: RegionMetadataRef,
    parts: RwLock<Vec<BulkPartWithId>>,
    encoded_parts: RwLock<Vec<EncodedBulkPart>>,
    next_part_id: AtomicU32,
    merge_mode: MergeMode,
}

impl Memtable for BulkMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn write(&self, _kvs: &KeyValues) -> Result<()> {
        unimplemented!()
    }

    fn write_one(&self, _key_value: KeyValue) -> Result<()> {
        unimplemented!()
    }

    fn write_bulk(&self, fragment: BulkPart) -> Result<()> {
        let part_id = self.next_part_id.fetch_add(1, Ordering::Relaxed);

        let part = BulkPartWithId {
            bulk_part: fragment,
            id: part_id,
        };

        let mut parts = self.parts.write().unwrap();
        parts.push(part);

        Ok(())
    }

    #[cfg(any(test, feature = "test"))]
    fn iter(
        &self,
        _projection: Option<&[ColumnId]>,
        _predicate: Option<table::predicate::Predicate>,
        _sequence: Option<SequenceNumber>,
    ) -> Result<crate::memtable::BoxedBatchIterator> {
        todo!()
    }

    fn ranges(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: PredicateGroup,
        sequence: Option<SequenceNumber>,
    ) -> Result<MemtableRanges> {
        let iter_context = Arc::new(BulkIterContext::new(
            self.metadata.clone(),
            &projection,
            predicate.predicate().cloned(),
        ));

        let parts: Vec<_> = self
            .parts
            .read()
            .unwrap()
            .iter()
            .map(|part| part.clone())
            .collect();
        let stats = self.stats();
        let mut ranges = BTreeMap::new();
        for part in parts {
            let builder = Box::new(RecordBatchIterBuilder {
                context: iter_context.clone(),
                part: part.bulk_part,
                sequence,
                merge_mode: self.merge_mode,
            });

            let part_id = part.id;
            let context = Arc::new(MemtableRangeContext::new(
                self.id,
                builder,
                predicate.clone(),
            ));
            ranges.insert(
                part_id as usize,
                MemtableRange::new(context, stats.num_rows),
            );
        }

        Ok(MemtableRanges { ranges, stats })
    }

    fn is_empty(&self) -> bool {
        let parts_empty = self.parts.read().unwrap().is_empty();
        let encoded_empty = self.encoded_parts.read().unwrap().is_empty();

        parts_empty && encoded_empty
    }

    fn freeze(&self) -> Result<()> {
        Ok(())
    }

    fn stats(&self) -> MemtableStats {
        todo!()
    }

    fn fork(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(Self {
            id,
            metadata: metadata.clone(),
            parts: RwLock::new(vec![]),
            encoded_parts: RwLock::new(vec![]),
            next_part_id: AtomicU32::new(0),
            merge_mode: self.merge_mode,
        })
    }
}

impl std::fmt::Debug for BulkMemtable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkMemtable")
            .field("id", &self.id)
            .field("parts_count", &self.parts.read().unwrap().len())
            .field(
                "encoded_parts_count",
                &self.encoded_parts.read().unwrap().len(),
            )
            .field("next_part_id", &self.next_part_id.load(Ordering::Relaxed))
            .finish()
    }
}

struct RecordBatchIterBuilder {
    context: BulkIterContextRef,
    part: BulkPart,
    sequence: Option<SequenceNumber>,
    merge_mode: MergeMode,
}

impl IterBuilder for RecordBatchIterBuilder {
    fn build(&self) -> Result<BoxedBatchIterator> {
        let iter =
            RecordBatchIter::new(self.part.batch.clone(), self.context.clone(), self.sequence);

        if self.merge_mode == MergeMode::LastNonNull {
            let iter = LastNonNullIter::new(iter);
            Ok(Box::new(iter))
        } else {
            Ok(Box::new(iter))
        }
    }
}
