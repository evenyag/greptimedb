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

#[allow(unused)]
pub mod context;
#[allow(unused)]
pub mod part;
pub mod part_reader;
mod row_group_reader;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use mito_codec::key_values::KeyValue;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceNumber};

use crate::error::{Result, UnsupportedOperationSnafu};
use crate::flush::WriteBufferManagerRef;
use crate::memtable::bulk::context::BulkIterContext;
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::bulk::part_reader::BulkPartRecordBatchIter;
use crate::memtable::stats::WriteMetrics;
use crate::memtable::{
    AllocTracker, BoxedBatchIterator, BoxedRecordBatchIterator, IterBuilder, KeyValues,
    MemScanMetrics, Memtable, MemtableId, MemtableRange, MemtableRangeContext, MemtableRanges,
    MemtableRef, MemtableStats, PredicateGroup,
};

pub struct BulkMemtable {
    id: MemtableId,
    parts: RwLock<Vec<BulkPart>>,
    metadata: RegionMetadataRef,
    alloc_tracker: AllocTracker,
    max_timestamp: AtomicI64,
    min_timestamp: AtomicI64,
    max_sequence: AtomicU64,
    num_rows: AtomicUsize,
}

impl std::fmt::Debug for BulkMemtable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkMemtable")
            .field("id", &self.id)
            .field("num_parts", &self.parts.read().unwrap().len())
            .field("num_rows", &self.num_rows.load(Ordering::Relaxed))
            .field("min_timestamp", &self.min_timestamp.load(Ordering::Relaxed))
            .field("max_timestamp", &self.max_timestamp.load(Ordering::Relaxed))
            .field("max_sequence", &self.max_sequence.load(Ordering::Relaxed))
            .finish()
    }
}

impl Memtable for BulkMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn write(&self, _kvs: &KeyValues) -> Result<()> {
        UnsupportedOperationSnafu {
            err_msg: "write() is not supported for bulk memtable",
        }
        .fail()
    }

    fn write_one(&self, _key_value: KeyValue) -> Result<()> {
        UnsupportedOperationSnafu {
            err_msg: "write_one() is not supported for bulk memtable",
        }
        .fail()
    }

    fn write_bulk(&self, fragment: BulkPart) -> Result<()> {
        let local_metrics = WriteMetrics {
            key_bytes: 0,
            value_bytes: fragment.estimated_size(),
            min_ts: fragment.min_ts,
            max_ts: fragment.max_ts,
            num_rows: fragment.num_rows(),
            max_sequence: fragment.sequence,
        };

        let mut parts = self.parts.write().unwrap();
        parts.push(fragment);

        // Since this operation should be fast, we do it in parts lock scope.
        self.update_stats(local_metrics);

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
        let parts = self.parts.read().unwrap();
        let mut ranges = BTreeMap::new();

        for (range_id, part) in parts.iter().enumerate() {
            // Skip empty parts
            if part.num_rows() == 0 {
                continue;
            }

            let range = MemtableRange::new(
                Arc::new(MemtableRangeContext::new(
                    self.id,
                    Box::new(BulkRangeIterBuilder {
                        part: part.clone(),
                        projection: projection.map(|p| p.to_vec()),
                        predicate: predicate.clone(),
                        sequence,
                        metadata: self.metadata.clone(),
                    }),
                    predicate.clone(),
                )),
                part.num_rows(),
            );
            ranges.insert(range_id, range);
        }

        let mut stats = self.stats();
        stats.num_ranges = ranges.len();

        Ok(MemtableRanges { ranges, stats })
    }

    fn is_empty(&self) -> bool {
        self.parts.read().unwrap().is_empty()
    }

    fn freeze(&self) -> Result<()> {
        self.alloc_tracker.done_allocating();
        Ok(())
    }

    fn stats(&self) -> MemtableStats {
        let estimated_bytes = self.alloc_tracker.bytes_allocated();

        if estimated_bytes == 0 || self.num_rows.load(Ordering::Relaxed) == 0 {
            return MemtableStats {
                estimated_bytes,
                time_range: None,
                num_rows: 0,
                num_ranges: 0,
                max_sequence: 0,
                series_count: 0,
            };
        }

        let ts_type = self
            .metadata
            .time_index_column()
            .column_schema
            .data_type
            .clone()
            .as_timestamp()
            .expect("Timestamp column must have timestamp type");
        let max_timestamp = ts_type.create_timestamp(self.max_timestamp.load(Ordering::Relaxed));
        let min_timestamp = ts_type.create_timestamp(self.min_timestamp.load(Ordering::Relaxed));

        let num_ranges = self.parts.read().unwrap().len();

        MemtableStats {
            estimated_bytes,
            time_range: Some((min_timestamp, max_timestamp)),
            num_rows: self.num_rows.load(Ordering::Relaxed),
            num_ranges,
            max_sequence: self.max_sequence.load(Ordering::Relaxed),
            series_count: self.estimated_series_count(),
        }
    }

    fn fork(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(Self {
            id,
            parts: RwLock::new(vec![]),
            metadata: metadata.clone(),
            alloc_tracker: AllocTracker::new(self.alloc_tracker.write_buffer_manager()),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
            max_sequence: AtomicU64::new(0),
            num_rows: AtomicUsize::new(0),
        })
    }
}

impl BulkMemtable {
    /// Creates a new BulkMemtable
    pub fn new(
        id: MemtableId,
        metadata: RegionMetadataRef,
        write_buffer_manager: Option<WriteBufferManagerRef>,
    ) -> Self {
        Self {
            id,
            parts: RwLock::new(vec![]),
            metadata,
            alloc_tracker: AllocTracker::new(write_buffer_manager),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
            max_sequence: AtomicU64::new(0),
            num_rows: AtomicUsize::new(0),
        }
    }

    /// Updates memtable stats.
    fn update_stats(&self, stats: WriteMetrics) {
        self.alloc_tracker
            .on_allocation(stats.key_bytes + stats.value_bytes);

        self.max_timestamp.fetch_max(stats.max_ts, Ordering::SeqCst);
        self.min_timestamp.fetch_min(stats.min_ts, Ordering::SeqCst);
        self.max_sequence
            .fetch_max(stats.max_sequence, Ordering::SeqCst);
        self.num_rows.fetch_add(stats.num_rows, Ordering::SeqCst);
    }

    /// Returns the estimated time series count.
    fn estimated_series_count(&self) -> usize {
        let parts = self.parts.read().unwrap();
        parts.iter().map(|part| part.estimated_series_count()).sum()
    }
}

/// Iterator builder for bulk range
struct BulkRangeIterBuilder {
    part: BulkPart,
    projection: Option<Vec<ColumnId>>,
    predicate: PredicateGroup,
    sequence: Option<SequenceNumber>,
    metadata: RegionMetadataRef,
}

impl IterBuilder for BulkRangeIterBuilder {
    fn build(&self, _metrics: Option<MemScanMetrics>) -> Result<BoxedBatchIterator> {
        UnsupportedOperationSnafu {
            err_msg: "BatchIterator is not supported for bulk memtable",
        }
        .fail()
    }

    fn is_record_batch(&self) -> bool {
        true
    }

    fn build_record_batch(
        &self,
        _metrics: Option<MemScanMetrics>,
    ) -> Result<BoxedRecordBatchIterator> {
        let context = Arc::new(BulkIterContext::new(
            self.metadata.clone(),
            &self.projection.as_ref().map(|p| p.as_slice()),
            self.predicate.predicate().cloned(),
        ));

        let iter = BulkPartRecordBatchIter::new(self.part.batch.clone(), context, self.sequence);

        Ok(Box::new(iter))
    }
}
