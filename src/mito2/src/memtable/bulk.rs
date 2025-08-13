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
    AllocTracker, BoxedBatchIterator, BoxedRecordBatchIterator, EncodedBulkPart, IterBuilder,
    KeyValues, MemScanMetrics, Memtable, MemtableId, MemtableRange, MemtableRangeContext,
    MemtableRanges, MemtableRef, MemtableStats, PredicateGroup,
};

pub struct BulkMemtable {
    id: MemtableId,
    parts: RwLock<Vec<BulkPart>>,
    encoded_parts: RwLock<Vec<EncodedBulkPart>>,
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
        // This ensure the statistics in `ranges()` are correct. What's more,
        // it guarantees no rows are out of the time range so we don't need to
        // prune rows by time range again in the iterator of the MemtableRange.
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
        let mut ranges = BTreeMap::new();
        let mut range_id = 0;

        // Create context once and reuse it
        let context = Arc::new(BulkIterContext::new(
            self.metadata.clone(),
            &projection,
            predicate.predicate().cloned(),
        ));

        // Add ranges for regular parts
        {
            let parts = self.parts.read().unwrap();
            for part in parts.iter() {
                // Skip empty parts
                if part.num_rows() == 0 {
                    continue;
                }

                let range = MemtableRange::new(
                    Arc::new(MemtableRangeContext::new(
                        self.id,
                        Box::new(BulkRangeIterBuilder {
                            part: part.clone(),
                            context: context.clone(),
                            sequence,
                        }),
                        predicate.clone(),
                    )),
                    part.num_rows(),
                );
                ranges.insert(range_id, range);
                range_id += 1;
            }
        }

        // Add ranges for encoded parts
        {
            let encoded_parts = self.encoded_parts.read().unwrap();
            for encoded_part in encoded_parts.iter() {
                // Skip empty parts
                if encoded_part.metadata().num_rows == 0 {
                    continue;
                }

                let range = MemtableRange::new(
                    Arc::new(MemtableRangeContext::new(
                        self.id,
                        Box::new(EncodedBulkRangeIterBuilder {
                            part: encoded_part.clone(),
                            context: context.clone(),
                            sequence,
                        }),
                        predicate.clone(),
                    )),
                    encoded_part.metadata().num_rows,
                );
                ranges.insert(range_id, range);
                range_id += 1;
            }
        }

        let mut stats = self.stats();
        stats.num_ranges = ranges.len();

        // TODO(yingwen): Supports per range stats.
        Ok(MemtableRanges { ranges, stats })
    }

    fn is_empty(&self) -> bool {
        let parts_empty = self.parts.read().unwrap().is_empty();
        let encoded_parts_empty = self.encoded_parts.read().unwrap().is_empty();
        parts_empty && encoded_parts_empty
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
            encoded_parts: RwLock::new(vec![]),
            metadata: metadata.clone(),
            alloc_tracker: AllocTracker::new(self.alloc_tracker.write_buffer_manager()),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
            max_sequence: AtomicU64::new(0),
            num_rows: AtomicUsize::new(0),
        })
    }

    fn should_compact(&self) -> bool {
        false
    }

    fn compact(&self) -> Result<()> {
        Ok(())
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
            encoded_parts: RwLock::new(vec![]),
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
    context: Arc<BulkIterContext>,
    sequence: Option<SequenceNumber>,
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
        let iter = BulkPartRecordBatchIter::new(
            self.part.batch.clone(),
            self.context.clone(),
            self.sequence,
        );

        Ok(Box::new(iter))
    }
}

/// Iterator builder for encoded bulk range
struct EncodedBulkRangeIterBuilder {
    part: EncodedBulkPart,
    context: Arc<BulkIterContext>,
    sequence: Option<SequenceNumber>,
}

impl IterBuilder for EncodedBulkRangeIterBuilder {
    fn build(&self, _metrics: Option<MemScanMetrics>) -> Result<BoxedBatchIterator> {
        UnsupportedOperationSnafu {
            err_msg: "BatchIterator is not supported for encoded bulk memtable",
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
        if let Some(iter) = self.part.read(self.context.clone(), self.sequence)? {
            Ok(iter)
        } else {
            // Return an empty iterator if no data to read
            Ok(Box::new(std::iter::empty()))
        }
    }
}

struct MemtableCompactor;

impl MemtableCompactor {
    /// Returns true if the bulk parts should be merged.
    fn should_merge_parts(parts: &RwLock<Vec<BulkPart>>) -> bool {
        todo!()
    }

    /// Merges bulk parts and then encodes the result to an [EncodedBulkPart].
    fn merge_parts(
        &mut self,
        parts: &RwLock<Vec<BulkPart>>,
        encoded_parts: &RwLock<Vec<EncodedBulkPart>>,
    ) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    use mito_codec::row_converter::build_primary_key_codec;

    use super::*;
    use crate::memtable::bulk::part::BulkPartConverter;
    use crate::read::scan_region::PredicateGroup;
    use crate::sst::{to_flat_sst_arrow_schema, FlatSchemaOptions};
    use crate::test_util::memtable_util::{build_key_values_with_ts_seq_values, metadata_for_test};

    fn create_bulk_part_with_converter(
        k0: &str,
        k1: u32,
        timestamps: Vec<i64>,
        values: Vec<Option<f64>>,
        sequence: u64,
    ) -> Result<BulkPart> {
        let metadata = metadata_for_test();
        let capacity = 100;
        let primary_key_codec = build_primary_key_codec(&metadata);
        let schema = to_flat_sst_arrow_schema(
            &metadata,
            &FlatSchemaOptions::from_encoding(metadata.primary_key_encoding),
        );

        let mut converter =
            BulkPartConverter::new(&metadata, schema, capacity, primary_key_codec, true);

        let key_values = build_key_values_with_ts_seq_values(
            &metadata,
            k0.to_string(),
            k1,
            timestamps.into_iter(),
            values.into_iter(),
            sequence,
        );

        converter.append_key_values(&key_values)?;
        converter.convert()
    }

    #[test]
    fn test_bulk_memtable_write_multiple_parts() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(456, metadata.clone(), None);

        let part1 = create_bulk_part_with_converter(
            "key1",
            1,
            vec![1000, 2000],
            vec![Some(1.0), Some(2.0)],
            100,
        )
        .unwrap();

        let part2 = create_bulk_part_with_converter(
            "key2",
            2,
            vec![3000, 4000],
            vec![Some(3.0), Some(4.0)],
            200,
        )
        .unwrap();

        memtable.write_bulk(part1).unwrap();
        memtable.write_bulk(part2).unwrap();

        let stats = memtable.stats();
        assert_eq!(4, stats.num_rows);
        assert_eq!(2, stats.num_ranges);
        assert_eq!(201, stats.max_sequence);

        let (min_ts, max_ts) = stats.time_range.unwrap();
        assert_eq!(1000, min_ts.value());
        assert_eq!(4000, max_ts.value());
    }

    #[test]
    fn test_bulk_memtable_write_read() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(999, metadata.clone(), None);

        let test_data = vec![
            (
                "key_a",
                1u32,
                vec![1000i64, 2000i64],
                vec![Some(10.5), Some(20.5)],
                100u64,
            ),
            (
                "key_b",
                2u32,
                vec![1500i64, 2500i64],
                vec![Some(15.5), Some(25.5)],
                200u64,
            ),
            ("key_c", 3u32, vec![3000i64], vec![Some(30.5)], 300u64),
        ];

        for (k0, k1, timestamps, values, seq) in test_data.iter() {
            let part =
                create_bulk_part_with_converter(k0, *k1, timestamps.clone(), values.clone(), *seq)
                    .unwrap();
            memtable.write_bulk(part).unwrap();
        }

        let stats = memtable.stats();
        assert_eq!(5, stats.num_rows);
        assert_eq!(3, stats.num_ranges);
        assert_eq!(300, stats.max_sequence);

        let (min_ts, max_ts) = stats.time_range.unwrap();
        assert_eq!(1000, min_ts.value());
        assert_eq!(3000, max_ts.value());

        let predicate_group = PredicateGroup::new(&metadata, &[]);
        let ranges = memtable.ranges(None, predicate_group, None).unwrap();

        assert_eq!(3, ranges.ranges.len());
        assert_eq!(5, ranges.stats.num_rows);

        for (_range_id, range) in ranges.ranges.iter() {
            assert!(range.num_rows() > 0);
            assert!(range.is_record_batch());

            let record_batch_iter = range.build_record_batch_iter(None).unwrap();

            let mut total_rows = 0;
            for batch_result in record_batch_iter {
                let batch = batch_result.unwrap();
                total_rows += batch.num_rows();
                assert!(batch.num_rows() > 0);
                assert_eq!(8, batch.num_columns());
            }
            assert_eq!(total_rows, range.num_rows());
        }
    }

    #[test]
    fn test_bulk_memtable_ranges_with_projection() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(111, metadata.clone(), None);

        let bulk_part = create_bulk_part_with_converter(
            "projection_test",
            5,
            vec![5000, 6000, 7000],
            vec![Some(50.0), Some(60.0), Some(70.0)],
            500,
        )
        .unwrap();

        memtable.write_bulk(bulk_part).unwrap();

        let projection = vec![4u32];
        let predicate_group = PredicateGroup::new(&metadata, &[]);
        let ranges = memtable
            .ranges(Some(&projection), predicate_group, None)
            .unwrap();

        assert_eq!(1, ranges.ranges.len());
        let range = ranges.ranges.get(&0).unwrap();

        assert!(range.is_record_batch());
        let record_batch_iter = range.build_record_batch_iter(None).unwrap();

        let mut total_rows = 0;
        for batch_result in record_batch_iter {
            let batch = batch_result.unwrap();
            assert!(batch.num_rows() > 0);
            assert_eq!(5, batch.num_columns());
            total_rows += batch.num_rows();
        }
        assert_eq!(3, total_rows);
    }

    #[test]
    fn test_bulk_memtable_unsupported_operations() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(111, metadata.clone(), None);

        let key_values = build_key_values_with_ts_seq_values(
            &metadata,
            "test".to_string(),
            1,
            vec![1000].into_iter(),
            vec![Some(1.0)].into_iter(),
            1,
        );

        let err = memtable.write(&key_values).unwrap_err();
        assert!(err.to_string().contains("not supported"));

        let kv = key_values.iter().next().unwrap();
        let err = memtable.write_one(kv).unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn test_bulk_memtable_freeze() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(222, metadata.clone(), None);

        let bulk_part = create_bulk_part_with_converter(
            "freeze_test",
            10,
            vec![10000],
            vec![Some(100.0)],
            1000,
        )
        .unwrap();

        memtable.write_bulk(bulk_part).unwrap();
        assert_eq!(memtable.freeze().unwrap(), ());

        let stats_after_freeze = memtable.stats();
        assert_eq!(1, stats_after_freeze.num_rows);
    }

    #[test]
    fn test_bulk_memtable_fork() {
        let metadata = metadata_for_test();
        let original_memtable = BulkMemtable::new(333, metadata.clone(), None);

        let bulk_part =
            create_bulk_part_with_converter("fork_test", 15, vec![15000], vec![Some(150.0)], 1500)
                .unwrap();

        original_memtable.write_bulk(bulk_part).unwrap();

        let forked_memtable = original_memtable.fork(444, &metadata);

        assert_eq!(forked_memtable.id(), 444);
        assert!(forked_memtable.is_empty());
        assert_eq!(0, forked_memtable.stats().num_rows);

        assert!(!original_memtable.is_empty());
        assert_eq!(1, original_memtable.stats().num_rows);
    }
}
