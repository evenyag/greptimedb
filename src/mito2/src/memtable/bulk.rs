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
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use api::v1::OpType;
use mito_codec::key_values::{KeyValue, KeyValues};
use mito_codec::row_converter::build_primary_key_codec;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceNumber};
use table::predicate::Predicate;

use crate::error::Result;
use crate::memtable::bulk::buffer::{BulkBuffer, BulkValueBuilder, SortedBatchBuffer};
use crate::memtable::bulk::context::BulkIterContext;
use crate::memtable::bulk::part::{BulkPart, BulkPartEncoder, EncodedBulkPart};
use crate::memtable::bulk::primary_key::PrimaryKeyBufferWriter;
use crate::memtable::stats::WriteMetrics;
use crate::memtable::{
    AllocTracker, BoxedBatchIterator, IterBuilder, Memtable, MemtableBuilder, MemtableId,
    MemtableRange, MemtableRangeContext, MemtableRanges, MemtableRef, MemtableStats,
    PredicateGroup, WriteBufferManagerRef,
};
use crate::metrics::BULK_MEMTABLE_STAGE_ELAPSED;
use crate::sst::parquet::DEFAULT_ROW_GROUP_SIZE;

#[allow(unused)]
pub(crate) mod buffer;
#[allow(unused)]
mod context;
#[allow(unused)]
pub(crate) mod part;
mod part_reader;
#[allow(unused)]
pub(crate) mod primary_key;
mod row_group_reader;

pub struct BulkMemtable {
    id: MemtableId,
    parts: RwLock<Vec<EncodedBulkPart>>,
    metadata: RegionMetadataRef,
    primary_key_writer: PrimaryKeyBufferWriter,
    part_encoder: BulkPartEncoder,
    bulk_buffers: Vec<RwLock<BulkBuffer>>,
    buffer_selector: AtomicUsize,
    sorted_batch_buffers: Vec<RwLock<SortedBatchBuffer>>,
    alloc_tracker: AllocTracker,
    max_timestamp: AtomicI64,
    min_timestamp: AtomicI64,
    max_sequence: AtomicU64,
    num_rows: AtomicUsize,
}

/// Iterator builder for a single EncodedBulkPart range.
struct BulkMemtableIterBuilder {
    part: EncodedBulkPart,
    metadata: RegionMetadataRef,
    projection: Option<Vec<ColumnId>>,
    predicate: Option<Predicate>,
    sequence: Option<SequenceNumber>,
}

impl IterBuilder for BulkMemtableIterBuilder {
    fn build(&self) -> Result<BoxedBatchIterator> {
        let context = Arc::new(BulkIterContext::new(
            self.metadata.clone(),
            &self.projection.as_deref(),
            self.predicate.clone(),
        ));

        if let Some(iter) = self.part.read(context, self.sequence)? {
            Ok(iter)
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }
}

impl std::fmt::Debug for BulkMemtable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkMemtable")
            .field("id", &self.id)
            .field("parts", &self.parts)
            .field("metadata", &self.metadata)
            .field("part_encoder", &"<BulkPartEncoder>")
            .field(
                "bulk_buffers",
                &format!("<Vec<RwLock<BulkBuffer>>>[{}]", self.bulk_buffers.len()),
            )
            .field("buffer_selector", &self.buffer_selector)
            .field(
                "sorted_batch_buffers",
                &format!(
                    "<Vec<RwLock<SortedBatchBuffer>>>[{}]",
                    self.sorted_batch_buffers.len()
                ),
            )
            .field("alloc_tracker", &self.alloc_tracker)
            .field("max_timestamp", &self.max_timestamp)
            .field("min_timestamp", &self.min_timestamp)
            .field("max_sequence", &self.max_sequence)
            .field("num_rows", &self.num_rows)
            .finish()
    }
}

impl BulkMemtable {
    pub fn new(
        id: MemtableId,
        metadata: RegionMetadataRef,
        write_buffer_manager: Option<WriteBufferManagerRef>,
    ) -> Self {
        let primary_key_codec = build_primary_key_codec(&metadata);
        let primary_key_writer = PrimaryKeyBufferWriter::new(primary_key_codec, metadata.clone());
        let part_encoder = BulkPartEncoder::new(metadata.clone(), true, DEFAULT_ROW_GROUP_SIZE);

        // Create multiple bulk buffers for parallelism
        let num_buffers = common_config::utils::get_cpus();
        let mut bulk_buffers = Vec::with_capacity(num_buffers);
        let mut sorted_batch_buffers = Vec::with_capacity(num_buffers);

        for _ in 0..num_buffers {
            // Create builders for buffer: fields, time index, primary key, sequence, op type
            let mut builders = Vec::new();

            // Add field columns
            for field_column in metadata.field_columns() {
                builders.push(BulkValueBuilder::new_regular(
                    &field_column.column_schema.data_type,
                    1024,
                ));
            }

            // Add time index column
            builders.push(BulkValueBuilder::new_timestamp(
                metadata.time_index_column().column_schema.data_type.clone(),
                1024,
            ));

            // Add primary key column (binary)
            builders.push(BulkValueBuilder::new_regular(
                &datatypes::data_type::ConcreteDataType::binary_datatype(),
                1024,
            ));

            // Add sequence column
            builders.push(BulkValueBuilder::new_sequence(1024));

            // Add op type column
            builders.push(BulkValueBuilder::new_op_type(1024));

            let bulk_buffer = BulkBuffer::new(builders);
            bulk_buffers.push(RwLock::new(bulk_buffer));

            // Create corresponding sorted batch buffer
            sorted_batch_buffers.push(RwLock::new(SortedBatchBuffer::default()));
        }

        Self {
            id,
            parts: RwLock::new(Vec::new()),
            metadata,
            primary_key_writer,
            part_encoder,
            bulk_buffers,
            buffer_selector: AtomicUsize::new(0),
            sorted_batch_buffers,
            alloc_tracker: AllocTracker::new(write_buffer_manager),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
            max_sequence: AtomicU64::new(0),
            num_rows: AtomicUsize::new(0),
        }
    }

    fn check_and_flush_buffer(&self, buffer_index: usize, force: bool) -> Result<()> {
        // First check and flush the specific bulk_buffer to sorted_batch_buffer
        {
            let mut buffer = self.bulk_buffers[buffer_index].write().unwrap();

            // Check if buffer has more than 1024 rows or force is true
            if force || buffer.row_count() > 1024 {
                let _timer = BULK_MEMTABLE_STAGE_ELAPSED
                    .with_label_values(&["build_record_batch"])
                    .start_timer();

                // Build record batch from buffer (this resets the buffer internally)
                if let Some(record_batch) =
                    self.primary_key_writer.build_record_batch(&mut buffer)?
                {
                    // Push to sorted batch buffer
                    let mut sorted_buffer =
                        self.sorted_batch_buffers[buffer_index].write().unwrap();
                    sorted_buffer.push(record_batch);
                }
                // Buffer is already reset by build_record_batch, no need to recreate
            }
        }

        // Then check if sorted_batch_buffer needs to be flushed to parts
        {
            let mut sorted_buffer = self.sorted_batch_buffers[buffer_index].write().unwrap();

            // Check if total rows in sorted_batch_buffer reach DEFAULT_ROW_GROUP_SIZE or force is true
            if force
                || sorted_buffer.num_rows()
                    >= DEFAULT_ROW_GROUP_SIZE / self.sorted_batch_buffers.len()
            {
                let _timer = BULK_MEMTABLE_STAGE_ELAPSED
                    .with_label_values(&["sort_encode_batches"])
                    .start_timer();

                // Finish the sorted batch buffer and get all batches
                let batches = sorted_buffer.finish();

                // Sort the partial sorted batches
                let sorted_batches = self.primary_key_writer.sort_partial_sorted(batches)?;

                // Encode the sorted batches into EncodedBulkPart
                if let Some(encoded_part) = self.part_encoder.encode_record_batches(
                    &sorted_batches,
                    &self.primary_key_writer.column_indices(),
                )? {
                    // Push the encoded part into parts
                    let mut parts = self.parts.write().unwrap();
                    parts.push(encoded_part);
                }
            }
        }

        Ok(())
    }

    /// Updates memtable statistics from WriteMetrics
    fn update_stats(&self, stats: &WriteMetrics) {
        // Update allocation tracker
        self.alloc_tracker
            .on_allocation(stats.key_bytes + stats.value_bytes);

        // Update atomic fields for global tracking
        self.max_timestamp
            .fetch_max(stats.max_ts, Ordering::Relaxed);
        self.min_timestamp
            .fetch_min(stats.min_ts, Ordering::Relaxed);
        self.max_sequence
            .fetch_max(stats.max_sequence, Ordering::Relaxed);
        self.num_rows.fetch_add(stats.num_rows, Ordering::Relaxed);
    }

    /// Updates memtable statistics from BulkPart
    fn update_stats_from_bulk_part(&self, part: &BulkPart) {
        let estimated_size = part.estimated_size();
        self.alloc_tracker.on_allocation(estimated_size);

        // Update atomic fields for global tracking
        self.max_timestamp.fetch_max(part.max_ts, Ordering::Relaxed);
        self.min_timestamp.fetch_min(part.min_ts, Ordering::Relaxed);
        self.max_sequence
            .fetch_max(part.sequence, Ordering::Relaxed);
        self.num_rows.fetch_add(part.num_rows(), Ordering::Relaxed);
    }
}

impl Memtable for BulkMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn write(&self, kvs: &KeyValues) -> Result<()> {
        let mut write_metrics = WriteMetrics::default();

        // Select buffer using round-robin
        let buffer_index =
            self.buffer_selector.fetch_add(1, Ordering::Relaxed) % self.bulk_buffers.len();

        {
            let _timer = BULK_MEMTABLE_STAGE_ELAPSED
                .with_label_values(&["write_bulk_buffers"])
                .start_timer();

            let mut buffer = self.bulk_buffers[buffer_index].write().unwrap();
            self.primary_key_writer
                .write(&mut buffer, kvs, &mut write_metrics)?;
        }

        // Update statistics
        self.update_stats(&write_metrics);

        // Check if we need to flush the buffer we just wrote to
        self.check_and_flush_buffer(buffer_index, false)?;

        Ok(())
    }

    fn write_one(&self, key_value: KeyValue) -> Result<()> {
        let mut write_metrics = WriteMetrics::default();

        // Select buffer using round-robin
        let buffer_index =
            self.buffer_selector.fetch_add(1, Ordering::Relaxed) % self.bulk_buffers.len();

        {
            let _timer = BULK_MEMTABLE_STAGE_ELAPSED
                .with_label_values(&["write_bulk_buffers"])
                .start_timer();

            let mut buffer = self.bulk_buffers[buffer_index].write().unwrap();
            self.primary_key_writer
                .write_one(&mut buffer, key_value, &mut write_metrics)?;
        }

        // Update statistics
        self.update_stats(&write_metrics);

        // Check if we need to flush the buffer we just wrote to
        self.check_and_flush_buffer(buffer_index, false)?;

        Ok(())
    }

    fn write_bulk(&self, fragment: BulkPart) -> Result<()> {
        // Use PrimaryKeyBufferWriter to encode and sort the record batch
        let batch_encoding = self.primary_key_writer.primary_key_codec().encoding();
        let encoded_batch = self.primary_key_writer.encode_primary_key_record_batch(
            &fragment.batch,
            batch_encoding,
            fragment.sequence,
            OpType::Put,
        )?;

        // Sort the batch by primary key
        let sorted_batch = self
            .primary_key_writer
            .sort_primary_key_batch(&encoded_batch)?;

        // Update statistics from BulkPart
        self.update_stats_from_bulk_part(&fragment);

        // Select sorted batch buffer using round-robin
        let buffer_index =
            self.buffer_selector.fetch_add(1, Ordering::Relaxed) % self.sorted_batch_buffers.len();

        // Push to sorted batch buffer
        let mut sorted_buffer = self.sorted_batch_buffers[buffer_index].write().unwrap();
        sorted_buffer.push(sorted_batch);

        Ok(())
    }

    fn iter(
        &self,
        _projection: Option<&[ColumnId]>,
        _predicate: Option<Predicate>,
        _sequence: Option<SequenceNumber>,
    ) -> Result<BoxedBatchIterator> {
        todo!()
    }

    fn ranges(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: PredicateGroup,
        sequence: Option<SequenceNumber>,
    ) -> Result<MemtableRanges> {
        let projection = projection.map(|p| p.to_vec());
        let mut ranges = BTreeMap::new();

        // Create a range for each EncodedBulkPart
        let parts = self.parts.read().unwrap();
        for (range_id, part) in parts.iter().enumerate() {
            let builder = Box::new(BulkMemtableIterBuilder {
                part: part.clone(),
                metadata: self.metadata.clone(),
                projection: projection.clone(),
                predicate: predicate.predicate().cloned(),
                sequence,
            });

            let context = Arc::new(MemtableRangeContext::new(
                self.id,
                builder,
                predicate.clone(),
            ));

            ranges.insert(range_id, MemtableRange::new(context));
        }

        Ok(MemtableRanges {
            ranges,
            stats: self.stats(),
        })
    }

    fn is_empty(&self) -> bool {
        // Check if parts are empty
        if !self.parts.read().unwrap().is_empty() {
            return false;
        }

        // Check if all sorted batch buffers are empty
        for sorted_buffer in &self.sorted_batch_buffers {
            if sorted_buffer.read().unwrap().num_rows() != 0 {
                return false;
            }
        }

        // Check if all bulk buffers are empty
        for buffer in &self.bulk_buffers {
            if buffer.read().unwrap().row_count() != 0 {
                return false;
            }
        }

        true
    }

    fn freeze(&self) -> Result<()> {
        // Flush all buffers
        for i in 0..self.bulk_buffers.len() {
            self.check_and_flush_buffer(i, true)?;
        }
        self.alloc_tracker.done_allocating();
        Ok(())
    }

    fn stats(&self) -> MemtableStats {
        let estimated_bytes = self.alloc_tracker.bytes_allocated();
        let num_rows = self.num_rows.load(Ordering::Relaxed);

        if num_rows == 0 {
            // no rows ever written
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

        MemtableStats {
            estimated_bytes,
            time_range: Some((min_timestamp, max_timestamp)),
            num_rows,
            num_ranges: 1,
            max_sequence: self.max_sequence.load(Ordering::Relaxed),
            series_count: 1,
        }
    }

    fn fork(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(Self::new(
            id,
            metadata.clone(),
            self.alloc_tracker.write_buffer_manager(),
        ))
    }
}

/// Builder for BulkMemtable
#[derive(Debug)]
pub struct BulkMemtableBuilder {
    write_buffer_manager: Option<WriteBufferManagerRef>,
}

impl BulkMemtableBuilder {
    /// Creates a new builder with specific `write_buffer_manager`.
    pub fn new(write_buffer_manager: Option<WriteBufferManagerRef>) -> Self {
        Self {
            write_buffer_manager,
        }
    }
}

impl MemtableBuilder for BulkMemtableBuilder {
    fn build(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(BulkMemtable::new(
            id,
            metadata.clone(),
            self.write_buffer_manager.clone(),
        ))
    }
}
