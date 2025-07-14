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

use std::sync::{Arc, RwLock};

use api::v1::OpType;
use mito_codec::key_values::{KeyValue, KeyValues};
use mito_codec::row_converter::build_primary_key_codec;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceNumber};
use table::predicate::Predicate;

use crate::error::Result;
use crate::memtable::bulk::buffer::{BulkBuffer, BulkValueBuilder, SortedBatchBuffer};
use crate::memtable::bulk::part::{BulkPart, EncodedBulkPart};
use crate::memtable::bulk::primary_key::PrimaryKeyBufferWriter;
use crate::memtable::{
    BoxedBatchIterator, Memtable, MemtableId, MemtableRanges, MemtableRef, MemtableStats,
    PredicateGroup,
};

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
    bulk_buffer: RwLock<BulkBuffer>,
    sorted_batch_buffer: RwLock<SortedBatchBuffer>,
}

impl std::fmt::Debug for BulkMemtable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkMemtable")
            .field("id", &self.id)
            .field("parts", &self.parts)
            .field("metadata", &self.metadata)
            .field("bulk_buffer", &"<RwLock<BulkBuffer>>")
            .field("sorted_batch_buffer", &"<RwLock<SortedBatchBuffer>>")
            .finish()
    }
}

impl BulkMemtable {
    pub fn new(id: MemtableId, metadata: RegionMetadataRef) -> Self {
        let primary_key_codec = build_primary_key_codec(&metadata);
        let primary_key_writer = PrimaryKeyBufferWriter::new(primary_key_codec, metadata.clone());

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

        Self {
            id,
            parts: RwLock::new(Vec::new()),
            metadata,
            primary_key_writer,
            bulk_buffer: RwLock::new(bulk_buffer),
            sorted_batch_buffer: RwLock::new(SortedBatchBuffer::default()),
        }
    }

    fn check_and_flush_buffer(&self) -> Result<()> {
        let mut buffer = self.bulk_buffer.write().unwrap();

        // Check if buffer has more than 4096 rows
        if buffer.row_count() > 4096 {
            // Build record batch from buffer (this resets the buffer internally)
            if let Some(record_batch) = self.primary_key_writer.build_record_batch(&mut buffer)? {
                // Push to sorted batch buffer
                let mut sorted_buffer = self.sorted_batch_buffer.write().unwrap();
                sorted_buffer.push(record_batch);
            }
            // Buffer is already reset by build_record_batch, no need to recreate
        }

        Ok(())
    }
}

impl Memtable for BulkMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn write(&self, kvs: &KeyValues) -> Result<()> {
        {
            let mut buffer = self.bulk_buffer.write().unwrap();
            self.primary_key_writer.write(&mut buffer, kvs)?;
        }

        // Check if we need to flush the buffer
        self.check_and_flush_buffer()?;

        Ok(())
    }

    fn write_one(&self, key_value: KeyValue) -> Result<()> {
        {
            let mut buffer = self.bulk_buffer.write().unwrap();
            self.primary_key_writer.write_one(&mut buffer, key_value)?;
        }

        // Check if we need to flush the buffer
        self.check_and_flush_buffer()?;

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

        // Push to sorted batch buffer
        let mut sorted_buffer = self.sorted_batch_buffer.write().unwrap();
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
        _projection: Option<&[ColumnId]>,
        _predicate: PredicateGroup,
        _sequence: Option<SequenceNumber>,
    ) -> Result<MemtableRanges> {
        todo!()
    }

    fn is_empty(&self) -> bool {
        self.parts.read().unwrap().is_empty()
    }

    fn freeze(&self) -> Result<()> {
        Ok(())
    }

    fn stats(&self) -> MemtableStats {
        todo!()
    }

    fn fork(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(Self::new(id, metadata.clone()))
    }
}
