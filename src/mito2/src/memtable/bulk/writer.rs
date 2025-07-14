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

//! Writer for bulk buffer operations with primary key encoding

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::OpType;
use datatypes::value::ValueRef;
use mito_codec::key_values::{KeyValue, KeyValues};
use mito_codec::row_converter::PrimaryKeyCodec;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::error::{EncodeSnafu, Result};
use crate::memtable::bulk::buffer::BulkBuffer;

/// Writer for bulk buffer operations that encodes primary keys
pub struct PrimaryKeyBufferWriter {
    /// Primary key codec for encoding keys
    primary_key_codec: Arc<dyn PrimaryKeyCodec>,
    /// Region metadata for schema information
    metadata: RegionMetadataRef,
    /// Column indices mapping for efficient access
    column_indices: ColumnIndices,
}

/// Mapping of column types to their buffer indices
#[derive(Debug)]
struct ColumnIndices {
    /// Index of time column in the buffer
    time_index: usize,
    /// Index of primary key column in the buffer
    primary_key_index: usize,
    /// Index of sequence column in the buffer
    sequence_index: usize,
    /// Index of op type column in the buffer
    op_type_index: usize,
}

impl PrimaryKeyBufferWriter {
    /// Creates a new PrimaryKeyBufferWriter
    pub fn new(primary_key_codec: Arc<dyn PrimaryKeyCodec>, metadata: RegionMetadataRef) -> Self {
        let column_indices = Self::create_column_indices(&metadata);

        Self {
            primary_key_codec,
            metadata,
            column_indices,
        }
    }

    /// Creates the column indices based on the schema
    /// Schema follows format: field 0, field 1, ..., field N, time index, primary key, sequence, op type
    fn create_column_indices(metadata: &RegionMetadataRef) -> ColumnIndices {
        let mut buffer_index = 0;

        // Add field columns first (in the order they appear in metadata)
        for field_column in metadata.field_columns() {
            buffer_index += 1;
        }

        // Add time index column
        let time_index = buffer_index;
        buffer_index += 1;

        // Add primary key column (dictionary encoded binary)
        let primary_key_index = buffer_index;
        buffer_index += 1;

        // Add sequence column
        let sequence_index = buffer_index;
        buffer_index += 1;

        // Add op type column
        let op_type_index = buffer_index;

        ColumnIndices {
            time_index,
            primary_key_index,
            sequence_index,
            op_type_index,
        }
    }

    /// Writes a single KeyValue to the buffer
    pub fn write_one(&self, buffer: &mut BulkBuffer, key_value: KeyValue) -> Result<()> {
        // Encode the primary key
        let mut primary_key_bytes = Vec::new();
        self.primary_key_codec
            .encode_key_value(&key_value, &mut primary_key_bytes)
            .context(EncodeSnafu)?;

        // Extract values from the key_value
        let timestamp = key_value.timestamp();
        let sequence = key_value.sequence();
        let op_type = key_value.op_type();

        // Write timestamp
        buffer.push_value(self.column_indices.time_index, timestamp)?;

        // Write primary key
        buffer.push_value(
            self.column_indices.primary_key_index,
            ValueRef::Binary(&primary_key_bytes),
        )?;

        // Write sequence
        buffer.push_value(
            self.column_indices.sequence_index,
            ValueRef::UInt64(sequence),
        )?;

        // Write op type
        buffer.push_value(
            self.column_indices.op_type_index,
            ValueRef::UInt8(op_type as u8),
        )?;

        // Write field values
        for (buffer_index, value) in key_value.fields().enumerate() {
            buffer.push_value(buffer_index, value)?;
        }

        // Finish the row
        buffer.finish_one();
        Ok(())
    }

    /// Writes multiple KeyValues to the buffer
    pub fn write(&self, buffer: &mut BulkBuffer, key_values: KeyValues) -> Result<()> {
        for key_value in key_values.iter() {
            self.write_one(buffer, key_value)?;
        }
        Ok(())
    }
}
