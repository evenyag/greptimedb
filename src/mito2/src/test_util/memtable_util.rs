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

//! Memtable test utilities.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::value::ValueData;
use api::v1::{Row, Rows, SemanticType};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::storage::{ColumnId, RegionId, SequenceNumber};
use table::predicate::Predicate;

use crate::error::Result;
use crate::memtable::{
    BoxedBatchIterator, KeyValues, Memtable, MemtableBuilder, MemtableId, MemtableRef,
    MemtableStats,
};

/// Empty memtable for test.
#[derive(Debug, Default)]
pub(crate) struct EmptyMemtable {
    /// Id of this memtable.
    id: MemtableId,
}

impl EmptyMemtable {
    /// Returns a new memtable with specific `id`.
    pub(crate) fn new(id: MemtableId) -> EmptyMemtable {
        EmptyMemtable { id }
    }
}

impl Memtable for EmptyMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn write(&self, _kvs: &KeyValues) -> Result<()> {
        Ok(())
    }

    fn iter(
        &self,
        _projection: Option<&[ColumnId]>,
        _filters: Option<Predicate>,
    ) -> Result<BoxedBatchIterator> {
        Ok(Box::new(std::iter::empty()))
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn mark_immutable(&self) {}

    fn stats(&self) -> MemtableStats {
        MemtableStats::default()
    }
}

/// Empty memtable builder.
#[derive(Debug, Default)]
pub(crate) struct EmptyMemtableBuilder {
    /// Next memtable id.
    next_id: AtomicU32,
}

impl MemtableBuilder for EmptyMemtableBuilder {
    fn build(&self, _metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(EmptyMemtable::new(
            self.next_id.fetch_add(1, Ordering::Relaxed),
        ))
    }
}

/// Creates a region metadata to test memtable.
///
/// The schema is `k0, k1, ts, v0, v1`.
pub(crate) fn schema_for_test() -> RegionMetadataRef {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(123, 456));
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), false),
            semantic_type: SemanticType::Tag,
            column_id: 0,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("k1", ConcreteDataType::int64_datatype(), false),
            semantic_type: SemanticType::Tag,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 2,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("v0", ConcreteDataType::int64_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 3,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("v1", ConcreteDataType::float64_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 4,
        })
        .primary_key(vec![0, 1]);
    let region_metadata = builder.build().unwrap();
    Arc::new(region_metadata)
}

/// Builds key values with `len` rows for test.
pub(crate) fn build_key_values(
    schema: &RegionMetadataRef,
    k0: String,
    k1: i64,
    len: usize,
) -> KeyValues {
    build_key_values_with_ts_seq(schema, k0, k1, (0..len).map(|ts| ts as i64), 0)
}

/// Builds key values with timestamps (ms) and sequences for test.
pub(crate) fn build_key_values_with_ts_seq(
    schema: &RegionMetadataRef,
    k0: String,
    k1: i64,
    timestamps: impl Iterator<Item = i64>,
    sequence: SequenceNumber,
) -> KeyValues {
    let column_schema = schema
        .column_metadatas
        .iter()
        .map(|c| api::v1::ColumnSchema {
            column_name: c.column_schema.name.clone(),
            datatype: ColumnDataTypeWrapper::try_from(c.column_schema.data_type.clone())
                .unwrap()
                .datatype() as i32,
            semantic_type: c.semantic_type as i32,
            ..Default::default()
        })
        .collect();

    let rows = timestamps
        .map(|ts| Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(k0.clone())),
                },
                api::v1::Value {
                    value_data: Some(ValueData::I64Value(k1)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(ts)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::I64Value(ts)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::F64Value(ts as f64)),
                },
            ],
        })
        .collect();
    let mutation = api::v1::Mutation {
        op_type: 1,
        sequence,
        rows: Some(Rows {
            schema: column_schema,
            rows,
        }),
    };
    KeyValues::new(schema.as_ref(), mutation).unwrap()
}
