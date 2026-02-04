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

//! Statistics for pruning record batches in MultiBulkPart.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use api::v1::SemanticType;
use common_recordbatch::DfRecordBatch as RecordBatch;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use datatypes::arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::data_type::DataType;
use datatypes::prelude::MutableVector;
use datatypes::value::OrderedFloat;
use store_api::metadata::{ColumnMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;

/// Column statistics for all batches in a part, stored as arrays.
///
/// Each array has one element per batch, enabling efficient pruning
/// via DataFusion's `PruningStatistics` trait.
#[derive(Debug, Clone)]
pub struct PartColumnStats {
    /// Min values for each batch (array length = num_batches).
    pub min_values: ArrayRef,
    /// Max values for each batch (array length = num_batches).
    pub max_values: ArrayRef,
    /// Null counts for each batch.
    pub null_counts: ArrayRef,
}

/// Statistics for all batches in a `MultiBulkPart`.
///
/// Stores per-column min/max/null_count statistics as arrays,
/// where each array element corresponds to one batch.
#[derive(Debug, Clone)]
pub struct MultiBulkPartStats {
    /// Column ID to its statistics arrays.
    columns: HashMap<ColumnId, PartColumnStats>,
    /// Row count for each batch.
    row_counts: UInt64Array,
}

impl MultiBulkPartStats {
    /// Creates empty statistics.
    pub fn empty() -> Self {
        Self {
            columns: HashMap::new(),
            row_counts: UInt64Array::from(Vec::<u64>::new()),
        }
    }

    /// Computes statistics from batches for the given metadata.
    ///
    /// Computes min/max/null_count for all columns that support min/max operations.
    pub fn compute(batches: &[RecordBatch], metadata: &RegionMetadataRef) -> Self {
        if batches.is_empty() {
            return Self::empty();
        }

        let mut columns = HashMap::new();

        // Compute row counts
        let row_counts: UInt64Array = batches
            .iter()
            .map(|b| Some(b.num_rows() as u64))
            .collect();

        // Compute stats for all columns in metadata
        for col in metadata.column_metadatas.iter() {
            if let Some(stats) = Self::compute_column_stats(batches, col) {
                columns.insert(col.column_id, stats);
            }
        }

        Self {
            columns,
            row_counts,
        }
    }

    /// Computes statistics for a single column across all batches.
    fn compute_column_stats(
        batches: &[RecordBatch],
        col_meta: &ColumnMetadata,
    ) -> Option<PartColumnStats> {
        let col_name = &col_meta.column_schema.name;
        let data_type = &col_meta.column_schema.data_type;

        // Create mutable vectors for min/max values
        let mut min_builder = data_type.create_mutable_vector(batches.len());
        let mut max_builder = data_type.create_mutable_vector(batches.len());
        let mut null_counts: Vec<u64> = Vec::with_capacity(batches.len());

        for batch in batches {
            let (col_idx, _) = batch.schema().column_with_name(col_name)?;
            let array = batch.column(col_idx);
            null_counts.push(array.null_count() as u64);

            // Compute min/max using Arrow compute and push to builders
            Self::compute_and_push_min_max(array, &mut min_builder, &mut max_builder)?;
        }

        let min_array = min_builder.to_vector().to_arrow_array();
        let max_array = max_builder.to_vector().to_arrow_array();
        let null_array: UInt64Array = null_counts.into_iter().map(Some).collect();

        Some(PartColumnStats {
            min_values: min_array,
            max_values: max_array,
            null_counts: Arc::new(null_array),
        })
    }

    /// Computes min/max using Arrow compute functions and pushes to builders.
    fn compute_and_push_min_max(
        array: &ArrayRef,
        min_builder: &mut Box<dyn MutableVector>,
        max_builder: &mut Box<dyn MutableVector>,
    ) -> Option<()> {
        use datatypes::arrow::array::*;
        use datatypes::arrow::compute;
        use datatypes::value::ValueRef;

        match array.data_type() {
            // Integer types
            ArrowDataType::Int8 => {
                let arr = array.as_any().downcast_ref::<Int8Array>()?;
                let min_ref = compute::min(arr).map(ValueRef::Int8).unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr).map(ValueRef::Int8).unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            ArrowDataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>()?;
                let min_ref = compute::min(arr).map(ValueRef::Int16).unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr).map(ValueRef::Int16).unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            ArrowDataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>()?;
                let min_ref = compute::min(arr).map(ValueRef::Int32).unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr).map(ValueRef::Int32).unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            ArrowDataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>()?;
                let min_ref = compute::min(arr).map(ValueRef::Int64).unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr).map(ValueRef::Int64).unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            ArrowDataType::UInt8 => {
                let arr = array.as_any().downcast_ref::<UInt8Array>()?;
                let min_ref = compute::min(arr).map(ValueRef::UInt8).unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr).map(ValueRef::UInt8).unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            ArrowDataType::UInt16 => {
                let arr = array.as_any().downcast_ref::<UInt16Array>()?;
                let min_ref = compute::min(arr).map(ValueRef::UInt16).unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr).map(ValueRef::UInt16).unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            ArrowDataType::UInt32 => {
                let arr = array.as_any().downcast_ref::<UInt32Array>()?;
                let min_ref = compute::min(arr).map(ValueRef::UInt32).unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr).map(ValueRef::UInt32).unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            ArrowDataType::UInt64 => {
                let arr = array.as_any().downcast_ref::<UInt64Array>()?;
                let min_ref = compute::min(arr).map(ValueRef::UInt64).unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr).map(ValueRef::UInt64).unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            // Float types
            ArrowDataType::Float32 => {
                let arr = array.as_any().downcast_ref::<Float32Array>()?;
                let min_ref = compute::min(arr)
                    .map(|v| ValueRef::Float32(OrderedFloat(v)))
                    .unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr)
                    .map(|v| ValueRef::Float32(OrderedFloat(v)))
                    .unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            ArrowDataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>()?;
                let min_ref = compute::min(arr)
                    .map(|v| ValueRef::Float64(OrderedFloat(v)))
                    .unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr)
                    .map(|v| ValueRef::Float64(OrderedFloat(v)))
                    .unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            // Timestamp types
            ArrowDataType::Timestamp(unit, _) => {
                Self::compute_and_push_timestamp_min_max(array, unit, min_builder, max_builder)?;
            }
            // String types
            ArrowDataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>()?;
                let min_ref = compute::min_string(arr)
                    .map(ValueRef::String)
                    .unwrap_or(ValueRef::Null);
                let max_ref = compute::max_string(arr)
                    .map(ValueRef::String)
                    .unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            ArrowDataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>()?;
                let min_ref = compute::min_string(arr)
                    .map(ValueRef::String)
                    .unwrap_or(ValueRef::Null);
                let max_ref = compute::max_string(arr)
                    .map(ValueRef::String)
                    .unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            ArrowDataType::Utf8View => {
                let arr = array.as_any().downcast_ref::<StringViewArray>()?;
                let min_ref = compute::min_string_view(arr)
                    .map(ValueRef::String)
                    .unwrap_or(ValueRef::Null);
                let max_ref = compute::max_string_view(arr)
                    .map(ValueRef::String)
                    .unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            // Binary types
            ArrowDataType::Binary => {
                let arr = array.as_any().downcast_ref::<BinaryArray>()?;
                let min_ref = compute::min_binary(arr)
                    .map(ValueRef::Binary)
                    .unwrap_or(ValueRef::Null);
                let max_ref = compute::max_binary(arr)
                    .map(ValueRef::Binary)
                    .unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            ArrowDataType::LargeBinary => {
                let arr = array.as_any().downcast_ref::<LargeBinaryArray>()?;
                let min_ref = compute::min_binary(arr)
                    .map(ValueRef::Binary)
                    .unwrap_or(ValueRef::Null);
                let max_ref = compute::max_binary(arr)
                    .map(ValueRef::Binary)
                    .unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            ArrowDataType::BinaryView => {
                let arr = array.as_any().downcast_ref::<BinaryViewArray>()?;
                let min_ref = compute::min_binary_view(arr)
                    .map(ValueRef::Binary)
                    .unwrap_or(ValueRef::Null);
                let max_ref = compute::max_binary_view(arr)
                    .map(ValueRef::Binary)
                    .unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            // Date types
            ArrowDataType::Date32 => {
                let arr = array.as_any().downcast_ref::<Date32Array>()?;
                let min_ref = compute::min(arr)
                    .map(|v| ValueRef::Date(common_time::date::Date::from(v)))
                    .unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr)
                    .map(|v| ValueRef::Date(common_time::date::Date::from(v)))
                    .unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            // Boolean type
            ArrowDataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>()?;
                let min_ref = compute::min_boolean(arr)
                    .map(ValueRef::Boolean)
                    .unwrap_or(ValueRef::Null);
                let max_ref = compute::max_boolean(arr)
                    .map(ValueRef::Boolean)
                    .unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            // Unsupported types - push nulls
            _ => {
                min_builder.push_null();
                max_builder.push_null();
            }
        }
        Some(())
    }

    /// Helper for timestamp min/max computation.
    fn compute_and_push_timestamp_min_max(
        array: &ArrayRef,
        unit: &datatypes::arrow::datatypes::TimeUnit,
        min_builder: &mut Box<dyn MutableVector>,
        max_builder: &mut Box<dyn MutableVector>,
    ) -> Option<()> {
        use common_time::timestamp::TimeUnit as TsUnit;
        use datatypes::arrow::array::*;
        use datatypes::arrow::compute;
        use datatypes::arrow::datatypes::TimeUnit;
        use datatypes::value::ValueRef;

        let ts_unit = match unit {
            TimeUnit::Second => TsUnit::Second,
            TimeUnit::Millisecond => TsUnit::Millisecond,
            TimeUnit::Microsecond => TsUnit::Microsecond,
            TimeUnit::Nanosecond => TsUnit::Nanosecond,
        };

        match unit {
            TimeUnit::Second => {
                let arr = array.as_any().downcast_ref::<TimestampSecondArray>()?;
                let min_ref = compute::min(arr)
                    .map(|v| ValueRef::Timestamp(common_time::Timestamp::new(v, ts_unit)))
                    .unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr)
                    .map(|v| ValueRef::Timestamp(common_time::Timestamp::new(v, ts_unit)))
                    .unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            TimeUnit::Millisecond => {
                let arr = array.as_any().downcast_ref::<TimestampMillisecondArray>()?;
                let min_ref = compute::min(arr)
                    .map(|v| ValueRef::Timestamp(common_time::Timestamp::new(v, ts_unit)))
                    .unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr)
                    .map(|v| ValueRef::Timestamp(common_time::Timestamp::new(v, ts_unit)))
                    .unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            TimeUnit::Microsecond => {
                let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
                let min_ref = compute::min(arr)
                    .map(|v| ValueRef::Timestamp(common_time::Timestamp::new(v, ts_unit)))
                    .unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr)
                    .map(|v| ValueRef::Timestamp(common_time::Timestamp::new(v, ts_unit)))
                    .unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
            TimeUnit::Nanosecond => {
                let arr = array.as_any().downcast_ref::<TimestampNanosecondArray>()?;
                let min_ref = compute::min(arr)
                    .map(|v| ValueRef::Timestamp(common_time::Timestamp::new(v, ts_unit)))
                    .unwrap_or(ValueRef::Null);
                let max_ref = compute::max(arr)
                    .map(|v| ValueRef::Timestamp(common_time::Timestamp::new(v, ts_unit)))
                    .unwrap_or(ValueRef::Null);
                min_builder.push_value_ref(&min_ref);
                max_builder.push_value_ref(&max_ref);
            }
        }
        Some(())
    }

    /// Returns the statistics for a column.
    pub fn get(&self, column_id: &ColumnId) -> Option<&PartColumnStats> {
        self.columns.get(column_id)
    }

    /// Returns the row counts array.
    pub fn row_counts(&self) -> &UInt64Array {
        &self.row_counts
    }

    /// Returns the number of batches.
    pub fn num_batches(&self) -> usize {
        self.row_counts.len()
    }
}

/// Statistics for pruning record batches in MultiBulkPart.
pub struct BatchPruningStats<'a> {
    /// Statistics for all batches.
    stats: &'a MultiBulkPartStats,
    /// Region metadata.
    metadata: RegionMetadataRef,
    /// If true, skip columns with Field semantic type during pruning.
    skip_fields: bool,
}

impl<'a> BatchPruningStats<'a> {
    /// Creates a new BatchPruningStats.
    pub fn new(
        stats: &'a MultiBulkPartStats,
        metadata: RegionMetadataRef,
        skip_fields: bool,
    ) -> Self {
        Self {
            stats,
            metadata,
            skip_fields,
        }
    }

    /// Returns the column id for pruning if the column should be pruned.
    /// Returns None if skip_fields is true and the column is a Field.
    fn column_id_to_prune(&self, name: &str) -> Option<ColumnId> {
        let col = self.metadata.column_by_name(name)?;

        // Skip field columns when skip_fields is enabled
        if self.skip_fields && col.semantic_type == SemanticType::Field {
            return None;
        }

        Some(col.column_id)
    }
}

impl PruningStatistics for BatchPruningStats<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        self.stats.get(&column_id).map(|s| s.min_values.clone())
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        self.stats.get(&column_id).map(|s| s.max_values.clone())
    }

    fn num_containers(&self) -> usize {
        self.stats.num_batches()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        self.stats.get(&column_id).map(|s| s.null_counts.clone())
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        Some(Arc::new(self.stats.row_counts().clone()) as ArrayRef)
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datatypes::arrow::array::{
        Float64Array, Int64Array, StringArray, TimestampMillisecondArray,
    };
    use datatypes::arrow::datatypes::{Field, Schema, TimeUnit};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

    /// Creates a simple test metadata with timestamp and two field columns.
    fn test_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v0", ConcreteDataType::int64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v1", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("tag", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 3,
            })
            .primary_key(vec![3]);
        Arc::new(builder.build().unwrap())
    }

    /// Creates an Arrow schema matching the test metadata.
    fn test_arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                datatypes::arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("v0", datatypes::arrow::datatypes::DataType::Int64, true),
            Field::new("v1", datatypes::arrow::datatypes::DataType::Float64, true),
            Field::new("tag", datatypes::arrow::datatypes::DataType::Utf8, false),
        ]))
    }

    /// Creates a test record batch.
    fn create_batch(
        ts: Vec<i64>,
        v0: Vec<Option<i64>>,
        v1: Vec<Option<f64>>,
        tag: Vec<&str>,
    ) -> RecordBatch {
        let schema = test_arrow_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Int64Array::from(v0)),
                Arc::new(Float64Array::from(v1)),
                Arc::new(StringArray::from(tag)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_empty_stats() {
        let stats = MultiBulkPartStats::empty();
        assert_eq!(0, stats.num_batches());
        assert!(stats.row_counts().is_empty());
        assert!(stats.get(&0).is_none());
    }

    #[test]
    fn test_compute_empty_batches() {
        let metadata = test_metadata();
        let stats = MultiBulkPartStats::compute(&[], &metadata);
        assert_eq!(0, stats.num_batches());
    }

    #[test]
    fn test_compute_single_batch() {
        let metadata = test_metadata();
        let batch = create_batch(
            vec![100, 200, 300],
            vec![Some(10), Some(20), Some(30)],
            vec![Some(1.5), Some(2.5), Some(3.5)],
            vec!["a", "b", "c"],
        );

        let stats = MultiBulkPartStats::compute(&[batch], &metadata);

        assert_eq!(1, stats.num_batches());

        // Check row counts
        let row_counts = stats.row_counts();
        assert_eq!(1, row_counts.len());
        assert_eq!(3, row_counts.value(0));

        // Check timestamp stats (column_id = 0)
        let ts_stats = stats.get(&0).expect("should have ts stats");
        let min_ts = ts_stats
            .min_values
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let max_ts = ts_stats
            .max_values
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(100, min_ts.value(0));
        assert_eq!(300, max_ts.value(0));

        // Check v0 stats (column_id = 1)
        let v0_stats = stats.get(&1).expect("should have v0 stats");
        let min_v0 = v0_stats
            .min_values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let max_v0 = v0_stats
            .max_values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(10, min_v0.value(0));
        assert_eq!(30, max_v0.value(0));

        // Check v1 stats (column_id = 2)
        let v1_stats = stats.get(&2).expect("should have v1 stats");
        let min_v1 = v1_stats
            .min_values
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let max_v1 = v1_stats
            .max_values
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((1.5 - min_v1.value(0)).abs() < f64::EPSILON);
        assert!((3.5 - max_v1.value(0)).abs() < f64::EPSILON);

        // Check tag stats (column_id = 3)
        let tag_stats = stats.get(&3).expect("should have tag stats");
        let min_tag = tag_stats
            .min_values
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let max_tag = tag_stats
            .max_values
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("a", min_tag.value(0));
        assert_eq!("c", max_tag.value(0));
    }

    #[test]
    fn test_compute_multiple_batches() {
        let metadata = test_metadata();
        let batch1 = create_batch(
            vec![100, 200],
            vec![Some(10), Some(20)],
            vec![Some(1.0), Some(2.0)],
            vec!["a", "b"],
        );
        let batch2 = create_batch(
            vec![300, 400, 500],
            vec![Some(5), Some(15), Some(25)],
            vec![Some(0.5), Some(1.5), Some(2.5)],
            vec!["c", "d", "e"],
        );

        let stats = MultiBulkPartStats::compute(&[batch1, batch2], &metadata);

        assert_eq!(2, stats.num_batches());

        // Check row counts
        let row_counts = stats.row_counts();
        assert_eq!(2, row_counts.len());
        assert_eq!(2, row_counts.value(0)); // batch1 has 2 rows
        assert_eq!(3, row_counts.value(1)); // batch2 has 3 rows

        // Check v0 stats for both batches
        let v0_stats = stats.get(&1).expect("should have v0 stats");
        let min_v0 = v0_stats
            .min_values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let max_v0 = v0_stats
            .max_values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // Batch 1: min=10, max=20
        assert_eq!(10, min_v0.value(0));
        assert_eq!(20, max_v0.value(0));
        // Batch 2: min=5, max=25
        assert_eq!(5, min_v0.value(1));
        assert_eq!(25, max_v0.value(1));
    }

    #[test]
    fn test_compute_with_nulls() {
        let metadata = test_metadata();
        let batch = create_batch(
            vec![100, 200, 300],
            vec![Some(10), None, Some(30)],
            vec![None, None, Some(3.0)],
            vec!["a", "b", "c"],
        );

        let stats = MultiBulkPartStats::compute(&[batch], &metadata);

        // Check null counts for v0
        let v0_stats = stats.get(&1).expect("should have v0 stats");
        let null_counts = v0_stats
            .null_counts
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(1, null_counts.value(0)); // 1 null in v0

        // Check null counts for v1
        let v1_stats = stats.get(&2).expect("should have v1 stats");
        let null_counts = v1_stats
            .null_counts
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(2, null_counts.value(0)); // 2 nulls in v1
    }

    #[test]
    fn test_batch_pruning_stats_min_max() {
        let metadata = test_metadata();
        let batch = create_batch(
            vec![100, 200, 300],
            vec![Some(10), Some(20), Some(30)],
            vec![Some(1.0), Some(2.0), Some(3.0)],
            vec!["a", "b", "c"],
        );

        let stats = MultiBulkPartStats::compute(&[batch], &metadata);
        let pruning_stats = BatchPruningStats::new(&stats, metadata.clone(), false);

        // Test num_containers
        assert_eq!(1, pruning_stats.num_containers());

        // Test min_values
        let ts_col = Column::from_name("ts");
        let min_ts = pruning_stats.min_values(&ts_col).unwrap();
        let min_ts_arr = min_ts
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(100, min_ts_arr.value(0));

        // Test max_values
        let max_ts = pruning_stats.max_values(&ts_col).unwrap();
        let max_ts_arr = max_ts
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(300, max_ts_arr.value(0));

        // Test row_counts
        let row_counts = pruning_stats.row_counts(&ts_col).unwrap();
        let row_counts_arr = row_counts
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(3, row_counts_arr.value(0));
    }

    #[test]
    fn test_batch_pruning_stats_skip_fields() {
        let metadata = test_metadata();
        let batch = create_batch(
            vec![100, 200],
            vec![Some(10), Some(20)],
            vec![Some(1.0), Some(2.0)],
            vec!["a", "b"],
        );

        let stats = MultiBulkPartStats::compute(&[batch], &metadata);

        // With skip_fields = false, should return field stats
        let pruning_stats = BatchPruningStats::new(&stats, metadata.clone(), false);
        let v0_col = Column::from_name("v0");
        assert!(pruning_stats.min_values(&v0_col).is_some());

        // With skip_fields = true, should not return field stats
        let pruning_stats_skip = BatchPruningStats::new(&stats, metadata.clone(), true);
        assert!(pruning_stats_skip.min_values(&v0_col).is_none());

        // Timestamp column should still work with skip_fields = true
        let ts_col = Column::from_name("ts");
        assert!(pruning_stats_skip.min_values(&ts_col).is_some());

        // Tag column should still work with skip_fields = true
        let tag_col = Column::from_name("tag");
        assert!(pruning_stats_skip.min_values(&tag_col).is_some());
    }

    #[test]
    fn test_batch_pruning_stats_unknown_column() {
        let metadata = test_metadata();
        let batch = create_batch(
            vec![100, 200],
            vec![Some(10), Some(20)],
            vec![Some(1.0), Some(2.0)],
            vec!["a", "b"],
        );

        let stats = MultiBulkPartStats::compute(&[batch], &metadata);
        let pruning_stats = BatchPruningStats::new(&stats, metadata, false);

        // Unknown column should return None
        let unknown_col = Column::from_name("unknown");
        assert!(pruning_stats.min_values(&unknown_col).is_none());
        assert!(pruning_stats.max_values(&unknown_col).is_none());
        assert!(pruning_stats.null_counts(&unknown_col).is_none());
    }

    #[test]
    fn test_batch_pruning_stats_contained() {
        let metadata = test_metadata();
        let batch = create_batch(
            vec![100, 200],
            vec![Some(10), Some(20)],
            vec![Some(1.0), Some(2.0)],
            vec!["a", "b"],
        );

        let stats = MultiBulkPartStats::compute(&[batch], &metadata);
        let pruning_stats = BatchPruningStats::new(&stats, metadata, false);

        // contained() should always return None (not implemented)
        let ts_col = Column::from_name("ts");
        let values = HashSet::new();
        assert!(pruning_stats.contained(&ts_col, &values).is_none());
    }
}
