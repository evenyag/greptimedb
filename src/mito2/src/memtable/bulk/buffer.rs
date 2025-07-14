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

//! Unordered buffer for bulk memtable operations

use std::sync::Arc;

use api::v1::OpType;
use datatypes::arrow::array::{
    ArrayRef, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt64Array, UInt8Array,
};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::{MutableVector, Vector, VectorRef};
use datatypes::types::TimestampType;
use datatypes::value::ValueRef;
use datatypes::vectors::{
    Helper, TimestampMicrosecondVector, TimestampMillisecondVector, TimestampNanosecondVector,
    TimestampSecondVector, UInt64Vector, UInt8Vector,
};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{self, ComputeVectorSnafu, Result};
use crate::memtable::builder::{FieldBuilder, StringBuilder};

/// Builder for bulk operations that extends FieldBuilder functionality
pub(crate) enum BulkValueBuilder {
    /// Regular field builder for regular columns (both primary keys and fields)
    Regular(FieldBuilder),
    /// Timestamp builder using Vec<i64> for direct timestamp values
    Timestamp(Vec<i64>, ConcreteDataType),
    /// Sequence builder using Vec<u64>
    Sequence(Vec<u64>),
    /// Operation type builder using Vec<u8>
    OpType(Vec<u8>),
}

impl BulkValueBuilder {
    /// Creates a new BulkValueBuilder for a regular column (field/primary key)
    pub fn new_regular(data_type: &ConcreteDataType, capacity: usize) -> Self {
        Self::Regular(FieldBuilder::create(data_type, capacity))
    }

    /// Creates a new BulkValueBuilder for timestamp
    pub fn new_timestamp(timestamp_type: ConcreteDataType, capacity: usize) -> Self {
        Self::Timestamp(Vec::with_capacity(capacity), timestamp_type)
    }

    /// Creates a new BulkValueBuilder for sequence
    pub fn new_sequence(capacity: usize) -> Self {
        Self::Sequence(Vec::with_capacity(capacity))
    }

    /// Creates a new BulkValueBuilder for operation type
    pub fn new_op_type(capacity: usize) -> Self {
        Self::OpType(Vec::with_capacity(capacity))
    }

    /// Pushes a value into the builder
    ///
    /// # Safety
    /// For timestamp values, this method unwraps the timestamp assuming it's valid.
    /// The caller must ensure the timestamp value is not null and is a valid timestamp.
    pub fn push_value(&mut self, value: ValueRef) -> Result<()> {
        match self {
            Self::Regular(builder) => builder.push(value).context(ComputeVectorSnafu),
            Self::Timestamp(vec, _) => {
                let ts = value.as_timestamp().context(error::DataTypeMismatchSnafu)?;
                // Safety: timestamp values should not be null in bulk operations
                vec.push(ts.unwrap().value());
                Ok(())
            }
            Self::Sequence(vec) => {
                let seq = value.as_u64().context(error::DataTypeMismatchSnafu)?;
                // Safety: sequence values should not be null in bulk operations
                vec.push(seq.unwrap());
                Ok(())
            }
            Self::OpType(vec) => {
                let op = value.as_u8().context(error::DataTypeMismatchSnafu)?;
                // Safety: op type values should not be null in bulk operations
                vec.push(op.unwrap());
                Ok(())
            }
        }
    }

    /// Pushes an Arrow ArrayRef into the builder
    pub fn push_array(&mut self, array: ArrayRef) -> Result<()> {
        match self {
            Self::Regular(FieldBuilder::String(builder)) => {
                let string_array = array.as_any().downcast_ref::<StringArray>().context(
                    error::InvalidBatchSnafu {
                        reason: "Expected StringArray for string field",
                    },
                )?;
                builder.append_array(string_array);
                Ok(())
            }
            Self::Regular(FieldBuilder::Other(builder)) => {
                let vector = Helper::try_into_vector(&array).context(ComputeVectorSnafu)?;
                builder
                    .extend_slice_of(&*vector, 0, vector.len())
                    .context(ComputeVectorSnafu)?;
                Ok(())
            }
            Self::Timestamp(vec, timestamp_type) => {
                match timestamp_type {
                    ConcreteDataType::Timestamp(TimestampType::Second(_)) => {
                        let ts_array = array
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .context(error::InvalidBatchSnafu {
                                reason: "Expected TimestampSecondArray for timestamp second",
                            })?;
                        vec.extend_from_slice(ts_array.values());
                    }
                    ConcreteDataType::Timestamp(TimestampType::Millisecond(_)) => {
                        let ts_array = array
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .context(error::InvalidBatchSnafu {
                            reason: "Expected TimestampMillisecondArray for timestamp millisecond",
                        })?;
                        vec.extend_from_slice(ts_array.values());
                    }
                    ConcreteDataType::Timestamp(TimestampType::Microsecond(_)) => {
                        let ts_array = array
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .context(error::InvalidBatchSnafu {
                            reason: "Expected TimestampMicrosecondArray for timestamp microsecond",
                        })?;
                        vec.extend_from_slice(ts_array.values());
                    }
                    ConcreteDataType::Timestamp(TimestampType::Nanosecond(_)) => {
                        let ts_array = array
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .context(error::InvalidBatchSnafu {
                                reason:
                                    "Expected TimestampNanosecondArray for timestamp nanosecond",
                            })?;
                        vec.extend_from_slice(ts_array.values());
                    }
                    _ => unreachable!(),
                }
                Ok(())
            }
            Self::Sequence(vec) => {
                let sequence_array = array.as_any().downcast_ref::<UInt64Array>().context(
                    error::InvalidBatchSnafu {
                        reason: "Expected UInt64Array for sequence",
                    },
                )?;
                vec.extend_from_slice(sequence_array.values());
                Ok(())
            }
            Self::OpType(vec) => {
                let op_type_array = array.as_any().downcast_ref::<UInt8Array>().context(
                    error::InvalidBatchSnafu {
                        reason: "Expected UInt8Array for op type",
                    },
                )?;
                vec.extend_from_slice(op_type_array.values());
                Ok(())
            }
        }
    }

    /// Pushes null values into the builder
    pub fn push_nulls(&mut self, count: usize) {
        match self {
            Self::Regular(builder) => builder.push_nulls(count),
            Self::Timestamp(vec, _) => {
                vec.extend(std::iter::repeat_n(0, count));
            }
            Self::Sequence(vec) => {
                vec.extend(std::iter::repeat_n(0, count));
            }
            Self::OpType(vec) => {
                vec.extend(std::iter::repeat_n(OpType::Put as u8, count));
            }
        }
    }

    /// Returns the current length of the builder
    pub fn len(&self) -> usize {
        match self {
            Self::Regular(FieldBuilder::String(builder)) => builder.len(),
            Self::Regular(FieldBuilder::Other(builder)) => builder.len(),
            Self::Timestamp(vec, _) => vec.len(),
            Self::Sequence(vec) => vec.len(),
            Self::OpType(vec) => vec.len(),
        }
    }

    /// Returns true if the builder is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Converts the builder to a VectorRef
    pub fn finish(&mut self) -> Result<VectorRef> {
        match self {
            Self::Regular(builder) => Ok(builder.finish()),
            Self::Timestamp(vec, timestamp_type) => {
                let timestamp: VectorRef = match timestamp_type {
                    ConcreteDataType::Timestamp(TimestampType::Second(_)) => {
                        Arc::new(TimestampSecondVector::from_vec(vec.clone()))
                    }
                    ConcreteDataType::Timestamp(TimestampType::Millisecond(_)) => {
                        Arc::new(TimestampMillisecondVector::from_vec(vec.clone()))
                    }
                    ConcreteDataType::Timestamp(TimestampType::Microsecond(_)) => {
                        Arc::new(TimestampMicrosecondVector::from_vec(vec.clone()))
                    }
                    ConcreteDataType::Timestamp(TimestampType::Nanosecond(_)) => {
                        Arc::new(TimestampNanosecondVector::from_vec(vec.clone()))
                    }
                    _ => unreachable!(),
                };
                Ok(timestamp)
            }
            Self::Sequence(vec) => Ok(Arc::new(UInt64Vector::from_vec(vec.clone()))),
            Self::OpType(vec) => Ok(Arc::new(UInt8Vector::from_vec(vec.clone()))),
        }
    }
}

/// Unordered buffer for bulk memtable operations
pub struct BulkBuffer {
    /// List of builders for each column
    builders: Vec<BulkValueBuilder>,
    /// Current row count
    row_count: usize,
}

impl BulkBuffer {
    /// Creates a new BulkBuffer with the given list of builders
    pub fn new(builders: Vec<BulkValueBuilder>) -> Self {
        Self {
            builders,
            row_count: 0,
        }
    }

    /// Pushes a value into the builder at the specified column index
    pub fn push_value(&mut self, column_index: usize, value: ValueRef) -> Result<()> {
        ensure!(
            column_index < self.builders.len(),
            error::InvalidBatchSnafu {
                reason: format!("Column index {} out of bounds", column_index),
            }
        );

        self.builders[column_index].push_value(value)
    }

    /// Pushes an ArrayRef into the builder at the specified column index
    pub fn push_array(&mut self, column_index: usize, array: ArrayRef) -> Result<()> {
        ensure!(
            column_index < self.builders.len(),
            error::InvalidBatchSnafu {
                reason: format!("Column index {} out of bounds", column_index),
            }
        );

        self.builders[column_index].push_array(array)
    }

    /// Finishes adding one row and bumps the row count
    pub fn finish_one(&mut self) {
        self.row_count += 1;
    }

    /// Returns the current row count
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Returns the number of columns
    pub fn column_count(&self) -> usize {
        self.builders.len()
    }

    /// Converts all builders to a list of VectorRef
    pub fn finish_vectors(&mut self) -> Result<Vec<VectorRef>> {
        let mut vectors = Vec::with_capacity(self.builders.len());
        for builder in &mut self.builders {
            vectors.push(builder.finish()?);
        }
        Ok(vectors)
    }

    /// Checks if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }
}

/// A buffer that contains a list of sorted record batches.
#[derive(Default)]
pub(crate) struct SortedBatchBuffer {
    batches: Vec<RecordBatch>,
    memory_size: usize,
    num_rows: usize,
}

impl SortedBatchBuffer {
    /// Pushes a new record batch into the buffer.
    pub(crate) fn push(&mut self, batch: RecordBatch) {
        self.memory_size += batch.get_array_memory_size();
        self.num_rows += batch.num_rows();
        self.batches.push(batch);
    }

    /// Returns the memory size of the buffer.
    pub(crate) fn memory_size(&self) -> usize {
        self.memory_size
    }

    /// Returns the number of rows in the buffer.
    pub(crate) fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Finishes the buffer and returns all record batches.
    pub(crate) fn finish(&mut self) -> Vec<RecordBatch> {
        self.memory_size = 0;
        self.num_rows = 0;
        std::mem::take(&mut self.batches)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_time::Timestamp;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::value::Value;
    use datatypes::vectors::{Int64Vector, TimestampMillisecondVector, UInt64Vector, UInt8Vector};

    use super::*;

    #[test]
    fn test_bulk_value_builder_regular() {
        let data_type = ConcreteDataType::int64_datatype();
        let mut builder = BulkValueBuilder::new_regular(&data_type, 10);

        builder.push_value(ValueRef::Int64(42)).unwrap();
        builder.push_value(ValueRef::Int64(24)).unwrap();

        assert_eq!(2, builder.len());

        let actual_vector = builder.finish().unwrap();
        let expected_vector = Arc::new(Int64Vector::from_vec(vec![42, 24])) as VectorRef;
        assert_eq!(expected_vector, actual_vector);
    }

    #[test]
    fn test_bulk_value_builder_timestamp() {
        let timestamp_type = ConcreteDataType::timestamp_millisecond_datatype();
        let mut builder = BulkValueBuilder::new_timestamp(timestamp_type, 10);

        builder
            .push_value(ValueRef::Timestamp(Timestamp::new_millisecond(1000)))
            .unwrap();
        builder
            .push_value(ValueRef::Timestamp(Timestamp::new_millisecond(2000)))
            .unwrap();

        assert_eq!(2, builder.len());

        let actual_vector = builder.finish().unwrap();
        let expected_vector =
            Arc::new(TimestampMillisecondVector::from_vec(vec![1000, 2000])) as VectorRef;
        assert_eq!(expected_vector, actual_vector);
    }

    #[test]
    fn test_bulk_value_builder_sequence() {
        let mut builder = BulkValueBuilder::new_sequence(10);

        builder.push_value(ValueRef::UInt64(100)).unwrap();
        builder.push_value(ValueRef::UInt64(200)).unwrap();

        assert_eq!(2, builder.len());

        let actual_vector = builder.finish().unwrap();
        let expected_vector = Arc::new(UInt64Vector::from_vec(vec![100, 200])) as VectorRef;
        assert_eq!(expected_vector, actual_vector);
    }

    #[test]
    fn test_bulk_value_builder_op_type() {
        let mut builder = BulkValueBuilder::new_op_type(10);

        builder.push_value(ValueRef::UInt8(1)).unwrap();
        builder.push_value(ValueRef::UInt8(2)).unwrap();

        assert_eq!(2, builder.len());

        let actual_vector = builder.finish().unwrap();
        let expected_vector = Arc::new(UInt8Vector::from_vec(vec![1, 2])) as VectorRef;
        assert_eq!(expected_vector, actual_vector);
    }

    #[test]
    fn test_bulk_value_builder_array() {
        let data_type = ConcreteDataType::int64_datatype();
        let mut builder = BulkValueBuilder::new_regular(&data_type, 10);

        let vector = Arc::new(Int64Vector::from_vec(vec![1, 2, 3]));
        let array = vector.to_arrow_array();

        builder.push_array(array).unwrap();

        assert_eq!(3, builder.len());

        let actual_vector = builder.finish().unwrap();
        let expected_vector = Arc::new(Int64Vector::from_vec(vec![1, 2, 3])) as VectorRef;
        assert_eq!(expected_vector, actual_vector);
    }

    #[test]
    fn test_bulk_value_builder_timestamp_array() {
        let timestamp_type = ConcreteDataType::timestamp_millisecond_datatype();
        let mut builder = BulkValueBuilder::new_timestamp(timestamp_type, 10);

        let vector = Arc::new(TimestampMillisecondVector::from_vec(vec![1000, 2000, 3000]));
        let array = vector.to_arrow_array();

        builder.push_array(array).unwrap();

        assert_eq!(3, builder.len());

        let actual_vector = builder.finish().unwrap();
        let expected_vector =
            Arc::new(TimestampMillisecondVector::from_vec(vec![1000, 2000, 3000])) as VectorRef;
        assert_eq!(expected_vector, actual_vector);
    }

    #[test]
    fn test_bulk_value_builder_sequence_array() {
        let mut builder = BulkValueBuilder::new_sequence(10);

        let vector = Arc::new(UInt64Vector::from_vec(vec![100, 200, 300]));
        let array = vector.to_arrow_array();

        builder.push_array(array).unwrap();

        assert_eq!(3, builder.len());

        let actual_vector = builder.finish().unwrap();
        let expected_vector = Arc::new(UInt64Vector::from_vec(vec![100, 200, 300])) as VectorRef;
        assert_eq!(expected_vector, actual_vector);
    }

    #[test]
    fn test_bulk_value_builder_op_type_array() {
        let mut builder = BulkValueBuilder::new_op_type(10);

        let vector = Arc::new(UInt8Vector::from_vec(vec![1, 2, 3]));
        let array = vector.to_arrow_array();

        builder.push_array(array).unwrap();

        assert_eq!(3, builder.len());

        let actual_vector = builder.finish().unwrap();
        let expected_vector = Arc::new(UInt8Vector::from_vec(vec![1, 2, 3])) as VectorRef;
        assert_eq!(expected_vector, actual_vector);
    }

    #[test]
    fn test_bulk_buffer_basic() {
        let builders = vec![
            BulkValueBuilder::new_timestamp(ConcreteDataType::timestamp_millisecond_datatype(), 10),
            BulkValueBuilder::new_sequence(10),
            BulkValueBuilder::new_op_type(10),
            BulkValueBuilder::new_regular(&ConcreteDataType::string_datatype(), 10),
            BulkValueBuilder::new_regular(&ConcreteDataType::int64_datatype(), 10),
        ];
        let mut buffer = BulkBuffer::new(builders);

        // Test basic properties
        assert_eq!(0, buffer.row_count());
        assert!(buffer.is_empty());
        assert_eq!(5, buffer.column_count());

        // Test finish_one
        buffer.finish_one();
        assert_eq!(1, buffer.row_count());
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_bulk_buffer_push_values() {
        let builders = vec![
            BulkValueBuilder::new_timestamp(ConcreteDataType::timestamp_millisecond_datatype(), 10),
            BulkValueBuilder::new_sequence(10),
            BulkValueBuilder::new_op_type(10),
        ];
        let mut buffer = BulkBuffer::new(builders);

        // Push values to different columns
        buffer
            .push_value(0, ValueRef::Timestamp(Timestamp::new_millisecond(1000)))
            .unwrap();
        buffer.push_value(1, ValueRef::UInt64(100)).unwrap();
        buffer.push_value(2, ValueRef::UInt8(1)).unwrap();

        buffer.finish_one();
        assert_eq!(1, buffer.row_count());
    }

    #[test]
    fn test_bulk_buffer_to_vectors() {
        let builders = vec![
            BulkValueBuilder::new_timestamp(ConcreteDataType::timestamp_millisecond_datatype(), 10),
            BulkValueBuilder::new_sequence(10),
            BulkValueBuilder::new_op_type(10),
        ];
        let mut buffer = BulkBuffer::new(builders);

        // Add some data
        buffer
            .push_value(0, ValueRef::Timestamp(Timestamp::new_millisecond(1000)))
            .unwrap();
        buffer.push_value(1, ValueRef::UInt64(100)).unwrap();
        buffer.push_value(2, ValueRef::UInt8(1)).unwrap();

        let actual_vectors = buffer.finish_vectors().unwrap();
        let expected_vectors = vec![
            Arc::new(TimestampMillisecondVector::from_vec(vec![1000])) as VectorRef,
            Arc::new(UInt64Vector::from_vec(vec![100])) as VectorRef,
            Arc::new(UInt8Vector::from_vec(vec![1])) as VectorRef,
        ];
        assert_eq!(expected_vectors, actual_vectors);
    }

    #[test]
    fn test_bulk_buffer_out_of_bounds() {
        let builders = vec![BulkValueBuilder::new_regular(
            &ConcreteDataType::int64_datatype(),
            10,
        )];
        let mut buffer = BulkBuffer::new(builders);

        let result = buffer.push_value(100, ValueRef::Int64(42));
        assert!(result.is_err());
    }

    #[test]
    fn test_bulk_value_builder_push_nulls() {
        let timestamp_type = ConcreteDataType::timestamp_millisecond_datatype();
        let mut ts_builder = BulkValueBuilder::new_timestamp(timestamp_type, 10);
        ts_builder.push_nulls(2);

        let actual_vector = ts_builder.finish().unwrap();
        let expected_vector =
            Arc::new(TimestampMillisecondVector::from_vec(vec![0, 0])) as VectorRef;
        assert_eq!(expected_vector, actual_vector);
    }
}
