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

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

use common_time::timestamp::TimeUnit;
use datafusion_common::arrow::array::{ArrayRef, UInt32Builder};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datatypes::value::Value;
use datatypes::vectors::Helper;
use twox_hash::XxHash32;

pub const PARTITION_HASH_UDF_NAME: &str = "partition_hash";
pub const PARTITION_HASH_SEED: u32 = 0;

pub fn hash_column_key(column: &str) -> String {
    // The collider represents both real columns and computed hash dimensions
    // with string keys. Real columns use their names directly, so a display-like
    // key such as `partition_hash(host)` could collide with a quoted column that
    // has that exact name. SQL identifiers cannot contain NUL; use it to put
    // computed dimensions in a private namespace while leaving ordinary column
    // keys unchanged for existing collider consumers.
    format!("\0{PARTITION_HASH_UDF_NAME}:{column}")
}

pub fn partition_hash_value(value: &Value) -> Option<u32> {
    if matches!(value, Value::Null) {
        return None;
    }

    let mut bytes = Vec::new();
    encode_value(value, &mut bytes)?;
    Some(XxHash32::oneshot(PARTITION_HASH_SEED, &bytes))
}

fn push_len_and_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn encode_time_unit(out: &mut Vec<u8>, unit: TimeUnit) {
    out.push(match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    });
}

fn encode_value(value: &Value, out: &mut Vec<u8>) -> Option<()> {
    match value {
        Value::Null => out.push(0),
        Value::Boolean(v) => out.extend_from_slice(&[1, u8::from(*v)]),
        Value::UInt8(v) => out.extend_from_slice(&[2, *v]),
        Value::UInt16(v) => {
            out.push(3);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::UInt32(v) => {
            out.push(4);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::UInt64(v) => {
            out.push(5);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Int8(v) => out.extend_from_slice(&[6, *v as u8]),
        Value::Int16(v) => {
            out.push(7);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Int32(v) => {
            out.push(8);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Int64(v) => {
            out.push(9);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Float32(v) => {
            out.push(10);
            out.extend_from_slice(&v.0.to_bits().to_le_bytes());
        }
        Value::Float64(v) => {
            out.push(11);
            out.extend_from_slice(&v.0.to_bits().to_le_bytes());
        }
        Value::Decimal128(v) => {
            out.push(12);
            push_len_and_bytes(out, v.to_string().as_bytes());
        }
        Value::String(v) => {
            out.push(13);
            push_len_and_bytes(out, v.as_utf8().as_bytes());
        }
        Value::Binary(v) => {
            out.push(14);
            push_len_and_bytes(out, v);
        }
        Value::Date(v) => {
            out.push(15);
            out.extend_from_slice(&v.val().to_le_bytes());
        }
        Value::Timestamp(v) => {
            out.push(16);
            encode_time_unit(out, v.unit());
            out.extend_from_slice(&v.value().to_le_bytes());
        }
        Value::Time(v) => {
            out.push(17);
            encode_time_unit(out, *v.unit());
            out.extend_from_slice(&v.value().to_le_bytes());
        }
        Value::Duration(v) => {
            out.push(18);
            encode_time_unit(out, v.unit());
            out.extend_from_slice(&v.value().to_le_bytes());
        }
        Value::IntervalYearMonth(v) => {
            out.push(19);
            out.extend_from_slice(&v.to_i32().to_le_bytes());
        }
        Value::IntervalDayTime(v) => {
            out.push(20);
            let x: i64 = (*v).into();
            out.extend_from_slice(&x.to_le_bytes());
        }
        Value::IntervalMonthDayNano(v) => {
            out.push(21);
            let x: i128 = (*v).into();
            out.extend_from_slice(&x.to_le_bytes());
        }
        // Complex values are not accepted by partition DDL today, but keep a stable fallback.
        Value::List(_) | Value::Struct(_) | Value::Json(_) => {
            out.push(255);
            let json = serde_json::to_vec(value).ok()?;
            push_len_and_bytes(out, &json);
        }
    }
    Some(())
}

#[derive(Eq)]
struct PartitionHashUdf;

impl Debug for PartitionHashUdf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(PARTITION_HASH_UDF_NAME)
    }
}

impl PartialEq for PartitionHashUdf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Hash for PartitionHashUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        PARTITION_HASH_UDF_NAME.hash(state);
    }
}

impl ScalarUDFImpl for PartitionHashUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        PARTITION_HASH_UDF_NAME
    }
    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::UInt32)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "{PARTITION_HASH_UDF_NAME} expects exactly one argument"
            )));
        }
        match &args.args[0] {
            ColumnarValue::Array(array) => hash_array(array.clone()).map(ColumnarValue::Array),
            ColumnarValue::Scalar(scalar) => {
                let value = Value::try_from(scalar.clone())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok(ColumnarValue::Scalar(ScalarValue::UInt32(
                    partition_hash_value(&value),
                )))
            }
        }
    }
}

fn hash_array(array: ArrayRef) -> datafusion_common::Result<ArrayRef> {
    let vector =
        Helper::try_into_vector(array).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut builder = UInt32Builder::with_capacity(vector.len());
    for idx in 0..vector.len() {
        match partition_hash_value(&vector.get(idx)) {
            Some(hash) => builder.append_value(hash),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn partition_hash_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PartitionHashUdf))
}

#[cfg(test)]
mod tests {
    use datafusion_common::arrow::array::{Array, StringArray, UInt32Array};

    use super::*;

    #[test]
    fn test_partition_hash_is_stable() {
        assert_eq!(
            partition_hash_value(&Value::String("a".into())),
            Some(1099135740)
        );
        assert_eq!(partition_hash_value(&Value::UInt32(42)), Some(3010843987));
        assert_eq!(partition_hash_value(&Value::Null), None);
    }

    #[test]
    fn test_hash_array_matches_scalar_hashing() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("b")]));
        let output = hash_array(input).unwrap();
        let output = output.as_any().downcast_ref::<UInt32Array>().unwrap();

        assert_eq!(
            output.value(0),
            partition_hash_value(&Value::String("a".into())).unwrap()
        );
        assert!(output.is_null(1));
        assert_eq!(
            output.value(2),
            partition_hash_value(&Value::String("b".into())).unwrap()
        );
    }
}
