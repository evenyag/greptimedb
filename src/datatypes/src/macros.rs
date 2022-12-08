// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Some helper macros for datatypes, copied from databend.

/// Apply the macro rules to all primitive types.
#[macro_export]
macro_rules! for_all_primitive_types {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { i8 },
            { i16 },
            { i32 },
            { i64 },
            { u8 },
            { u16 },
            { u32 },
            { u64 },
            { f32 },
            { f64 }
        }
    };
}

/// Match the logical type and apply `$body` to all primitive types and
/// `nbody` to other types.
#[macro_export]
macro_rules! with_match_primitive_type_id {
    ($key_type:expr, | $_:tt $T:ident | $body:tt, $nbody:tt) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        use $crate::type_id::LogicalTypeId;
        use $crate::types::{
            Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
            UInt32Type, UInt64Type, UInt8Type,
        };
        match $key_type {
            LogicalTypeId::Int8 => __with_ty__! { Int8Type },
            LogicalTypeId::Int16 => __with_ty__! { Int16Type },
            LogicalTypeId::Int32 => __with_ty__! { Int32Type },
            LogicalTypeId::Int64 => __with_ty__! { Int64Type },
            LogicalTypeId::UInt8 => __with_ty__! { UInt8Type },
            LogicalTypeId::UInt16 => __with_ty__! { UInt16Type },
            LogicalTypeId::UInt32 => __with_ty__! { UInt32Type },
            LogicalTypeId::UInt64 => __with_ty__! { UInt64Type },
            LogicalTypeId::Float32 => __with_ty__! { Float32Type },
            LogicalTypeId::Float64 => __with_ty__! { Float64Type },

            _ => $nbody,
        }
    }};
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use std::iter;
    use std::sync::Arc;
    use crate::error::{Result, Error};
    use crate::vectors::{Helper, ConstantVector, Int32Vector, Int8Vector, UInt8Vector, Float32Vector, UInt16Vector, Int16Vector, UInt32Vector};
    use num_traits::AsPrimitive;

    use chrono_tz::Tz;

    pub struct EvalContext {
        // _tz: Tz,
        // pub error: Option<Error>,
    }

    impl Default for EvalContext {
        fn default() -> Self {
            // let tz = "UTC".parse::<Tz>().unwrap();
            Self {
                // error: None,
                // _tz: tz,
            }
        }
    }

    // impl EvalContext {
    //     pub fn set_error(&mut self, e: Error) {
    //         if self.error.is_none() {
    //             self.error = Some(e);
    //         }
    //     }
    // }

    #[inline]
    fn min<T: PartialOrd>(input: T, min: T) -> T {
        if input < min {
            input
        } else {
            min
        }
    }

    #[inline]
    fn max<T: PartialOrd>(input: T, max: T) -> T {
        if input > max {
            input
        } else {
            max
        }
    }

    #[inline]
    fn scalar_min<S, T, O>(left: Option<S>, right: Option<T>, _ctx: &mut EvalContext) -> Option<O>
    where
        S: AsPrimitive<O>,
        T: AsPrimitive<O>,
        O: Scalar + Copy + PartialOrd,
    {
        match (left, right) {
            (Some(left), Some(right)) => Some(min(left.as_(), right.as_())),
            _ => None,
        }
    }

    #[inline]
    fn scalar_max<S, T, O>(left: Option<S>, right: Option<T>, _ctx: &mut EvalContext) -> Option<O>
    where
        S: AsPrimitive<O>,
        T: AsPrimitive<O>,
        O: Scalar + Copy + PartialOrd,
    {
        match (left, right) {
            (Some(left), Some(right)) => Some(max(left.as_(), right.as_())),
            _ => None,
        }
    }

    fn scalar_binary_op<L: Scalar, R: Scalar, O: Scalar, F>(
        l: &VectorRef,
        r: &VectorRef,
        f: F,
        ctx: &mut EvalContext,
    ) -> Result<<O as Scalar>::VectorType>
    where
        F: Fn(Option<L::RefType<'_>>, Option<R::RefType<'_>>, &mut EvalContext) -> Option<O>,
        // F: Fn(Option<L::RefType<'_>>, Option<R::RefType<'_>>) -> Option<O>,
    {
        debug_assert!(
            l.len() == r.len(),
            "Size of vectors must match to apply binary expression"
        );

        let result = {
            let left: &<L as Scalar>::VectorType = unsafe { Helper::static_cast(l) };
            let right: &ConstantVector = unsafe { Helper::static_cast(r) };
            let right: &<R as Scalar>::VectorType = unsafe { Helper::static_cast(right.inner()) };
            let b = right.get_data(0);

            let it = left.iter_data().map(|a| f(a, b, ctx));
            <O as Scalar>::VectorType::from_owned_iterator(it)
        };
        Ok(result)
    }

    fn eval_i64(columns: &[VectorRef]) -> Result<VectorRef> {
        with_match_primitive_type_id!(columns[0].data_type().logical_type_id(), |$S| {
            with_match_primitive_type_id!(columns[1].data_type().logical_type_id(), |$T| {
                with_match_primitive_type_id!(columns[2].data_type().logical_type_id(), |$R| {
                    // clip(a, min, max) is equals to min(max(a, min), max)
                    let col: VectorRef = Arc::new(scalar_binary_op::<
                        <$S as LogicalPrimitiveType>::Wrapper,
                        <$T as LogicalPrimitiveType>::Wrapper,
                        i64,
                        _,
                    >(
                        &columns[0],
                        &columns[1],
                        scalar_max,
                        &mut EvalContext::default(),
                    )?);
                    let col = scalar_binary_op::<i64, <$R as LogicalPrimitiveType>::Wrapper, i64, _>(
                        &col,
                        &columns[2],
                        scalar_min,
                        &mut EvalContext::default(),
                    )?;
                    Ok(Arc::new(col))
                }, {
                    unreachable!()
                })
            }, {
                unreachable!()
            })
        }, {
            unreachable!()
        })
    }

    fn eval_u64(columns: &[VectorRef]) -> Result<VectorRef> {
        with_match_primitive_type_id!(columns[0].data_type().logical_type_id(), |$S| {
            with_match_primitive_type_id!(columns[1].data_type().logical_type_id(), |$T| {
                with_match_primitive_type_id!(columns[2].data_type().logical_type_id(), |$R| {
                    // clip(a, min, max) is equals to min(max(a, min), max)
                    let col: VectorRef = Arc::new(scalar_binary_op::<
                        <$S as LogicalPrimitiveType>::Wrapper,
                        <$T as LogicalPrimitiveType>::Wrapper,
                        u64,
                        _,
                    >(
                        &columns[0],
                        &columns[1],
                        scalar_max,
                        &mut EvalContext::default(),
                    )?);
                    let col = scalar_binary_op::<u64, <$R as LogicalPrimitiveType>::Wrapper, u64, _>(
                        &col,
                        &columns[2],
                        scalar_min,
                        &mut EvalContext::default(),
                    )?;
                    Ok(Arc::new(col))
                }, {
                    unreachable!()
                })
            }, {
                unreachable!()
            })
        }, {
            unreachable!()
        })
    }

    fn eval_f64(columns: &[VectorRef]) -> Result<VectorRef> {
        with_match_primitive_type_id!(columns[0].data_type().logical_type_id(), |$S| {
            with_match_primitive_type_id!(columns[1].data_type().logical_type_id(), |$T| {
                with_match_primitive_type_id!(columns[2].data_type().logical_type_id(), |$R| {
                    // clip(a, min, max) is equals to min(max(a, min), max)
                    let col: VectorRef = Arc::new(scalar_binary_op::<
                        <$S as LogicalPrimitiveType>::Wrapper,
                        <$T as LogicalPrimitiveType>::Wrapper,
                        f64,
                        _,
                    >(
                        &columns[0],
                        &columns[1],
                        scalar_max,
                        &mut EvalContext::default(),
                    )?);
                    let col = scalar_binary_op::<f64, <$R as LogicalPrimitiveType>::Wrapper, f64, _>(
                        &col,
                        &columns[2],
                        scalar_min,
                        &mut EvalContext::default(),
                    )?;
                    Ok(Arc::new(col))
                }, {
                    unreachable!()
                })
            }, {
                unreachable!()
            })
        }, {
            unreachable!()
        })
    }

    #[test]
    fn test_clip_fn_signed() {
        // eval with signed integers
        let args: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from_values(0..10)),
            Arc::new(ConstantVector::new(
                Arc::new(Int8Vector::from_vec(vec![3])),
                10,
            )),
            Arc::new(ConstantVector::new(
                Arc::new(Int16Vector::from_vec(vec![6])),
                10,
            )),
        ];

        let vector = eval_i64(&args).unwrap();
        assert_eq!(10, vector.len());
    }

    #[test]
    fn test_clip_fn_unsigned() {
        // eval with unsigned integers
        let args: Vec<VectorRef> = vec![
            Arc::new(UInt8Vector::from_values(0..10)),
            Arc::new(ConstantVector::new(
                Arc::new(UInt32Vector::from_vec(vec![3])),
                10,
            )),
            Arc::new(ConstantVector::new(
                Arc::new(UInt16Vector::from_vec(vec![6])),
                10,
            )),
        ];

        let vector = eval_i64(&args).unwrap();
        assert_eq!(10, vector.len());
    }

    #[test]
    fn test_clip_fn_float() {
        // eval with floats
        let args: Vec<VectorRef> = vec![
            Arc::new(Int8Vector::from_values(0..10)),
            Arc::new(ConstantVector::new(
                Arc::new(UInt32Vector::from_vec(vec![3])),
                10,
            )),
            Arc::new(ConstantVector::new(
                Arc::new(Float32Vector::from_vec(vec![6f32])),
                10,
            )),
        ];

        let vector = eval_i64(&args).unwrap();
        assert_eq!(10, vector.len());
    }
}
