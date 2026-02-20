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

//! Post-filter readers that apply field filters after merge/dedup.
//!
//! In non-append mode, field filters cannot be applied during prefiltering because
//! deduplication may change which rows survive. `PostFilterReader` applies these
//! filters after the dedup step.

use std::ops::BitAnd;

use async_trait::async_trait;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datafusion_expr::Expr;
use datatypes::arrow::array::BooleanArray;
use datatypes::arrow::buffer::BooleanBuffer;
use snafu::ResultExt;
use store_api::storage::ColumnId;

use crate::error::{RecordBatchSnafu, Result};
use crate::read::{Batch, BatchReader, BoxedBatchReader};

/// A reader that applies field filters to batches after merge/dedup.
pub struct PostFilterReader {
    inner: BoxedBatchReader,
    /// Field filter evaluators, each paired with its column_id.
    field_filters: Vec<(ColumnId, SimpleFilterEvaluator)>,
    /// Number of rows filtered out by this reader.
    rows_filtered: usize,
}

impl PostFilterReader {
    /// Creates a new `PostFilterReader`.
    ///
    /// `postfilter_exprs` are field-referencing expressions that should be applied
    /// after deduplication. Each is converted to a `SimpleFilterEvaluator` if possible.
    pub fn new(
        inner: BoxedBatchReader,
        postfilter_exprs: &[Expr],
        metadata: &store_api::metadata::RegionMetadata,
    ) -> Self {
        let mut field_filters = Vec::new();
        for expr in postfilter_exprs {
            if let Some(evaluator) = SimpleFilterEvaluator::try_new(expr) {
                // Find the column_id for this evaluator's column
                if let Some(col_meta) = metadata.column_by_name(evaluator.column_name()) {
                    field_filters.push((col_meta.column_id, evaluator));
                }
            }
        }

        Self {
            inner,
            field_filters,
            rows_filtered: 0,
        }
    }

    /// Returns the number of rows filtered out.
    #[allow(dead_code)]
    pub fn rows_filtered(&self) -> usize {
        self.rows_filtered
    }
}

#[async_trait]
impl BatchReader for PostFilterReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        while let Some(mut batch) = self.inner.next_batch().await? {
            if self.field_filters.is_empty() {
                return Ok(Some(batch));
            }

            let num_rows_before = batch.num_rows();
            let mut mask = BooleanBuffer::new_set(num_rows_before);

            for (column_id, evaluator) in &self.field_filters {
                if let Some(batch_col) = batch.field_col_value(*column_id) {
                    let result = evaluator
                        .evaluate_vector(&batch_col.data)
                        .context(RecordBatchSnafu)?;
                    mask = mask.bitand(&result);
                }
                // If the field column is not present in the batch, skip this filter
            }

            batch.filter(&BooleanArray::from(mask).into())?;

            let filtered_rows = num_rows_before - batch.num_rows();
            self.rows_filtered += filtered_rows;

            if !batch.is_empty() {
                return Ok(Some(batch));
            }
            // Empty batch after filtering, continue to next
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::OpType;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{col, lit};

    use super::*;
    use crate::test_util::new_batch_builder;

    struct MockBatchReader {
        batches: Vec<Batch>,
        index: usize,
    }

    #[async_trait]
    impl BatchReader for MockBatchReader {
        async fn next_batch(&mut self) -> Result<Option<Batch>> {
            if self.index < self.batches.len() {
                let batch = self.batches[self.index].clone();
                self.index += 1;
                Ok(Some(batch))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test]
    async fn test_postfilter_no_filters() {
        use api::v1::SemanticType;
        use datatypes::prelude::ConcreteDataType;
        use datatypes::schema::ColumnSchema;
        use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
        use store_api::storage::RegionId;

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
                column_schema: ColumnSchema::new("v0", ConcreteDataType::uint64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .primary_key(vec![]);
        let metadata = builder.build().unwrap();

        let batch = new_batch_builder(
            b"",
            &[1, 2, 3],
            &[1, 1, 1],
            &[OpType::Put, OpType::Put, OpType::Put],
            1,
            &[10, 20, 30],
        )
        .build()
        .unwrap();

        let reader = Box::new(MockBatchReader {
            batches: vec![batch.clone()],
            index: 0,
        });

        let mut post_reader = PostFilterReader::new(reader, &[], &metadata);
        let result = post_reader.next_batch().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(post_reader.rows_filtered(), 0);
    }

    #[tokio::test]
    async fn test_postfilter_with_field_filter() {
        use api::v1::SemanticType;
        use datatypes::prelude::ConcreteDataType;
        use datatypes::schema::ColumnSchema;
        use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
        use store_api::storage::RegionId;

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
                column_schema: ColumnSchema::new("v0", ConcreteDataType::uint64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .primary_key(vec![]);
        let metadata = builder.build().unwrap();

        let batch = new_batch_builder(
            b"",
            &[1, 2, 3],
            &[1, 1, 1],
            &[OpType::Put, OpType::Put, OpType::Put],
            1,
            &[10, 20, 30],
        )
        .build()
        .unwrap();

        let reader = Box::new(MockBatchReader {
            batches: vec![batch],
            index: 0,
        });

        // Filter: v0 > 15
        let exprs = vec![col("v0").gt(lit(ScalarValue::UInt64(Some(15))))];
        let mut post_reader = PostFilterReader::new(reader, &exprs, &metadata);
        let result = post_reader.next_batch().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 2); // Only rows with v0=20 and v0=30
        assert_eq!(post_reader.rows_filtered(), 1);
    }
}
