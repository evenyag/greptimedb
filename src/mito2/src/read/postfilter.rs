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
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::error::{ComputeArrowSnafu, RecordBatchSnafu, Result};
use crate::read::{Batch, BatchReader, BoxedBatchReader, BoxedRecordBatchStream};
use crate::sst::parquet::file_range::PartitionFilterContext;

/// A reader that applies field filters to batches after merge/dedup.
pub struct PostFilterReader {
    inner: BoxedBatchReader,
    /// Field filter evaluators, each paired with its column_id.
    field_filters: Vec<(ColumnId, SimpleFilterEvaluator)>,
    /// Partition filter context for physical expression evaluation.
    partition_filter: Option<PartitionFilterContext>,
    /// Region metadata, needed for building RecordBatch from Batch for partition filter.
    metadata: RegionMetadataRef,
    /// Number of rows filtered out by this reader.
    rows_filtered: usize,
}

impl PostFilterReader {
    /// Creates a new `PostFilterReader`.
    ///
    /// `postfilter_exprs` are field-referencing expressions that should be applied
    /// after deduplication. Each is converted to a `SimpleFilterEvaluator` if possible.
    /// `partition_filter` is an optional partition filter using physical expressions
    /// for compound filter evaluation.
    pub fn new(
        inner: BoxedBatchReader,
        postfilter_exprs: &[Expr],
        metadata: RegionMetadataRef,
        partition_filter: Option<PartitionFilterContext>,
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
            partition_filter,
            metadata,
            rows_filtered: 0,
        }
    }

    /// Returns the number of rows filtered out.
    #[cfg(test)]
    fn rows_filtered(&self) -> usize {
        self.rows_filtered
    }
}

#[async_trait]
impl BatchReader for PostFilterReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        while let Some(mut batch) = self.inner.next_batch().await? {
            if self.field_filters.is_empty() && self.partition_filter.is_none() {
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

            // Apply partition filter using physical expressions
            if let Some(partition_filter) = &self.partition_filter {
                let record_batch = partition_filter.build_record_batch_from_batch(
                    &mut batch,
                    &self.metadata,
                    None, // No codec needed: postfilter partition expr only references fields
                )?;
                let partition_mask = partition_filter.evaluate(&record_batch)?;
                mask = mask.bitand(&partition_mask);
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

/// A stream wrapper that applies field filters to `RecordBatch`es after merge/dedup
/// in flat format scans.
pub struct FlatPostFilterStream {
    inner: BoxedRecordBatchStream,
    /// Field filter evaluators, each paired with its column name.
    field_filters: Vec<(String, SimpleFilterEvaluator)>,
    /// Partition filter context for physical expression evaluation.
    partition_filter: Option<PartitionFilterContext>,
}

impl FlatPostFilterStream {
    /// Creates a new `FlatPostFilterStream`.
    pub fn new(
        inner: BoxedRecordBatchStream,
        postfilter_exprs: &[Expr],
        metadata: &store_api::metadata::RegionMetadata,
        partition_filter: Option<PartitionFilterContext>,
    ) -> Self {
        let mut field_filters = Vec::new();
        for expr in postfilter_exprs {
            if let Some(evaluator) = SimpleFilterEvaluator::try_new(expr)
                && metadata.column_by_name(evaluator.column_name()).is_some()
            {
                field_filters.push((evaluator.column_name().to_string(), evaluator));
            }
        }

        Self {
            inner,
            field_filters,
            partition_filter,
        }
    }

    /// Converts this into a `BoxedRecordBatchStream`.
    pub fn into_stream(self) -> BoxedRecordBatchStream {
        Box::pin(futures::stream::unfold(self, |mut this| async move {
            loop {
                let batch = match this.inner.next().await? {
                    Ok(batch) => batch,
                    Err(e) => return Some((Err(e), this)),
                };

                if this.field_filters.is_empty() && this.partition_filter.is_none() {
                    return Some((Ok(batch), this));
                }

                match this.filter_record_batch(&batch) {
                    Ok(Some(filtered)) => return Some((Ok(filtered), this)),
                    Ok(None) => continue, // Batch completely filtered out
                    Err(e) => return Some((Err(e), this)),
                }
            }
        }))
    }

    fn filter_record_batch(&self, batch: &RecordBatch) -> Result<Option<RecordBatch>> {
        let mut mask = BooleanBuffer::new_set(batch.num_rows());

        for (col_name, evaluator) in &self.field_filters {
            if let Ok(col_idx) = batch.schema().index_of(col_name) {
                let column = batch.column(col_idx);
                let result = evaluator.evaluate_array(column).context(RecordBatchSnafu)?;
                mask = mask.bitand(&result);
            }
        }

        // Apply partition filter using physical expressions
        if let Some(partition_filter) = &self.partition_filter {
            let projected = partition_filter.project_record_batch(batch)?;
            let partition_mask = partition_filter.evaluate(&projected)?;
            mask = mask.bitand(&partition_mask);
        }

        if mask.count_set_bits() == 0 {
            return Ok(None);
        }

        let filtered =
            datatypes::arrow::compute::filter_record_batch(batch, &BooleanArray::from(mask))
                .context(ComputeArrowSnafu)?;

        if filtered.num_rows() > 0 {
            Ok(Some(filtered))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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

        let metadata = Arc::new(metadata);
        let mut post_reader = PostFilterReader::new(reader, &[], metadata, None);
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
        let metadata = Arc::new(metadata);
        let mut post_reader = PostFilterReader::new(reader, &exprs, metadata, None);
        let result = post_reader.next_batch().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 2); // Only rows with v0=20 and v0=30
        assert_eq!(post_reader.rows_filtered(), 1);
    }
}
