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

//! Plain batch merger.

use std::sync::Arc;

use async_stream::try_stream;
use datafusion::execution::memory_pool::{MemoryConsumer, UnboundedMemoryPool};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::DataFusionError;
use datatypes::arrow::compute::SortOptions;
use datatypes::arrow::datatypes::SchemaRef;
use futures::TryStreamExt;
use snafu::ResultExt;
use store_api::metadata::RegionMetadata;
use store_api::storage::consts::SEQUENCE_COLUMN_NAME;

use crate::error::{MergeStreamSnafu, Result};
use crate::read::batch::plain::PlainBatch;
use crate::read::BoxedPlainBatchStream;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::to_plain_sst_arrow_schema;

pub(crate) enum PlainSource {
    Stream(BoxedPlainBatchStream),
}

impl PlainSource {
    pub(crate) async fn next_batch(&mut self) -> Result<Option<PlainBatch>> {
        match self {
            PlainSource::Stream(stream) => stream.try_next().await,
        }
    }
}

/// Merges batches from multiple sorted sources into a single sorted stream.
/// Input sources must be sorted by primary key and have the same schema.
pub(crate) fn merge_plain(
    metadata: &RegionMetadata,
    sources: Vec<PlainSource>,
) -> Result<BoxedPlainBatchStream> {
    // TODO(yingwen): Can we pass the schema as an argument?
    let schema = to_plain_sst_arrow_schema(metadata);
    let streams = sources
        .into_iter()
        .map(|source| plain_source_to_stream(source, &schema))
        .collect();
    let exprs = sort_expressions(metadata);

    let memory_pool = Arc::new(UnboundedMemoryPool::default()) as _;
    let reservation = MemoryConsumer::new("merge_plain").register(&memory_pool);
    let mut stream = StreamingMergeBuilder::new()
        .with_schema(schema)
        .with_streams(streams)
        .with_expressions(&exprs)
        .with_batch_size(DEFAULT_READ_BATCH_SIZE)
        .with_reservation(reservation)
        .build()
        .context(MergeStreamSnafu)?;

    let stream = try_stream! {
        while let Some(record_batch) = stream.try_next().await.context(MergeStreamSnafu)? {
            yield PlainBatch::new(record_batch);
        }
    };

    Ok(Box::pin(stream))
}

/// Converts a [PlainSource] into a [SendableRecordBatchStream].
fn plain_source_to_stream(
    mut source: PlainSource,
    schema: &SchemaRef,
) -> SendableRecordBatchStream {
    let stream = try_stream! {
        while let Some(batch) = source
            .next_batch()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        {
            yield batch.into_record_batch();
        }
    };

    let stream = RecordBatchStreamAdapter::new(schema.clone(), stream);
    Box::pin(stream)
}

/// Builds the sort expressions from the region metadata
/// to sort by:
/// (primary key ASC, time index ASC, sequence DESC)
pub(crate) fn sort_expressions(metadata: &RegionMetadata) -> LexOrdering {
    let time_index_expr = create_sort_expr(
        &metadata.time_index_column().column_schema.name,
        metadata.time_index_column_pos(),
        false,
    );
    let sequence_expr =
        create_sort_expr(SEQUENCE_COLUMN_NAME, metadata.column_metadatas.len(), true);

    let exprs = metadata
        .primary_key
        .iter()
        .map(|id| {
            // Safety: We know the primary key exists in the metadata
            let index = metadata.column_index_by_id(*id).unwrap();
            let col_meta = &metadata.column_metadatas[index];
            create_sort_expr(&col_meta.column_schema.name, index, false)
        })
        .chain([time_index_expr, sequence_expr])
        .collect();
    LexOrdering::new(exprs)
}

/// Helper function to create a sort expression for a column.
fn create_sort_expr(column_name: &str, column_index: usize, descending: bool) -> PhysicalSortExpr {
    let column = Column::new(column_name, column_index);
    PhysicalSortExpr {
        expr: Arc::new(column),
        options: SortOptions {
            descending,
            nulls_first: true,
        },
    }
}
