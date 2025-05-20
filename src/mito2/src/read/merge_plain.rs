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

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::try_stream;
use datafusion::execution::memory_pool::{MemoryConsumer, UnboundedMemoryPool};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::DataFusionError;
use datatypes::arrow::array::{make_comparator, ArrayRef, UInt64Array};
use datatypes::arrow::compute::SortOptions;
use datatypes::arrow::datatypes::{Schema, SchemaRef};
use futures::TryStreamExt;
use snafu::ResultExt;
use store_api::metadata::RegionMetadata;
use store_api::storage::consts::SEQUENCE_COLUMN_NAME;

use crate::error::{ComputeArrowSnafu, MergeStreamSnafu, Result};
use crate::memtable::partition_tree::data::timestamp_array_to_i64_slice;
use crate::metrics::READ_STAGE_ELAPSED;
use crate::read::batch::plain::PlainBatch;
use crate::read::projection::PlainProjectionMapper;
use crate::read::{BoxedPlainBatchStream, Source};
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::{
    plain_column_metadata_to_field_for_read, plain_internal_fields, to_plain_sst_arrow_schema,
    to_plain_sst_arrow_schema_for_read,
};

pub(crate) enum PlainSource {
    Stream(BoxedPlainBatchStream),
}

impl PlainSource {
    /// Creates a [PlainSource] from [Source].
    pub(crate) fn from_source(source: Source) -> Self {
        if let Source::PlainStream(stream) = source {
            Self::Stream(stream)
        } else {
            panic!("Not a plain stream");
        }
    }

    /// Converts a [PlainSource] to a [Source].
    pub(crate) fn into_source(self) -> Source {
        match self {
            PlainSource::Stream(stream) => Source::PlainStream(stream),
        }
    }

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
    mapper: Option<&PlainProjectionMapper>,
) -> Result<BoxedPlainBatchStream> {
    // TODO(yingwen): Can we pass the schema as an argument?
    let schema = match mapper {
        Some(mapper) => {
            let batch_schema = mapper.batch_schema();
            let fields: Vec<_> = batch_schema
                .iter()
                .map(|(id, _)| {
                    let index = metadata.column_index_by_id(*id).unwrap();
                    plain_column_metadata_to_field_for_read(metadata, index)
                })
                .chain(plain_internal_fields())
                .collect();
            Arc::new(Schema::new(fields))
        }
        None => to_plain_sst_arrow_schema_for_read(metadata),
    };
    let streams = sources
        .into_iter()
        .map(|source| plain_source_to_stream(source, &schema))
        .collect();
    let exprs = sort_expressions(metadata, mapper);

    common_telemetry::info!("Merge plain, exprs: {:?}, schema: {:?}", exprs, schema);

    let memory_pool = Arc::new(UnboundedMemoryPool::default()) as _;
    let reservation = MemoryConsumer::new("merge_plain").register(&memory_pool);
    let metrics_set = ExecutionPlanMetricsSet::new();
    let baseline_metrics = BaselineMetrics::new(&metrics_set, 0);
    let mut stream = StreamingMergeBuilder::new()
        .with_schema(schema)
        .with_streams(streams)
        .with_expressions(&exprs)
        .with_batch_size(DEFAULT_READ_BATCH_SIZE)
        .with_reservation(reservation)
        .with_metrics(baseline_metrics)
        .build()
        .context(MergeStreamSnafu)?;

    let stream = try_stream! {
        while let Some(record_batch) = stream.try_next().await.context(MergeStreamSnafu)? {
            yield PlainBatch::new(record_batch);
        }
    };

    Ok(Box::pin(stream))
}

/// Merges batches from multiple sorted sources into a single sorted stream.
/// Input sources must be sorted by primary key and have the same schema.
pub(crate) async fn merge_plain_by_reader(
    metadata: &RegionMetadata,
    sources: Vec<PlainSource>,
    mapper: &PlainProjectionMapper,
) -> Result<BoxedPlainBatchStream> {
    let sort_indices = Arc::new(SortIndices::new(metadata, mapper));
    let sources = sources.into_iter().map(PlainSource::into_source).collect();
    let mut merge_reader = PlainMergeReader::new(sources, sort_indices).await?;

    let stream = try_stream! {
        while let Some(batch) = merge_reader.next_batch().await? {
            yield batch;
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
pub(crate) fn sort_expressions(
    metadata: &RegionMetadata,
    mapper: Option<&PlainProjectionMapper>,
) -> LexOrdering {
    // TODO(yingwen): Error handling.
    // TODO(yingwen): Return time index column id from metadata.
    let time_index_pos = match mapper {
        Some(mapper) => mapper
            .projected_index_by_id(metadata.time_index_column().column_id)
            .unwrap(),
        None => metadata.time_index_column_pos(),
    };
    let time_index_expr = create_sort_expr(
        &metadata.time_index_column().column_schema.name,
        time_index_pos,
        false,
    );
    let sequence_index = match mapper {
        Some(mapper) => mapper.batch_schema().len(),
        None => metadata.column_metadatas.len(),
    };
    let sequence_expr = create_sort_expr(SEQUENCE_COLUMN_NAME, sequence_index, true);

    common_telemetry::info!(
        "Sort exprs, time_index_pos: {}, sequence_index: {}",
        time_index_pos,
        sequence_index
    );

    let exprs = metadata
        .primary_key
        .iter()
        .map(|id| {
            // Safety: We know the primary key exists in the metadata
            let index = match mapper {
                Some(mapper) => mapper.projected_index_by_id(*id).unwrap(),
                None => metadata.column_index_by_id(*id).unwrap(),
            };
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

struct SortIndices {
    pk_indices: Vec<usize>,
    timestamp_index: usize,
}

impl SortIndices {
    fn new(metadata: &RegionMetadata, mapper: &PlainProjectionMapper) -> Self {
        let pk_indices = metadata
            .primary_key
            .iter()
            .map(|id| mapper.projected_index_by_id(*id).unwrap())
            .collect();
        let timestamp_index = mapper
            .projected_index_by_id(metadata.time_index_column().column_id)
            .unwrap();

        Self {
            pk_indices,
            timestamp_index,
        }
    }

    /// Compares primary keys.
    fn compare_primary_key(
        &self,
        left: &PlainBatch,
        left_row_idx: usize,
        right: &PlainBatch,
        right_row_idx: usize,
    ) -> Ordering {
        for &idx in &self.pk_indices {
            let left_array = left.column(idx);
            let right_array = right.column(idx);

            // Safety: We can ensure key columns have ordering.
            let comparator = make_comparator(
                left_array.as_ref(),
                right_array.as_ref(),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            )
            .unwrap();

            // Compare values at the specified row indices
            match comparator(left_row_idx, right_row_idx) {
                Ordering::Equal => {
                    // Keys match so far, continue checking other key columns
                    continue;
                }
                other => return other,
            }
        }

        Ordering::Equal
    }

    /// Compare timestamps of two batches.
    fn compare_timestamp(
        &self,
        left: &PlainBatch,
        left_row_idx: usize,
        right: &PlainBatch,
        right_row_idx: usize,
    ) -> Ordering {
        let left_slice = timestamp_array_to_i64_slice(left.column(self.timestamp_index));
        let right_slice = timestamp_array_to_i64_slice(right.column(self.timestamp_index));

        left_slice[left_row_idx].cmp(&right_slice[right_row_idx])
    }

    /// Compare sequences of two batches.
    fn compare_sequence(
        &self,
        left: &PlainBatch,
        left_row_idx: usize,
        right: &PlainBatch,
        right_row_idx: usize,
    ) -> Ordering {
        let left_slice = Self::sequence_array_to_u64_slice(left.column(left.num_rows() - 2));
        let right_slice = Self::sequence_array_to_u64_slice(right.column(right.num_rows() - 2));

        left_slice[left_row_idx].cmp(&right_slice[right_row_idx])
    }

    /// Get timestamp value of the batch.
    fn timestamp_value(&self, batch: &PlainBatch, index: usize) -> Option<i64> {
        let values = timestamp_array_to_i64_slice(batch.column(self.timestamp_index));
        values.get(index).copied()
    }

    /// Get timestamp values of the batch.
    fn timestamp_values<'a>(&self, batch: &'a PlainBatch) -> &'a [i64] {
        timestamp_array_to_i64_slice(batch.column(self.timestamp_index))
    }

    fn sequence_array_to_u64_slice(array: &ArrayRef) -> &[u64] {
        array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
    }
}

/// Reader to merge sorted batches.
///
/// The merge reader merges [Batch]es from multiple sources that yield sorted batches.
/// 1. Batch is ordered by primary key, time index, sequence desc, op type desc (we can
///    ignore op type as sequence is already unique).
/// 2. Batches from sources **must** not be empty.
///
/// The reader won't concatenate batches. Each batch returned by the reader also doesn't
/// contain duplicate rows. But the last (primary key, timestamp) of a batch may be the same
/// as the first one in the next batch.
pub struct PlainMergeReader {
    /// Holds [Node]s whose key range of current batch **is** overlapped with the merge window.
    /// Each node yields batches from a `source`.
    ///
    /// [Node] in this heap **must** not be empty. A `merge window` is the (primary key, timestamp)
    /// range of the **root node** in the `hot` heap.
    hot: BinaryHeap<Node>,
    /// Holds `Node` whose key range of current batch **isn't** overlapped with the merge window.
    ///
    /// `Node` in this heap **must** not be empty.
    cold: BinaryHeap<Node>,
    /// Batch to output.
    output_batch: Option<PlainBatch>,
    /// Local metrics.
    metrics: Metrics,

    /// Indices of sort key.
    sort_indices: Arc<SortIndices>,
}

impl PlainMergeReader {
    async fn next_batch(&mut self) -> Result<Option<PlainBatch>> {
        let start = Instant::now();
        while !self.hot.is_empty() && self.output_batch.is_none() {
            if self.hot.len() == 1 {
                // No need to do merge sort if only one batch in the hot heap.
                self.fetch_batch_from_hottest().await?;
                self.metrics.num_fetch_by_batches += 1;
            } else {
                // We could only fetch rows that less than the next node from the hottest node.
                self.fetch_rows_from_hottest().await?;
                self.metrics.num_fetch_by_rows += 1;
            }
        }

        if let Some(batch) = self.output_batch.take() {
            self.metrics.scan_cost += start.elapsed();
            self.metrics.num_output_rows += batch.num_rows();
            Ok(Some(batch))
        } else {
            // Nothing fetched.
            self.metrics.scan_cost += start.elapsed();
            Ok(None)
        }
    }
}

impl Drop for PlainMergeReader {
    fn drop(&mut self) {
        common_telemetry::debug!("Plain merge reader finished, metrics: {:?}", self.metrics);

        READ_STAGE_ELAPSED
            .with_label_values(&["merge"])
            .observe(self.metrics.scan_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["merge_fetch"])
            .observe(self.metrics.fetch_cost.as_secs_f64());
    }
}

/// Splits batches in the source by the time index.
fn split_by_timestamp(mut source: Source, timestamp_index: usize) -> Source {
    let stream = try_stream! {
        while let Some(batch) = source.next_plain_batch().await? {
            if batch.num_rows() < 2 {
                yield batch;
                continue;
            }

            let ts_array = batch.column(timestamp_index);
            let timestamps = timestamp_array_to_i64_slice(ts_array);

            let mut offset = 0;
            for next_idx in 1..timestamps.len() {
                if timestamps[next_idx - 1] > timestamps[next_idx] {
                    let length = next_idx - offset;
                    let left = batch.slice(offset, length);
                    yield left;
                    offset = next_idx;
                }
            }
            if offset < timestamps.len() {
                let remain = batch.slice(offset, timestamps.len() - offset);
                yield remain
            }
        }
    };

    Source::PlainStream(Box::pin(stream))
}

impl PlainMergeReader {
    /// Creates and initializes a new [MergeReader].
    pub async fn new(
        sources: Vec<Source>,
        sort_indices: Arc<SortIndices>,
    ) -> Result<PlainMergeReader> {
        let start = Instant::now();
        let mut metrics = Metrics::default();

        let sources: Vec<_> = sources
            .into_iter()
            .map(|source| split_by_timestamp(source, sort_indices.timestamp_index))
            .collect();

        let mut cold = BinaryHeap::with_capacity(sources.len());
        let hot = BinaryHeap::with_capacity(sources.len());
        for source in sources {
            let node = Node::new(source, sort_indices.clone(), &mut metrics).await?;
            if !node.is_eof() {
                // Ensure `cold` don't have eof nodes.
                cold.push(node);
            }
        }

        let mut reader = PlainMergeReader {
            hot,
            cold,
            output_batch: None,
            metrics,
            sort_indices,
        };
        // Initializes the reader.
        reader.refill_hot();

        reader.metrics.scan_cost += start.elapsed();
        Ok(reader)
    }

    /// Moves nodes in `cold` heap, whose key range is overlapped with current merge
    /// window to `hot` heap.
    fn refill_hot(&mut self) {
        while !self.cold.is_empty() {
            if let Some(merge_window) = self.hot.peek() {
                let warmest = self.cold.peek().unwrap();
                if warmest.is_behind(merge_window) {
                    // if the warmest node in the `cold` heap is totally after the
                    // `merge_window`, then no need to add more nodes into the `hot`
                    // heap for merge sorting.
                    break;
                }
            }

            let warmest = self.cold.pop().unwrap();
            self.hot.push(warmest);
        }
    }

    /// Fetches one batch from the hottest node.
    async fn fetch_batch_from_hottest(&mut self) -> Result<()> {
        assert_eq!(1, self.hot.len());

        let mut hottest = self.hot.pop().unwrap();
        let batch = hottest.fetch_batch(&mut self.metrics).await?;
        Self::maybe_output_batch(batch, &mut self.output_batch)?;
        self.reheap(hottest)
    }

    /// Fetches non-duplicated rows from the hottest node.
    async fn fetch_rows_from_hottest(&mut self) -> Result<()> {
        // Safety: `fetch_batches_to_output()` ensures the hot heap has more than 1 element.
        // Pop hottest node.
        let mut top_node = self.hot.pop().unwrap();
        let top = top_node.current_batch();
        // Min timestamp and its sequence in the next batch.
        let next_min_ts = {
            let next_node = self.hot.peek().unwrap();
            let next = next_node.current_batch();
            // top and next have overlapping rows so they must have same primary keys.
            debug_assert_eq!(
                Ordering::Equal,
                self.sort_indices.compare_primary_key(top, 0, next, 0)
            );
            // Safety: Batches in the heap is not empty, so we can use unwrap here.
            self.sort_indices.timestamp_value(next, 0).unwrap()
        };

        // Safety: Batches in the heap is not empty, so we can get values here.
        let timestamps = self.sort_indices.timestamp_values(top);
        // Binary searches the timestamp in the top batch.
        // Safety: Batches should have the same timestamp resolution so we can compare the native
        // value directly.
        let duplicate_pos = match timestamps.binary_search(&next_min_ts) {
            Ok(pos) => pos,
            Err(pos) => {
                // No duplicate timestamp. Outputs timestamp before `pos`.
                Self::maybe_output_batch(top.slice(0, pos), &mut self.output_batch)?;
                top_node.skip_rows(pos, &mut self.metrics).await?;
                return self.reheap(top_node);
            }
        };

        // No need to remove duplicate timestamps.
        let output_end = if duplicate_pos == 0 {
            // If the first timestamp of the top node is duplicate. We can simply return the first row
            // as the heap ensure it is the one with largest sequence.
            1
        } else {
            // We don't know which one has the larger sequence so we use the range before
            // the duplicate pos.
            duplicate_pos
        };
        Self::maybe_output_batch(top.slice(0, output_end), &mut self.output_batch)?;
        top_node.skip_rows(output_end, &mut self.metrics).await?;
        self.reheap(top_node)
    }

    /// Push the node popped from `hot` back to a proper heap.
    fn reheap(&mut self, node: Node) -> Result<()> {
        if node.is_eof() {
            // If the node is EOF, don't put it into the heap again.
            // The merge window would be updated, need to refill the hot heap.
            self.refill_hot();
        } else {
            // Find a proper heap for this node.
            let node_is_cold = if let Some(hottest) = self.hot.peek() {
                // If key range of this node is behind the hottest node's then we can
                // push it to the cold heap. Otherwise we should push it to the hot heap.
                node.is_behind(hottest)
            } else {
                // The hot heap is empty, but we don't known whether the current
                // batch of this node is still the hottest.
                true
            };

            if node_is_cold {
                self.cold.push(node);
            } else {
                self.hot.push(node);
            }
            // Anyway, the merge window has been changed, we need to refill the hot heap.
            self.refill_hot();
        }

        Ok(())
    }

    /// If `filter_deleted` is set to true, removes deleted entries and sets the `batch` to the `output_batch`.
    ///
    /// Ignores the `batch` if it is empty.
    fn maybe_output_batch(batch: PlainBatch, output_batch: &mut Option<PlainBatch>) -> Result<()> {
        debug_assert!(output_batch.is_none());
        if batch.is_empty() {
            return Ok(());
        }
        *output_batch = Some(batch);

        Ok(())
    }
}

/// Metrics for the merge reader.
#[derive(Debug, Default)]
struct Metrics {
    /// Total scan cost of the reader.
    scan_cost: Duration,
    /// Number of times to fetch batches.
    num_fetch_by_batches: usize,
    /// Number of times to fetch rows.
    num_fetch_by_rows: usize,
    /// Number of input rows.
    num_input_rows: usize,
    /// Number of output rows.
    num_output_rows: usize,
    /// Cost to fetch batches from sources.
    fetch_cost: Duration,
}

/// A `Node` represent an individual input data source to be merged.
struct Node {
    /// Data source of this `Node`.
    source: Source,
    /// Current batch to be read. The node ensures the batch is not empty.
    ///
    /// `None` means the `source` has reached EOF.
    current_batch: Option<CompareFirst>,

    sort_indices: Arc<SortIndices>,
}

impl Node {
    /// Initialize a node.
    ///
    /// It tries to fetch one batch from the `source`.
    async fn new(
        mut source: Source,
        sort_indices: Arc<SortIndices>,
        metrics: &mut Metrics,
    ) -> Result<Node> {
        // Ensures batch is not empty.
        let start = Instant::now();
        let current_batch = source
            .next_plain_batch()
            .await?
            .map(|batch| CompareFirst::new(batch, sort_indices.clone()));
        metrics.fetch_cost += start.elapsed();
        metrics.num_input_rows += current_batch
            .as_ref()
            .map(|b| b.batch.num_rows())
            .unwrap_or(0);

        Ok(Node {
            source,
            current_batch,
            sort_indices,
        })
    }

    /// Returns whether the node still has batch to read.
    fn is_eof(&self) -> bool {
        self.current_batch.is_none()
    }

    // /// Returns the primary key of current batch.
    // ///
    // /// # Panics
    // /// Panics if the node has reached EOF.
    // fn primary_key(&self) -> &[u8] {
    //     self.current_batch().primary_key()
    // }

    /// Returns current batch.
    ///
    /// # Panics
    /// Panics if the node has reached EOF.
    fn current_batch(&self) -> &PlainBatch {
        &self.current_batch.as_ref().unwrap().batch
    }

    /// Returns current batch and fetches next batch
    /// from the source.
    ///
    /// # Panics
    /// Panics if the node has reached EOF.
    async fn fetch_batch(&mut self, metrics: &mut Metrics) -> Result<PlainBatch> {
        let current = self.current_batch.take().unwrap();
        let start = Instant::now();
        // Ensures batch is not empty.
        self.current_batch = self
            .source
            .next_plain_batch()
            .await?
            .map(|batch| CompareFirst::new(batch, self.sort_indices.clone()));
        metrics.fetch_cost += start.elapsed();
        metrics.num_input_rows += self
            .current_batch
            .as_ref()
            .map(|b| b.batch.num_rows())
            .unwrap_or(0);
        Ok(current.batch)
    }

    /// Returns true if the key range of current batch in `self` is behind (exclusive) current
    /// batch in `other`.
    ///
    /// # Panics
    /// Panics if either `self` or `other` is EOF.
    fn is_behind(&self, other: &Node) -> bool {
        debug_assert!(!self.current_batch().is_empty());
        debug_assert!(!other.current_batch().is_empty());

        // We only compare pk and timestamp so nodes in the cold
        // heap don't have overlapping timestamps with the hottest node
        // in the hot heap.
        self.sort_indices
            .compare_primary_key(
                self.current_batch(),
                0,
                other.current_batch(),
                other.current_batch().num_rows() - 1,
            )
            .then_with(|| {
                self.sort_indices.compare_timestamp(
                    self.current_batch(),
                    0,
                    other.current_batch(),
                    other.current_batch().num_rows() - 1,
                )
            })
            == Ordering::Greater
    }

    /// Skips first `num_to_skip` rows from node's current batch. If current batch is empty it fetches
    /// next batch from the node.
    ///
    /// # Panics
    /// Panics if the node is EOF.
    async fn skip_rows(&mut self, num_to_skip: usize, metrics: &mut Metrics) -> Result<()> {
        let batch = self.current_batch();
        debug_assert!(batch.num_rows() >= num_to_skip);

        let remaining = batch.num_rows() - num_to_skip;
        if remaining == 0 {
            // Nothing remains, we need to fetch next batch to ensure the batch is not empty.
            self.fetch_batch(metrics).await?;
        } else {
            debug_assert!(!batch.is_empty());
            self.current_batch = Some(CompareFirst::new(
                batch.slice(num_to_skip, remaining),
                self.sort_indices.clone(),
            ));
        }

        Ok(())
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.current_batch == other.current_batch
    }
}

impl Eq for Node {}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Node) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Node) -> Ordering {
        // The std binary heap is a max heap, but we want the nodes are ordered in
        // ascend order, so we compare the nodes in reverse order.
        other.current_batch.cmp(&self.current_batch)
    }
}

/// Type to compare [PlainBatch] by first row.
///
/// It ignores op type as sequence is enough to distinguish different rows.
struct CompareFirst {
    batch: PlainBatch,
    indices: Arc<SortIndices>,
}

impl CompareFirst {
    fn new(batch: PlainBatch, indices: Arc<SortIndices>) -> Self {
        Self { batch, indices }
    }
}

impl PartialEq for CompareFirst {
    fn eq(&self, other: &Self) -> bool {
        self.indices
            .compare_primary_key(&self.batch, 0, &other.batch, 0)
            == Ordering::Equal
            && self
                .indices
                .compare_timestamp(&self.batch, 0, &other.batch, 0)
                == Ordering::Equal
            && self
                .indices
                .compare_sequence(&self.batch, 0, &other.batch, 0)
                == Ordering::Equal
    }
}

impl Eq for CompareFirst {}

impl PartialOrd for CompareFirst {
    fn partial_cmp(&self, other: &CompareFirst) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompareFirst {
    /// Compares by primary key, time index, sequence desc.
    fn cmp(&self, other: &CompareFirst) -> Ordering {
        self.indices
            .compare_primary_key(&self.batch, 0, &other.batch, 0)
            .then_with(|| {
                self.indices
                    .compare_timestamp(&self.batch, 0, &other.batch, 0)
            })
            .then_with(|| {
                self.indices
                    .compare_sequence(&other.batch, 0, &self.batch, 0)
            })
    }
}
