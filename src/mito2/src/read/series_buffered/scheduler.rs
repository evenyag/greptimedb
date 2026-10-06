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

//! Independently admitted source reads and complete range merges.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use async_stream::try_stream;
use datafusion::execution::memory_pool::MemoryPool;
use datatypes::arrow::array::Array;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt, TryStreamExt};
use store_api::region_engine::PartitionRange;
use tokio::sync::Semaphore;

use crate::error::Result;
use crate::read::BoxedRecordBatchStream;
use crate::read::flat_merge::FlatMergeReader;
use crate::read::pruner::PartitionPruner;
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::{
    PartitionMetrics, SplitRecordBatchStream, should_split_flat_batches_for_merge,
};
use crate::read::seq_scan::SeqScan;
use crate::read::series_buffered::{
    Options, PreparedRange, SourcePolicy, admission, nanos, validate_batch,
};
use crate::read::series_compact::CompactReadContext;
use crate::read::series_reader::{
    AssignedSeriesBatch, BufferedSource, MetricSeriesFilter, SeriesReader, buffered_sources,
};
use crate::read::series_result::budget::BudgetPool;
use crate::read::series_result::resources::{Charge, Kind};
use crate::read::series_result::{
    Placement, ResultBuilder, ResultHandle, StoreResources, checked_add, checked_mul, fail,
};
use crate::sst::file::FileTimeRange;

#[derive(Clone)]
pub(crate) struct Preparation {
    pub(crate) ctx: Arc<StreamContext>,
    pub(crate) compact: Arc<CompactReadContext>,
    pub(crate) partition_pruner: Arc<PartitionPruner>,
    pub(crate) metrics: PartitionMetrics,
    pub(crate) resources: Arc<StoreResources>,
    pub(crate) options: Options,
    pub(crate) cache: Option<Arc<super::cache::BufferedDataCache>>,
}

/// Closing returns unused credit; in-flight blocking work keeps actual charges.
struct TaskBudget(Arc<BudgetPool>);

impl Drop for TaskBudget {
    fn drop(&mut self) {
        self.0.close();
    }
}

struct SourceJob {
    source: usize,
    part: usize,
    read: BufferedSource,
}

struct RangeJob {
    cache_key: Option<super::cache::Key>,
    bounds: FileTimeRange,
    pending: VecDeque<SourceJob>,
    parts: Vec<Vec<Option<ResultHandle>>>,
    remaining: usize,
    merging: bool,
}

enum Completed {
    Source {
        range: usize,
        source: usize,
        part: usize,
        result: ResultHandle,
    },
    Range {
        partition: usize,
        bounds: FileTimeRange,
        result: ResultHandle,
    },
}

type Tasks = FuturesUnordered<BoxFuture<'static, Result<Completed>>>;

impl Preparation {
    pub(crate) async fn run(
        self,
        ranges: &[PartitionRange],
        chunks: &[Vec<AssignedSeriesBatch>],
        partitions: usize,
    ) -> Result<Vec<Vec<PreparedRange>>> {
        self.resources.count("candidate_chunks", chunks.len());
        if chunks.is_empty() {
            return Ok((0..partitions).map(|_| Vec::new()).collect());
        }
        let reclamation = self.budget(self.writer_bytes()?, "preparation spill")?;
        let pool: Arc<dyn MemoryPool> = reclamation.0.clone();
        match self.options.source_policy {
            SourcePolicy::SelectedSeriesPerPartition => {
                self.selected(ranges, chunks, partitions, pool).await
            }
            SourcePolicy::SharedSelectedSeries => {
                self.shared(ranges, chunks, partitions, pool).await
            }
        }
    }

    fn cache_key(
        &self,
        range: &PartitionRange,
        ids: &[crate::series_index::MetricSeriesId],
    ) -> Option<super::cache::Key> {
        self.cache.as_ref()?;
        let key = super::cache::Key::new(&self.ctx, range, ids, &self.resources.options);
        if key.is_none() {
            self.resources.count("buffered_cache_bypasses", 1);
        }
        key
    }

    fn cached(&self, key: Option<&super::cache::Key>) -> Result<Option<ResultHandle>> {
        match (&self.cache, key) {
            (Some(cache), Some(key)) => cache.get(key, &self.compact.catalog, &self.resources),
            _ => Ok(None),
        }
    }

    async fn cache_result(
        &self,
        key: Option<super::cache::Key>,
        result: ResultHandle,
        pool: Arc<dyn MemoryPool>,
    ) -> ResultHandle {
        if let (Some(cache), Some(key)) = (&self.cache, key)
            && let Some(cached) = cache
                .admit(
                    key,
                    &result,
                    &self.compact.catalog,
                    self.resources.clone(),
                    pool,
                )
                .await
        {
            return cached;
        }
        result
    }

    fn writer_bytes(&self) -> Result<usize> {
        // Protect dictionary serialization and footer finalization before a task
        // starts, including configurations with larger metadata limits.
        let dictionary = self
            .resources
            .fixed_keys()
            .map_or(0, |keys| keys.get_array_memory_size());
        checked_add(
            checked_mul(self.options.batch_bytes, 24)?,
            checked_add(
                checked_mul(self.options.file_metadata_bytes, 4)?,
                checked_mul(dictionary, 8)?,
            )?,
        )
    }

    fn budget(&self, bytes: usize, stage: &'static str) -> Result<TaskBudget> {
        self.resources
            .peak(&format!("{stage}_required_bytes"), bytes);
        BudgetPool::prepaid(&self.resources.pool(), bytes, stage)
            .map(TaskBudget)
            .map_err(|_| admission(&self.resources, stage, bytes))
    }

    fn reader_bytes(&self, range: PartitionRange) -> Result<usize> {
        self.ctx.ranges[range.identifier]
            .row_group_indices
            .iter()
            .try_fold(0, |sum, index| {
                let bytes = if self.ctx.is_file_range_index(*index) {
                    self.compact
                        .files(*index)?
                        .iter()
                        .map(|(file, _)| file.buffered_reader_bytes())
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .max()
                        .unwrap_or(0)
                } else {
                    checked_add(
                        self.ctx.input.memtables[index.index]
                            .stats()
                            .bytes_allocated(),
                        checked_mul(self.options.batch_bytes, 4)?,
                    )?
                };
                checked_add(sum, bytes)
            })
    }

    async fn selected(
        self,
        ranges: &[PartitionRange],
        chunks: &[Vec<AssignedSeriesBatch>],
        partitions: usize,
        reclamation: Arc<dyn MemoryPool>,
    ) -> Result<Vec<Vec<PreparedRange>>> {
        let mut results = (0..partitions).map(|_| Vec::new()).collect::<Vec<_>>();
        let mut metadata = self.resources.reserve(Kind::Metadata, 0)?;
        let mut tasks = Tasks::new();
        let mut jobs = VecDeque::new();
        for range in ranges {
            let readers = self.reader_bytes(*range)?;
            for chunk in chunks {
                for (p, assignment) in chunk.iter().enumerate() {
                    if !assignment.series().is_empty() {
                        let key = self.cache_key(range, assignment.series());
                        if let Some(key) = &key {
                            metadata.grow(checked_mul(key.bytes()?, 2)?)?;
                        }
                        if let Some(result) = self.cached(key.as_ref())? {
                            Self::accept_range(
                                Completed::Range {
                                    partition: p,
                                    bounds: self.ctx.ranges[range.identifier].time_range,
                                    result,
                                },
                                &mut results,
                                &mut metadata,
                            )?;
                            continue;
                        }
                        let bytes =
                            checked_add(readers, checked_mul(assignment.series().len(), 128)?)?;
                        metadata.grow(128)?;
                        jobs.push_back((*range, p, assignment, bytes, key));
                    }
                }
            }
        }
        while !jobs.is_empty() || !tasks.is_empty() {
            while tasks.len() < self.options.preparation_concurrency {
                let Some((range, partition, assignment, readers, key)) = jobs.front() else {
                    break;
                };
                let required = checked_add(*readers, self.writer_bytes()?)?;
                if !self
                    .make_room(&mut results, &mut [], required, &reclamation)
                    .await?
                {
                    if tasks.is_empty() {
                        return self.rejected("range preparation", required);
                    }
                    self.resources.count("range_admission_deferrals", 1);
                    break;
                }
                let budget = self.budget(required, "range preparation")?;
                let range = *range;
                let partition = *partition;
                let assignment = (*assignment).clone();
                let readers = *readers;
                let key = key.clone();
                jobs.pop_front();
                let preparation = self.clone();
                tasks.push(
                    async move {
                        let started = Instant::now();
                        let bounds = preparation.ctx.ranges[range.identifier].time_range;
                        let reader = SeriesReader::try_new(
                            preparation.ctx.clone(),
                            vec![range],
                            assignment,
                            preparation.partition_pruner.clone(),
                            Arc::new(Semaphore::new(1)),
                            preparation.metrics.clone(),
                        )?
                        .with_compact(Some(preparation.compact.clone()));
                        let pool: Arc<dyn MemoryPool> = budget.0.clone();
                        let charge = Arc::new(
                            preparation
                                .resources
                                .reserve_in(Kind::Workspace, readers, &pool)?
                                .with_owner("range_reader"),
                        );
                        preparation
                            .resources
                            .peak("range_reader_estimate_bytes", readers);
                        let stream = reader.build_complete_range(range).await?;
                        let result = preparation
                            .materialize(stream, bounds, pool.clone(), charge, false)
                            .await?;
                        drop(reader);
                        let result = preparation.cache_result(key, result, pool).await;
                        preparation
                            .resources
                            .count("range_preparation_ns", nanos(started));
                        preparation
                            .resources
                            .count("completed_range_sources_released", 1);
                        drop(budget);
                        Ok(Completed::Range {
                            partition,
                            bounds,
                            result,
                        })
                    }
                    .boxed(),
                );
                self.resources.peak("active_range_tasks", tasks.len());
            }
            if let Some(done) = tasks.next().await {
                Self::accept_range(done?, &mut results, &mut metadata)?;
            }
        }
        Ok(results)
    }

    async fn shared(
        self,
        ranges: &[PartitionRange],
        chunks: &[Vec<AssignedSeriesBatch>],
        partitions: usize,
        reclamation: Arc<dyn MemoryPool>,
    ) -> Result<Vec<Vec<PreparedRange>>> {
        let mut results = (0..partitions).map(|_| Vec::new()).collect::<Vec<_>>();
        let mut metadata = self.resources.reserve(Kind::Metadata, 0)?;
        let mut jobs = Vec::new();
        let mut seen = BTreeSet::new();
        // Union construction is chunk-local; the existing collector bounds the
        // selection unit, and retained candidates are still query-accounted.
        for (chunk_index, chunk) in chunks.iter().enumerate() {
            let count = chunk.iter().map(|a| a.series().len()).sum::<usize>();
            metadata.grow(checked_mul(count, 128)?)?;
            let mut ids = Vec::with_capacity(count);
            for assignment in chunk {
                ids.extend_from_slice(assignment.series());
            }
            let filter = MetricSeriesFilter::union(ids.clone());
            for range in ranges {
                let cache_key = self.cache_key(range, &ids);
                if let Some(key) = &cache_key {
                    metadata.grow(checked_mul(key.bytes()?, 2)?)?;
                }
                if let Some(result) = self.cached(cache_key.as_ref())? {
                    Self::accept_range(
                        Completed::Range {
                            partition: 0,
                            bounds: self.ctx.ranges[range.identifier].time_range,
                            result,
                        },
                        &mut results,
                        &mut metadata,
                    )?;
                    continue;
                }
                let sources = buffered_sources(
                    self.ctx.clone(),
                    *range,
                    self.compact.clone(),
                    filter.clone(),
                    self.metrics.clone(),
                )?;
                let count = sources.iter().map(Vec::len).sum::<usize>();
                self.resources.count("source_parts_planned", count);
                if chunk_index > 0 {
                    self.resources.count("source_parts_in_later_chunks", count);
                }
                metadata.grow(checked_add(
                    checked_mul(count, 512)?,
                    checked_mul(sources.len(), 64)?,
                )?)?;
                let mut pending = VecDeque::new();
                let mut parts = Vec::new();
                for (source, reads) in sources.into_iter().enumerate() {
                    parts.push((0..reads.len()).map(|_| None).collect());
                    for (part, read) in reads.into_iter().enumerate() {
                        if !seen.insert((range.identifier, source, read.ordinal)) {
                            self.resources.count("source_rereads_across_chunks", 1);
                        } else {
                            metadata.grow(64)?;
                        }
                        pending.push_back(SourceJob { source, part, read });
                    }
                }
                jobs.push(RangeJob {
                    cache_key,
                    bounds: self.ctx.ranges[range.identifier].time_range,
                    pending,
                    parts,
                    remaining: count,
                    merging: false,
                });
            }
        }
        let mut reads = Tasks::new();
        let mut merges = Tasks::new();
        loop {
            // Merge-ready ranges take priority so completed source storage can retire.
            while merges.len() < self.options.preparation_concurrency {
                let Some(index) = jobs.iter().position(|j| j.remaining == 0 && !j.merging) else {
                    break;
                };
                let required = loop {
                    let inputs = merge_bytes(&jobs[index].parts, self.options.batch_rows)?;
                    let required = checked_add(inputs, self.writer_bytes()?)?;
                    if !self
                        .make_room(&mut results, &mut jobs, required, &reclamation)
                        .await?
                    {
                        break None;
                    }
                    let after = checked_add(
                        merge_bytes(&jobs[index].parts, self.options.batch_rows)?,
                        self.writer_bytes()?,
                    )?;
                    if self.resources.available() >= after {
                        break Some(after);
                    }
                };
                let Some(required) = required else {
                    let required = checked_add(
                        merge_bytes(&jobs[index].parts, self.options.batch_rows)?,
                        self.writer_bytes()?,
                    )?;
                    if reads.is_empty() && merges.is_empty() {
                        return self.rejected("range merge", required);
                    }
                    self.resources.count("merge_admission_deferrals", 1);
                    break;
                };
                let budget = self.budget(required, "range merge")?;
                let parts = std::mem::take(&mut jobs[index].parts);
                let bounds = jobs[index].bounds;
                let cache_key = jobs[index].cache_key.take();
                let split = self
                    .ctx
                    .ranges
                    .iter()
                    .find(|range| range.time_range == bounds)
                    .is_some_and(|range| {
                        should_split_flat_batches_for_merge(&self.ctx, range).is_some()
                    });
                jobs[index].merging = true;
                let preparation = self.clone();
                merges.push(async move {
                    let started = Instant::now();
                    let pool: Arc<dyn MemoryPool> = budget.0.clone();
                    let mut sources = Vec::new();
                    for parts in parts {
                        if parts.is_empty() { continue; }
                        let pool = pool.clone();
                        sources.push(Box::pin(try_stream! {
                            for handle in parts {
                                let handle = handle.ok_or_else(|| fail("unfinished source buffer"))?;
                                let mut stream = handle.source_stream_in(pool.clone());
                                while let Some(batch) = stream.try_next().await? { yield batch; }
                            }
                        }) as BoxedRecordBatchStream);
                    }
                    if split {
                        sources = sources.into_iter().map(|stream| Box::pin(SplitRecordBatchStream::new(stream)) as BoxedRecordBatchStream).collect();
                    }
                    preparation.resources.peak("merge_inputs", sources.len());
                    // Decoded cursor payload is charged inside the prepaid pool. Fund
                    // the merge's output/head overlap separately from those leases.
                    let charge = Arc::new(preparation.resources.reserve_in(Kind::Workspace,
                        checked_mul(preparation.options.batch_bytes, 4)?, &pool)?);
                        let merge = FlatMergeReader::new(
                            preparation.compact.schema.schema.clone(), sources,
                            preparation.options.batch_rows,
                            Some(preparation.metrics.merge_metrics_reporter()),
                        ).await?;
                        let stream = SeqScan::finish_flat_reader(
                            &preparation.ctx, Box::pin(merge.into_stream()), Some(&preparation.metrics), false, 0,
                        )?;
                    let result = preparation.materialize(stream, bounds, pool.clone(), charge, false).await?;
                    let result = preparation.cache_result(cache_key, result, pool).await;
                    preparation.resources.count("range_merge_ns", nanos(started));
                    preparation.resources.count("completed_range_sources_released", 1);
                    drop(budget);
                    Ok(Completed::Range { partition: 0, bounds, result })
                }.boxed());
                self.resources.peak("active_merge_tasks", merges.len());
            }
            while reads.len() < self.options.preparation_concurrency {
                let Some(index) = jobs.iter().position(|j| !j.pending.is_empty()) else {
                    break;
                };
                let readers = jobs[index]
                    .pending
                    .front()
                    .ok_or_else(|| fail("missing source job"))?
                    .read
                    .reader_bytes;
                let required = checked_add(readers, self.writer_bytes()?)?;
                if !self
                    .make_room(&mut results, &mut jobs, required, &reclamation)
                    .await?
                {
                    if reads.is_empty() && merges.is_empty() {
                        return self.rejected("source preparation", required);
                    }
                    self.resources.count("source_admission_deferrals", 1);
                    break;
                }
                let budget = self.budget(required, "source preparation")?;
                let source = jobs[index]
                    .pending
                    .pop_front()
                    .ok_or_else(|| fail("missing source job"))?;
                let bounds = jobs[index].bounds;
                let preparation = self.clone();
                reads.push(
                    async move {
                        let pool: Arc<dyn MemoryPool> = budget.0.clone();
                        let charge = Arc::new(
                            preparation
                                .resources
                                .reserve_in(Kind::Workspace, readers, &pool)?
                                .with_owner("source_reader"),
                        );
                        preparation
                            .resources
                            .peak("source_reader_estimate_bytes", readers);
                        let result = preparation
                            .materialize(source.read.stream, bounds, pool, charge, true)
                            .await?;
                        preparation.resources.count("source_parts_completed", 1);
                        drop(budget);
                        Ok(Completed::Source {
                            range: index,
                            source: source.source,
                            part: source.part,
                            result,
                        })
                    }
                    .boxed(),
                );
                self.resources.peak("active_source_tasks", reads.len());
            }
            if reads.is_empty() && merges.is_empty() {
                break;
            }
            let done = tokio::select! {
                Some(done) = reads.next(), if !reads.is_empty() => done?,
                Some(done) = merges.next(), if !merges.is_empty() => done?,
            };
            match done {
                Completed::Source {
                    range,
                    source,
                    part,
                    result,
                } => {
                    let job = &mut jobs[range];
                    if job.parts[source][part].replace(result).is_some() {
                        return Err(fail("duplicate source completion"));
                    }
                    job.remaining -= 1;
                }
                range => Self::accept_range(range, &mut results, &mut metadata)?,
            }
        }
        if jobs.iter().any(|job| !job.merging) {
            return Err(fail("incomplete shared preparation"));
        }
        Ok(results)
    }

    fn accept_range(
        done: Completed,
        results: &mut [Vec<PreparedRange>],
        metadata: &mut Charge,
    ) -> Result<()> {
        let Completed::Range {
            partition,
            bounds,
            result,
        } = done
        else {
            return Err(fail("unexpected source completion"));
        };
        metadata.grow(std::mem::size_of::<PreparedRange>())?;
        results[partition].push(PreparedRange {
            bounds,
            result: Some(result),
        });
        Ok(())
    }

    fn rejected<T>(&self, stage: &str, required: usize) -> Result<T> {
        self.resources.count("admission_failures", 1);
        Err(admission(&self.resources, stage, required))
    }

    /// Only coordinator-owned completed buffers can migrate. Active merges own
    /// their handles, so reclamation cannot invalidate or wait on a consumer.
    async fn make_room(
        &self,
        results: &mut [Vec<PreparedRange>],
        jobs: &mut [RangeJob],
        required: usize,
        reclamation: &Arc<dyn MemoryPool>,
    ) -> Result<bool> {
        if self.options.retention == crate::config::buffered_scan::Retention::Resident {
            return Ok(self.resources.available() >= required);
        }
        for ranges in results {
            for range in ranges {
                if self.resources.available() >= required {
                    return Ok(true);
                }
                if range
                    .result
                    .as_ref()
                    .is_some_and(ResultHandle::has_resident)
                {
                    let handle = range.result.take().ok_or_else(|| fail("missing result"))?;
                    range.result = Some(handle.spill_in(reclamation.clone()).await?);
                }
            }
        }
        for job in jobs {
            for source in &mut job.parts {
                for part in source {
                    if self.resources.available() >= required {
                        return Ok(true);
                    }
                    if part.as_ref().is_some_and(ResultHandle::has_resident) {
                        let handle = part.take().ok_or_else(|| fail("missing source"))?;
                        *part = Some(handle.spill_in(reclamation.clone()).await?);
                    }
                }
            }
        }
        Ok(self.resources.available() >= required)
    }

    async fn materialize(
        &self,
        mut stream: BoxedRecordBatchStream,
        bounds: FileTimeRange,
        pool: Arc<dyn MemoryPool>,
        charge: Arc<Charge>,
        source: bool,
    ) -> Result<ResultHandle> {
        let start = Instant::now();
        let mut builder = ResultBuilder::with_workspace(
            self.resources.clone(),
            self.compact.schema.schema.clone(),
            pool,
        )?
        .with_phase(if source { "source" } else { "result" })
        .with_spill_on_pressure(
            self.options.retention != crate::config::buffered_scan::Retention::Resident,
        );
        while let Some(batch) = stream.try_next().await? {
            validate_batch(&batch, bounds)?;
            self.resources.count(
                if source {
                    "source_selected_rows"
                } else {
                    "range_result_rows"
                },
                batch.num_rows(),
            );
            self.resources.peak(
                if source {
                    "source_batch_bytes"
                } else {
                    "merge_batch_bytes"
                },
                crate::read::series_result::unique_batch_bytes(&batch)?,
            );
            let placement = if self.options.retention
                == crate::config::buffered_scan::Retention::ForcedSpill
                || (self.options.retention == crate::config::buffered_scan::Retention::Threshold
                    && (self.resources.pool().reserved() >= self.options.spill_threshold
                        || self.resources.available() < checked_mul(self.writer_bytes()?, 2)?))
            {
                builder = builder.spill_resident().await?;
                Placement::File
            } else {
                Placement::Resident
            };
            let append = Instant::now();
            builder = builder
                .append_prepared(batch, placement, charge.clone())
                .await?;
            self.resources.count(
                if source {
                    "source_append_ns"
                } else {
                    "result_append_ns"
                },
                nanos(append),
            );
            if placement == Placement::File {
                self.resources.count(
                    if source {
                        "source_spill_append_ns"
                    } else {
                        "spill_append_ns"
                    },
                    nanos(append),
                );
            }
        }
        drop(stream);
        drop(charge);
        let finish = Instant::now();
        let result = builder.finish().await?;
        self.resources.count(
            if source {
                "source_finalization_ns"
            } else {
                "spill_finalization_ns"
            },
            nanos(finish),
        );
        self.resources.count(
            if source {
                "source_preparation_ns"
            } else {
                "range_materialization_ns"
            },
            nanos(start),
        );
        Ok(result)
    }
}

fn merge_bytes(parts: &[Vec<Option<ResultHandle>>], batch_rows: usize) -> Result<usize> {
    parts.iter().try_fold(0, |sum, source| {
        let mut head = 0;
        let mut window = 0;
        for part in source {
            let handle = part
                .as_ref()
                .ok_or_else(|| fail("unfinished merge source"))?;
            head = head.max(handle.source_replay_bytes()?);
            window = checked_add(window, handle.merge_window_bytes(batch_rows)?)?;
        }
        // Two original heads may coexist during advancement. Summing each
        // part's window bound also covers windows crossing many tiny row groups.
        // Fund interleave/dedup and output overlap in addition to cursor payload.
        checked_add(
            sum,
            checked_add(checked_mul(head, 2)?, checked_mul(window, 4)?)?,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::series_result::tests::fixture;
    use crate::read::series_result::{Layout, StoreOptions};
    use datatypes::arrow::array::TimestampMillisecondArray;
    use datatypes::arrow::datatypes::Schema;
    use datatypes::arrow::record_batch::RecordBatch;

    #[tokio::test]
    async fn buffered_large_fan_in_uses_bytes_and_finalized_sources() {
        for layout in [Layout::OneSeries, Layout::MultipleSeries] {
            let dir = common_test_util::temp_dir::create_temp_dir("buffered-159-inputs");
            let template = fixture(&[(1, 1, 8), (2, 1, 8)]);
            let mut columns = template.columns().to_vec();
            columns[2] = Arc::new(TimestampMillisecondArray::from_iter_values(
                (0..8).chain(0..8),
            ));
            let mut fields = template.schema().fields().to_vec();
            fields[2] = Arc::new(
                fields[2]
                    .as_ref()
                    .clone()
                    .with_data_type(columns[2].data_type().clone()),
            );
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            let resources = StoreResources::new(
                dir.path(),
                StoreOptions {
                    layout,
                    batch_rows: 4,
                    batch_bytes: 8192,
                    memory_bytes: 64 * 1024 * 1024,
                    metadata_bytes: 8 * 1024 * 1024,
                    ..Default::default()
                },
            )
            .unwrap();
            let mut parts = Vec::new();
            for index in 0..159 {
                let placement = if index % 2 == 0 {
                    Placement::File
                } else {
                    Placement::Resident
                };
                let result = ResultBuilder::with_resources(resources.clone(), batch.schema())
                    .unwrap()
                    .with_phase("source")
                    .append(batch.clone(), placement)
                    .await
                    .unwrap()
                    .finish()
                    .await
                    .unwrap();
                parts.push(vec![Some(result)]);
            }
            let required = merge_bytes(&parts, 4).unwrap();
            assert!(
                required < resources.available(),
                "merge requires {required}, available {}",
                resources.available()
            );
            let budget = TaskBudget(
                BudgetPool::prepaid(&resources.pool(), required, "range merge").unwrap(),
            );
            let pool: Arc<dyn MemoryPool> = budget.0.clone();
            let writes = resources.snapshot().counts["filesystem_write_bytes"];
            let sources = parts
                .into_iter()
                .map(|mut part| {
                    let handle = part.pop().unwrap().unwrap();
                    let pool = pool.clone();
                    Box::pin(try_stream! {
                        let mut source = handle.source_stream_in(pool);
                        while let Some(batch) = source.try_next().await? { yield batch; }
                    }) as BoxedRecordBatchStream
                })
                .collect();
            let merge = FlatMergeReader::new(batch.schema(), sources, 4, None)
                .await
                .unwrap();
            let mut stream = Box::pin(merge.into_stream());
            let mut rows = 0;
            while let Some(batch) = stream.try_next().await.unwrap() {
                rows += batch.num_rows();
            }
            assert_eq!(159 * 16, rows);
            drop(stream);
            drop(budget);
            resources.drain_cleanup().await.unwrap();
            let snapshot = resources.snapshot();
            assert_eq!(writes, snapshot.counts["filesystem_write_bytes"]);
            assert_eq!(0, snapshot.workspace_bytes);
            assert_eq!(0, snapshot.payload_bytes);
            assert_eq!(0, snapshot.disk_bytes);
            assert!(snapshot.ownership.values().all(|bytes| *bytes == 0));
            // A byte request larger than the same query budget fails even though
            // it could represent just one wide input; there is no count gate.
            assert!(
                BudgetPool::prepaid(&resources.pool(), 65 * 1024 * 1024, "range merge").is_err()
            );
        }
    }

    #[tokio::test]
    async fn buffered_writer_credit_is_shared_and_survives_escaped_allocations() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer};
        let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1000));
        let query: Arc<dyn MemoryPool> = BudgetPool::query(&parent, 800);
        let source = TaskBudget(BudgetPool::prepaid(&query, 300, "source preparation").unwrap());
        let merge = TaskBudget(BudgetPool::prepaid(&query, 400, "range merge").unwrap());
        let competitor = MemoryConsumer::new("other partition").register(&query);
        assert!(competitor.try_grow(101).is_err());
        let pool: Arc<dyn MemoryPool> = source.0.clone();
        let writer = MemoryConsumer::new("queued writer").register(&pool);
        writer.try_grow(250).unwrap();
        assert_eq!(700, parent.reserved());
        drop(source);
        assert_eq!(650, parent.reserved());
        drop(merge);
        assert_eq!(250, parent.reserved());
        drop(writer);
        assert_eq!(0, parent.reserved());
    }
}
