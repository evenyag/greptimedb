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

//! Development-only complete preparation and bounded range replay.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_stream::try_stream;
use common_time::Timestamp;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::timestamp::timestamp_array_to_primitive;
use futures::TryStreamExt;
use serde::Deserialize;
use store_api::region_engine::PartitionRange;
use tokio::sync::Semaphore;

use crate::error::Result;
use crate::read::pruner::{Pruner, PrunerOptions};
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::PartitionMetrics;
use crate::read::series_candidate::SeriesCandidateScanner;
use crate::read::series_compact::{CompactMetrics, CompactReadContext, CompactSchema, TagCatalog};
use crate::read::series_prepare::{ReadinessReceiver, start_preparation};
use crate::read::series_reader::{SeriesBatchCollector, SeriesReader};
use crate::read::series_result::budget::BudgetPool;
use crate::read::series_result::resources::Kind;
use crate::read::series_result::{
    Placement, ResultBuilder, ResultHandle, StoreOptions, StoreResources, checked_add, checked_mul,
    fail, pin_batch,
};
use crate::read::stream::{ScanBatch, ScanBatchStream};
use crate::series_index::MetricSeriesId;
use crate::sst::file::FileTimeRange;
use crate::sst::parquet::flat_format::time_index_column_index;

/// Explicit development experiment, never part of serialized MitoConfig.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Options {
    pub(crate) scratch: PathBuf,
    pub(crate) memory_bytes: usize,
    pub(crate) spill_threshold: usize,
    pub(crate) disk_bytes: usize,
    pub(crate) batch_rows: usize,
    pub(crate) batch_bytes: usize,
    pub(crate) layout: String,
    #[serde(default)]
    pub(crate) compression: Option<String>,
}

impl Options {
    pub(crate) fn store(&self) -> Result<StoreOptions> {
        if self.spill_threshold == 0 || self.spill_threshold > self.memory_bytes {
            return Err(fail(
                "spill threshold must be positive and no greater than memory budget",
            ));
        }
        Ok(StoreOptions {
            memory_bytes: self.memory_bytes,
            metadata_bytes: self.memory_bytes,
            disk_bytes: self.disk_bytes,
            batch_rows: self.batch_rows,
            batch_bytes: self.batch_bytes,
            layout: match self.layout.as_str() {
                "one_series" => crate::read::series_result::Layout::OneSeries,
                "multiple_series" => crate::read::series_result::Layout::MultipleSeries,
                _ => return Err(fail("layout must be one_series or multiple_series")),
            },
            compression: match self.compression.as_deref() {
                None | Some("none") => None,
                Some("lz4") => Some(arrow_ipc::CompressionType::LZ4_FRAME),
                Some("zstd") => Some(arrow_ipc::CompressionType::ZSTD),
                _ => return Err(fail("compression must be none, lz4 or zstd")),
            },
            ..StoreOptions::default()
        })
    }
}

/// A fresh process selects its experiment using an external JSON options file.
#[cfg(feature = "dev-tools")]
pub(crate) async fn development_options() -> Result<Option<Options>> {
    let Some(path) = std::env::var_os("GREPTIME_BUFFERED_SERIES_SCAN_OPTIONS") else {
        return Ok(None);
    };
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| fail(format!("development options: {e}")))?;
    let options: Options =
        serde_json::from_slice(&bytes).map_err(|e| fail(format!("development options: {e}")))?;
    options.store()?;
    Ok(Some(options))
}

pub(crate) struct BufferedScan {
    options: Options,
    pub(crate) resources: Arc<StoreResources>,
    receivers: Mutex<Vec<Option<ReadinessReceiver<Manifest>>>>,
}

impl BufferedScan {
    pub(crate) async fn new(options: Options, parent: Arc<dyn MemoryPool>) -> Result<Self> {
        let store = options.store()?;
        let pool: Arc<dyn MemoryPool> = BudgetPool::query(&parent, options.memory_bytes);
        let root = options.scratch.clone();
        let resources = common_runtime::spawn_blocking_query(move || {
            StoreResources::with_pool(&root, store, pool)
        })
        .await
        .map_err(|e| fail(e.to_string()))??;
        Ok(Self {
            options,
            resources,
            receivers: Mutex::new(vec![]),
        })
    }

    pub(crate) fn settings(&self) -> serde_json::Value {
        serde_json::json!({
            "query_memory_budget_bytes": self.options.memory_bytes,
            "spill_threshold_bytes": self.options.spill_threshold,
            "disk_limit_bytes": self.options.disk_bytes,
            "ipc_layout": self.options.layout,
            "ipc_batch_rows": self.options.batch_rows,
            "ipc_batch_bytes": self.options.batch_bytes,
            "compression": self.options.compression,
            "preparation_concurrency": 1,
            "prefetch_batches": 0,
            "source_policy": "selected_series_per_partition",
            "consumer_retention": "one_complete_series_plus_next_batch",
        })
    }

    pub(crate) fn reset(&self) {
        self.receivers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    pub(crate) fn stream(
        &self,
        partition: usize,
        ctx: Arc<StreamContext>,
        partitions: Vec<Vec<PartitionRange>>,
        metrics: PartitionMetrics,
        metrics_set: ExecutionPlanMetricsSet,
    ) -> Result<ScanBatchStream> {
        let mut receivers = self.receivers.lock().unwrap_or_else(|e| e.into_inner());
        if receivers.is_empty() {
            let resources = self.resources.clone();
            let options = self.options.clone();
            let prepare_metrics = metrics.clone();
            let num_partitions = partitions.len();
            // Preparation alone owns retained source builders. The scanner's pruner
            // must not keep prepared file contexts alive through replay.
            let pruner = Arc::new(Pruner::new_with_options(
                ctx.clone(),
                1,
                PrunerOptions {
                    retain_builders: true,
                    enable_predicate_prefilter: false,
                },
            ));
            *receivers = start_preparation(num_partitions, async move {
                prepare(
                    ctx,
                    partitions,
                    pruner,
                    prepare_metrics,
                    metrics_set,
                    resources,
                    options,
                )
                .await
            })
            .into_iter()
            .map(Some)
            .collect();
        }
        let receiver = receivers
            .get_mut(partition)
            .and_then(Option::take)
            .ok_or_else(|| fail(format!("partition {partition} already consumed")))?;
        let resources = self.resources.clone();
        Ok(Box::pin(try_stream! {
            metrics.on_first_poll();
            let ready = Instant::now();
            let manifest = receiver.ready().await?;
            resources.count("readiness_wait_ns", nanos(ready));
            if manifest.partition != partition { Err(fail("wrong readiness partition"))?; }
            let mut replay = replay(manifest.payload);
            let mut scan_metrics = crate::read::ScannerMetrics::default();
            let mut fetch_start = ready;
            while let Some(batch) = replay.try_next().await? {
                scan_metrics.scan_cost += fetch_start.elapsed();
                scan_metrics.num_rows += batch.num_rows();
                scan_metrics.num_batches += 1;
                let yielded = Instant::now();
                yield ScanBatch::RecordBatch(batch);
                scan_metrics.yield_cost += yielded.elapsed();
                fetch_start = Instant::now();
            }
            scan_metrics.scan_cost += fetch_start.elapsed();
            metrics.merge_metrics(&scan_metrics);
            metrics.on_finish();
        }))
    }
}

struct PreparedRange {
    bounds: FileTimeRange,
    result: Option<ResultHandle>,
}

struct Manifest {
    ids: Vec<MetricSeriesId>,
    ranges: Vec<PreparedRange>,
    schema: Arc<CompactSchema>,
    catalog: Arc<TagCatalog>,
    resources: Arc<StoreResources>,
    replay: Arc<BudgetPool>,
    last_row: bool,
    tag_bytes: usize,
    _metadata: crate::read::series_result::resources::Charge,
}

impl Drop for Manifest {
    fn drop(&mut self) {
        self.replay.close();
    }
}

fn nanos(start: Instant) -> usize {
    usize::try_from(start.elapsed().as_nanos()).unwrap_or(usize::MAX)
}

fn admission(resources: &StoreResources, stage: &str, required: usize) -> crate::error::Error {
    crate::error::BufferedScanMemorySnafu {
        stage: stage.to_owned(),
        required,
        available: resources.available(),
        limit: resources.options.memory_bytes,
        reason: "irreducible request after unpublished result reclamation".to_owned(),
    }
    .build()
}

/// Spill only exclusive, unpublished handles, oldest complete range first.
async fn reclaim(
    results: &mut [Vec<PreparedRange>],
    resources: &Arc<StoreResources>,
    required: usize,
    stage: &str,
) -> Result<()> {
    while resources.available() < required {
        let mut victim = None;
        for (p, ranges) in results.iter().enumerate() {
            for (r, range) in ranges.iter().enumerate() {
                if range
                    .result
                    .as_ref()
                    .is_some_and(ResultHandle::has_resident)
                {
                    victim = Some((p, r));
                    break;
                }
            }
            if victim.is_some() {
                break;
            }
        }
        let Some((p, r)) = victim else {
            if stage == "spill threshold" {
                return Ok(());
            }
            return Err(admission(resources, stage, required));
        };
        let result = results[p][r]
            .result
            .take()
            .ok_or_else(|| fail("missing spill victim"))?;
        let start = Instant::now();
        results[p][r].result = Some(result.spill().await?);
        resources.count("spill_finalization_ns", nanos(start));
    }
    Ok(())
}

async fn prepare(
    ctx: Arc<StreamContext>,
    partitions: Vec<Vec<PartitionRange>>,
    pruner: Arc<Pruner>,
    metrics: PartitionMetrics,
    metrics_set: ExecutionPlanMetricsSet,
    resources: Arc<StoreResources>,
    options: Options,
) -> Result<Vec<Manifest>> {
    let started = Instant::now();
    let num_partitions = partitions.len();
    let semaphore = Arc::new(Semaphore::new(1));
    let catalog = Arc::new(TagCatalog::new(
        ctx.input.region_metadata().clone(),
        &resources.pool(),
        CompactMetrics::new(&metrics_set, num_partitions),
    ));
    let scanner = SeriesCandidateScanner::try_new(
        ctx.clone(),
        partitions.clone(),
        pruner,
        semaphore.clone(),
        resources.pool(),
        metrics_set,
        metrics.clone(),
    )?
    .with_catalog(catalog.clone());
    let partition_pruner = scanner.partition_pruner();
    let mut candidates = scanner.build_stream().await?;
    let assignments_memory =
        MemoryConsumer::new("BufferedSeriesScan::assignments").register(&resources.pool());
    let mut chunks = Vec::new();
    let mut collector =
        SeriesBatchCollector::new(num_partitions).ok_or_else(|| fail("no output partitions"))?;
    while let Some(batch) = candidates.try_next().await? {
        assignments_memory
            .try_grow(checked_mul(
                batch.capacity(),
                2 * std::mem::size_of::<MetricSeriesId>(),
            )?)
            .map_err(|e| fail(e.to_string()))?;
        collector.push(batch);
        if collector.len() >= 1_000_000 {
            chunks.push(collector.finish(false));
            collector = SeriesBatchCollector::new(num_partitions)
                .ok_or_else(|| fail("no output partitions"))?;
        }
    }
    drop(candidates);
    drop(scanner);
    if collector.len() > 0 {
        chunks.push(collector.finish(false));
    }
    let mut ranges = partitions.into_iter().flatten().collect::<Vec<_>>();
    ranges.sort_unstable_by_key(|r| ctx.ranges[r.identifier].time_range.0);
    for range in &ranges {
        let bounds = ctx.ranges[range.identifier].time_range;
        if bounds.0 > bounds.1 {
            return Err(fail("inverted complete range bounds"));
        }
    }
    for pair in ranges.windows(2) {
        let previous = ctx.ranges[pair[0].identifier].time_range;
        let next = ctx.ranges[pair[1].identifier].time_range;
        validate_separation(previous, next)?;
    }
    let mut compact = CompactReadContext::preflight_inner(
        &ctx,
        &ranges,
        &partition_pruner,
        &metrics,
        catalog.clone(),
        assignments_memory,
        Some(resources.clone()),
    )
    .await?;
    compact.buffered_resources = Some(resources.clone());
    let compact = Arc::new(compact);
    let mut results = (0..num_partitions).map(|_| Vec::new()).collect::<Vec<_>>();
    let mut identities = vec![Vec::new(); num_partitions];
    let mut metadata = (0..num_partitions)
        .map(|_| resources.reserve(Kind::Metadata, 256))
        .collect::<Result<Vec<_>>>()?;
    for chunk in &chunks {
        for (p, assignment) in chunk.iter().enumerate() {
            metadata[p].grow(checked_mul(
                assignment.series().len(),
                std::mem::size_of::<MetricSeriesId>(),
            )?)?;
            identities[p]
                .try_reserve_exact(assignment.series().len())
                .map_err(|e| fail(e.to_string()))?;
            identities[p].extend_from_slice(assignment.series());
        }
    }
    // This workspace is kept free alongside every admitted merge, so reclamation
    // cannot require releasing memory owned by that same merge.
    let writer_bytes = checked_add(checked_mul(options.batch_bytes, 24)?, 4 * 1024 * 1024)?;
    for range in &ranges {
        let bounds = ctx.ranges[range.identifier].time_range;
        let mut reader_bytes = 0;
        for index in &ctx.ranges[range.identifier].row_group_indices {
            let bytes = if ctx.is_file_range_index(*index) {
                compact
                    .files(*index)?
                    .iter()
                    .map(|(file, _)| file.buffered_reader_bytes())
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .max()
                    .unwrap_or(0)
            } else {
                // Memtable source allocations already belong to the engine. Reserve
                // their scan materialization and merge head conservatively.
                checked_add(
                    ctx.input.memtables[index.index].stats().bytes_allocated(),
                    checked_mul(options.batch_bytes, 4)?,
                )?
            };
            reader_bytes = checked_add(reader_bytes, bytes)?;
        }
        for chunk in &chunks {
            for (p, assignment) in chunk.iter().enumerate() {
                if assignment.series().is_empty() {
                    continue;
                }
                // The selected-series filter owns a sorted vector, a hash set and
                // transient assignment copies for the duration of this merge.
                let filter_bytes = checked_mul(assignment.series().len(), 128)?;
                let reader_bytes = checked_add(reader_bytes, filter_bytes)?;
                reclaim(
                    &mut results,
                    &resources,
                    checked_add(reader_bytes, writer_bytes)?,
                    "range preparation",
                )
                .await?;
                resources.peak("range_reader_estimate_bytes", reader_bytes);
                let range_started = Instant::now();
                let reader_charge = Arc::new(resources.reserve(Kind::Workspace, reader_bytes)?);
                let reader = SeriesReader::try_new(
                    ctx.clone(),
                    vec![*range],
                    assignment.clone(),
                    partition_pruner.clone(),
                    semaphore.clone(),
                    metrics.clone(),
                )?
                .with_compact(Some(compact.clone()));
                let mut stream = reader.build_complete_range(*range).await?;
                let mut builder = ResultBuilder::with_resources(
                    resources.clone(),
                    compact.schema.schema.clone(),
                )?;
                while let Some(batch) = stream.try_next().await? {
                    validate_batch(&batch, bounds)?;
                    resources.peak(
                        "merge_batch_bytes",
                        crate::read::series_result::unique_batch_bytes(&batch)?,
                    );
                    // Reserve writer headroom before retaining more payload. The builder
                    // can mix completed resident and file batches within this range.
                    let placement = if resources.pool().reserved() >= options.spill_threshold
                        || resources.available() < checked_mul(writer_bytes, 2)?
                    {
                        builder = builder.spill_resident().await?;
                        Placement::File
                    } else {
                        Placement::Resident
                    };
                    let append = Instant::now();
                    builder = builder
                        .append_prepared(batch, placement, reader_charge.clone())
                        .await?;
                    resources.count("result_append_ns", nanos(append));
                    if placement == Placement::File {
                        resources.count("spill_append_ns", nanos(append));
                    }
                }
                drop(stream);
                drop(reader);
                resources.count("range_preparation_ns", nanos(range_started));
                drop(reader_charge);
                if resources.live_readers() != 0 {
                    return Err(fail("range completed with live SST readers"));
                }
                resources.count("completed_range_sources_released", 1);
                let finish = Instant::now();
                let result = builder.finish().await?;
                resources.count("spill_finalization_ns", nanos(finish));
                metadata[p].grow(std::mem::size_of::<PreparedRange>())?;
                results[p]
                    .try_reserve_exact(1)
                    .map_err(|e| fail(e.to_string()))?;
                results[p].push(PreparedRange {
                    bounds,
                    result: Some(result),
                });
                if resources.pool().reserved() >= options.spill_threshold {
                    let target = options
                        .memory_bytes
                        .saturating_sub(options.spill_threshold)
                        .max(writer_bytes);
                    reclaim(&mut results, &resources, target, "spill threshold").await?;
                }
            }
        }
    }
    let schema = Arc::new(CompactSchema::new(&ctx.input.mapper));
    let last_row = ctx.input.series_row_selector.is_some();
    // No source/mapping context is retained by readiness manifests.
    drop(compact);
    drop(chunks);
    drop(partition_pruner);
    let tag_bytes = catalog.buffered_tag_bytes(options.batch_rows)?;
    // Spill can increase replay requirements, so recompute after every reclamation.
    let reservations = loop {
        let sizes = results
            .iter()
            .zip(&identities)
            .map(|(ranges, ids)| {
                // PromSeriesDivide retains a complete identity before concatenation.
                // Fund its escaped output leases separately from the bounded current
                // replay batch; arbitrary collect-all consumers are not supported.
                let output = ids.iter().try_fold(0, |maximum: usize, id| {
                    let mut bytes = 0;
                    for range in ranges.iter().rev() {
                        let result = range
                            .result
                            .as_ref()
                            .ok_or_else(|| fail("missing prepared result"))?;
                        bytes = checked_add(bytes, result.series_output_bytes(*id, tag_bytes)?)?;
                        if last_row && result.has_series(*id) {
                            break;
                        }
                    }
                    Ok::<usize, crate::error::Error>(maximum.max(bytes))
                })?;
                resources.peak("publication_partition_output_bytes", output);
                ranges
                    .iter()
                    .try_fold(0, |n, r| {
                        Ok(n.max(
                            r.result
                                .as_ref()
                                .ok_or_else(|| fail("missing prepared result"))?
                                .replay_bytes()?,
                        ))
                    })
                    .and_then(|payload| {
                        if payload == 0 {
                            Ok(0)
                        } else {
                            // The stream adapter may retain the previous output while polling
                            // the next one. Fund both payload and assembled tags twice.
                            let active = checked_mul(checked_add(payload, tag_bytes)?, 2)?;
                            resources.peak("publication_partition_active_replay_bytes", active);
                            checked_add(active, output)
                        }
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let total = sizes.iter().try_fold(0, |n, b| checked_add(n, *b))?;
        if resources.available() >= total {
            resources.peak("publication_reserved_bytes", total);
            break sizes;
        }
        let before = resources.available();
        reclaim(
            &mut results,
            &resources,
            checked_add(before, 1)?,
            "publication",
        )
        .await?;
    };
    let mut manifests = Vec::with_capacity(num_partitions);
    for (((mut ids, mut ranges), bytes), metadata) in identities
        .into_iter()
        .zip(results)
        .zip(reservations)
        .zip(metadata)
    {
        ids.sort_unstable();
        ids.dedup();
        ranges.sort_by_key(|r| r.bounds.0);
        for id in &ids {
            let mut previous = None;
            for range in &ranges {
                if range.result.as_ref().is_some_and(|h| h.has_series(*id)) {
                    if let Some(previous) = previous {
                        validate_separation(previous, range.bounds)?;
                    }
                    previous = Some(range.bounds);
                }
            }
        }
        let replay = BudgetPool::replay(&resources.pool(), bytes)
            .map_err(|_| admission(&resources, "publication", bytes))?;
        manifests.push(Manifest {
            ids,
            ranges,
            schema: schema.clone(),
            catalog: catalog.clone(),
            resources: resources.clone(),
            replay,
            last_row,
            tag_bytes,
            _metadata: metadata,
        });
    }
    resources.count("preparation_ns", nanos(started));
    resources.count("published_manifests", manifests.len());
    Ok(manifests)
}

fn validate_separation(previous: FileTimeRange, next: FileTimeRange) -> Result<()> {
    if previous.0 > previous.1 || next.0 > next.1 || previous.1 >= next.0 {
        return Err(fail(format!(
            "incomplete range grouping: previous={previous:?}, next={next:?}"
        )));
    }
    Ok(())
}

fn validate_batch(batch: &RecordBatch, bounds: FileTimeRange) -> Result<()> {
    let (timestamps, unit) =
        timestamp_array_to_primitive(batch.column(time_index_column_index(batch.num_columns())))
            .ok_or_else(|| fail("prepared range has no timestamp column"))?;
    for value in timestamps.iter() {
        let value = value.ok_or_else(|| fail("null prepared timestamp"))?;
        let time = Timestamp::new(value, unit.into());
        if time < bounds.0 || time > bounds.1 {
            return Err(fail(format!(
                "prepared timestamp {time:?} outside declared bounds {bounds:?}"
            )));
        }
    }
    Ok(())
}

fn replay(manifest: Manifest) -> crate::read::BoxedRecordBatchStream {
    Box::pin(try_stream! {
        let pool: Arc<dyn MemoryPool> = manifest.replay.clone();
        for id in &manifest.ids {
            // Range-level LastRow has already selected the last timestamp. Disjoint
            // inclusive ranges make the last nonempty contribution the final selector,
            // including every equal-timestamp append-mode row without buffering it.
            let last = manifest.last_row.then(|| manifest.ranges.iter().rposition(|r| r.result.as_ref().is_some_and(|h| h.has_series(*id)))).flatten();
            for (index, range) in manifest.ranges.iter().enumerate() {
                if manifest.last_row && last != Some(index) { continue; }
                let handle = range.result.as_ref().ok_or_else(|| fail("missing published result"))?;
                if !handle.has_series(*id) { continue; }
                let mut cursor = handle.series_cursor_in(*id, pool.clone())?;
                loop {
                    let start = Instant::now();
                    let next = cursor.next().await?;
                    manifest.resources.count("replay_ns", nanos(start));
                    let Some(lease) = next else { break; };
                    let mut charge = manifest.resources.reserve_in(Kind::Workspace, manifest.tag_bytes, &pool)?;
                    let start = Instant::now();
                    let batch = lease.with_batch(|batch| manifest.schema.assemble(batch.clone(), &manifest.catalog))?;
                    manifest.resources.count("tag_assembly_ns", nanos(start));
                    let incremental = lease.with_batch(|input| crate::read::series_result::incremental_batch_bytes(&batch, input))?;
                    charge.resize(incremental)?;
                    // Charge covers new tag/output allocations; fields still pin the
                    // original resident or decoded payload through their buffer leases.
                    let batch = pin_batch(batch, charge)?;
                    drop(lease);
                    yield batch;
                }
                cursor.close().await?;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_time::timestamp::TimeUnit;
    use datatypes::arrow::array::{
        ArrayRef, BinaryArray, TimestampMicrosecondArray, UInt8Array, UInt64Array,
    };
    use datatypes::arrow::datatypes::{Field, Schema};

    #[tokio::test]
    async fn publication_reclaims_unpublished_results_and_rejects_irreducible_capacity() {
        use crate::read::series_result::tests::fixture;
        use common_error::ext::ErrorExt;
        let dir = common_test_util::temp_dir::create_temp_dir("buffered-publication");
        let batch = fixture(&[(1, 1, 16_384)]);
        let builder = ResultBuilder::new(
            dir.path(),
            batch.schema(),
            StoreOptions {
                batch_rows: 128,
                batch_bytes: 8192,
                memory_bytes: 8 * 1024 * 1024,
                metadata_bytes: 2 * 1024 * 1024,
                disk_bytes: 8 * 1024 * 1024,
                ..StoreOptions::default()
            },
        )
        .await
        .unwrap();
        let resources = builder.resources();
        let result = builder
            .append(batch, Placement::Resident)
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let mut results = vec![vec![PreparedRange {
            bounds: (Timestamp::new_second(0), Timestamp::new_second(1)),
            result: Some(result),
        }]];
        let required = resources.available() + 1;
        reclaim(&mut results, &resources, required, "publication")
            .await
            .unwrap();
        let handle = results[0][0].result.as_ref().unwrap();
        assert!(!handle.has_resident());
        assert_eq!(0, resources.snapshot().payload_bytes);
        let bytes = handle.replay_bytes().unwrap();
        let consumers = (0..8)
            .map(|_| BudgetPool::replay(&resources.pool(), bytes).unwrap())
            .collect::<Vec<_>>();
        assert!(resources.snapshot().memory_bytes <= 8 * 1024 * 1024);
        for consumer in consumers {
            consumer.close();
        }
        let error = reclaim(&mut results, &resources, 9 * 1024 * 1024, "publication")
            .await
            .unwrap_err();
        assert_eq!(
            common_error::status_code::StatusCode::RuntimeResourcesExhausted,
            error.status_code()
        );
        assert!(error.to_string().contains("stage=publication"));
        // A threshold is a reclamation target, not an irreducible-workspace error.
        reclaim(&mut results, &resources, 9 * 1024 * 1024, "spill threshold")
            .await
            .unwrap();
        drop(results);
        resources.drain_cleanup().await.unwrap();
        assert_eq!(0, resources.snapshot().disk_bytes);
    }

    #[test]
    fn inclusive_bounds_use_timestamp_units_and_reject_invalid_spans() {
        let previous = (Timestamp::new_second(-2), Timestamp::new_second(1));
        let touching = (Timestamp::new_millisecond(1000), Timestamp::new_second(2));
        assert!(validate_separation(previous, touching).is_err());
        assert!(
            validate_separation(
                previous,
                (
                    Timestamp::new_microsecond(1_000_001),
                    Timestamp::new_second(2)
                )
            )
            .is_ok()
        );
        assert!(
            validate_separation(
                (Timestamp::new_second(2), Timestamp::new_second(1)),
                touching
            )
            .is_err()
        );
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(TimestampMicrosecondArray::from(vec![-2_000_000, 1_000_000])),
            Arc::new(BinaryArray::from_iter_values([b"key".as_slice(); 2])),
            Arc::new(UInt64Array::from(vec![1, 1])),
            Arc::new(UInt8Array::from(vec![1, 1])),
        ];
        let schema = Arc::new(Schema::new(
            arrays
                .iter()
                .enumerate()
                .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), false))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(schema, arrays).unwrap();
        assert!(validate_batch(&batch, previous).is_ok());
        assert!(
            validate_batch(&batch, (Timestamp::new(-1, TimeUnit::Second), previous.1)).is_err()
        );
    }
}
