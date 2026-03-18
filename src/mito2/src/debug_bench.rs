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

use std::collections::VecDeque;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common_time::Timestamp;
use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, col, lit};
use datatypes::arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use object_store::services::Memory;
use partition::expr::PartitionExpr;
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::PathType;
use store_api::storage::{ColumnId, FileId, RegionId};
use table::predicate::Predicate;

use crate::cache::{CacheManager, CacheStrategy};
use crate::error::Result;
use crate::read::Batch;
use crate::sst::file::{FileHandle, FileMeta};
use crate::sst::file_purger::NoopFilePurger;
use crate::sst::index::bloom_filter::applier::{
    BloomFilterIndexApplierBuilder, BloomFilterIndexApplierRef,
};
use crate::sst::index::fulltext_index::applier::FulltextIndexApplierRef;
use crate::sst::index::fulltext_index::applier::builder::FulltextIndexApplierBuilder;
use crate::sst::index::inverted_index::applier::InvertedIndexApplierRef;
use crate::sst::index::inverted_index::applier::builder::InvertedIndexApplierBuilder;
use crate::sst::index::puffin_manager::PuffinManagerFactory;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::file_range::FileRange;
use crate::sst::parquet::metadata::MetadataLoader;
use crate::sst::parquet::reader::{MetadataCacheMetrics, ParquetReaderBuilder, ReaderMetrics};

const BENCH_PROJECTION_POSITIONS: [usize; 6] = [0, 1, 2, 3, 4, 79];
const BENCH_TIME_START_MS: i64 = 1_742_550_540_001;
const BENCH_TIME_END_MS: i64 = 1_742_552_400_000;
const BENCH_TABLE_ID: u32 = 1182;

#[derive(Debug)]
struct ExternalBenchConfig {
    parquet_file: String,
    iterations: usize,
}

impl ExternalBenchConfig {
    fn from_env() -> Self {
        let parquet_file = env::var("MITO_BENCH_PARQUET_FILE")
            .expect("MITO_BENCH_PARQUET_FILE must point to the parquet file to benchmark");
        let iterations = env::var("MITO_BENCH_ITERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(5);
        Self {
            parquet_file,
            iterations,
        }
    }
}

struct ExternalBenchFixture {
    object_store: ObjectStore,
    table_dir: String,
    path_type: PathType,
    file_handle: FileHandle,
    metadata: RegionMetadataRef,
    puffin_manager_factory: PuffinManagerFactory,
}

#[derive(Default, Debug, Clone)]
struct ReaderRunStats {
    build_input_cost: Duration,
    build_reader_cost: Duration,
    scan_cost: Duration,
    convert_cost: Duration,
    prune_cost: Duration,
    wall_time: Duration,
    selected_row_groups: usize,
    selected_rows: usize,
    output_batches: usize,
    output_rows: usize,
}

fn avg_duration(total: Duration, iterations: usize) -> Duration {
    Duration::from_secs_f64(total.as_secs_f64() / iterations as f64)
}

fn avg_count(total: usize, iterations: usize) -> f64 {
    total as f64 / iterations as f64
}

fn scan_request_projection_ids(metadata: &RegionMetadataRef) -> Vec<ColumnId> {
    BENCH_PROJECTION_POSITIONS
        .iter()
        .map(|idx| {
            metadata
                .column_metadatas
                .get(*idx)
                .unwrap_or_else(|| {
                    panic!(
                        "projection position {} is out of bounds for metadata with {} columns",
                        idx,
                        metadata.column_metadatas.len()
                    )
                })
                .column_id
        })
        .collect()
}

fn scan_request_predicate() -> Predicate {
    Predicate::new(vec![
        col("mode").eq(lit("idle")),
        col("region").eq(lit("us-west-2")),
        col("greptime_timestamp").gt_eq(lit(ScalarValue::TimestampMillisecond(
            Some(BENCH_TIME_START_MS),
            None,
        ))),
        col("greptime_timestamp").lt_eq(lit(ScalarValue::TimestampMillisecond(
            Some(BENCH_TIME_END_MS),
            None,
        ))),
        col("__table_id").eq(lit(BENCH_TABLE_ID)),
    ])
}

fn external_file_meta(
    region_id: RegionId,
    file_id: FileId,
    file_size: u64,
    num_rows: u64,
    num_row_groups: u64,
    partition_expr: Option<&str>,
) -> FileMeta {
    FileMeta {
        region_id,
        file_id,
        time_range: (
            Timestamp::new_millisecond(i64::MIN),
            Timestamp::new_millisecond(i64::MAX),
        ),
        level: 0,
        file_size,
        max_row_group_uncompressed_size: 0,
        available_indexes: Default::default(),
        indexes: Default::default(),
        index_file_size: 0,
        index_version: 0,
        num_rows,
        num_row_groups,
        sequence: None,
        partition_expr: partition_expr.and_then(|expr| {
            PartitionExpr::from_json_str(expr)
                .expect("partition expression in parquet metadata should be valid JSON")
        }),
        num_series: 0,
    }
}

async fn load_external_bench_fixture(config: &ExternalBenchConfig) -> Result<ExternalBenchFixture> {
    let bytes = tokio::fs::read(&config.parquet_file)
        .await
        .unwrap_or_else(|e| panic!("failed to read parquet file {}: {e}", config.parquet_file));
    let file_size = bytes.len() as u64;
    let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
    let table_dir = "bench_series_scan".to_string();
    let path_type = PathType::Bare;
    let file_purger = Arc::new(NoopFilePurger);
    let aux_path =
        std::env::temp_dir().join(format!("mito2-bench-puffin-{}", uuid::Uuid::new_v4()));
    let puffin_manager_factory = PuffinManagerFactory::new(&aux_path, 4096, None, None).await?;

    // Write once to decode embedded region metadata, then write again to the final path that
    // matches the real region id from the file metadata.
    let probe_file_id = FileId::random();
    let probe_handle = FileHandle::new(
        external_file_meta(RegionId::new(0, 0), probe_file_id, file_size, 0, 0, None),
        file_purger.clone(),
    );
    let probe_path = probe_handle.file_path(&table_dir, path_type);
    object_store
        .write(&probe_path, bytes.clone())
        .await
        .unwrap_or_else(|e| panic!("failed to write probe parquet object {probe_path}: {e}"));

    let mut cache_metrics = MetadataCacheMetrics::default();
    let loader = MetadataLoader::new(object_store.clone(), &probe_path, file_size);
    let parquet_meta = loader.load(&mut cache_metrics).await?;
    let metadata = std::sync::Arc::new(ParquetReaderBuilder::get_region_metadata(
        &probe_path,
        parquet_meta.file_metadata().key_value_metadata(),
    )?);

    let final_file_id = FileId::random();
    let file_handle = FileHandle::new(
        external_file_meta(
            metadata.region_id,
            final_file_id,
            file_size,
            parquet_meta.file_metadata().num_rows() as u64,
            parquet_meta.row_groups().len() as u64,
            metadata.partition_expr.as_deref(),
        ),
        file_purger,
    );
    let final_path = file_handle.file_path(&table_dir, path_type);
    object_store
        .write(&final_path, bytes)
        .await
        .unwrap_or_else(|e| panic!("failed to write benchmark parquet object {final_path}: {e}"));

    Ok(ExternalBenchFixture {
        object_store,
        table_dir,
        path_type,
        file_handle,
        metadata,
        puffin_manager_factory,
    })
}

fn build_index_appliers(
    fixture: &ExternalBenchFixture,
    filters: &[Expr],
) -> (
    [Option<InvertedIndexApplierRef>; 2],
    [Option<BloomFilterIndexApplierRef>; 2],
    [Option<FulltextIndexApplierRef>; 2],
) {
    let metadata = fixture.metadata.as_ref();

    let inverted_index_applier = InvertedIndexApplierBuilder::new(
        fixture.table_dir.clone(),
        fixture.path_type,
        fixture.object_store.clone(),
        metadata,
        metadata.inverted_indexed_column_ids(std::iter::empty()),
        fixture.puffin_manager_factory.clone(),
    )
    .build(filters)
    .ok()
    .flatten()
    .map(Arc::new);

    let bloom_filter_applier = BloomFilterIndexApplierBuilder::new(
        fixture.table_dir.clone(),
        fixture.path_type,
        fixture.object_store.clone(),
        metadata,
        fixture.puffin_manager_factory.clone(),
    )
    .build(filters)
    .ok()
    .flatten()
    .map(Arc::new);

    let fulltext_index_applier = FulltextIndexApplierBuilder::new(
        fixture.table_dir.clone(),
        fixture.path_type,
        fixture.object_store.clone(),
        fixture.puffin_manager_factory.clone(),
        metadata,
    )
    .build(filters)
    .ok()
    .flatten()
    .map(Arc::new);

    (
        [inverted_index_applier, None],
        [bloom_filter_applier, None],
        [None, fulltext_index_applier],
    )
}

fn create_bench_cache() -> Arc<CacheManager> {
    Arc::new(
        CacheManager::builder()
            .sst_meta_cache_size(1024 * 1024)
            .page_cache_size(256 * 1024 * 1024)
            .index_result_cache_size(1024 * 1024)
            .index_metadata_size(1024 * 1024)
            .index_content_page_size(1024 * 1024)
            .index_content_size(1024 * 1024)
            .puffin_metadata_size(1024 * 1024)
            .build(),
    )
}

fn new_bench_builder(
    fixture: &ExternalBenchFixture,
    flat_format: bool,
    cache_strategy: CacheStrategy,
) -> ParquetReaderBuilder {
    let predicate = scan_request_predicate();
    let (inverted_index_appliers, bloom_filter_index_appliers, fulltext_index_appliers) =
        build_index_appliers(fixture, predicate.exprs());

    ParquetReaderBuilder::new(
        fixture.table_dir.clone(),
        fixture.path_type,
        fixture.file_handle.clone(),
        fixture.object_store.clone(),
    )
    .projection(Some(scan_request_projection_ids(&fixture.metadata)))
    .predicate(Some(predicate))
    .flat_format(flat_format)
    .cache(cache_strategy)
    .inverted_index_appliers(inverted_index_appliers)
    .bloom_filter_index_appliers(bloom_filter_index_appliers)
    .fulltext_index_appliers(fulltext_index_appliers)
}

async fn bench_raw_parquet_next(
    fixture: &ExternalBenchFixture,
    flat_format: bool,
    cache_strategy: CacheStrategy,
    iterations: usize,
) -> Result<ReaderRunStats> {
    let mut stats = ReaderRunStats::default();

    for _ in 0..iterations {
        let builder = new_bench_builder(fixture, flat_format, cache_strategy.clone());
        let mut metrics = ReaderMetrics::default();

        let build_input_start = Instant::now();
        let Some((context, selection)) = builder.build_reader_input(&mut metrics).await? else {
            panic!("benchmark selection is empty for flat_format={flat_format}");
        };
        stats.build_input_cost += build_input_start.elapsed();
        stats.selected_row_groups += selection.row_group_count();
        stats.selected_rows += selection.row_count();

        let ranges: Vec<_> = selection
            .iter()
            .map(|(row_group_idx, row_selection)| (*row_group_idx, row_selection.clone()))
            .collect();
        let context = std::sync::Arc::new(context);

        let wall_start = Instant::now();
        for (row_group_idx, row_selection) in ranges {
            let build_reader_start = Instant::now();
            let mut parquet_reader = context
                .reader_builder()
                .build(row_group_idx, Some(row_selection), None)
                .await?;
            stats.build_reader_cost += build_reader_start.elapsed();

            let scan_start = Instant::now();
            while let Some(batch_result) = parquet_reader.next() {
                let batch = batch_result.unwrap_or_else(|e| {
                    panic!(
                        "raw parquet next() failed for row_group={} flat_format={}: {e}",
                        row_group_idx, flat_format
                    )
                });
                stats.output_rows += batch.num_rows();
                stats.output_batches += 1;
            }
            stats.scan_cost += scan_start.elapsed();
        }
        stats.wall_time += wall_start.elapsed();
    }

    Ok(stats)
}

async fn bench_wrapped_reader(
    fixture: &ExternalBenchFixture,
    flat_format: bool,
    should_split: bool,
    cache_strategy: CacheStrategy,
    iterations: usize,
) -> Result<ReaderRunStats> {
    let mut stats = ReaderRunStats::default();

    for _ in 0..iterations {
        let builder = new_bench_builder(fixture, flat_format, cache_strategy.clone());
        let mut metrics = ReaderMetrics::default();

        let build_input_start = Instant::now();
        let Some((context, selection)) = builder.build_reader_input(&mut metrics).await? else {
            panic!("benchmark selection is empty for flat_format={flat_format}");
        };
        stats.build_input_cost += build_input_start.elapsed();
        stats.selected_row_groups += selection.row_group_count();
        stats.selected_rows += selection.row_count();

        let ranges: Vec<_> = selection
            .iter()
            .map(|(row_group_idx, row_selection)| (*row_group_idx, row_selection.clone()))
            .collect();
        let context = std::sync::Arc::new(context);

        let wall_start = Instant::now();
        for (row_group_idx, row_selection) in ranges {
            let range = FileRange::new(context.clone(), row_group_idx, Some(row_selection));
            let build_reader_start = Instant::now();

            if flat_format {
                let Some(mut reader) = range.flat_reader(None, None, should_split).await? else {
                    continue;
                };
                stats.build_reader_cost += build_reader_start.elapsed();

                while let Some(batch) = reader.next_batch()? {
                    stats.output_rows += batch.num_rows();
                    stats.output_batches += 1;
                }

                let reader_metrics = reader.metrics();
                stats.scan_cost += reader_metrics.scan_cost;
                stats.convert_cost += reader_metrics.convert_cost;
                stats.prune_cost += reader_metrics.prune_cost;
            } else {
                let Some(mut reader) = range.reader(None, None).await? else {
                    continue;
                };
                stats.build_reader_cost += build_reader_start.elapsed();

                while let Some(batch) = reader.next_batch().await? {
                    stats.output_rows += batch.num_rows();
                    stats.output_batches += 1;
                }

                let reader_metrics = reader.metrics();
                stats.scan_cost += reader_metrics.scan_cost;
                stats.convert_cost += reader_metrics.convert_cost;
                stats.prune_cost += reader_metrics.prune_cost;
            }
        }
        stats.wall_time += wall_start.elapsed();
    }

    Ok(stats)
}

fn print_bench_stats(label: &str, stats: &ReaderRunStats, iterations: usize) {
    println!(
        "{label}: avg_build_input={:?}, avg_build_reader={:?}, avg_scan={:?}, avg_convert={:?}, avg_prune={:?}, avg_wall={:?}, avg_selected_row_groups={:.2}, avg_selected_rows={:.2}, avg_output_batches={:.2}, avg_output_rows={:.2}",
        avg_duration(stats.build_input_cost, iterations),
        avg_duration(stats.build_reader_cost, iterations),
        avg_duration(stats.scan_cost, iterations),
        avg_duration(stats.convert_cost, iterations),
        avg_duration(stats.prune_cost, iterations),
        avg_duration(stats.wall_time, iterations),
        avg_count(stats.selected_row_groups, iterations),
        avg_count(stats.selected_rows, iterations),
        avg_count(stats.output_batches, iterations),
        avg_count(stats.output_rows, iterations),
    );
}

pub async fn run_sparse_series_scan_reader_bench_from_env() -> Result<()> {
    let config = ExternalBenchConfig::from_env();
    let fixture = load_external_bench_fixture(&config).await?;
    let primary_cache = create_bench_cache();
    let flat_cache = create_bench_cache();

    println!(
        "Benchmark file: {}, region_id: {}, projection_positions: {:?}, projection_ids: {:?}, iterations: {}",
        config.parquet_file,
        fixture.metadata.region_id,
        BENCH_PROJECTION_POSITIONS,
        scan_request_projection_ids(&fixture.metadata),
        config.iterations,
    );

    let raw_primary_no_cache =
        bench_raw_parquet_next(&fixture, false, CacheStrategy::Disabled, config.iterations).await?;
    let raw_flat_no_cache =
        bench_raw_parquet_next(&fixture, true, CacheStrategy::Disabled, config.iterations).await?;
    let wrapped_primary_no_cache = bench_wrapped_reader(
        &fixture,
        false,
        false,
        CacheStrategy::Disabled,
        config.iterations,
    )
    .await?;
    let wrapped_flat_no_cache = bench_wrapped_reader(
        &fixture,
        true,
        false,
        CacheStrategy::Disabled,
        config.iterations,
    )
    .await?;
    let wrapped_flat_split_no_cache = bench_wrapped_reader(
        &fixture,
        true,
        true,
        CacheStrategy::Disabled,
        config.iterations,
    )
    .await?;

    let raw_primary_cache = bench_raw_parquet_next(
        &fixture,
        false,
        CacheStrategy::EnableAll(primary_cache.clone()),
        config.iterations,
    )
    .await?;
    let raw_flat_cache = bench_raw_parquet_next(
        &fixture,
        true,
        CacheStrategy::EnableAll(flat_cache.clone()),
        config.iterations,
    )
    .await?;
    let wrapped_primary_cache = bench_wrapped_reader(
        &fixture,
        false,
        false,
        CacheStrategy::EnableAll(primary_cache),
        config.iterations,
    )
    .await?;
    let flat_split_cache = create_bench_cache();
    let wrapped_flat_cache = bench_wrapped_reader(
        &fixture,
        true,
        false,
        CacheStrategy::EnableAll(flat_cache),
        config.iterations,
    )
    .await?;
    let wrapped_flat_split_cache = bench_wrapped_reader(
        &fixture,
        true,
        true,
        CacheStrategy::EnableAll(flat_split_cache),
        config.iterations,
    )
    .await?;

    println!("Raw parquet next():");
    print_bench_stats(
        "  no_cache primary_key",
        &raw_primary_no_cache,
        config.iterations,
    );
    print_bench_stats("  no_cache flat", &raw_flat_no_cache, config.iterations);
    print_bench_stats("  cache primary_key", &raw_primary_cache, config.iterations);
    print_bench_stats("  cache flat", &raw_flat_cache, config.iterations);

    println!("Wrapped readers:");
    print_bench_stats(
        "  no_cache RowGroupReader",
        &wrapped_primary_no_cache,
        config.iterations,
    );
    print_bench_stats(
        "  no_cache FlatRowGroupReader",
        &wrapped_flat_no_cache,
        config.iterations,
    );
    print_bench_stats(
        "  no_cache FlatRowGroupReader(split)",
        &wrapped_flat_split_no_cache,
        config.iterations,
    );
    print_bench_stats(
        "  cache RowGroupReader",
        &wrapped_primary_cache,
        config.iterations,
    );
    print_bench_stats(
        "  cache FlatRowGroupReader",
        &wrapped_flat_cache,
        config.iterations,
    );
    print_bench_stats(
        "  cache FlatRowGroupReader(split)",
        &wrapped_flat_split_cache,
        config.iterations,
    );

    Ok(())
}

const MAX_COLLECT_BATCHES: usize = 100;

/// Collects raw parquet RecordBatches from the file, up to `MAX_COLLECT_BATCHES`.
async fn collect_raw_batches(
    fixture: &ExternalBenchFixture,
    flat_format: bool,
) -> Result<Vec<RecordBatch>> {
    let builder = new_bench_builder(fixture, flat_format, CacheStrategy::Disabled);
    let mut metrics = ReaderMetrics::default();

    let Some((context, selection)) = builder.build_reader_input(&mut metrics).await? else {
        panic!("benchmark selection is empty for flat_format={flat_format}");
    };
    let context = Arc::new(context);

    let mut raw_batches = Vec::new();
    for (row_group_idx, row_selection) in selection.iter() {
        if raw_batches.len() >= MAX_COLLECT_BATCHES {
            break;
        }
        let mut parquet_reader = context
            .reader_builder()
            .build(*row_group_idx, Some(row_selection.clone()), None)
            .await?;
        while let Some(batch_result) = parquet_reader.next() {
            let batch = batch_result.unwrap_or_else(|e| {
                panic!("raw parquet next() failed for row_group={row_group_idx}: {e}")
            });
            raw_batches.push(batch);
            if raw_batches.len() >= MAX_COLLECT_BATCHES {
                break;
            }
        }
    }

    Ok(raw_batches)
}

/// Benchmark that isolates convert_batch and precise_filter costs.
///
/// Set `MITO_BENCH_MODE` to `convert`, `filter`, or `all` (default).
pub async fn run_convert_and_filter_bench_from_env() -> Result<()> {
    let config = ExternalBenchConfig::from_env();
    let fixture = load_external_bench_fixture(&config).await?;
    let bench_mode = env::var("MITO_BENCH_MODE")
        .unwrap_or_else(|_| "all".to_string())
        .to_lowercase();

    println!(
        "Benchmark file: {}, region_id: {}, iterations: {}, mode: {}",
        config.parquet_file, fixture.metadata.region_id, config.iterations, bench_mode,
    );

    // Build contexts for both formats.
    let flat_builder = new_bench_builder(&fixture, true, CacheStrategy::Disabled);
    let pk_builder = new_bench_builder(&fixture, false, CacheStrategy::Disabled);
    let mut flat_metrics = ReaderMetrics::default();
    let mut pk_metrics = ReaderMetrics::default();

    let Some((flat_context, _)) = flat_builder.build_reader_input(&mut flat_metrics).await? else {
        panic!("benchmark selection is empty for flat_format=true");
    };
    let Some((pk_context, _)) = pk_builder.build_reader_input(&mut pk_metrics).await? else {
        panic!("benchmark selection is empty for flat_format=false");
    };
    let flat_context = Arc::new(flat_context);
    let pk_context = Arc::new(pk_context);

    // Collect raw batches.
    let flat_raw_batches = collect_raw_batches(&fixture, true).await?;
    let pk_raw_batches = collect_raw_batches(&fixture, false).await?;
    let flat_total_rows: usize = flat_raw_batches.iter().map(|b| b.num_rows()).sum();
    let pk_total_rows: usize = pk_raw_batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "Collected {} flat raw batches ({} rows), {} pk raw batches ({} rows)",
        flat_raw_batches.len(),
        flat_total_rows,
        pk_raw_batches.len(),
        pk_total_rows,
    );

    let run_convert = bench_mode == "convert" || bench_mode == "all";
    let run_filter = bench_mode == "filter" || bench_mode == "all";

    let pprof_file = env::var("MITO_BENCH_PPROF_FILE").ok();

    // Start profiling if pprof_file is specified.
    #[cfg(unix)]
    let profiler_guard = if let Some(ref pprof_path) = pprof_file {
        println!("Starting profiling, output: {}", pprof_path);
        Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(99)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
                .unwrap_or_else(|e| panic!("Failed to start profiler: {e}")),
        )
    } else {
        None
    };

    #[cfg(not(unix))]
    if pprof_file.is_some() {
        eprintln!("Warning: Profiling is not supported on this platform");
    }

    // --- Convert benchmarks ---
    if run_convert {
        // FlatReadFormat::convert_batch
        let flat_format = flat_context.read_format().as_flat().unwrap();
        let flat_override_seq = flat_format.new_override_sequence_array(DEFAULT_READ_BATCH_SIZE);
        let mut total_convert_flat = Duration::ZERO;
        let mut convert_flat_rows = 0usize;
        for _ in 0..config.iterations {
            let start = Instant::now();
            for batch in &flat_raw_batches {
                let converted =
                    flat_format.convert_batch(batch.clone(), flat_override_seq.as_ref())?;
                convert_flat_rows += converted.num_rows();
            }
            total_convert_flat += start.elapsed();
        }
        println!(
            "FlatReadFormat::convert_batch: avg={:?}, batches={}, avg_rows={:.0}",
            avg_duration(total_convert_flat, config.iterations),
            flat_raw_batches.len(),
            avg_count(convert_flat_rows, config.iterations),
        );

        // PrimaryKeyReadFormat::convert_record_batch
        let pk_format = pk_context.read_format().as_primary_key().unwrap();
        let pk_override_seq = pk_format.new_override_sequence_array(DEFAULT_READ_BATCH_SIZE);
        let mut total_convert_pk = Duration::ZERO;
        let mut convert_pk_rows = 0usize;
        for _ in 0..config.iterations {
            let start = Instant::now();
            for batch in &pk_raw_batches {
                let mut batches_out = VecDeque::new();
                pk_format.convert_record_batch(
                    batch,
                    pk_override_seq.as_ref(),
                    &mut batches_out,
                )?;
                convert_pk_rows += batches_out.iter().map(|b| b.num_rows()).sum::<usize>();
            }
            total_convert_pk += start.elapsed();
        }
        println!(
            "PrimaryKeyReadFormat::convert_record_batch: avg={:?}, batches={}, avg_rows={:.0}",
            avg_duration(total_convert_pk, config.iterations),
            pk_raw_batches.len(),
            avg_count(convert_pk_rows, config.iterations),
        );
    }

    // --- Filter benchmarks ---
    if run_filter {
        // Pre-convert batches (not timed).
        let flat_format = flat_context.read_format().as_flat().unwrap();
        let flat_override_seq = flat_format.new_override_sequence_array(DEFAULT_READ_BATCH_SIZE);
        let mut converted_flat_batches = Vec::new();
        for batch in &flat_raw_batches {
            let converted = flat_format.convert_batch(batch.clone(), flat_override_seq.as_ref())?;
            converted_flat_batches.push(converted);
        }

        let pk_format = pk_context.read_format().as_primary_key().unwrap();
        let pk_override_seq = pk_format.new_override_sequence_array(DEFAULT_READ_BATCH_SIZE);
        let mut converted_pk_batches: Vec<Batch> = Vec::new();
        for batch in &pk_raw_batches {
            let mut batches_out = VecDeque::new();
            pk_format.convert_record_batch(batch, pk_override_seq.as_ref(), &mut batches_out)?;
            converted_pk_batches.extend(batches_out);
        }

        // precise_filter_flat
        let mut total_filter_flat = Duration::ZERO;
        let mut filter_flat_output_rows = 0usize;
        for _ in 0..config.iterations {
            let start = Instant::now();
            for batch in &converted_flat_batches {
                if let Some(filtered) = flat_context.precise_filter_flat(batch.clone(), false)? {
                    filter_flat_output_rows += filtered.num_rows();
                }
            }
            total_filter_flat += start.elapsed();
        }
        println!(
            "precise_filter_flat: avg={:?}, input_batches={}, avg_output_rows={:.0}",
            avg_duration(total_filter_flat, config.iterations),
            converted_flat_batches.len(),
            avg_count(filter_flat_output_rows, config.iterations),
        );

        // precise_filter
        let mut total_filter_pk = Duration::ZERO;
        let mut filter_pk_output_rows = 0usize;
        for _ in 0..config.iterations {
            let start = Instant::now();
            for batch in &converted_pk_batches {
                if let Some(filtered) = pk_context.precise_filter(batch.clone(), false)? {
                    filter_pk_output_rows += filtered.num_rows();
                }
            }
            total_filter_pk += start.elapsed();
        }
        println!(
            "precise_filter: avg={:?}, input_batches={}, avg_output_rows={:.0}",
            avg_duration(total_filter_pk, config.iterations),
            converted_pk_batches.len(),
            avg_count(filter_pk_output_rows, config.iterations),
        );
    }

    // Stop profiling and generate flamegraph.
    #[cfg(unix)]
    if let (Some(guard), Some(pprof_path)) = (profiler_guard, &pprof_file) {
        println!("Generating flamegraph...");
        match guard.report().build() {
            Ok(report) => {
                let mut flamegraph_data = Vec::new();
                if let Err(e) = report.flamegraph(&mut flamegraph_data) {
                    eprintln!("Failed to generate flamegraph: {e}");
                } else if let Err(e) = std::fs::write(pprof_path, flamegraph_data) {
                    eprintln!("Failed to write flamegraph to {pprof_path}: {e}");
                } else {
                    println!("Flamegraph saved to {pprof_path}");
                }
            }
            Err(e) => {
                eprintln!("Failed to generate pprof report: {e}");
            }
        }
    }

    Ok(())
}
