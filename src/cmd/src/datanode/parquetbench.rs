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

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use colored::Colorize;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use futures::StreamExt;
use mito2::cache::CacheStrategy;
use mito2::read::range::FileRangeBuilder;
use mito2::read::read_columns::ReadColumns;
use mito2::sst::file::{FileHandle, FileMeta, RegionFileId};
use mito2::sst::file_purger::NoopFilePurger;
use mito2::sst::location::sst_file_path;
use mito2::sst::parquet::metadata::MetadataLoader;
use mito2::sst::parquet::push_decoder::{
    SstParquetRangeFetcher, build_sst_parquet_record_batch_stream,
};
use mito2::sst::parquet::reader::{MetadataCacheMetrics, ParquetReaderBuilder, ReaderMetrics};
use mito2::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use serde::Deserialize;
use smallvec::SmallVec;
use snafu::ResultExt;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::region_request::PathType;
use store_api::storage::consts::{PRIMARY_KEY_COLUMN_NAME, is_internal_column};
use store_api::storage::{ColumnId, FileId, RegionId};

use crate::datanode::research::{ResearchTrace, Trace, observe, process_memory};
use crate::datanode::tool_util::{
    build_object_store, build_research_object_store, extract_region_metadata, format_bytes,
    parse_config, parse_file_id, parse_path_type, parse_region_id,
};
use crate::error;
use datatypes::arrow::array::{Array, BinaryArray, DictionaryArray};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use mito_codec::row_converter::{
    CompositeValues, PrimaryKeyCodec, SparsePrimaryKeyCodec, build_primary_key_codec,
};
use serde_json::json;

const DEFAULT_READ_BATCH_SIZE: usize = 8 * 1024;

/// Parquet benchmark command - benchmarks scanning a single parquet SST directly.
#[derive(Debug, Clone, Parser)]
pub struct ParquetbenchCommand {
    /// Path to config TOML file (same format as standalone/datanode config)
    #[clap(long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Region ID: either numeric u64 (e.g. "4398046511104") or "table_id:region_num" (e.g. "1024:0")
    #[clap(long)]
    region_id: Option<String>,

    /// Table directory relative to data home (e.g. "data/greptime/public/1024/")
    #[clap(long)]
    table_dir: Option<String>,

    /// SST file id to benchmark.
    #[clap(long)]
    file_id: Option<String>,

    /// Local parquet SST file to benchmark with the direct reader.
    #[clap(long, value_name = "FILE")]
    file_path: Option<PathBuf>,

    /// Path to scan request JSON config file (supports projection_names only)
    #[clap(long, value_name = "FILE")]
    scan_config: Option<PathBuf>,

    /// Number of iterations for benchmarking
    #[clap(long, default_value = "1")]
    iterations: usize,

    /// Number of rows per record batch. Flat-prune requires a matching runtime batch cap.
    #[clap(long, value_parser = parse_batch_size)]
    batch_size: Option<usize>,

    /// Path type for the region: bare, data, metadata
    #[clap(long, default_value = "bare")]
    path_type: String,

    /// Verbose output
    #[clap(short, long, default_value_t = false)]
    verbose: bool,

    /// Output pprof flamegraph
    #[clap(long, value_name = "FILE")]
    pprof_file: Option<PathBuf>,

    /// Start pprof after the first iteration (use first iteration as warmup).
    #[clap(long, default_value_t = false)]
    pprof_after_warmup: bool,

    /// Read the `__primary_key` column as BinaryArray instead of DictionaryArray.
    /// Only affects the `direct` reader.
    #[clap(long, default_value_t = false)]
    pk_as_binary: bool,

    /// Incremental bounded observations as JSONL (new file).
    #[clap(long, value_name = "FILE")]
    research_file: Option<PathBuf>,

    /// Inventory referenced encoded keys and decoded source-schema tags.
    #[clap(long, requires = "research_file")]
    inventory_keys: bool,

    /// JSON list of local SST paths; inventory keys across all files in one process.
    #[clap(long, requires = "inventory_keys", conflicts_with_all = ["file_path", "file_id"])]
    inventory_key_files: Option<PathBuf>,

    /// Reader implementation to benchmark.
    #[clap(long, value_enum, default_value = "direct")]
    reader: ReaderMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ReaderMode {
    /// Read directly via the push-decoder parquet stream.
    Direct,
    /// Read via ParquetReaderBuilder, FileRange, and FlatPruneReader.
    FlatPrune,
}

#[derive(Debug, Deserialize, Default)]
struct ParquetScanConfig {
    projection_names: Option<Vec<String>>,
    row_groups: Option<Vec<usize>>,
}

#[derive(Debug, Default, Clone)]
struct IterationStats {
    rows: usize,
    record_batches: usize,
    /// Number of columns in the output record batches.
    columns: usize,
    /// Schema of the output record batches, captured from the first batch.
    schema: Option<SchemaRef>,
    elapsed: Duration,
}

#[derive(Debug)]
enum ParquetbenchInput {
    LocalFile {
        file_path: PathBuf,
    },
    Region {
        config: PathBuf,
        region_id: String,
        table_dir: String,
        file_id: String,
        path_type: PathType,
    },
}

struct ParquetbenchSource {
    object_store: object_store::ObjectStore,
    file_path: String,
    display_path: String,
    region_id_label: String,
    region_id: RegionId,
    file_id: FileId,
    region_file_id: RegionFileId,
    path_type: Option<PathType>,
    table_dir: Option<String>,
}

impl ParquetbenchCommand {
    pub async fn run(&self) -> error::Result<()> {
        let trace = ResearchTrace::open(
            self.research_file.as_deref(),
            json!({
                "tool": "parquetbench", "file_path": self.file_path, "file_id": self.file_id,
                "config": self.config.as_ref().and_then(|p| std::fs::read_to_string(p).ok()),
                "requested_batch_size": self.batch_size, "runtime_batch_cap": mito2::sst::parquet::read_batch_size(),
                "reader": format!("{:?}", self.reader), "cache": "disabled",
                "scan_config": self.scan_config.as_ref().and_then(|p| std::fs::read_to_string(p).ok())
            }),
        )?;
        if self.inventory_keys && self.reader != ReaderMode::Direct {
            return error::IllegalConfigSnafu {
                msg: "key inventory requires direct reader".to_string(),
            }
            .fail();
        }
        let mut keys = KeyInventory::default();
        if let Some(list) = &self.inventory_key_files {
            let content = std::fs::read_to_string(list)
                .map_err(|e| error::IllegalConfigSnafu { msg: e.to_string() }.build())?;
            let paths: Vec<PathBuf> =
                serde_json::from_str(&content).context(error::SerdeJsonSnafu)?;
            for path in paths {
                let mut command = self.clone();
                command.file_path = Some(path);
                command.run_file(&trace, &mut keys).await?;
            }
        } else {
            self.run_file(&trace, &mut keys).await?;
        }
        observe(
            &trace,
            "key_inventory_summary",
            json!({"unique_encoded_keys": keys.keys.len(),
            "unique_series_identities": (keys.unknown_identity_keys == 0).then_some(keys.identities.len()),
            "unknown_identity_encoded_keys": keys.unknown_identity_keys, "referenced_rows": keys.rows,
            "encoded_key_bytes": keys.keys.iter().map(|k| k.len()).sum::<usize>(),
            "encoded_key_capacity_bytes": keys.keys.iter().map(|k| k.capacity()).sum::<usize>(),
            "key_set_capacity": keys.keys.capacity(), "identity_set_capacity": keys.identities.capacity(),
            "inventory_own_structures": true, "process": process_memory()}),
        );
        drop(keys);
        observe(&trace, "key_inventory_released", process_memory());
        if let Some(trace) = trace {
            trace.finish()?;
        }
        Ok(())
    }

    async fn run_file(&self, trace: &Trace, keys: &mut KeyInventory) -> error::Result<()> {
        let batch_size = self.batch_size.unwrap_or_else(|| {
            if self.reader == ReaderMode::Direct {
                DEFAULT_READ_BATCH_SIZE
            } else {
                mito2::sst::parquet::read_batch_size()
            }
        });
        observe(
            trace,
            "effective_batch_size",
            json!({"requested": self.batch_size, "effective": batch_size}),
        );
        if self.verbose {
            common_telemetry::init_default_ut_logging();
        }

        println!("{}", "Starting parquetbench...".cyan().bold());

        if self.iterations <= 1 && self.pprof_after_warmup && self.pprof_file.is_some() {
            return error::IllegalConfigSnafu {
                msg: "pprof-after-warmup requires at least 2 iterations (1 warmup + 1 profiled)"
                    .to_string(),
            }
            .fail();
        }

        let input = self.resolve_input()?;
        let mut source = build_source(input, trace.is_some()).await?;

        let file_size = source
            .object_store
            .stat(&source.file_path)
            .await
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("stat failed for {}: {}", source.display_path, e),
                }
                .build()
            })?
            .content_length();
        let mut metadata_metrics = MetadataCacheMetrics::default();
        let parquet_meta =
            MetadataLoader::new(source.object_store.clone(), &source.file_path, file_size)
                .load(&mut metadata_metrics)
                .await
                .map_err(|e| {
                    error::IllegalConfigSnafu {
                        msg: format!(
                            "read parquet metadata failed for {}: {:?}",
                            source.display_path, e
                        ),
                    }
                    .build()
                })?;
        observe(
            trace,
            "footer_loaded",
            json!({"file": source.display_path,
            "metadata_memory_size": parquet_meta.memory_size(), "process": process_memory()}),
        );
        let region_meta = extract_region_metadata(&source.display_path, &parquet_meta)?;
        if source.table_dir.is_none() {
            source.region_id = region_meta.region_id;
            source.region_id_label = source.region_id.as_u64().to_string();
            source.region_file_id = RegionFileId::new(source.region_id, source.file_id);
        }
        let scan_config = self.load_scan_config().await?;
        observe(
            trace,
            "source_schema",
            json!({"file": source.display_path, "schema": region_meta}),
        );
        let key_codec = build_primary_key_codec(&region_meta);
        let projection = if self.reader == ReaderMode::Direct {
            resolve_projection_names(&scan_config, &region_meta)?
        } else {
            None
        };
        let projection_column_ids = if self.reader == ReaderMode::FlatPrune {
            resolve_projection_column_ids(&scan_config, &region_meta)?
        } else {
            None
        };
        let row_groups = resolve_row_groups(&scan_config, parquet_meta.num_row_groups())?;
        let read_all_row_groups = scan_config.row_groups.is_none();
        let scanned_bytes = scanned_row_group_bytes(&parquet_meta, &row_groups);
        let projected_columns = projection_names_display(&scan_config);
        let row_groups_display = row_groups_display(&row_groups, parquet_meta.num_row_groups());
        let mut sst_schema = to_flat_sst_arrow_schema(
            &region_meta,
            &FlatSchemaOptions::from_encoding(region_meta.primary_key_encoding),
        );
        if self.pk_as_binary {
            sst_schema = override_pk_to_binary(&sst_schema);
        }

        println!(
            "{} Reader: {}",
            "✓".green(),
            match self.reader {
                ReaderMode::Direct => "direct",
                ReaderMode::FlatPrune => "flat-prune",
            }
            .cyan()
        );
        println!(
            "{} Region ID: {} (u64: {})",
            "✓".green(),
            source.region_id_label,
            source.region_id.as_u64()
        );
        println!("{} File path: {}", "✓".green(), source.display_path.cyan());
        println!(
            "{} Columns: {}",
            "✓".green(),
            projected_columns.as_deref().unwrap_or("all columns").cyan()
        );
        println!("{} Row groups: {}", "✓".green(), row_groups_display.cyan());
        if !read_all_row_groups {
            println!(
                "{} Scanned bytes (selected row groups): {}",
                "✓".green(),
                format_bytes(scanned_bytes).cyan()
            );
        }
        println!(
            "{} __primary_key type: {}",
            "✓".green(),
            if self.pk_as_binary {
                "Binary"
            } else {
                "Dictionary(UInt32, Binary)"
            }
            .cyan()
        );
        println!("{} Batch size: {}", "✓".green(), batch_size);
        if self.reader == ReaderMode::FlatPrune
            && batch_size > mito2::sst::parquet::read_batch_size()
        {
            return error::IllegalConfigSnafu {
                msg: "flat-prune batch exceeds GREPTIME_MITO_READ_BATCH_SIZE; set it before starting a fresh process".to_string(),
            }.fail();
        }
        println!(
            "{} Parquet rows: {}, row groups: {}, file size: {}",
            "✓".green(),
            parquet_meta.file_metadata().num_rows(),
            parquet_meta.num_row_groups(),
            format_bytes(file_size)
        );
        println!(
            "{} Metadata reads: {}, bytes: {}",
            "✓".green(),
            metadata_metrics.num_reads,
            format_bytes(metadata_metrics.bytes_read)
        );

        #[cfg(unix)]
        let mut profiler_guard = if self.pprof_file.is_some() && !self.pprof_after_warmup {
            println!("{} Starting profiling...", "⚡".yellow());
            Some(
                pprof::ProfilerGuardBuilder::default()
                    .frequency(99)
                    .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                    .build()
                    .map_err(|e| {
                        error::IllegalConfigSnafu {
                            msg: format!("Failed to start profiler: {e}"),
                        }
                        .build()
                    })?,
            )
        } else {
            None
        };

        #[cfg(not(unix))]
        if self.pprof_file.is_some() {
            eprintln!(
                "{}: Profiling is not supported on this platform",
                "Warning".yellow()
            );
        }

        let mut total_elapsed_all = Duration::ZERO;
        let mut total_rows_all = 0usize;
        let mut total_batches_all = 0usize;
        let mut schema_printed = false;
        let file_handle = FileHandle::new(
            FileMeta {
                region_id: source.region_id,
                file_id: source.file_id,
                time_range: Default::default(),
                level: 0,
                file_size,
                max_row_group_uncompressed_size: 0,
                available_indexes: Default::default(),
                indexes: Default::default(),
                index_file_size: 0,
                index_version: 0,
                num_rows: parquet_meta.file_metadata().num_rows() as u64,
                num_row_groups: parquet_meta.num_row_groups() as u64,
                sequence: None,
                partition_expr: None,
                num_series: 0,
                primary_key_min: None,
                primary_key_max: None,
                preserve_row_sequence: false,
            },
            Arc::new(NoopFilePurger),
        );

        for iteration in 0..self.iterations {
            let stats = match self.reader {
                ReaderMode::Direct => {
                    run_direct_iteration(
                        source.object_store.clone(),
                        source.file_path.clone(),
                        source.region_file_id,
                        parquet_meta.clone(),
                        projection.clone(),
                        row_groups.clone(),
                        sst_schema.clone(),
                        batch_size,
                        trace,
                        keys,
                        self.inventory_keys.then_some(key_codec.clone()),
                    )
                    .await?
                }
                ReaderMode::FlatPrune => {
                    run_flat_prune_iteration(
                        source.object_store.clone(),
                        source.table_dir.clone().ok_or_else(|| {
                            error::IllegalConfigSnafu {
                                msg: "flat-prune reader requires --table-dir".to_string(),
                            }
                            .build()
                        })?,
                        source.path_type.ok_or_else(|| {
                            error::IllegalConfigSnafu {
                                msg: "flat-prune reader requires --path-type".to_string(),
                            }
                            .build()
                        })?,
                        file_handle.clone(),
                        region_meta.clone(),
                        projection_column_ids.clone(),
                        row_groups.clone(),
                        read_all_row_groups,
                        batch_size,
                        trace,
                    )
                    .await?
                }
            };

            total_elapsed_all += stats.elapsed;
            total_rows_all += stats.rows;
            total_batches_all += stats.record_batches;

            if !schema_printed && let Some(schema) = &stats.schema {
                println!(
                    "{} Output schema ({} columns):",
                    "✓".green(),
                    schema.fields().len()
                );
                for field in schema.fields() {
                    println!("    - {}: {}", field.name().cyan(), field.data_type());
                }
                schema_printed = true;
            }

            println!(
                "  Iteration {}: {} rows, {} columns, {} record batches in {:?} ({}/s, {}/s)",
                iteration + 1,
                stats.rows,
                stats.columns,
                stats.record_batches,
                stats.elapsed,
                format_rate(stats.rows as f64 / stats.elapsed.as_secs_f64()),
                format_bytes_per_sec(scanned_bytes as f64 / stats.elapsed.as_secs_f64()),
            );

            #[cfg(unix)]
            if iteration == 0 && self.pprof_after_warmup && self.pprof_file.is_some() {
                println!("{} Starting profiling after warmup...", "⚡".yellow());
                profiler_guard = Some(
                    pprof::ProfilerGuardBuilder::default()
                        .frequency(99)
                        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                        .build()
                        .map_err(|e| {
                            error::IllegalConfigSnafu {
                                msg: format!("Failed to start profiler: {e}"),
                            }
                            .build()
                        })?,
                );
            }
        }

        #[cfg(unix)]
        if let (Some(guard), Some(pprof_file)) = (profiler_guard, &self.pprof_file) {
            println!("{} Generating flamegraph...", "🔥".yellow());
            match guard.report().build() {
                Ok(report) => {
                    let mut flamegraph_data = Vec::new();
                    if let Err(e) = report.flamegraph(&mut flamegraph_data) {
                        println!("{}: Failed to generate flamegraph: {}", "Error".red(), e);
                    } else if let Err(e) = std::fs::write(pprof_file, flamegraph_data) {
                        println!(
                            "{}: Failed to write flamegraph to {}: {}",
                            "Error".red(),
                            pprof_file.display(),
                            e
                        );
                    } else {
                        println!(
                            "{} Flamegraph saved to {}",
                            "✓".green(),
                            pprof_file.display().to_string().cyan()
                        );
                    }
                }
                Err(e) => {
                    println!("{}: Failed to generate pprof report: {}", "Error".red(), e);
                }
            }
        }

        if self.iterations > 1 {
            let avg_elapsed = total_elapsed_all / self.iterations as u32;
            let avg_rows = total_rows_all / self.iterations;
            let avg_batches = total_batches_all / self.iterations;
            println!(
                "\n{} Average: {} rows, {} record batches in {:?} over {} iterations",
                "ℹ".blue(),
                avg_rows,
                avg_batches,
                avg_elapsed,
                self.iterations
            );
        }

        println!("\n{}", "Benchmark completed!".green().bold());
        Ok(())
    }

    fn resolve_input(&self) -> error::Result<ParquetbenchInput> {
        let has_region_args = self.config.is_some()
            || self.region_id.is_some()
            || self.table_dir.is_some()
            || self.file_id.is_some();

        if let Some(file_path) = &self.file_path {
            if self.reader == ReaderMode::FlatPrune {
                return Err(error::IllegalConfigSnafu {
                    msg: "--file-path currently supports only --reader direct".to_string(),
                }
                .build());
            }
            if has_region_args {
                return Err(error::IllegalConfigSnafu {
                    msg: "--file-path cannot be used with --config, --region-id, --table-dir, or --file-id".to_string(),
                }
                .build());
            }
            return Ok(ParquetbenchInput::LocalFile {
                file_path: file_path.clone(),
            });
        }

        let config = self.config.clone().ok_or_else(|| {
            error::IllegalConfigSnafu {
                msg: "missing --config unless --file-path is specified".to_string(),
            }
            .build()
        })?;
        let region_id = self.region_id.clone().ok_or_else(|| {
            error::IllegalConfigSnafu {
                msg: "missing --region-id unless --file-path is specified".to_string(),
            }
            .build()
        })?;
        let table_dir = self.table_dir.clone().ok_or_else(|| {
            error::IllegalConfigSnafu {
                msg: "missing --table-dir unless --file-path is specified".to_string(),
            }
            .build()
        })?;
        let file_id = self.file_id.clone().ok_or_else(|| {
            error::IllegalConfigSnafu {
                msg: "missing --file-id unless --file-path is specified".to_string(),
            }
            .build()
        })?;
        let path_type = parse_path_type(&self.path_type)?;

        Ok(ParquetbenchInput::Region {
            config,
            region_id,
            table_dir,
            file_id,
            path_type,
        })
    }

    async fn load_scan_config(&self) -> error::Result<ParquetScanConfig> {
        if let Some(path) = &self.scan_config {
            let content = tokio::fs::read_to_string(path)
                .await
                .context(error::FileIoSnafu)?;
            serde_json::from_str::<ParquetScanConfig>(&content).context(error::SerdeJsonSnafu)
        } else {
            Ok(ParquetScanConfig::default())
        }
    }
}

async fn build_source(
    input: ParquetbenchInput,
    research: bool,
) -> error::Result<ParquetbenchSource> {
    match input {
        ParquetbenchInput::LocalFile { file_path } => build_local_file_source(&file_path),
        ParquetbenchInput::Region {
            config,
            region_id,
            table_dir,
            file_id,
            path_type,
        } => {
            build_region_source(
                &config, &region_id, table_dir, &file_id, path_type, research,
            )
            .await
        }
    }
}

fn build_local_file_source(file_path: &Path) -> error::Result<ParquetbenchSource> {
    let file_path = std::fs::canonicalize(file_path).map_err(|e| {
        error::IllegalConfigSnafu {
            msg: format!("invalid --file-path {}: {e}", file_path.display()),
        }
        .build()
    })?;
    if !file_path.is_file() {
        return Err(error::IllegalConfigSnafu {
            msg: format!("--file-path {} is not a file", file_path.display()),
        }
        .build());
    }
    let parent = file_path.parent().ok_or_else(|| {
        error::IllegalConfigSnafu {
            msg: format!(
                "--file-path {} has no parent directory",
                file_path.display()
            ),
        }
        .build()
    })?;
    let file_name = file_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            error::IllegalConfigSnafu {
                msg: format!("invalid UTF-8 file name in {}", file_path.display()),
            }
            .build()
        })?
        .to_string();
    let object_store = object_store::ObjectStore::new(object_store::services::Fs::default().root(
        parent.to_str().ok_or_else(|| {
            error::IllegalConfigSnafu {
                msg: format!("invalid UTF-8 parent directory in {}", file_path.display()),
            }
            .build()
        })?,
    ))
    .map_err(|e| {
        error::IllegalConfigSnafu {
            msg: format!("failed to build local file object store: {e:?}"),
        }
        .build()
    })?;

    let file_id = file_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| FileId::parse_str(stem).ok())
        .unwrap_or_else(FileId::random);
    let region_id = RegionId::new(0, 0);
    let region_file_id = RegionFileId::new(region_id, file_id);
    let display_path = file_path.display().to_string();

    Ok(ParquetbenchSource {
        object_store,
        file_path: file_name,
        display_path,
        region_id_label: region_id.as_u64().to_string(),
        region_id,
        file_id,
        region_file_id,
        path_type: None,
        table_dir: None,
    })
}

async fn build_region_source(
    config: &Path,
    region_id: &str,
    table_dir: String,
    file_id: &str,
    path_type: PathType,
    research: bool,
) -> error::Result<ParquetbenchSource> {
    let region = parse_region_id(region_id)?;
    let file_id = parse_file_id(file_id)?;
    let region_file_id = RegionFileId::new(region, file_id);
    let file_path = sst_file_path(&table_dir, region_file_id, path_type);

    let (store_cfg, _mito_config, _wal_config) = parse_config(config)?;
    let object_store = if research {
        build_research_object_store(&store_cfg)?
    } else {
        build_object_store(&store_cfg).await?
    };

    Ok(ParquetbenchSource {
        object_store,
        display_path: file_path.clone(),
        file_path,
        region_id_label: region_id.to_string(),
        region_id: region,
        file_id,
        region_file_id,
        path_type: Some(path_type),
        table_dir: Some(table_dir),
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_direct_iteration(
    object_store: object_store::ObjectStore,
    file_path: String,
    region_file_id: RegionFileId,
    parquet_meta: parquet::file::metadata::ParquetMetaData,
    projection: Option<Vec<usize>>,
    row_groups: Vec<usize>,
    sst_schema: SchemaRef,
    batch_size: usize,
    trace: &Trace,
    keys: &mut KeyInventory,
    codec: Option<Arc<dyn PrimaryKeyCodec>>,
) -> error::Result<IterationStats> {
    let parquet_meta = Arc::new(parquet_meta);
    let arrow_metadata = ArrowReaderMetadata::try_new(
        parquet_meta.clone(),
        ArrowReaderOptions::new().with_schema(sst_schema),
    )
    .map_err(|e| {
        error::IllegalConfigSnafu {
            msg: format!(
                "Failed to build parquet arrow metadata for {}: {}",
                file_path, e
            ),
        }
        .build()
    })?;
    let projection_mask = match projection.as_ref() {
        Some(projection) => {
            ProjectionMask::roots(arrow_metadata.parquet_schema(), projection.iter().copied())
        }
        None => ProjectionMask::all(),
    };
    let start = Instant::now();
    let mut stats = IterationStats::default();
    observe(
        trace,
        "arrow_metadata_constructed",
        json!({"file": file_path, "process": process_memory()}),
    );
    for row_group_idx in row_groups {
        observe(
            trace,
            "reader_construction",
            json!({"file": file_path, "row_group": row_group_idx, "process": process_memory()}),
        );
        let fetcher = SstParquetRangeFetcher::new(
            region_file_id,
            file_path.clone(),
            object_store.clone(),
            CacheStrategy::Disabled,
            row_group_idx,
            None,
        );
        let mut stream = build_sst_parquet_record_batch_stream(
            arrow_metadata.clone(),
            row_group_idx,
            None,
            projection_mask.clone(),
            fetcher,
            file_path.clone(),
            batch_size,
        )
        .map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!(
                    "Failed to build parquet record batch stream for {}: {e:?}",
                    file_path
                ),
            }
            .build()
        })?;
        let mut first_batch = true;
        while let Some(batch) = stream.next().await.transpose().map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("Failed to scan parquet file {}: {e:?}", file_path),
            }
            .build()
        })? {
            if first_batch {
                observe(
                    trace,
                    "reader_first_batch",
                    json!({"file": file_path, "row_group": row_group_idx,
                    "rows": batch.num_rows(), "retained_batch_bytes": mito2::read::retained_batch_buffer_size(std::slice::from_ref(&batch)), "process": process_memory()}),
                );
                first_batch = false;
            }
            if let Some(trace) = trace {
                trace.batch(
                    &batch,
                    json!({"file": file_path, "row_group": row_group_idx}),
                );
            }
            if let Some(codec) = &codec {
                let mut inventory = std::mem::take(keys);
                let codec = codec.clone();
                let batch = batch.clone();
                let trace = trace.clone();
                let source = file_path.clone();
                inventory = tokio::task::spawn_blocking(move || {
                    inventory.observe(&batch, codec.as_ref(), &trace, &source)?;
                    Ok::<_, error::Error>(inventory)
                })
                .await
                .map_err(|e| error::IllegalConfigSnafu { msg: e.to_string() }.build())??;
                *keys = inventory;
            }
            stats.rows += batch.num_rows();
            stats.record_batches += 1;
            stats.columns = batch.num_columns();
            if stats.schema.is_none() {
                stats.schema = Some(batch.schema());
            }
        }
        drop(stream);
        observe(
            trace,
            "reader_released",
            json!({"file": file_path, "row_group": row_group_idx, "process": process_memory()}),
        );
    }
    stats.elapsed = start.elapsed();
    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
async fn run_flat_prune_iteration(
    object_store: object_store::ObjectStore,
    table_dir: String,
    path_type: PathType,
    file_handle: FileHandle,
    region_meta: RegionMetadataRef,
    projection: Option<Vec<ColumnId>>,
    row_groups: Vec<usize>,
    read_all_row_groups: bool,
    batch_size: usize,
    trace: &Trace,
) -> error::Result<IterationStats> {
    let file_id = file_handle.file_id().to_string();
    let reader_builder = ParquetReaderBuilder::new(table_dir, path_type, file_handle, object_store)
        .batch_size(batch_size)
        .expected_metadata(Some(region_meta))
        .cache(CacheStrategy::Disabled)
        .projection(projection.map(ReadColumns::new));
    let mut reader_metrics = ReaderMetrics::default();
    let start = Instant::now();
    let mut stats = IterationStats::default();
    let Some((context, selection)) = reader_builder
        .build_reader_input(&mut reader_metrics)
        .await
        .map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("build flat prune reader input failed: {e:?}"),
            }
            .build()
        })?
    else {
        stats.elapsed = start.elapsed();
        return Ok(stats);
    };

    let range_builder = FileRangeBuilder::new(Arc::new(context), selection);
    let mut ranges = SmallVec::new();
    if read_all_row_groups {
        range_builder.build_ranges(-1, &mut ranges);
    } else {
        for row_group_idx in row_groups {
            range_builder.build_ranges(row_group_idx as i64, &mut ranges);
        }
    }

    for (range_index, range) in ranges.into_iter().enumerate() {
        observe(
            trace,
            "reader_construction",
            json!({"range": range_index, "process": process_memory()}),
        );
        let row_group = range.row_group_index();
        let Some(mut reader) = range.flat_reader(None, None).await.map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("build flat prune reader failed: {e:?}"),
            }
            .build()
        })?
        else {
            continue;
        };
        let mut first_batch = true;
        while let Some(batch) = reader.next_batch().await.map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("scan flat prune reader failed: {e:?}"),
            }
            .build()
        })? {
            if first_batch {
                observe(
                    trace,
                    "reader_first_batch",
                    json!({"file": file_id, "row_group": row_group,
                    "rows": batch.num_rows(), "retained_batch_bytes": mito2::read::retained_batch_buffer_size(std::slice::from_ref(&batch)), "process": process_memory()}),
                );
                first_batch = false;
            }
            if let Some(trace) = trace {
                trace.batch(
                    &batch,
                    json!({"range": range_index, "file": file_id, "row_group": row_group}),
                );
            }
            stats.rows += batch.num_rows();
            stats.record_batches += 1;
            stats.columns = batch.num_columns();
            if stats.schema.is_none() {
                stats.schema = Some(batch.schema());
            }
        }
        drop(reader);
        observe(
            trace,
            "reader_released",
            json!({"file": file_id, "row_group": row_group, "process": process_memory()}),
        );
    }

    stats.elapsed = start.elapsed();
    Ok(stats)
}

fn resolve_projection_names(
    scan_config: &ParquetScanConfig,
    metadata: &RegionMetadata,
) -> error::Result<Option<Vec<usize>>> {
    let Some(projection_names) = &scan_config.projection_names else {
        return Ok(None);
    };

    let sst_schema = to_flat_sst_arrow_schema(
        metadata,
        &FlatSchemaOptions::from_encoding(metadata.primary_key_encoding),
    );
    let available_columns = sst_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let projection = projection_names
        .iter()
        .map(|name| {
            sst_schema
                .column_with_name(name)
                .map(|x| x.0)
                .ok_or_else(|| {
                    error::IllegalConfigSnafu {
                        msg: format!(
                            "Unknown column '{}' in projection_names, available columns: [{}]",
                            name, available_columns
                        ),
                    }
                    .build()
                })
        })
        .collect::<error::Result<Vec<_>>>()?;
    Ok(Some(projection))
}

fn resolve_projection_column_ids(
    scan_config: &ParquetScanConfig,
    metadata: &RegionMetadata,
) -> error::Result<Option<Vec<ColumnId>>> {
    let Some(projection_names) = &scan_config.projection_names else {
        return Ok(None);
    };

    let available_columns = metadata
        .column_metadatas
        .iter()
        .map(|column| column.column_schema.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let projection = projection_names
        .iter()
        .filter_map(|name| {
            if is_internal_column(name) {
                return None;
            }
            Some(
                metadata
                    .column_metadatas
                    .iter()
                    .find(|column| column.column_schema.name == *name)
                    .map(|column| column.column_id)
                    .ok_or_else(|| {
                        error::IllegalConfigSnafu {
                            msg: format!(
                                "Unknown column '{}' in projection_names, available columns: [{}]",
                                name, available_columns
                            ),
                        }
                        .build()
                    }),
            )
        })
        .collect::<error::Result<Vec<_>>>()?;
    Ok(Some(projection))
}

fn resolve_row_groups(
    scan_config: &ParquetScanConfig,
    num_row_groups: usize,
) -> error::Result<Vec<usize>> {
    match &scan_config.row_groups {
        Some(row_groups) => {
            for row_group_idx in row_groups {
                if *row_group_idx >= num_row_groups {
                    return Err(error::IllegalConfigSnafu {
                        msg: format!(
                            "Invalid row group {} in row_groups, parquet file has row groups [0, {})",
                            row_group_idx, num_row_groups
                        ),
                    }
                    .build());
                }
            }
            Ok(row_groups.clone())
        }
        None => Ok((0..num_row_groups).collect()),
    }
}

/// Sums the compressed byte size of the selected row groups, used as the denominator for
/// scan throughput so partial-row-group benchmarks aren't measured against the whole file.
fn scanned_row_group_bytes(
    parquet_meta: &parquet::file::metadata::ParquetMetaData,
    row_groups: &[usize],
) -> u64 {
    row_groups
        .iter()
        .map(|&idx| parquet_meta.row_group(idx).compressed_size() as u64)
        .sum()
}

fn projection_names_display(scan_config: &ParquetScanConfig) -> Option<String> {
    scan_config
        .projection_names
        .as_ref()
        .map(|cols| cols.join(", "))
}

fn row_groups_display(row_groups: &[usize], total_row_groups: usize) -> String {
    if row_groups.len() == total_row_groups {
        "all row groups".to_string()
    } else {
        row_groups
            .iter()
            .map(|idx| idx.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn override_pk_to_binary(schema: &SchemaRef) -> SchemaRef {
    let new_fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| {
            if f.name() == PRIMARY_KEY_COLUMN_NAME {
                Arc::new(Field::new(
                    PRIMARY_KEY_COLUMN_NAME,
                    ArrowDataType::Binary,
                    f.is_nullable(),
                ))
            } else {
                f.clone()
            }
        })
        .collect();
    Arc::new(Schema::new(new_fields))
}

fn parse_batch_size(s: &str) -> Result<usize, String> {
    let batch_size = s
        .parse::<usize>()
        .map_err(|e| format!("invalid batch size '{s}': {e}"))?;
    if batch_size == 0 {
        return Err("batch size must be greater than 0".to_string());
    }
    Ok(batch_size)
}

fn format_rate(rate: f64) -> String {
    if !rate.is_finite() {
        return "inf rows".to_string();
    }
    format!("{rate:.2} rows")
}

fn format_bytes_per_sec(bytes_per_sec: f64) -> String {
    if !bytes_per_sec.is_finite() {
        return "inf B/s".to_string();
    }
    format!("{}/s", format_bytes(bytes_per_sec as u64))
}

/// Retains one encoded key per variant and one pair per actual series identity.
#[derive(Default)]
struct KeyInventory {
    keys: HashSet<Vec<u8>>,
    identities: HashSet<(u32, u64)>,
    unknown_identity_keys: usize,
    rows: u64,
}

impl KeyInventory {
    fn observe(
        &mut self,
        batch: &RecordBatch,
        codec: &dyn PrimaryKeyCodec,
        trace: &Trace,
        source: &str,
    ) -> error::Result<()> {
        let array = batch
            .column_by_name(PRIMARY_KEY_COLUMN_NAME)
            .ok_or_else(|| {
                error::IllegalConfigSnafu {
                    msg: "key inventory projection must include __primary_key".to_string(),
                }
                .build()
            })?;
        self.rows += batch.num_rows() as u64;
        let mut referenced = HashSet::new();
        let mut add = |key: &[u8]| -> error::Result<()> {
            if self.keys.contains(key) {
                return Ok(());
            }
            let decoded = codec.decode(key).map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("decode source key: {e}"),
                }
                .build()
            })?;
            let values = match decoded {
                CompositeValues::Dense(values) => values,
                CompositeValues::Sparse(values) => values
                    .iter()
                    .map(|(id, value)| (*id, value.clone()))
                    .collect(),
            };
            let ids = SparsePrimaryKeyCodec::with_fields(Vec::new())
                .decode_ids(key)
                .ok();
            if let Some(ids) = ids {
                self.identities.insert(ids);
            } else {
                self.unknown_identity_keys += 1;
            }
            if let Some(trace) = trace {
                trace.observe_required("unique_key", json!({
                "source": source, "encoded_key_hex": key.iter().map(|b| format!("{b:02x}")).collect::<String>(),
                "encoded_bytes": key.len(), "series_identity": ids, "decoded_source_values": values
            }))?;
            }
            self.keys.insert(key.to_vec());
            Ok(())
        };
        if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    error::IllegalConfigSnafu {
                        msg: "key dictionary values must be binary".to_string(),
                    }
                    .build()
                })?;
            let mut previous = None;
            for index in dict.keys().iter().flatten() {
                if previous != Some(index) {
                    referenced.insert(index as usize);
                    previous = Some(index);
                }
            }
            for index in &referenced {
                if !values.is_null(*index) {
                    add(values.value(*index))?;
                }
            }
            observe(
                trace,
                "key_dictionary",
                json!({"source": source, "dictionary_entries": values.len(),
                "used_entries": referenced.len(), "value_array_memory_bytes": values.get_array_memory_size(),
                "index_array_memory_bytes": dict.keys().get_array_memory_size(), "rows": batch.num_rows()}),
            );
        } else if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
            for key in values.iter().flatten() {
                add(key)?;
            }
        } else {
            return error::IllegalConfigSnafu {
                msg: "key inventory needs binary or UInt32 dictionary keys".to_string(),
            }
            .fail();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use serde_json::json;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::ColumnSchema;

    use super::*;

    fn test_command() -> ParquetbenchCommand {
        ParquetbenchCommand {
            config: None,
            region_id: None,
            table_dir: None,
            file_id: None,
            file_path: None,
            scan_config: None,
            iterations: 1,
            batch_size: Some(DEFAULT_READ_BATCH_SIZE),
            path_type: "bare".to_string(),
            verbose: false,
            pprof_file: None,
            pprof_after_warmup: false,
            pk_as_binary: false,
            reader: ReaderMode::Direct,
            research_file: None,
            inventory_keys: false,
            inventory_key_files: None,
        }
    }

    #[test]
    fn test_referenced_keys_exclude_unused_dictionary_values() {
        use datatypes::arrow::array::UInt32Array;
        let codec = SparsePrimaryKeyCodec::with_fields(Vec::new());
        let keys: Vec<Vec<u8>> = (1..=3)
            .map(|tsid| {
                let mut key = Vec::new();
                codec.encode_internal(1071, tsid, &mut key).unwrap();
                key
            })
            .collect();
        let values = Arc::new(BinaryArray::from(
            keys.iter().map(Vec::as_slice).collect::<Vec<_>>(),
        ));
        let dictionary =
            DictionaryArray::<UInt32Type>::try_new(UInt32Array::from(vec![0, 0, 1]), values)
                .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            PRIMARY_KEY_COLUMN_NAME,
            dictionary.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dictionary)]).unwrap();
        let mut inventory = KeyInventory::default();
        inventory.observe(&batch, &codec, &None, "fixture").unwrap();
        inventory.observe(&batch, &codec, &None, "fixture").unwrap();
        assert_eq!(inventory.rows, 6);
        assert_eq!(inventory.keys.len(), 2);
        assert_eq!(inventory.identities.len(), 2);
    }

    fn new_test_metadata() -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 0));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "host",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![1]);
        builder.build().unwrap()
    }

    #[test]
    fn test_resolve_input_accepts_direct_file_for_direct_reader() {
        let mut command = test_command();
        command.file_path = Some(PathBuf::from("/tmp/source.parquet"));

        let input = command.resolve_input().unwrap();
        match input {
            ParquetbenchInput::LocalFile { file_path } => {
                assert_eq!(file_path, PathBuf::from("/tmp/source.parquet"));
            }
            ParquetbenchInput::Region { .. } => panic!("expected local file input"),
        }
    }

    #[test]
    fn test_resolve_input_rejects_mixed_input_modes() {
        let mut command = test_command();
        command.file_path = Some(PathBuf::from("/tmp/source.parquet"));
        command.config = Some(PathBuf::from("config.toml"));

        let err = command.resolve_input().unwrap_err();
        assert!(err.to_string().contains("--file-path cannot be used with"));
    }

    #[test]
    fn test_parse_scan_config_projection_names() {
        let config: ParquetScanConfig =
            serde_json::from_value(json!({ "projection_names": ["host", "ts"] })).unwrap();
        assert_eq!(
            config.projection_names,
            Some(vec!["host".to_string(), "ts".to_string()])
        );
    }

    #[test]
    fn test_parse_scan_config_row_groups() {
        let config: ParquetScanConfig =
            serde_json::from_value(json!({ "row_groups": [0, 2, 4] })).unwrap();
        assert_eq!(config.row_groups, Some(vec![0, 2, 4]));
    }

    #[test]
    fn test_resolve_projection_names() {
        let metadata = new_test_metadata();
        let projection = resolve_projection_names(
            &ParquetScanConfig {
                projection_names: Some(vec!["cpu".to_string(), "host".to_string()]),
                row_groups: None,
            },
            &metadata,
        )
        .unwrap();
        assert_eq!(projection, Some(vec![1, 0]));
    }

    #[test]
    fn test_resolve_projection_column_ids() {
        let metadata = new_test_metadata();
        let projection = resolve_projection_column_ids(
            &ParquetScanConfig {
                projection_names: Some(vec!["cpu".to_string(), "host".to_string()]),
                row_groups: None,
            },
            &metadata,
        )
        .unwrap();
        assert_eq!(projection, Some(vec![2, 1]));
    }

    #[test]
    fn test_resolve_projection_column_ids_ignores_internal_columns() {
        let metadata = new_test_metadata();
        let projection = resolve_projection_column_ids(
            &ParquetScanConfig {
                projection_names: Some(vec![
                    "cpu".to_string(),
                    "__primary_key".to_string(),
                    "__sequence".to_string(),
                    "__op_type".to_string(),
                ]),
                row_groups: None,
            },
            &metadata,
        )
        .unwrap();
        assert_eq!(projection, Some(vec![2]));
    }

    #[test]
    fn test_resolve_projection_names_unknown() {
        let metadata = new_test_metadata();
        let err = resolve_projection_names(
            &ParquetScanConfig {
                projection_names: Some(vec!["memory".to_string()]),
                row_groups: None,
            },
            &metadata,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("projection_names"));
        assert!(msg.contains("host"));
        assert!(msg.contains("cpu"));
        assert!(msg.contains("ts"));
    }

    #[test]
    fn test_resolve_row_groups_all() {
        assert_eq!(
            resolve_row_groups(&ParquetScanConfig::default(), 3).unwrap(),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn test_resolve_row_groups_subset() {
        let config = ParquetScanConfig {
            projection_names: None,
            row_groups: Some(vec![2, 0]),
        };
        assert_eq!(resolve_row_groups(&config, 4).unwrap(), vec![2, 0]);
    }

    #[test]
    fn test_resolve_row_groups_invalid() {
        let config = ParquetScanConfig {
            projection_names: None,
            row_groups: Some(vec![3]),
        };
        let err = resolve_row_groups(&config, 3).unwrap_err();
        assert!(err.to_string().contains("Invalid row group 3"));
    }

    #[test]
    fn test_sst_file_path_resolution() {
        let file_id = FileId::parse_str("00020380-009c-426d-953e-b4e34c15af34").unwrap();
        let region_file_id = RegionFileId::new(RegionId::new(1024, 0), file_id);
        assert_eq!(
            sst_file_path("data/greptime/public/1024", region_file_id, PathType::Bare),
            "data/greptime/public/1024/1024_0000000000/00020380-009c-426d-953e-b4e34c15af34.parquet"
        );
    }
}
