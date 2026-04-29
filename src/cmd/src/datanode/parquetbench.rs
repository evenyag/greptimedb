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

use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::Parser;
use colored::Colorize;
use datatypes::arrow::array::DictionaryArray;
use datatypes::arrow::datatypes::{SchemaRef, UInt32Type};
use futures::StreamExt;
use mito2::cache::CacheStrategy;
use mito2::sst::file::RegionFileId;
use mito2::sst::location::sst_file_path;
use mito2::sst::parquet::async_reader::SstAsyncFileReader;
use mito2::sst::parquet::metadata::MetadataLoader;
use mito2::sst::parquet::reader::MetadataCacheMetrics;
use mito2::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use serde::Deserialize;
use snafu::ResultExt;
use store_api::metadata::RegionMetadata;
use store_api::region_request::PathType;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use store_api::storage::{FileId, RegionId};

use crate::datanode::objbench::{build_object_store, extract_region_metadata, parse_config};
use crate::error;

/// Parquet benchmark command - benchmarks scanning a single parquet SST directly.
#[derive(Debug, Parser)]
pub struct ParquetbenchCommand {
    /// Path to config TOML file (same format as standalone/datanode config)
    #[clap(long, value_name = "FILE")]
    config: PathBuf,

    /// Region ID: either numeric u64 (e.g. "4398046511104") or "table_id:region_num" (e.g. "1024:0")
    #[clap(long)]
    region_id: String,

    /// Table directory relative to data home (e.g. "data/greptime/public/1024/")
    #[clap(long)]
    table_dir: String,

    /// SST file id to benchmark.
    #[clap(long)]
    file_id: String,

    /// Path to scan request JSON config file (supports projection_names only)
    #[clap(long, value_name = "FILE")]
    scan_config: Option<PathBuf>,

    /// Number of iterations for benchmarking
    #[clap(long, default_value = "1")]
    iterations: usize,

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
}

#[derive(Debug, Deserialize, Default)]
struct ParquetScanConfig {
    projection_names: Option<Vec<String>>,
    row_groups: Option<Vec<usize>>,
}

#[derive(Debug, Default, Clone, Copy)]
struct IterationStats {
    rows: usize,
    record_batches: usize,
    elapsed: Duration,
}

impl ParquetbenchCommand {
    pub async fn run(&self) -> error::Result<()> {
        if self.verbose {
            common_telemetry::init_default_ut_logging();
        }

        println!("{}", "Starting parquetbench...".cyan().bold());

        let region_id = parse_region_id(&self.region_id)?;
        let path_type = parse_path_type(&self.path_type)?;
        let file_id = FileId::parse_str(&self.file_id).map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("invalid file_id '{}': {}", self.file_id, e),
            }
            .build()
        })?;
        let region_file_id = RegionFileId::new(region_id, file_id);
        let file_path = sst_file_path(&self.table_dir, region_file_id, path_type);

        let (store_cfg, _mito_config, _wal_config) = parse_config(&self.config)?;
        let object_store = build_object_store(&store_cfg).await?;

        let file_size = object_store
            .stat(&file_path)
            .await
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("stat failed for {}: {}", file_path, e),
                }
                .build()
            })?
            .content_length();
        let mut metadata_metrics = MetadataCacheMetrics::default();
        let parquet_meta = MetadataLoader::new(object_store.clone(), &file_path, file_size)
            .load(&mut metadata_metrics)
            .await
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("read parquet metadata failed for {}: {:?}", file_path, e),
                }
                .build()
            })?;
        let region_meta = extract_region_metadata(&file_path, &parquet_meta)?;
        let scan_config = self.load_scan_config().await?;
        let projection = resolve_projection_names(&scan_config, &region_meta)?;
        let row_groups = resolve_row_groups(&scan_config, parquet_meta.num_row_groups())?;
        let projected_columns = projection_names_display(&scan_config);
        let row_groups_display = row_groups_display(&row_groups, parquet_meta.num_row_groups());
        let sst_schema = to_flat_sst_arrow_schema(
            &region_meta,
            &FlatSchemaOptions::from_encoding(region_meta.primary_key_encoding),
        );

        println!(
            "{} Region ID: {} (u64: {})",
            "✓".green(),
            self.region_id,
            region_id.as_u64()
        );
        println!("{} File path: {}", "✓".green(), file_path.cyan());
        println!(
            "{} Columns: {}",
            "✓".green(),
            projected_columns.as_deref().unwrap_or("all columns").cyan()
        );
        println!("{} Row groups: {}", "✓".green(), row_groups_display.cyan());
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

        for iteration in 0..self.iterations {
            let stats = run_iteration(
                object_store.clone(),
                file_path.clone(),
                region_file_id,
                parquet_meta.clone(),
                projection.clone(),
                row_groups.clone(),
                sst_schema.clone(),
            )
            .await?;

            total_elapsed_all += stats.elapsed;
            total_rows_all += stats.rows;
            total_batches_all += stats.record_batches;

            println!(
                "  Iteration {}: {} rows, {} record batches in {:?} ({}/s, {}/s)",
                iteration + 1,
                stats.rows,
                stats.record_batches,
                stats.elapsed,
                format_rate(stats.rows as f64 / stats.elapsed.as_secs_f64()),
                format_bytes_per_sec(file_size as f64 / stats.elapsed.as_secs_f64()),
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

async fn run_iteration(
    object_store: object_store::ObjectStore,
    file_path: String,
    region_file_id: RegionFileId,
    parquet_meta: parquet::file::metadata::ParquetMetaData,
    projection: Option<Vec<usize>>,
    row_groups: Vec<usize>,
    sst_schema: SchemaRef,
) -> error::Result<IterationStats> {
    let parquet_meta = std::sync::Arc::new(parquet_meta);
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
    let start = Instant::now();
    let mut stats = IterationStats::default();
    for row_group_idx in row_groups {
        let async_reader = SstAsyncFileReader::new(
            region_file_id,
            file_path.clone(),
            object_store.clone(),
            CacheStrategy::Disabled,
            parquet_meta.clone(),
            row_group_idx,
        );
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
            async_reader,
            arrow_metadata.clone(),
        )
        .with_row_groups(vec![row_group_idx]);
        if let Some(projection) = projection.as_ref() {
            builder = builder.with_projection(ProjectionMask::roots(
                arrow_metadata.parquet_schema(),
                projection.iter().copied(),
            ));
        }
        let mut stream = builder.build().map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!(
                    "Failed to build parquet record batch stream for {}: {}",
                    file_path, e
                ),
            }
            .build()
        })?;
        while let Some(batch) = stream.next().await.transpose().map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("Failed to scan parquet file {}: {}", file_path, e),
            }
            .build()
        })? {
            if let Some((idx, _)) = batch.schema_ref().column_with_name(PRIMARY_KEY_COLUMN_NAME)
                && let Some(dict) = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
            {
                println!(
                    "  Batch {} {} dictionary values: {}",
                    stats.record_batches,
                    PRIMARY_KEY_COLUMN_NAME,
                    dict.values().len()
                );
            }
            stats.rows += batch.num_rows();
            stats.record_batches += 1;
        }
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

fn parse_region_id(s: &str) -> error::Result<RegionId> {
    if s.contains(':') {
        let parts: Vec<&str> = s.splitn(2, ':').collect();
        let table_id: u32 = parts[0].parse().map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("invalid table_id in region_id '{}': {}", s, e),
            }
            .build()
        })?;
        let region_num: u32 = parts[1].parse().map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("invalid region_num in region_id '{}': {}", s, e),
            }
            .build()
        })?;
        Ok(RegionId::new(table_id, region_num))
    } else {
        let id: u64 = s.parse().map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("invalid region_id '{}': {}", s, e),
            }
            .build()
        })?;
        Ok(RegionId::from_u64(id))
    }
}

fn parse_path_type(s: &str) -> error::Result<PathType> {
    match s.to_lowercase().as_str() {
        "bare" => Ok(PathType::Bare),
        "data" => Ok(PathType::Data),
        "metadata" => Ok(PathType::Metadata),
        _ => Err(error::IllegalConfigSnafu {
            msg: format!("invalid path_type '{}', expected: bare, data, metadata", s),
        }
        .build()),
    }
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
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

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use serde_json::json;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::ColumnSchema;

    use super::*;

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
    fn test_parse_region_id() {
        assert_eq!(parse_region_id("1024:7").unwrap(), RegionId::new(1024, 7));
        assert_eq!(
            parse_region_id(&RegionId::new(1, 2).as_u64().to_string()).unwrap(),
            RegionId::new(1, 2)
        );
    }

    #[test]
    fn test_parse_path_type() {
        assert_eq!(parse_path_type("bare").unwrap(), PathType::Bare);
        assert_eq!(parse_path_type("data").unwrap(), PathType::Data);
        assert_eq!(parse_path_type("metadata").unwrap(), PathType::Metadata);
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
