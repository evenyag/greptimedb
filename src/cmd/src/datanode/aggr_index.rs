// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand, ValueEnum};
use colored::Colorize;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datafusion_common::ScalarValue;
use datatypes::arrow::array::{
    Array, BinaryArray, Int32Array, Int64Array, ListArray, MapArray, StringArray, UInt32Array,
    UInt64Array,
};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::value::Value;
use futures::StreamExt;
use mito_codec::row_converter::{CompositeValues, build_primary_key_codec};
use mito2::aggr_index::index_io::IndexReader;
use mito2::aggr_index::input::{merge_sources, open_sst_stream, validate_same_schema};
use mito2::aggr_index::{
    IndexKind, TransformFormat, build_indexes, merge_index_files, transform_pk_index,
};
use mito2::sst::file::{FileMeta, RegionFileId};
use mito2::sst::parquet::metadata::MetadataLoader;
use mito2::sst::parquet::reader::MetadataCacheMetrics;
use object_store::ObjectStore;
use object_store::services::Fs;
use serde::Deserialize;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::FileId;
use store_api::storage::consts::ReservedColumnId;

use crate::datanode::objbench::{build_object_store, extract_region_metadata, parse_config};
use crate::datanode::scanbench::{parse_path_type, parse_region_id, resolve_filter_exprs};
use crate::error;

#[derive(Debug, Parser)]
pub struct AggrIndexCommand {
    #[clap(subcommand)]
    subcmd: AggrIndexSubCommand,
}

#[derive(Debug, Subcommand)]
enum AggrIndexSubCommand {
    Build(BuildCommand),
    Merge(MergeCommand),
    Read(ReadCommand),
    TransformPk(TransformPkCommand),
    BenchFilter(BenchFilterCommand),
}

#[derive(Debug, Parser)]
struct BuildCommand {
    #[clap(long, value_name = "FILE")]
    config: PathBuf,
    #[clap(long)]
    table_dir: String,
    #[clap(long, default_value = "bare")]
    path_type: String,
    #[clap(long)]
    region_id: String,
    #[clap(long, required = true)]
    input: Vec<String>,
    #[clap(long)]
    output_dir: PathBuf,
    #[clap(long, default_value_t = 8 * 1024 * 1024)]
    buffer_bytes: usize,
}

#[derive(Debug, Parser)]
struct MergeCommand {
    #[clap(long, value_enum)]
    kind: CliKind,
    #[clap(long, required = true)]
    input: Vec<PathBuf>,
    #[clap(long)]
    output: PathBuf,
}

#[derive(Debug, Parser)]
struct TransformPkCommand {
    #[clap(long, value_name = "FILE")]
    config: PathBuf,
    #[clap(long)]
    table_dir: String,
    #[clap(long, default_value = "bare")]
    path_type: String,
    #[clap(long)]
    region_id: String,
    #[clap(long)]
    input: String,
    #[clap(long)]
    pk_input: PathBuf,
    #[clap(long)]
    output_dir: PathBuf,
    #[clap(long, value_enum)]
    format: Vec<CliTransformFormat>,
}

#[derive(Debug, Parser)]
struct ReadCommand {
    #[clap(long, value_enum)]
    kind: CliKind,
    #[clap(long)]
    input: PathBuf,
    #[clap(long)]
    table_id: Option<u32>,
    #[clap(long)]
    column_id: Option<u32>,
    #[clap(long)]
    tag_value: Option<String>,
    #[clap(long)]
    primary_key_hex: Option<String>,
}

#[derive(Debug, Parser)]
struct BenchFilterCommand {
    #[clap(long)]
    input: PathBuf,
    #[clap(long, value_name = "FILE")]
    filter_config: PathBuf,
    #[clap(long, value_name = "FILE")]
    metadata_parquet: Option<PathBuf>,
    #[clap(long, default_value_t = 1)]
    iterations: usize,
}

#[derive(Debug, Deserialize)]
struct FilterConfig {
    filters: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default)]
struct BenchFilterCosts {
    metadata_load: Duration,
    filter_parse: Duration,
    index_read: Duration,
    pk_filter: Duration,
    pk_decode_tsid: Duration,
    tag_filter: Duration,
    tsid_intersect: Duration,
    total: Duration,
}

#[derive(Debug)]
struct BenchFilterOutput {
    kind: IndexKind,
    rows: usize,
    batches: usize,
    matched_tsids: usize,
    costs: BenchFilterCosts,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliKind {
    Pk,
    TableTag,
    Tag,
    TableTagTsid,
    PkMap,
    PkMapName,
    PkColumns,
    PkColumnsV2,
}
impl From<CliKind> for IndexKind {
    fn from(v: CliKind) -> Self {
        match v {
            CliKind::Pk => IndexKind::Pk,
            CliKind::TableTag => IndexKind::TableTag,
            CliKind::Tag => IndexKind::Tag,
            CliKind::TableTagTsid => IndexKind::TableTagTsid,
            CliKind::PkMap => IndexKind::PkMap,
            CliKind::PkMapName => IndexKind::PkMapName,
            CliKind::PkColumns => IndexKind::PkColumns,
            CliKind::PkColumnsV2 => IndexKind::PkColumnsV2,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliTransformFormat {
    TableTagTsid,
    Map,
    MapName,
    Columns,
    ColumnsV2,
}

impl From<CliTransformFormat> for TransformFormat {
    fn from(v: CliTransformFormat) -> Self {
        match v {
            CliTransformFormat::TableTagTsid => TransformFormat::TableTagTsid,
            CliTransformFormat::Map => TransformFormat::PkMap,
            CliTransformFormat::MapName => TransformFormat::PkMapName,
            CliTransformFormat::Columns => TransformFormat::PkColumns,
            CliTransformFormat::ColumnsV2 => TransformFormat::PkColumnsV2,
        }
    }
}

impl AggrIndexCommand {
    pub async fn run(&self) -> error::Result<()> {
        match &self.subcmd {
            AggrIndexSubCommand::Build(cmd) => cmd.run().await,
            AggrIndexSubCommand::Merge(cmd) => cmd.run().await,
            AggrIndexSubCommand::Read(cmd) => cmd.run().await,
            AggrIndexSubCommand::TransformPk(cmd) => cmd.run().await,
            AggrIndexSubCommand::BenchFilter(cmd) => cmd.run().await,
        }
    }
}

/// Builds a dedicated local-filesystem object store rooted at the filesystem
/// root, so any absolute local path can be addressed by stripping its leading
/// `/` (see [`store_path`]).
fn local_fs_store() -> error::Result<ObjectStore> {
    ObjectStore::new(Fs::default().root("/"))
        .map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("init local fs object store: {e}"),
            }
            .build()
        })
        .map(|builder| builder.finish())
}

/// Maps a local path to a path within the [`local_fs_store`] (rooted at `/`) by
/// absolutizing it and stripping the leading `/`.
fn store_path(p: &Path) -> error::Result<String> {
    let abs = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("resolve current dir: {e}"),
                }
                .build()
            })?
            .join(p)
    };
    Ok(abs.to_string_lossy().trim_start_matches('/').to_string())
}

impl BuildCommand {
    async fn run(&self) -> error::Result<()> {
        let (store_cfg, _, _) = parse_config(&self.config)?;
        let object_store = build_object_store(&store_cfg).await?;
        let region_id = parse_region_id(&self.region_id)?;
        let path_type = parse_path_type(&self.path_type)?;
        let mut streams = Vec::new();
        let mut metadata = None;
        let mut schema = None;
        for input in &self.input {
            let file_id = parse_file_id(input)?;
            let path = sst_path(&self.table_dir, region_id, path_type, file_id);
            let stat = object_store.stat(&path).await.map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("stat {path} failed: {e}"),
                }
                .build()
            })?;
            let file_meta = FileMeta {
                region_id,
                file_id,
                file_size: stat.content_length(),
                ..Default::default()
            };
            let sst = open_sst_stream(
                self.table_dir.clone(),
                path_type,
                object_store.clone(),
                file_meta,
            )
            .await
            .map_err(to_cmd_err)?;
            if let Some(expected) = &schema {
                validate_same_schema(expected, &sst.schema).map_err(to_cmd_err)?;
            } else {
                schema = Some(sst.schema.clone());
            }
            if metadata.is_none() {
                metadata = Some(sst.metadata.clone());
            }
            streams.push(sst.stream);
        }
        let schema = schema.context(error::IllegalConfigSnafu {
            msg: "no input SSTs",
        })?;
        let metadata = metadata.context(error::IllegalConfigSnafu {
            msg: "no input SST metadata",
        })?;
        let merged = merge_sources(schema, streams).await.map_err(to_cmd_err)?;
        let index_store = local_fs_store()?;
        let output_dir = store_path(&self.output_dir)?;
        let output = build_indexes(
            metadata,
            merged,
            index_store,
            &output_dir,
            self.buffer_bytes,
        )
        .await
        .map_err(to_cmd_err)?;
        print_file("pk", &self.output_dir.join("pk.parquet"), output.pk_rows)?;
        print_file(
            "table-tag",
            &self.output_dir.join("table_tag.parquet"),
            output.table_tag_rows,
        )?;
        print_file("tag", &self.output_dir.join("tag.parquet"), output.tag_rows)?;
        println!(
            "costs: sst_iter_decode_collect={:?} legacy_pk_write={:?} table_tag_run_write={:?} tag_run_write={:?} table_tag_merge_final_write={:?} tag_merge_final_write={:?}",
            output.costs.sst_iteration_decode_collect,
            output.costs.legacy_pk_write,
            output.costs.table_tag_run_write,
            output.costs.tag_run_write,
            output.costs.table_tag_merge_final_write,
            output.costs.tag_merge_final_write
        );
        Ok(())
    }
}

impl TransformPkCommand {
    async fn run(&self) -> error::Result<()> {
        let (store_cfg, _, _) = parse_config(&self.config)?;
        let object_store = build_object_store(&store_cfg).await?;
        let region_id = parse_region_id(&self.region_id)?;
        let path_type = parse_path_type(&self.path_type)?;
        let file_id = parse_file_id(&self.input)?;
        let path = sst_path(&self.table_dir, region_id, path_type, file_id);
        let stat = object_store.stat(&path).await.map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("stat {path} failed: {e}"),
            }
            .build()
        })?;
        let file_meta = FileMeta {
            region_id,
            file_id,
            file_size: stat.content_length(),
            ..Default::default()
        };
        let sst = open_sst_stream(self.table_dir.clone(), path_type, object_store, file_meta)
            .await
            .map_err(to_cmd_err)?;
        let index_store = local_fs_store()?;
        let pk_input = store_path(&self.pk_input)?;
        let output_dir = store_path(&self.output_dir)?;
        let formats = self
            .format
            .iter()
            .copied()
            .map(TransformFormat::from)
            .collect::<Vec<_>>();
        let output =
            transform_pk_index(sst.metadata, index_store, &pk_input, &output_dir, &formats)
                .await
                .map_err(to_cmd_err)?;

        if output.table_tag_tsid_path.is_some() {
            print_file(
                "table-tag-tsid",
                &self.output_dir.join("table_tag_tsid.parquet"),
                output.table_tag_tsid_rows,
            )?;
        }
        if output.pk_map_path.is_some() {
            print_file(
                "pk-map",
                &self.output_dir.join("pk_map.parquet"),
                output.pk_map_rows,
            )?;
        }
        if output.pk_map_name_path.is_some() {
            print_file(
                "pk-map-name",
                &self.output_dir.join("pk_map_name.parquet"),
                output.pk_map_name_rows,
            )?;
        }
        if output.pk_columns_path.is_some() {
            print_file(
                "pk-columns",
                &self.output_dir.join("pk_columns.parquet"),
                output.pk_columns_rows,
            )?;
        }
        if output.pk_columns_v2_path.is_some() {
            print_file(
                "pk-columns-v2",
                &self.output_dir.join("pk_columns_v2.parquet"),
                output.pk_columns_v2_rows,
            )?;
        }
        println!(
            "costs: old_pk_read_iter={:?} pk_decode_tag_extract={:?} table_tag_tsid_write={:?} pk_map_write={:?} pk_map_name_write={:?} pk_columns_write={:?} pk_columns_v2_write={:?}",
            output.costs.read_iteration,
            output.costs.decode_transform,
            output.costs.table_tag_tsid_write,
            output.costs.pk_map_write,
            output.costs.pk_map_name_write,
            output.costs.pk_columns_write,
            output.costs.pk_columns_v2_write
        );
        Ok(())
    }
}

impl MergeCommand {
    async fn run(&self) -> error::Result<()> {
        let index_store = local_fs_store()?;
        let inputs = self
            .input
            .iter()
            .map(|p| store_path(p))
            .collect::<error::Result<Vec<_>>>()?;
        let output = store_path(&self.output)?;
        let rows = merge_index_files(self.kind.into(), &index_store, &inputs, &output)
            .await
            .map_err(to_cmd_err)?;
        print_file("merged", &self.output, rows)
    }
}

impl ReadCommand {
    async fn run(&self) -> error::Result<()> {
        let kind = self.kind.into();
        let tag_value = self.tag_value.as_deref();
        let primary_key = self
            .primary_key_hex
            .as_deref()
            .map(hex::decode)
            .transpose()
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("invalid primary-key-hex: {e}"),
                }
                .build()
            })?;
        let index_store = local_fs_store()?;
        let input = store_path(&self.input)?;
        let mut reader = IndexReader::try_new(&index_store, &input, kind)
            .await
            .map_err(to_cmd_err)?;
        while let Some(batch) = reader.next().await {
            let batch = batch.map_err(to_cmd_err)?;
            match kind {
                IndexKind::Pk => print_pk(&batch, primary_key.as_deref())?,
                IndexKind::TableTag => {
                    print_table_tag(&batch, self.table_id, self.column_id, tag_value)?
                }
                IndexKind::Tag => print_tag(&batch, self.column_id, tag_value)?,
                IndexKind::TableTagTsid => {
                    print_table_tag_tsid(&batch, self.table_id, self.column_id, tag_value)?
                }
                IndexKind::PkMap | IndexKind::PkMapName => print_pk_map(&batch)?,
                IndexKind::PkColumns => print_pk_columns(&batch)?,
                IndexKind::PkColumnsV2 => print_pk_columns_v2(&batch)?,
            }
        }
        Ok(())
    }
}

impl BenchFilterCommand {
    async fn run(&self) -> error::Result<()> {
        if self.iterations == 0 {
            return error::IllegalConfigSnafu {
                msg: "iterations must be greater than 0".to_string(),
            }
            .fail();
        }

        let index_store = local_fs_store()?;
        let input = store_path(&self.input)?;
        let filter_config = load_filter_config(&self.filter_config).await?;

        let metadata_start = Instant::now();
        let metadata = match &self.metadata_parquet {
            Some(path) => Some(load_metadata_from_parquet(&index_store, path).await?),
            None => None,
        };
        let metadata_load = metadata_start.elapsed();

        let filter_parse_start = Instant::now();
        let filters = build_simple_filters(&filter_config.filters, metadata.as_deref())?;
        let filter_parse = filter_parse_start.elapsed();

        println!("{}", "Starting aggr-index bench-filter...".cyan().bold());
        println!("{} Input: {}", "✓".green(), self.input.display());
        println!(
            "{} Filters: {}",
            "✓".green(),
            filter_config.filters.join(" AND ").cyan()
        );
        if let Some(path) = &self.metadata_parquet {
            println!("{} Metadata parquet: {}", "✓".green(), path.display());
        }

        let mut total = BenchFilterCosts::default();
        let mut matched_tsids = 0usize;
        let mut rows = 0usize;
        let mut batches = 0usize;
        let mut kind = None;

        for iteration in 0..self.iterations {
            let mut output =
                bench_filter_once(&index_store, &input, metadata.as_ref(), filters.clone()).await?;
            output.costs.metadata_load = if iteration == 0 {
                metadata_load
            } else {
                Duration::ZERO
            };
            output.costs.filter_parse = if iteration == 0 {
                filter_parse
            } else {
                Duration::ZERO
            };
            output.costs.total += output.costs.metadata_load + output.costs.filter_parse;
            add_costs(&mut total, output.costs);
            matched_tsids = output.matched_tsids;
            rows = output.rows;
            batches = output.batches;
            kind = Some(output.kind);

            println!(
                "  [iter {}] kind={:?} rows={} batches={} matched_tsids={} total={:?}",
                iteration + 1,
                output.kind,
                output.rows,
                output.batches,
                output.matched_tsids,
                output.costs.total,
            );
            print_costs("    costs", output.costs);
        }

        let iterations = self.iterations as u32;
        println!(
            "{} Summary: kind={:?} rows={} batches={} matched_tsids={} avg_total={:?}",
            "✓".green(),
            kind.context(error::IllegalConfigSnafu {
                msg: "no benchmark iterations executed",
            })?,
            rows,
            batches,
            matched_tsids,
            total.total / iterations,
        );
        print_costs("  total_costs", total);
        print_costs("  avg_costs", div_costs(total, iterations));

        Ok(())
    }
}

async fn bench_filter_once(
    index_store: &ObjectStore,
    input: &str,
    metadata: Option<&RegionMetadataRef>,
    filters: Arc<Vec<SimpleFilterEvaluator>>,
) -> error::Result<BenchFilterOutput> {
    let total_start = Instant::now();
    let (kind, batches, index_read) = read_detected_index(index_store, input).await?;
    let rows = batches.iter().map(|batch| batch.num_rows()).sum();
    let mut costs = BenchFilterCosts {
        index_read,
        ..Default::default()
    };

    let matched_tsids = match kind {
        IndexKind::Pk => {
            let metadata = require_metadata(metadata, kind)?;
            validate_filters_are_tag_columns(metadata, &filters)?;
            let start = Instant::now();
            let codec = build_primary_key_codec(metadata);
            let mut pk_filter = codec.primary_key_filter(metadata, filters);
            let mut matched_pks = Vec::new();
            for batch in &batches {
                let pk = col::<BinaryArray>(batch, 0)?;
                for row in 0..batch.num_rows() {
                    let pk_value = pk.value(row);
                    if pk_filter.matches(pk_value).map_err(to_illegal_config)? {
                        matched_pks.push(pk_value.to_vec());
                    }
                }
            }
            costs.pk_filter = start.elapsed();

            let start = Instant::now();
            let mut tsids = HashSet::new();
            for pk in matched_pks {
                let values = codec.decode(&pk).map_err(to_illegal_config)?;
                let tsid = composite_u64(&values, ReservedColumnId::tsid(), "__tsid")?;
                tsids.insert(tsid);
            }
            costs.pk_decode_tsid = start.elapsed();
            tsids.len()
        }
        IndexKind::PkColumns | IndexKind::PkColumnsV2 => {
            let start = Instant::now();
            let tsids = filter_pk_columns(&batches, &filters)?;
            costs.tag_filter = start.elapsed();
            tsids.len()
        }
        IndexKind::TableTagTsid => {
            let metadata = require_metadata(metadata, kind)?;
            validate_filters_are_tag_columns(metadata, &filters)?;
            let start = Instant::now();
            let per_filter = filter_table_tag_tsid(&batches, metadata, &filters)?;
            costs.tag_filter = start.elapsed();

            let start = Instant::now();
            let matched = intersect_tsid_sets(per_filter);
            costs.tsid_intersect = start.elapsed();
            matched
        }
        IndexKind::PkMap | IndexKind::PkMapName | IndexKind::TableTag | IndexKind::Tag => {
            return error::IllegalConfigSnafu {
                msg: format!("unsupported index kind for bench-filter: {kind:?}"),
            }
            .fail();
        }
    };

    costs.total = total_start.elapsed();
    Ok(BenchFilterOutput {
        kind,
        rows,
        batches: batches.len(),
        matched_tsids,
        costs,
    })
}

async fn load_filter_config(path: &Path) -> error::Result<FilterConfig> {
    let content = tokio::fs::read_to_string(path)
        .await
        .context(error::FileIoSnafu)?;
    let config = serde_json::from_str::<FilterConfig>(&content).context(error::SerdeJsonSnafu)?;
    if config.filters.is_empty() {
        return error::IllegalConfigSnafu {
            msg: "filter config must contain at least one filter".to_string(),
        }
        .fail();
    }
    Ok(config)
}

async fn load_metadata_from_parquet(
    object_store: &ObjectStore,
    path: &Path,
) -> error::Result<RegionMetadataRef> {
    let store_path = store_path(path)?;
    let file_size = object_store
        .stat(&store_path)
        .await
        .map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("stat {} failed: {e}", path.display()),
            }
            .build()
        })?
        .content_length();
    let mut metrics = MetadataCacheMetrics::default();
    let parquet_meta = MetadataLoader::new(object_store.clone(), &store_path, file_size)
        .load(&mut metrics)
        .await
        .map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("read parquet metadata failed for {}: {e}", path.display()),
            }
            .build()
        })?;
    extract_region_metadata(&store_path, &parquet_meta)
}

fn build_simple_filters(
    filters: &[String],
    metadata: Option<&RegionMetadata>,
) -> error::Result<Arc<Vec<SimpleFilterEvaluator>>> {
    let exprs = if let Some(metadata) = metadata {
        resolve_filter_exprs(filters, metadata)?
    } else {
        parse_filter_exprs_without_metadata(filters)?
    };

    let simple_filters = exprs
        .iter()
        .zip(filters)
        .map(|(expr, raw)| {
            SimpleFilterEvaluator::try_new(expr).context(error::IllegalConfigSnafu {
                msg: format!("unsupported filter '{raw}'; bench-filter only supports SimpleFilterEvaluator expressions"),
            })
        })
        .collect::<error::Result<Vec<_>>>()?;

    Ok(Arc::new(simple_filters))
}

fn parse_filter_exprs_without_metadata(
    filters: &[String],
) -> error::Result<Vec<datafusion::logical_expr::Expr>> {
    use datafusion::execution::SessionStateBuilder;
    use datafusion_common::ToDFSchema;
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use sqlparser::ast::ExprWithAlias as SqlExprWithAlias;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser as SqlParser;

    let state = SessionStateBuilder::new()
        .with_config(Default::default())
        .with_runtime_env(Default::default())
        .with_default_features()
        .build();
    let mut sql_exprs = Vec::with_capacity(filters.len());
    let mut column_names = Vec::new();
    for (idx, filter) in filters.iter().enumerate() {
        let mut parser = SqlParser::new(&GenericDialect {})
            .try_with_sql(filter)
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("Invalid filter at index {idx} ('{filter}'): {e}"),
                }
                .build()
            })?;
        let sql_expr = parser.parse_expr().map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("Invalid filter at index {idx} ('{filter}'): {e}"),
            }
            .build()
        })?;
        collect_sql_column_names(&sql_expr, &mut column_names);
        sql_exprs.push(sql_expr);
    }
    column_names.sort();
    column_names.dedup();
    let df_schema = Schema::new(
        column_names
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    )
    .to_dfschema()
    .map_err(|e| {
        error::IllegalConfigSnafu {
            msg: format!("Failed to build filter schema: {e}"),
        }
        .build()
    })?;

    sql_exprs
        .into_iter()
        .zip(filters)
        .enumerate()
        .map(|(idx, (sql_expr, filter))| {
            state
                .create_logical_expr_from_sql_expr(
                    SqlExprWithAlias {
                        expr: sql_expr,
                        alias: None,
                    },
                    &df_schema,
                )
                .map_err(|e| {
                    error::IllegalConfigSnafu {
                        msg: format!(
                            "Failed to convert filter at index {idx} ('{filter}') to logical expr: {e}"
                        ),
                    }
                    .build()
                })
        })
        .collect()
}

fn collect_sql_column_names(expr: &sqlparser::ast::Expr, output: &mut Vec<String>) {
    use sqlparser::ast::Expr as SqlExpr;

    match expr {
        SqlExpr::Identifier(ident) => output.push(ident.value.clone()),
        SqlExpr::CompoundIdentifier(idents) => {
            if let Some(ident) = idents.last() {
                output.push(ident.value.clone());
            }
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_sql_column_names(left, output);
            collect_sql_column_names(right, output);
        }
        SqlExpr::Nested(expr) => collect_sql_column_names(expr, output),
        SqlExpr::UnaryOp { expr, .. } => collect_sql_column_names(expr, output),
        SqlExpr::IsNull(expr) | SqlExpr::IsNotNull(expr) => collect_sql_column_names(expr, output),
        _ => {}
    }
}

async fn read_detected_index(
    object_store: &ObjectStore,
    input: &str,
) -> error::Result<(IndexKind, Vec<RecordBatch>, Duration)> {
    let start = Instant::now();
    let mut last_err = None;
    for kind in [
        IndexKind::Pk,
        IndexKind::TableTagTsid,
        IndexKind::PkColumnsV2,
        IndexKind::PkColumns,
        IndexKind::PkMapName,
        IndexKind::PkMap,
        IndexKind::TableTag,
        IndexKind::Tag,
    ] {
        match IndexReader::try_new(object_store, input, kind).await {
            Ok(mut reader) => {
                let mut batches = Vec::new();
                while let Some(batch) = reader.next().await {
                    batches.push(batch.map_err(to_cmd_err)?);
                }
                return Ok((kind, batches, start.elapsed()));
            }
            Err(e) => last_err = Some(e),
        }
    }

    let msg = match last_err {
        Some(e) => format!("failed to detect aggregate index kind for {input}: {e}"),
        None => format!("failed to detect aggregate index kind for {input}"),
    };
    error::IllegalConfigSnafu { msg }.fail()
}

fn require_metadata<'a>(
    metadata: Option<&'a RegionMetadataRef>,
    kind: IndexKind,
) -> error::Result<&'a RegionMetadataRef> {
    metadata.context(error::IllegalConfigSnafu {
        msg: format!("{kind:?} bench-filter requires --metadata-parquet"),
    })
}

fn validate_filters_are_tag_columns(
    metadata: &RegionMetadataRef,
    filters: &[SimpleFilterEvaluator],
) -> error::Result<()> {
    for filter in filters {
        let column =
            metadata
                .column_by_name(filter.column_name())
                .context(error::IllegalConfigSnafu {
                    msg: format!("unknown filter column '{}'", filter.column_name()),
                })?;
        if column.semantic_type != api::v1::SemanticType::Tag {
            return error::IllegalConfigSnafu {
                msg: format!(
                    "filter column '{}' is not a tag column",
                    filter.column_name()
                ),
            }
            .fail();
        }
    }
    Ok(())
}

fn filter_pk_columns(
    batches: &[RecordBatch],
    filters: &[SimpleFilterEvaluator],
) -> error::Result<HashSet<u64>> {
    let mut tsids = HashSet::new();
    for batch in batches {
        let tsid = col::<UInt64Array>(batch, 4)?;
        let masks = filters
            .iter()
            .map(|filter| {
                let (_, array) = batch
                    .schema()
                    .column_with_name(filter.column_name())
                    .and_then(|(idx, _)| batch.columns().get(idx).map(|array| (idx, array)))
                    .context(error::IllegalConfigSnafu {
                        msg: format!(
                            "filter column '{}' not found in PkColumns index",
                            filter.column_name()
                        ),
                    })?;
                filter.evaluate_array(array).map_err(|e| {
                    error::IllegalConfigSnafu {
                        msg: format!("failed to evaluate filter '{}': {e}", filter.column_name()),
                    }
                    .build()
                })
            })
            .collect::<error::Result<Vec<_>>>()?;

        for row in 0..batch.num_rows() {
            if masks.iter().all(|mask| mask.value(row)) {
                tsids.insert(tsid.value(row));
            }
        }
    }
    Ok(tsids)
}

fn filter_table_tag_tsid(
    batches: &[RecordBatch],
    metadata: &RegionMetadataRef,
    filters: &[SimpleFilterEvaluator],
) -> error::Result<Vec<HashSet<u64>>> {
    let column_ids = filters
        .iter()
        .map(|filter| {
            metadata
                .column_by_name(filter.column_name())
                .map(|column| column.column_id)
                .context(error::IllegalConfigSnafu {
                    msg: format!("unknown filter column '{}'", filter.column_name()),
                })
        })
        .collect::<error::Result<Vec<_>>>()?;
    let mut sets = vec![HashSet::new(); filters.len()];

    for batch in batches {
        let column = col::<UInt32Array>(batch, 1)?;
        let value = col::<StringArray>(batch, 2)?;
        let tsid = col::<UInt64Array>(batch, 3)?;
        let mut value_cache: HashMap<String, ScalarValue> = HashMap::new();
        for row in 0..batch.num_rows() {
            for (filter_idx, filter) in filters.iter().enumerate() {
                if column.value(row) != column_ids[filter_idx] {
                    continue;
                }
                let tag_value = value.value(row);
                let scalar = value_cache
                    .entry(tag_value.to_string())
                    .or_insert_with(|| ScalarValue::Utf8(Some(tag_value.to_string())));
                let matched = filter.evaluate_scalar(scalar).map_err(|e| {
                    error::IllegalConfigSnafu {
                        msg: format!("failed to evaluate filter '{}': {e}", filter.column_name()),
                    }
                    .build()
                })?;
                if matched {
                    sets[filter_idx].insert(tsid.value(row));
                }
            }
        }
    }

    Ok(sets)
}

fn intersect_tsid_sets(mut sets: Vec<HashSet<u64>>) -> usize {
    let Some((first_idx, _)) = sets.iter().enumerate().min_by_key(|(_, set)| set.len()) else {
        return 0;
    };
    let first = sets.swap_remove(first_idx);
    first
        .iter()
        .filter(|tsid| sets.iter().all(|set| set.contains(tsid)))
        .count()
}

fn composite_u64(values: &CompositeValues, column_id: u32, name: &str) -> error::Result<u64> {
    let value = match values {
        CompositeValues::Dense(values) => values
            .iter()
            .find(|(id, _)| *id == column_id)
            .map(|(_, value)| value),
        CompositeValues::Sparse(values) => values.get(&column_id),
    };
    match value {
        Some(Value::UInt64(v)) => Ok(*v),
        other => error::IllegalConfigSnafu {
            msg: format!("missing/invalid {name}: {other:?}"),
        }
        .fail(),
    }
}

fn add_costs(total: &mut BenchFilterCosts, costs: BenchFilterCosts) {
    total.metadata_load += costs.metadata_load;
    total.filter_parse += costs.filter_parse;
    total.index_read += costs.index_read;
    total.pk_filter += costs.pk_filter;
    total.pk_decode_tsid += costs.pk_decode_tsid;
    total.tag_filter += costs.tag_filter;
    total.tsid_intersect += costs.tsid_intersect;
    total.total += costs.total;
}

fn div_costs(costs: BenchFilterCosts, divisor: u32) -> BenchFilterCosts {
    BenchFilterCosts {
        metadata_load: costs.metadata_load / divisor,
        filter_parse: costs.filter_parse / divisor,
        index_read: costs.index_read / divisor,
        pk_filter: costs.pk_filter / divisor,
        pk_decode_tsid: costs.pk_decode_tsid / divisor,
        tag_filter: costs.tag_filter / divisor,
        tsid_intersect: costs.tsid_intersect / divisor,
        total: costs.total / divisor,
    }
}

fn print_costs(prefix: &str, costs: BenchFilterCosts) {
    println!(
        "{prefix}: metadata_load={:?} filter_parse={:?} index_read={:?} pk_filter={:?} pk_decode_tsid={:?} tag_filter={:?} tsid_intersect={:?} total={:?}",
        costs.metadata_load,
        costs.filter_parse,
        costs.index_read,
        costs.pk_filter,
        costs.pk_decode_tsid,
        costs.tag_filter,
        costs.tsid_intersect,
        costs.total,
    );
}

fn print_pk(
    batch: &datatypes::arrow::record_batch::RecordBatch,
    filter: Option<&[u8]>,
) -> error::Result<()> {
    let pk = col::<BinaryArray>(batch, 0)?;
    let min = col::<Int64Array>(batch, 1)?;
    let max = col::<Int64Array>(batch, 2)?;
    let cnt = col::<UInt64Array>(batch, 3)?;
    for i in 0..batch.num_rows() {
        if filter.is_none_or(|f| f == pk.value(i)) {
            println!(
                "pk={} min_ts={} max_ts={} row_count={}",
                hex::encode(pk.value(i)),
                min.value(i),
                max.value(i),
                cnt.value(i)
            );
        }
    }
    Ok(())
}
fn print_table_tag(
    batch: &datatypes::arrow::record_batch::RecordBatch,
    table_filter: Option<u32>,
    col_filter: Option<u32>,
    val_filter: Option<&str>,
) -> error::Result<()> {
    let t = col::<UInt32Array>(batch, 0)?;
    let c = col::<UInt32Array>(batch, 1)?;
    let v = col::<StringArray>(batch, 2)?;
    for i in 0..batch.num_rows() {
        if table_filter.is_none_or(|x| x == t.value(i))
            && col_filter.is_none_or(|x| x == c.value(i))
            && val_filter.is_none_or(|x| x == v.value(i))
        {
            println!(
                "table_id={} column_id={} tag_value={}",
                t.value(i),
                c.value(i),
                v.value(i)
            );
        }
    }
    Ok(())
}
fn print_table_tag_tsid(
    batch: &datatypes::arrow::record_batch::RecordBatch,
    table_filter: Option<u32>,
    col_filter: Option<u32>,
    val_filter: Option<&str>,
) -> error::Result<()> {
    let t = col::<UInt32Array>(batch, 0)?;
    let c = col::<UInt32Array>(batch, 1)?;
    let v = col::<StringArray>(batch, 2)?;
    let tsid = col::<UInt64Array>(batch, 3)?;
    for i in 0..batch.num_rows() {
        if table_filter.is_none_or(|x| x == t.value(i))
            && col_filter.is_none_or(|x| x == c.value(i))
            && val_filter.is_none_or(|x| x == v.value(i))
        {
            println!(
                "table_id={} column_id={} tag_value={} tsid={}",
                t.value(i),
                c.value(i),
                v.value(i),
                tsid.value(i)
            );
        }
    }
    Ok(())
}

fn print_pk_map(batch: &datatypes::arrow::record_batch::RecordBatch) -> error::Result<()> {
    let min = col::<Int64Array>(batch, 0)?;
    let max = col::<Int64Array>(batch, 1)?;
    let cnt = col::<UInt64Array>(batch, 2)?;
    let table = col::<UInt32Array>(batch, 3)?;
    let tsid = col::<UInt64Array>(batch, 4)?;
    let tags = col::<MapArray>(batch, 5)?;
    for i in 0..batch.num_rows() {
        println!(
            "min_ts={} max_ts={} row_count={} table_id={} tsid={} tags={}",
            min.value(i),
            max.value(i),
            cnt.value(i),
            table.value(i),
            tsid.value(i),
            format_map(tags, i)?
        );
    }
    Ok(())
}

fn print_pk_columns(batch: &datatypes::arrow::record_batch::RecordBatch) -> error::Result<()> {
    let min = col::<Int64Array>(batch, 0)?;
    let max = col::<Int64Array>(batch, 1)?;
    let cnt = col::<UInt64Array>(batch, 2)?;
    let table = col::<UInt32Array>(batch, 3)?;
    let tsid = col::<UInt64Array>(batch, 4)?;
    for i in 0..batch.num_rows() {
        let mut tags = Vec::new();
        for idx in 5..batch.num_columns() {
            let array = col::<StringArray>(batch, idx)?;
            if array.is_null(i) {
                tags.push(format!("{}=NULL", batch.schema().field(idx).name()));
            } else {
                tags.push(format!(
                    "{}={}",
                    batch.schema().field(idx).name(),
                    array.value(i)
                ));
            }
        }
        println!(
            "min_ts={} max_ts={} row_count={} table_id={} tsid={} {}",
            min.value(i),
            max.value(i),
            cnt.value(i),
            table.value(i),
            tsid.value(i),
            tags.join(" ")
        );
    }
    Ok(())
}

fn print_pk_columns_v2(batch: &RecordBatch) -> error::Result<()> {
    let min = col::<Int64Array>(batch, 0)?;
    let max = col::<Int64Array>(batch, 1)?;
    let cnt = col::<UInt64Array>(batch, 2)?;
    let table = col::<UInt32Array>(batch, 3)?;
    let tsid = col::<UInt64Array>(batch, 4)?;
    let positions = col::<ListArray>(batch, 5)?;
    for row in 0..batch.num_rows() {
        let row_positions = positions.value(row);
        let row_positions = row_positions
            .as_any()
            .downcast_ref::<Int32Array>()
            .context(error::IllegalConfigSnafu {
                msg: "invalid pk_columns_v2 tag position values".to_string(),
            })?;
        let mut position_values = Vec::with_capacity(row_positions.len());
        let mut tags = Vec::with_capacity(row_positions.len());
        for position in row_positions.values() {
            let position = usize::try_from(*position).map_err(|_| {
                error::IllegalConfigSnafu {
                    msg: format!("invalid negative pk_columns_v2 tag position {position}"),
                }
                .build()
            })?;
            if position < 6 || position >= batch.num_columns() {
                return error::IllegalConfigSnafu {
                    msg: format!("invalid pk_columns_v2 tag position {position}"),
                }
                .fail();
            }
            let array = col::<StringArray>(batch, position)?;
            if array.is_null(row) {
                return error::IllegalConfigSnafu {
                    msg: format!(
                        "pk_columns_v2 tag position {position} references a null value at row {row}"
                    ),
                }
                .fail();
            }
            position_values.push(position);
            tags.push(format!(
                "{}={}",
                batch.schema().field(position).name(),
                array.value(row)
            ));
        }
        println!(
            "min_ts={} max_ts={} row_count={} table_id={} tsid={} tag_positions={:?} {}",
            min.value(row),
            max.value(row),
            cnt.value(row),
            table.value(row),
            tsid.value(row),
            position_values,
            tags.join(" ")
        );
    }
    Ok(())
}

fn format_map(tags: &MapArray, row: usize) -> error::Result<String> {
    let entries = tags.value(row);
    let struct_array = entries
        .as_any()
        .downcast_ref::<datatypes::arrow::array::StructArray>()
        .context(error::IllegalConfigSnafu {
            msg: "invalid map entries".to_string(),
        })?;
    let key_array = struct_array.column(0);
    let values = struct_array
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .context(error::IllegalConfigSnafu {
            msg: "invalid map value column".to_string(),
        })?
        .clone();
    let mut parts = Vec::new();
    if let Some(keys) = key_array.as_any().downcast_ref::<UInt32Array>() {
        for i in 0..entries.len() {
            parts.push(format!("{}={}", keys.value(i), values.value(i)));
        }
    } else if let Some(keys) = key_array.as_any().downcast_ref::<StringArray>() {
        for i in 0..entries.len() {
            parts.push(format!("{}={}", keys.value(i), values.value(i)));
        }
    } else {
        return Err(error::IllegalConfigSnafu {
            msg: "invalid map key column".to_string(),
        }
        .build());
    }
    Ok(format!("{{{}}}", parts.join(",")))
}

fn print_tag(
    batch: &datatypes::arrow::record_batch::RecordBatch,
    col_filter: Option<u32>,
    val_filter: Option<&str>,
) -> error::Result<()> {
    let c = col::<UInt32Array>(batch, 0)?;
    let v = col::<StringArray>(batch, 1)?;
    for i in 0..batch.num_rows() {
        if col_filter.is_none_or(|x| x == c.value(i)) && val_filter.is_none_or(|x| x == v.value(i))
        {
            println!("column_id={} tag_value={}", c.value(i), v.value(i));
        }
    }
    Ok(())
}
fn col<T: 'static>(
    batch: &datatypes::arrow::record_batch::RecordBatch,
    idx: usize,
) -> error::Result<&T> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<T>()
        .context(error::IllegalConfigSnafu {
            msg: format!("invalid index column {idx}"),
        })
}

fn sst_path(
    table_dir: &str,
    region_id: store_api::storage::RegionId,
    path_type: store_api::region_request::PathType,
    file_id: FileId,
) -> String {
    mito2::sst::location::sst_file_path(table_dir, RegionFileId::new(region_id, file_id), path_type)
}
fn parse_file_id(input: &str) -> error::Result<FileId> {
    let stem = Path::new(input)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(input.trim_end_matches(".parquet"));
    FileId::parse_str(stem).map_err(|e| {
        error::IllegalConfigSnafu {
            msg: format!("invalid SST filename {input}: {e}"),
        }
        .build()
    })
}
fn to_cmd_err(e: mito2::error::Error) -> error::Error {
    error::IllegalConfigSnafu { msg: e.to_string() }.build()
}
fn to_illegal_config(e: impl std::fmt::Display) -> error::Error {
    error::IllegalConfigSnafu { msg: e.to_string() }.build()
}
fn print_file(label: &str, path: &Path, rows: usize) -> error::Result<()> {
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    println!(
        "{} {} rows={} size={} path={}",
        "✓".green(),
        label,
        rows,
        size,
        path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datatypes::arrow::array::{
        Int32Array, Int64Array, ListArray, StringArray, UInt32Array, UInt64Array,
    };
    use datatypes::arrow::buffer::OffsetBuffer;
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use mito2::aggr_index::schema::{pk_columns_base_schema, pk_columns_v2_base_schema};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::storage::RegionId;

    use super::*;

    fn test_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(11, 0));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "host".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "region".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts".to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![1, 2]);
        Arc::new(builder.build().unwrap())
    }

    #[test]
    fn test_build_simple_filters_rejects_unsupported_filter() {
        let err = build_simple_filters(&["host IS NULL".to_string()], None).unwrap_err();
        assert!(err.to_string().contains("unsupported filter"), "{err}");
    }

    #[test]
    fn test_filter_pk_columns_counts_distinct_matching_tsids() {
        let mut fields = pk_columns_base_schema().fields().to_vec();
        fields.push(Arc::new(Field::new("host", DataType::Utf8, true)));
        fields.push(Arc::new(Field::new("region", DataType::Utf8, true)));
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1])) as _,
                Arc::new(Int64Array::from(vec![2, 2, 2])) as _,
                Arc::new(UInt64Array::from(vec![10, 10, 10])) as _,
                Arc::new(UInt32Array::from(vec![11, 11, 11])) as _,
                Arc::new(UInt64Array::from(vec![101, 102, 101])) as _,
                Arc::new(StringArray::from(vec!["web-1", "web-2", "web-1"])) as _,
                Arc::new(StringArray::from(vec!["us-west", "us-west", "us-west"])) as _,
            ],
        )
        .unwrap();
        let filters = build_simple_filters(
            &[
                "host = 'web-1'".to_string(),
                "region = 'us-west'".to_string(),
            ],
            None,
        )
        .unwrap();

        let tsids = filter_pk_columns(&[batch], &filters).unwrap();
        assert_eq!(tsids.len(), 1);
        assert!(tsids.contains(&101));
    }

    #[test]
    fn test_filter_pk_columns_v2_counts_distinct_matching_tsids() {
        let mut fields = pk_columns_v2_base_schema().fields().to_vec();
        fields.push(Arc::new(Field::new("host", DataType::Utf8, true)));
        fields.push(Arc::new(Field::new("region", DataType::Utf8, true)));
        let schema = Arc::new(Schema::new(fields));
        let position_field = match schema.field(5).data_type() {
            DataType::List(field) => field.clone(),
            _ => unreachable!("pk_columns_v2 tag positions field is a list"),
        };
        let positions = ListArray::new(
            position_field,
            OffsetBuffer::from_lengths([2, 2, 2]),
            Arc::new(Int32Array::from(vec![6, 7, 6, 7, 6, 7])),
            None,
        );
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1])) as _,
                Arc::new(Int64Array::from(vec![2, 2, 2])) as _,
                Arc::new(UInt64Array::from(vec![10, 10, 10])) as _,
                Arc::new(UInt32Array::from(vec![11, 11, 11])) as _,
                Arc::new(UInt64Array::from(vec![101, 102, 101])) as _,
                Arc::new(positions) as _,
                Arc::new(StringArray::from(vec!["web-1", "web-2", "web-1"])) as _,
                Arc::new(StringArray::from(vec!["us-west", "us-west", "us-west"])) as _,
            ],
        )
        .unwrap();
        let filters = build_simple_filters(
            &[
                "host = 'web-1'".to_string(),
                "region = 'us-west'".to_string(),
            ],
            None,
        )
        .unwrap();

        let tsids = filter_pk_columns(&[batch], &filters).unwrap();
        assert_eq!(tsids.len(), 1);
        assert!(tsids.contains(&101));
    }

    #[test]
    fn test_filter_table_tag_tsid_intersects_filter_sets() {
        let metadata = test_metadata();
        let batch = RecordBatch::try_new(
            IndexKind::TableTagTsid.schema(),
            vec![
                Arc::new(UInt32Array::from(vec![11, 11, 11, 11])) as _,
                Arc::new(UInt32Array::from(vec![1, 2, 1, 2])) as _,
                Arc::new(StringArray::from(vec![
                    "web-1", "us-west", "web-1", "us-east",
                ])) as _,
                Arc::new(UInt64Array::from(vec![101, 101, 102, 102])) as _,
            ],
        )
        .unwrap();
        let filters = build_simple_filters(
            &[
                "host = 'web-1'".to_string(),
                "region = 'us-west'".to_string(),
            ],
            Some(&metadata),
        )
        .unwrap();

        let sets = filter_table_tag_tsid(&[batch], &metadata, &filters).unwrap();
        assert_eq!(intersect_tsid_sets(sets), 1);
    }
}
