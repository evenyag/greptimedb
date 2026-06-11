// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use colored::Colorize;
use datatypes::arrow::array::{
    Array, BinaryArray, Int64Array, MapArray, StringArray, UInt32Array, UInt64Array,
};
use futures::StreamExt;
use mito2::aggr_index::index_io::IndexReader;
use mito2::aggr_index::input::{merge_sources, open_sst_stream, validate_same_schema};
use mito2::aggr_index::{
    IndexKind, TransformFormat, build_indexes, merge_index_files, transform_pk_index,
};
use mito2::sst::file::{FileMeta, RegionFileId};
use object_store::ObjectStore;
use object_store::services::Fs;
use snafu::OptionExt;
use store_api::storage::FileId;

use crate::datanode::objbench::{build_object_store, parse_config};
use crate::datanode::scanbench::{parse_path_type, parse_region_id};
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliKind {
    Pk,
    TableTag,
    Tag,
    TableTagTsid,
    PkMap,
    PkMapName,
    PkColumns,
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
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliTransformFormat {
    TableTagTsid,
    Map,
    MapName,
    Columns,
}

impl From<CliTransformFormat> for TransformFormat {
    fn from(v: CliTransformFormat) -> Self {
        match v {
            CliTransformFormat::TableTagTsid => TransformFormat::TableTagTsid,
            CliTransformFormat::Map => TransformFormat::PkMap,
            CliTransformFormat::MapName => TransformFormat::PkMapName,
            CliTransformFormat::Columns => TransformFormat::PkColumns,
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
        println!(
            "costs: old_pk_read_iter={:?} pk_decode_tag_extract={:?} table_tag_tsid_write={:?} pk_map_write={:?} pk_map_name_write={:?} pk_columns_write={:?}",
            output.costs.read_iteration,
            output.costs.decode_transform,
            output.costs.table_tag_tsid_write,
            output.costs.pk_map_write,
            output.costs.pk_map_name_write,
            output.costs.pk_columns_write
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
            }
        }
        Ok(())
    }
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
