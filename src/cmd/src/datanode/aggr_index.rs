// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use colored::Colorize;
use datatypes::arrow::array::{BinaryArray, Int64Array, StringArray, UInt32Array, UInt64Array};
use mito2::aggr_index::index_io::IndexReader;
use mito2::aggr_index::input::{merge_sources, open_sst_stream, validate_same_schema};
use mito2::aggr_index::{IndexKind, build_indexes, merge_index_files};
use mito2::sst::file::{FileMeta, RegionFileId};
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
}
impl From<CliKind> for IndexKind {
    fn from(v: CliKind) -> Self {
        match v {
            CliKind::Pk => IndexKind::Pk,
            CliKind::TableTag => IndexKind::TableTag,
            CliKind::Tag => IndexKind::Tag,
        }
    }
}

impl AggrIndexCommand {
    pub async fn run(&self) -> error::Result<()> {
        match &self.subcmd {
            AggrIndexSubCommand::Build(cmd) => cmd.run().await,
            AggrIndexSubCommand::Merge(cmd) => cmd.run(),
            AggrIndexSubCommand::Read(cmd) => cmd.run(),
        }
    }
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
        let output = build_indexes(metadata, merged, &self.output_dir, self.buffer_bytes)
            .await
            .map_err(to_cmd_err)?;
        print_file("pk", &output.pk_path, output.pk_rows)?;
        print_file("table-tag", &output.table_tag_path, output.table_tag_rows)?;
        print_file("tag", &output.tag_path, output.tag_rows)?;
        Ok(())
    }
}

impl MergeCommand {
    fn run(&self) -> error::Result<()> {
        let rows =
            merge_index_files(self.kind.into(), &self.input, &self.output).map_err(to_cmd_err)?;
        print_file("merged", &self.output, rows)
    }
}

impl ReadCommand {
    fn run(&self) -> error::Result<()> {
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
        let mut reader = IndexReader::try_new(&self.input, kind).map_err(to_cmd_err)?;
        for batch in &mut reader {
            let batch = batch.map_err(to_cmd_err)?;
            match kind {
                IndexKind::Pk => print_pk(&batch, primary_key.as_deref())?,
                IndexKind::TableTag => {
                    print_table_tag(&batch, self.table_id, self.column_id, tag_value)?
                }
                IndexKind::Tag => print_tag(&batch, self.column_id, tag_value)?,
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
