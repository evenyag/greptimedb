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

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_array::RecordBatchReader;
use clap::{Parser, ValueEnum};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::schema::types::ColumnPath;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error;

/// Display parquet file metadata.
#[derive(Debug, Parser)]
pub struct ParquetMetaCommand {
    /// Path to input parquet file.
    #[clap(long, value_name = "FILE")]
    input: PathBuf,

    /// Output format.
    #[clap(long, value_enum, default_value = "text")]
    format: MetaOutputFormat,
}

/// Read and rewrite a parquet file with different writer properties.
#[derive(Debug, Parser)]
pub struct ParquetRewriteCommand {
    /// Path to input parquet file.
    #[clap(long, value_name = "FILE")]
    input: PathBuf,

    /// Path to output parquet file in rewrite mode.
    #[clap(long, value_name = "FILE")]
    output: Option<PathBuf>,

    /// Path to writer properties TOML in rewrite mode.
    #[clap(long, value_name = "FILE")]
    properties: Option<PathBuf>,

    /// Dump writer properties TOML inferred from the input parquet file.
    #[clap(long, value_name = "FILE")]
    dump_properties: Option<PathBuf>,

    /// Number of rows per record batch.
    #[clap(long)]
    batch_size: Option<usize>,

    /// Overwrite output files.
    #[clap(long, default_value_t = false)]
    overwrite: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum MetaOutputFormat {
    Text,
    Json,
}

#[derive(Debug, Serialize)]
struct FileMetaView {
    input: String,
    num_rows: i64,
    num_row_groups: usize,
    num_columns: usize,
    key_value_metadata: BTreeMap<String, Option<String>>,
    row_groups: Vec<RowGroupMetaView>,
}

#[derive(Debug, Serialize)]
struct RowGroupMetaView {
    index: usize,
    num_rows: i64,
    uncompressed_size: i64,
    compressed_size: i64,
    compression_ratio: Option<f64>,
    data_pages: Option<usize>,
    dictionary_page_bytes: i64,
    columns: Vec<ColumnChunkMetaView>,
}

#[derive(Debug, Serialize)]
struct ColumnChunkMetaView {
    index: usize,
    path: String,
    physical_type: String,
    encodings: Vec<String>,
    compression: String,
    num_values: i64,
    uncompressed_size: i64,
    compressed_size: i64,
    compression_ratio: Option<f64>,
    data_page_offset: i64,
    dictionary_page_offset: Option<i64>,
    dictionary_page_bytes: Option<i64>,
    data_pages: Option<usize>,
    has_statistics: bool,
    column_index_offset: Option<i64>,
    column_index_length: Option<i32>,
    offset_index_offset: Option<i64>,
    offset_index_length: Option<i32>,
    bloom_filter_offset: Option<i64>,
    bloom_filter_length: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
struct RewriteProperties {
    writer: WriterConfig,
    columns: Vec<ColumnConfig>,
}

impl Default for RewriteProperties {
    fn default() -> Self {
        Self {
            writer: WriterConfig::default(),
            columns: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default, deny_unknown_fields)]
struct WriterConfig {
    dictionary_enabled: Option<bool>,
    compression: Option<CompressionConfig>,
    compression_level: Option<u32>,
    max_row_group_row_count: Option<usize>,
    data_page_size_limit: Option<usize>,
    data_page_row_count_limit: Option<usize>,
    dictionary_page_size_limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
struct ColumnConfig {
    path: Vec<String>,
    dictionary_enabled: Option<bool>,
    compression: Option<CompressionConfig>,
    compression_level: Option<u32>,
}

impl Default for ColumnConfig {
    fn default() -> Self {
        Self {
            path: Vec::new(),
            dictionary_enabled: None,
            compression: None,
            compression_level: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum CompressionConfig {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
    Lz4Raw,
}

impl ParquetMetaCommand {
    pub async fn run(&self) -> error::Result<()> {
        let metadata = load_metadata_with_page_index(&self.input)?;
        let view = build_file_meta_view(&self.input, &metadata);

        match self.format {
            MetaOutputFormat::Text => print_meta_text(&view),
            MetaOutputFormat::Json => {
                let json = serde_json::to_string_pretty(&view).context(error::SerdeJsonSnafu)?;
                println!("{json}");
            }
        }

        Ok(())
    }
}

impl ParquetRewriteCommand {
    pub async fn run(&self) -> error::Result<()> {
        match (&self.dump_properties, &self.output, &self.properties) {
            (Some(path), None, None) => self.dump_properties(path),
            (None, Some(output), Some(properties)) => self.rewrite(output, properties),
            (Some(_), Some(_), _) | (Some(_), _, Some(_)) => illegal_config(
                "use either --dump-properties or rewrite mode, not both".to_string(),
            ),
            (None, _, _) => illegal_config(
                "rewrite mode requires --output and --properties; config dump mode requires --dump-properties".to_string(),
            ),
        }
    }

    fn dump_properties(&self, path: &Path) -> error::Result<()> {
        ensure_can_write(path, self.overwrite)?;
        let metadata = load_metadata_with_page_index(&self.input)?;
        let properties = infer_rewrite_properties(&metadata);
        let content = toml::to_string_pretty(&properties).map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("failed to serialize writer properties: {e}"),
            }
            .build()
        })?;
        std::fs::write(path, content).context(error::FileIoSnafu)?;
        println!("Wrote writer properties to {}", path.display());
        Ok(())
    }

    fn rewrite(&self, output: &Path, properties: &Path) -> error::Result<()> {
        ensure_can_write(output, self.overwrite)?;

        let props = load_rewrite_properties(properties)?;
        let input_reader = SerializedFileReader::new(open_file(&self.input)?)
            .map_err(|e| parquet_error("read source parquet metadata", &self.input, e))?;
        let key_value_metadata = input_reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .cloned();

        let mut reader_builder = ParquetRecordBatchReaderBuilder::try_new(open_file(&self.input)?)
            .map_err(|e| parquet_error("open source parquet", &self.input, e))?;
        if let Some(batch_size) = self.batch_size {
            if batch_size == 0 {
                return illegal_config("--batch-size must be greater than 0".to_string());
            }
            reader_builder = reader_builder.with_batch_size(batch_size);
        }
        let reader = reader_builder
            .build()
            .map_err(|e| parquet_error("build parquet reader", &self.input, e))?;
        let schema = reader.schema();

        let writer_props = build_writer_properties(props, key_value_metadata)?;
        let mut writer = ArrowWriter::try_new(create_file(output)?, schema, Some(writer_props))
            .map_err(|e| parquet_error("create parquet writer", output, e))?;

        for batch in reader {
            let batch =
                batch.map_err(|e| parquet_error("read parquet batch", &self.input, e.into()))?;
            writer
                .write(&batch)
                .map_err(|e| parquet_error("write parquet batch", output, e))?;
        }
        writer
            .close()
            .map_err(|e| parquet_error("close parquet writer", output, e))?;

        println!("Wrote parquet file to {}", output.display());
        Ok(())
    }
}

fn load_metadata_with_page_index(path: &Path) -> error::Result<ParquetMetaData> {
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&mut open_file(path)?)
        .map_err(|e| parquet_error("read parquet metadata", path, e))
}

fn build_file_meta_view(path: &Path, metadata: &ParquetMetaData) -> FileMetaView {
    let key_value_metadata = metadata
        .file_metadata()
        .key_value_metadata()
        .into_iter()
        .flatten()
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect();
    let offset_index = metadata.offset_index();
    let row_groups = metadata
        .row_groups()
        .iter()
        .enumerate()
        .map(|(row_group_idx, row_group)| {
            let columns: Vec<_> = row_group
                .columns()
                .iter()
                .enumerate()
                .map(|(column_idx, column)| {
                    let data_pages = offset_index
                        .and_then(|index| index.get(row_group_idx))
                        .and_then(|columns| columns.get(column_idx))
                        .map(|index| index.page_locations().len());
                    let dictionary_page_bytes = dictionary_page_bytes(
                        column.dictionary_page_offset(),
                        column.data_page_offset(),
                    );

                    ColumnChunkMetaView {
                        index: column_idx,
                        path: column.column_path().string(),
                        physical_type: format!("{:?}", column.column_type()),
                        encodings: column.encodings().map(|enc| format!("{enc:?}")).collect(),
                        compression: compression_to_string(column.compression()).to_string(),
                        num_values: column.num_values(),
                        uncompressed_size: column.uncompressed_size(),
                        compressed_size: column.compressed_size(),
                        compression_ratio: compression_ratio(
                            column.uncompressed_size(),
                            column.compressed_size(),
                        ),
                        data_page_offset: column.data_page_offset(),
                        dictionary_page_offset: column.dictionary_page_offset(),
                        dictionary_page_bytes,
                        data_pages,
                        has_statistics: column.statistics().is_some(),
                        column_index_offset: column.column_index_offset(),
                        column_index_length: column.column_index_length(),
                        offset_index_offset: column.offset_index_offset(),
                        offset_index_length: column.offset_index_length(),
                        bloom_filter_offset: column.bloom_filter_offset(),
                        bloom_filter_length: column.bloom_filter_length(),
                    }
                })
                .collect();
            let data_pages = if columns.iter().all(|column| column.data_pages.is_some()) {
                Some(
                    columns
                        .iter()
                        .map(|column| column.data_pages.unwrap_or_default())
                        .sum(),
                )
            } else {
                None
            };
            let dictionary_page_bytes = columns
                .iter()
                .map(|column| column.dictionary_page_bytes.unwrap_or_default())
                .sum();

            RowGroupMetaView {
                index: row_group_idx,
                num_rows: row_group.num_rows(),
                uncompressed_size: row_group.total_byte_size(),
                compressed_size: row_group.compressed_size(),
                compression_ratio: compression_ratio(
                    row_group.total_byte_size(),
                    row_group.compressed_size(),
                ),
                data_pages,
                dictionary_page_bytes,
                columns,
            }
        })
        .collect();

    FileMetaView {
        input: path.display().to_string(),
        num_rows: metadata.file_metadata().num_rows(),
        num_row_groups: metadata.num_row_groups(),
        num_columns: metadata.file_metadata().schema_descr().num_columns(),
        key_value_metadata,
        row_groups,
    }
}

fn print_meta_text(view: &FileMetaView) {
    println!("file: {}", view.input);
    println!("rows: {}", view.num_rows);
    println!("row_groups: {}", view.num_row_groups);
    println!("columns: {}", view.num_columns);
    println!("key_value_metadata: {}", view.key_value_metadata.len());
    for row_group in &view.row_groups {
        println!(
            "row_group[{}]: rows={}, uncompressed_size={}, compressed_size={}, compression_ratio={}, data_pages={}, dictionary_page_bytes={}",
            row_group.index,
            row_group.num_rows,
            row_group.uncompressed_size,
            row_group.compressed_size,
            format_ratio(row_group.compression_ratio),
            format_optional_usize(row_group.data_pages),
            row_group.dictionary_page_bytes,
        );
        for column in &row_group.columns {
            println!(
                "  column[{}] path={}: type={}, compression={}, encodings=[{}], values={}, uncompressed_size={}, compressed_size={}, compression_ratio={}, data_pages={}, dictionary_page_offset={}, dictionary_page_bytes={}, data_page_offset={}, statistics={}, column_index={}/{}, offset_index={}/{}, bloom_filter={}/{}",
                column.index,
                column.path,
                column.physical_type,
                column.compression,
                column.encodings.join(","),
                column.num_values,
                column.uncompressed_size,
                column.compressed_size,
                format_ratio(column.compression_ratio),
                format_optional_usize(column.data_pages),
                format_optional_i64(column.dictionary_page_offset),
                column
                    .dictionary_page_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string()),
                column.data_page_offset,
                column.has_statistics,
                format_optional_i64(column.column_index_offset),
                format_optional_i32(column.column_index_length),
                format_optional_i64(column.offset_index_offset),
                format_optional_i32(column.offset_index_length),
                format_optional_i64(column.bloom_filter_offset),
                format_optional_i32(column.bloom_filter_length),
            );
        }
    }
}

fn infer_rewrite_properties(metadata: &ParquetMetaData) -> RewriteProperties {
    let first_column = metadata
        .row_groups()
        .first()
        .and_then(|row_group| row_group.columns().first());
    let compression = first_column.map(|column| compression_to_config(column.compression()));
    let dictionary_enabled = first_column
        .map(|column| column.dictionary_page_offset().is_some())
        .or(Some(true));

    let columns = metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let first_chunk = metadata
                .row_groups()
                .first()
                .and_then(|row_group| row_group.columns().get(idx));
            ColumnConfig {
                path: column.path().parts().to_vec(),
                dictionary_enabled: first_chunk
                    .map(|chunk| chunk.dictionary_page_offset().is_some()),
                compression: first_chunk.map(|chunk| compression_to_config(chunk.compression())),
                compression_level: None,
            }
        })
        .collect();

    RewriteProperties {
        writer: WriterConfig {
            dictionary_enabled,
            compression,
            compression_level: None,
            max_row_group_row_count: None,
            data_page_size_limit: None,
            data_page_row_count_limit: None,
            dictionary_page_size_limit: None,
        },
        columns,
    }
}

fn load_rewrite_properties(path: &Path) -> error::Result<RewriteProperties> {
    let content = std::fs::read_to_string(path).context(error::FileIoSnafu)?;
    toml::from_str(&content).map_err(|e| {
        error::IllegalConfigSnafu {
            msg: format!("failed to parse writer properties {}: {e}", path.display()),
        }
        .build()
    })
}

fn build_writer_properties(
    config: RewriteProperties,
    key_value_metadata: Option<Vec<parquet::file::metadata::KeyValue>>,
) -> error::Result<WriterProperties> {
    let mut builder = WriterProperties::builder().set_key_value_metadata(key_value_metadata);
    if let Some(dictionary_enabled) = config.writer.dictionary_enabled {
        builder = builder.set_dictionary_enabled(dictionary_enabled);
    }
    if let Some(compression) = config.writer.compression {
        builder = builder.set_compression(to_parquet_compression(
            compression,
            config.writer.compression_level,
        )?);
    }
    if let Some(max_row_group_row_count) = config.writer.max_row_group_row_count {
        if max_row_group_row_count == 0 {
            return illegal_config("max_row_group_row_count must be greater than 0".to_string());
        }
        builder = builder.set_max_row_group_row_count(Some(max_row_group_row_count));
    }
    if let Some(data_page_size_limit) = config.writer.data_page_size_limit {
        builder = builder.set_data_page_size_limit(data_page_size_limit);
    }
    if let Some(data_page_row_count_limit) = config.writer.data_page_row_count_limit {
        builder = builder.set_data_page_row_count_limit(data_page_row_count_limit);
    }
    if let Some(dictionary_page_size_limit) = config.writer.dictionary_page_size_limit {
        builder = builder.set_dictionary_page_size_limit(dictionary_page_size_limit);
    }

    for column in config.columns {
        if column.path.is_empty() {
            return illegal_config("column path must not be empty".to_string());
        }
        let path = ColumnPath::new(column.path);
        if let Some(dictionary_enabled) = column.dictionary_enabled {
            builder = builder.set_column_dictionary_enabled(path.clone(), dictionary_enabled);
        }
        if let Some(compression) = column.compression {
            builder = builder.set_column_compression(
                path,
                to_parquet_compression(compression, column.compression_level)?,
            );
        }
    }

    Ok(builder.build())
}

fn ensure_can_write(path: &Path, overwrite: bool) -> error::Result<()> {
    if !overwrite && path.exists() {
        return illegal_config(format!(
            "{} already exists; pass --overwrite to replace it",
            path.display()
        ));
    }
    Ok(())
}

fn open_file(path: &Path) -> error::Result<File> {
    File::open(path).context(error::FileIoSnafu)
}

fn create_file(path: &Path) -> error::Result<File> {
    File::create(path).context(error::FileIoSnafu)
}

fn illegal_config<T>(msg: String) -> error::Result<T> {
    error::IllegalConfigSnafu { msg }.fail()
}

fn parquet_error(
    action: &'static str,
    path: &Path,
    error: parquet::errors::ParquetError,
) -> error::Error {
    error::IllegalConfigSnafu {
        msg: format!("{action} failed for {}: {error}", path.display()),
    }
    .build()
}

fn dictionary_page_bytes(
    dictionary_page_offset: Option<i64>,
    data_page_offset: i64,
) -> Option<i64> {
    dictionary_page_offset.map(|offset| data_page_offset.saturating_sub(offset))
}

fn compression_ratio(uncompressed: i64, compressed: i64) -> Option<f64> {
    (uncompressed > 0).then(|| compressed as f64 / uncompressed as f64)
}

fn format_ratio(ratio: Option<f64>) -> String {
    ratio
        .map(|value| format!("{value:.4}"))
        .unwrap_or_else(|| "unknown".to_string())
}

fn format_optional_usize(value: Option<usize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn format_optional_i64(value: Option<i64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn format_optional_i32(value: Option<i32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn to_parquet_compression(
    compression: CompressionConfig,
    level: Option<u32>,
) -> error::Result<Compression> {
    Ok(match compression {
        CompressionConfig::Uncompressed => Compression::UNCOMPRESSED,
        CompressionConfig::Snappy => Compression::SNAPPY,
        CompressionConfig::Gzip => Compression::GZIP(match level {
            Some(level) => GzipLevel::try_new(level).map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("invalid gzip compression level {level}: {e}"),
                }
                .build()
            })?,
            None => GzipLevel::default(),
        }),
        CompressionConfig::Lzo => Compression::LZO,
        CompressionConfig::Brotli => Compression::BROTLI(match level {
            Some(level) => BrotliLevel::try_new(level).map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("invalid brotli compression level {level}: {e}"),
                }
                .build()
            })?,
            None => BrotliLevel::default(),
        }),
        CompressionConfig::Lz4 => Compression::LZ4,
        CompressionConfig::Zstd => Compression::ZSTD(match level {
            Some(level) => ZstdLevel::try_new(level as i32).map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("invalid zstd compression level {level}: {e}"),
                }
                .build()
            })?,
            None => ZstdLevel::default(),
        }),
        CompressionConfig::Lz4Raw => Compression::LZ4_RAW,
    })
}

fn compression_to_config(compression: Compression) -> CompressionConfig {
    match compression {
        Compression::UNCOMPRESSED => CompressionConfig::Uncompressed,
        Compression::SNAPPY => CompressionConfig::Snappy,
        Compression::GZIP(_) => CompressionConfig::Gzip,
        Compression::LZO => CompressionConfig::Lzo,
        Compression::BROTLI(_) => CompressionConfig::Brotli,
        Compression::LZ4 => CompressionConfig::Lz4,
        Compression::ZSTD(_) => CompressionConfig::Zstd,
        Compression::LZ4_RAW => CompressionConfig::Lz4Raw,
    }
}

fn compression_to_string(compression: Compression) -> &'static str {
    match compression_to_config(compression) {
        CompressionConfig::Uncompressed => "uncompressed",
        CompressionConfig::Snappy => "snappy",
        CompressionConfig::Gzip => "gzip",
        CompressionConfig::Lzo => "lzo",
        CompressionConfig::Brotli => "brotli",
        CompressionConfig::Lz4 => "lz4",
        CompressionConfig::Zstd => "zstd",
        CompressionConfig::Lz4Raw => "lz4-raw",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use parquet::file::metadata::KeyValue;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_parse_rewrite_properties() {
        let config: RewriteProperties = toml::from_str(
            r#"
            [writer]
            dictionary_enabled = true
            compression = "zstd"
            compression_level = 3
            max_row_group_row_count = 100000

            [[columns]]
            path = ["host"]
            dictionary_enabled = false
            compression = "snappy"
            "#,
        )
        .unwrap();

        assert_eq!(config.writer.dictionary_enabled, Some(true));
        assert_eq!(config.writer.compression, Some(CompressionConfig::Zstd));
        assert_eq!(config.columns.len(), 1);
        assert_eq!(config.columns[0].path, vec!["host"]);
        assert_eq!(config.columns[0].dictionary_enabled, Some(false));
    }

    #[test]
    fn test_meta_view_unknown_page_count() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("input.parquet");
        write_test_parquet(&path, true);

        let metadata = load_metadata_with_page_index(&path).unwrap();
        let view = build_file_meta_view(&path, &metadata);

        assert_eq!(view.num_row_groups, 1);
        assert!(view.row_groups[0].uncompressed_size > 0);
        assert!(view.row_groups[0].compressed_size > 0);
        assert!(view.row_groups[0].compression_ratio.is_some());
        assert_eq!(view.row_groups[0].columns.len(), 2);
    }

    #[test]
    fn test_dump_and_rewrite_preserves_key_value_metadata_and_disables_dictionary() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("input.parquet");
        let output = dir.path().join("output.parquet");
        write_test_parquet(&input, true);

        let metadata = load_metadata_with_page_index(&input).unwrap();
        let mut properties = infer_rewrite_properties(&metadata);
        properties.columns[1].dictionary_enabled = Some(false);
        let props_path = dir.path().join("props.toml");
        std::fs::write(&props_path, toml::to_string(&properties).unwrap()).unwrap();

        let command = ParquetRewriteCommand {
            input: input.clone(),
            output: Some(output.clone()),
            properties: Some(props_path),
            dump_properties: None,
            batch_size: Some(2),
            overwrite: false,
        };
        command
            .rewrite(&output, command.properties.as_ref().unwrap())
            .unwrap();

        let rewritten = load_metadata_with_page_index(&output).unwrap();
        assert_eq!(rewritten.file_metadata().num_rows(), 4);
        let key_values = rewritten
            .file_metadata()
            .key_value_metadata()
            .cloned()
            .unwrap_or_default();
        assert!(key_values.iter().any(|kv| kv.key == "greptime:test"));
        assert!(
            rewritten
                .row_groups()
                .iter()
                .all(|row_group| row_group.column(1).dictionary_page_offset().is_none())
        );
    }

    fn write_test_parquet(path: &Path, dictionary_enabled: bool) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("host", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "a", "b", "b"])),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_dictionary_enabled(dictionary_enabled)
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "greptime:test".to_string(),
                "value".to_string(),
            )]))
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
}
