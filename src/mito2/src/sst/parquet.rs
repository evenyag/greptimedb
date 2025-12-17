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

//! SST in parquet format.

use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use parquet::file::metadata::ParquetMetaData;
use store_api::storage::FileId;

use crate::sst::DEFAULT_WRITE_BUFFER_SIZE;
use crate::sst::file::FileTimeRange;
use crate::sst::index::IndexOutput;

pub(crate) mod file_range;
pub mod flat_format;
pub mod format;
pub(crate) mod helper;
pub(crate) mod metadata;
pub mod reader;
pub mod row_group;
pub mod row_selection;
pub(crate) mod stats;
pub mod writer;

/// Key of metadata in parquet SST.
pub const PARQUET_METADATA_KEY: &str = "greptime:metadata";

/// Default batch size to read parquet files.
pub(crate) const DEFAULT_READ_BATCH_SIZE: usize = 1024;
/// Default row group size for parquet files.
pub const DEFAULT_ROW_GROUP_SIZE: usize = 100 * DEFAULT_READ_BATCH_SIZE;

/// Parquet write options.
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Buffer size for async writer.
    pub write_buffer_size: ReadableSize,
    /// Row group size.
    pub row_group_size: usize,
    /// Max single output file size.
    /// Note: This is not a hard limit as we can only observe the file size when
    /// ArrowWrite writes to underlying writers.
    pub max_file_size: Option<usize>,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            max_file_size: None,
        }
    }
}

/// Parquet SST info returned by the writer.
#[derive(Debug, Default)]
pub struct SstInfo {
    /// SST file id.
    pub file_id: FileId,
    /// Time range of the SST. The timestamps have the same time unit as the
    /// data in the SST.
    pub time_range: FileTimeRange,
    /// File size in bytes.
    pub file_size: u64,
    /// Maximum uncompressed row group size in bytes. 0 if unknown.
    pub max_row_group_uncompressed_size: u64,
    /// Number of rows.
    pub num_rows: usize,
    /// Number of row groups
    pub num_row_groups: u64,
    /// File Meta Data
    pub file_metadata: Option<Arc<ParquetMetaData>>,
    /// Index Meta Data
    pub index_metadata: IndexOutput,
    /// Number of series
    pub num_series: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use api::v1::OpType;
    use common_time::Timestamp;
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::{BinaryExpr, Expr, Literal, Operator, col, lit};
    use datatypes::arrow;
    use datatypes::arrow::array::{
        ArrayRef, BinaryDictionaryBuilder, RecordBatch, StringDictionaryBuilder,
        TimestampMillisecondArray, UInt8Array, UInt64Array,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Schema, UInt32Type};
    use parquet::arrow::AsyncArrowWriter;
    use parquet::basic::{Compression, Encoding, ZstdLevel};
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    use store_api::region_request::PathType;
    use table::predicate::Predicate;
    use tokio_util::compat::FuturesAsyncWriteCompatExt;

    use super::*;
    use crate::access_layer::{FilePathProvider, Metrics, RegionFilePathFactory, WriteType};
    use crate::cache::{CacheManager, CacheStrategy, PageKey};
    use crate::config::IndexConfig;
    use crate::read::{BatchBuilder, BatchReader, FlatSource};
    use crate::region::options::{IndexOptions, InvertedIndexOptions};
    use crate::sst::file::{FileHandle, FileMeta, RegionFileId, RegionIndexId};
    use crate::sst::file_purger::NoopFilePurger;
    use crate::sst::index::bloom_filter::applier::BloomFilterIndexApplierBuilder;
    use crate::sst::index::intermediate::IntermediateManager;
    use crate::sst::index::inverted_index::applier::builder::InvertedIndexApplierBuilder;
    use crate::sst::index::puffin_manager::PuffinManagerFactory;
    use crate::sst::index::{IndexBuildType, Indexer, IndexerBuilder, IndexerBuilderImpl};
    use crate::sst::parquet::format::PrimaryKeyWriteFormat;
    use crate::sst::parquet::reader::{ParquetReader, ParquetReaderBuilder, ReaderMetrics};
    use crate::sst::parquet::writer::ParquetWriter;
    use crate::sst::{
        DEFAULT_WRITE_CONCURRENCY, FlatSchemaOptions, location, to_flat_sst_arrow_schema,
    };
    use crate::test_util::sst_util::{
        assert_parquet_metadata_eq, build_test_binary_test_region_metadata, new_batch_by_range,
        new_batch_with_binary, new_batch_with_custom_sequence, new_primary_key, new_source,
        sst_file_handle, sst_file_handle_with_file_id, sst_region_metadata,
    };
    use crate::test_util::{TestEnv, check_reader_result};

    const FILE_DIR: &str = "/";

    #[derive(Clone)]
    struct FixedPathProvider {
        region_file_id: RegionFileId,
    }

    impl FilePathProvider for FixedPathProvider {
        fn build_index_file_path(&self, _file_id: RegionFileId) -> String {
            location::index_file_path_legacy(FILE_DIR, self.region_file_id, PathType::Bare)
        }

        fn build_index_file_path_with_version(&self, index_id: RegionIndexId) -> String {
            location::index_file_path(FILE_DIR, index_id, PathType::Bare)
        }

        fn build_sst_file_path(&self, _file_id: RegionFileId) -> String {
            location::sst_file_path(FILE_DIR, self.region_file_id, PathType::Bare)
        }
    }

    struct NoopIndexBuilder;

    #[async_trait::async_trait]
    impl IndexerBuilder for NoopIndexBuilder {
        async fn build(&self, _file_id: FileId, _index_version: u64) -> Indexer {
            Indexer::default()
        }
    }

    #[tokio::test]
    async fn test_write_read() {
        let mut env = TestEnv::new().await;
        let object_store = env.init_object_store_manager();
        let handle = sst_file_handle(0, 1000);
        let file_path = FixedPathProvider {
            region_file_id: handle.file_id(),
        };
        let metadata = Arc::new(sst_region_metadata());
        let source = new_source(&[
            new_batch_by_range(&["a", "d"], 0, 60),
            new_batch_by_range(&["b", "f"], 0, 40),
            new_batch_by_range(&["b", "h"], 100, 200),
        ]);
        // Use a small row group size for test.
        let write_opts = WriteOptions {
            row_group_size: 50,
            ..Default::default()
        };

        let mut metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            metadata.clone(),
            IndexConfig::default(),
            NoopIndexBuilder,
            file_path,
            &mut metrics,
        )
        .await;

        let info = writer
            .write_all(source, None, &write_opts)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(200, info.num_rows);
        assert!(info.file_size > 0);
        assert_eq!(
            (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(199)
            ),
            info.time_range
        );

        let builder = ParquetReaderBuilder::new(
            FILE_DIR.to_string(),
            PathType::Bare,
            handle.clone(),
            object_store,
        );
        let mut reader = builder.build().await.unwrap();
        check_reader_result(
            &mut reader,
            &[
                new_batch_by_range(&["a", "d"], 0, 50),
                new_batch_by_range(&["a", "d"], 50, 60),
                new_batch_by_range(&["b", "f"], 0, 40),
                new_batch_by_range(&["b", "h"], 100, 150),
                new_batch_by_range(&["b", "h"], 150, 200),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_read_with_cache() {
        let mut env = TestEnv::new().await;
        let object_store = env.init_object_store_manager();
        let handle = sst_file_handle(0, 1000);
        let metadata = Arc::new(sst_region_metadata());
        let source = new_source(&[
            new_batch_by_range(&["a", "d"], 0, 60),
            new_batch_by_range(&["b", "f"], 0, 40),
            new_batch_by_range(&["b", "h"], 100, 200),
        ]);
        // Use a small row group size for test.
        let write_opts = WriteOptions {
            row_group_size: 50,
            ..Default::default()
        };
        // Prepare data.
        let mut metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            metadata.clone(),
            IndexConfig::default(),
            NoopIndexBuilder,
            FixedPathProvider {
                region_file_id: handle.file_id(),
            },
            &mut metrics,
        )
        .await;

        let sst_info = writer
            .write_all(source, None, &write_opts)
            .await
            .unwrap()
            .remove(0);

        // Enable page cache.
        let cache = CacheStrategy::EnableAll(Arc::new(
            CacheManager::builder()
                .page_cache_size(64 * 1024 * 1024)
                .build(),
        ));
        let builder = ParquetReaderBuilder::new(
            FILE_DIR.to_string(),
            PathType::Bare,
            handle.clone(),
            object_store,
        )
        .cache(cache.clone());
        for _ in 0..3 {
            let mut reader = builder.build().await.unwrap();
            check_reader_result(
                &mut reader,
                &[
                    new_batch_by_range(&["a", "d"], 0, 50),
                    new_batch_by_range(&["a", "d"], 50, 60),
                    new_batch_by_range(&["b", "f"], 0, 40),
                    new_batch_by_range(&["b", "h"], 100, 150),
                    new_batch_by_range(&["b", "h"], 150, 200),
                ],
            )
            .await;
        }

        let parquet_meta = sst_info.file_metadata.unwrap();
        let get_ranges = |row_group_idx: usize| {
            let row_group = parquet_meta.row_group(row_group_idx);
            let mut ranges = Vec::with_capacity(row_group.num_columns());
            for i in 0..row_group.num_columns() {
                let (start, length) = row_group.column(i).byte_range();
                ranges.push(start..start + length);
            }

            ranges
        };

        // Cache 4 row groups.
        for i in 0..4 {
            let page_key = PageKey::new(handle.file_id().file_id(), i, get_ranges(i));
            assert!(cache.get_pages(&page_key).is_some());
        }
        let page_key = PageKey::new(handle.file_id().file_id(), 5, vec![]);
        assert!(cache.get_pages(&page_key).is_none());
    }

    #[tokio::test]
    async fn test_parquet_metadata_eq() {
        // create test env
        let mut env = crate::test_util::TestEnv::new().await;
        let object_store = env.init_object_store_manager();
        let handle = sst_file_handle(0, 1000);
        let metadata = Arc::new(sst_region_metadata());
        let source = new_source(&[
            new_batch_by_range(&["a", "d"], 0, 60),
            new_batch_by_range(&["b", "f"], 0, 40),
            new_batch_by_range(&["b", "h"], 100, 200),
        ]);
        let write_opts = WriteOptions {
            row_group_size: 50,
            ..Default::default()
        };

        // write the sst file and get sst info
        // sst info contains the parquet metadata, which is converted from FileMetaData
        let mut metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            metadata.clone(),
            IndexConfig::default(),
            NoopIndexBuilder,
            FixedPathProvider {
                region_file_id: handle.file_id(),
            },
            &mut metrics,
        )
        .await;

        let sst_info = writer
            .write_all(source, None, &write_opts)
            .await
            .unwrap()
            .remove(0);
        let writer_metadata = sst_info.file_metadata.unwrap();

        // read the sst file metadata
        let builder = ParquetReaderBuilder::new(
            FILE_DIR.to_string(),
            PathType::Bare,
            handle.clone(),
            object_store,
        );
        let reader = builder.build().await.unwrap();
        let reader_metadata = reader.parquet_metadata();

        assert_parquet_metadata_eq(writer_metadata, reader_metadata)
    }

    #[tokio::test]
    async fn test_read_with_tag_filter() {
        let mut env = TestEnv::new().await;
        let object_store = env.init_object_store_manager();
        let handle = sst_file_handle(0, 1000);
        let metadata = Arc::new(sst_region_metadata());
        let source = new_source(&[
            new_batch_by_range(&["a", "d"], 0, 60),
            new_batch_by_range(&["b", "f"], 0, 40),
            new_batch_by_range(&["b", "h"], 100, 200),
        ]);
        // Use a small row group size for test.
        let write_opts = WriteOptions {
            row_group_size: 50,
            ..Default::default()
        };
        // Prepare data.
        let mut metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            metadata.clone(),
            IndexConfig::default(),
            NoopIndexBuilder,
            FixedPathProvider {
                region_file_id: handle.file_id(),
            },
            &mut metrics,
        )
        .await;
        writer
            .write_all(source, None, &write_opts)
            .await
            .unwrap()
            .remove(0);

        // Predicate
        let predicate = Some(Predicate::new(vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("tag_0"))),
            op: Operator::Eq,
            right: Box::new("a".lit()),
        })]));

        let builder = ParquetReaderBuilder::new(
            FILE_DIR.to_string(),
            PathType::Bare,
            handle.clone(),
            object_store,
        )
        .predicate(predicate);
        let mut reader = builder.build().await.unwrap();
        check_reader_result(
            &mut reader,
            &[
                new_batch_by_range(&["a", "d"], 0, 50),
                new_batch_by_range(&["a", "d"], 50, 60),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_read_empty_batch() {
        let mut env = TestEnv::new().await;
        let object_store = env.init_object_store_manager();
        let handle = sst_file_handle(0, 1000);
        let metadata = Arc::new(sst_region_metadata());
        let source = new_source(&[
            new_batch_by_range(&["a", "z"], 0, 0),
            new_batch_by_range(&["a", "z"], 100, 100),
            new_batch_by_range(&["a", "z"], 200, 230),
        ]);
        // Use a small row group size for test.
        let write_opts = WriteOptions {
            row_group_size: 50,
            ..Default::default()
        };
        // Prepare data.
        let mut metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            metadata.clone(),
            IndexConfig::default(),
            NoopIndexBuilder,
            FixedPathProvider {
                region_file_id: handle.file_id(),
            },
            &mut metrics,
        )
        .await;
        writer
            .write_all(source, None, &write_opts)
            .await
            .unwrap()
            .remove(0);

        let builder = ParquetReaderBuilder::new(
            FILE_DIR.to_string(),
            PathType::Bare,
            handle.clone(),
            object_store,
        );
        let mut reader = builder.build().await.unwrap();
        check_reader_result(&mut reader, &[new_batch_by_range(&["a", "z"], 200, 230)]).await;
    }

    #[tokio::test]
    async fn test_read_with_field_filter() {
        let mut env = TestEnv::new().await;
        let object_store = env.init_object_store_manager();
        let handle = sst_file_handle(0, 1000);
        let metadata = Arc::new(sst_region_metadata());
        let source = new_source(&[
            new_batch_by_range(&["a", "d"], 0, 60),
            new_batch_by_range(&["b", "f"], 0, 40),
            new_batch_by_range(&["b", "h"], 100, 200),
        ]);
        // Use a small row group size for test.
        let write_opts = WriteOptions {
            row_group_size: 50,
            ..Default::default()
        };
        // Prepare data.
        let mut metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            metadata.clone(),
            IndexConfig::default(),
            NoopIndexBuilder,
            FixedPathProvider {
                region_file_id: handle.file_id(),
            },
            &mut metrics,
        )
        .await;

        writer
            .write_all(source, None, &write_opts)
            .await
            .unwrap()
            .remove(0);

        // Predicate
        let predicate = Some(Predicate::new(vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("field_0"))),
            op: Operator::GtEq,
            right: Box::new(150u64.lit()),
        })]));

        let builder = ParquetReaderBuilder::new(
            FILE_DIR.to_string(),
            PathType::Bare,
            handle.clone(),
            object_store,
        )
        .predicate(predicate);
        let mut reader = builder.build().await.unwrap();
        check_reader_result(&mut reader, &[new_batch_by_range(&["b", "h"], 150, 200)]).await;
    }

    #[tokio::test]
    async fn test_read_large_binary() {
        let mut env = TestEnv::new().await;
        let object_store = env.init_object_store_manager();
        let handle = sst_file_handle(0, 1000);
        let file_path = handle.file_path(FILE_DIR, PathType::Bare);

        let write_opts = WriteOptions {
            row_group_size: 50,
            ..Default::default()
        };

        let metadata = build_test_binary_test_region_metadata();
        let json = metadata.to_json().unwrap();
        let key_value_meta = KeyValue::new(PARQUET_METADATA_KEY.to_string(), json);

        let props_builder = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![key_value_meta]))
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_encoding(Encoding::PLAIN)
            .set_max_row_group_size(write_opts.row_group_size);

        let writer_props = props_builder.build();

        let write_format = PrimaryKeyWriteFormat::new(metadata);
        let fields: Vec<_> = write_format
            .arrow_schema()
            .fields()
            .into_iter()
            .map(|field| {
                let data_type = field.data_type().clone();
                if data_type == DataType::Binary {
                    Field::new(field.name(), DataType::LargeBinary, field.is_nullable())
                } else {
                    Field::new(field.name(), data_type, field.is_nullable())
                }
            })
            .collect();

        let arrow_schema = Arc::new(Schema::new(fields));

        // Ensures field_0 has LargeBinary type.
        assert_eq!(
            &DataType::LargeBinary,
            arrow_schema.field_with_name("field_0").unwrap().data_type()
        );
        let mut writer = AsyncArrowWriter::try_new(
            object_store
                .writer_with(&file_path)
                .concurrent(DEFAULT_WRITE_CONCURRENCY)
                .await
                .map(|w| w.into_futures_async_write().compat_write())
                .unwrap(),
            arrow_schema.clone(),
            Some(writer_props),
        )
        .unwrap();

        let batch = new_batch_with_binary(&["a"], 0, 60);
        let arrow_batch = write_format.convert_batch(&batch).unwrap();
        let arrays: Vec<_> = arrow_batch
            .columns()
            .iter()
            .map(|array| {
                let data_type = array.data_type().clone();
                if data_type == DataType::Binary {
                    arrow::compute::cast(array, &DataType::LargeBinary).unwrap()
                } else {
                    array.clone()
                }
            })
            .collect();
        let result = RecordBatch::try_new(arrow_schema, arrays).unwrap();

        writer.write(&result).await.unwrap();
        writer.close().await.unwrap();

        let builder = ParquetReaderBuilder::new(
            FILE_DIR.to_string(),
            PathType::Bare,
            handle.clone(),
            object_store,
        );
        let mut reader = builder.build().await.unwrap();
        check_reader_result(
            &mut reader,
            &[
                new_batch_with_binary(&["a"], 0, 50),
                new_batch_with_binary(&["a"], 50, 60),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_write_multiple_files() {
        common_telemetry::init_default_ut_logging();
        // create test env
        let mut env = TestEnv::new().await;
        let object_store = env.init_object_store_manager();
        let metadata = Arc::new(sst_region_metadata());
        let batches = &[
            new_batch_by_range(&["a", "d"], 0, 1000),
            new_batch_by_range(&["b", "f"], 0, 1000),
            new_batch_by_range(&["c", "g"], 0, 1000),
            new_batch_by_range(&["b", "h"], 100, 200),
            new_batch_by_range(&["b", "h"], 200, 300),
            new_batch_by_range(&["b", "h"], 300, 1000),
        ];
        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();

        let source = new_source(batches);
        let write_opts = WriteOptions {
            row_group_size: 50,
            max_file_size: Some(1024 * 16),
            ..Default::default()
        };

        let path_provider = RegionFilePathFactory {
            table_dir: "test".to_string(),
            path_type: PathType::Bare,
        };
        let mut metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            metadata.clone(),
            IndexConfig::default(),
            NoopIndexBuilder,
            path_provider,
            &mut metrics,
        )
        .await;

        let files = writer.write_all(source, None, &write_opts).await.unwrap();
        assert_eq!(2, files.len());

        let mut rows_read = 0;
        for f in &files {
            let file_handle = sst_file_handle_with_file_id(
                f.file_id,
                f.time_range.0.value(),
                f.time_range.1.value(),
            );
            let builder = ParquetReaderBuilder::new(
                "test".to_string(),
                PathType::Bare,
                file_handle,
                object_store.clone(),
            );
            let mut reader = builder.build().await.unwrap();
            while let Some(batch) = reader.next_batch().await.unwrap() {
                rows_read += batch.num_rows();
            }
        }
        assert_eq!(total_rows, rows_read);
    }

    #[tokio::test]
    async fn test_write_read_with_index() {
        let mut env = TestEnv::new().await;
        let object_store = env.init_object_store_manager();
        let file_path = RegionFilePathFactory::new(FILE_DIR.to_string(), PathType::Bare);
        let metadata = Arc::new(sst_region_metadata());
        let row_group_size = 50;

        let source = new_source(&[
            new_batch_by_range(&["a", "d"], 0, 20),
            new_batch_by_range(&["b", "d"], 0, 20),
            new_batch_by_range(&["c", "d"], 0, 20),
            new_batch_by_range(&["c", "f"], 0, 40),
            new_batch_by_range(&["c", "h"], 100, 200),
        ]);
        // Use a small row group size for test.
        let write_opts = WriteOptions {
            row_group_size,
            ..Default::default()
        };

        let puffin_manager = env
            .get_puffin_manager()
            .build(object_store.clone(), file_path.clone());
        let intermediate_manager = env.get_intermediate_manager();

        let indexer_builder = IndexerBuilderImpl {
            build_type: IndexBuildType::Flush,
            metadata: metadata.clone(),
            row_group_size,
            puffin_manager,
            write_cache_enabled: false,
            intermediate_manager,
            index_options: IndexOptions {
                inverted_index: InvertedIndexOptions {
                    segment_row_count: 1,
                    ..Default::default()
                },
            },
            inverted_index_config: Default::default(),
            fulltext_index_config: Default::default(),
            bloom_filter_index_config: Default::default(),
        };

        let mut metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            metadata.clone(),
            IndexConfig::default(),
            indexer_builder,
            file_path.clone(),
            &mut metrics,
        )
        .await;

        let info = writer
            .write_all(source, None, &write_opts)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(200, info.num_rows);
        assert!(info.file_size > 0);
        assert!(info.index_metadata.file_size > 0);

        assert!(info.index_metadata.inverted_index.index_size > 0);
        assert_eq!(info.index_metadata.inverted_index.row_count, 200);
        assert_eq!(info.index_metadata.inverted_index.columns, vec![0]);

        assert!(info.index_metadata.bloom_filter.index_size > 0);
        assert_eq!(info.index_metadata.bloom_filter.row_count, 200);
        assert_eq!(info.index_metadata.bloom_filter.columns, vec![1]);

        assert_eq!(
            (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(199)
            ),
            info.time_range
        );

        let handle = FileHandle::new(
            FileMeta {
                region_id: metadata.region_id,
                file_id: info.file_id,
                time_range: info.time_range,
                level: 0,
                file_size: info.file_size,
                max_row_group_uncompressed_size: info.max_row_group_uncompressed_size,
                available_indexes: info.index_metadata.build_available_indexes(),
                indexes: info.index_metadata.build_indexes(),
                index_file_size: info.index_metadata.file_size,
                index_version: 0,
                num_row_groups: info.num_row_groups,
                num_rows: info.num_rows as u64,
                sequence: None,
                partition_expr: match &metadata.partition_expr {
                    Some(json_str) => partition::expr::PartitionExpr::from_json_str(json_str)
                        .expect("partition expression should be valid JSON"),
                    None => None,
                },
                num_series: 0,
            },
            Arc::new(NoopFilePurger),
        );

        let cache = Arc::new(
            CacheManager::builder()
                .index_result_cache_size(1024 * 1024)
                .index_metadata_size(1024 * 1024)
                .index_content_page_size(1024 * 1024)
                .index_content_size(1024 * 1024)
                .puffin_metadata_size(1024 * 1024)
                .build(),
        );
        let index_result_cache = cache.index_result_cache().unwrap();

        let build_inverted_index_applier = |exprs: &[Expr]| {
            InvertedIndexApplierBuilder::new(
                FILE_DIR.to_string(),
                PathType::Bare,
                object_store.clone(),
                &metadata,
                HashSet::from_iter([0]),
                env.get_puffin_manager(),
            )
            .with_puffin_metadata_cache(cache.puffin_metadata_cache().cloned())
            .with_inverted_index_cache(cache.inverted_index_cache().cloned())
            .build(exprs)
            .unwrap()
            .map(Arc::new)
        };

        let build_bloom_filter_applier = |exprs: &[Expr]| {
            BloomFilterIndexApplierBuilder::new(
                FILE_DIR.to_string(),
                PathType::Bare,
                object_store.clone(),
                &metadata,
                env.get_puffin_manager(),
            )
            .with_puffin_metadata_cache(cache.puffin_metadata_cache().cloned())
            .with_bloom_filter_index_cache(cache.bloom_filter_index_cache().cloned())
            .build(exprs)
            .unwrap()
            .map(Arc::new)
        };

        // Data: ts tag_0 tag_1
        // Data: 0-20 [a, d]
        //       0-20 [b, d]
        //       0-20 [c, d]
        //       0-40 [c, f]
        //    100-200 [c, h]
        //
        // Pred: tag_0 = "b"
        //
        // Row groups & rows pruning:
        //
        // Row Groups:
        // - min-max: filter out row groups 1..=3
        //
        // Rows:
        // - inverted index: hit row group 0, hit 20 rows
        let preds = vec![col("tag_0").eq(lit("b"))];
        let inverted_index_applier = build_inverted_index_applier(&preds);
        let bloom_filter_applier = build_bloom_filter_applier(&preds);

        let builder = ParquetReaderBuilder::new(
            FILE_DIR.to_string(),
            PathType::Bare,
            handle.clone(),
            object_store.clone(),
        )
        .predicate(Some(Predicate::new(preds)))
        .inverted_index_appliers([inverted_index_applier.clone(), None])
        .bloom_filter_index_appliers([bloom_filter_applier.clone(), None])
        .cache(CacheStrategy::EnableAll(cache.clone()));

        let mut metrics = ReaderMetrics::default();
        let (context, selection) = builder.build_reader_input(&mut metrics).await.unwrap();
        let mut reader = ParquetReader::new(Arc::new(context), selection)
            .await
            .unwrap();
        check_reader_result(&mut reader, &[new_batch_by_range(&["b", "d"], 0, 20)]).await;

        assert_eq!(metrics.filter_metrics.rg_total, 4);
        assert_eq!(metrics.filter_metrics.rg_minmax_filtered, 3);
        assert_eq!(metrics.filter_metrics.rg_inverted_filtered, 0);
        assert_eq!(metrics.filter_metrics.rows_inverted_filtered, 30);
        let cached = index_result_cache
            .get(
                inverted_index_applier.unwrap().predicate_key(),
                handle.file_id().file_id(),
            )
            .unwrap();
        // inverted index will search all row groups
        assert!(cached.contains_row_group(0));
        assert!(cached.contains_row_group(1));
        assert!(cached.contains_row_group(2));
        assert!(cached.contains_row_group(3));

        // Data: ts tag_0 tag_1
        // Data: 0-20 [a, d]
        //       0-20 [b, d]
        //       0-20 [c, d]
        //       0-40 [c, f]
        //    100-200 [c, h]
        //
        // Pred: 50 <= ts && ts < 200 && tag_1 = "d"
        //
        // Row groups & rows pruning:
        //
        // Row Groups:
        // - min-max: filter out row groups 0..=1
        // - bloom filter: filter out row groups 2..=3
        let preds = vec![
            col("ts").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(50), None))),
            col("ts").lt(lit(ScalarValue::TimestampMillisecond(Some(200), None))),
            col("tag_1").eq(lit("d")),
        ];
        let inverted_index_applier = build_inverted_index_applier(&preds);
        let bloom_filter_applier = build_bloom_filter_applier(&preds);

        let builder = ParquetReaderBuilder::new(
            FILE_DIR.to_string(),
            PathType::Bare,
            handle.clone(),
            object_store.clone(),
        )
        .predicate(Some(Predicate::new(preds)))
        .inverted_index_appliers([inverted_index_applier.clone(), None])
        .bloom_filter_index_appliers([bloom_filter_applier.clone(), None])
        .cache(CacheStrategy::EnableAll(cache.clone()));

        let mut metrics = ReaderMetrics::default();
        let (context, selection) = builder.build_reader_input(&mut metrics).await.unwrap();
        let mut reader = ParquetReader::new(Arc::new(context), selection)
            .await
            .unwrap();
        check_reader_result(&mut reader, &[]).await;

        assert_eq!(metrics.filter_metrics.rg_total, 4);
        assert_eq!(metrics.filter_metrics.rg_minmax_filtered, 2);
        assert_eq!(metrics.filter_metrics.rg_bloom_filtered, 2);
        assert_eq!(metrics.filter_metrics.rows_bloom_filtered, 100);
        let cached = index_result_cache
            .get(
                bloom_filter_applier.unwrap().predicate_key(),
                handle.file_id().file_id(),
            )
            .unwrap();
        assert!(cached.contains_row_group(2));
        assert!(cached.contains_row_group(3));
        assert!(!cached.contains_row_group(0));
        assert!(!cached.contains_row_group(1));

        // Remove the pred of `ts`, continue to use the pred of `tag_1`
        // to test if cache works.

        // Data: ts tag_0 tag_1
        // Data: 0-20 [a, d]
        //       0-20 [b, d]
        //       0-20 [c, d]
        //       0-40 [c, f]
        //    100-200 [c, h]
        //
        // Pred: tag_1 = "d"
        //
        // Row groups & rows pruning:
        //
        // Row Groups:
        // - bloom filter: filter out row groups 2..=3
        //
        // Rows:
        // - bloom filter: hit row group 0, hit 50 rows
        //                 hit row group 1, hit 10 rows
        let preds = vec![col("tag_1").eq(lit("d"))];
        let inverted_index_applier = build_inverted_index_applier(&preds);
        let bloom_filter_applier = build_bloom_filter_applier(&preds);

        let builder = ParquetReaderBuilder::new(
            FILE_DIR.to_string(),
            PathType::Bare,
            handle.clone(),
            object_store.clone(),
        )
        .predicate(Some(Predicate::new(preds)))
        .inverted_index_appliers([inverted_index_applier.clone(), None])
        .bloom_filter_index_appliers([bloom_filter_applier.clone(), None])
        .cache(CacheStrategy::EnableAll(cache.clone()));

        let mut metrics = ReaderMetrics::default();
        let (context, selection) = builder.build_reader_input(&mut metrics).await.unwrap();
        let mut reader = ParquetReader::new(Arc::new(context), selection)
            .await
            .unwrap();
        check_reader_result(
            &mut reader,
            &[
                new_batch_by_range(&["a", "d"], 0, 20),
                new_batch_by_range(&["b", "d"], 0, 20),
                new_batch_by_range(&["c", "d"], 0, 10),
                new_batch_by_range(&["c", "d"], 10, 20),
            ],
        )
        .await;

        assert_eq!(metrics.filter_metrics.rg_total, 4);
        assert_eq!(metrics.filter_metrics.rg_minmax_filtered, 0);
        assert_eq!(metrics.filter_metrics.rg_bloom_filtered, 2);
        assert_eq!(metrics.filter_metrics.rows_bloom_filtered, 140);
        let cached = index_result_cache
            .get(
                bloom_filter_applier.unwrap().predicate_key(),
                handle.file_id().file_id(),
            )
            .unwrap();
        assert!(cached.contains_row_group(0));
        assert!(cached.contains_row_group(1));
        assert!(cached.contains_row_group(2));
        assert!(cached.contains_row_group(3));
    }

    /// Creates a flat format RecordBatch for testing.
    /// Similar to `new_batch_by_range` but returns a RecordBatch in flat format.
    fn new_record_batch_by_range(tags: &[&str], start: usize, end: usize) -> RecordBatch {
        assert!(end >= start);
        let metadata = Arc::new(sst_region_metadata());
        let flat_schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());

        let num_rows = end - start;
        let mut columns = Vec::new();

        // Add primary key columns (tag_0, tag_1) as dictionary arrays
        let mut tag_0_builder = StringDictionaryBuilder::<UInt32Type>::new();
        let mut tag_1_builder = StringDictionaryBuilder::<UInt32Type>::new();

        for _ in 0..num_rows {
            tag_0_builder.append_value(tags[0]);
            tag_1_builder.append_value(tags[1]);
        }

        columns.push(Arc::new(tag_0_builder.finish()) as ArrayRef);
        columns.push(Arc::new(tag_1_builder.finish()) as ArrayRef);

        // Add field column (field_0)
        let field_values: Vec<u64> = (start..end).map(|v| v as u64).collect();
        columns.push(Arc::new(UInt64Array::from(field_values)));

        // Add time index column (ts)
        let timestamps: Vec<i64> = (start..end).map(|v| v as i64).collect();
        columns.push(Arc::new(TimestampMillisecondArray::from(timestamps)));

        // Add encoded primary key column
        let pk = new_primary_key(tags);
        let mut pk_builder = BinaryDictionaryBuilder::<UInt32Type>::new();
        for _ in 0..num_rows {
            pk_builder.append(&pk).unwrap();
        }
        columns.push(Arc::new(pk_builder.finish()));

        // Add sequence column
        columns.push(Arc::new(UInt64Array::from_value(1000, num_rows)));

        // Add op_type column
        columns.push(Arc::new(UInt8Array::from_value(
            OpType::Put as u8,
            num_rows,
        )));

        RecordBatch::try_new(flat_schema, columns).unwrap()
    }

    /// Creates a FlatSource from flat format RecordBatches.
    fn new_flat_source_from_record_batches(batches: Vec<RecordBatch>) -> FlatSource {
        FlatSource::Iter(Box::new(batches.into_iter().map(Ok)))
    }

    /// Builds region metadata for the CPU benchmark test.
    fn build_cpu_benchmark_region_metadata() -> store_api::metadata::RegionMetadata {
        use std::collections::HashMap;

        use api::v1::SemanticType;
        use datatypes::data_type::ConcreteDataType;
        use datatypes::schema::ColumnSchema;
        use store_api::codec::PrimaryKeyEncoding;
        use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};

        let mut builder =
            RegionMetadataBuilder::new(store_api::storage::RegionId::from_u64(4398046511104));

        // Tag columns
        let tag_columns = vec![
            ("hostname", 0, true), // has inverted index
            ("region", 1, false),
            ("datacenter", 2, false),
            ("rack", 3, false),
            ("os", 4, false),
            ("arch", 5, false),
            ("team", 6, false),
            ("service", 7, false),
            ("service_version", 8, false),
            ("service_environment", 9, false),
        ];

        for (name, id, has_inverted) in tag_columns {
            let mut metadata_map = HashMap::new();
            if has_inverted {
                metadata_map.insert("greptime:inverted_index".to_string(), "true".to_string());
            }

            let column_schema = ColumnSchema::new(name, ConcreteDataType::string_datatype(), true)
                .with_metadata(metadata_map);

            builder.push_column_metadata(ColumnMetadata {
                column_schema,
                semantic_type: SemanticType::Tag,
                column_id: id,
            });
        }

        // Field columns
        let field_columns = vec![
            ("usage_user", 10),
            ("usage_system", 11),
            ("usage_idle", 12),
            ("usage_nice", 13),
            ("usage_iowait", 14),
            ("usage_irq", 15),
            ("usage_softirq", 16),
            ("usage_steal", 17),
            ("usage_guest", 18),
            ("usage_guest_nice", 19),
        ];

        for (name, id) in field_columns {
            let column_schema = ColumnSchema::new(name, ConcreteDataType::int64_datatype(), true);

            builder.push_column_metadata(ColumnMetadata {
                column_schema,
                semantic_type: SemanticType::Field,
                column_id: id,
            });
        }

        // Time index column
        let mut time_metadata = HashMap::new();
        time_metadata.insert("greptime:time_index".to_string(), "true".to_string());

        let time_column = ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_nanosecond_datatype(),
            false,
        )
        .with_metadata(time_metadata);

        builder.push_column_metadata(ColumnMetadata {
            column_schema: time_column,
            semantic_type: SemanticType::Timestamp,
            column_id: 20,
        });

        // Set primary key: [region, hostname, datacenter, rack, os, arch, team, service, service_version, service_environment]
        builder.primary_key(vec![1, 0, 2, 3, 4, 5, 6, 7, 8, 9]);
        builder.primary_key_encoding(PrimaryKeyEncoding::Dense);

        builder.build().unwrap()
    }

    #[tokio::test]
    async fn test_write_flat_with_index() {
        let mut env = TestEnv::new().await;
        let object_store = env.init_object_store_manager();
        let file_path = RegionFilePathFactory::new(FILE_DIR.to_string(), PathType::Bare);
        let metadata = Arc::new(sst_region_metadata());
        let row_group_size = 50;

        // Create flat format RecordBatches
        let flat_batches = vec![
            new_record_batch_by_range(&["a", "d"], 0, 20),
            new_record_batch_by_range(&["b", "d"], 0, 20),
            new_record_batch_by_range(&["c", "d"], 0, 20),
            new_record_batch_by_range(&["c", "f"], 0, 40),
            new_record_batch_by_range(&["c", "h"], 100, 200),
        ];

        let flat_source = new_flat_source_from_record_batches(flat_batches);

        let write_opts = WriteOptions {
            row_group_size,
            ..Default::default()
        };

        let puffin_manager = env
            .get_puffin_manager()
            .build(object_store.clone(), file_path.clone());
        let intermediate_manager = env.get_intermediate_manager();

        let indexer_builder = IndexerBuilderImpl {
            build_type: IndexBuildType::Flush,
            metadata: metadata.clone(),
            row_group_size,
            puffin_manager,
            write_cache_enabled: false,
            intermediate_manager,
            index_options: IndexOptions {
                inverted_index: InvertedIndexOptions {
                    segment_row_count: 1,
                    ..Default::default()
                },
            },
            inverted_index_config: Default::default(),
            fulltext_index_config: Default::default(),
            bloom_filter_index_config: Default::default(),
        };

        let mut metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            metadata.clone(),
            IndexConfig::default(),
            indexer_builder,
            file_path.clone(),
            &mut metrics,
        )
        .await;

        let info = writer
            .write_all_flat(flat_source, &write_opts)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(200, info.num_rows);
        assert!(info.file_size > 0);
        assert!(info.index_metadata.file_size > 0);

        assert!(info.index_metadata.inverted_index.index_size > 0);
        assert_eq!(info.index_metadata.inverted_index.row_count, 200);
        assert_eq!(info.index_metadata.inverted_index.columns, vec![0]);

        assert!(info.index_metadata.bloom_filter.index_size > 0);
        assert_eq!(info.index_metadata.bloom_filter.row_count, 200);
        assert_eq!(info.index_metadata.bloom_filter.columns, vec![1]);

        assert_eq!(
            (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(199)
            ),
            info.time_range
        );
    }

    #[tokio::test]
    async fn test_read_with_override_sequence() {
        let mut env = TestEnv::new().await;
        let object_store = env.init_object_store_manager();
        let handle = sst_file_handle(0, 1000);
        let file_path = FixedPathProvider {
            region_file_id: handle.file_id(),
        };
        let metadata = Arc::new(sst_region_metadata());

        // Create batches with sequence 0 to trigger override functionality
        let batch1 = new_batch_with_custom_sequence(&["a", "d"], 0, 60, 0);
        let batch2 = new_batch_with_custom_sequence(&["b", "f"], 0, 40, 0);
        let source = new_source(&[batch1, batch2]);

        let write_opts = WriteOptions {
            row_group_size: 50,
            ..Default::default()
        };

        let mut metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            metadata.clone(),
            IndexConfig::default(),
            NoopIndexBuilder,
            file_path,
            &mut metrics,
        )
        .await;

        writer
            .write_all(source, None, &write_opts)
            .await
            .unwrap()
            .remove(0);

        // Read without override sequence (should read sequence 0)
        let builder = ParquetReaderBuilder::new(
            FILE_DIR.to_string(),
            PathType::Bare,
            handle.clone(),
            object_store.clone(),
        );
        let mut reader = builder.build().await.unwrap();
        let mut normal_batches = Vec::new();
        while let Some(batch) = reader.next_batch().await.unwrap() {
            normal_batches.push(batch);
        }

        // Read with override sequence using FileMeta.sequence
        let custom_sequence = 12345u64;
        let file_meta = handle.meta_ref();
        let mut override_file_meta = file_meta.clone();
        override_file_meta.sequence = Some(std::num::NonZero::new(custom_sequence).unwrap());
        let override_handle = FileHandle::new(
            override_file_meta,
            Arc::new(crate::sst::file_purger::NoopFilePurger),
        );

        let builder = ParquetReaderBuilder::new(
            FILE_DIR.to_string(),
            PathType::Bare,
            override_handle,
            object_store.clone(),
        );
        let mut reader = builder.build().await.unwrap();
        let mut override_batches = Vec::new();
        while let Some(batch) = reader.next_batch().await.unwrap() {
            override_batches.push(batch);
        }

        // Compare the results
        assert_eq!(normal_batches.len(), override_batches.len());
        for (normal, override_batch) in normal_batches.into_iter().zip(override_batches.iter()) {
            // Create expected batch with override sequence
            let expected_batch = {
                let num_rows = normal.num_rows();
                let mut builder = BatchBuilder::from(normal);
                builder
                    .sequences_array(Arc::new(UInt64Array::from_value(custom_sequence, num_rows)))
                    .unwrap();

                builder.build().unwrap()
            };

            // Override batch should match expected batch
            assert_eq!(*override_batch, expected_batch);
        }
    }

    #[tokio::test]
    async fn test_read_external_parquet_and_write_with_index() {
        use async_stream::try_stream;
        use object_store::ObjectStore;
        use smallvec::smallvec;
        use snafu::ResultExt;

        use crate::error::ArrowReaderSnafu;
        use crate::read::FlatSource;
        use crate::sst::file::{ColumnIndexMetadata, IndexType};
        use crate::sst::parquet::reader::{ParquetReaderBuilder, ReaderMetrics};

        common_telemetry::init_default_ut_logging();

        // Create ObjectStore with root set to /
        let builder = object_store::services::Fs::default().root("/");
        let read_store = ObjectStore::new(builder).unwrap().finish();

        let output_dir = "/Users/evenyag/Documents/test/bulk/output";
        let input_dir = "/Users/evenyag/Documents/test/bulk";
        let file_path = RegionFilePathFactory::new(output_dir.to_string(), PathType::Bare);
        let row_group_size = DEFAULT_ROW_GROUP_SIZE;

        // Parse file metadata from JSON
        let external_file_id = FileId::parse_str("264ccc3d-cc09-451e-b937-6221876bc6ba").unwrap();
        let external_region_id = store_api::storage::RegionId::from_u64(4398046511104);

        // Create FileHandle with the actual file metadata from JSON
        let external_handle = FileHandle::new(
            FileMeta {
                region_id: external_region_id,
                file_id: external_file_id,
                time_range: (
                    Timestamp::new_nanosecond(1686640080000000000),
                    Timestamp::new_nanosecond(1686657590000000000),
                ),
                level: 1,
                file_size: 62218019,
                max_row_group_uncompressed_size: 0,
                available_indexes: smallvec![IndexType::InvertedIndex],
                indexes: vec![ColumnIndexMetadata {
                    column_id: 0,
                    created_indexes: smallvec![IndexType::InvertedIndex],
                }],
                index_file_size: 114123,
                index_version: 0,
                num_rows: 7005000,
                num_row_groups: 69,
                sequence: std::num::NonZero::new(86400000),
                partition_expr: None,
                num_series: 4000,
            },
            Arc::new(NoopFilePurger),
        );

        // Use ParquetReaderBuilder to build reader input
        let reader_builder = ParquetReaderBuilder::new(
            input_dir.to_string(),
            PathType::Bare,
            external_handle.clone(),
            read_store.clone(),
        )
        .flat_format(true);

        let mut metrics = ReaderMetrics::default();
        let (context, mut selection) = reader_builder
            .build_reader_input(&mut metrics)
            .await
            .unwrap();
        let context = Arc::new(context);
        let file_path_for_error = context.file_path().to_string();

        // Create a stream from the row groups using try_stream macro
        let stream = try_stream! {
            while let Some((row_group_idx, row_selection)) = selection.pop_first() {
                let mut parquet_reader = context
                    .reader_builder()
                    .build(row_group_idx, Some(row_selection), None)
                    .await?;

                // Yield all record batches from this row group
                while let Some(record_batch_result) = parquet_reader.next() {
                    let record_batch = record_batch_result.context(ArrowReaderSnafu {
                        path: file_path_for_error.clone(),
                    })?;
                    yield record_batch;
                }
            }
        };

        // Create flat source from the stream
        let flat_source = FlatSource::Stream(Box::pin(stream));

        let write_opts = WriteOptions {
            row_group_size,
            ..Default::default()
        };

        let write_store = read_store;
        let metadata = Arc::new(build_cpu_benchmark_region_metadata());

        let index_aux_path = "/Users/evenyag/Documents/test/bulk/output/aux";
        let puffin_manager_factory = PuffinManagerFactory::new(index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intermediate_manager = IntermediateManager::init_fs(index_aux_path).await.unwrap();
        let puffin_manager = puffin_manager_factory.build(write_store.clone(), file_path.clone());

        let indexer_builder = IndexerBuilderImpl {
            build_type: IndexBuildType::Flush,
            metadata: metadata.clone(),
            row_group_size,
            puffin_manager,
            write_cache_enabled: false,
            intermediate_manager,
            index_options: IndexOptions {
                inverted_index: InvertedIndexOptions {
                    segment_row_count: 1024,
                    ..Default::default()
                },
            },
            inverted_index_config: Default::default(),
            fulltext_index_config: Default::default(),
            bloom_filter_index_config: Default::default(),
        };

        let mut write_metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            write_store.clone(),
            metadata.clone(),
            IndexConfig::default(),
            indexer_builder,
            file_path.clone(),
            &mut write_metrics,
        )
        .await;

        let info = writer
            .write_all_flat(flat_source, &write_opts)
            .await
            .unwrap()
            .remove(0);

        assert!(info.num_rows > 0);
        assert!(info.file_size > 0);
        assert!(info.index_metadata.file_size > 0);
        assert!(info.index_metadata.inverted_index.index_size > 0);
        assert_eq!(info.index_metadata.inverted_index.row_count, info.num_rows);
        // hostname (column 0) should have inverted index
        assert_eq!(info.index_metadata.inverted_index.columns, vec![0]);

        let region_file_id = RegionFileId::new(metadata.region_id, info.file_id);
        let sst_file_path = location::sst_file_path(output_dir, region_file_id, PathType::Bare);
        let index_file_path = location::index_file_path(
            output_dir,
            RegionIndexId::new(region_file_id, 0),
            PathType::Bare,
        );

        println!(
            "Successfully wrote {} rows with inverted index",
            info.num_rows
        );
        println!("Output SST file: {}", sst_file_path);
        println!("Output index file: {}", index_file_path);
        println!("File size: {} bytes", info.file_size);
        println!(
            "Index size: {} bytes",
            info.index_metadata.inverted_index.index_size
        );
    }

    #[tokio::test]
    async fn test_read_external_parquet_filter() {
        use object_store::ObjectStore;
        use smallvec::smallvec;

        use crate::sst::file::{ColumnIndexMetadata, IndexType};
        use crate::sst::parquet::reader::{ParquetReaderBuilder, ReaderMetrics};

        common_telemetry::init_default_ut_logging();

        // Create ObjectStore with root set to /
        let builder = object_store::services::Fs::default().root("/");
        let read_store = ObjectStore::new(builder).unwrap().finish();

        let input_dir = "/Users/evenyag/Documents/test/bulk/output/";

        // Parse file metadata from JSON
        let external_file_id = FileId::parse_str("cae27413-891f-4c4d-b89f-a013bfcd6439").unwrap();
        let external_region_id = store_api::storage::RegionId::from_u64(4398046511104);

        // Build region metadata
        let metadata = Arc::new(build_cpu_benchmark_region_metadata());

        // Create FileHandle with the actual file metadata from JSON
        let external_handle = FileHandle::new(
            FileMeta {
                region_id: external_region_id,
                file_id: external_file_id,
                time_range: (
                    Timestamp::new_nanosecond(1686640080000000000),
                    Timestamp::new_nanosecond(1686657590000000000),
                ),
                level: 1,
                file_size: 62021648,
                max_row_group_uncompressed_size: 0,
                available_indexes: smallvec![IndexType::InvertedIndex],
                indexes: vec![ColumnIndexMetadata {
                    column_id: 0,
                    created_indexes: smallvec![IndexType::InvertedIndex],
                }],
                index_file_size: 110066,
                // index_file_size: 114123,
                index_version: 0,
                num_rows: 7005000,
                num_row_groups: 69,
                sequence: std::num::NonZero::new(86400000),
                partition_expr: None,
                num_series: 4000,
            },
            Arc::new(NoopFilePurger),
        );

        // Build inverted index applier for hostname='host_1003'
        let filters = vec![col("hostname").eq(lit("host_1003"))];

        let index_aux_path = "/Users/evenyag/Documents/test/bulk/output/aux";
        let puffin_manager_factory = PuffinManagerFactory::new(index_aux_path, 4096, None, None)
            .await
            .unwrap();

        let inverted_index_applier = InvertedIndexApplierBuilder::new(
            input_dir.to_string(),
            PathType::Bare,
            read_store.clone(),
            metadata.as_ref(),
            HashSet::from_iter([0]), // hostname is column 0
            puffin_manager_factory.clone(),
        )
        .build(&filters)
        .unwrap()
        .map(Arc::new);

        // Use ParquetReaderBuilder to build reader input
        let reader_builder = ParquetReaderBuilder::new(
            input_dir.to_string(),
            PathType::Bare,
            external_handle.clone(),
            read_store.clone(),
        )
        .flat_format(true)
        .inverted_index_appliers([inverted_index_applier, None]);

        let mut metrics = ReaderMetrics::default();
        let (_context, selection) = reader_builder
            .build_reader_input(&mut metrics)
            .await
            .unwrap();
        common_telemetry::info!("selection len: {}", selection.row_group_count());
        common_telemetry::info!("metrics: {:?}", metrics);
        assert!(!selection.is_empty());
    }
}
