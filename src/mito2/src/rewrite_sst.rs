use std::collections::HashMap;
use std::sync::Arc;

use api::v1::SemanticType;
use common_base::readable_size::ReadableSize;
use common_time::Timestamp;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use mito2::access_layer::{OperationType, RegionFilePathFactory};
use mito2::config::{BloomFilterConfig, FulltextIndexConfig, InvertedIndexConfig};
use mito2::read::{BoxedBatchReader, Source};
use mito2::region::options::IndexOptions;
use mito2::sst::file::{FileHandle, FileId, FileMeta, IndexType, Level};
use mito2::sst::file_purger::{FilePurger, FilePurgerRef, PurgeRequest};
use mito2::sst::index::intermediate::IntermediateManager;
use mito2::sst::index::puffin_manager::PuffinManagerFactory;
use mito2::sst::index::IndexerBuilderImpl;
use mito2::sst::parquet::reader::ParquetReaderBuilder;
use mito2::sst::parquet::writer::ParquetWriter;
use mito2::sst::parquet::{WriteOptions, DEFAULT_ROW_GROUP_SIZE};
use object_store::services::Fs;
use object_store::ObjectStore;
use smallvec::SmallVec;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::region_request::PathType;
use store_api::storage::ColumnId;

fn new_fs_store(path: &str) -> ObjectStore {
    let builder = Fs::default();
    ObjectStore::new(builder.root(path)).unwrap().finish()
}

#[derive(Debug)]
struct NoopFilePurger;

impl FilePurger for NoopFilePurger {
    fn send_request(&self, _request: PurgeRequest) {}
}

fn new_noop_file_purger() -> FilePurgerRef {
    Arc::new(NoopFilePurger {})
}

/// Test util to create file handles.
fn new_file_handle(
    file_id: FileId,
    start_ts_millis: i64,
    end_ts_millis: i64,
    level: Level,
    file_size: u64,
    index_file_size: u64,
) -> FileHandle {
    let file_purger = new_noop_file_purger();
    FileHandle::new(
        FileMeta {
            region_id: 0.into(),
            file_id,
            time_range: (
                Timestamp::new_millisecond(start_ts_millis),
                Timestamp::new_millisecond(end_ts_millis),
            ),
            level,
            file_size,
            available_indexes: SmallVec::from_iter([
                IndexType::InvertedIndex,
                IndexType::BloomFilterIndex,
                IndexType::FulltextIndex,
            ]),
            index_file_size,
            num_rows: 4553997,
            num_row_groups: 45,
            sequence: None,
        },
        file_purger,
    )
}

fn create_region_metadata_from_json(fulltext_granularity: usize) -> Arc<RegionMetadata> {
    // Based on the JSON metadata provided:
    // - message (column_id=0): String with fulltext index
    // - level (column_id=1): String with inverted index
    // - target (column_id=2): String with bloom filter
    // - pod_name (column_id=3): String with bloom filter
    // - container_name (column_id=4): String with bloom filter
    // - pod_ip (column_id=5): String with bloom filter
    // - app.kubernetes.io/component (column_id=6): String with bloom filter
    // - app.kubernetes.io/instance (column_id=7): String with bloom filter
    // - app.kubernetes.io/managed-by (column_id=8): String with bloom filter
    // - app.kubernetes.io/name (column_id=9): String with bloom filter
    // - apps.kubernetes.io/pod-index (column_id=10): String with bloom filter
    // - controller-revision-hash (column_id=11): String with bloom filter
    // - helm.sh/chart (column_id=12): String with bloom filter
    // - timestamp (column_id=13): Timestamp(Nanosecond) - time index
    // - statefulset.kubernetes.io/pod-name (column_id=14): String

    // Create region metadata using the consuming builder pattern
    let mut message_metadata = HashMap::new();
    message_metadata.insert(
        "greptime:fulltext".to_string(),
        format!("{{\"enable\":true,\"analyzer\":\"English\",\"case-sensitive\":false,\"backend\":\"bloom\",\"granularity\":{fulltext_granularity},\"false-positive-rate-in-10000\":100}}")
    );

    let mut level_metadata = HashMap::new();
    level_metadata.insert("greptime:inverted_index".to_string(), "true".to_string());

    let mut timestamp_metadata = HashMap::new();
    timestamp_metadata.insert("greptime:time_index".to_string(), "true".to_string());

    let mut builder = RegionMetadataBuilder::new(4569845202944u64.into()); // region_id from JSON
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "message".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            )
            .with_metadata(message_metadata),
            semantic_type: SemanticType::Field,
            column_id: ColumnId::from(0u32),
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "level".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            )
            .with_metadata(level_metadata),
            semantic_type: SemanticType::Field,
            column_id: ColumnId::from(1u32),
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "timestamp".to_string(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
                false,
            )
            .with_metadata(timestamp_metadata),
            semantic_type: SemanticType::Timestamp,
            column_id: ColumnId::from(13u32),
        });

    // Add other columns with bloom filter indexes
    let bloom_filter_columns = [
        ("target", 2u32),
        ("pod_name", 3u32),
        ("container_name", 4u32),
        ("pod_ip", 5u32),
        ("app.kubernetes.io/component", 6u32),
        ("app.kubernetes.io/instance", 7u32),
        ("app.kubernetes.io/managed-by", 8u32),
        ("app.kubernetes.io/name", 9u32),
        ("apps.kubernetes.io/pod-index", 10u32),
        ("controller-revision-hash", 11u32),
        ("helm.sh/chart", 12u32),
        ("statefulset.kubernetes.io/pod-name", 14u32),
    ];

    for (name, id) in bloom_filter_columns {
        let mut metadata = HashMap::new();
        if id != 14 {
            // statefulset.kubernetes.io/pod-name doesn't have bloom filter
            metadata.insert(
                "greptime:skipping_index".to_string(),
                "{\"granularity\":10240,\"false-positive-rate-in-10000\":100,\"index-type\":\"BloomFilter\"}".to_string()
            );
        }

        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                name.to_string(),
                ConcreteDataType::string_datatype(),
                true,
            )
            .with_metadata(metadata),
            semantic_type: SemanticType::Field,
            column_id: ColumnId::from(id),
        });
    }

    Arc::new(builder.build().unwrap())
}

#[tokio::main]
async fn main() {
    common_telemetry::init_default_ut_logging();

    // Get environment variables - similar to read_sst.rs
    let file_dir = std::env::var("FILE_DIR").unwrap_or_else(|_| "inverted".to_string());
    let input_file_id = std::env::var("INPUT_FILE_ID")
        .unwrap_or_else(|_| "0235c947-9988-4d46-96a9-9fcee770896a".to_string());
    let file_root = std::env::var("FILE_ROOT")
        .unwrap_or_else(|_| "/Users/evenyag/Documents/test/etcd-fulltext/".to_string());
    let output_dir = std::env::var("OUTPUT_DIR")
        .unwrap_or_else(|_| "/Users/evenyag/Documents/test/etcd-fulltext/rewrite".to_string());
    let new_file_id = std::env::var("NEW_FILE_ID").unwrap_or_else(|_| FileId::random().to_string());
    let data_page_row_count = std::env::var("DATA_PAGE_ROW_COUNT")
        .ok()
        .and_then(|x| x.parse::<usize>().ok());
    let data_page_size = std::env::var("DATA_PAGE_SIZE")
        .ok()
        .and_then(|x| x.parse::<usize>().ok());
    let fulltext_granularity = std::env::var("FULLTEXT_GRAN")
        .ok()
        .and_then(|x| x.parse::<usize>().ok())
        .unwrap_or(1024);

    common_telemetry::info!("Reading SST from: {}/{}", file_root, input_file_id);
    common_telemetry::info!("Writing new SST to: {}", output_dir);
    common_telemetry::info!("New file ID: {}", new_file_id);
    common_telemetry::info!("granularity: {}", fulltext_granularity);

    // Create necessary components - same as read_sst.rs
    let input_object_store = new_fs_store(&file_root);

    // Get actual file sizes from object store
    let parsed_input_file_id = FileId::parse_str(&input_file_id).unwrap();
    let parquet_path = format!(
        "{}/0_0000000000/{}.parquet",
        file_dir.trim_end_matches('/'),
        parsed_input_file_id
    );
    let puffin_path = format!(
        "{}/0_0000000000/index/{}.puffin",
        file_dir.trim_end_matches('/'),
        parsed_input_file_id
    );

    let parquet_meta = input_object_store.stat(&parquet_path).await.unwrap();
    let puffin_meta = input_object_store.stat(&puffin_path).await.unwrap();

    let actual_file_size = parquet_meta.content_length();
    let actual_index_file_size = puffin_meta.content_length();

    let input_file_handle = new_file_handle(
        parsed_input_file_id,
        0,
        1000,
        1,
        actual_file_size,
        actual_index_file_size,
    );
    let output_object_store = new_fs_store(&output_dir);
    let region_metadata = create_region_metadata_from_json(fulltext_granularity);

    // Create PuffinManagerFactory for input reading
    let temp_dir = std::env::temp_dir().join("greptime_rewrite_sst");
    let puffin_factory = PuffinManagerFactory::new(
        temp_dir,
        1024 * 1024, // 1MB staging capacity
        None,        // no write buffer size limit
        None,        // no staging TTL
    )
    .await
    .unwrap();

    // Create parquet reader to read from existing SST
    let reader_builder = ParquetReaderBuilder::new(
        file_dir.clone(),
        PathType::Bare,
        input_file_handle,
        input_object_store,
    )
    .expected_metadata(Some(region_metadata.clone()));

    let reader = reader_builder.build().await.unwrap();

    // Create source from the reader
    let source = Source::Reader(Box::new(reader) as BoxedBatchReader);

    // Create path provider for output
    let path_provider = RegionFilePathFactory::new("0_0000000000".to_string(), PathType::Bare);

    // Create intermediate manager for inverted index
    let intermediate_manager = IntermediateManager::init_fs("index_tmp").await.unwrap();

    // Create indexer builder for output - following access_layer.rs pattern
    let indexer_builder = IndexerBuilderImpl {
        op_type: OperationType::Compact, // This is a rewrite/compaction operation
        metadata: region_metadata.clone(),
        row_group_size: DEFAULT_ROW_GROUP_SIZE, // Default row group size
        puffin_manager: puffin_factory.build(output_object_store.clone(), path_provider.clone()),
        intermediate_manager,
        index_options: IndexOptions::default(),
        inverted_index_config: InvertedIndexConfig::default(),
        fulltext_index_config: FulltextIndexConfig::default(),
        bloom_filter_index_config: BloomFilterConfig::default(),
    };

    // Create ParquetWriter
    let mut writer = ParquetWriter::new_with_object_store(
        output_object_store,
        region_metadata.clone(),
        indexer_builder,
        path_provider,
    )
    .await;

    // Write options
    let write_options = WriteOptions {
        write_buffer_size: ReadableSize::mb(8),
        row_group_size: DEFAULT_ROW_GROUP_SIZE,
        // 300MB max file size
        max_file_size: Some(1024 * 1024 * 300),
        data_page_row_count,
        data_page_size,
    };

    // Write all data from input SST to new SST
    common_telemetry::info!("Starting rewrite from SST to new SST...");
    let sst_infos = writer
        .write_all(source, None, &write_options)
        .await
        .unwrap();

    common_telemetry::info!("Successfully created {} SST files:", sst_infos.len());
    for sst_info in &sst_infos {
        common_telemetry::info!(
            "  File ID: {}, Size: {} bytes, Rows: {}, Row groups: {}, Index size: {} bytes",
            sst_info.file_id,
            sst_info.file_size,
            sst_info.num_rows,
            sst_info.num_row_groups,
            sst_info.index_metadata.file_size
        );
    }
}
