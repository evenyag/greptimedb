use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use common_function::function::FunctionRef;
use common_function::function_factory::ScalarFunctionFactory;
use common_function::scalars::matches_term::MatchesTermFunction;
use common_time::Timestamp;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{col, lit, BinaryExpr, Expr, Operator};
use mito2::cache::{CacheManagerBuilder, CacheStrategy};
use mito2::read::BatchReader;
use mito2::sst::file::{FileHandle, FileId, FileMeta, IndexType, Level};
use mito2::sst::file_purger::{FilePurger, FilePurgerRef, PurgeRequest};
use mito2::sst::index::bloom_filter::applier::BloomFilterIndexApplierBuilder;
use mito2::sst::index::fulltext_index::applier::builder::FulltextIndexApplierBuilder;
use mito2::sst::index::inverted_index::applier::builder::InvertedIndexApplierBuilder;
use mito2::sst::parquet::reader::ParquetReaderBuilder;
use object_store::services::Fs;
use object_store::util::with_instrument_layers;
use object_store::ObjectStore;
use smallvec::SmallVec;
use store_api::metadata::RegionMetadata;
use store_api::region_request::PathType;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

fn new_fs_store(path: &str) -> ObjectStore {
    let builder = Fs::default();
    let store = ObjectStore::new(builder.root(path)).unwrap().finish();
    with_instrument_layers(store, false)
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

fn create_region_metadata_from_json() -> Arc<RegionMetadata> {
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

    use api::v1::SemanticType;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};

    // Create region metadata using the consuming builder pattern
    let mut message_metadata = HashMap::new();
    message_metadata.insert(
        "greptime:fulltext".to_string(),
        "{\"enable\":true,\"analyzer\":\"English\",\"case-sensitive\":false,\"backend\":\"bloom\",\"granularity\":10240,\"false-positive-rate-in-10000\":100}".to_string()
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

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    common_telemetry::init_default_ut_logging();

    let file_dir = std::env::var("FILE_DIR").unwrap_or_else(|_| "inverted".to_string());
    let file_id = std::env::var("FILE_ID")
        .unwrap_or_else(|_| "0235c947-9988-4d46-96a9-9fcee770896a".to_string());
    let file_root = std::env::var("FILE_ROOT")
        .unwrap_or_else(|_| "/Users/evenyag/Documents/test/etcd-fulltext/".to_string());
    let loops = std::env::var("LOOPS")
        .ok()
        .and_then(|x| x.parse::<usize>().ok())
        .unwrap_or(1);
    let use_fulltext = std::env::var("USE_FULLTEXT")
        .ok()
        .and_then(|x| x.parse::<bool>().ok())
        .unwrap_or(true);

    let object_store = new_fs_store(&file_root);

    // Get actual file sizes from object store
    let parsed_file_id = FileId::parse_str(&file_id).unwrap();
    let parquet_path = format!(
        "{}/0_0000000000/{}.parquet",
        file_dir.trim_end_matches('/'),
        parsed_file_id
    );
    let puffin_path = format!(
        "{}/0_0000000000/index/{}.puffin",
        file_dir.trim_end_matches('/'),
        parsed_file_id
    );

    let parquet_meta = object_store.stat(&parquet_path).await.unwrap();
    let puffin_meta = object_store.stat(&puffin_path).await.unwrap();

    let actual_file_size = parquet_meta.content_length();
    let actual_index_file_size = puffin_meta.content_length();

    common_telemetry::info!(
        "Read {} ({} bytes) and {} ({} bytes)",
        parquet_path,
        actual_file_size,
        puffin_path,
        actual_index_file_size
    );

    let file_handle = new_file_handle(
        parsed_file_id,
        0,
        1000,
        1,
        actual_file_size,
        actual_index_file_size,
    );
    let region_metadata = create_region_metadata_from_json();

    // Hard-coded projection: only read level (column_id=1) and message (column_id=0)
    let projection = Some(vec![ColumnId::from(0u32), ColumnId::from(1u32)]);
    // let projection = Some(vec![ColumnId::from(1u32)]);

    // Hard-coded predicate for (level = 'error' OR level = 'warn')
    let level_filter = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(col("level").eq(lit("error"))),
        op: Operator::Or,
        right: Box::new(col("level").eq(lit("warn"))),
    });
    // Build fulltext index applier for message column using matches_term
    let matches_term_func = Arc::new(
        ScalarFunctionFactory::from(Arc::new(MatchesTermFunction) as FunctionRef)
            .provide(Default::default()),
    );

    let fulltext_filter = Expr::ScalarFunction(ScalarFunction {
        args: vec![
            Expr::Column(Column::from_name("message")),
            Expr::Literal(ScalarValue::Utf8(Some("leader failed".to_string()))),
        ],
        func: matches_term_func,
    });
    let filters = vec![level_filter, fulltext_filter.clone()];
    let predicate = Some(Predicate::new(filters.clone()));

    // let filters = vec![level_filter];
    // let predicate = Some(Predicate::new(filters.clone()));

    // Create PuffinManagerFactory with proper parameters
    let temp_dir = Path::new(&file_root);
    let puffin_factory = mito2::sst::index::puffin_manager::PuffinManagerFactory::new(
        temp_dir,
        64 * 1024 * 1024, // 64MB staging capacity
        None,             // no write buffer size limit
        None,             // no staging TTL
    )
    .await
    .unwrap();

    let cache = Arc::new(
        CacheManagerBuilder::default()
            .index_content_size(64 * 1024 * 1024)
            .index_content_page_size(64 * 1024)
            .build(),
    );

    // Build inverted index applier for level column
    let inverted_index_applier = InvertedIndexApplierBuilder::new(
        file_dir.clone(),
        PathType::Bare,
        object_store.clone(),
        region_metadata.as_ref(),
        HashSet::from([ColumnId::from(1u32)]), // level column has inverted index
        puffin_factory.clone(),
    )
    .build(&filters)
    .ok()
    .flatten()
    .map(Arc::new);

    let fulltext_index_applier = FulltextIndexApplierBuilder::new(
        file_dir.clone(),
        PathType::Bare,
        object_store.clone(),
        puffin_factory.clone(),
        region_metadata.as_ref(),
    )
    .with_bloom_filter_cache(cache.bloom_filter_index_cache().cloned())
    .build(&[fulltext_filter])
    .ok()
    .flatten()
    .map(Arc::new);

    // Build bloom filter applier
    let bloom_filter_applier = BloomFilterIndexApplierBuilder::new(
        file_dir.clone(),
        PathType::Bare,
        object_store.clone(),
        region_metadata.as_ref(),
        puffin_factory.clone(),
    )
    .build(&filters)
    .ok()
    .flatten()
    .map(Arc::new);

    for i in 0..loops {
        let start = Instant::now();
        let mut builder = ParquetReaderBuilder::new(
            file_dir.clone(),
            PathType::Bare,
            file_handle.clone(),
            object_store.clone(),
        )
        .projection(projection.clone())
        .predicate(predicate.clone())
        .expected_metadata(Some(region_metadata.clone()));

        // Apply inverted index applier if available
        if let Some(applier) = &inverted_index_applier {
            builder = builder.inverted_index_applier(Some(applier.clone()));
        }

        // Apply fulltext index applier if available
        if use_fulltext {
            common_telemetry::info!("Use fulltext");

            if let Some(applier) = &fulltext_index_applier {
                builder = builder.fulltext_index_applier(Some(applier.clone()));
            }
        }

        // Apply bloom filter applier if available
        if let Some(applier) = &bloom_filter_applier {
            builder = builder.bloom_filter_index_applier(Some(applier.clone()));
        }

        let mut reader = builder.build().await.unwrap();

        common_telemetry::info!("loop: {}, build reader cost is {:?}", i, start.elapsed());
        let mut total_batches = 0;
        let mut total_rows = 0;

        while let Some(batch) = reader.next_batch().await.unwrap() {
            if total_rows == 0 {
                common_telemetry::info!(
                    "loop: {}, number of fields is {:?}",
                    i,
                    batch.fields().len()
                );
            }

            total_batches += 1;
            total_rows += batch.num_rows();
            // common_telemetry::info!(
            //     "Batch {}: {} rows, primary_key: {:?}",
            //     total_batches,
            //     batch.num_rows(),
            //     std::str::from_utf8(batch.primary_key()).unwrap_or("<invalid utf8>")
            // );
        }

        common_telemetry::info!(
            "loop: {}, scan reader done, {} batches, {} rows, total cost {:?}",
            i,
            total_batches,
            total_rows,
            start.elapsed()
        );
    }
}
