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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::Rows;
use common_base::Plugins;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use common_test_util::temp_dir::create_temp_dir;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit, Expr};
use futures::future::try_join_all;
use futures::TryStreamExt;
use object_store::manager::ObjectStoreManager;
use object_store::services::Fs;
use object_store::ObjectStore;
use store_api::region_engine::{PrepareRequest, RegionEngine, RegionRole, RegionScanner};
use store_api::region_request::{
    RegionCloseRequest, RegionOpenRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::{RegionId, ScanRequest, TimeSeriesDistribution};
use tokio::sync::oneshot;

use crate::compaction::compactor::{open_compaction_region, OpenCompactionRegionRequest};
use crate::config::MitoConfig;
use crate::engine::{MitoEngine, ScanRegion};
use crate::error;
use crate::region::options::RegionOptions;
use crate::test_util::{
    build_rows, flush_region, mock_schema_metadata_manager, put_rows, reopen_region, rows_schema,
    CreateRequestBuilder, RaftEngineLogStoreFactory, TestEnv,
};

#[tokio::test]
async fn test_engine_open_empty() {
    let mut env = TestEnv::with_prefix("open-empty");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir: "empty".to_string(),
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(StatusCode::RegionNotFound, err.status_code());
    let err = engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap_err();
    assert_eq!(StatusCode::RegionNotFound, err.status_code());
    let role = engine.role(region_id);
    assert_eq!(role, None);
}

#[tokio::test]
async fn test_engine_open_existing() {
    let mut env = TestEnv::with_prefix("open-exiting");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_engine_reopen_region() {
    let mut env = TestEnv::with_prefix("reopen-region");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    reopen_region(&engine, region_id, region_dir, false, Default::default()).await;
    assert!(engine.is_region_exists(region_id));
}

#[tokio::test]
async fn test_engine_open_readonly() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    reopen_region(&engine, region_id, region_dir, false, Default::default()).await;

    // Region is readonly.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 2),
    };
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: rows.clone(),
                hint: None,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(StatusCode::RegionNotReady, err.status_code());

    assert_eq!(Some(RegionRole::Follower), engine.role(region_id));
    // Converts region to leader.
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    assert_eq!(Some(RegionRole::Leader), engine.role(region_id));

    put_rows(&engine, region_id, rows).await;
}

#[tokio::test]
async fn test_engine_region_open_with_options() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Close the region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    // Open the region again with options.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::from([("ttl".to_string(), "4d".to_string())]),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(
        region.version().options.ttl,
        Some(Duration::from_secs(3600 * 24 * 4).into())
    );
}

#[tokio::test]
async fn test_engine_region_open_with_custom_store() {
    let mut env = TestEnv::new();
    let engine = env
        .create_engine_with_multiple_object_stores(MitoConfig::default(), None, None, &["Gcs"])
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("storage", "Gcs")
        .build();
    let region_dir = request.region_dir.clone();

    // Create a custom region.
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    // Close the custom region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    // Open the custom region.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::from([("storage".to_string(), "Gcs".to_string())]),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    // The region should not be opened with the default object store.
    let region = engine.get_region(region_id).unwrap();
    let object_store_manager = env.get_object_store_manager().unwrap();
    assert!(!object_store_manager
        .default_object_store()
        .exists(region.access_layer.region_dir())
        .await
        .unwrap());
    assert!(object_store_manager
        .find("Gcs")
        .unwrap()
        .exists(region.access_layer.region_dir())
        .await
        .unwrap());
}

#[tokio::test]
async fn test_open_region_skip_wal_replay() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    flush_region(&engine, region_id, None).await;

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 5),
    };
    put_rows(&engine, region_id, rows).await;

    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    // Skip the WAL replay .
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir: region_dir.to_string(),
                options: Default::default(),
                skip_wal_replay: true,
            }),
        )
        .await
        .unwrap();

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());

    // Replay the WAL.
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    // Open the region again with options.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: Default::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_open_region_wait_for_opening_region_ok() {
    let mut env = TestEnv::with_prefix("wait-for-opening-region-ok");
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let worker = engine.inner.workers.worker(region_id);
    let (tx, rx) = oneshot::channel();
    let opening_regions = worker.opening_regions().clone();
    opening_regions.insert_sender(region_id, tx.into());
    assert!(engine.is_region_opening(region_id));

    let handle_open = tokio::spawn(async move {
        engine
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: String::new(),
                    region_dir: "empty".to_string(),
                    options: HashMap::default(),
                    skip_wal_replay: false,
                }),
            )
            .await
    });

    // Wait for conditions
    while opening_regions.sender_len(region_id) != 2 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let senders = opening_regions.remove_sender(region_id);
    for sender in senders {
        sender.send(Ok(0));
    }

    assert_eq!(handle_open.await.unwrap().unwrap().affected_rows, 0);
    assert_eq!(rx.await.unwrap().unwrap(), 0);
}

#[tokio::test]
async fn test_open_region_wait_for_opening_region_err() {
    let mut env = TestEnv::with_prefix("wait-for-opening-region-err");
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let worker = engine.inner.workers.worker(region_id);
    let (tx, rx) = oneshot::channel();
    let opening_regions = worker.opening_regions().clone();
    opening_regions.insert_sender(region_id, tx.into());
    assert!(engine.is_region_opening(region_id));

    let handle_open = tokio::spawn(async move {
        engine
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: String::new(),
                    region_dir: "empty".to_string(),
                    options: HashMap::default(),
                    skip_wal_replay: false,
                }),
            )
            .await
    });

    // Wait for conditions
    while opening_regions.sender_len(region_id) != 2 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let senders = opening_regions.remove_sender(region_id);
    for sender in senders {
        sender.send(Err(error::RegionNotFoundSnafu { region_id }.build()));
    }

    assert_eq!(
        handle_open.await.unwrap().unwrap_err().status_code(),
        StatusCode::RegionNotFound
    );
    assert_eq!(
        rx.await.unwrap().unwrap_err().status_code(),
        StatusCode::RegionNotFound
    );
}

#[tokio::test]
async fn test_open_compaction_region() {
    let mut env = TestEnv::new();
    let mut mito_config = MitoConfig::default();
    mito_config
        .sanitize(&env.data_home().display().to_string())
        .unwrap();

    let engine = env.create_engine(mito_config.clone()).await;

    let region_id = RegionId::new(1, 1);
    let schema_metadata_manager = env.get_schema_metadata_manager();
    schema_metadata_manager
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Close the region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    let object_store_manager = env.get_object_store_manager().unwrap();

    let req = OpenCompactionRegionRequest {
        region_id,
        region_dir: region_dir.clone(),
        region_options: RegionOptions::default(),
        max_parallelism: 1,
    };

    let compaction_region = open_compaction_region(
        &req,
        &mito_config,
        object_store_manager.clone(),
        schema_metadata_manager,
    )
    .await
    .unwrap();

    assert_eq!(region_id, compaction_region.region_id);
}

#[derive(Debug)]
enum ScanType {
    SeqScan,
    UnorderedScan,
    UnorderedMultiSeriesScan,
}

#[tokio::test]
async fn test_engine_prom_seq_scan() {
    run_engine_prom_scan(ScanType::SeqScan).await;
}

#[tokio::test]
async fn test_engine_prom_unordered_scan() {
    run_engine_prom_scan(ScanType::UnorderedScan).await;
}

#[tokio::test]
async fn test_engine_prom_unordered_multi_series_scan() {
    run_engine_prom_scan(ScanType::UnorderedMultiSeriesScan).await;
}

#[tokio::test]
async fn test_engine_prombench_seq_scan() {
    run_engine_prombench_scan(ScanType::SeqScan).await;
}

#[tokio::test]
async fn test_engine_prombench_unordered_scan() {
    run_engine_prombench_scan(ScanType::UnorderedScan).await;
}

#[tokio::test]
async fn test_engine_prombench_unordered_multi_series_scan() {
    run_engine_prombench_scan(ScanType::UnorderedMultiSeriesScan).await;
}

async fn run_engine_prom_scan(scan_type: ScanType) {
    common_telemetry::init_default_ut_logging();

    let test_dir = create_temp_dir("prom");
    let data_home = test_dir.path();
    let wal_path = data_home.join("wal");
    let factory = RaftEngineLogStoreFactory;
    let log_store = factory.create_log_store(wal_path).await;
    let data_path = "/home/yangyw/greptime/data-prom/data".to_string();
    let builder = Fs::default().root(&data_path);
    let object_store = ObjectStore::new(builder).unwrap().finish();
    let object_store_manager = Arc::new(ObjectStoreManager::new("default", object_store));
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
    let engine = MitoEngine::new(
        "/home/yangyw/greptime/data-prom/",
        MitoConfig::default(),
        Arc::new(log_store),
        object_store_manager,
        schema_metadata_manager,
        Plugins::new(),
    )
    .await
    .unwrap();

    let region_id = RegionId::new(1024, 0);
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir: "greptime/public/1024/1024_0000000000/data/".to_string(),
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    let request = ScanRequest {
        projection: Some(vec![
            4, 5, 6, 0, 1, 7, 24, 25, 8, 49, 9, 10, 26, 11, 12, 18, 14, 15, 16,
        ]),
        filters: vec![
            col("namespace").eq(lit("app-0")),
            col("cluster").eq(lit("eks-us-east-1-qa1")),
            col("greptime_timestamp").gt_eq(Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(1743439965000),
                None,
            ))),
            col("greptime_timestamp").lt_eq(Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(1743566580000),
                None,
            ))),
            col("__table_id").eq(Expr::Literal(ScalarValue::UInt32(Some(1085)))),
        ],
        output_ordering: None,
        limit: None,
        series_row_selector: None,
        sequence: None,
        distribution: Some(TimeSeriesDistribution::PerSeries),
    };
    let loops = std::env::var("LOOPS")
        .map(|v| v.parse::<usize>().unwrap_or(1))
        .unwrap_or(1);

    let loop_start = Instant::now();
    for i in 0..loops {
        let scan_region = engine.scan_region(region_id, request.clone()).unwrap();
        match scan_type {
            ScanType::SeqScan => test_seq_scan(scan_region, i).await,
            ScanType::UnorderedScan => test_unordered_scan(scan_region, i, false).await,
            ScanType::UnorderedMultiSeriesScan => test_unordered_scan(scan_region, i, true).await,
        }
    }
    common_telemetry::info!(
        "Loop {} times, scan type: {:?}, elapsed time: {:?}",
        loops,
        scan_type,
        loop_start.elapsed()
    );
}

async fn run_engine_prombench_scan(scan_type: ScanType) {
    common_telemetry::init_default_ut_logging();

    let test_dir = create_temp_dir("prom");
    let data_home = test_dir.path();
    let wal_path = data_home.join("wal");
    let factory = RaftEngineLogStoreFactory;
    let log_store = factory.create_log_store(wal_path).await;
    let data_path = "/home/yangyw/greptime/data-prombench/data".to_string();
    let builder = Fs::default().root(&data_path);
    let object_store = ObjectStore::new(builder).unwrap().finish();
    let object_store_manager = Arc::new(ObjectStoreManager::new("default", object_store));
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
    let engine = MitoEngine::new(
        "/home/yangyw/greptime/data-prombench/",
        MitoConfig::default(),
        Arc::new(log_store),
        object_store_manager,
        schema_metadata_manager,
        Plugins::new(),
    )
    .await
    .unwrap();

    let region_id = RegionId::new(1024, 0);
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir: "greptime/public/1024/1024_0000000000/data/".to_string(),
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    let request = ScanRequest {
        projection: Some(vec![2, 5, 0, 1, 3, 25, 4]),
        filters: vec![
            col("region").eq(lit("us-west-2")),
            col("mode").eq(lit("idle")),
            col("greptime_timestamp").gt_eq(Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(1742550240000),
                None,
            ))),
            col("greptime_timestamp").lt_eq(Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(1742552700000),
                None,
            ))),
            col("__table_id").eq(Expr::Literal(ScalarValue::UInt32(Some(1032)))),
        ],
        output_ordering: None,
        limit: None,
        series_row_selector: None,
        sequence: None,
        distribution: Some(TimeSeriesDistribution::PerSeries),
    };
    let loops = std::env::var("LOOPS")
        .map(|v| v.parse::<usize>().unwrap_or(1))
        .unwrap_or(1);

    let loop_start = Instant::now();
    for i in 0..loops {
        let scan_region = engine.scan_region(region_id, request.clone()).unwrap();
        match scan_type {
            ScanType::SeqScan => test_seq_scan(scan_region, i).await,
            ScanType::UnorderedScan => test_unordered_scan(scan_region, i, false).await,
            ScanType::UnorderedMultiSeriesScan => test_unordered_scan(scan_region, i, true).await,
        }
    }
    common_telemetry::info!(
        "Loop {} times, scan type: {:?}, elapsed time: {:?}",
        loops,
        scan_type,
        loop_start.elapsed()
    );
}

async fn test_seq_scan(scan_region: ScanRegion, run: usize) {
    let start = Instant::now();
    let mut seq_scan = scan_region.seq_scan().unwrap();
    seq_scan
        .prepare(PrepareRequest {
            ranges: None,
            distinguish_partition_range: None,
            target_partitions: Some(8),
        })
        .unwrap();
    let metrics_set = ExecutionPlanMetricsSet::default();
    let mut stream = seq_scan.scan_partition(&metrics_set, 0).unwrap();

    let mut num_rows = 0;
    while let Some(rb) = stream.try_next().await.unwrap() {
        num_rows += rb.num_rows();
    }

    common_telemetry::info!(
        "SeqScan per-series loop {} scan {} rows, total time: {:?}",
        run,
        num_rows,
        start.elapsed()
    );
}

async fn test_unordered_scan(scan_region: ScanRegion, run: usize, multi_series: bool) {
    let num_target_partitions = 8;

    let start = Instant::now();
    let mut unordered_scan = scan_region.unordered_scan().unwrap();
    let ranges: Vec<_> = unordered_scan
        .properties()
        .partitions
        .iter()
        .flatten()
        .cloned()
        .collect();
    let actual_part_num = ranges.len().min(num_target_partitions);
    let mut partition_ranges = vec![vec![]; actual_part_num];
    for (i, range) in ranges.into_iter().enumerate() {
        partition_ranges[i % actual_part_num].push(range);
    }
    common_telemetry::info!(
        "Test series scan, loop {}, actual_part_num: {}",
        run,
        actual_part_num
    );

    unordered_scan
        .prepare(PrepareRequest {
            ranges: Some(partition_ranges),
            distinguish_partition_range: None,
            target_partitions: Some(num_target_partitions),
        })
        .unwrap();
    let num_partitions = unordered_scan.properties().num_partitions();
    let mut tasks = Vec::with_capacity(num_partitions);
    let metrics_set = ExecutionPlanMetricsSet::default();
    for i in 0..num_partitions {
        let mut stream = if multi_series {
            unordered_scan
                .scan_partition_multi_series(&metrics_set, i)
                .unwrap()
        } else {
            unordered_scan.scan_partition(&metrics_set, i).unwrap()
        };

        let task = common_runtime::spawn_global(async move {
            let mut num_rows = 0;
            while let Some(rb) = stream.try_next().await.unwrap() {
                num_rows += rb.num_rows();
            }
            num_rows
        });
        tasks.push(task);
    }
    let results = try_join_all(tasks).await.unwrap();
    let num_rows = results.iter().sum::<usize>();

    common_telemetry::info!(
        "SeriesScan per-series loop {} scan {} rows, total time: {:?}",
        run,
        num_rows,
        start.elapsed()
    );
}
