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
use std::ops::Range;
use std::sync::Arc;

use api::v1::value::ValueData;
use api::v1::{ColumnSchema, Row, Rows, SemanticType};
use common_query::Output;
use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
use datatypes::prelude::{ConcreteDataType, ScalarVector};
use datatypes::vectors::TimestampMillisecondVector;
use object_store::layers::LoggingLayer;
use object_store::manager::ObjectStoreManager;
use object_store::services::S3;
use object_store::ObjectStore;
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    RegionCompactRequest, RegionCreateRequest, RegionDeleteRequest, RegionFlushRequest,
    RegionRequest,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::test_util::{
    build_rows_for_key, column_metadata_to_column_schema, put_rows, CreateRequestBuilder, TestEnv,
};

async fn put_and_flush(
    engine: &MitoEngine,
    region_id: RegionId,
    column_schemas: &[ColumnSchema],
    rows: Range<usize>,
) {
    let rows = Rows {
        schema: column_schemas.to_vec(),
        rows: build_rows_for_key("a", rows.start, rows.end, 0),
    };
    put_rows(engine, region_id, rows).await;

    let Output::AffectedRows(rows) = engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(0, rows);
}

async fn delete_and_flush(
    engine: &MitoEngine,
    region_id: RegionId,
    column_schemas: &[ColumnSchema],
    rows: Range<usize>,
) {
    let row_cnt = rows.len();
    let rows = Rows {
        schema: column_schemas.to_vec(),
        rows: build_rows_for_key("a", rows.start, rows.end, 0),
    };

    let deleted = engine
        .handle_request(
            region_id,
            RegionRequest::Delete(RegionDeleteRequest { rows }),
        )
        .await
        .unwrap();

    let Output::AffectedRows(rows_affected) = deleted else {
        unreachable!()
    };
    assert_eq!(row_cnt, rows_affected);

    let Output::AffectedRows(rows) = engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(0, rows);
}

async fn collect_stream_ts(stream: SendableRecordBatchStream) -> Vec<i64> {
    let mut res = Vec::new();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    for batch in batches {
        let ts_col = batch
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondVector>()
            .unwrap();
        res.extend(ts_col.iter_data().map(|t| t.unwrap().0.value()));
    }
    res
}

#[tokio::test]
async fn test_compaction_region() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 5 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    delete_and_flush(&engine, region_id, &column_schemas, 15..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 15..25).await;

    let output = engine
        .handle_request(region_id, RegionRequest::Compact(RegionCompactRequest {}))
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(0)));

    let scanner = engine.scanner(region_id, ScanRequest::default()).unwrap();
    assert_eq!(
        1,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();

    let vec = collect_stream_ts(stream).await;
    assert_eq!((0..25).map(|v| v * 1000).collect::<Vec<_>>(), vec);
}

fn new_string_value_create_request() -> RegionCreateRequest {
    let column_metadatas = vec![
        ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                "ts".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 0,
        },
        ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                format!("value"),
                ConcreteDataType::binary_datatype(),
                false,
            ),
            semantic_type: SemanticType::Field,
            column_id: 1,
        },
    ];

    RegionCreateRequest {
        // We use empty engine name as we already locates the engine.
        engine: String::new(),
        column_metadatas,
        primary_key: vec![],
        options: HashMap::new(),
        region_dir: "data".to_string(),
    }
}

fn new_value_by_number(number: i64, value_size: usize) -> Vec<u8> {
    let data = number.to_string().into_bytes();
    let mut dst = Vec::with_capacity(value_size);
    while dst.len() < value_size {
        if dst.len() + data.len() < value_size {
            dst.extend_from_slice(&data);
        } else {
            dst.extend_from_slice(&data[..value_size - dst.len()]);
        }
    }
    dst
}

fn build_value_rows(start: i64, num_rows: i64, value_size: usize) -> Vec<Row> {
    (start..start + num_rows)
        .map(|i| api::v1::Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(i * 1000)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::BinaryValue(new_value_by_number(i, value_size))),
                },
            ],
        })
        .collect()
}

async fn put_and_flush_value(
    engine: &MitoEngine,
    region_id: RegionId,
    column_schemas: &[ColumnSchema],
    start: i64,
    num_rows: i64,
    value_size: usize,
) {
    let rows = Rows {
        schema: column_schemas.to_vec(),
        rows: build_value_rows(start, num_rows, value_size),
    };
    put_rows(engine, region_id, rows).await;

    let Output::AffectedRows(rows) = engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(0, rows);
}

#[tokio::test]
async fn test_files_larger_than_write_buffer() {
    common_telemetry::init_default_ut_logging();

    let mut builder = S3::default();
    builder
        .root("/mito2-unittest")
        .access_key_id(&std::env::var("GT_S3_ACCESS_KEY_ID").unwrap())
        .secret_access_key(&std::env::var("GT_S3_ACCESS_KEY").unwrap())
        .region(&std::env::var("GT_S3_REGION").unwrap())
        .bucket(&std::env::var("GT_S3_BUCKET").unwrap());

    let object_store = ObjectStore::new(builder).unwrap().finish().layer(
        LoggingLayer::default()
            .with_error_level(Some("debug"))
            .unwrap(),
    );
    let object_store_manager = ObjectStoreManager::new("default", object_store);

    let mut env = TestEnv::with_object_store_manager(Arc::new(object_store_manager));
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = new_string_value_create_request();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let rows_per_sst = 1024 * 8;
    // Flush 5 SSTs for compaction.
    put_and_flush_value(&engine, region_id, &column_schemas, 0, rows_per_sst, 1024).await;
    put_and_flush_value(
        &engine,
        region_id,
        &column_schemas,
        rows_per_sst,
        rows_per_sst * 2,
        1024,
    )
    .await;
    put_and_flush_value(
        &engine,
        region_id,
        &column_schemas,
        rows_per_sst * 2,
        rows_per_sst * 3,
        1024,
    )
    .await;
    put_and_flush_value(
        &engine,
        region_id,
        &column_schemas,
        rows_per_sst * 3,
        rows_per_sst * 4,
        1024,
    )
    .await;
    put_and_flush_value(
        &engine,
        region_id,
        &column_schemas,
        rows_per_sst * 4,
        rows_per_sst * 5,
        1024,
    )
    .await;

    let output = engine
        .handle_request(region_id, RegionRequest::Compact(RegionCompactRequest {}))
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(0)));

    let scanner = engine.scanner(region_id, ScanRequest::default()).unwrap();
    assert_eq!(
        1,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
}
