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

//! Behavioral coverage for series-index reconciliation.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use api::v1::helper::row;
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, Rows, SemanticType, WriteHint};
use common_time::{TimeToLive, Timestamp};
use object_store::ObjectStore;
use object_store::layers::mock::{self, MockLayerBuilder, oio};
use object_store::services::Memory;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metric_engine_consts::PRIMARY_KEY_ENCODING;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionPutRequest, RegionRequest};
use store_api::storage::RegionId;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

use super::catalog::{
    SeriesIndexCatalog, SeriesIndexEntry, load_catalog, range_index_path, series_catalog_path,
    series_index_path,
};
use super::maintenance::reconcile_series_indexes;
use super::purger::{purge_file, series_index_channel};
use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::region::{MitoRegionRef, RegionMap, RegionMapRef};
use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};
use crate::test_util::{CreateRequestBuilder, TestEnv, flush_region, rows_schema};

/// Builds real sparse SSTs; background maintenance is disabled so tests control publication.
async fn prepare_region(env: &mut TestEnv) -> (MitoEngine, MitoRegionRef, RegionMapRef) {
    let engine = env.create_engine(MitoConfig::default()).await;
    let metadata = Arc::new(sst_region_metadata_with_encoding(
        PrimaryKeyEncoding::Sparse,
    ));
    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();
    request.column_metadatas = metadata.column_metadatas.clone();
    request.primary_key = metadata.primary_key.clone();
    request
        .options
        .insert(PRIMARY_KEY_ENCODING.to_string(), "sparse".to_string());
    request
        .options
        .insert("memtable.type".to_string(), "bulk".to_string());
    request
        .options
        .insert("sst_format".to_string(), "flat".to_string());
    request
        .options
        .insert("compaction.type".to_string(), "twcs".to_string());
    request.options.insert(
        "compaction.twcs.time_window".to_string(),
        "100s".to_string(),
    );
    let full_schema = rows_schema(&request);
    let mut pk_column = full_schema[0].clone();
    pk_column.column_name = PRIMARY_KEY_COLUMN_NAME.to_string();
    pk_column.datatype = ColumnDataType::Binary.into();
    pk_column.semantic_type = SemanticType::Tag.into();
    let schema = vec![pk_column, full_schema[5].clone(), full_schema[4].clone()];
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    for ts in [1000, 2000] {
        engine
            .handle_request(
                region_id,
                RegionRequest::Put(RegionPutRequest {
                    rows: Rows {
                        schema: schema.clone(),
                        rows: vec![row(vec![
                            ValueData::BinaryValue(new_sparse_primary_key(
                                &["a", "x"],
                                &metadata,
                                10,
                                0,
                            )),
                            ValueData::TimestampMillisecondValue(ts),
                            ValueData::U64Value(1),
                        ])],
                    },
                    hint: Some(WriteHint {
                        primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse.into(),
                    }),
                    partition_expr_version: None,
                }),
            )
            .await
            .unwrap();
        flush_region(&engine, region_id, None).await;
    }
    let region = engine.get_region(region_id).unwrap();
    let regions = Arc::new(RegionMap::default());
    regions.insert_region(region.clone());
    (engine, region, regions)
}

#[tokio::test]
async fn test_reconcile_reuses_coverage_rejects_stale_publication_and_expires_series() {
    let mut env = TestEnv::with_prefix("series-reconcile").await;
    let (engine, region, regions) = prepare_region(&mut env).await;
    let store = ObjectStore::new(Memory::default()).unwrap().finish();
    let (purger, mut receiver) = series_index_channel(store.clone());
    let stats = reconcile_series_indexes(
        0,
        store.clone(),
        regions.clone(),
        region.clone(),
        Duration::from_secs(100),
        0,
        purger.clone(),
    )
    .await
    .unwrap();
    assert_eq!((2, 1), (stats.built_range, stats.built_series));
    let first = region.series_index_version();
    let first_id = *first.series_indexes.keys().next().unwrap();
    let path = series_index_path(region.region_id, first_id);
    let parquet =
        ParquetRecordBatchReaderBuilder::try_new(store.read(&path).await.unwrap().to_bytes())
            .unwrap();
    let footer = parquet
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap();
    let coverage: SeriesIndexEntry = serde_json::from_str(
        footer
            .iter()
            .find(|kv| kv.key == "greptime.series_index")
            .unwrap()
            .value
            .as_ref()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        (Timestamp::new_second(0), Timestamp::new_second(100)),
        (coverage.bucket_start, coverage.bucket_end)
    );
    assert_eq!(
        first.range_indexes,
        coverage
            .source_file_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>()
    );

    let stats = reconcile_series_indexes(
        0,
        store.clone(),
        regions.clone(),
        region.clone(),
        Duration::from_secs(100),
        0,
        purger.clone(),
    )
    .await
    .unwrap();
    assert_eq!((0, 0), (stats.built_range, stats.built_series));
    assert!(
        region
            .series_index_version()
            .series_indexes
            .contains_key(&first_id)
    );

    // A range file removed on close must be rebuilt even when its catalog entry survives.
    let range_path = range_index_path(
        region.region_id,
        *first.range_indexes.iter().next().unwrap(),
    );
    store.delete(&range_path).await.unwrap();
    let stats = reconcile_series_indexes(
        0,
        store.clone(),
        regions.clone(),
        region.clone(),
        Duration::from_secs(100),
        0,
        purger.clone(),
    )
    .await
    .unwrap();
    assert_eq!((1, 0), (stats.built_range, stats.built_series));
    assert!(store.exists(&range_path).await.unwrap());

    // A stale region map forces rejection after building different bucket coverage.
    let before_stale = region.series_index_version();
    reconcile_series_indexes(
        0,
        store.clone(),
        Arc::new(RegionMap::default()),
        region.clone(),
        Duration::from_secs(200),
        0,
        purger.clone(),
    )
    .await
    .unwrap();
    assert!(Arc::ptr_eq(&before_stale, &region.series_index_version()));
    assert!(purge_file(&store, receiver.try_recv().unwrap()).await);
    for id in &first.range_indexes {
        assert!(
            store
                .exists(&range_index_path(region.region_id, *id))
                .await
                .unwrap()
        );
    }
    drop(before_stale);

    let stats = reconcile_series_indexes(
        0,
        store.clone(),
        regions.clone(),
        region.clone(),
        Duration::from_secs(200),
        0,
        purger.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        (0, 1, 1),
        (stats.built_range, stats.built_series, stats.removed_series)
    );
    // An old snapshot pins the previous series file across replacement.
    assert!(receiver.try_recv().is_err());
    assert!(store.exists(&path).await.unwrap());
    drop(first);
    assert!(purge_file(&store, receiver.try_recv().unwrap()).await);
    assert!(!store.exists(&path).await.unwrap());

    let mut options = region.version().options.clone();
    options.ttl = Some(TimeToLive::Duration(Duration::from_secs(1)));
    region.version_control.alter_options(options);
    let stats = reconcile_series_indexes(
        0,
        store.clone(),
        regions,
        region.clone(),
        Duration::from_secs(200),
        202000,
        purger,
    )
    .await
    .unwrap();
    assert_eq!((1, 1), (stats.skipped_buckets, stats.removed_series));
    let current = region.series_index_version();
    assert!(current.series_indexes.is_empty());
    assert_eq!(2, current.range_indexes.len());
    let (catalog, repair) =
        load_catalog::<SeriesIndexCatalog>(&store, &series_catalog_path(region.region_id))
            .await
            .unwrap();
    assert!(!repair);
    assert!(catalog.indexes.is_empty());
    engine.stop().await.unwrap();
}

struct FailingSeriesWriter {
    inner: oio::Writer,
    fail: bool,
}

impl mock::Write for FailingSeriesWriter {
    async fn write(&mut self, buffer: mock::Buffer) -> mock::Result<()> {
        self.inner.write(buffer).await
    }

    async fn close(&mut self) -> mock::Result<mock::Metadata> {
        if self.fail {
            return Err(mock::Error::new(
                mock::ErrorKind::Unexpected,
                "injected series write failure",
            ));
        }
        self.inner.close().await
    }

    async fn abort(&mut self) -> mock::Result<()> {
        self.inner.abort().await
    }
}

#[tokio::test]
async fn test_failed_series_build_keeps_completed_sst_range_indexes() {
    let mut env = TestEnv::with_prefix("series-build-failure").await;
    let (engine, region, regions) = prepare_region(&mut env).await;
    let store = ObjectStore::new(Memory::default()).unwrap().finish();
    let layer = MockLayerBuilder::default()
        .writer_factory(Arc::new(|path, _, inner| {
            Box::new(FailingSeriesWriter {
                inner,
                fail: path.contains("/series/"),
            })
        }))
        .build()
        .unwrap();
    let failing_store = store.clone().layer(layer);
    let (purger, mut receiver) = series_index_channel(store.clone());
    assert!(
        reconcile_series_indexes(
            0,
            failing_store,
            regions.clone(),
            region.clone(),
            Duration::from_secs(100),
            0,
            purger.clone()
        )
        .await
        .is_err()
    );
    assert!(region.series_index_version().range_indexes.is_empty());
    assert!(receiver.try_recv().is_err());
    for file in region
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
    {
        let path = range_index_path(region.region_id, file.file_id().file_id());
        assert!(store.exists(&path).await.unwrap());
    }
    // A retry can rebuild and publish the complete snapshot.
    let stats = reconcile_series_indexes(
        0,
        store,
        regions,
        region,
        Duration::from_secs(100),
        0,
        purger,
    )
    .await
    .unwrap();
    assert_eq!((2, 1), (stats.built_range, stats.built_series));
    engine.stop().await.unwrap();
}
