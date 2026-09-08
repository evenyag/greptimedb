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

use std::sync::Arc;
use std::time::Duration;

use api::v1::helper::row;
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, Rows, SemanticType, WriteHint};
use object_store::ObjectStore;
use object_store::layers::mock::{self, MockLayerBuilder, oio};
use object_store::services::Memory;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metric_engine_consts::PRIMARY_KEY_ENCODING;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionPutRequest, RegionRequest};
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use store_api::storage::{FileId, RegionId};

use super::catalog::{
    delete_catalogs, load_catalog, load_version_control, range_catalog_path, range_index_path,
    series_catalog_path, series_index_path,
};
use super::maintenance::reconcile_series_indexes;
use super::purger::series_index_channel;
use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::memtable::MemtableBuilderProvider;
use crate::region::opener::RegionOpener;
use crate::region::{MitoRegionRef, RegionMap};
use crate::schedule::scheduler::LocalScheduler;
use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};
use crate::test_util::{
    CreateRequestBuilder, TestEnv, flush_region, noop_partition_expr_fetcher, rows_schema,
};
use crate::time_provider::StdTimeProvider;
use crate::wal::Wal;

/// Builds real sparse SSTs; background maintenance is disabled so tests control publication.
async fn prepare_region(env: &mut TestEnv) -> (MitoEngine, MitoRegionRef) {
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
    (engine, region)
}

#[tokio::test]
async fn test_reconcile_restores_and_reuses_indexes() {
    let mut env = TestEnv::with_prefix("series-reconcile").await;
    let (engine, region) = prepare_region(&mut env).await;
    let store = ObjectStore::new(Memory::default()).unwrap().finish();
    let (purger, _receiver) = series_index_channel(store.clone());
    let stats = reconcile_series_indexes(
        0,
        store.clone(),
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

    let missing_paths = [
        range_index_path(
            region.region_id,
            *first.range_indexes.iter().next().unwrap(),
        ),
        series_index_path(region.region_id, first_id),
    ];
    for path in &missing_paths {
        store.delete(path).await.unwrap();
    }

    // Restore catalog entries even when their index files are missing.
    let config = Arc::new(MitoConfig::default());
    let crate::test_util::LogStoreImpl::RaftEngine(log_store) = env.get_log_store().unwrap() else {
        unreachable!();
    };
    let reopened = RegionOpener::new(
        region.region_id,
        region.table_dir(),
        region.access_layer.path_type(),
        MemtableBuilderProvider::new(None, config.clone()),
        env.get_object_store_manager().unwrap(),
        Arc::new(LocalScheduler::new(1)),
        env.get_puffin_manager(),
        env.get_intermediate_manager(),
        Arc::new(StdTimeProvider),
        engine.file_ref_manager(),
        noop_partition_expr_fetcher(),
    )
    .options(region.version().options.clone())
    .unwrap()
    .series_index_store(Some(store.clone()))
    .series_index_purger(Some(purger.clone()))
    .open(&config, &Wal::new(log_store))
    .await
    .unwrap();
    let restored = reopened.series_index_version();
    assert_eq!(first.range_indexes, restored.range_indexes);
    assert_eq!(
        first.series_indexes[&first_id].entry(),
        restored.series_indexes[&first_id].entry()
    );

    // Catalog changes are not reloaded, and missing index files are not repaired.
    for path in [
        range_catalog_path(region.region_id),
        series_catalog_path(region.region_id),
    ] {
        store.write(&path, "{}").await.unwrap();
    }
    let stats = reconcile_series_indexes(
        0,
        store.clone(),
        reopened.clone(),
        Duration::from_secs(100),
        0,
        purger,
    )
    .await
    .unwrap();
    assert_eq!((0, 0), (stats.built_range, stats.built_series));
    assert_eq!(
        first.range_indexes,
        reopened.series_index_version().range_indexes
    );
    assert!(
        reopened
            .series_index_version()
            .series_indexes
            .contains_key(&first_id)
    );
    for path in &missing_paths {
        assert!(!store.exists(path).await.unwrap());
    }
    engine.stop().await.unwrap();
}

struct FailingCatalogReader;

impl mock::Read for FailingCatalogReader {
    async fn read(&mut self) -> mock::Result<mock::Buffer> {
        Err(mock::Error::new(
            mock::ErrorKind::Unexpected,
            "injected catalog read failure",
        ))
    }
}

#[tokio::test]
async fn test_load_catalog_defaults_on_missing_invalid_or_unreadable_catalog() {
    let store = ObjectStore::new(Memory::default()).unwrap().finish();
    let region_id = RegionId::new(1, 1);
    let (purger, _receiver) = series_index_channel(store.clone());
    assert!(
        load_catalog::<super::catalog::SeriesIndexCatalog>(&store, &series_catalog_path(region_id))
            .await
            .is_none()
    );
    let control = load_version_control(&store, region_id, &purger).await;
    assert!(control.current().range_indexes.is_empty());
    assert!(control.current().series_indexes.is_empty());
    let file_id = store_api::storage::FileId::random();
    store
        .write(
            &range_catalog_path(region_id),
            serde_json::to_vec(&super::catalog::RangeIndexCatalog {
                indexes: vec![file_id],
            })
            .unwrap(),
        )
        .await
        .unwrap();
    store
        .write(&series_catalog_path(region_id), "invalid")
        .await
        .unwrap();
    let control = load_version_control(&store, region_id, &purger).await;
    assert!(control.current().range_indexes.contains(&file_id));
    assert!(control.current().series_indexes.is_empty());
    let layer = MockLayerBuilder::default()
        .reader_factory(Arc::new(|_, _, _| Box::new(FailingCatalogReader)))
        .build()
        .unwrap();
    assert!(
        load_catalog::<super::catalog::SeriesIndexCatalog>(&store, &series_catalog_path(region_id))
            .await
            .is_none()
    );
    let store = store.layer(layer);
    assert!(
        load_catalog::<super::catalog::RangeIndexCatalog>(&store, &range_catalog_path(region_id))
            .await
            .is_none()
    );
    let control = load_version_control(&store, region_id, &purger).await;
    assert!(control.current().range_indexes.is_empty());
    assert!(control.current().series_indexes.is_empty());
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
    let (engine, region) = prepare_region(&mut env).await;
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
    let stats = reconcile_series_indexes(0, store, region, Duration::from_secs(100), 0, purger)
        .await
        .unwrap();
    assert_eq!((2, 1), (stats.built_range, stats.built_series));
    engine.stop().await.unwrap();
}

#[rstest::rstest]
#[case::during_build(false)]
#[case::during_catalog_write(true)]
#[tokio::test]
async fn test_reconcile_publishes_after_region_version_changes(#[case] during_catalog_write: bool) {
    let mut env = TestEnv::with_prefix("series-version-change").await;
    let (engine, region) = prepare_region(&mut env).await;
    let initial_version = region.version();
    let store = ObjectStore::new(Memory::default()).unwrap().finish();
    let target_region = region.clone();
    let catalog_path = range_catalog_path(region.region_id);
    let layer = MockLayerBuilder::default()
        .writer_factory(Arc::new(move |path, _, inner| {
            let should_advance = if during_catalog_write {
                path == catalog_path
            } else {
                path.contains("/series/")
            };
            if should_advance {
                // Replace the version while reconciliation is writing its captured snapshot.
                target_region
                    .version_control
                    .alter_options(target_region.version().options.clone());
            }
            inner
        }))
        .build()
        .unwrap();
    let (purger, mut receiver) = series_index_channel(store.clone());
    let stats = reconcile_series_indexes(
        0,
        store.clone().layer(layer),
        region.clone(),
        Duration::from_secs(100),
        0,
        purger.clone(),
    )
    .await
    .unwrap();

    assert!(!Arc::ptr_eq(&initial_version, &region.version()));
    assert_eq!((2, 1), (stats.built_range, stats.built_series));
    let published = region.series_index_version();
    assert_eq!(2, published.range_indexes.len());
    assert_eq!(1, published.series_indexes.len());
    let restored = load_version_control(&store, region.region_id, &purger).await;
    assert_eq!(published.range_indexes, restored.current().range_indexes);
    let handle = published.series_indexes.values().next().unwrap();
    assert_eq!(
        handle.entry(),
        restored.current().series_indexes[&handle.entry().index_uuid].entry()
    );
    assert!(receiver.try_recv().is_err());
    engine.stop().await.unwrap();
}

#[rstest::rstest]
#[case(true)]
#[case(false)]
#[tokio::test]
async fn test_maintenance_wakeup_and_timer(#[case] immediate: bool) {
    let mut env = TestEnv::with_prefix("series-task").await;
    let (engine, region) = prepare_region(&mut env).await;
    let store = ObjectStore::new(Memory::default()).unwrap().finish();
    let (purger, receiver) = series_index_channel(store.clone());
    let state = Arc::new(super::task::SeriesIndexTaskState::new());
    // A notification issued before the task starts must also trigger maintenance.
    if immediate {
        state.wake();
    }
    let regions = Arc::new(RegionMap::default());
    regions.insert_region(region.clone());
    let task = super::task::spawn_series_index_tasks(
        0,
        store,
        regions,
        state.clone(),
        Duration::from_secs(100),
        purger,
        receiver,
        if immediate {
            Duration::from_secs(3600)
        } else {
            Duration::from_millis(20)
        },
    );
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        while region.series_index_version().series_indexes.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    state.stop();
    task.await.unwrap();
    engine.stop().await.unwrap();
    result.unwrap();
}

#[tokio::test]
async fn test_drop_catalogs_and_retained_snapshot_after_task_stop() {
    use common_time::Timestamp;

    use super::catalog::{SeriesIndexCatalog, SeriesIndexEntry, store_catalog};

    let store = ObjectStore::new(Memory::default()).unwrap().finish();
    let region_id = RegionId::new(1, 1);
    let entry = SeriesIndexEntry {
        index_uuid: FileId::random(),
        bucket_start: Timestamp::new_second(0),
        bucket_end: Timestamp::new_second(100),
        source_file_ids: vec![FileId::random()],
        min_file_sequence: 1,
        max_file_sequence: 2,
    };
    let path = series_index_path(region_id, entry.index_uuid);
    store.write(&path, "index").await.unwrap();
    store_catalog(
        &store,
        &series_catalog_path(region_id),
        &SeriesIndexCatalog {
            indexes: vec![entry.clone()],
        },
    )
    .await
    .unwrap();
    store_catalog(
        &store,
        &range_catalog_path(region_id),
        &super::catalog::RangeIndexCatalog::default(),
    )
    .await
    .unwrap();
    let (purger, receiver) = series_index_channel(store.clone());
    let control = load_version_control(&store, region_id, &purger).await;
    let snapshot = control.current();
    assert_eq!(&entry, snapshot.series_indexes[&entry.index_uuid].entry());
    let state = Arc::new(super::task::SeriesIndexTaskState::new());
    state.stop();
    super::task::spawn_series_index_tasks(
        0,
        store.clone(),
        Arc::new(RegionMap::default()),
        state,
        Duration::from_secs(100),
        purger,
        receiver,
        Duration::from_secs(3600),
    )
    .await
    .unwrap();
    delete_catalogs(&store, region_id).await;
    // Removing already absent catalogs is harmless.
    delete_catalogs(&store, region_id).await;
    assert!(!store.exists(&range_catalog_path(region_id)).await.unwrap());
    assert!(!store.exists(&series_catalog_path(region_id)).await.unwrap());
    control.mark_dropped();
    assert!(store.exists(&path).await.unwrap());
    drop(snapshot);
    tokio::time::timeout(Duration::from_secs(10), async {
        while store.exists(&path).await.unwrap() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
