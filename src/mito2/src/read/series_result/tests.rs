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

use super::*;
use datatypes::arrow::array::{ArrayRef, Float64Array, StringArray, UInt8Array, UInt64Array};
use datatypes::arrow::datatypes::Field;
use mito_codec::row_converter::SparsePrimaryKeyCodec;

pub(crate) fn fixture(series: &[(u32, u64, usize)]) -> RecordBatch {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut strings = Vec::new();
    let mut times = Vec::new();
    for (table, tsid, rows) in series {
        let mut key = vec![];
        SparsePrimaryKeyCodec::schemaless()
            .encode_internal(*table, *tsid, &mut key)
            .unwrap();
        for i in 0..*rows {
            keys.push(key.clone());
            values.push((i % 3 != 0).then_some(i as f64));
            strings.push((i % 4 != 0).then_some(format!("v{}", i % 5)));
            times.push(i as u64);
        }
    }
    let key = BinaryArray::from_iter_values(keys.iter().map(|k| k.as_slice()));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(values)),
        Arc::new(StringArray::from(strings)),
        Arc::new(UInt64Array::from(times)),
        Arc::new(key),
        Arc::new(UInt64Array::from(vec![7; keys.len()])),
        Arc::new(UInt8Array::from(vec![1; keys.len()])),
    ];
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), i < 2))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    let mut fields = batch.schema().fields().to_vec();
    fields[1] = Arc::new(
        fields[1]
            .as_ref()
            .clone()
            .with_data_type(DataType::Dictionary(
                Box::new(DataType::UInt32),
                Box::new(DataType::Utf8),
            )),
    );
    fields[3] = Arc::new(
        fields[3]
            .as_ref()
            .clone()
            .with_data_type(DataType::Dictionary(
                Box::new(DataType::UInt32),
                Box::new(DataType::Binary),
            )),
    );
    convert(&batch, &Arc::new(Schema::new(fields))).unwrap()
}

fn logical(batch: &RecordBatch) -> RecordBatch {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|f| {
            let typ = match f.data_type() {
                DataType::Dictionary(_, value) => *value.clone(),
                t => t.clone(),
            };
            Arc::new(f.as_ref().clone().with_data_type(typ))
        })
        .collect::<Vec<_>>();
    convert(batch, &Arc::new(Schema::new(fields))).unwrap()
}

async fn collect(mut cursor: ResultCursor, schema: SchemaRef) -> RecordBatch {
    let mut batches = vec![];
    while let Some(lease) = cursor.next().await.unwrap() {
        batches.push(lease.with_batch(logical));
    }
    let schema = logical(&RecordBatch::new_empty(schema)).schema();
    concat_batches(&schema, &batches).unwrap()
}

#[tokio::test]
async fn round_trip_layouts_placements_and_cross_file_series() {
    for layout in [Layout::OneSeries, Layout::MultipleSeries] {
        for placements in [
            vec![Placement::Resident],
            vec![Placement::File],
            vec![Placement::Resident, Placement::File],
        ] {
            for compression in [
                None,
                Some(CompressionType::LZ4_FRAME),
                Some(CompressionType::ZSTD),
            ] {
                let dir = common_test_util::temp_dir::create_temp_dir("series-result");
                let input = fixture(&[(1, 3, 3), (2, 3, 40), (2, 8, 2)]);
                let options = StoreOptions {
                    layout,
                    batch_rows: 5,
                    file_batches: 2,
                    compression,
                    ..Default::default()
                };
                let mut builder = ResultBuilder::new(dir.path(), input.schema(), options)
                    .await
                    .unwrap();
                let resources = builder.resources();
                for (i, start) in (0..input.num_rows()).step_by(7).enumerate() {
                    // Re-encode input slices to force independently assigned dictionaries.
                    let part = convert(
                        &logical(&input.slice(start, 7.min(input.num_rows() - start))),
                        &input.schema(),
                    )
                    .unwrap();
                    builder = builder
                        .append(part, placements[i % placements.len()])
                        .await
                        .unwrap();
                }
                let handle = builder.finish().await.unwrap();
                assert_eq!(
                    collect(handle.cursor().unwrap(), input.schema()).await,
                    logical(&input)
                );
                let id = MetricSeriesId {
                    table_id: 2,
                    tsid: 3,
                };
                assert_eq!(
                    collect(handle.series_cursor(id).unwrap(), input.schema()).await,
                    logical(&input.slice(3, 40))
                );
                assert!(
                    handle
                        .series_cursor(MetricSeriesId {
                            table_id: 9,
                            tsid: 3
                        })
                        .unwrap()
                        .next()
                        .await
                        .unwrap()
                        .is_none()
                );
                drop(handle);
                resources.drain_cleanup().await.unwrap();
                let snapshot = resources.snapshot();
                assert_eq!(snapshot.disk_bytes, 0);
                assert_eq!(snapshot.memory_bytes, snapshot.resource_bytes);
            }
        }
    }
}

#[tokio::test]
async fn independent_cursors_shared_metadata_and_leases() {
    let dir = common_test_util::temp_dir::create_temp_dir("series-result-cursors");
    let spec = (0..2000).map(|i| (1, i, 1)).collect::<Vec<_>>();
    let input = fixture(&spec);
    let options = StoreOptions {
        layout: Layout::OneSeries,
        batch_rows: 8,
        file_batches: 97,
        ..Default::default()
    };
    let builder = ResultBuilder::new(dir.path(), input.schema(), options)
        .await
        .unwrap();
    let resources = builder.resources();
    let handle = builder
        .append(input.clone(), Placement::File)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    assert_eq!(handle.num_batches(), 2000);
    let baseline = resources.snapshot();
    assert_eq!(baseline.counts["metadata_initializations"], 21);
    let mut a = handle
        .series_cursor(MetricSeriesId {
            table_id: 1,
            tsid: 5,
        })
        .unwrap();
    let mut b = handle
        .series_cursor(MetricSeriesId {
            table_id: 1,
            tsid: 1900,
        })
        .unwrap();
    assert_eq!(baseline.metadata_bytes, resources.snapshot().metadata_bytes);
    let (first, second) = tokio::join!(a.next(), b.next());
    let first = first.unwrap().unwrap();
    let second = second.unwrap().unwrap();
    assert_eq!(first.with_batch(logical), logical(&input.slice(5, 1)));
    assert_eq!(second.with_batch(logical), logical(&input.slice(1900, 1)));
    assert!(a.next().await.unwrap().is_none());
    assert!(b.next().await.unwrap().is_none());
    assert_eq!(resources.snapshot().counts["metadata_initializations"], 21);
    drop(handle);
    assert!(resources.snapshot().disk_bytes > 0);
    drop(a);
    drop(b);
    resources.drain_cleanup().await.unwrap();
    assert_eq!(resources.snapshot().disk_bytes, 0);
    assert!(resources.snapshot().memory_bytes > 0); // Returned payload leases remain alive.
    drop(first);
    drop(second);
    assert_eq!(
        resources.snapshot().memory_bytes,
        resources.snapshot().resource_bytes
    );
}

#[tokio::test]
async fn mixed_batch_lookup_retains_unrelated_rows_and_reuses_decoding() {
    let dir = common_test_util::temp_dir::create_temp_dir("series-result-mixed");
    let input = fixture(&[(1, 1, 2), (1, 2, 2), (1, 3, 2)]);
    let builder = ResultBuilder::new(dir.path(), input.schema(), StoreOptions::default())
        .await
        .unwrap();
    let resources = builder.resources();
    let handle = builder
        .append(input, Placement::File)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    let mut cursor = handle
        .series_cursor(MetricSeriesId {
            table_id: 1,
            tsid: 2,
        })
        .unwrap();
    let lease = cursor.next().await.unwrap().unwrap();
    assert_eq!(lease.num_rows(), 2);
    assert_eq!(lease.retained_rows(), 6);
    assert_eq!(resources.snapshot().peaks["lookup_unrelated_rows"], 4);
    drop(lease);
    drop(cursor);
    drop(handle);
    resources.drain_cleanup().await.unwrap();
    assert_eq!(
        resources.snapshot().memory_bytes,
        resources.snapshot().resource_bytes
    );
}

#[tokio::test]
async fn fixed_dictionaries_are_immutable_and_shared() {
    for layout in [Layout::OneSeries, Layout::MultipleSeries] {
        for compression in [
            None,
            Some(CompressionType::LZ4_FRAME),
            Some(CompressionType::ZSTD),
        ] {
            let dir = common_test_util::temp_dir::create_temp_dir("series-result-dictionary");
            let input = fixture(&[(1, 7, 20), (2, 7, 20)]);
            let plain = logical(&input);
            let keys = plain
                .column(3)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            let fixed = Arc::new(BinaryArray::from_iter_values([
                keys.value(0),
                keys.value(20),
            ]));
            let options = StoreOptions {
                layout,
                compression,
                fixed_keys: Some(fixed.clone()),
                batch_rows: 3,
                file_batches: 4,
                ..Default::default()
            };
            let builder = ResultBuilder::new(dir.path(), input.schema(), options)
                .await
                .unwrap();
            let resources = builder.resources();
            let handle = builder
                .append(input.clone(), Placement::File)
                .await
                .unwrap()
                .finish()
                .await
                .unwrap();
            assert_eq!(
                collect(handle.cursor().unwrap(), input.schema()).await,
                logical(&input)
            );
            assert_eq!(
                collect(
                    handle
                        .series_cursor(MetricSeriesId {
                            table_id: 2,
                            tsid: 7
                        })
                        .unwrap(),
                    input.schema()
                )
                .await,
                logical(&input.slice(20, 20))
            );
            drop(handle);
            resources.drain_cleanup().await.unwrap();
            assert_eq!(resources.snapshot().disk_bytes, 0);
            assert_eq!(
                resources.snapshot().memory_bytes,
                resources.snapshot().resource_bytes
            );
        }
    }
}

async fn assert_clean(resources: &Arc<StoreResources>) {
    resources.drain_cleanup().await.unwrap();
    let snapshot = resources.snapshot();
    assert_eq!(
        snapshot.memory_bytes, snapshot.resource_bytes,
        "{snapshot:?}"
    );
    assert_eq!(snapshot.disk_bytes, 0, "{snapshot:?}");
    assert_eq!(snapshot.failed_cleanup, 0);
    assert_eq!(std::fs::read_dir(&resources.root).unwrap().count(), 0);
}

#[tokio::test]
async fn write_finalization_and_read_failures_release_owned_files() {
    use std::sync::atomic::Ordering;
    for phase in ["write", "finish", "read", "remove"] {
        let dir = common_test_util::temp_dir::create_temp_dir("series-result-failure");
        let sentinel = dir.path().join("unrelated.arrow");
        std::fs::write(&sentinel, b"keep").unwrap();
        let input = fixture(&[(1, 1, 100)]);
        let builder = ResultBuilder::new(
            dir.path(),
            input.schema(),
            StoreOptions {
                batch_rows: 8,
                file_batches: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let resources = builder.resources();
        match phase {
            "write" => *resources.faults.write_after.lock().unwrap() = Some(800),
            "finish" => resources.faults.finish.store(true, Ordering::SeqCst),
            _ => {}
        }
        let result = match builder.append(input, Placement::File).await {
            Ok(builder) => builder.finish().await,
            Err(e) => Err(e),
        };
        if phase == "write" || phase == "finish" {
            assert!(result.is_err());
        } else {
            let handle = result.unwrap();
            if phase == "read" {
                resources.faults.read.store(true, Ordering::SeqCst);
                let mut cursor = handle.cursor().unwrap();
                assert!(cursor.next().await.is_err());
                assert!(cursor.next().await.is_err());
                drop(cursor);
            } else {
                resources.faults.remove.store(true, Ordering::SeqCst);
            }
            drop(handle);
            if phase == "remove" {
                assert!(resources.drain_cleanup().await.is_err());
                assert!(resources.snapshot().disk_bytes > 0);
                resources.faults.remove.store(false, Ordering::SeqCst);
            }
        }
        assert_clean(&resources).await;
        assert_eq!(std::fs::read(sentinel).unwrap(), b"keep");
    }
}

#[tokio::test]
async fn cancellation_waits_for_blocking_writer_ownership() {
    let dir = common_test_util::temp_dir::create_temp_dir("series-result-cancel");
    let input = fixture(&[(1, 1, 100)]);
    let builder = ResultBuilder::new(
        dir.path(),
        input.schema(),
        StoreOptions {
            batch_rows: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let resources = builder.resources();
    let (started, receiver) = std::sync::mpsc::channel();
    let (resume, gate) = std::sync::mpsc::channel();
    *resources.faults.write_gate.lock().unwrap() = Some((started, gate));
    let task = tokio::spawn(builder.append(input, Placement::File));
    common_runtime::spawn_blocking_query(move || {
        receiver.recv_timeout(std::time::Duration::from_secs(10))
    })
    .await
    .unwrap()
    .unwrap();
    for file in std::fs::read_dir(&resources.root).unwrap() {
        assert_eq!(file.unwrap().path().extension().unwrap(), "partial");
    }
    task.abort();
    assert!(task.await.is_err());
    assert!(resources.snapshot().memory_bytes > 0);
    resume.send(()).unwrap();
    // The detached blocking writer must finish/drop before cleanup can be drained.
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            resources.drain_cleanup().await.unwrap();
            if resources.snapshot().memory_bytes == resources.snapshot().resource_bytes
                && resources.snapshot().disk_bytes == 0
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_clean(&resources).await;
}

#[tokio::test]
async fn quotas_invalid_input_and_oversized_rows_fail_cleanly() {
    for case in [
        "disk", "memory", "metadata", "batch", "order", "fixed", "file",
    ] {
        let dir = common_test_util::temp_dir::create_temp_dir("series-result-quota");
        let input = if case == "order" {
            fixture(&[(2, 1, 20), (1, 1, 20)])
        } else {
            fixture(&[(1, 1, 20), (2, 1, 20)])
        };
        let mut options = StoreOptions {
            batch_rows: 1,
            ..Default::default()
        };
        match case {
            "disk" => options.disk_bytes = 1000,
            "memory" => options.memory_bytes = 64 * 1024,
            "metadata" => {
                options.metadata_bytes = 32 * 1024;
                options.file_batches = 2;
            }
            "batch" => options.batch_bytes = 10,
            "file" => options.file_bytes = 5000,
            "fixed" => {
                options.fixed_keys =
                    Some(Arc::new(BinaryArray::from_iter_values([logical(&input)
                        .column(3)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap()
                        .value(0)])))
            }
            _ => {}
        }
        let builder = ResultBuilder::new(dir.path(), input.schema(), options).await;
        let Ok(builder) = builder else {
            assert!(case == "memory" || case == "metadata");
            continue;
        };
        let resources = builder.resources();
        let result = match builder.append(input, Placement::File).await {
            Ok(b) => b.finish().await,
            Err(e) => Err(e),
        };
        assert!(result.is_err(), "{case} unexpectedly passed");
        resources.drain_cleanup().await.unwrap();
        assert_eq!(resources.snapshot().disk_bytes, 0);
        if case != "fixed" {
            assert_eq!(
                resources.snapshot().memory_bytes,
                resources.snapshot().resource_bytes
            );
        }
    }
    assert!(checked_add(usize::MAX, 1).is_err());
    assert!(checked_mul(usize::MAX, 2).is_err());
}

#[tokio::test]
async fn corrupt_and_truncated_payloads_fail_without_publication_or_leaks() {
    use std::io::{Seek, SeekFrom, Write};
    for truncate in [false, true] {
        let dir = common_test_util::temp_dir::create_temp_dir("series-result-corrupt");
        let input = fixture(&[(1, 1, 10)]);
        let builder = ResultBuilder::new(dir.path(), input.schema(), StoreOptions::default())
            .await
            .unwrap();
        let resources = builder.resources();
        let handle = builder
            .append(input, Placement::File)
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let path = std::fs::read_dir(&resources.root)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        if truncate {
            file.set_len(8).unwrap();
        } else {
            // Destroy the record message, leaving the already-shared footer alone.
            let block = match &handle.0.directory[0].source {
                Source::File { part, .. } => part.test_first_offset(),
                _ => unreachable!(),
            };
            file.seek(SeekFrom::Start(block)).unwrap();
            file.write_all(&[0; 8]).unwrap();
        }
        drop(file);
        let mut cursor = handle.cursor().unwrap();
        assert!(cursor.next().await.is_err());
        drop(cursor);
        drop(handle);
        assert_clean(&resources).await;
    }
}

#[tokio::test]
async fn escaped_arrays_keep_their_allocation_charge() {
    let dir = common_test_util::temp_dir::create_temp_dir("series-result-escaped");
    for placement in [Placement::Resident, Placement::File] {
        let input = fixture(&[(1, 1, 8)]);
        let builder = ResultBuilder::new(dir.path(), input.schema(), StoreOptions::default())
            .await
            .unwrap();
        let resources = builder.resources();
        let handle = builder
            .append(input, placement)
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let mut cursor = handle.cursor().unwrap();
        let lease = cursor.next().await.unwrap().unwrap();
        let escaped = lease.with_batch(|b| b.column(0).clone());
        let payload = lease.retained_bytes();
        drop(lease);
        drop(cursor);
        drop(handle);
        resources.drain_cleanup().await.unwrap();
        assert_eq!(
            resources.snapshot().memory_bytes,
            payload + resources.snapshot().resource_bytes
        );
        drop(escaped);
        assert_clean(&resources).await;
    }
}

#[tokio::test]
async fn metadata_and_payload_limits_rotate_files_independently() {
    for metadata_limited in [false, true] {
        let dir = common_test_util::temp_dir::create_temp_dir("series-result-rotation");
        let input = fixture(&(0..100).map(|i| (1, i, 1)).collect::<Vec<_>>());
        let options = StoreOptions {
            layout: Layout::OneSeries,
            file_batches: 10000,
            file_metadata_bytes: if metadata_limited { 8192 } else { 1024 * 1024 },
            file_bytes: if metadata_limited {
                64 * 1024 * 1024
            } else {
                32 * 1024
            },
            ..Default::default()
        };
        let limit = options.file_bytes;
        let builder = ResultBuilder::new(dir.path(), input.schema(), options)
            .await
            .unwrap();
        let resources = builder.resources();
        let handle = builder
            .append(input.clone(), Placement::File)
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        assert!(resources.snapshot().counts["files"] > 1);
        for file in std::fs::read_dir(&resources.root).unwrap() {
            assert!(file.unwrap().metadata().unwrap().len() <= limit as u64);
        }
        assert_eq!(
            collect(handle.cursor().unwrap(), input.schema()).await,
            logical(&input)
        );
        drop(handle);
        assert_clean(&resources).await;
    }
}

#[tokio::test]
async fn empty_results_and_schema_metadata_survive_round_trip() {
    let dir = common_test_util::temp_dir::create_temp_dir("series-result-empty");
    let input = fixture(&[]);
    let mut fields = input.schema().fields().to_vec();
    fields[0] = Arc::new(fields[0].as_ref().clone().with_metadata(
        std::collections::HashMap::from([("unit".to_string(), "bytes".to_string())]),
    ));
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        std::collections::HashMap::from([("origin".to_string(), "compact".to_string())]),
    ));
    for nonempty in [false, true] {
        let input = if nonempty {
            fixture(&[(1, 1, 3)])
        } else {
            input.clone()
        };
        let input = RecordBatch::try_new(schema.clone(), input.columns().to_vec()).unwrap();
        let builder = ResultBuilder::new(dir.path(), schema.clone(), StoreOptions::default())
            .await
            .unwrap();
        let resources = builder.resources();
        let handle = builder
            .append(input.clone(), Placement::File)
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let mut cursor = handle.cursor().unwrap();
        if nonempty {
            let lease = cursor.next().await.unwrap().unwrap();
            lease.with_batch(|b| assert_eq!(b.schema(), schema));
        }
        assert!(cursor.next().await.unwrap().is_none());
        drop(cursor);
        drop(handle);
        assert_clean(&resources).await;
    }
}

#[tokio::test]
async fn replay_payload_is_bounded_as_series_grows_across_files() {
    let mut peaks = vec![];
    for rows in [32, 512] {
        let dir = common_test_util::temp_dir::create_temp_dir("series-result-bounded");
        let input = fixture(&[(1, 1, rows)]);
        let options = StoreOptions {
            batch_rows: 8,
            file_batches: 2,
            ..Default::default()
        };
        let builder = ResultBuilder::new(dir.path(), input.schema(), options)
            .await
            .unwrap();
        let resources = builder.resources();
        let handle = builder
            .append(input, Placement::File)
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let mut cursor = handle
            .series_cursor(MetricSeriesId {
                table_id: 1,
                tsid: 1,
            })
            .unwrap();
        let mut seen = 0;
        while let Some(lease) = cursor.next().await.unwrap() {
            assert!(lease.num_rows() <= 8);
            seen += lease.num_rows();
        }
        assert_eq!(seen, rows);
        peaks.push(resources.snapshot().peaks["replay_payload_bytes"]);
        drop(cursor);
        drop(handle);
        assert_clean(&resources).await;
    }
    assert_eq!(peaks[0], peaks[1]);
}

#[tokio::test]
async fn publication_never_overwrites_an_existing_artifact() {
    let dir = common_test_util::temp_dir::create_temp_dir("series-result-no-clobber");
    let input = fixture(&[(1, 1, 8)]);
    let builder = ResultBuilder::new(
        dir.path(),
        input.schema(),
        StoreOptions {
            batch_rows: 4,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let resources = builder.resources();
    let builder = builder.append(input, Placement::File).await.unwrap();
    let partial = std::fs::read_dir(&resources.root)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let sentinel = partial.with_extension("arrow");
    std::fs::write(&sentinel, b"unrelated artifact").unwrap();
    assert!(builder.finish().await.is_err());
    resources.drain_cleanup().await.unwrap();
    assert_eq!(std::fs::read(&sentinel).unwrap(), b"unrelated artifact");
    assert!(!partial.exists());
    assert_eq!(resources.snapshot().disk_bytes, 0);
    assert_eq!(
        resources.snapshot().memory_bytes,
        resources.snapshot().resource_bytes
    );
}

#[tokio::test]
async fn unpublished_spill_and_prepaid_independent_replay() {
    use crate::read::series_result::budget::BudgetPool;
    for layout in [Layout::OneSeries, Layout::MultipleSeries] {
        let root = common_test_util::temp_dir::create_temp_dir("buffered-stage4");
        let input = fixture(&[(1, 1, 33), (2, 1, 19)]);
        let mut builder = ResultBuilder::new(
            root.path(),
            input.schema(),
            StoreOptions {
                layout,
                batch_rows: 8,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let resources = builder.resources();
        builder = builder
            .append(input.slice(0, 20), Placement::Resident)
            .await
            .unwrap();
        builder = builder.spill_resident().await.unwrap();
        builder = builder
            .append(input.slice(20, 32), Placement::File)
            .await
            .unwrap();
        let handle = builder.finish().await.unwrap();
        assert!(!handle.has_resident());
        let before = resources.snapshot();
        let bytes = handle.replay_bytes().unwrap() * 2;
        let credit = BudgetPool::replay(&resources.pool(), bytes).unwrap();
        let pool: Arc<dyn MemoryPool> = credit.clone();
        let mut first = handle
            .series_cursor_in(
                MetricSeriesId {
                    table_id: 1,
                    tsid: 1,
                },
                pool.clone(),
            )
            .unwrap();
        let mut second = handle
            .series_cursor_in(
                MetricSeriesId {
                    table_id: 2,
                    tsid: 1,
                },
                pool,
            )
            .unwrap();
        let mut rows = [0, 0];
        loop {
            let a = first.next().await.unwrap();
            let b = second.next().await.unwrap();
            if a.is_none() && b.is_none() {
                break;
            }
            rows[0] += a.map_or(0, |b| b.num_rows());
            rows[1] += b.map_or(0, |b| b.num_rows());
        }
        assert_eq!([33, 19], rows);
        assert_eq!(
            before.counts.get("filesystem_write_bytes"),
            resources.snapshot().counts.get("filesystem_write_bytes")
        );
        drop((first, second, handle));
        credit.close();
        drop(credit);
        resources.drain_cleanup().await.unwrap();
        assert_eq!(0, resources.snapshot().disk_bytes);
        assert_eq!(0, resources.snapshot().payload_bytes);
    }
}

#[tokio::test]
async fn spilling_shared_results_is_rejected_and_partial_spill_cleans_up() {
    let root = common_test_util::temp_dir::create_temp_dir("buffered-stage4");
    let input = fixture(&[(1, 1, 64)]);
    let builder = ResultBuilder::new(root.path(), input.schema(), StoreOptions::default())
        .await
        .unwrap();
    let resources = builder.resources();
    let handle = builder
        .append(input, Placement::Resident)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    assert!(handle.clone().spill().await.is_err());
    *resources.faults.write_after.lock().unwrap() = Some(0);
    assert!(handle.spill().await.is_err());
    resources.drain_cleanup().await.unwrap();
    assert_eq!(0, resources.snapshot().disk_bytes);
    assert_eq!(0, resources.snapshot().payload_bytes);
}

#[tokio::test]
async fn lazy_replay_payload_is_independent_of_complete_range_count() {
    use crate::read::series_result::budget::BudgetPool;
    for layout in [Layout::OneSeries, Layout::MultipleSeries] {
        let mut baseline = None;
        for count in [1, 8, 64] {
            let dir = common_test_util::temp_dir::create_temp_dir("lazy-range-replay");
            let input = fixture(&[(1, 1, 16), (1, 2, 16)]);
            let builder = ResultBuilder::new(
                dir.path(),
                input.schema(),
                StoreOptions {
                    layout,
                    batch_rows: 8,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let resources = builder.resources();
            drop(builder);
            let mut handles = Vec::new();
            for _ in 0..count {
                handles.push(
                    ResultBuilder::with_resources(resources.clone(), input.schema())
                        .unwrap()
                        .append(input.clone(), Placement::File)
                        .await
                        .unwrap()
                        .finish()
                        .await
                        .unwrap(),
                );
            }
            let metadata = resources.snapshot().metadata_bytes;
            let bytes = handles[0].replay_bytes().unwrap();
            let credit = BudgetPool::replay(&resources.pool(), bytes).unwrap();
            let pool: Arc<dyn MemoryPool> = credit.clone();
            for handle in &handles {
                let mut cursor = handle
                    .series_cursor_in(
                        MetricSeriesId {
                            table_id: 1,
                            tsid: 1,
                        },
                        pool.clone(),
                    )
                    .unwrap();
                let mut rows = 0;
                while let Some(batch) = cursor.next().await.unwrap() {
                    rows += batch.num_rows();
                }
                assert_eq!(16, rows);
                cursor.close().await.unwrap();
                assert_eq!(0, resources.snapshot().payload_bytes);
            }
            let snapshot = resources.snapshot();
            let payload = snapshot.peaks["replay_payload_bytes"];
            if let Some(first) = baseline {
                assert_eq!(first, payload);
            } else {
                baseline = Some(payload);
            }
            assert!(metadata > count * std::mem::size_of::<ResultHandle>());
            drop(handles);
            credit.close();
            drop((credit, pool));
            resources.drain_cleanup().await.unwrap();
            assert_eq!(0, resources.snapshot().disk_bytes);
        }
    }
}

/// Equal-sequence winners are inherited from the merge's source ordering; IPC
/// placement must preserve the chosen row rather than introduce a new tie-break.
#[tokio::test]
async fn equal_sequence_ties_preserve_the_range_merge_winner() {
    use crate::read::BoxedRecordBatchStream;
    use crate::read::flat_dedup::{FlatDedupReader, FlatLastRow};
    use crate::read::flat_merge::FlatMergeReader;
    use datatypes::arrow::array::TimestampMillisecondArray;
    use futures::TryStreamExt;
    for order in [[10.0, 20.0], [20.0, 10.0]] {
        let template = fixture(&[(1, 1, 1)]);
        let inputs = order
            .into_iter()
            .map(|value| {
                let mut columns = template.columns().to_vec();
                columns[0] = Arc::new(Float64Array::from(vec![value]));
                columns[2] = Arc::new(TimestampMillisecondArray::from(vec![1000]));
                let mut fields = template.schema().fields().to_vec();
                fields[2] = Arc::new(
                    fields[2]
                        .as_ref()
                        .clone()
                        .with_data_type(columns[2].data_type().clone()),
                );
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
            })
            .collect::<Vec<_>>();
        let schema = inputs[0].schema();
        let streams = inputs
            .into_iter()
            .map(|batch| Box::pin(futures::stream::iter([Ok(batch)])) as BoxedRecordBatchStream)
            .collect();
        let merge = FlatMergeReader::new(schema.clone(), streams, 8, None)
            .await
            .unwrap();
        let output =
            FlatDedupReader::new(Box::pin(merge.into_stream()), FlatLastRow::new(true), None)
                .into_stream()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
        assert_eq!(1, output.iter().map(RecordBatch::num_rows).sum::<usize>());
        let chosen = logical(&output[0]);
        let winner = chosen
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        // This fixture's two equal cursors preserve the first source's row. It
        // characterizes this shape, not a promise of stable ordering for all ties.
        assert_eq!(order[0], winner);
        for layout in [Layout::OneSeries, Layout::MultipleSeries] {
            for placement in [Placement::Resident, Placement::File] {
                let dir = common_test_util::temp_dir::create_temp_dir("buffered-ties");
                let builder = ResultBuilder::new(
                    dir.path(),
                    schema.clone(),
                    StoreOptions {
                        layout,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
                let resources = builder.resources();
                let handle = builder
                    .append(output[0].clone(), placement)
                    .await
                    .unwrap()
                    .finish()
                    .await
                    .unwrap();
                assert_eq!(
                    chosen,
                    collect(handle.cursor().unwrap(), schema.clone()).await
                );
                drop(handle);
                resources.drain_cleanup().await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn repeated_spill_checks_preserve_file_batch_packing() {
    let dir = common_test_util::temp_dir::create_temp_dir("file-packing");
    let first = fixture(&[(1, 1, 2)]);
    let second = fixture(&[(1, 2, 2)]);
    let builder = ResultBuilder::new(
        dir.path(),
        first.schema(),
        StoreOptions {
            layout: Layout::MultipleSeries,
            batch_rows: 128,
            ..StoreOptions::default()
        },
    )
    .await
    .unwrap();
    let resources = builder.resources();
    let builder = builder.append(first, Placement::File).await.unwrap();
    let builder = builder.spill_resident().await.unwrap();
    let result = builder
        .append(second, Placement::File)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    assert_eq!(1, result.num_batches());
    assert_eq!(2, result.num_spans());
    drop(result);
    resources.drain_cleanup().await.unwrap();
    assert_eq!(0, resources.snapshot().disk_bytes);
}

#[tokio::test]
async fn preparation_detaches_result_from_source_allocation_charges() {
    let dir = common_test_util::temp_dir::create_temp_dir("buffered-source-ownership");
    let batch = fixture(&[(1, 1, 128)]);
    let builder = ResultBuilder::new(
        dir.path(),
        batch.schema(),
        StoreOptions {
            batch_rows: 2,
            batch_bytes: 8192,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let resources = builder.resources();
    let source = builder
        .with_phase("source")
        .append(batch.clone(), Placement::File)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    let mut cursor = source.cursor().unwrap();
    let lease = cursor.next().await.unwrap().unwrap();
    let budget = crate::read::series_result::budget::BudgetPool::prepaid(
        &resources.pool(),
        8 * 1024 * 1024,
        "preparation",
    )
    .unwrap();
    let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> = budget.clone();
    let result = ResultBuilder::with_workspace(resources.clone(), batch.schema(), pool)
        .unwrap()
        .with_phase("result")
        .append(lease.with_batch(Clone::clone), Placement::Resident)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    drop(lease);
    cursor.close().await.unwrap();
    drop(cursor);
    drop(source);
    budget.close();
    resources.drain_cleanup().await.unwrap();
    assert_eq!(0, resources.snapshot().ownership["source_payload_bytes"]);
    assert!(resources.snapshot().ownership["result_payload_bytes"] > 0);
    drop(result);
    resources.drain_cleanup().await.unwrap();
    assert_eq!(0, resources.snapshot().payload_bytes);
    assert_eq!(0, resources.snapshot().workspace_bytes);
    assert_eq!(0, resources.snapshot().disk_bytes);
}

#[tokio::test]
async fn buffered_source_batch_boundaries_preserve_equal_sequence_winners() {
    use crate::read::BoxedRecordBatchStream;
    use crate::read::flat_dedup::{FlatDedupReader, FlatLastRow};
    use crate::read::flat_merge::FlatMergeReader;
    use datatypes::arrow::array::TimestampMillisecondArray;
    use futures::TryStreamExt;

    async fn winners(
        schema: SchemaRef,
        sources: Vec<BoxedRecordBatchStream>,
        rows: usize,
    ) -> Vec<Option<f64>> {
        let merge = FlatMergeReader::new(schema, sources, rows, None)
            .await
            .unwrap();
        let mut stream = Box::pin(
            FlatDedupReader::new(Box::pin(merge.into_stream()), FlatLastRow::new(true), None)
                .into_stream(),
        );
        let mut output = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            output.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter(),
            );
        }
        output
    }

    let inputs = (0..3)
        .map(|source| {
            let template = fixture(&[(1, 1, 16), (2, 1, 16)]);
            let mut columns = template.columns().to_vec();
            columns[0] = Arc::new(Float64Array::from(vec![source as f64; 32]));
            columns[2] = Arc::new(TimestampMillisecondArray::from_iter_values(
                (0..16).chain(0..16),
            ));
            let mut fields = template.schema().fields().to_vec();
            fields[2] = Arc::new(
                fields[2]
                    .as_ref()
                    .clone()
                    .with_data_type(columns[2].data_type().clone()),
            );
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
        })
        .collect::<Vec<_>>();
    let schema = inputs[0].schema();
    let reference = winners(
        schema.clone(),
        inputs
            .iter()
            .map(|batch| {
                Box::pin(futures::stream::iter([Ok(batch.clone())])) as BoxedRecordBatchStream
            })
            .collect(),
        8192,
    )
    .await;
    for layout in [Layout::OneSeries, Layout::MultipleSeries] {
        let dir = common_test_util::temp_dir::create_temp_dir("buffered-source-ties");
        let resources = StoreResources::new(
            dir.path(),
            StoreOptions {
                layout,
                batch_rows: 2,
                ..Default::default()
            },
        )
        .unwrap();
        let mut sources = Vec::new();
        for (index, batch) in inputs.iter().enumerate() {
            let placement = if index == 0 {
                Placement::Resident
            } else {
                Placement::File
            };
            let handle = ResultBuilder::with_resources(resources.clone(), schema.clone())
                .unwrap()
                .with_phase("source")
                .append(batch.clone(), placement)
                .await
                .unwrap()
                .finish()
                .await
                .unwrap();
            sources.push(Box::pin(async_stream::try_stream! {
                let pool = handle.0.resources.pool();
                let mut stream = handle.source_stream_in(pool);
                while let Some(batch) = stream.try_next().await? { yield batch; }
            }) as BoxedRecordBatchStream);
        }
        assert_eq!(reference, winners(schema.clone(), sources, 2).await);
        resources.drain_cleanup().await.unwrap();
        assert_eq!(0, resources.snapshot().disk_bytes);
        assert_eq!(0, resources.snapshot().payload_bytes);
    }
}

#[tokio::test]
async fn buffered_source_reassembly_releases_fragment_decoders() {
    use futures::TryStreamExt;

    for layout in [Layout::OneSeries, Layout::MultipleSeries] {
        let dir = common_test_util::temp_dir::create_temp_dir("buffered-source-fragments");
        let resources = StoreResources::new(
            dir.path(),
            StoreOptions {
                layout,
                batch_rows: 16,
                batch_bytes: 64 * 1024,
                memory_bytes: 8 * 1024 * 1024,
                metadata_bytes: 2 * 1024 * 1024,
                ..Default::default()
            },
        )
        .unwrap();
        // One original decoder batch spans hundreds of independently decoded
        // one-series IPC fragments. Its compact fields include dictionaries.
        let batch = fixture(&(0..512).map(|id| (1, id, 1)).collect::<Vec<_>>());
        let handle = ResultBuilder::with_resources(resources.clone(), batch.schema())
            .unwrap()
            .with_phase("source")
            .append(batch.clone(), Placement::File)
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let window = handle.replay_bytes().unwrap();
        let required = handle.source_replay_bytes().unwrap();
        let pool =
            budget::BudgetPool::prepaid(&resources.pool(), required, "source merge").unwrap();
        let mut stream = handle.source_stream_in(pool.clone());
        let output = stream.try_next().await.unwrap().unwrap();
        assert_eq!(logical(&batch), logical(&output));
        assert!(stream.try_next().await.unwrap().is_none());
        drop(stream);
        pool.close();
        resources.drain_cleanup().await.unwrap();
        let snapshot = resources.snapshot();
        assert_eq!(512, snapshot.counts["source_ipc_decoded_rows"]);
        assert!(snapshot.peaks["source_payload_bytes"] <= 2 * window);
        assert_eq!(0, snapshot.ownership["source_payload_bytes"]);
        assert!(snapshot.ownership["source_reassembly_workspace_bytes"] > 0);
        assert_eq!(0, snapshot.disk_bytes);
        drop(output);
        let snapshot = resources.snapshot();
        assert_eq!(0, snapshot.workspace_bytes);
        assert_eq!(0, snapshot.payload_bytes);
        assert!(snapshot.ownership.values().all(|bytes| *bytes == 0));
    }
}

#[tokio::test]
async fn cached_files_outlive_producer_and_charge_independent_consumers() {
    for layout in [Layout::OneSeries, Layout::MultipleSeries] {
        for placement in [Placement::Resident, Placement::File] {
            let dir = common_test_util::temp_dir::create_temp_dir("cache-ownership");
            let input = fixture(&[(1, 3, 5), (2, 3, 19)]);
            let options = StoreOptions {
                layout,
                batch_rows: 4,
                ..Default::default()
            };
            let mut builder = ResultBuilder::new(dir.path(), input.schema(), options.clone())
                .await
                .unwrap();
            let producer = builder.resources();
            let weak = Arc::downgrade(&producer);
            builder = builder.append(input.clone(), placement).await.unwrap();
            let original = builder.finish().await.unwrap();
            let cache = StoreResources::new(dir.path(), options.clone()).unwrap();
            let cached = original
                .copy_to_cache(cache.clone(), producer.clone(), producer.pool())
                .await
                .unwrap();
            assert!(!cached.has_resident());
            drop(original);
            producer.drain_cleanup().await.unwrap();
            assert_eq!(producer.snapshot().disk_bytes, 0);
            drop(producer);
            assert!(weak.upgrade().is_none());
            let writes = cache
                .snapshot()
                .counts
                .get("write_calls")
                .copied()
                .unwrap_or(0);
            let query = StoreResources::new(dir.path(), options).unwrap();
            let id = MetricSeriesId {
                table_id: 2,
                tsid: 3,
            };
            let mut first = cached
                .series_cursor_with_resources(id, query.pool(), query.clone())
                .unwrap();
            let second = cached
                .series_cursor_with_resources(id, query.pool(), query.clone())
                .unwrap();
            let lease = first.next().await.unwrap().unwrap();
            assert!(query.snapshot().payload_bytes > 0);
            assert_eq!(cache.snapshot().payload_bytes, 0);
            drop(cached); // Equivalent to eviction: only consumer pins remain.
            assert!(cache.snapshot().disk_bytes > 0);
            assert_eq!(
                collect(second, input.schema()).await,
                logical(&input.slice(5, 19))
            );
            first.close().await.unwrap();
            drop(first);
            query.drain_cleanup().await.unwrap();
            cache.drain_cleanup().await.unwrap();
            assert_eq!(cache.snapshot().disk_bytes, 0);
            assert!(query.snapshot().payload_bytes > 0); // Escaped output owns its charge.
            drop(lease);
            query.drain_cleanup().await.unwrap();
            assert_eq!(query.snapshot().payload_bytes, 0);
            assert_eq!(query.snapshot().workspace_bytes, 0);
            assert_eq!(
                cache
                    .snapshot()
                    .counts
                    .get("write_calls")
                    .copied()
                    .unwrap_or(0),
                writes
            );
        }
    }
}

#[tokio::test]
async fn optional_cache_copy_failure_preserves_original() {
    let dir = common_test_util::temp_dir::create_temp_dir("cache-copy-failure");
    let input = fixture(&[(1, 3, 20)]);
    let builder = ResultBuilder::new(dir.path(), input.schema(), StoreOptions::default())
        .await
        .unwrap();
    let producer = builder.resources();
    let original = builder
        .append(input.clone(), Placement::Resident)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    let cache = StoreResources::new(
        dir.path(),
        StoreOptions {
            disk_bytes: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        original
            .copy_to_cache(cache.clone(), producer.clone(), producer.pool())
            .await
            .is_err()
    );
    assert_eq!(
        collect(original.cursor().unwrap(), input.schema()).await,
        logical(&input)
    );
    cache.drain_cleanup().await.unwrap();
    assert_eq!(cache.snapshot().disk_bytes, 0);
    assert_eq!(
        cache.snapshot().metadata_bytes,
        cache.snapshot().resource_bytes
    );
}

#[tokio::test]
async fn cancelled_cache_copy_releases_staging_and_preserves_original() {
    for placement in [Placement::File, Placement::Resident] {
        let dir = common_test_util::temp_dir::create_temp_dir("cache-copy-cancel");
        let input = fixture(&[(1, 3, 20)]);
        let options = StoreOptions {
            batch_rows: 2,
            file_batches: 2,
            ..Default::default()
        };
        let builder = ResultBuilder::new(dir.path(), input.schema(), options.clone())
            .await
            .unwrap();
        let query = builder.resources();
        let original = builder
            .append(input.clone(), placement)
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let cache = StoreResources::new(dir.path(), options).unwrap();
        let (started, receiver) = std::sync::mpsc::channel();
        let (resume, gate) = std::sync::mpsc::channel();
        *cache.faults.write_gate.lock().unwrap() = Some((started, gate));
        let source = original.clone();
        let producer = query.clone();
        let destination = cache.clone();
        let task = tokio::spawn(async move {
            source
                .copy_to_cache(destination, producer.clone(), producer.pool())
                .await
        });
        common_runtime::spawn_blocking_query(move || {
            receiver.recv_timeout(std::time::Duration::from_secs(10))
        })
        .await
        .unwrap()
        .unwrap();
        task.abort();
        assert!(task.await.is_err());
        resume.send(()).unwrap();
        query.drain_cleanup().await.unwrap();
        cache.drain_cleanup().await.unwrap();
        assert_eq!(cache.snapshot().disk_bytes, 0);
        assert_eq!(
            cache.snapshot().metadata_bytes,
            cache.snapshot().resource_bytes
        );
        assert_eq!(
            collect(original.cursor().unwrap(), input.schema()).await,
            logical(&input)
        );
    }
}
