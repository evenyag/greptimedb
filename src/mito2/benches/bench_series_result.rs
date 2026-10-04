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

//! Deterministic Stage 3 synthetic comparisons. No database or retained data is opened.
//! SERIES_RESULT_BENCH_DIR is a fresh external evidence directory.

use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arrow_ipc::CompressionType;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, Float64Array, StringArray, UInt8Array, UInt64Array,
};
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use mito2::read::series_result::{
    Layout, Placement, ResultBuilder, StoreOptions, operation_values,
};
use mito2::series_index::MetricSeriesId;
use serde_json::json;

fn schema(wide: bool) -> SchemaRef {
    let mut fields = vec![Field::new("value", DataType::Float64, true)];
    if wide {
        fields.push(Field::new(
            "label",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            true,
        ));
    }
    fields.extend([
        Field::new("timestamp", DataType::UInt64, false),
        Field::new(
            "__primary_key",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
            false,
        ),
        Field::new("__sequence", DataType::UInt64, false),
        Field::new("__op_type", DataType::UInt8, false),
    ]);
    Arc::new(Schema::new(fields))
}

fn key(series: usize) -> Vec<u8> {
    let mut key = vec![];
    SparsePrimaryKeyCodec::schemaless()
        .encode_internal(1, series as u64, &mut key)
        .unwrap();
    key
}

fn batch(schema: &SchemaRef, rows: &[(usize, usize)], wide: bool) -> RecordBatch {
    let values = Float64Array::from_iter(
        rows.iter()
            .map(|(id, t)| (t % 7 != 0).then_some((*id * 1000000 + t) as f64)),
    );
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(values)];
    if wide {
        let labels = StringArray::from_iter(rows.iter().map(|(id, t)| {
            (t % 11 != 0).then(|| format!("{id:08}-{:04}-{}", t % 19, "x".repeat(240)))
        }));
        arrays.push(cast(&labels, schema.field(1).data_type()).unwrap());
    }
    let keys = rows.iter().map(|(id, _)| key(*id)).collect::<Vec<_>>();
    let keys = BinaryArray::from_iter_values(keys.iter().map(|k| k.as_slice()));
    arrays.extend([
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|(_, t)| *t as u64),
        )) as ArrayRef,
        cast(&keys, schema.field(schema.fields().len() - 3).data_type()).unwrap(),
        Arc::new(UInt64Array::from(vec![7; rows.len()])),
        Arc::new(UInt8Array::from(vec![1; rows.len()])),
    ]);
    RecordBatch::try_new(schema.clone(), arrays).unwrap()
}

fn verify(batch: &RecordBatch, wide: bool, expected_series: Option<usize>) {
    let pk = batch.num_columns() - 3;
    let keys = cast(batch.column(pk), &DataType::Binary).unwrap();
    let keys = keys.as_any().downcast_ref::<BinaryArray>().unwrap();
    let times = batch
        .column(pk - 1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let labels = wide.then(|| cast(batch.column(1), &DataType::Utf8).unwrap());
    for row in 0..batch.num_rows() {
        let (table, series) = SparsePrimaryKeyCodec::schemaless()
            .decode_ids(keys.value(row))
            .unwrap();
        assert_eq!(table, 1);
        if let Some(id) = expected_series {
            assert_eq!(series, id as u64);
        }
        let t = times.value(row);
        assert_eq!(values.is_null(row), t % 7 == 0);
        if t % 7 != 0 {
            assert_eq!(values.value(row), (series * 1000000 + t) as f64);
        }
        if let Some(labels) = &labels {
            let labels = labels.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(labels.is_null(row), t % 11 == 0);
            if t % 11 != 0 {
                assert_eq!(
                    labels.value(row),
                    format!("{series:08}-{:04}-{}", t % 19, "x".repeat(240))
                );
            }
        }
    }
}

fn cpu() -> Option<f64> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        use nix::time::{ClockId, clock_gettime};
        clock_gettime(ClockId::CLOCK_PROCESS_CPUTIME_ID)
            .ok()
            .map(|t| t.tv_sec() as f64 + t.tv_nsec() as f64 * 1e-9)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    root: &Path,
    workload: &str,
    lengths: &[usize],
    wide: bool,
    layout: Layout,
    batch_rows: usize,
    fixed: bool,
    compression: Option<CompressionType>,
    sample: usize,
) -> serde_json::Value {
    let schema = schema(wide);
    let keys = lengths
        .iter()
        .enumerate()
        .map(|(i, _)| key(i))
        .collect::<Vec<_>>();
    let options = StoreOptions {
        layout,
        batch_rows,
        fixed_keys: fixed.then(|| {
            Arc::new(BinaryArray::from_iter_values(
                keys.iter().map(|k| k.as_slice()),
            ))
        }),
        compression,
        ..Default::default()
    };
    let mut builder = ResultBuilder::new(root, schema.clone(), options)
        .await
        .unwrap();
    let resources = builder.resources();
    let mut rows = Vec::with_capacity(8192);
    let mut write_seconds = 0.;
    let process_start = cpu();
    let mut generated_bytes = 0usize;
    for (series, count) in lengths.iter().enumerate() {
        for t in 0..*count {
            rows.push((series, t));
            if rows.len() == 8192 {
                let input = batch(&schema, &rows, wide);
                generated_bytes += input.get_array_memory_size();
                let start = Instant::now();
                builder = builder.append(input, Placement::File).await.unwrap();
                write_seconds += start.elapsed().as_secs_f64();
                rows.clear();
            }
        }
    }
    if !rows.is_empty() {
        let input = batch(&schema, &rows, wide);
        generated_bytes += input.get_array_memory_size();
        let start = Instant::now();
        builder = builder.append(input, Placement::File).await.unwrap();
        write_seconds += start.elapsed().as_secs_f64();
    }
    let start = Instant::now();
    let handle = builder.finish().await.unwrap();
    write_seconds += start.elapsed().as_secs_f64();
    let after_write = resources.snapshot();
    let start = Instant::now();
    let mut cursor = handle.cursor().unwrap();
    let mut sequential_rows = 0;
    while let Some(lease) = cursor.next().await.unwrap() {
        sequential_rows += lease.num_rows();
        if sample == 0 {
            lease.with_batch(|b| verify(b, wide, None));
        }
        black_box(lease);
    }
    let sequential_seconds = start.elapsed().as_secs_f64();
    drop(cursor);
    assert_eq!(sequential_rows, lengths.iter().sum::<usize>());
    let after_sequential = resources.snapshot();
    let start = Instant::now();
    let lookups = lengths.len().min(128);
    let mut random_rows = 0;
    let mut seed = 42u64;
    for _ in 0..lookups {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let series = (seed >> 32) as usize % lengths.len();
        let mut cursor = handle
            .series_cursor(MetricSeriesId {
                table_id: 1,
                tsid: series as u64,
            })
            .unwrap();
        let mut count = 0;
        while let Some(lease) = cursor.next().await.unwrap() {
            count += lease.num_rows();
            if sample == 0 {
                lease.with_batch(|b| verify(b, wide, Some(series)));
            }
            black_box(lease);
        }
        assert_eq!(count, lengths[series]);
        random_rows += count;
    }
    let random_seconds = start.elapsed().as_secs_f64();
    let after_read = resources.snapshot();
    let batches = handle.num_batches();
    let spans = handle.num_spans();
    drop(handle);
    let start = Instant::now();
    resources.drain_cleanup().await.unwrap();
    let cleanup_seconds = start.elapsed().as_secs_f64();
    let after_cleanup = resources.snapshot();
    assert_eq!(after_cleanup.disk_bytes, 0);
    assert_eq!(after_cleanup.pending_cleanup, 0);
    assert_eq!(after_cleanup.failed_cleanup, 0);
    assert_eq!(after_cleanup.memory_bytes, after_cleanup.resource_bytes);
    let process_cpu_seconds = cpu().zip(process_start).map(|(end, start)| end - start);
    let mut row_distribution = std::collections::BTreeMap::<usize, usize>::new();
    let mut value_byte_distribution = std::collections::BTreeMap::<usize, usize>::new();
    for rows in lengths {
        *row_distribution.entry(*rows).or_default() += 1;
        // Logical nonnull values only: excludes offsets, validity, padding and dictionaries.
        let bytes = rows * 39
            + (rows - rows.div_ceil(7)) * 8
            + if wide {
                (rows - rows.div_ceil(11)) * 254
            } else {
                0
            };
        *value_byte_distribution.entry(bytes).or_default() += 1;
    }
    json!({ "workload": workload, "wide": wide, "layout": format!("{layout:?}"), "batch_rows": batch_rows, "fixed_dictionary": fixed, "compression": format!("{compression:?}"), "sample": sample, "verification_run": sample == 0,
        "rows": sequential_rows, "series": lengths.len(), "series_row_distribution": row_distribution, "series_nonnull_value_byte_distribution": value_byte_distribution, "min_series_rows": lengths.iter().min(), "max_series_rows": lengths.iter().max(), "generated_array_bytes": generated_bytes,
        "batches": batches, "spans": spans, "write_seconds": write_seconds, "sequential_seconds": sequential_seconds, "random_seconds": random_seconds, "random_rows": random_rows, "lookups": lookups,
        "cleanup_seconds": cleanup_seconds, "process_cpu_seconds_including_generation": process_cpu_seconds,
        "after_write": after_write, "after_sequential": after_sequential, "after_read": after_read, "after_cleanup": after_cleanup, "operations": operation_values(&resources),
        "physical_storage_bytes": null, "cache_state": "OS cache retained; cursor payload cache starts empty", "prefetch_batches": 0 })
}

#[allow(clippy::print_stdout)]
fn main() {
    let root = std::env::var_os("SERIES_RESULT_BENCH_DIR")
        .expect("set SERIES_RESULT_BENCH_DIR to an external evidence directory");
    let root = Path::new(&root);
    std::fs::create_dir_all(root).unwrap();
    let quick = std::env::var_os("SERIES_RESULT_BENCH_QUICK").is_some();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let workloads = [
        ("singletons", vec![1; if quick { 64 } else { 4096 }]),
        (
            "uniform",
            vec![if quick { 32 } else { 512 }; if quick { 8 } else { 128 }],
        ),
        (
            "skewed",
            (0..128)
                .map(|i| {
                    if i == 0 {
                        if quick { 4096 } else { 262144 }
                    } else {
                        8
                    }
                })
                .collect::<Vec<_>>(),
        ),
    ];
    for compression in [
        None,
        Some(CompressionType::LZ4_FRAME),
        Some(CompressionType::ZSTD),
    ] {
        for fixed in [false, true] {
            for (name, lengths) in &workloads {
                for wide in [false, true] {
                    for batch_rows in [1024, 8192] {
                        for layout in [Layout::OneSeries, Layout::MultipleSeries] {
                            for sample in 0..=if quick { 0 } else { 3 } {
                                let result = runtime.block_on(run(
                                    root,
                                    name,
                                    lengths,
                                    wide,
                                    layout,
                                    batch_rows,
                                    fixed,
                                    compression,
                                    sample,
                                ));
                                println!("{result}");
                            }
                        }
                    }
                }
            }
        }
    }
}
