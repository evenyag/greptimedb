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

//! Benchmark for `FlatReadFormat::convert_batch` on the sparse-encoded legacy
//! parquet read path. The hot work is decoding the `__primary_key` dictionary
//! values for every batch — this bench measures that cost with and without
//! the recyclable per-string buffer pool plumbed through `convert_batch`.

use std::hint::black_box;
use std::sync::Arc;

use api::v1::SemanticType;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datatypes::arrow::array::{
    ArrayRef, BinaryArray, DictionaryArray, RecordBatch, TimestampMillisecondArray, UInt8Array,
    UInt32Array, UInt64Array,
};
use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use mito2::sst::parquet::flat_format::FlatReadFormat;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::storage::RegionId;
use store_api::storage::consts::ReservedColumnId;

const NUM_LABELS: usize = 5;
const TABLE_ID_VALUE: u32 = 1024;
const ROWS_PER_BATCH: usize = 8192;

fn build_sparse_metadata() -> RegionMetadataRef {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "__table_id",
                ConcreteDataType::uint32_datatype(),
                false,
            ),
            semantic_type: SemanticType::Tag,
            column_id: ReservedColumnId::table_id(),
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("__tsid", ConcreteDataType::uint64_datatype(), false),
            semantic_type: SemanticType::Tag,
            column_id: ReservedColumnId::tsid(),
        });
    let mut pk = vec![ReservedColumnId::table_id(), ReservedColumnId::tsid()];
    for i in 0..NUM_LABELS {
        let id = (i + 1) as u32;
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                format!("label_{i}"),
                ConcreteDataType::string_datatype(),
                true,
            ),
            semantic_type: SemanticType::Tag,
            column_id: id,
        });
        pk.push(id);
    }
    builder.push_column_metadata(ColumnMetadata {
        column_schema: ColumnSchema::new("value", ConcreteDataType::float64_datatype(), false),
        semantic_type: SemanticType::Field,
        column_id: 100,
    });
    builder.push_column_metadata(ColumnMetadata {
        column_schema: ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        semantic_type: SemanticType::Timestamp,
        column_id: 101,
    });
    builder.primary_key(pk);
    builder.primary_key_encoding(PrimaryKeyEncoding::Sparse);
    Arc::new(builder.build().unwrap())
}

/// Build a primary-key dictionary array with `unique_keys` sparse-encoded values
/// and `ROWS_PER_BATCH` rows. Each unique key has `NUM_LABELS` random strings of
/// length `string_len`.
fn build_pk_dict_array(unique_keys: usize, string_len: usize) -> ArrayRef {
    let codec = SparsePrimaryKeyCodec::new(&build_sparse_metadata());
    let mut rng = StdRng::seed_from_u64(0xC0FFEE);
    let alphabet: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

    let mut encoded = Vec::with_capacity(unique_keys);
    for series_id in 0..unique_keys {
        let mut buf = Vec::new();
        codec
            .encode_internal(TABLE_ID_VALUE, series_id as u64, &mut buf)
            .unwrap();
        let labels: Vec<(u32, Vec<u8>)> = (0..NUM_LABELS)
            .map(|label_idx| {
                let bytes: Vec<u8> = (0..string_len)
                    .map(|_| alphabet[rng.random_range(0..alphabet.len())])
                    .collect();
                ((label_idx + 1) as u32, bytes)
            })
            .collect();
        codec
            .encode_raw_tag_value(labels.iter().map(|(c, b)| (*c, b.as_slice())), &mut buf)
            .unwrap();
        encoded.push(buf);
    }
    let values = BinaryArray::from_iter_values(encoded.iter().map(|v| v.as_slice()));
    let keys: Vec<u32> = (0..ROWS_PER_BATCH)
        .map(|i| (i % unique_keys) as u32)
        .collect();
    Arc::new(DictionaryArray::new(
        UInt32Array::from(keys),
        Arc::new(values),
    ))
}

fn build_legacy_record_batch(unique_keys: usize, string_len: usize) -> RecordBatch {
    // Sparse legacy schema: field columns + ts + __primary_key + __sequence + __op_type.
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", ArrowDataType::Float64, false),
        Field::new(
            "ts",
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "__primary_key",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::UInt32),
                Box::new(ArrowDataType::Binary),
            ),
            false,
        ),
        Field::new("__sequence", ArrowDataType::UInt64, false),
        Field::new("__op_type", ArrowDataType::UInt8, false),
    ]));

    let value: ArrayRef = Arc::new(datatypes::arrow::array::Float64Array::from(vec![
        1.0_f64;
        ROWS_PER_BATCH
    ]));
    let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![0_i64; ROWS_PER_BATCH]));
    let pk = build_pk_dict_array(unique_keys, string_len);
    let seq: ArrayRef = Arc::new(UInt64Array::from(vec![1_u64; ROWS_PER_BATCH]));
    let op: ArrayRef = Arc::new(UInt8Array::from(vec![1_u8; ROWS_PER_BATCH]));

    RecordBatch::try_new(schema, vec![value, ts, pk, seq, op]).unwrap()
}

fn bench_convert_batch(c: &mut Criterion) {
    let metadata = build_sparse_metadata();
    let column_ids: Vec<u32> = metadata
        .column_metadatas
        .iter()
        .map(|c| c.column_id)
        .collect();

    let mut group = c.benchmark_group("flat_format/convert_batch");
    for &unique_keys in &[100usize, 1024, 4096, 8192] {
        for &string_len in &[8usize, 24, 64] {
            let format = FlatReadFormat::new(
                Arc::clone(&metadata),
                column_ids.iter().copied(),
                None,
                "bench",
                false,
            )
            .unwrap();
            let batch = build_legacy_record_batch(unique_keys, string_len);
            group.bench_function(
                BenchmarkId::from_parameter(format!("keys={unique_keys}/strlen={string_len}")),
                |b| {
                    let mut buffers: Vec<Vec<u8>> = Vec::new();
                    b.iter(|| {
                        let out = format
                            .convert_batch(black_box(batch.clone()), None, &mut buffers)
                            .unwrap();
                        black_box(out);
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_convert_batch);
criterion_main!(benches);
