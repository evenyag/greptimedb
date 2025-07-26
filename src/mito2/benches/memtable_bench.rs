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

use std::sync::Arc;
use std::thread;

use api::v1::value::ValueData;
use api::v1::{Row, Rows, SemanticType};
use common_time::Timestamp;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_common::Column;
use datafusion_expr::{lit, Expr};
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use mito2::memtable::bulk::BulkMemtable;
use mito2::memtable::partition_tree::{PartitionTreeConfig, PartitionTreeMemtable};
use mito2::memtable::time_series::TimeSeriesMemtable;
use mito2::memtable::{KeyValues, Memtable};
use mito2::read::scan_region::PredicateGroup;
use mito2::region::options::MergeMode;
use mito2::test_util::memtable_util::{self, region_metadata_to_row_schema};
use mito_codec::row_converter::DensePrimaryKeyCodec;
use rand::rngs::ThreadRng;
use rand::seq::IndexedRandom;
use rand::Rng;
use store_api::metadata::{
    ColumnMetadata, RegionMetadata, RegionMetadataBuilder, RegionMetadataRef,
};
use store_api::storage::RegionId;
use table::predicate::Predicate;

/// Writes rows.
fn write_rows(c: &mut Criterion) {
    let metadata = memtable_util::metadata_with_primary_key(vec![1, 0], true);
    let timestamps = (0..100).collect::<Vec<_>>();

    // Note that this test only generate one time series.
    let mut group = c.benchmark_group("write");
    group.bench_function("partition_tree", |b| {
        let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));
        let memtable = PartitionTreeMemtable::new(
            1,
            codec,
            metadata.clone(),
            None,
            &PartitionTreeConfig::default(),
        );
        let kvs =
            memtable_util::build_key_values(&metadata, "hello".to_string(), 42, &timestamps, 1);
        b.iter(|| {
            memtable.write(&kvs).unwrap();
        });
    });
    group.bench_function("time_series", |b| {
        let memtable = TimeSeriesMemtable::new(metadata.clone(), 1, None, true, MergeMode::LastRow);
        let kvs =
            memtable_util::build_key_values(&metadata, "hello".to_string(), 42, &timestamps, 1);
        b.iter(|| {
            memtable.write(&kvs).unwrap();
        });
    });
}

/// Scans all rows.
fn full_scan(c: &mut Criterion) {
    let metadata = Arc::new(cpu_metadata());
    let config = PartitionTreeConfig::default();
    let start_sec = 1710043200;
    let generator = CpuDataGenerator::new(metadata.clone(), 4000, start_sec, start_sec + 3600 * 2);

    let mut group = c.benchmark_group("full_scan");
    group.sample_size(10);
    group.bench_function("partition_tree", |b| {
        let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));
        let memtable = PartitionTreeMemtable::new(1, codec, metadata.clone(), None, &config);
        for kvs in generator.iter() {
            memtable.write(&kvs).unwrap();
        }

        b.iter(|| {
            let iter = memtable.iter(None, None, None).unwrap();
            for batch in iter {
                let _batch = batch.unwrap();
            }
        });
    });
    group.bench_function("time_series", |b| {
        let memtable = TimeSeriesMemtable::new(metadata.clone(), 1, None, true, MergeMode::LastRow);
        for kvs in generator.iter() {
            memtable.write(&kvs).unwrap();
        }

        b.iter(|| {
            let iter = memtable.iter(None, None, None).unwrap();
            for batch in iter {
                let _batch = batch.unwrap();
            }
        });
    });
    group.bench_function("bulk", |b| {
        let memtable = BulkMemtable::new(1, metadata.clone(), None);
        for kvs in generator.iter() {
            memtable.write(&kvs).unwrap();
        }
        memtable.freeze().unwrap();

        b.iter(|| {
            let iter = memtable.iter(None, None, None).unwrap();
            for batch in iter {
                let _batch = batch.unwrap();
            }
        });
    });
}

/// Filters 1 host.
fn filter_1_host(c: &mut Criterion) {
    let metadata = Arc::new(cpu_metadata());
    let config = PartitionTreeConfig::default();
    let start_sec = 1710043200;
    let generator = CpuDataGenerator::new(metadata.clone(), 4000, start_sec, start_sec + 3600 * 2);

    let mut group = c.benchmark_group("filter_1_host");
    group.sample_size(10);
    group.bench_function("partition_tree", |b| {
        let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));
        let memtable = PartitionTreeMemtable::new(1, codec, metadata.clone(), None, &config);
        for kvs in generator.iter() {
            memtable.write(&kvs).unwrap();
        }
        let predicate = generator.random_host_filter();

        b.iter(|| {
            let iter = memtable.iter(None, Some(predicate.clone()), None).unwrap();
            for batch in iter {
                let _batch = batch.unwrap();
            }
        });
    });
    group.bench_function("time_series", |b| {
        let memtable = TimeSeriesMemtable::new(metadata.clone(), 1, None, true, MergeMode::LastRow);
        for kvs in generator.iter() {
            memtable.write(&kvs).unwrap();
        }
        let predicate = generator.random_host_filter();

        b.iter(|| {
            let iter = memtable.iter(None, Some(predicate.clone()), None).unwrap();
            for batch in iter {
                let _batch = batch.unwrap();
            }
        });
    });
    group.bench_function("bulk", |b| {
        let memtable = BulkMemtable::new(1, metadata.clone(), None);
        for kvs in generator.iter() {
            memtable.write(&kvs).unwrap();
        }
        memtable.freeze().unwrap();
        let predicate = generator.random_host_filter();

        b.iter(|| {
            let ranges = memtable
                .ranges(
                    None,
                    PredicateGroup::new(&metadata, predicate.exprs()),
                    None,
                )
                .unwrap();
            for range in ranges.ranges.values() {
                let iter = range
                    .build_iter((Timestamp::MIN_MILLISECOND, Timestamp::MAX_MILLISECOND))
                    .unwrap();
                for batch in iter {
                    let _batch = batch.unwrap();
                }
            }
        });
    });
}

struct Host {
    hostname: String,
    region: String,
    datacenter: String,
    rack: String,
    os: String,
    arch: String,
    team: String,
    service: String,
    service_version: String,
    service_environment: String,
}

impl Host {
    fn random_with_id(id: usize) -> Host {
        let mut rng = rand::rng();
        let region = format!("ap-southeast-{}", rng.random_range(0..10));
        let datacenter = format!(
            "{}{}",
            region,
            ['a', 'b', 'c', 'd', 'e'].choose(&mut rng).unwrap()
        );
        Host {
            hostname: format!("host_{id}"),
            region,
            datacenter,
            rack: rng.random_range(0..100).to_string(),
            os: "Ubuntu16.04LTS".to_string(),
            arch: "x86".to_string(),
            team: "CHI".to_string(),
            service: rng.random_range(0..100).to_string(),
            service_version: rng.random_range(0..10).to_string(),
            service_environment: "test".to_string(),
        }
    }

    fn fill_values(&self, values: &mut Vec<api::v1::Value>) {
        let tags = [
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.hostname.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.region.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.datacenter.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.rack.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.os.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.arch.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.team.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.service.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.service_version.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.service_environment.clone())),
            },
        ];
        for tag in tags {
            values.push(tag);
        }
    }
}

struct CpuDataGenerator {
    metadata: RegionMetadataRef,
    column_schemas: Vec<api::v1::ColumnSchema>,
    hosts: Vec<Host>,
    start_sec: i64,
    end_sec: i64,
}

impl CpuDataGenerator {
    fn new(metadata: RegionMetadataRef, num_hosts: usize, start_sec: i64, end_sec: i64) -> Self {
        let column_schemas = region_metadata_to_row_schema(&metadata);
        Self {
            metadata,
            column_schemas,
            hosts: Self::generate_hosts(num_hosts),
            start_sec,
            end_sec,
        }
    }

    fn new_with_host_range(
        metadata: RegionMetadataRef,
        host_start: usize,
        host_count: usize,
        start_sec: i64,
        end_sec: i64,
    ) -> Self {
        let column_schemas = region_metadata_to_row_schema(&metadata);
        Self {
            metadata,
            column_schemas,
            hosts: Self::generate_hosts_with_range(host_start, host_count),
            start_sec,
            end_sec,
        }
    }

    fn iter(&self) -> impl Iterator<Item = KeyValues> + '_ {
        // point per 10s.
        (self.start_sec..self.end_sec)
            .step_by(10)
            .enumerate()
            .map(|(seq, ts)| self.build_key_values(seq, ts))
    }

    fn build_key_values(&self, seq: usize, current_sec: i64) -> KeyValues {
        let rows = self
            .hosts
            .iter()
            .map(|host| {
                let mut rng = rand::rng();
                let mut values = Vec::with_capacity(21);
                values.push(api::v1::Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(current_sec * 1000)),
                });
                host.fill_values(&mut values);
                for _ in 0..10 {
                    values.push(api::v1::Value {
                        value_data: Some(ValueData::F64Value(Self::random_f64(&mut rng))),
                    });
                }
                Row { values }
            })
            .collect();
        let mutation = api::v1::Mutation {
            op_type: api::v1::OpType::Put as i32,
            sequence: seq as u64,
            rows: Some(Rows {
                schema: self.column_schemas.clone(),
                rows,
            }),
            write_hint: None,
        };

        KeyValues::new(&self.metadata, mutation).unwrap()
    }

    fn random_host_filter(&self) -> Predicate {
        let host = self.random_hostname();
        let expr = Expr::Column(Column::from_name("hostname")).eq(lit(host));
        Predicate::new(vec![expr])
    }

    fn random_hostname(&self) -> String {
        let mut rng = rand::rng();
        self.hosts.choose(&mut rng).unwrap().hostname.clone()
    }

    fn random_f64(rng: &mut ThreadRng) -> f64 {
        let base: u32 = rng.random_range(30..95);
        base as f64
    }

    fn generate_hosts(num_hosts: usize) -> Vec<Host> {
        (0..num_hosts).map(Host::random_with_id).collect()
    }

    fn generate_hosts_with_range(host_start: usize, host_count: usize) -> Vec<Host> {
        (host_start..host_start + host_count)
            .map(Host::random_with_id)
            .collect()
    }
}

/// Creates a metadata for TSBS cpu-like table.
fn cpu_metadata() -> RegionMetadata {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
    builder.push_column_metadata(ColumnMetadata {
        column_schema: ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        semantic_type: SemanticType::Timestamp,
        column_id: 0,
    });
    let mut column_id = 1;
    let tags = [
        "hostname",
        "region",
        "datacenter",
        "rack",
        "os",
        "arch",
        "team",
        "service",
        "service_version",
        "service_environment",
    ];
    for tag in tags {
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(tag, ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id,
        });
        column_id += 1;
    }
    let fields = [
        "usage_user",
        "usage_system",
        "usage_idle",
        "usage_nice",
        "usage_iowait",
        "usage_irq",
        "usage_softirq",
        "usage_steal",
        "usage_guest",
        "usage_guest_nice",
    ];
    for field in fields {
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(field, ConcreteDataType::float64_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id,
        });
        column_id += 1;
    }
    builder.primary_key(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    builder.build().unwrap()
}

fn bulk_memtable_write_2m_rows(c: &mut Criterion) {
    let metadata = Arc::new(cpu_metadata());
    let start_sec = 1710043200;
    let generator = CpuDataGenerator::new(metadata.clone(), 4000, start_sec, start_sec + 3600 * 2);

    let mut group = c.benchmark_group("write_2m_bulk_memtable");
    group.sample_size(10);
    group.bench_function("write_2m_rows", |b| {
        b.iter(|| {
            let memtable = BulkMemtable::new(1, metadata.clone(), None);
            let mut total_rows = 0;
            for kvs in generator.iter() {
                memtable.write(&kvs).unwrap();
                total_rows += kvs.num_rows();
                if total_rows >= 2_000_000 {
                    break;
                }
            }
        });
    });
}

fn time_series_memtable_write_2m_rows(c: &mut Criterion) {
    let metadata = Arc::new(cpu_metadata());
    let start_sec = 1710043200;
    let generator = CpuDataGenerator::new(metadata.clone(), 4000, start_sec, start_sec + 3600 * 2);

    let mut group = c.benchmark_group("write_2m_time_series_memtable");
    group.sample_size(10);
    group.bench_function("write_2m_rows", |b| {
        b.iter(|| {
            let memtable =
                TimeSeriesMemtable::new(metadata.clone(), 1, None, true, MergeMode::LastRow);
            let mut total_rows = 0;
            for kvs in generator.iter() {
                memtable.write(&kvs).unwrap();
                total_rows += kvs.num_rows();
                if total_rows >= 2_000_000 {
                    break;
                }
            }
        });
    });
}

fn concurrent_write_benchmark(c: &mut Criterion) {
    let metadata = Arc::new(cpu_metadata());
    let start_sec = 1710043200;
    let num_hosts = 8000;
    let num_threads = 8;
    let writes_per_thread = 1_000_000;
    let hosts_per_thread = num_hosts / num_threads;

    let mut group = c.benchmark_group("concurrent_write");
    group.sample_size(10);

    group.bench_function("bulk_memtable_concurrent", |b| {
        b.iter(|| {
            let memtable = Arc::new(BulkMemtable::new(1, metadata.clone(), None));

            let handles: Vec<_> = (0..num_threads)
                .map(|thread_id| {
                    let memtable = Arc::clone(&memtable);
                    let metadata = Arc::clone(&metadata);

                    thread::spawn(move || {
                        let host_start = thread_id * hosts_per_thread;
                        let thread_generator = CpuDataGenerator::new_with_host_range(
                            metadata,
                            host_start,
                            hosts_per_thread,
                            start_sec,
                            start_sec + 10_000,
                        );

                        let mut writes_done = 0;
                        for kvs in thread_generator.iter() {
                            if writes_done >= writes_per_thread {
                                break;
                            }
                            memtable.write(&kvs).unwrap();
                            writes_done += 1;
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });

    group.bench_function("time_series_memtable_concurrent", |b| {
        b.iter(|| {
            let memtable = Arc::new(TimeSeriesMemtable::new(
                metadata.clone(),
                1,
                None,
                true,
                MergeMode::LastRow,
            ));

            let handles: Vec<_> = (0..num_threads)
                .map(|thread_id| {
                    let memtable = Arc::clone(&memtable);
                    let metadata = Arc::clone(&metadata);

                    thread::spawn(move || {
                        let host_start = thread_id * hosts_per_thread;
                        let thread_generator = CpuDataGenerator::new_with_host_range(
                            metadata,
                            host_start,
                            hosts_per_thread,
                            start_sec,
                            start_sec + 10_000,
                        );

                        let mut writes_done = 0;
                        for kvs in thread_generator.iter() {
                            if writes_done >= writes_per_thread {
                                break;
                            }
                            memtable.write(&kvs).unwrap();
                            writes_done += 1;
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
}

criterion_group!(
    benches,
    write_rows,
    full_scan,
    filter_1_host,
    bulk_memtable_write_2m_rows,
    time_series_memtable_write_2m_rows,
    concurrent_write_benchmark
);
criterion_main!(benches);
