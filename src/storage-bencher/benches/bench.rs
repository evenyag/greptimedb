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

use criterion::*;
use storage_bencher::config::BenchConfig;

const CONFIG_PATH: &str = "./bench-config.toml";

struct BenchContext {
    print_metrics: bool,
}

fn init_bench() -> BenchConfig {
    BenchConfig::parse_toml(CONFIG_PATH)
}

fn bench_storage_iter(b: &mut Bencher<'_>, ctx: &BenchContext) {
    b.iter(|| {
        if ctx.print_metrics {
            println!("metrics");
        }
    })
}

fn bench_storage(c: &mut Criterion) {
    let config = init_bench();

    println!("config is {:?}", config);

    let mut group = c.benchmark_group("scan_parquet");

    group.measurement_time(config.measurement_time);
    group.sample_size(config.sample_size);

    let ctx = BenchContext {
        print_metrics: config.print_metrics,
    };
    group.bench_with_input(
        BenchmarkId::new("test", config.parquet_path),
        &ctx,
        bench_storage_iter,
    );

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = bench_storage,
);

criterion_main!(benches);
