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

use std::env;
use std::sync::{Mutex, Once};

use common_runtime::{create_runtime, Runtime};
use common_telemetry::logging;
use criterion::*;
use once_cell::sync::Lazy;
use storage_bencher::config::BenchConfig;
use storage_bencher::loader::ParquetLoader;
use storage_bencher::scan_bench::ScanBench;
use storage_bencher::target::Target;

const BENCH_CONFIG_KEY: &str = "BENCH_CONFIG";
const BENCH_ENABLE_LOG_KEY: &str = "BENCH_ENABLE_LOG";
const DEFAULT_CONFIG_PATH: &str = "./bench-config.toml";
static GLOBAL_CONFIG: Lazy<Mutex<BenchConfig>> = Lazy::new(|| Mutex::new(BenchConfig::default()));

struct BenchContext {
    config: BenchConfig,
    runtime: Runtime,
}

impl BenchContext {
    fn new(config: BenchConfig) -> BenchContext {
        let runtime = create_runtime("bench", "bench-worker", config.runtime_size);
        BenchContext { config, runtime }
    }

    async fn new_scan_bench(&self) -> ScanBench {
        let loader = ParquetLoader::new(
            self.config.parquet_path.clone(),
            self.config.load_batch_size,
        );
        let target = Target::new(
            &self.config.storage.path,
            self.config.storage.engine_config(),
            self.config.storage.region_id,
        )
        .await;

        ScanBench::new(loader, target, self.config.scan_batch_size)
    }
}

fn init_bench() -> BenchConfig {
    let enable_log: bool = env::var(BENCH_ENABLE_LOG_KEY)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(false);
    if enable_log {
        common_telemetry::init_default_ut_logging();
    }
    let cwd = env::current_dir().unwrap();
    logging::info!("Init bench, current dir: {}", cwd.display());
    let config_path =
        env::var(BENCH_CONFIG_KEY).unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());

    static START: Once = Once::new();

    START.call_once(|| {
        logging::info!("Loading config from path: {}", config_path);

        let mut config = GLOBAL_CONFIG.lock().unwrap();
        *config = BenchConfig::parse_toml(&config_path);
    });

    let config = GLOBAL_CONFIG.lock().unwrap();
    (*config).clone()
}

fn bench_storage_iter(b: &mut Bencher<'_>, input: &(BenchContext, ScanBench)) {
    let mut times = 0;
    b.iter(|| {
        let metrics = input.0.runtime.block_on(async { input.1.run().await });

        if input.0.config.print_metrics_every > 0 {
            times += 1;
            if times % input.0.config.print_metrics_every == 0 {
                logging::info!("Metrics at times {} is: {:?}", times, metrics);
            }
        }
    })
}

fn bench_full_scan(c: &mut Criterion) {
    let config = init_bench();

    logging::info!("config is {:?}", config);

    let mut group = c.benchmark_group("full_scan");

    group.measurement_time(config.measurement_time);
    group.sample_size(config.sample_size);

    let parquet_path = config.parquet_path.clone();
    let ctx = BenchContext::new(config);
    let scan_bench = ctx.runtime.block_on(async {
        let mut scan_bench = ctx.new_scan_bench().await;
        scan_bench.maybe_prepare_data().await;

        scan_bench
    });
    let input = (ctx, scan_bench);
    group.bench_with_input(
        BenchmarkId::new("test", parquet_path),
        &input,
        bench_storage_iter,
    );

    input.0.runtime.block_on(async {
        input.1.shutdown().await;
    });

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = bench_full_scan,
);

criterion_main!(benches);
