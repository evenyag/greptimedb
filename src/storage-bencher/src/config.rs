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

//! Benchmark configs.

use std::fs::File;
use std::io::Read;
use std::time::Duration;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct BenchConfig {
    pub parquet_path: String,
    #[serde(with = "humantime_serde")]
    pub measurement_time: Duration,
    pub sample_size: usize,
    pub scan_batch_size: usize,
    pub print_metrics: bool,
    /// Index of columns to read, empty for all columns.
    pub columns: Vec<usize>,
}

impl Default for BenchConfig {
    fn default() -> BenchConfig {
        BenchConfig {
            parquet_path: "".to_string(),
            measurement_time: Duration::from_secs(30),
            sample_size: 30,
            scan_batch_size: 1024,
            print_metrics: false,
            columns: Vec::new(),
        }
    }
}

impl BenchConfig {
    pub fn parse_toml(path: &str) -> BenchConfig {
        let mut file = File::open(path).unwrap();
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();

        toml::from_str(&content).unwrap()
    }
}
