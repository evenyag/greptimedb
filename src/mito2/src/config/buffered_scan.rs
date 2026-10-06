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

//! Startup-resolved configuration for experimental buffered scans.

use std::path::PathBuf;

use common_base::memory_limit::MemoryLimit;
use common_base::readable_size::ReadableSize;
use common_stat::{get_total_cpu_cores, get_total_memory_bytes};
use serde::{Deserialize, Serialize};

use crate::error::{InvalidConfigSnafu, Result};
use crate::read::series_buffered::Options;
pub use crate::read::series_buffered::SourcePolicy;

/// Experimental complete-range preparation. Limits apply to one SeriesScan,
/// not the aggregate of all scans in a SQL or PromQL query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BufferedScanConfig {
    /// Enables buffered scans for eligible native sparse metric sources.
    pub enabled: bool,
    /// Hard tracked-memory budget, resolved against startup resource limits.
    pub memory_limit: MemoryLimit,
    /// Preparation reclamation threshold; omitted means one quarter of the budget.
    pub spill_threshold: Option<ReadableSize>,
    /// Per-scan spill disk quota, independent of the optional cache.
    pub disk_limit: ReadableSize,
    /// Scratch parent; omitted means an engine-owned directory under data home.
    pub scratch: Option<PathBuf>,
    /// Per-stage task ceiling; zero selects a startup CPU-based value.
    pub preparation_concurrency: usize,
    /// Source selection strategy. Byte admission still governs runnable work.
    pub source_policy: SourcePolicy,
    /// Maximum identities per discovery assignment chunk.
    pub candidate_chunk_size: usize,
    /// IPC result batch layout.
    pub layout: Layout,
    /// Compact identity storage encoding.
    pub key_encoding: KeyEncoding,
    /// IPC payload compression.
    pub compression: Compression,
    /// Target rows per IPC batch; a large series spans bounded batches.
    pub batch_rows: usize,
    /// Target logical bytes per IPC batch.
    pub batch_bytes: ReadableSize,
    /// Upper bound on one IPC file, including payload and metadata.
    pub file_bytes: ReadableSize,
    /// Maximum record batches in one IPC file.
    pub file_batches: usize,
    /// Upper bound on one IPC file's metadata.
    pub file_metadata_bytes: ReadableSize,
    /// Placement policy for preparation results.
    pub retention: Retention,
    /// Optional engine-owned cache, provisioned independently of scan budgets.
    pub cache: Option<BufferedCacheConfig>,
    /// Runtime options are resolved once by engine startup, never deserialized.
    #[serde(skip)]
    pub(crate) resolved: Option<Options>,
}

/// IPC batch scope, retaining both alternatives for regression attribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Layout {
    /// Each batch contains rows from one identity.
    OneSeries,
    /// Batches can contain rows from several identities.
    MultipleSeries,
}

/// Compact key encoding in prepared IPC files.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyEncoding {
    /// Store the 22-byte compact identity directly.
    #[default]
    Plain,
    /// Store indexes into one immutable dictionary built from selected identities.
    FixedDictionary,
}

/// IPC compression, independent of SST encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    /// Uncompressed IPC payloads.
    None,
    /// LZ4 frame compression.
    Lz4,
    /// Zstandard compression.
    Zstd,
}

/// Preparation placement policy; final replay never spills.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Retention {
    /// Spill unpublished results as the threshold or admission requires.
    #[default]
    Threshold,
    /// Store all preparation payload in IPC files.
    ForcedSpill,
    /// Keep payload resident and fail if admission requires spilling.
    Resident,
}

/// Optional ephemeral cache capacities. Cached output owns its tags and files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferedCacheConfig {
    /// Dedicated cache parent, distinct from the scan scratch parent.
    pub directory: PathBuf,
    /// Disk remains charged until pinned or retired files are deleted.
    pub disk_limit: ReadableSize,
    /// Capacity for indexes, fingerprints, dictionaries, and independently owned tags.
    pub metadata_limit: ReadableSize,
}

impl Default for BufferedScanConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            memory_limit: MemoryLimit::Percentage(50),
            spill_threshold: None,
            disk_limit: ReadableSize::gb(20),
            scratch: None,
            preparation_concurrency: 0,
            source_policy: SourcePolicy::SelectedSeriesPerPartition,
            candidate_chunk_size: 1_000_000,
            layout: Layout::MultipleSeries,
            key_encoding: KeyEncoding::Plain,
            compression: Compression::None,
            batch_rows: 1024,
            batch_bytes: ReadableSize::mb(1),
            file_bytes: ReadableSize::mb(64),
            file_batches: 1024,
            file_metadata_bytes: ReadableSize::mb(1),
            retention: Retention::Threshold,
            cache: None,
            resolved: None,
        }
    }
}

impl BufferedScanConfig {
    pub(crate) fn sanitize(&mut self, data_home: &str) -> Result<()> {
        self.resolved = None;
        if self.enabled {
            let memory = u64::try_from(get_total_memory_bytes()).unwrap_or_default();
            let cores = get_total_cpu_cores().max(1);
            let options = self.resolve(data_home, memory, cores)?;
            common_telemetry::info!(
                "Buffered scan startup settings: memory_available={memory}, cpu_cores={cores}, configured={self:?}, resolved={options:?}"
            );
            self.resolved = Some(options);
        }
        Ok(())
    }

    fn resolve(&self, data_home: &str, memory: u64, cores: usize) -> Result<Options> {
        let invalid = |reason: &str| {
            InvalidConfigSnafu {
                reason: format!("experimental_buffered_series_scan: {reason}"),
            }
            .build()
        };
        let bytes = |size: u64| {
            usize::try_from(size).map_err(|_| invalid("capacity exceeds platform address space"))
        };
        let memory_bytes = bytes(self.memory_limit.resolve(memory))?;
        if memory_bytes == 0 {
            return Err(invalid(
                "memory_limit must be finite and positive; set an absolute limit when system memory is unavailable",
            ));
        }
        let options = Options {
            scratch: self
                .scratch
                .clone()
                .unwrap_or_else(|| PathBuf::from(data_home).join("buffered-scan-scratch")),
            memory_bytes,
            spill_threshold: self
                .spill_threshold
                .map(|v| bytes(v.as_bytes()))
                .transpose()?
                .unwrap_or((memory_bytes / 4).max(1)),
            disk_bytes: bytes(self.disk_limit.as_bytes())?,
            preparation_concurrency: if self.preparation_concurrency == 0 {
                (cores / 4).clamp(1, 4)
            } else {
                self.preparation_concurrency
            },
            source_policy: self.source_policy,
            candidate_chunk_size: self.candidate_chunk_size,
            layout: match self.layout {
                Layout::OneSeries => "one_series",
                Layout::MultipleSeries => "multiple_series",
            }
            .to_owned(),
            compression: match self.compression {
                Compression::None => None,
                Compression::Lz4 => Some("lz4".to_owned()),
                Compression::Zstd => Some("zstd".to_owned()),
            },
            batch_rows: self.batch_rows,
            batch_bytes: bytes(self.batch_bytes.as_bytes())?,
            file_bytes: bytes(self.file_bytes.as_bytes())?,
            file_batches: self.file_batches,
            file_metadata_bytes: bytes(self.file_metadata_bytes.as_bytes())?,
            retention: self.retention,
            key_encoding: self.key_encoding,
            cache: self
                .cache
                .as_ref()
                .map(|cache| -> Result<_> {
                    if cache.directory.as_os_str().is_empty()
                        || cache.disk_limit.as_bytes() == 0
                        || cache.metadata_limit.as_bytes() == 0
                    {
                        return Err(invalid(
                            "cache directory and capacities must be nonempty and positive",
                        ));
                    }
                    Ok(crate::read::series_buffered::cache::Options {
                        directory: cache.directory.clone(),
                        disk_bytes: bytes(cache.disk_limit.as_bytes())?,
                        metadata_bytes: bytes(cache.metadata_limit.as_bytes())?,
                    })
                })
                .transpose()?,
        };
        if options.scratch.as_os_str().is_empty()
            || options.disk_bytes == 0
            || options.batch_rows == 0
            || options.batch_rows > u32::MAX as usize
            || options.batch_bytes == 0
            || options.file_bytes == 0
            || options.file_batches == 0
            || options.file_metadata_bytes == 0
        {
            return Err(invalid(
                "scratch must be nonempty and capacities positive; batch_rows must fit u32",
            ));
        }
        options.store().map_err(|e| invalid(&e.to_string()))?;
        Ok(options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loading_and_startup_resolution() {
        let default = BufferedScanConfig::default();
        assert!(!default.enabled);
        let automatic = default.resolve("/data", 16 << 30, 8).unwrap();
        assert_eq!(8 << 30, automatic.memory_bytes);
        assert_eq!(2 << 30, automatic.spill_threshold);
        assert_eq!(2, automatic.preparation_concurrency);
        let config: BufferedScanConfig = toml::from_str(
            r#"
            enabled = true
            memory_limit = "256MB"
            spill_threshold = "64MB"
            preparation_concurrency = 1
            layout = "one_series"
            compression = "lz4"
        "#,
        )
        .unwrap();
        assert_eq!(
            config,
            toml::from_str(&toml::to_string(&config).unwrap()).unwrap()
        );
        let explicit = config.resolve("/data", 0, 64).unwrap();
        assert_eq!(256 << 20, explicit.memory_bytes);
        assert_eq!(1, explicit.preparation_concurrency);
        assert_eq!(Some("lz4"), explicit.compression.as_deref());
        assert!(default.resolve("/data", 0, 1).is_err());
        let invalid = BufferedScanConfig {
            spill_threshold: Some(ReadableSize::gb(1)),
            ..config
        };
        assert!(invalid.resolve("/data", 0, 1).is_err());
    }
}
