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

#![allow(dead_code)]

use std::fs::read_to_string;
use std::path::Path;

#[cfg(target_os = "linux")]
use nix::sys::{statfs, statfs::statfs};
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntGauge, Opts};

const CGROUP_UNIFIED_MOUNTPOINT: &str = "/sys/fs/cgroup";

const MEMORY_MAX_FILE_CGROUP_V2: &str = "memory.max";
const MEMORY_MAX_FILE_CGROUP_V1: &str = "memory.limit_in_bytes";
const MEMORY_USAGE_FILE_CGROUP_V2: &str = "memory.current";
const CPU_MAX_FILE_CGROUP_V2: &str = "cpu.max";
const CPU_QUOTA_FILE_CGROUP_V1: &str = "cpu.cfs_quota_us";
const CPU_PERIOD_FILE_CGROUP_V1: &str = "cpu.cfs_period_us";
const CPU_USAGE_FILE_CGROUP_V2: &str = "cpu.stat";

// `MAX_VALUE_CGROUP_V2` string in `/sys/fs/cgroup/cpu.max` and `/sys/fs/cgroup/memory.max` to indicate that the resource is unlimited.
const MAX_VALUE_CGROUP_V2: &str = "max";

// For cgroup v1, if the memory is unlimited, it will return a very large value(different from platform) that close to 2^63.
// For easier comparison, if the memory limit is larger than 1PB we consider it as unlimited.
const MAX_MEMORY_IN_BYTES: i64 = 1125899906842624; // 1PB

/// Finite limits for the current process and its visible cgroup ancestors.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ProcessCgroupLimits {
    /// Hard memory limit; unrelated to current usage or reclaimable page cache.
    pub memory_bytes: Option<u64>,
    /// CPU quota, in millicores. CPU affinity must be considered separately.
    pub cpu_millicores: Option<u64>,
}

/// Resolves process membership against cgroup mounts, including systemd scopes.
/// Missing, unreadable, and unlimited controls contribute no finite limit.
/// This does not change the existing mount-root resource helpers.
pub fn get_process_cgroup_limits() -> ProcessCgroupLimits {
    #[cfg(target_os = "linux")]
    {
        process_cgroup_limits(
            &read_to_string("/proc/self/cgroup").unwrap_or_default(),
            &read_to_string("/proc/self/mountinfo").unwrap_or_default(),
            |path| read_to_string(path).ok(),
        )
    }
    #[cfg(not(target_os = "linux"))]
    ProcessCgroupLimits::default()
}

#[cfg(any(target_os = "linux", test))]
fn process_cgroup_limits(
    membership: &str,
    mounts: &str,
    read: impl Fn(&Path) -> Option<String>,
) -> ProcessCgroupLimits {
    let mut limits = ProcessCgroupLimits::default();
    for mount in mounts.lines() {
        let Some((description, filesystem)) = mount.split_once(" - ") else {
            continue;
        };
        let fields: Vec<_> = description.split_whitespace().collect();
        let fs: Vec<_> = filesystem.split_whitespace().collect();
        if fields.len() < 6 || fs.len() < 3 || !matches!(fs[0], "cgroup" | "cgroup2") {
            continue;
        }
        let root = unescape_mount_path(fields[3]);
        let mountpoint = unescape_mount_path(fields[4]);
        let mountpoint = Path::new(&mountpoint);
        for member in membership.lines() {
            let member: Vec<_> = member.splitn(3, ':').collect();
            if member.len() != 3 {
                continue;
            }
            let unified = fs[0] == "cgroup2" && member[1].is_empty();
            let controller = |name| {
                fs[0] == "cgroup"
                    && fs[2].split(',').any(|v| v == name)
                    && member[1].split(',').any(|v| v == name)
            };
            let memory = unified || controller("memory");
            let cpu = unified || controller("cpu");
            if !memory && !cpu {
                continue;
            }
            let path = Path::new(member[2]);
            // A cgroup namespace reports membership relative to its root,
            // while mountinfo can retain the original subtree mount root.
            let relative = path.strip_prefix(&root).or_else(|_| path.strip_prefix("/"));
            let Ok(relative) = relative else { continue };
            if relative
                .components()
                .any(|c| !matches!(c, std::path::Component::Normal(_)))
            {
                continue;
            }
            let mut directory = mountpoint.join(relative);
            loop {
                if memory {
                    let file = if unified {
                        MEMORY_MAX_FILE_CGROUP_V2
                    } else {
                        MEMORY_MAX_FILE_CGROUP_V1
                    };
                    if let Some(value) =
                        read(&directory.join(file)).and_then(|v| v.trim().parse::<u64>().ok())
                        && (unified || value < MAX_MEMORY_IN_BYTES as u64)
                    {
                        limits.memory_bytes =
                            Some(limits.memory_bytes.map_or(value, |old| old.min(value)));
                    }
                }
                if cpu {
                    let quota_period = if unified {
                        read(&directory.join(CPU_MAX_FILE_CGROUP_V2)).and_then(|v| {
                            let mut values = v.split_whitespace();
                            Some((
                                values.next()?.parse::<u64>().ok()?,
                                values.next()?.parse::<u64>().ok()?,
                            ))
                        })
                    } else {
                        read(&directory.join(CPU_QUOTA_FILE_CGROUP_V1)).and_then(|quota| {
                            Some((
                                quota.trim().parse::<u64>().ok()?,
                                read(&directory.join(CPU_PERIOD_FILE_CGROUP_V1))?
                                    .trim()
                                    .parse::<u64>()
                                    .ok()?,
                            ))
                        })
                    };
                    if let Some((quota, period)) = quota_period
                        && quota > 0
                        && period > 0
                        && let Some(value) = quota.checked_mul(1000).map(|v| v.div_ceil(period))
                    {
                        limits.cpu_millicores =
                            Some(limits.cpu_millicores.map_or(value, |old| old.min(value)));
                    }
                }
                if directory == mountpoint || !directory.pop() {
                    break;
                }
            }
        }
    }
    limits
}

#[cfg(any(target_os = "linux", test))]
fn unescape_mount_path(path: &str) -> String {
    path.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

/// Get the limit of memory in bytes from cgroups filesystem.
///
/// - If the cgroup total memory is unset, return `None`.
/// - Return `None` if it fails to read the memory limit or not on linux.
pub fn get_memory_limit_from_cgroups() -> Option<i64> {
    #[cfg(target_os = "linux")]
    {
        let memory_max_file = if is_cgroup_v2()? {
            // Read `/sys/fs/cgroup/memory.max` to get the memory limit.
            MEMORY_MAX_FILE_CGROUP_V2
        } else {
            // Read `/sys/fs/cgroup/memory.limit_in_bytes` to get the memory limit.
            MEMORY_MAX_FILE_CGROUP_V1
        };

        // For cgroup v1, it will return a very large value(different from platform) if the memory is unset.
        let memory_limit =
            read_value_from_file(Path::new(CGROUP_UNIFIED_MOUNTPOINT).join(memory_max_file))?;

        // If memory limit exceeds 1PB(cgroup v1), consider it as unset.
        if memory_limit > MAX_MEMORY_IN_BYTES {
            return None;
        }
        Some(memory_limit)
    }

    #[cfg(not(target_os = "linux"))]
    None
}

/// Get the usage of memory in bytes from cgroups filesystem.
///
/// - Return `None` if it fails to read the memory usage or not on linux or cgroup is v1.
pub fn get_memory_usage_from_cgroups() -> Option<i64> {
    #[cfg(target_os = "linux")]
    {
        if is_cgroup_v2()? {
            let usage = read_value_from_file(
                Path::new(CGROUP_UNIFIED_MOUNTPOINT).join(MEMORY_USAGE_FILE_CGROUP_V2),
            )?;
            Some(usage)
        } else {
            None
        }
    }

    #[cfg(not(target_os = "linux"))]
    None
}

/// Get the limit of cpu in millicores from cgroups filesystem.
///
/// - If the cpu limit is unset, return `None`.
/// - Return `None` if it fails to read the cpu limit or not on linux.
pub fn get_cpu_limit_from_cgroups() -> Option<i64> {
    #[cfg(target_os = "linux")]
    if is_cgroup_v2()? {
        // Read `/sys/fs/cgroup/cpu.max` to get the cpu limit.
        get_cgroup_v2_cpu_limit(Path::new(CGROUP_UNIFIED_MOUNTPOINT).join(CPU_MAX_FILE_CGROUP_V2))
    } else {
        // Read `/sys/fs/cgroup/cpu.cfs_quota_us` and `/sys/fs/cgroup/cpu.cfs_period_us` to get the cpu limit.
        let quota = read_value_from_file(
            Path::new(CGROUP_UNIFIED_MOUNTPOINT).join(CPU_QUOTA_FILE_CGROUP_V1),
        )?;

        let period = read_value_from_file(
            Path::new(CGROUP_UNIFIED_MOUNTPOINT).join(CPU_PERIOD_FILE_CGROUP_V1),
        )?;

        // Return the cpu limit in millicores.
        Some(quota * 1000 / period)
    }

    #[cfg(not(target_os = "linux"))]
    None
}

/// Get the usage of cpu in millicores from cgroups filesystem.
///
/// - Return `None` if it's not in the cgroups v2 environment or fails to read the cpu usage.
pub fn get_cpu_usage_from_cgroups() -> Option<i64> {
    // In certain bare-metal environments, the `/sys/fs/cgroup/cpu.stat` file may be present and reflect system-wide CPU usage rather than container-specific metrics.
    // To ensure accurate collection of container-level CPU usage, verify the existence of the `/sys/fs/cgroup/memory.current` file.
    // The presence of this file typically indicates execution within a containerized environment, thereby validating the relevance of the collected CPU usage data.
    if !Path::new(CGROUP_UNIFIED_MOUNTPOINT)
        .join(MEMORY_USAGE_FILE_CGROUP_V2)
        .exists()
    {
        return None;
    }

    // Read `/sys/fs/cgroup/cpu.stat` to get `usage_usec`.
    let content =
        read_to_string(Path::new(CGROUP_UNIFIED_MOUNTPOINT).join(CPU_USAGE_FILE_CGROUP_V2)).ok()?;

    // Read the first line of the content. It will be like this: `usage_usec 447926`.
    let first_line = content.lines().next()?;
    let fields = first_line.split(' ').collect::<Vec<&str>>();
    if fields.len() != 2 {
        return None;
    }

    fields[1].trim().parse::<i64>().ok()
}

// Calculate the cpu usage in millicores from cgroups filesystem.
//
// - Return `0` if the current cpu usage is equal to the last cpu usage or the interval is 0.
pub(crate) fn calculate_cpu_usage(
    current_cpu_usage_usecs: i64,
    last_cpu_usage_usecs: i64,
    interval_milliseconds: i64,
) -> i64 {
    let diff = current_cpu_usage_usecs - last_cpu_usage_usecs;
    if diff > 0 && interval_milliseconds > 0 {
        ((diff as f64 / interval_milliseconds as f64).round() as i64).max(1)
    } else {
        0
    }
}

// Check whether the cgroup is v2.
// - Return `true` if the cgroup is v2, otherwise return `false`.
// - Return `None` if the detection fails or not on linux.
fn is_cgroup_v2() -> Option<bool> {
    #[cfg(target_os = "linux")]
    {
        let path = Path::new(CGROUP_UNIFIED_MOUNTPOINT);
        let fs_stat = statfs(path).ok()?;
        Some(fs_stat.filesystem_type() == statfs::CGROUP2_SUPER_MAGIC)
    }

    #[cfg(not(target_os = "linux"))]
    None
}

fn read_value_from_file<P: AsRef<Path>>(path: P) -> Option<i64> {
    let content = read_to_string(&path).ok()?;

    // If the content starts with "max", return `None`.
    if content.starts_with(MAX_VALUE_CGROUP_V2) {
        return None;
    }

    content.trim().parse::<i64>().ok()
}

fn get_cgroup_v2_cpu_limit<P: AsRef<Path>>(path: P) -> Option<i64> {
    let content = read_to_string(&path).ok()?;

    let fields = content.trim().split(' ').collect::<Vec<&str>>();
    if fields.len() != 2 {
        return None;
    }

    // If the cgroup cpu limit is unset, return `None`.
    let quota = fields[0].trim();
    if quota == MAX_VALUE_CGROUP_V2 {
        return None;
    }

    let quota = quota.parse::<i64>().ok()?;

    let period = fields[1].trim();
    let period = period.parse::<i64>().ok()?;

    // Return the cpu limit in millicores.
    Some(quota * 1000 / period)
}

/// A collector that collects cgroups metrics.
#[derive(Debug)]
pub struct CgroupsMetricsCollector {
    descs: Vec<Desc>,
    memory_usage: IntGauge,
    cpu_usage: IntGauge,
}

impl Default for CgroupsMetricsCollector {
    fn default() -> Self {
        let mut descs = vec![];
        let cpu_usage = IntGauge::with_opts(Opts::new(
            "greptime_cgroups_cpu_usage_microseconds",
            "the current cpu usage in microseconds that collected from cgroups filesystem",
        ))
        .unwrap();
        descs.extend(cpu_usage.desc().into_iter().cloned());

        let memory_usage = IntGauge::with_opts(Opts::new(
            "greptime_cgroups_memory_usage_bytes",
            "the current memory usage that collected from cgroups filesystem",
        ))
        .unwrap();
        descs.extend(memory_usage.desc().into_iter().cloned());

        Self {
            descs,
            memory_usage,
            cpu_usage,
        }
    }
}

impl Collector for CgroupsMetricsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        if let Some(cpu_usage) = get_cpu_usage_from_cgroups() {
            self.cpu_usage.set(cpu_usage);
        }

        if let Some(memory_usage) = get_memory_usage_from_cgroups() {
            self.memory_usage.set(memory_usage);
        }

        let mut mfs = Vec::with_capacity(self.descs.len());
        mfs.extend(self.cpu_usage.collect());
        mfs.extend(self.memory_usage.collect());
        mfs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_cgroup_limits() {
        let files = [
            ("/cg/memory.max", "max"),
            ("/cg/system.slice/memory.max", "2048"),
            ("/cg/system.slice/db.service/memory.max", "4096"),
            ("/cg/system.slice/cpu.max", "150000 100000"),
            ("/cg/system.slice/db.service/cpu.max", "max 100000"),
        ];
        let read = |path: &Path| {
            files
                .iter()
                .find(|(p, _)| path == Path::new(p))
                .map(|(_, v)| v.to_string())
        };
        let mounts = "1 0 0:1 / /cg rw - cgroup2 cgroup rw";
        assert_eq!(
            process_cgroup_limits("0::/system.slice/db.service", mounts, read),
            ProcessCgroupLimits {
                memory_bytes: Some(2048),
                cpu_millicores: Some(1500)
            }
        );
        assert_eq!(
            process_cgroup_limits("0::/", mounts, read),
            ProcessCgroupLimits::default()
        );
        assert_eq!(
            process_cgroup_limits("0::/../outside", mounts, read),
            ProcessCgroupLimits::default()
        );
    }

    #[test]
    fn test_process_cgroup_subtree_mounts() {
        let mount = "1 0 0:1 /host/container /cg\\040space rw - cgroup2 cgroup rw";
        for membership in ["0::/host/container/worker", "0::/worker"] {
            let limits = process_cgroup_limits(membership, mount, |path| {
                (path == Path::new("/cg space/worker/memory.max")).then(|| "1024".to_owned())
            });
            assert_eq!(limits.memory_bytes, Some(1024));
        }
    }

    #[test]
    fn test_process_cgroup_v1_limits() {
        let mounts = "1 0 0:1 / /memory rw - cgroup cgroup rw,memory\n2 0 0:2 / /cpu rw - cgroup cgroup rw,cpu,cpuacct";
        let files = [
            ("/memory/service/memory.limit_in_bytes", "2048"),
            ("/memory/memory.limit_in_bytes", "9223372036854771712"),
            ("/cpu/service/cpu.cfs_quota_us", "25000"),
            ("/cpu/service/cpu.cfs_period_us", "100000"),
            ("/cpu/cpu.cfs_quota_us", "-1"),
            ("/cpu/cpu.cfs_period_us", "0"),
        ];
        let limits = process_cgroup_limits(
            "5:memory:/service\n4:cpu,cpuacct:/service",
            mounts,
            |path| {
                files
                    .iter()
                    .find(|(p, _)| path == Path::new(p))
                    .map(|(_, v)| v.to_string())
            },
        );
        assert_eq!(
            limits,
            ProcessCgroupLimits {
                memory_bytes: Some(2048),
                cpu_millicores: Some(250)
            }
        );
    }

    #[test]
    fn test_read_value_from_file() {
        assert_eq!(
            read_value_from_file(Path::new("testdata").join("memory.max")).unwrap(),
            100000
        );
        assert_eq!(
            read_value_from_file(Path::new("testdata").join("memory.max.unlimited")),
            None
        );
        assert_eq!(read_value_from_file(Path::new("non_existent_file")), None);
    }

    #[test]
    fn test_get_cgroup_v2_cpu_limit() {
        assert_eq!(
            get_cgroup_v2_cpu_limit(Path::new("testdata").join("cpu.max")).unwrap(),
            1500
        );
        assert_eq!(
            get_cgroup_v2_cpu_limit(Path::new("testdata").join("cpu.max.unlimited")),
            None
        );
        assert_eq!(
            get_cgroup_v2_cpu_limit(Path::new("non_existent_file")),
            None
        );
    }
}
