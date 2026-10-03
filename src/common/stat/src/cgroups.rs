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
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntGauge, Opts};

const MAX_MEMORY_IN_BYTES: i64 = 1125899906842624;

/// A controller's process directory and the boundary of its visible mount.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ControllerPath {
    directory: PathBuf,
    mount: PathBuf,
    v2: bool,
}

#[derive(Debug, Default)]
struct CgroupPaths {
    memory: Option<ControllerPath>,
    cpu: Option<ControllerPath>,
    cpuacct: Option<ControllerPath>,
}

static PATHS: LazyLock<CgroupPaths> = LazyLock::new(|| {
    #[cfg(target_os = "linux")]
    {
        let paths = match (
            read_to_string("/proc/self/cgroup"),
            read_to_string("/proc/self/mountinfo"),
        ) {
            (Ok(membership), Ok(mounts)) => CgroupPaths::parse(&membership, &mounts),
            _ => CgroupPaths::default(),
        };
        common_telemetry::info!("Resolved process cgroup controllers: {:?}", paths);
        paths
    }
    #[cfg(not(target_os = "linux"))]
    CgroupPaths::default()
});

impl CgroupPaths {
    fn parse(membership: &str, mounts: &str) -> Self {
        Self {
            memory: resolve_controller(membership, mounts, "memory"),
            cpu: resolve_controller(membership, mounts, "cpu"),
            cpuacct: resolve_controller(membership, mounts, "cpuacct"),
        }
    }
}

// mountinfo escapes spaces, tabs, newlines and backslashes as octal bytes.
fn unescape_mount(value: &str) -> String {
    value
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

fn resolve_controller(membership: &str, mounts: &str, controller: &str) -> Option<ControllerPath> {
    let dedicated = membership.lines().any(|line| {
        line.split(':')
            .nth(1)
            .is_some_and(|controllers| controllers.split(',').any(|c| c == controller))
    });
    let mut candidates = Vec::new();
    for line in membership.lines() {
        let fields: Vec<_> = line.splitn(3, ':').collect();
        if fields.len() != 3 {
            continue;
        }
        let unified = fields[0] == "0" && fields[1].is_empty();
        if unified && dedicated {
            continue;
        }
        if !unified && !fields[1].split(',').any(|c| c == controller) {
            continue;
        }
        let group = Path::new(fields[2]);
        if !group.is_absolute()
            || group
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            continue;
        }
        for mount in mounts.lines() {
            let Some((left, right)) = mount.split_once(" - ") else {
                continue;
            };
            let left: Vec<_> = left.split_whitespace().collect();
            let right: Vec<_> = right.split_whitespace().collect();
            if left.len() < 6 || right.len() < 3 {
                continue;
            }
            if unified {
                if right[0] != "cgroup2" {
                    continue;
                }
            } else if right[0] != "cgroup" || !right[2].split(',').any(|c| c == controller) {
                continue;
            }
            let root = PathBuf::from(unescape_mount(left[3]));
            let mount = PathBuf::from(unescape_mount(left[4]));
            // In a cgroup namespace, membership is relative to the namespace
            // root, even when mountinfo exposes the host's non-root mount root.
            let relative = match group.strip_prefix(&root) {
                Ok(relative) => relative,
                Err(_) if group == Path::new("/") => Path::new(""),
                Err(_) => continue,
            };
            candidates.push((
                root.components().count(),
                ControllerPath {
                    directory: mount.join(relative),
                    mount,
                    v2: unified,
                },
            ));
        }
    }
    candidates
        .into_iter()
        .max_by_key(|(depth, _)| *depth)
        .map(|(_, path)| path)
}

impl ControllerPath {
    fn minimum_limit(&self, read: impl Fn(&Path) -> Option<i64>) -> Option<i64> {
        self.directory
            .ancestors()
            .take_while(|p| p.starts_with(&self.mount))
            .filter_map(read)
            .min()
    }
}

/// Returns the smallest visible hard memory cap, excluding memory.high.
pub fn get_memory_limit_from_cgroups() -> Option<i64> {
    let path = PATHS.memory.as_ref()?;
    path.minimum_limit(|dir| {
        let value = read_value_from_file(dir.join(if path.v2 {
            "memory.max"
        } else {
            "memory.limit_in_bytes"
        }))?;
        (value >= 0 && (path.v2 || value < MAX_MEMORY_IN_BYTES)).then_some(value)
    })
}

/// Returns the process group's pressure threshold, independently of its hard cap.
pub fn get_memory_high_from_cgroups() -> Option<i64> {
    let path = PATHS.memory.as_ref()?;
    if !path.v2 {
        return None;
    }
    path.minimum_limit(|dir| read_value_from_file(dir.join("memory.high")))
}

/// Returns current memory usage of the process group, in bytes.
pub fn get_memory_usage_from_cgroups() -> Option<i64> {
    let path = PATHS.memory.as_ref()?;
    read_value_from_file(path.directory.join(if path.v2 {
        "memory.current"
    } else {
        "memory.usage_in_bytes"
    }))
}

/// Returns the smallest visible CPU quota in millicores.
pub fn get_cpu_limit_from_cgroups() -> Option<i64> {
    let path = PATHS.cpu.as_ref()?;
    path.minimum_limit(|dir| {
        if path.v2 {
            get_cgroup_v2_cpu_limit(dir.join("cpu.max"))
        } else {
            cpu_limit(
                read_value_from_file(dir.join("cpu.cfs_quota_us"))?,
                read_value_from_file(dir.join("cpu.cfs_period_us"))?,
            )
        }
    })
}

/// Returns cumulative CPU usage in microseconds (v1 cpuacct is nanoseconds).
pub fn get_cpu_usage_from_cgroups() -> Option<i64> {
    let path = PATHS.cpuacct.as_ref()?;
    if path.v2 {
        cpu_usage_usec(&read_to_string(path.directory.join("cpu.stat")).ok()?)
    } else {
        read_value_from_file(path.directory.join("cpuacct.usage")).map(|value| value / 1000)
    }
}

fn cpu_usage_usec(content: &str) -> Option<i64> {
    content.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        if fields.next()? != "usage_usec" {
            return None;
        }
        let value = fields.next()?.parse::<i64>().ok()?;
        (value >= 0 && fields.next().is_none()).then_some(value)
    })
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

fn read_value_from_file<P: AsRef<Path>>(path: P) -> Option<i64> {
    read_to_string(path).ok()?.trim().parse().ok()
}

fn cpu_limit(quota: i64, period: i64) -> Option<i64> {
    if quota <= 0 || period <= 0 {
        return None;
    }
    quota.checked_mul(1000)?.checked_div(period)
}

fn get_cgroup_v2_cpu_limit<P: AsRef<Path>>(path: P) -> Option<i64> {
    let content = read_to_string(path).ok()?;
    let fields: Vec<_> = content.split_whitespace().collect();
    if fields.len() != 2 {
        return None;
    }
    cpu_limit(fields[0].parse().ok()?, fields[1].parse().ok()?)
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
        let mut mfs = Vec::with_capacity(self.descs.len());
        if let Some(cpu_usage) = get_cpu_usage_from_cgroups() {
            self.cpu_usage.set(cpu_usage);
            mfs.extend(self.cpu_usage.collect());
        }
        if let Some(memory_usage) = get_memory_usage_from_cgroups() {
            self.memory_usage.set(memory_usage);
            mfs.extend(self.memory_usage.collect());
        }
        mfs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    #[test]
    fn test_process_mount_mapping() {
        let mounts = "31 20 0:28 / /sys/fs/cgroup rw - cgroup2 cgroup rw";
        let path = resolve_controller("0::/user.slice/user-1000.slice/job.scope", mounts, "memory")
            .unwrap();
        assert_eq!(
            path.directory,
            Path::new("/sys/fs/cgroup/user.slice/user-1000.slice/job.scope")
        );
        let mounts = "31 20 0:28 /tenant /visible rw - cgroup2 cgroup rw";
        assert_eq!(
            resolve_controller("0::/tenant/job", mounts, "cpu")
                .unwrap()
                .directory,
            Path::new("/visible/job")
        );
        assert_eq!(
            resolve_controller("0::/", mounts, "cpu").unwrap().directory,
            Path::new("/visible")
        );
        assert!(resolve_controller("0::/elsewhere/job", mounts, "cpu").is_none());
        assert!(resolve_controller("0::/../../escape", mounts, "cpu").is_none());
        let mounts = "31 20 0:28 / /cg/memory rw - cgroup cgroup rw,memory\n32 20 0:29 / /cg/cpu rw - cgroup cgroup rw,cpu,cpuacct";
        assert_eq!(
            resolve_controller("5:memory:/job\n4:cpu,cpuacct:/job", mounts, "memory")
                .unwrap()
                .directory,
            Path::new("/cg/memory/job")
        );
        assert!(
            !resolve_controller("4:cpu,cpuacct:/job", mounts, "cpuacct")
                .unwrap()
                .v2
        );
        let hybrid_mounts = format!("{mounts}\n33 20 0:30 / /cg/unified rw - cgroup2 cgroup rw");
        assert_eq!(
            resolve_controller("4:cpu,cpuacct:/job\n0::/other", &hybrid_mounts, "cpu")
                .unwrap()
                .directory,
            Path::new("/cg/cpu/job")
        );
        assert!(resolve_controller("bad", "bad", "cpu").is_none());
    }

    #[test]
    fn test_inherited_limits_and_parsing() {
        let path = ControllerPath {
            directory: PathBuf::from("/cg/a/b"),
            mount: PathBuf::from("/cg"),
            v2: true,
        };
        let limit = path.minimum_limit(|p| {
            // Assert that no inaccessible ancestor escapes the mount boundary.
            assert!(p.starts_with("/cg"));
            if p == Path::new("/cg/a") {
                Some(100)
            } else if p == Path::new("/cg") {
                Some(200)
            } else {
                None
            }
        });
        assert_eq!(limit, Some(100));
        assert_eq!(
            cpu_usage_usec("user_usec 99\nusage_usec 1234\nsystem_usec 5"),
            Some(1234)
        );
        assert_eq!(cpu_usage_usec("usage_usec broken"), None);
        assert_eq!(cpu_limit(-1, 1000), None);
        assert_eq!(cpu_limit(1000, 0), None);
        assert_eq!(cpu_limit(i64::MAX, 1), None);
    }
}
