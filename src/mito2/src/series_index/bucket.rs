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

//! Event-time bucket planning and source coverage.

use crate::sst::file::FileHandle;
use common_time::Timestamp;
use smallvec::{SmallVec, smallvec};
use std::time::Duration;

pub(crate) struct SeriesBucket {
    pub start: Timestamp,
    pub end: Timestamp,
    pub files: SmallVec<[FileHandle; 2]>,
}

pub(crate) fn rounded_bucket_width(
    requested: Duration,
    compaction_window: Duration,
) -> Option<i64> {
    let window_secs = i64::try_from(compaction_window.as_secs()).ok()?.max(1);
    let requested_secs = i64::try_from(requested.as_secs())
        .unwrap_or(i64::MAX)
        .max(1);
    let multiples = requested_secs / window_secs + i64::from(requested_secs % window_secs != 0);
    multiples.checked_mul(window_secs)
}

/// Groups SSTs across levels into sorted, disjoint time buckets without planning builds.
///
/// Both widths must be positive, and `width_secs` must be a multiple of the compaction
/// window width. Inclusive SST ranges are rounded outward to aligned,
/// half-open intervals in seconds. Overlapping intervals merge; adjacent ones stay
/// separate. Each merged bucket tracks its maximum sequence and any unknown sequence.
pub(crate) fn group_files_into_series_buckets(
    files: &[FileHandle],
    width_secs: i64,
) -> Vec<SeriesBucket> {
    let mut spans = files
        .iter()
        .map(|file| {
            let start = file.time_range().0.split().0;
            let end = file.time_range().1.split().0;
            SeriesBucket {
                start: Timestamp::new_second(
                    start.div_euclid(width_secs).saturating_mul(width_secs),
                ),
                end: Timestamp::new_second(
                    end.div_euclid(width_secs)
                        .saturating_add(1)
                        .saturating_mul(width_secs),
                ),
                files: smallvec![file.clone()],
            }
        })
        .collect::<Vec<_>>();
    spans.sort_unstable_by(|a, b| a.start.cmp(&b.start).then_with(|| b.end.cmp(&a.end)));
    group_series_buckets(spans)
}

/// Merges overlapping spans sorted by nondecreasing start; equal starts are allowed.
/// Returns sorted, disjoint buckets. Adjacent half-open intervals remain separate.
/// All spans must use the same compaction-window width for their SST summaries.
fn group_series_buckets(spans: Vec<SeriesBucket>) -> Vec<SeriesBucket> {
    let mut buckets: Vec<SeriesBucket> = Vec::new();
    for mut span in spans {
        if let Some(last) = buckets.last_mut()
            && span.start < last.end
        {
            last.end = last.end.max(span.end);
            last.files.append(&mut span.files);
        } else {
            buckets.push(span);
        }
    }
    buckets
}
