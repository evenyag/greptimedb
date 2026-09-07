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

use std::time::Duration;

use common_time::Timestamp;
use store_api::storage::FileId;

use super::catalog::SeriesIndexEntry;
use crate::sst::file::FileHandle;

#[derive(Debug, Clone)]
pub(super) struct SeriesBucket {
    pub(super) start: Timestamp,
    pub(super) end: Timestamp,
    pub(super) files: Vec<FileHandle>,
    pub(super) has_unknown_sequence: bool,
}

pub(super) fn rounded_bucket_width(
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

pub(super) fn plan_series_buckets(files: &[FileHandle], width_secs: i64) -> Vec<SeriesBucket> {
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
                files: vec![file.clone()],
                has_unknown_sequence: file.meta_ref().sequence.is_none(),
            }
        })
        .collect::<Vec<_>>();
    spans.sort_by_key(|span| (span.start, span.end));
    let mut buckets: Vec<SeriesBucket> = Vec::new();
    for mut span in spans {
        if let Some(last) = buckets.last_mut()
            && span.start < last.end
        {
            last.end = last.end.max(span.end);
            last.files.append(&mut span.files);
            last.has_unknown_sequence |= span.has_unknown_sequence;
        } else {
            buckets.push(span);
        }
    }
    buckets
}

pub(super) fn series_entry(bucket: &SeriesBucket) -> Option<SeriesIndexEntry> {
    if bucket.has_unknown_sequence || bucket.files.len() < 2 {
        return None;
    }
    let mut source_file_ids = bucket
        .files
        .iter()
        .map(|file| file.file_id().file_id())
        .collect::<Vec<_>>();
    source_file_ids.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    let mut sequences = bucket
        .files
        .iter()
        .filter_map(|file| file.meta_ref().sequence.map(|sequence| sequence.get()));
    let first = sequences.next()?;
    let (mut min_file_sequence, mut max_file_sequence) = (first, first);
    for sequence in sequences {
        min_file_sequence = min_file_sequence.min(sequence);
        max_file_sequence = max_file_sequence.max(sequence);
    }
    Some(SeriesIndexEntry {
        index_uuid: FileId::random(),
        bucket_start: bucket.start,
        bucket_end: bucket.end,
        source_file_ids,
        min_file_sequence,
        max_file_sequence,
    })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use common_time::timestamp::TimeUnit;

    use super::*;
    use crate::sst::file::FileMeta;
    use crate::test_util::new_noop_file_purger;

    fn file(sequence: Option<u64>, level: u8, start: Timestamp, end: Timestamp) -> FileHandle {
        FileHandle::new(
            FileMeta {
                file_id: FileId::random(),
                sequence: sequence.and_then(NonZeroU64::new),
                level,
                time_range: (start, end),
                ..Default::default()
            },
            new_noop_file_purger(),
        )
    }

    #[test]
    fn test_second_resolution_buckets_merge_spans_across_levels() {
        let width = rounded_bucket_width(Duration::from_secs(11), Duration::from_secs(10)).unwrap();
        let files = [
            file(
                Some(1),
                0,
                Timestamp::new_millisecond(-1),
                Timestamp::new_millisecond(19999),
            ),
            file(
                Some(2),
                1,
                Timestamp::new_microsecond(19000000),
                Timestamp::new_microsecond(39000000),
            ),
            file(
                Some(3),
                2,
                Timestamp::new_nanosecond(39000000000),
                Timestamp::new_nanosecond(40000000000),
            ),
            file(
                Some(4),
                1,
                Timestamp::new_second(60),
                Timestamp::new_second(61),
            ),
        ];
        let buckets = plan_series_buckets(&files, width);
        let spans = buckets
            .iter()
            .map(|b| (b.start, b.end, b.files.len()))
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                (Timestamp::new_second(-20), Timestamp::new_second(60), 3),
                (Timestamp::new_second(60), Timestamp::new_second(80), 1),
            ],
            spans
        );
        let entry = series_entry(&buckets[0]).unwrap();
        assert_eq!((1, 3), (entry.min_file_sequence, entry.max_file_sequence));
        assert!(series_entry(&buckets[1]).is_none());
        let mut files = files.to_vec();
        files.push(file(
            None,
            0,
            Timestamp::new_second(0),
            Timestamp::new_second(1),
        ));
        assert!(series_entry(&plan_series_buckets(&files, width)[0]).is_none());
    }

    #[test]
    fn test_seconds_do_not_require_millisecond_conversion() {
        let start = Timestamp::new_second(i64::MAX / 1000 + 100);
        assert!(start.convert_to(TimeUnit::Millisecond).is_none());
        let buckets = plan_series_buckets(&[file(Some(1), 0, start, start)], 1);
        assert_eq!(start, buckets[0].start);
        assert_eq!(Timestamp::new_second(start.value() + 1), buckets[0].end);

        let width = rounded_bucket_width(Duration::ZERO, Duration::from_millis(100)).unwrap();
        let buckets = plan_series_buckets(
            &[file(
                Some(1),
                0,
                Timestamp::new_nanosecond(-1),
                Timestamp::new_microsecond(1),
            )],
            width,
        );
        assert_eq!(
            (Timestamp::new_second(-1), Timestamp::new_second(1)),
            (buckets[0].start, buckets[0].end)
        );
    }
}
