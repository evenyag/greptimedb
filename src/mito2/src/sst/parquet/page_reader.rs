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

//! Parquet page reader.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use lazy_static::lazy_static;
use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::errors::Result;

lazy_static! {
    /// Global page reader metrics.
    static ref GLOBAL_PAGE_METRICS: GlobalPageMetrics = GlobalPageMetrics::default();
}

/// Global page reader metrics.
#[derive(Debug, Default)]
struct GlobalPageMetrics {
    /// Total number of pages read.
    pages_read: AtomicUsize,
    /// Total number of pages skipped.
    pages_skipped: AtomicUsize,
    /// Total bytes of pages read.
    total_bytes: AtomicU64,
    /// Total duration of page reads (in microseconds).
    total_read_duration_us: AtomicU64,
}

impl GlobalPageMetrics {
    /// Adds page reader metrics to the global metrics.
    fn add_metrics(&self, metrics: &PageReaderMetrics) {
        self.pages_read
            .fetch_add(metrics.num_get, Ordering::Relaxed);
        self.pages_skipped
            .fetch_add(metrics.num_skip, Ordering::Relaxed);
        self.total_bytes
            .fetch_add(metrics.total_bytes, Ordering::Relaxed);
        self.total_read_duration_us.fetch_add(
            metrics.total_read_duration.as_micros() as u64,
            Ordering::Relaxed,
        );
    }

    /// Gets the current metrics values.
    fn get_metrics(&self) -> (usize, usize, u64, Duration) {
        let pages_read = self.pages_read.load(Ordering::Relaxed);
        let pages_skipped = self.pages_skipped.load(Ordering::Relaxed);
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);
        let total_read_duration_us = self.total_read_duration_us.load(Ordering::Relaxed);
        (
            pages_read,
            pages_skipped,
            total_bytes,
            Duration::from_micros(total_read_duration_us),
        )
    }

    /// Resets all metrics to zero.
    fn reset(&self) {
        self.pages_read.store(0, Ordering::Relaxed);
        self.pages_skipped.store(0, Ordering::Relaxed);
        self.total_bytes.store(0, Ordering::Relaxed);
        self.total_read_duration_us.store(0, Ordering::Relaxed);
    }
}

/// A reader that reads all pages from a cache.
pub(crate) struct RowGroupCachedReader {
    /// Cached pages.
    pages: VecDeque<Page>,
}

impl RowGroupCachedReader {
    /// Returns a new reader from pages of a column in a row group.
    pub(crate) fn new(pages: &[Page]) -> Self {
        Self {
            pages: pages.iter().cloned().collect(),
        }
    }
}

impl PageReader for RowGroupCachedReader {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        Ok(self.pages.pop_front())
    }

    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        Ok(self.pages.front().map(page_to_page_meta))
    }

    fn skip_next_page(&mut self) -> Result<()> {
        // When the `SerializedPageReader` is in `SerializedPageReaderState::Pages` state, it never pops
        // the dictionary page. So it always return the dictionary page as the first page. See:
        // https://github.com/apache/arrow-rs/blob/1d6feeacebb8d0d659d493b783ba381940973745/parquet/src/file/serialized_reader.rs#L766-L770
        // But the `GenericColumnReader` will read the dictionary page before skipping records so it won't skip dictionary page.
        // So we don't need to handle the dictionary page specifically in this method.
        // https://github.com/apache/arrow-rs/blob/65f7be856099d389b0d0eafa9be47fad25215ee6/parquet/src/column/reader.rs#L322-L331
        self.pages.pop_front();
        Ok(())
    }
}

impl Iterator for RowGroupCachedReader {
    type Item = Result<Page>;
    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

/// Get the size of a page in bytes.
fn page_size(page: &Page) -> u64 {
    match page {
        Page::DataPage { buf, .. } => buf.len() as u64,
        Page::DataPageV2 { buf, .. } => buf.len() as u64,
        Page::DictionaryPage { buf, .. } => buf.len() as u64,
    }
}

/// Get [PageMetadata] from `page`.
///
/// The conversion is based on [decode_page()](https://github.com/apache/arrow-rs/blob/1d6feeacebb8d0d659d493b783ba381940973745/parquet/src/file/serialized_reader.rs#L438-L481)
/// and [PageMetadata](https://github.com/apache/arrow-rs/blob/65f7be856099d389b0d0eafa9be47fad25215ee6/parquet/src/column/page.rs#L279-L301).
fn page_to_page_meta(page: &Page) -> PageMetadata {
    match page {
        Page::DataPage { num_values, .. } => PageMetadata {
            num_rows: None,
            num_levels: Some(*num_values as usize),
            is_dict: false,
        },
        Page::DataPageV2 {
            num_values,
            num_rows,
            ..
        } => PageMetadata {
            num_rows: Some(*num_rows as usize),
            num_levels: Some(*num_values as usize),
            is_dict: false,
        },
        Page::DictionaryPage { .. } => PageMetadata {
            num_rows: None,
            num_levels: None,
            is_dict: true,
        },
    }
}

/// A reader that reads all pages from a cache.
pub(crate) struct MetricsPageReader<T> {
    row_group: usize,
    column_idx: usize,
    reader: T,
    num_get: usize,
    num_skip: usize,
    total_bytes: u64,
    total_read_duration: Duration,
}

impl<T> MetricsPageReader<T> {
    /// Returns a new reader from pages of a column in a row group.
    pub(crate) fn new(row_group: usize, column_idx: usize, reader: T) -> Self {
        Self {
            row_group,
            column_idx,
            reader,
            num_get: 0,
            num_skip: 0,
            total_bytes: 0,
            total_read_duration: Duration::ZERO,
        }
    }

    /// Returns the collected metrics.
    pub(crate) fn metrics(&self) -> PageReaderMetrics {
        PageReaderMetrics {
            row_group: self.row_group,
            column_idx: self.column_idx,
            num_get: self.num_get,
            num_skip: self.num_skip,
            total_bytes: self.total_bytes,
            total_read_duration: self.total_read_duration,
        }
    }
}

impl<T: PageReader> PageReader for MetricsPageReader<T> {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        let start = Instant::now();
        let result = self.reader.get_next_page();
        self.total_read_duration += start.elapsed();

        if let Ok(Some(ref page)) = result {
            self.num_get += 1;
            self.total_bytes += page_size(page);
        }

        result
    }

    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        self.reader.peek_next_page()
    }

    fn skip_next_page(&mut self) -> Result<()> {
        let start = Instant::now();
        let result = self.reader.skip_next_page();
        self.total_read_duration += start.elapsed();

        if result.is_ok() {
            self.num_skip += 1;
        }

        result
    }
}

impl<T: PageReader> Iterator for MetricsPageReader<T> {
    type Item = Result<Page>;
    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

/// Metrics collected by MetricsPageReader.
#[derive(Debug, Clone)]
pub(crate) struct PageReaderMetrics {
    pub row_group: usize,
    pub column_idx: usize,
    pub num_get: usize,
    pub num_skip: usize,
    pub total_bytes: u64,
    pub total_read_duration: Duration,
}

/// Gets the global page reader metrics and resets them.
pub fn get_and_reset_global_page_metrics() -> (usize, usize, u64, Duration) {
    let metrics = GLOBAL_PAGE_METRICS.get_metrics();
    GLOBAL_PAGE_METRICS.reset();
    metrics
}

impl<T> Drop for MetricsPageReader<T> {
    fn drop(&mut self) {
        if self.column_idx != 0 {
            return;
        }

        let metrics = self.metrics();
        GLOBAL_PAGE_METRICS.add_metrics(&metrics);
    }
}

// impl<T> Drop for MetricsPageReader<T> {
//     fn drop(&mut self) {
//         common_telemetry::info!(
//             "Page reader metrics: row group: {}, column: {}, num get: {}, num skip: {}",
//             self.row_group,
//             self.column_idx,
//             self.num_get,
//             self.num_skip
//         );
//     }
// }
