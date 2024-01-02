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

use lazy_static::lazy_static;
use prometheus::*;

/// Stage label.
pub const STAGE_LABEL: &str = "stage";
/// Type label.
pub const TYPE_LABEL: &str = "type";
/// Reason to flush.
pub const FLUSH_REASON: &str = "reason";

lazy_static! {
    /// Global write buffer size in bytes.
    pub static ref WRITE_BUFFER_BYTES: IntGauge =
        register_int_gauge!("mito_write_buffer_bytes", "mito write buffer bytes").unwrap();
    /// Gauge for open regions
    pub static ref REGION_COUNT: IntGauge =
        register_int_gauge!("mito_region_count", "mito region count").unwrap();
    /// Elapsed time to handle requests.
    pub static ref HANDLE_REQUEST_ELAPSED: HistogramVec = register_histogram_vec!(
            "mito_handle_request_elapsed",
            "mito handle request elapsed",
            &[TYPE_LABEL]
        )
        .unwrap();

    // ------ Flush related metrics
    /// Counter of scheduled flush requests.
    /// Note that the flush scheduler may merge some flush requests.
    pub static ref FLUSH_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "mito_flush_requests_total",
            "mito flush requests total",
            &[FLUSH_REASON]
        )
        .unwrap();
    /// Counter of scheduled failed flush jobs.
    pub static ref FLUSH_ERRORS_TOTAL: IntCounter =
        register_int_counter!("mito_flush_errors_total", "mito flush errors total").unwrap();
    /// Elapsed time of a flush job.
    pub static ref FLUSH_ELAPSED: HistogramVec = register_histogram_vec!(
            "mito_flush_elapsed",
            "mito flush elapsed",
            &[TYPE_LABEL]
        )
        .unwrap();
    /// Histogram of flushed bytes.
    pub static ref FLUSH_BYTES_TOTAL: IntCounter =
        register_int_counter!("mito_flush_bytes_total", "mito flush bytes total").unwrap();
    // ------ End of flush related metrics


    // ------ Write related metrics
    /// Counter of stalled write requests.
    pub static ref WRITE_STALL_TOTAL: IntCounter =
        register_int_counter!("mito_write_stall_total", "mito write stall total").unwrap();
    /// Counter of rejected write requests.
    pub static ref WRITE_REJECT_TOTAL: IntCounter =
        register_int_counter!("mito_write_reject_total", "mito write reject total").unwrap();
    /// Elapsed time of each write stage.
    pub static ref WRITE_STAGE_ELAPSED: HistogramVec = register_histogram_vec!(
            "mito_write_stage_elapsed",
            "mito write stage elapsed",
            &[STAGE_LABEL]
        )
        .unwrap();
    /// Counter of rows to write.
    pub static ref WRITE_ROWS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "mito_write_rows_total",
        "mito write rows total",
        &[TYPE_LABEL]
    )
    .unwrap();
    // ------ End of write related metrics


    // Compaction metrics
    /// Timer of different stages in compaction.
    pub static ref COMPACTION_STAGE_ELAPSED: HistogramVec = register_histogram_vec!(
        "mito_compaction_stage_elapsed",
        "mito compaction stage elapsed",
        &[STAGE_LABEL]
    )
    .unwrap();
    /// Timer of whole compaction task.
    pub static ref COMPACTION_ELAPSED_TOTAL: Histogram =
        register_histogram!("mito_compaction_total_elapsed", "mito compaction total elapsed").unwrap();
    /// Counter of all requested compaction task.
    pub static ref COMPACTION_REQUEST_COUNT: IntCounter =
        register_int_counter!("mito_compaction_requests_total", "mito compaction requests total").unwrap();
    /// Counter of failed compaction task.
    pub static ref COMPACTION_FAILURE_COUNT: IntCounter =
        register_int_counter!("mito_compaction_failure_total", "mito compaction failure total").unwrap();
    // ------- End of compaction metrics.

    // Query metrics.
    /// Timer of different stages in query.
    pub static ref READ_STAGE_ELAPSED: HistogramVec = register_histogram_vec!(
        "mito_read_stage_elapsed",
        "mito read stage elapsed",
        &[STAGE_LABEL]
    )
    .unwrap();
    /// Counter of rows read.
    pub static ref READ_ROWS_TOTAL: IntCounterVec =
        register_int_counter_vec!("mito_read_rows_total", "mito read rows total", &[TYPE_LABEL]).unwrap();
    /// Counter of filtered rows during merge.
    pub static ref MERGE_FILTER_ROWS_TOTAL: IntCounterVec =
        register_int_counter_vec!("mito_merge_filter_rows_total", "mito merge filter rows total", &[TYPE_LABEL]).unwrap();
    // ------- End of query metrics.

    // Cache related metrics.
    /// Cache hit counter.
    pub static ref CACHE_HIT: IntCounterVec = register_int_counter_vec!(
        "mito_cache_hit",
        "mito cache hit",
        &[TYPE_LABEL]
    )
    .unwrap();
    /// Cache miss counter.
    pub static ref CACHE_MISS: IntCounterVec = register_int_counter_vec!(
        "mito_cache_miss",
        "mito cache miss",
        &[TYPE_LABEL]
    )
    .unwrap();
    /// Cache size in bytes.
    pub static ref CACHE_BYTES: IntGaugeVec = register_int_gauge_vec!(
        "mito_cache_bytes",
        "mito cache bytes",
        &[TYPE_LABEL]
    )
    .unwrap();
    /// Timer of upload.
    pub static ref UPLOAD_ELAPSED_TOTAL: Histogram =
        register_histogram!("mito_upload_total_elapsed", "mito upload total elapsed").unwrap();
}
