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

//! Series module for grouping BulkParts by primary key.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use datatypes::arrow::array::{ArrayRef, BinaryArray, UInt32Array};
use datatypes::arrow::datatypes::SchemaRef;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::SequenceRange;

use crate::error::{ComputeArrowSnafu, Result};
use crate::memtable::bulk::context::BulkIterContext;
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::{BoxedRecordBatchIterator, MemScanMetrics, MemScanMetricsData};
use crate::metrics::{READ_ROWS_TOTAL, READ_STAGE_ELAPSED};
use crate::sst::parquet::format::PrimaryKeyArray;

/// Reference to a single row in a SeriesPart.
#[derive(Debug, Clone)]
struct PartRef {
    /// Index into SeriesSet.parts
    part_index: usize,
    /// Row index in the part
    row_index: usize,
    /// Timestamp for this row (for sorting)
    timestamp: i64,
    /// Sequence number for this row (for sorting)
    sequence: u64,
}

/// A collection of part references for a single series (primary key).
/// Similar to Series in time_series.rs but for bulk data.
struct Series {
    /// References to slices in the shared parts storage.
    /// Stored in insertion order, sorted during iteration by (timestamp ASC, sequence DESC).
    part_refs: Vec<PartRef>,
    /// Schema for reconstructing RecordBatches (shared across all part_refs).
    schema: SchemaRef,
}

/// Shared column storage for all series.
/// Data from one BulkPart, with primary key column removed.
struct SeriesPart {
    /// All columns EXCEPT __primary_key (includes data cols + __sequence + __op_type)
    /// in their original order.
    columns: Vec<ArrayRef>,
}

/// Inner data for SeriesSet protected by a single RwLock.
struct SeriesSetInner {
    /// Shared storage for all column data (excluding primary keys).
    parts: Vec<SeriesPart>,
    /// Map from encoded primary key to per-series data.
    series: BTreeMap<Vec<u8>, Series>,
}

/// A collection of BulkParts grouped by primary key (series).
#[derive(Clone)]
pub(crate) struct SeriesSet {
    /// Inner data protected by a single RwLock.
    inner: Arc<RwLock<SeriesSetInner>>,
    /// Region metadata.
    _region_metadata: RegionMetadataRef,
    /// Total number of unique series.
    series_count: Arc<AtomicUsize>,
}

impl SeriesSet {
    /// Creates a new SeriesSet for the given region.
    pub(crate) fn new(region_metadata: RegionMetadataRef) -> Self {
        Self {
            inner: Arc::new(RwLock::new(SeriesSetInner {
                parts: Vec::new(),
                series: BTreeMap::new(),
            })),
            _region_metadata: region_metadata,
            series_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Pushes a BulkPart to the appropriate series.
    /// Returns the estimated memory allocated.
    ///
    /// Supports both single-series and multi-series BulkParts.
    /// Multi-series parts are split by primary key and stored separately.
    pub(crate) fn push(&self, part: BulkPart) -> Result<usize> {
        use datatypes::arrow::array::Array;

        use crate::memtable::bulk::part::record_batch_estimated_size;
        use crate::sst::parquet::flat_format::primary_key_column_index;

        let batch = &part.batch;
        let num_columns = batch.num_columns();

        // Extract PrimaryKeyArray
        let pk_column_idx = primary_key_column_index(num_columns);
        let pk_column = batch.column(pk_column_idx);
        let pk_dict_array = pk_column
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .expect("Primary key column must be a PrimaryKeyArray");

        // Compute series boundaries
        let offsets = primary_key_offsets(pk_dict_array)?;
        if offsets.len() <= 1 {
            // Empty batch
            return Ok(0);
        }

        // Get the binary values array from the dictionary
        let values_array = pk_dict_array.values();
        let binary_array = values_array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Primary key values must be binary");

        // Build columns list excluding __primary_key
        let mut columns = Vec::with_capacity(num_columns - 1);
        for (i, col) in batch.columns().iter().enumerate() {
            if i != pk_column_idx {
                columns.push(col.clone());
            }
        }

        // Calculate memory size
        let allocated_size = record_batch_estimated_size(batch);

        // Get schema for this batch
        let schema = batch.schema();

        // Lock the inner data and add the SeriesPart and PartRefs
        let mut inner = self.inner.write().unwrap();

        // Add SeriesPart to shared storage
        let part_index = inner.parts.len();
        inner.parts.push(SeriesPart { columns });

        // Get timestamp and sequence columns
        let timestamp_col_idx = num_columns - 4;
        let sequence_col_idx = num_columns - 2;
        let timestamp_column = batch.column(timestamp_col_idx);
        let sequence_column = batch.column(sequence_col_idx);

        let timestamp_array = timestamp_column
            .as_any()
            .downcast_ref::<datatypes::arrow::array::Int64Array>()
            .expect("Timestamp column must be Int64Array");
        let sequence_array = sequence_column
            .as_any()
            .downcast_ref::<datatypes::arrow::array::UInt64Array>()
            .expect("Sequence column must be UInt64Array");

        // For each unique primary key segment
        for i in 0..offsets.len() - 1 {
            let row_start = offsets[i];
            let row_end = offsets[i + 1];

            // Get the key index for this segment
            let key_index = pk_dict_array.keys().value(row_start) as usize;
            let encoded_key = binary_array.value(key_index).to_vec();

            // Lookup or create Series
            let is_new_series = !inner.series.contains_key(&encoded_key);

            let series = inner.series.entry(encoded_key).or_insert_with(|| Series {
                part_refs: Vec::new(),
                schema: schema.clone(),
            });

            // Create one PartRef per row in this segment
            for row_idx in row_start..row_end {
                let timestamp = timestamp_array.value(row_idx);
                let sequence = sequence_array.value(row_idx);

                let part_ref = PartRef {
                    part_index,
                    row_index: row_idx,
                    timestamp,
                    sequence,
                };

                series.part_refs.push(part_ref);
            }

            // Update series count if new
            if is_new_series {
                self.series_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(allocated_size)
    }

    /// Returns the current number of unique series.
    pub(crate) fn series_count(&self) -> usize {
        self.series_count.load(Ordering::Relaxed)
    }

    /// Checks if the SeriesSet is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.read().unwrap().series.is_empty()
    }

    /// Returns the total number of rows across all series.
    pub(crate) fn total_rows(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner
            .series
            .values()
            .map(|series| series.part_refs.len())
            .sum()
    }

    /// Creates an iterator over all series data.
    pub(crate) fn iter(
        &self,
        context: Arc<BulkIterContext>,
        sequence: Option<SequenceRange>,
        mem_scan_metrics: Option<MemScanMetrics>,
    ) -> Result<Option<BoxedRecordBatchIterator>> {
        if self.is_empty() {
            return Ok(None);
        }

        Ok(Some(Box::new(SeriesSetIter {
            inner: self.inner.clone(),
            context,
            sequence,
            last_key: None,
            metrics: MemScanMetricsData {
                total_series: self.series_count(),
                ..Default::default()
            },
            mem_scan_metrics,
        })))
    }
}

/// Iterator over SeriesSet that yields RecordBatches.
/// Steps through the series map one series at a time.
struct SeriesSetIter {
    inner: Arc<RwLock<SeriesSetInner>>,
    context: Arc<BulkIterContext>,
    sequence: Option<SequenceRange>,
    last_key: Option<Vec<u8>>,
    /// Metrics for this iterator.
    metrics: MemScanMetricsData,
    /// Optional memory scan metrics to report to.
    mem_scan_metrics: Option<MemScanMetrics>,
}

impl SeriesSetIter {
    fn report_mem_scan_metrics(&mut self) {
        if let Some(mem_scan_metrics) = self.mem_scan_metrics.take() {
            mem_scan_metrics.merge_inner(&self.metrics);
        }
    }
}

impl Iterator for SeriesSetIter {
    type Item = Result<datatypes::arrow::record_batch::RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        use std::collections::Bound;
        use std::time::Instant;

        use datatypes::arrow::record_batch::RecordBatch;

        use crate::memtable::bulk::part_reader::apply_combined_filters;
        use crate::sst::parquet::file_range::PreFilterMode;
        use crate::sst::parquet::flat_format::primary_key_column_index;

        loop {
            let start = Instant::now();

            // Get next series from the map
            let (primary_key, part_refs, schema) = {
                let inner = self.inner.read().unwrap();
                let range = match &self.last_key {
                    None => inner.series.range::<Vec<u8>, _>(..),
                    Some(last_key) => inner
                        .series
                        .range::<Vec<u8>, _>((Bound::Excluded(last_key), Bound::Unbounded)),
                };

                let mut next_series = None;
                for (pk, series) in range {
                    self.last_key = Some(pk.clone());

                    // Clone part_refs and sort by (timestamp ASC, sequence DESC)
                    let mut sorted_refs = series.part_refs.clone();
                    sorted_refs.sort_by(|a, b| {
                        a.timestamp
                            .cmp(&b.timestamp)
                            .then_with(|| b.sequence.cmp(&a.sequence)) // DESC
                    });

                    next_series = Some((pk.clone(), sorted_refs, series.schema.clone()));
                    break;
                }

                drop(inner);

                match next_series {
                    Some(series) => series,
                    None => {
                        // All series exhausted
                        self.metrics.scan_cost += start.elapsed();
                        self.report_mem_scan_metrics();
                        return None;
                    }
                }
            };

            // Now process the series data without holding the lock
            let inner = self.inner.read().unwrap();

            // Collect rows from all PartRefs
            let mut row_columns: Vec<Vec<ArrayRef>> = Vec::new();

            for part_ref in &part_refs {
                let series_part = &inner.parts[part_ref.part_index];

                // Get single row from each column
                let row: Vec<ArrayRef> = series_part
                    .columns
                    .iter()
                    .map(|col| col.slice(part_ref.row_index, 1))
                    .collect();

                row_columns.push(row);
            }

            drop(inner);

            if row_columns.is_empty() {
                // Empty series, try next
                self.metrics.scan_cost += start.elapsed();
                continue;
            }

            // Concatenate all rows into columns
            let num_columns = row_columns[0].len();
            let num_rows = row_columns.len();

            let mut concatenated_columns = Vec::with_capacity(num_columns);
            for col_idx in 0..num_columns {
                let arrays_to_concat: Vec<&dyn datatypes::arrow::array::Array> = row_columns
                    .iter()
                    .map(|row| row[col_idx].as_ref())
                    .collect();

                let concatenated = match datatypes::arrow::compute::concat(&arrays_to_concat)
                    .context(ComputeArrowSnafu)
                {
                    Ok(col) => col,
                    Err(e) => {
                        self.metrics.scan_cost += start.elapsed();
                        return Some(Err(e));
                    }
                };
                concatenated_columns.push(concatenated);
            }

            // Rebuild primary key column for all rows
            let pk_column = rebuild_primary_key_column(&primary_key, num_rows);

            // Insert primary key at correct position
            let pk_column_idx = primary_key_column_index(concatenated_columns.len() + 1);
            let mut full_columns = Vec::with_capacity(concatenated_columns.len() + 1);

            for (i, col) in concatenated_columns.iter().enumerate() {
                if i == pk_column_idx {
                    full_columns.push(pk_column.clone());
                }
                full_columns.push(col.clone());
            }

            if pk_column_idx == concatenated_columns.len() {
                full_columns.push(pk_column.clone());
            }

            // Create RecordBatch
            let batch = match RecordBatch::try_new(schema, full_columns).context(ComputeArrowSnafu)
            {
                Ok(batch) => batch,
                Err(e) => {
                    self.metrics.scan_cost += start.elapsed();
                    return Some(Err(e));
                }
            };

            // Apply filtering (predicate and sequence filters)
            // Determine skip_fields based on pre_filter_mode
            let skip_fields = match self.context.pre_filter_mode() {
                PreFilterMode::All => false,
                PreFilterMode::SkipFields => true,
                PreFilterMode::SkipFieldsOnDelete => true,
            };

            let filtered_batch =
                match apply_combined_filters(&self.context, &self.sequence, batch, skip_fields) {
                    Ok(Some(batch)) => batch,
                    Ok(None) => {
                        // Batch filtered out completely, try next series
                        self.metrics.scan_cost += start.elapsed();
                        continue;
                    }
                    Err(e) => {
                        self.metrics.scan_cost += start.elapsed();
                        return Some(Err(e));
                    }
                };

            // Update metrics
            self.metrics.num_batches += 1;
            self.metrics.num_rows += filtered_batch.num_rows();
            self.metrics.scan_cost += start.elapsed();

            return Some(Ok(filtered_batch));
        }
    }
}

impl Drop for SeriesSetIter {
    fn drop(&mut self) {
        common_telemetry::debug!(
            "SeriesSetIter region: {}, metrics: total_series={}, num_rows={}, num_batches={}, scan_cost={:?}",
            self.context.region_id(),
            self.metrics.total_series,
            self.metrics.num_rows,
            self.metrics.num_batches,
            self.metrics.scan_cost
        );

        // Report MemScanMetrics if not already reported
        self.report_mem_scan_metrics();

        READ_ROWS_TOTAL
            .with_label_values(&["bulk_memtable"])
            .inc_by(self.metrics.num_rows as u64);
        READ_STAGE_ELAPSED
            .with_label_values(&["scan_memtable"])
            .observe(self.metrics.scan_cost.as_secs_f64());
    }
}

/// Computes offsets where the primary key changes in a PrimaryKeyArray.
/// Returns indices including 0 at start and keys.len() at end.
fn primary_key_offsets(pk_dict_array: &PrimaryKeyArray) -> Result<Vec<usize>> {
    if pk_dict_array.is_empty() {
        return Ok(Vec::new());
    }

    let mut offsets = vec![0];
    let keys = pk_dict_array.keys();
    let pk_indices = keys.values();

    for (i, key) in pk_indices.iter().take(keys.len() - 1).enumerate() {
        if *key != pk_indices[i + 1] {
            offsets.push(i + 1);
        }
    }
    offsets.push(keys.len());

    Ok(offsets)
}

/// Rebuilds a primary key column from an encoded key.
/// Creates a PrimaryKeyArray where all rows point to the same encoded key.
fn rebuild_primary_key_column(encoded_key: &[u8], num_rows: usize) -> ArrayRef {
    let values = Arc::new(BinaryArray::from_iter_values([encoded_key]));
    let keys = UInt32Array::from_value(0, num_rows);
    Arc::new(PrimaryKeyArray::new(keys, values))
}
