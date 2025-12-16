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

use store_api::metadata::RegionMetadataRef;
use store_api::storage::SequenceRange;

use crate::error::Result;
use crate::memtable::bulk::context::BulkIterContext;
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::BoxedRecordBatchIterator;

/// A collection of BulkParts grouped by primary key (series).
#[derive(Clone)]
pub(crate) struct SeriesSet {
    /// Map from encoded primary key to list of BulkParts for that series.
    series: Arc<RwLock<BTreeMap<Vec<u8>, Vec<BulkPart>>>>,
    /// Region metadata.
    _region_metadata: RegionMetadataRef,
    /// Total number of unique series.
    series_count: Arc<AtomicUsize>,
}

impl SeriesSet {
    /// Creates a new SeriesSet for the given region.
    pub(crate) fn new(region_metadata: RegionMetadataRef) -> Self {
        Self {
            series: Arc::new(RwLock::new(BTreeMap::new())),
            _region_metadata: region_metadata,
            series_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Pushes a BulkPart to the appropriate series.
    /// Returns the estimated memory allocated.
    ///
    /// For now, only single-series parts are supported. Multi-series parts
    /// should be routed to regular parts storage.
    pub(crate) fn push(&self, part: BulkPart) -> Result<usize> {
        use crate::error::UnsupportedOperationSnafu;
        use crate::memtable::bulk::part::record_batch_estimated_size;
        use crate::sst::parquet::flat_format::primary_key_column_index;
        use crate::sst::parquet::format::PrimaryKeyArray;
        use datatypes::arrow::array::Array;

        // Check if this is a single-series part
        let series_count = part.primary_key_dict_values_count();
        if series_count != 1 {
            return UnsupportedOperationSnafu {
                err_msg: format!(
                    "SeriesSet only supports single-series parts, got {} series",
                    series_count
                ),
            }
            .fail();
        }

        // Extract the primary key from the first row
        let pk_column_idx = primary_key_column_index(part.batch.num_columns());
        let pk_column = part.batch.column(pk_column_idx);

        let dict_array = pk_column
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .unwrap();

        // Get the first key value (all rows have the same key since series_count == 1)
        let values_array = dict_array.values();
        let binary_array = values_array
            .as_any()
            .downcast_ref::<datatypes::arrow::array::BinaryArray>()
            .unwrap();
        let key_value = binary_array.value(0);
        let primary_key = key_value.to_vec();

        // Calculate size
        let allocated_size = record_batch_estimated_size(&part.batch);

        // Store the part in the series map
        let mut series = self.series.write().unwrap();
        let is_new_series = !series.contains_key(&primary_key);

        series
            .entry(primary_key)
            .or_default()
            .push(part);

        // Update series count if this is a new series
        if is_new_series {
            self.series_count.fetch_add(1, Ordering::Relaxed);
        }

        Ok(allocated_size)
    }

    /// Returns the current number of unique series.
    pub(crate) fn series_count(&self) -> usize {
        self.series_count.load(Ordering::Relaxed)
    }

    /// Checks if the SeriesSet is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.series.read().unwrap().is_empty()
    }

    /// Returns the total number of rows across all series.
    pub(crate) fn total_rows(&self) -> usize {
        let series = self.series.read().unwrap();
        series
            .values()
            .flat_map(|parts| parts.iter())
            .map(|part| part.num_rows())
            .sum()
    }

    /// Creates an iterator over all series data.
    pub(crate) fn iter(
        &self,
        context: Arc<BulkIterContext>,
        sequence: Option<SequenceRange>,
    ) -> Result<Option<BoxedRecordBatchIterator>> {
        use crate::memtable::bulk::part_reader::BulkPartRecordBatchIter;

        let series = self.series.read().unwrap();

        if series.is_empty() {
            return Ok(None);
        }

        // Collect all parts from all series into a single iterator
        // For now, we create one iterator per series and chain them together
        let mut iterators: Vec<BoxedRecordBatchIterator> = Vec::new();

        for (_primary_key, parts) in series.iter() {
            for part in parts {
                let series_count = part.primary_key_dict_values_count();
                let iter = BulkPartRecordBatchIter::new(
                    part.batch.clone(),
                    context.clone(),
                    sequence,
                    series_count,
                    None, // No metrics for now
                );
                iterators.push(Box::new(iter));
            }
        }

        if iterators.is_empty() {
            Ok(None)
        } else {
            // Chain all iterators together
            Ok(Some(Box::new(iterators.into_iter().flatten())))
        }
    }
}
