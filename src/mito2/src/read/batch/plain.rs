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

//! Plain Batch.

use datatypes::arrow::array::{ArrayRef, BooleanArray};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::compute::filter_record_batch;
use snafu::ResultExt;

use crate::error::{ComputeArrowSnafu, Result};

/// [PlainBatch] represents a batch of rows.
/// It is a wrapper around [RecordBatch] that provides additional functionality for multi-series data.
/// The columns order is the same as the order of the columns read the SST.
/// It also contains the internal columns.
#[derive(Debug)]
pub struct PlainBatch {
    /// The original record batch.
    record_batch: RecordBatch,
}

impl PlainBatch {
    /// Creates a new [MultiSeries] from a [RecordBatch].
    pub fn new(record_batch: RecordBatch) -> Self {
        Self { record_batch }
    }

    /// Returns the number of rows in the batch.
    pub fn num_rows(&self) -> usize {
        self.record_batch.num_rows()
    }

    /// Returns true if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Returns the array of column at index `idx`.
    pub fn column(&self, idx: usize) -> &ArrayRef {
        self.record_batch.column(idx)
    }

    /// Filters this batch by the boolean array.
    pub(crate) fn filter(&self, predicate: &BooleanArray) -> Result<Self> {
        let record_batch =
            filter_record_batch(&self.record_batch, predicate).context(ComputeArrowSnafu)?;
        Ok(Self::new(record_batch))
    }
}
