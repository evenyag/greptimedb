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

//! Statistics of parquet SSTs.

use std::borrow::Borrow;
use std::collections::HashSet;

use datafusion::physical_optimizer::pruning::PruningStatistics;
use datafusion_common::Column;
use datatypes::arrow::array::ArrayRef;
use parquet::file::metadata::RowGroupMetaData;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::sst::parquet::format::{AppendReadFormat, ReadFormat};

/// Statistics for pruning row groups.
pub(crate) struct RowGroupPruningStats<'a, T> {
    /// Metadata of SST row groups.
    row_groups: &'a [T],
    /// Helper to read the SST.
    read_format: &'a ReadFormat,
    /// Projected column ids to read.
    ///
    /// We need column ids to distinguish different columns with the same name.
    /// e.g. Drops and then adds a column again.
    column_ids: HashSet<ColumnId>,
}

impl<'a, T> RowGroupPruningStats<'a, T> {
    /// Creates a new statistics to prune specific `row_groups`.
    pub(crate) fn new(
        row_groups: &'a [T],
        read_format: &'a ReadFormat,
        column_ids: HashSet<ColumnId>,
    ) -> Self {
        Self {
            row_groups,
            read_format,
            column_ids,
        }
    }

    /// Returns the column id of specific column name if we need to read it.
    fn column_id_to_prune(&self, name: &str) -> Option<ColumnId> {
        // Only use stats when the column to read has the same id as the column in the SST.
        self.read_format
            .metadata()
            .column_by_name(name)
            .and_then(|col| self.column_ids.get(&col.column_id).copied())
    }
}

impl<'a, T: Borrow<RowGroupMetaData>> PruningStatistics for RowGroupPruningStats<'a, T> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        self.read_format.min_values(self.row_groups, column_id)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        self.read_format.max_values(self.row_groups, column_id)
    }

    fn num_containers(&self) -> usize {
        self.row_groups.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        self.read_format.null_counts(self.row_groups, column_id)
    }
}

/// Statistics for pruning row groups under append mode.
pub(crate) struct AppendModePruningStats<'a, T> {
    /// Metadata of SST row groups.
    row_groups: &'a [T],
    /// Helper to read the SST.
    read_format: &'a AppendReadFormat,
    /// Latest region metadata, we use it to find the column id of a column.
    ///
    /// Use the SST's metadata if it is None.
    latest_metadata: Option<RegionMetadataRef>,
}

impl<'a, T> AppendModePruningStats<'a, T> {
    /// Creates a new statistics to prune specific `row_groups`.
    pub(crate) fn new(
        row_groups: &'a [T],
        read_format: &'a AppendReadFormat,
        latest_metadata: Option<RegionMetadataRef>,
    ) -> Self {
        Self {
            row_groups,
            read_format,
            latest_metadata,
        }
    }

    /// Returns the column id of specific column name in latest metadata.
    ///
    /// Use the column id in the latest schema instead of the SST so we support dropping
    /// and adding a column with the same name.
    fn column_id_to_prune(&self, name: &str) -> Option<ColumnId> {
        let latest_metadata = self
            .latest_metadata
            .as_ref()
            .unwrap_or_else(|| self.read_format.sst_metadata());
        latest_metadata
            .column_by_name(name)
            .map(|col| col.column_id)
    }
}

impl<'a, T: Borrow<RowGroupMetaData>> PruningStatistics for AppendModePruningStats<'a, T> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        self.read_format.min_values(self.row_groups, column_id)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        self.read_format.max_values(self.row_groups, column_id)
    }

    fn num_containers(&self) -> usize {
        self.row_groups.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        self.read_format.null_counts(self.row_groups, column_id)
    }
}
