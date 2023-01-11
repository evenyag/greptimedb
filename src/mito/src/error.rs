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

use std::any::Any;

use common_error::ext::BoxedError;
use common_error::prelude::*;
use table::metadata::{TableInfoBuilderError, TableMetaBuilderError};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to create region, source: {}", source))]
    CreateRegion {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display(
        "Failed to build table meta for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildTableMeta {
        source: TableMetaBuilderError,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to build table info for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildTableInfo {
        source: TableInfoBuilderError,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid primary key: {}", msg))]
    InvalidPrimaryKey { msg: String, backtrace: Backtrace },

    #[snafu(display("Missing timestamp index for table: {}", table_name))]
    MissingTimestampIndex {
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to build row key descriptor for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildRowKeyDescriptor {
        source: store_api::storage::RowKeyDescriptorBuilderError,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to build column descriptor for table: {}, column: {}, source: {}",
        table_name,
        column_name,
        source,
    ))]
    BuildColumnDescriptor {
        source: store_api::storage::ColumnDescriptorBuilderError,
        table_name: String,
        column_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to build column family descriptor for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildColumnFamilyDescriptor {
        source: store_api::storage::ColumnFamilyDescriptorBuilderError,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to build region descriptor for table: {}, region: {}, source: {}",
        table_name,
        region_name,
        source,
    ))]
    BuildRegionDescriptor {
        source: store_api::storage::RegionDescriptorBuilderError,
        table_name: String,
        region_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to update table metadata to manifest,  table: {}, source: {}",
        table_name,
        source,
    ))]
    UpdateTableManifest {
        #[snafu(backtrace)]
        source: storage::error::Error,
        table_name: String,
    },

    #[snafu(display(
        "Failed to scan table metadata from manifest,  table: {}, source: {}",
        table_name,
        source,
    ))]
    ScanTableManifest {
        #[snafu(backtrace)]
        source: storage::error::Error,
        table_name: String,
    },

    #[snafu(display("Table already exists: {}", table_name))]
    TableExists {
        backtrace: Backtrace,
        table_name: String,
    },

    #[snafu(display("Table not found: {}", table_name))]
    TableNotFound {
        backtrace: Backtrace,
        table_name: String,
    },

    #[snafu(display("Failed to alter table {}, source: {}", table_name, source))]
    AlterTable {
        table_name: String,
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display(
        "Projected columnd not found in region, column: {}",
        column_qualified_name
    ))]
    ProjectedColumnNotFound {
        backtrace: Backtrace,
        column_qualified_name: String,
    },

    #[snafu(display(
        "Failed to convert metadata from deserialized data, source: {}",
        source
    ))]
    ConvertRaw {
        #[snafu(backtrace)]
        source: table::metadata::ConvertError,
    },

    #[snafu(display("Failed to submit procedure, source: {}", source))]
    SubmitProcedure {
        #[snafu(backtrace)]
        source: common_procedure::Error,
    },

    #[snafu(display("Failed to join procedure, source: {}", source))]
    JoinProcedure {
        #[snafu(backtrace)]
        source: common_procedure::Error,
    },

    #[snafu(display("Failed to serialize procedure to json, source: {}", source))]
    SerializeProcedure {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to deserialize procedure from json, source: {}", source))]
    DeserializeProcedure {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid raw schema, source: {}", source))]
    InvalidRawSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            CreateRegion { source, .. } => source.status_code(),

            AlterTable { source, .. } => source.status_code(),

            BuildRowKeyDescriptor { .. }
            | BuildColumnDescriptor { .. }
            | BuildColumnFamilyDescriptor { .. }
            | BuildTableMeta { .. }
            | BuildTableInfo { .. }
            | BuildRegionDescriptor { .. }
            | TableExists { .. }
            | ProjectedColumnNotFound { .. }
            | InvalidPrimaryKey { .. }
            | MissingTimestampIndex { .. } => StatusCode::InvalidArguments,

            TableNotFound { .. } => StatusCode::TableNotFound,

            ConvertRaw { .. } | InvalidRawSchema { .. } => StatusCode::Unexpected,

            ScanTableManifest { .. } | UpdateTableManifest { .. } | SubmitProcedure { .. } => {
                StatusCode::StorageUnavailable
            }

            JoinProcedure { .. } | SerializeProcedure { .. } | DeserializeProcedure { .. } => {
                StatusCode::Internal
            }
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<Error> for common_procedure::Error {
    fn from(e: Error) -> common_procedure::Error {
        common_procedure::Error::external(e)
    }
}

#[cfg(test)]
mod tests {
    use common_error::ext::BoxedError;
    use common_error::mock::MockError;

    use super::*;

    fn throw_create_table(code: StatusCode) -> Result<()> {
        let mock_err = MockError::with_backtrace(code);
        Err(BoxedError::new(mock_err)).context(CreateRegionSnafu)
    }

    #[test]
    fn test_error() {
        let err = throw_create_table(StatusCode::InvalidArguments)
            .err()
            .unwrap();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
        assert!(err.backtrace_opt().is_some());
    }
}
