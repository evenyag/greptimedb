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

//! Procedure for creating table.

use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_procedure::{Context, LockKey, Procedure, Result as ProcedureResult, Status};
use datatypes::schema::RawSchema;
use serde::{Deserialize, Serialize};
use table::engine::TableEngineRef;
use table::metadata::TableId;

// TODO(yingwen): Replace fields by CreateTableRequest once it use RawSchema.
/// Serializable data of [CreateTableProcedure].
#[derive(Debug, Serialize, Deserialize)]
struct CreateTableData {
    table_id: TableId,
    catalog_name: String,
    schema_name: String,
    table_name: String,
    desc: Option<String>,
    schema: RawSchema,
    region_numbers: Vec<u32>,
    primary_key_indices: Vec<usize>,
    create_if_not_exists: bool,
}

struct CreateTableProcedure {
    catalog_manager: CatalogManagerRef,
    table_engine: TableEngineRef,
}
