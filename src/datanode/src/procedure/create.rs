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
use snafu::ResultExt;
use table::engine::{TableEngineRef, TableReference};
use table::metadata::TableId;

use crate::error::SerializeProcedureSnafu;

/// Represents each step while creating table in the datanode.
#[derive(Debug, Serialize, Deserialize)]
enum CreateTableState {
    /// Prepare to create table.
    Prepare,
    /// Create table in the table engine.
    EngineCreateTable,
    /// Register the table to the catalog.
    RegisterCatalog,
}

// TODO(yingwen): Replace fields by CreateTableRequest once it use RawSchema.
/// Serializable data of [CreateTableProcedure].
#[derive(Debug, Serialize, Deserialize)]
struct CreateTableData {
    state: CreateTableState,
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
    data: CreateTableData,
    catalog_manager: CatalogManagerRef,
    table_engine: TableEngineRef,
}

#[async_trait]
impl Procedure for CreateTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &Context) -> ProcedureResult<Status> {
        todo!()
    }

    fn dump(&self) -> ProcedureResult<String> {
        let json = serde_json::to_string(&self.data).context(SerializeProcedureSnafu)?;
        Ok(json)
    }

    fn lock_key(&self) -> Option<LockKey> {
        let table_ref = TableReference {
            catalog: &self.data.catalog_name,
            schema: &self.data.schema_name,
            table: &self.data.table_name,
        };
        let key = table_ref.to_string();
        Some(LockKey::new(key))
    }
}

impl CreateTableProcedure {
    const TYPE_NAME: &str = "datanode::CreateTable";
}
