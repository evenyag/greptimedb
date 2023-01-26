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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use catalog::{CatalogManagerRef, RegisterTableRequest};
use common_procedure::{
    Context, Error, LockKey, Procedure, ProcedureId, ProcedureManagerRef, ProcedureState,
    ProcedureWithId, Result, Status,
};
use common_telemetry::logging;
use datatypes::schema::{RawSchema, Schema};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngineRef, TableReference};
use table::metadata::TableId;
use table::requests::CreateTableRequest;
use tokio::sync::oneshot::{self, Receiver, Sender};

use crate::error::{
    CatalogNotFoundSnafu, CatalogSnafu, DeserializeProcedureSnafu, InvalidRawSchemaSnafu,
    SchemaNotFoundSnafu, SerializeProcedureSnafu,
};

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
    /// Id of the procedure that creates table in the table engine.
    sub_procedure_id: Option<ProcedureId>,
}

impl CreateTableData {
    // TODO(yingwen): Avoid re-create the request, or convert the schema.
    fn to_request(&self) -> Result<CreateTableRequest> {
        Ok(CreateTableRequest {
            id: self.table_id,
            catalog_name: self.catalog_name.clone(),
            schema_name: self.schema_name.clone(),
            table_name: self.table_name.clone(),
            desc: self.desc.clone(),
            schema: Arc::new(Schema::try_from(self.schema.clone()).context(InvalidRawSchemaSnafu)?),
            region_numbers: self.region_numbers.clone(),
            primary_key_indices: self.primary_key_indices.clone(),
            create_if_not_exists: self.create_if_not_exists,
            table_options: HashMap::new(),
        })
    }
}

pub struct CreateTableProcedure {
    data: CreateTableData,
    catalog_manager: CatalogManagerRef,
    table_engine: TableEngineRef,
    procedure_manager: ProcedureManagerRef,
    sender: Option<Sender<()>>,
}

#[async_trait]
impl Procedure for CreateTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
        match self.data.state {
            CreateTableState::Prepare => self.on_prepare(),
            CreateTableState::EngineCreateTable => self.on_engine_create_table().await,
            CreateTableState::RegisterCatalog => self.on_register_catalog().await,
        }
    }

    fn dump(&self) -> Result<String> {
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

    /// Returns a new [CreateTableProcedure] and a receiver to wait for the result.
    ///
    /// The [CreateTableRequest] must be valid.
    pub(crate) fn new(
        request: CreateTableRequest,
        catalog_manager: CatalogManagerRef,
        table_engine: TableEngineRef,
        procedure_manager: ProcedureManagerRef,
    ) -> (Self, Receiver<()>) {
        let (sender, receiver) = oneshot::channel();

        let procedure = CreateTableProcedure {
            data: CreateTableData {
                state: CreateTableState::Prepare,
                table_id: request.id,
                catalog_name: request.catalog_name,
                schema_name: request.schema_name,
                table_name: request.table_name,
                desc: request.desc,
                schema: RawSchema::from(request.schema.as_ref()),
                region_numbers: request.region_numbers,
                primary_key_indices: request.primary_key_indices,
                create_if_not_exists: request.create_if_not_exists,
                sub_procedure_id: None,
            },
            catalog_manager,
            table_engine,
            procedure_manager,
            sender: Some(sender),
        };

        (procedure, receiver)
    }

    /// Recover the procedure from json.
    fn from_json(
        json: &str,
        catalog_manager: CatalogManagerRef,
        table_engine: TableEngineRef,
        procedure_manager: ProcedureManagerRef,
    ) -> Result<Self> {
        let data: CreateTableData =
            serde_json::from_str(json).context(DeserializeProcedureSnafu)?;

        Ok(CreateTableProcedure {
            data,
            catalog_manager,
            table_engine,
            procedure_manager,
            sender: None,
        })
    }

    /// Register the loader of this procedure to the `procedure_manager`.
    ///
    /// # Panics
    /// Panics on error.
    pub(crate) fn register_loader(
        catalog_manager: CatalogManagerRef,
        table_engine: TableEngineRef,
        procedure_manager: ProcedureManagerRef,
    ) {
        let pm = procedure_manager.clone();
        procedure_manager
            .register_loader(
                Self::TYPE_NAME,
                Box::new(move |data| {
                    Self::from_json(
                        data,
                        catalog_manager.clone(),
                        table_engine.clone(),
                        pm.clone(),
                    )
                    .map(|p| Box::new(p) as _)
                }),
            )
            .unwrap()
    }

    fn on_prepare(&mut self) -> Result<Status> {
        // Check whether catalog and schema exist.
        let catalog = self
            .catalog_manager
            .catalog(&self.data.catalog_name)
            .context(CatalogSnafu)?
            .with_context(|| {
                logging::error!(
                    "Failed to create table {}.{}.{}, catalog not found",
                    &self.data.catalog_name,
                    &self.data.schema_name,
                    &self.data.table_name
                );
                CatalogNotFoundSnafu {
                    name: &self.data.catalog_name,
                }
            })?;
        catalog
            .schema(&self.data.schema_name)
            .context(CatalogSnafu)?
            .with_context(|| {
                logging::error!(
                    "Failed to create table {}.{}.{}, schema not found",
                    &self.data.catalog_name,
                    &self.data.schema_name,
                    &self.data.table_name
                );
                SchemaNotFoundSnafu {
                    name: &self.data.schema_name,
                }
            })?;

        self.data.state = CreateTableState::EngineCreateTable;
        self.data.sub_procedure_id = Some(ProcedureId::random());

        Ok(Status::executing(true))
    }

    async fn on_engine_create_table(&mut self) -> Result<Status> {
        // Safety: sub procedure id is always some in this state.
        let sub_id = self.data.sub_procedure_id.unwrap();

        // Check procedure state.
        if let Some(sub_state) = self.procedure_manager.procedure_state(sub_id).await? {
            match sub_state {
                ProcedureState::Running => Ok(Status::Suspended {
                    subprocedures: Vec::new(),
                    persist: false,
                }),
                ProcedureState::Done => {
                    logging::info!(
                        "On engine create table {}, done, sub_id: {}",
                        self.data.table_name,
                        sub_id
                    );
                    // The sub procedure is done, we can execute next step.
                    self.data.state = CreateTableState::RegisterCatalog;
                    Ok(Status::executing(true))
                }
                ProcedureState::Failed => {
                    // If failed, try to create a new procedure to create table.
                    let engine_ctx = EngineContext::default();
                    let procedure = self
                        .table_engine
                        .create_table_procedure(&engine_ctx, self.data.to_request()?)
                        .map_err(Error::external)?;
                    let sub_id = ProcedureId::random();
                    // Store the procedure id.
                    self.data.sub_procedure_id = Some(sub_id);

                    Ok(Status::Suspended {
                        subprocedures: vec![ProcedureWithId {
                            id: sub_id,
                            procedure,
                        }],
                        persist: true,
                    })
                }
            }
        } else {
            logging::info!(
                "On engine create table {}, not found, sub_id: {}",
                self.data.table_name,
                sub_id
            );

            // If the sub procedure is not found, we create a new sub procedure with the same id.
            let engine_ctx = EngineContext::default();
            let procedure = self
                .table_engine
                .create_table_procedure(&engine_ctx, self.data.to_request()?)
                .map_err(Error::external)?;
            Ok(Status::Suspended {
                subprocedures: vec![ProcedureWithId {
                    id: sub_id,
                    procedure,
                }],
                persist: true,
            })
        }
    }

    async fn on_register_catalog(&mut self) -> Result<Status> {
        let catalog = self
            .catalog_manager
            .catalog(&self.data.catalog_name)
            .context(CatalogSnafu)?
            .with_context(|| CatalogNotFoundSnafu {
                name: &self.data.catalog_name,
            })?;
        let schema = catalog
            .schema(&self.data.schema_name)
            .context(CatalogSnafu)?
            .with_context(|| SchemaNotFoundSnafu {
                name: &self.data.schema_name,
            })?;
        let table_exists = schema
            .table(&self.data.table_name)
            .map_err(Error::external)?
            .is_some();
        if table_exists {
            // Table already exists.
            self.may_notify();
            return Ok(Status::Done);
        }

        let engine_ctx = EngineContext::default();
        let table_ref = TableReference {
            catalog: &self.data.catalog_name,
            schema: &self.data.schema_name,
            table: &self.data.table_name,
        };
        // Safety: The procedure should owns the lock so the table should exist.
        let table = self
            .table_engine
            .get_table(&engine_ctx, &table_ref)
            .map_err(Error::external)?
            .unwrap();

        let register_req = RegisterTableRequest {
            catalog: self.data.catalog_name.clone(),
            schema: self.data.schema_name.clone(),
            table_name: self.data.table_name.clone(),
            table_id: self.data.table_id,
            table,
        };
        self.catalog_manager
            .register_table(register_req)
            .await
            .map_err(Error::external)?;

        self.may_notify();
        Ok(Status::Done)
    }

    fn may_notify(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(());
        }
    }
}
