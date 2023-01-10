use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_procedure::{
    Context, Error as ProcedureError, Procedure, Result as ProcedureResult, Status,
};
use datatypes::schema::{RawSchema, SchemaRef};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{
    ColumnDescriptorBuilder, ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, ColumnId,
    CreateOptions, EngineContext, OpenOptions, RegionDescriptor, RegionDescriptorBuilder, RegionId,
    RowKeyDescriptor, RowKeyDescriptorBuilder, StorageEngine,
};
use table::engine::TableReference;
use table::metadata::{TableId, TableInfoBuilder, TableMetaBuilder, TableType};
use table::requests::CreateTableRequest;

use crate::engine::{self, MitoEngineInner};
use crate::error::{
    BuildColumnDescriptorSnafu, BuildColumnFamilyDescriptorSnafu, BuildRegionDescriptorSnafu,
    BuildRowKeyDescriptorSnafu, BuildTableInfoSnafu, BuildTableMetaSnafu,
    MissingTimestampIndexSnafu, Result, TableExistsSnafu,
};
use crate::table::MitoTable;

/// `CreateTableState` represents each step while creating table.
#[derive(Debug)]
enum CreateTableState {
    /// Prepare to create region.
    Prepare,
    /// Creating region.
    CreateRegion,
    /// Writing metadata to table manifest.
    WriteTableManifest,
}

/// Serializable data of [CreateTableProcedure].
#[derive(Debug)]
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
    table_options: HashMap<String, String>,
    /// Next id for column.
    ///
    /// Available in [CreateTableState::WriteTableManifest] state.
    next_column_id: Option<ColumnId>,
}

/// Procedure to create a [MitoTable].
pub struct CreateTableProcedure<S: StorageEngine> {
    data: CreateTableData,
    schema: SchemaRef,
    engine_inner: Arc<MitoEngineInner<S>>,
    /// Region for the table.
    ///
    /// The region is `Some` while [CreateTableData::state] is
    /// [CreateTableState::WriteTableManifest].
    region: Option<S::Region>,
}

#[async_trait]
impl<S: StorageEngine> Procedure for CreateTableProcedure<S> {
    async fn execute(&mut self, ctx: &Context) -> ProcedureResult<Status> {
        match self.data.state {
            CreateTableState::Prepare => self.on_prepare().map_err(ProcedureError::execute),
            CreateTableState::CreateRegion => self.on_create_region().await,
            CreateTableState::WriteTableManifest => self.on_write_table_manifest().await,
        }
    }
}

impl<S: StorageEngine> CreateTableProcedure<S> {
    /// Creates a new [CreateTableProcedure].
    ///
    /// The [CreateTableRequest] must be valid.
    pub(crate) fn new(
        request: CreateTableRequest,
        engine_inner: Arc<MitoEngineInner<S>>,
    ) -> CreateTableProcedure<S> {
        // Now we only support creating one region in the table.
        assert_eq!(request.region_numbers.len(), 1);

        CreateTableProcedure {
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
                table_options: request.table_options,
                next_column_id: None,
            },
            schema: request.schema,
            engine_inner,
            region: None,
        }
    }

    /// Checks whether the table exists.
    fn on_prepare(&mut self) -> Result<Status> {
        let table_ref = TableReference {
            catalog: &self.data.catalog_name,
            schema: &self.data.schema_name,
            table: &self.data.table_name,
        };
        if self.engine_inner.get_table(&table_ref).is_some() {
            // If the table already exists.
            ensure!(
                self.data.create_if_not_exists,
                TableExistsSnafu {
                    table_name: format!("{table_ref}"),
                }
            );

            return Ok(Status::Done);
        }

        self.data.state = CreateTableState::CreateRegion;

        Ok(Status::executing(true))
    }

    /// Creates regions for the table.
    async fn on_create_region(&mut self) -> ProcedureResult<Status> {
        // Try to open the region.
        let region_number = self.data.region_numbers[0];
        let region_name = engine::region_name(self.data.table_id, region_number);

        let engine_ctx = EngineContext::default();
        let table_dir = engine::table_dir(&self.data.schema_name, self.data.table_id);
        let opts = OpenOptions {
            parent_dir: table_dir.clone(),
        };

        if let Some(region) = self
            .engine_inner
            .storage_engine
            .open_region(&engine_ctx, &region_name, &opts)
            .await
            .map_err(ProcedureError::execute)?
        {
            // The region has been created, we could move to the next step.
            self.switch_to_write_table_manifest(region);

            return Ok(Status::executing(true));
        }

        // Create a new region.
        let region_id = engine::region_id(self.data.table_id, region_number);
        let region_desc = self
            .build_region_desc(region_id, &region_name)
            .map_err(ProcedureError::execute)?;
        let opts = CreateOptions {
            parent_dir: table_dir,
        };
        let region = self
            .engine_inner
            .storage_engine
            .create_region(&engine_ctx, region_desc, &opts)
            .await
            .map_err(ProcedureError::execute)?;

        self.switch_to_write_table_manifest(region);

        Ok(Status::executing(true))
    }

    /// Writes metadata to the table manifest.
    async fn on_write_table_manifest(&mut self) -> ProcedureResult<Status> {
        let table_ref = TableReference {
            catalog: &self.data.catalog_name,
            schema: &self.data.schema_name,
            table: &self.data.table_name,
        };
        if self.engine_inner.get_table(&table_ref).is_some() {
            // If the table is opened, we are done.
            return Ok(Status::Done);
        }

        // Try to open the table, as the table manifest might already exist.
        let table_dir = engine::table_dir(&self.data.schema_name, self.data.table_id);
        // Safety: The region is not None in `WriteTableManifest` state.
        let region = self.region.clone().unwrap();
        let table_opt = MitoTable::open(
            &self.data.table_name,
            &table_dir,
            region.clone(),
            self.engine_inner.object_store.clone(),
        )
        .await
        .map_err(ProcedureError::execute)?;
        if let Some(table) = table_opt {
            // We already have the table manifest, just need to insert the table into the table map.
            self.engine_inner
                .tables
                .write()
                .unwrap()
                .insert(table_ref.to_string(), Arc::new(table));
            return Ok(Status::Done);
        }

        // We need to persist the table manifest and create the table instance.
        let table = self
            .write_manifest_and_create_table(&table_dir, region)
            .await
            .map_err(ProcedureError::execute)?;
        self.engine_inner
            .tables
            .write()
            .unwrap()
            .insert(table_ref.to_string(), Arc::new(table));
        Ok(Status::Done)
    }

    /// Switchs to [CreateTableState::WriteTableManifest] state and set [CreateTableProcedure::region].
    fn switch_to_write_table_manifest(&mut self, region: S::Region) {
        self.data.state = CreateTableState::WriteTableManifest;
        self.region = Some(region);
    }

    /// Builds [RegionDescriptor] and cache next column id in [CreateTableProcedure::data].
    fn build_region_desc(
        &mut self,
        region_id: RegionId,
        region_name: &str,
    ) -> Result<RegionDescriptor> {
        let primary_key_indices = &self.data.primary_key_indices;
        let (next_column_id, default_cf) = build_column_family(
            engine::INIT_COLUMN_ID,
            &self.data.table_name,
            &self.schema,
            primary_key_indices,
        )?;
        let (next_column_id, row_key) = build_row_key_desc(
            next_column_id,
            &self.data.table_name,
            &self.schema,
            primary_key_indices,
        )?;

        let region_desc = RegionDescriptorBuilder::default()
            .id(region_id)
            .name(region_name)
            .row_key(row_key)
            .default_cf(default_cf)
            .build()
            .context(BuildRegionDescriptorSnafu {
                table_name: &self.data.table_name,
                region_name,
            })?;

        self.data.next_column_id = Some(next_column_id);

        Ok(region_desc)
    }

    /// Write metadata to the table manifest and return the created table.
    async fn write_manifest_and_create_table(
        &self,
        table_dir: &str,
        region: S::Region,
    ) -> Result<MitoTable<S::Region>> {
        // Safety: We are in `WriteTableManifest` state.
        let next_column_id = self.data.next_column_id.unwrap();

        let table_meta = TableMetaBuilder::default()
            .schema(self.schema.clone())
            .engine(engine::MITO_ENGINE)
            .next_column_id(next_column_id)
            .primary_key_indices(self.data.primary_key_indices.clone())
            .region_numbers(self.data.region_numbers.clone())
            .build()
            .context(BuildTableMetaSnafu {
                table_name: &self.data.table_name,
            })?;

        let table_info = TableInfoBuilder::new(self.data.table_name.clone(), table_meta)
            .ident(self.data.table_id)
            .table_version(engine::INIT_TABLE_VERSION)
            .table_type(TableType::Base)
            .catalog_name(&self.data.catalog_name)
            .schema_name(&self.data.schema_name)
            .desc(self.data.desc.clone())
            .build()
            .context(BuildTableInfoSnafu {
                table_name: &self.data.table_name,
            })?;

        let table = MitoTable::create(
            &self.data.table_name,
            table_dir,
            table_info,
            region,
            self.engine_inner.object_store.clone(),
        )
        .await?;

        Ok(table)
    }
}

fn build_column_family(
    mut column_id: ColumnId,
    table_name: &str,
    table_schema: &SchemaRef,
    primary_key_indices: &[usize],
) -> Result<(ColumnId, ColumnFamilyDescriptor)> {
    let mut builder = ColumnFamilyDescriptorBuilder::default();

    let ts_index = table_schema
        .timestamp_index()
        .context(MissingTimestampIndexSnafu { table_name })?;
    let column_schemas = table_schema
        .column_schemas()
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != ts_index && !primary_key_indices.contains(index));

    for (_, column_schema) in column_schemas {
        let column = ColumnDescriptorBuilder::new(
            column_id,
            column_schema.name.clone(),
            column_schema.data_type.clone(),
        )
        .default_constraint(column_schema.default_constraint().cloned())
        .is_nullable(column_schema.is_nullable())
        .build()
        .context(BuildColumnDescriptorSnafu {
            column_name: &column_schema.name,
            table_name,
        })?;

        builder = builder.push_column(column);
        column_id += 1;
    }

    Ok((
        column_id,
        builder
            .build()
            .context(BuildColumnFamilyDescriptorSnafu { table_name })?,
    ))
}

fn build_row_key_desc(
    mut column_id: ColumnId,
    table_name: &str,
    table_schema: &SchemaRef,
    primary_key_indices: &Vec<usize>,
) -> Result<(ColumnId, RowKeyDescriptor)> {
    let ts_column_schema = table_schema
        .timestamp_column()
        .context(MissingTimestampIndexSnafu { table_name })?;
    // `unwrap` is safe because we've checked the `timestamp_column` above
    let timestamp_index = table_schema.timestamp_index().unwrap();

    let ts_column = ColumnDescriptorBuilder::new(
        column_id,
        ts_column_schema.name.clone(),
        ts_column_schema.data_type.clone(),
    )
    .default_constraint(ts_column_schema.default_constraint().cloned())
    .is_nullable(ts_column_schema.is_nullable())
    .is_time_index(true)
    .build()
    .context(BuildColumnDescriptorSnafu {
        column_name: &ts_column_schema.name,
        table_name,
    })?;
    column_id += 1;

    let column_schemas = &table_schema.column_schemas();

    //TODO(boyan): enable version column by table option?
    let mut builder = RowKeyDescriptorBuilder::new(ts_column);

    for index in primary_key_indices {
        if *index == timestamp_index {
            continue;
        }

        let column_schema = &column_schemas[*index];

        let column = ColumnDescriptorBuilder::new(
            column_id,
            column_schema.name.clone(),
            column_schema.data_type.clone(),
        )
        .default_constraint(column_schema.default_constraint().cloned())
        .is_nullable(column_schema.is_nullable())
        .build()
        .context(BuildColumnDescriptorSnafu {
            column_name: &column_schema.name,
            table_name,
        })?;

        builder = builder.push_column(column);
        column_id += 1;
    }

    Ok((
        column_id,
        builder
            .build()
            .context(BuildRowKeyDescriptorSnafu { table_name })?,
    ))
}
