// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::sync::Arc;

use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use store_api::storage::ColumnId;
use store_api::storage::consts::ReservedColumnId;

pub const PRIMARY_KEY_COL: &str = "__primary_key";
pub const MIN_TS_COL: &str = "min_ts";
pub const MAX_TS_COL: &str = "max_ts";
pub const ROW_COUNT_COL: &str = "row_count";
pub const TABLE_ID_COL: &str = "__table_id";
pub const COLUMN_ID_COL: &str = "column_id";
pub const TAG_VALUE_COL: &str = "tag_value";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Pk,
    TableTag,
    Tag,
}

impl IndexKind {
    pub fn schema(self) -> SchemaRef {
        match self {
            IndexKind::Pk => pk_schema(),
            IndexKind::TableTag => table_tag_schema(),
            IndexKind::Tag => tag_schema(),
        }
    }

    pub fn file_name(self) -> &'static str {
        match self {
            IndexKind::Pk => "pk.parquet",
            IndexKind::TableTag => "table_tag.parquet",
            IndexKind::Tag => "tag.parquet",
        }
    }
}

pub fn pk_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(PRIMARY_KEY_COL, DataType::Binary, false),
        Field::new(MIN_TS_COL, DataType::Int64, false),
        Field::new(MAX_TS_COL, DataType::Int64, false),
        Field::new(ROW_COUNT_COL, DataType::UInt64, false),
    ]))
}

pub fn table_tag_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(TABLE_ID_COL, DataType::UInt32, false),
        Field::new(COLUMN_ID_COL, DataType::UInt32, false),
        Field::new(TAG_VALUE_COL, DataType::Utf8, false),
    ]))
}

pub fn tag_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COLUMN_ID_COL, DataType::UInt32, false),
        Field::new(TAG_VALUE_COL, DataType::Utf8, false),
    ]))
}

pub fn validate_schema(kind: IndexKind, schema: &Schema) -> crate::error::Result<()> {
    if schema != kind.schema().as_ref() {
        return crate::error::InvalidMetaSnafu {
            reason: format!("invalid {:?} index schema: {schema:?}", kind),
        }
        .fail();
    }
    Ok(())
}

pub fn is_reserved_tag_column(column_id: ColumnId) -> bool {
    column_id == ReservedColumnId::table_id() || column_id == ReservedColumnId::tsid()
}
