// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::sync::Arc;

use datatypes::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use store_api::storage::ColumnId;
use store_api::storage::consts::ReservedColumnId;

pub const PRIMARY_KEY_COL: &str = "__primary_key";
pub const MIN_TS_COL: &str = "min_ts";
pub const MAX_TS_COL: &str = "max_ts";
pub const ROW_COUNT_COL: &str = "row_count";
pub const TABLE_ID_COL: &str = "__table_id";
pub const COLUMN_ID_COL: &str = "column_id";
pub const TAG_VALUE_COL: &str = "tag_value";
pub const TSID_COL: &str = "__tsid";
pub const TAGS_COL: &str = "tags";
pub const MAP_KEY_FIELD: &str = "key";
pub const MAP_VALUE_FIELD: &str = "value";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Pk,
    TableTag,
    Tag,
    TableTagTsid,
    PkMap,
    PkColumns,
}

impl IndexKind {
    pub fn schema(self) -> SchemaRef {
        match self {
            IndexKind::Pk => pk_schema(),
            IndexKind::TableTag => table_tag_schema(),
            IndexKind::Tag => tag_schema(),
            IndexKind::TableTagTsid => table_tag_tsid_schema(),
            IndexKind::PkMap => pk_map_schema(),
            IndexKind::PkColumns => pk_columns_base_schema(),
        }
    }

    pub fn file_name(self) -> &'static str {
        match self {
            IndexKind::Pk => "pk.parquet",
            IndexKind::TableTag => "table_tag.parquet",
            IndexKind::Tag => "tag.parquet",
            IndexKind::TableTagTsid => "table_tag_tsid.parquet",
            IndexKind::PkMap => "pk_map.parquet",
            IndexKind::PkColumns => "pk_columns.parquet",
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

pub fn table_tag_tsid_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(TABLE_ID_COL, DataType::UInt32, false),
        Field::new(COLUMN_ID_COL, DataType::UInt32, false),
        Field::new(TAG_VALUE_COL, DataType::Utf8, false),
        Field::new(TSID_COL, DataType::UInt64, false),
    ]))
}

pub fn pk_map_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(PRIMARY_KEY_COL, DataType::Binary, false),
        Field::new(MIN_TS_COL, DataType::Int64, false),
        Field::new(MAX_TS_COL, DataType::Int64, false),
        Field::new(ROW_COUNT_COL, DataType::UInt64, false),
        Field::new(TABLE_ID_COL, DataType::UInt32, false),
        Field::new(TSID_COL, DataType::UInt64, false),
        tags_map_field(),
    ]))
}

pub fn pk_columns_base_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(PRIMARY_KEY_COL, DataType::Binary, false),
        Field::new(MIN_TS_COL, DataType::Int64, false),
        Field::new(MAX_TS_COL, DataType::Int64, false),
        Field::new(ROW_COUNT_COL, DataType::UInt64, false),
        Field::new(TABLE_ID_COL, DataType::UInt32, false),
        Field::new(TSID_COL, DataType::UInt64, false),
    ]))
}

pub fn tags_map_field() -> Field {
    let entry_fields = Fields::from(vec![
        Field::new(MAP_KEY_FIELD, DataType::UInt32, false),
        Field::new(MAP_VALUE_FIELD, DataType::Utf8, false),
    ]);
    let entries = Field::new("entries", DataType::Struct(entry_fields), false);
    Field::new(TAGS_COL, DataType::Map(Arc::new(entries), false), false)
}

pub fn tag_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COLUMN_ID_COL, DataType::UInt32, false),
        Field::new(TAG_VALUE_COL, DataType::Utf8, false),
    ]))
}

pub fn validate_schema(kind: IndexKind, schema: &Schema) -> crate::error::Result<()> {
    if kind == IndexKind::PkColumns {
        return validate_pk_columns_schema(schema);
    }
    if schema != kind.schema().as_ref() {
        return crate::error::InvalidMetaSnafu {
            reason: format!("invalid {:?} index schema: {schema:?}", kind),
        }
        .fail();
    }
    Ok(())
}

fn validate_pk_columns_schema(schema: &Schema) -> crate::error::Result<()> {
    let base = pk_columns_base_schema();
    if schema.fields().len() < base.fields().len() {
        return crate::error::InvalidMetaSnafu {
            reason: format!("invalid PkColumns index schema: {schema:?}"),
        }
        .fail();
    }
    for (actual, expected) in schema
        .fields()
        .iter()
        .take(base.fields().len())
        .zip(base.fields())
    {
        if actual.as_ref() != expected.as_ref() {
            return crate::error::InvalidMetaSnafu {
                reason: format!("invalid PkColumns index schema: {schema:?}"),
            }
            .fail();
        }
    }
    for field in schema.fields().iter().skip(base.fields().len()) {
        if field.data_type() != &DataType::Utf8 || !field.is_nullable() {
            return crate::error::InvalidMetaSnafu {
                reason: format!("invalid PkColumns tag field: {field:?}"),
            }
            .fail();
        }
    }
    Ok(())
}

pub fn is_reserved_tag_column(column_id: ColumnId) -> bool {
    column_id == ReservedColumnId::table_id() || column_id == ReservedColumnId::tsid()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_new_index_schemas_validate() {
        validate_schema(IndexKind::TableTagTsid, table_tag_tsid_schema().as_ref()).unwrap();
        validate_schema(IndexKind::PkMap, pk_map_schema().as_ref()).unwrap();
    }

    #[test]
    fn test_pk_columns_dynamic_schema_validation() {
        let mut fields = pk_columns_base_schema().fields().to_vec();
        fields.push(Arc::new(Field::new("tag_0", DataType::Utf8, true)));
        fields.push(Arc::new(Field::new("tag_1", DataType::Utf8, true)));
        let schema = Schema::new(fields);
        validate_schema(IndexKind::PkColumns, &schema).unwrap();

        let mut fields = pk_columns_base_schema().fields().to_vec();
        fields.push(Arc::new(Field::new("bad_tag", DataType::Utf8, false)));
        let schema = Schema::new(fields);
        assert!(validate_schema(IndexKind::PkColumns, &schema).is_err());
    }
}
