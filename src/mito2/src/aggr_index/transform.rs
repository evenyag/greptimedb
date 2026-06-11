// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, Int64Array, MapArray, StringArray, StructArray, UInt32Array,
    UInt64Array,
};
use datatypes::arrow::buffer::OffsetBuffer;
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::value::Value;
use futures::StreamExt;
use mito_codec::row_converter::{CompositeValues, SparseValues, build_primary_key_codec};
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use store_api::storage::consts::ReservedColumnId;

use crate::aggr_index::index_io::{IndexReader, IndexWriter};
use crate::aggr_index::schema::{
    IndexKind, MAP_KEY_FIELD, MAP_VALUE_FIELD, is_reserved_tag_column, pk_columns_base_schema,
};
use crate::error::{InvalidMetaSnafu, InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TransformFormat {
    TableTagTsid,
    PkMap,
    PkColumns,
}

impl TransformFormat {
    pub fn kind(self) -> IndexKind {
        match self {
            Self::TableTagTsid => IndexKind::TableTagTsid,
            Self::PkMap => IndexKind::PkMap,
            Self::PkColumns => IndexKind::PkColumns,
        }
    }

    pub fn all() -> Vec<Self> {
        vec![Self::TableTagTsid, Self::PkMap, Self::PkColumns]
    }
}

#[derive(Debug, Clone, Default)]
pub struct TransformCosts {
    pub read_iteration: Duration,
    pub decode_transform: Duration,
    pub table_tag_tsid_write: Duration,
    pub pk_map_write: Duration,
    pub pk_columns_write: Duration,
}

#[derive(Debug, Clone)]
pub struct TransformOutput {
    pub table_tag_tsid_path: Option<String>,
    pub pk_map_path: Option<String>,
    pub pk_columns_path: Option<String>,
    pub table_tag_tsid_rows: usize,
    pub pk_map_rows: usize,
    pub pk_columns_rows: usize,
    pub costs: TransformCosts,
}

#[derive(Debug, Clone)]
struct DecodedPkRow {
    min_ts: i64,
    max_ts: i64,
    row_count: u64,
    table_id: u32,
    tsid: u64,
    tags: BTreeMap<ColumnId, String>,
}

pub async fn transform_pk_index(
    metadata: RegionMetadataRef,
    object_store: ObjectStore,
    pk_input: &str,
    output_dir: &str,
    formats: &[TransformFormat],
) -> Result<TransformOutput> {
    ensure!(
        metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse,
        InvalidMetaSnafu {
            reason: "aggregate index transform only supports sparse primary-key encoding"
        }
    );

    let formats = if formats.is_empty() {
        TransformFormat::all()
    } else {
        formats.to_vec()
    };
    let output_dir = output_dir.trim_end_matches('/');
    let codec = build_primary_key_codec(&metadata);
    let tag_columns = tag_columns(&metadata);

    let read_start = Instant::now();
    let mut reader = IndexReader::try_new(&object_store, pk_input, IndexKind::Pk).await?;
    let mut batches = Vec::new();
    while let Some(batch) = reader.next().await {
        batches.push(batch?);
    }
    let read_iteration = read_start.elapsed();

    let decode_start = Instant::now();
    let mut rows = Vec::new();
    for batch in &batches {
        let pk = binary_col(batch, 0)?;
        let min = int64_col(batch, 1)?;
        let max = int64_col(batch, 2)?;
        let cnt = u64_col(batch, 3)?;
        for row in 0..batch.num_rows() {
            let sparse = match codec
                .decode(pk.value(row))
                .context(crate::error::DecodeSnafu)?
            {
                CompositeValues::Sparse(sparse) => sparse,
                other => {
                    return InvalidMetaSnafu {
                        reason: format!("decoded primary key is not sparse: {other:?}"),
                    }
                    .fail();
                }
            };
            let table_id = required_u32(&sparse, ReservedColumnId::table_id(), "__table_id")?;
            let tsid = required_u64(&sparse, ReservedColumnId::tsid(), "__tsid")?;
            let mut tags = BTreeMap::new();
            for (column_id, _) in &tag_columns {
                if let Some(value) = sparse.get(column_id) {
                    tags.insert(*column_id, string_value(*column_id, value)?);
                }
            }
            rows.push(DecodedPkRow {
                min_ts: min.value(row),
                max_ts: max.value(row),
                row_count: cnt.value(row),
                table_id,
                tsid,
                tags,
            });
        }
    }
    let decode_transform = decode_start.elapsed();

    let mut output = TransformOutput {
        table_tag_tsid_path: None,
        pk_map_path: None,
        pk_columns_path: None,
        table_tag_tsid_rows: 0,
        pk_map_rows: 0,
        pk_columns_rows: 0,
        costs: TransformCosts {
            read_iteration,
            decode_transform,
            ..Default::default()
        },
    };

    if formats.contains(&TransformFormat::TableTagTsid) {
        let path = format!("{output_dir}/{}", IndexKind::TableTagTsid.file_name());
        let start = Instant::now();
        output.table_tag_tsid_rows = write_table_tag_tsid(&object_store, &path, &rows).await?;
        output.costs.table_tag_tsid_write = start.elapsed();
        output.table_tag_tsid_path = Some(path);
    }
    if formats.contains(&TransformFormat::PkMap) {
        let path = format!("{output_dir}/{}", IndexKind::PkMap.file_name());
        let start = Instant::now();
        output.pk_map_rows = write_pk_map(&object_store, &path, &rows).await?;
        output.costs.pk_map_write = start.elapsed();
        output.pk_map_path = Some(path);
    }
    if formats.contains(&TransformFormat::PkColumns) {
        let path = format!("{output_dir}/{}", IndexKind::PkColumns.file_name());
        let start = Instant::now();
        output.pk_columns_rows =
            write_pk_columns(&object_store, &path, &rows, &tag_columns).await?;
        output.costs.pk_columns_write = start.elapsed();
        output.pk_columns_path = Some(path);
    }

    Ok(output)
}

async fn write_table_tag_tsid(
    object_store: &ObjectStore,
    path: &str,
    rows: &[DecodedPkRow],
) -> Result<usize> {
    let mut table_ids = Vec::new();
    let mut column_ids = Vec::new();
    let mut values = Vec::new();
    let mut tsids = Vec::new();
    for row in rows {
        for (column_id, value) in &row.tags {
            table_ids.push(row.table_id);
            column_ids.push(*column_id);
            values.push(value.as_str());
            tsids.push(row.tsid);
        }
    }
    let batch = RecordBatch::try_new(
        IndexKind::TableTagTsid.schema(),
        vec![
            Arc::new(UInt32Array::from(table_ids)) as _,
            Arc::new(UInt32Array::from(column_ids)) as _,
            Arc::new(StringArray::from_iter_values(values)) as _,
            Arc::new(UInt64Array::from(tsids)) as _,
        ],
    )
    .context(NewRecordBatchSnafu)?;
    write_one_batch(object_store, path, IndexKind::TableTagTsid.schema(), batch).await
}

async fn write_pk_map(
    object_store: &ObjectStore,
    path: &str,
    rows: &[DecodedPkRow],
) -> Result<usize> {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut lengths = Vec::new();
    for row in rows {
        lengths.push(row.tags.len());
        for (column_id, value) in &row.tags {
            keys.push(*column_id);
            values.push(value.as_str());
        }
    }
    let key_field = Arc::new(Field::new(MAP_KEY_FIELD, DataType::UInt32, false));
    let value_field = Arc::new(Field::new(MAP_VALUE_FIELD, DataType::Utf8, false));
    let struct_array = StructArray::from(vec![
        (key_field, Arc::new(UInt32Array::from(keys)) as ArrayRef),
        (
            value_field,
            Arc::new(StringArray::from_iter_values(values)) as ArrayRef,
        ),
    ]);
    let entries = match IndexKind::PkMap.schema().field(5).data_type() {
        DataType::Map(entries, _) => entries.clone(),
        _ => unreachable!("pk_map tags field is a map"),
    };
    let map = MapArray::new(
        entries,
        OffsetBuffer::from_lengths(lengths),
        struct_array,
        None,
        false,
    );

    let batch = RecordBatch::try_new(
        IndexKind::PkMap.schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.min_ts).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.max_ts).collect::<Vec<_>>(),
            )) as _,
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.row_count).collect::<Vec<_>>(),
            )) as _,
            Arc::new(UInt32Array::from(
                rows.iter().map(|row| row.table_id).collect::<Vec<_>>(),
            )) as _,
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.tsid).collect::<Vec<_>>(),
            )) as _,
            Arc::new(map) as _,
        ],
    )
    .context(NewRecordBatchSnafu)?;
    write_one_batch(object_store, path, IndexKind::PkMap.schema(), batch).await
}

async fn write_pk_columns(
    object_store: &ObjectStore,
    path: &str,
    rows: &[DecodedPkRow],
    tag_columns: &[(ColumnId, String)],
) -> Result<usize> {
    let mut fields = pk_columns_base_schema().fields().to_vec();
    fields.extend(
        tag_columns
            .iter()
            .map(|(_, name)| Arc::new(Field::new(name, DataType::Utf8, true))),
    );
    let schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.min_ts).collect::<Vec<_>>(),
        )) as _,
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.max_ts).collect::<Vec<_>>(),
        )) as _,
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.row_count).collect::<Vec<_>>(),
        )) as _,
        Arc::new(UInt32Array::from(
            rows.iter().map(|row| row.table_id).collect::<Vec<_>>(),
        )) as _,
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.tsid).collect::<Vec<_>>(),
        )) as _,
    ];
    for (column_id, _) in tag_columns {
        let values = rows
            .iter()
            .map(|row| row.tags.get(column_id).map(|v| v.as_str()))
            .collect::<Vec<_>>();
        arrays.push(Arc::new(StringArray::from(values)) as _);
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays).context(NewRecordBatchSnafu)?;
    write_one_batch(object_store, path, schema, batch).await
}

async fn write_one_batch(
    object_store: &ObjectStore,
    path: &str,
    schema: SchemaRef,
    batch: RecordBatch,
) -> Result<usize> {
    let mut writer = IndexWriter::try_new(object_store, path, schema).await?;
    writer.write(&batch).await?;
    writer.close().await
}

fn tag_columns(metadata: &RegionMetadataRef) -> Vec<(ColumnId, String)> {
    metadata
        .primary_key_columns()
        .filter(|col| !is_reserved_tag_column(col.column_id))
        .map(|col| (col.column_id, col.column_schema.name.clone()))
        .collect()
}

fn required_u32(values: &SparseValues, column_id: ColumnId, name: &str) -> Result<u32> {
    match values.get(&column_id) {
        Some(Value::UInt32(v)) => Ok(*v),
        other => InvalidMetaSnafu {
            reason: format!("missing/invalid sparse {name}: {other:?}"),
        }
        .fail(),
    }
}

fn required_u64(values: &SparseValues, column_id: ColumnId, name: &str) -> Result<u64> {
    match values.get(&column_id) {
        Some(Value::UInt64(v)) => Ok(*v),
        other => InvalidMetaSnafu {
            reason: format!("missing/invalid sparse {name}: {other:?}"),
        }
        .fail(),
    }
}

fn string_value(column_id: ColumnId, value: &Value) -> Result<String> {
    match value {
        Value::String(value) => Ok(value.as_utf8().to_string()),
        other => InvalidMetaSnafu {
            reason: format!("aggregate PK transform expects string tag value for column {column_id}, got {other:?}"),
        }
        .fail(),
    }
}

fn binary_col(batch: &RecordBatch, idx: usize) -> Result<&BinaryArray> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context(InvalidRecordBatchSnafu {
            reason: format!("column {idx} is not Binary"),
        })
}
fn int64_col(batch: &RecordBatch, idx: usize) -> Result<&Int64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context(InvalidRecordBatchSnafu {
            reason: format!("column {idx} is not Int64"),
        })
}
fn u64_col(batch: &RecordBatch, idx: usize) -> Result<&UInt64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context(InvalidRecordBatchSnafu {
            reason: format!("column {idx} is not UInt64"),
        })
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use datatypes::arrow::array::StringArray;
    use datatypes::value::ValueRef;
    use object_store::services::Fs;
    use store_api::codec::PrimaryKeyEncoding;

    use super::*;
    use crate::test_util::sst_util::sst_region_metadata_with_encoding;

    fn temp_store(root: &str) -> ObjectStore {
        ObjectStore::new(Fs::default().root(root)).unwrap().finish()
    }

    fn encode_pk(
        metadata: &RegionMetadataRef,
        table_id: u32,
        tsid: u64,
        tag_0: Option<&str>,
        tag_1: Option<&str>,
    ) -> Vec<u8> {
        let codec = build_primary_key_codec(metadata);
        let mut values = vec![
            (ReservedColumnId::table_id(), ValueRef::UInt32(table_id)),
            (ReservedColumnId::tsid(), ValueRef::UInt64(tsid)),
        ];
        if let Some(v) = tag_0 {
            values.push((0, ValueRef::String(v)));
        }
        if let Some(v) = tag_1 {
            values.push((1, ValueRef::String(v)));
        }
        let mut buffer = Vec::new();
        codec.encode_value_refs(&values, &mut buffer).unwrap();
        buffer
    }

    async fn write_legacy_pk(store: &ObjectStore, metadata: &RegionMetadataRef, path: &str) {
        let rows = vec![
            encode_pk(metadata, 11, 101, Some("a"), Some("x")),
            encode_pk(metadata, 11, 102, Some("b"), None),
        ];
        let batch = RecordBatch::try_new(
            IndexKind::Pk.schema(),
            vec![
                Arc::new(BinaryArray::from_iter_values(
                    rows.iter().map(|v| v.as_slice()),
                )) as _,
                Arc::new(Int64Array::from(vec![1, 3])) as _,
                Arc::new(Int64Array::from(vec![2, 5])) as _,
                Arc::new(UInt64Array::from(vec![10, 20])) as _,
            ],
        )
        .unwrap();
        let mut writer = IndexWriter::try_new(store, path, IndexKind::Pk.schema())
            .await
            .unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_transform_pk_outputs() {
        let dir = create_temp_dir("aggr_index_transform");
        let store = temp_store(dir.path().to_str().unwrap());
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        write_legacy_pk(&store, &metadata, "old/pk.parquet").await;

        let output = transform_pk_index(metadata, store.clone(), "old/pk.parquet", "new", &[])
            .await
            .unwrap();
        assert_eq!(output.table_tag_tsid_rows, 3);
        assert_eq!(output.pk_map_rows, 2);
        assert_eq!(output.pk_columns_rows, 2);

        let mut reader = IndexReader::try_new(
            &store,
            "new/table_tag_tsid.parquet",
            IndexKind::TableTagTsid,
        )
        .await
        .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);

        let mut reader = IndexReader::try_new(&store, "new/pk_map.parquet", IndexKind::PkMap)
            .await
            .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(0).name(), "min_ts");
        assert_eq!(batch.schema().field(5).name(), "tags");
        let table = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let tsid = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(table.value(0), 11);
        assert_eq!(tsid.value(1), 102);

        let mut reader =
            IndexReader::try_new(&store, "new/pk_columns.parquet", IndexKind::PkColumns)
                .await
                .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.schema().field(0).name(), "min_ts");
        assert_eq!(batch.schema().field(5).name(), "tag_0");
        assert_eq!(batch.schema().field(6).name(), "tag_1");
        let tag_0 = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tag_1 = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(tag_0.value(0), "a");
        assert!(tag_1.is_null(1));
    }

    #[tokio::test]
    async fn test_transform_format_selection() {
        let dir = create_temp_dir("aggr_index_transform_selection");
        let store = temp_store(dir.path().to_str().unwrap());
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        write_legacy_pk(&store, &metadata, "old/pk.parquet").await;

        let output = transform_pk_index(
            metadata,
            store.clone(),
            "old/pk.parquet",
            "new",
            &[TransformFormat::PkMap],
        )
        .await
        .unwrap();
        assert!(output.table_tag_tsid_path.is_none());
        assert!(output.pk_columns_path.is_none());
        assert_eq!(output.pk_map_rows, 2);
        assert!(store.stat("new/pk_map.parquet").await.is_ok());
        assert!(store.stat("new/table_tag_tsid.parquet").await.is_err());
    }
}
