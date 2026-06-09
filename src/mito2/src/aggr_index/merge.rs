// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use datatypes::arrow::array::{
    Array, BinaryArray, Int64Array, StringArray, UInt32Array, UInt64Array,
};
use datatypes::arrow::record_batch::RecordBatch;
use snafu::{OptionExt, ResultExt};

use crate::aggr_index::index_io::{IndexReader, IndexWriter};
use crate::aggr_index::schema::IndexKind;
use crate::error::{InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result};

pub fn merge_index_files(
    kind: IndexKind,
    inputs: &[impl AsRef<Path>],
    output: impl AsRef<Path>,
) -> Result<usize> {
    match kind {
        IndexKind::Pk => merge_pk(inputs, output),
        IndexKind::TableTag => merge_table_tag(inputs, output),
        IndexKind::Tag => merge_tag(inputs, output),
    }
}

fn merge_pk(inputs: &[impl AsRef<Path>], output: impl AsRef<Path>) -> Result<usize> {
    let mut map: BTreeMap<Vec<u8>, (i64, i64, u64)> = BTreeMap::new();
    for input in inputs {
        let mut reader = IndexReader::try_new(input, IndexKind::Pk)?;
        for batch in &mut reader {
            let batch = batch?;
            let pk = binary_col(&batch, 0)?;
            let min = int64_col(&batch, 1)?;
            let max = int64_col(&batch, 2)?;
            let cnt = u64_col(&batch, 3)?;
            for row in 0..batch.num_rows() {
                let e = map
                    .entry(pk.value(row).to_vec())
                    .or_insert((i64::MAX, i64::MIN, 0));
                e.0 = e.0.min(min.value(row));
                e.1 = e.1.max(max.value(row));
                e.2 += cnt.value(row);
            }
        }
    }
    let mut writer = IndexWriter::try_new(output, IndexKind::Pk.schema())?;
    let rows: Vec<_> = map.into_iter().collect();
    let batch = RecordBatch::try_new(
        IndexKind::Pk.schema(),
        vec![
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|(k, _)| k.as_slice()),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, v)| v.0).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, v)| v.1).collect::<Vec<_>>(),
            )) as _,
            Arc::new(UInt64Array::from(
                rows.iter().map(|(_, v)| v.2).collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .context(NewRecordBatchSnafu)?;
    writer.write(&batch)?;
    writer.close()
}

fn merge_table_tag(inputs: &[impl AsRef<Path>], output: impl AsRef<Path>) -> Result<usize> {
    let mut set = BTreeMap::<(u32, u32, String), ()>::new();
    for input in inputs {
        let mut reader = IndexReader::try_new(input, IndexKind::TableTag)?;
        for batch in &mut reader {
            let batch = batch?;
            let table = u32_col(&batch, 0)?;
            let col = u32_col(&batch, 1)?;
            let val = string_col(&batch, 2)?;
            for row in 0..batch.num_rows() {
                set.insert(
                    (table.value(row), col.value(row), val.value(row).to_string()),
                    (),
                );
            }
        }
    }
    let rows: Vec<_> = set.into_keys().collect();
    let mut writer = IndexWriter::try_new(output, IndexKind::TableTag.schema())?;
    let batch = RecordBatch::try_new(
        IndexKind::TableTag.schema(),
        vec![
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as _,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.2.as_str()),
            )) as _,
        ],
    )
    .context(NewRecordBatchSnafu)?;
    writer.write(&batch)?;
    writer.close()
}

fn merge_tag(inputs: &[impl AsRef<Path>], output: impl AsRef<Path>) -> Result<usize> {
    let mut set = BTreeMap::<(u32, String), ()>::new();
    for input in inputs {
        let mut reader = IndexReader::try_new(input, IndexKind::Tag)?;
        for batch in &mut reader {
            let batch = batch?;
            let col = u32_col(&batch, 0)?;
            let val = string_col(&batch, 1)?;
            for row in 0..batch.num_rows() {
                set.insert((col.value(row), val.value(row).to_string()), ());
            }
        }
    }
    let rows: Vec<_> = set.into_keys().collect();
    let mut writer = IndexWriter::try_new(output, IndexKind::Tag.schema())?;
    let batch = RecordBatch::try_new(
        IndexKind::Tag.schema(),
        vec![
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.1.as_str()),
            )) as _,
        ],
    )
    .context(NewRecordBatchSnafu)?;
    writer.write(&batch)?;
    writer.close()
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
fn u32_col(batch: &RecordBatch, idx: usize) -> Result<&UInt32Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .context(InvalidRecordBatchSnafu {
            reason: format!("column {idx} is not UInt32"),
        })
}

fn string_col(batch: &RecordBatch, idx: usize) -> Result<&StringArray> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .context(InvalidRecordBatchSnafu {
            reason: format!("column {idx} is not Utf8"),
        })
}
