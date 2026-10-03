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

//! Bounded exact comparison of logical rows, independent of Arrow batch layout.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use datatypes::arrow::datatypes::DataType;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::arrow::row::{RowConverter, SortField};

/// One partition's streaming reference writer or exact comparator.
///
/// Rows use Arrow's logical sort encoding: dictionary indices and batch
/// boundaries do not participate. No hashes or whole-result buffers are used.
pub(crate) struct ExactRowComparison {
    writer: Option<BufWriter<File>>,
    reader: Option<BufReader<File>>,
    converter: Option<RowConverter>,
    order_converter: Option<RowConverter>,
    order_columns: Vec<usize>,
    previous_order: Option<Vec<u8>>,
    schema: Option<String>,
    rows: u64,
    check_order: bool,
}

impl ExactRowComparison {
    pub(crate) fn new(
        write: Option<&Path>,
        compare: Option<&Path>,
        check_order: bool,
    ) -> Result<Self, String> {
        Ok(Self {
            writer: write
                .map(|p| {
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(p)
                        .map(BufWriter::new)
                })
                .transpose()
                .map_err(|e| e.to_string())?,
            reader: compare
                .map(|p| File::open(p).map(BufReader::new))
                .transpose()
                .map_err(|e| e.to_string())?,
            converter: None,
            order_converter: None,
            order_columns: vec![],
            previous_order: None,
            schema: None,
            rows: 0,
            check_order,
        })
    }

    pub(crate) fn consume(&mut self, batch: &RecordBatch) -> Result<(), String> {
        let fields = batch
            .schema()
            .fields()
            .iter()
            .map(|f| {
                let ty = match f.data_type() {
                    DataType::Dictionary(_, value) => value.as_ref(),
                    ty => ty,
                };
                (f.name().clone(), ty.clone())
            })
            .collect::<Vec<_>>();
        let schema = format!("{fields:?}");
        if let Some(previous) = &self.schema {
            if previous != &schema {
                return Err("logical schema changed within a partition".into());
            }
        } else {
            self.frame(schema.as_bytes())?;
            self.converter = Some(
                RowConverter::new(
                    fields
                        .iter()
                        .map(|(_, ty)| SortField::new(ty.clone()))
                        .collect(),
                )
                .map_err(|e| e.to_string())?,
            );
            if self.check_order {
                for name in ["__table_id", "__tsid", "greptime_timestamp"] {
                    if let Some(index) = fields.iter().position(|(field, _)| field == name) {
                        self.order_columns.push(index);
                    }
                }
                if !fields.iter().any(|(name, _)| name == "__tsid")
                    || !fields.iter().any(|(name, _)| name == "greptime_timestamp")
                {
                    return Err(
                        "order checking requires projected __tsid and greptime_timestamp".into(),
                    );
                }
                self.order_converter = Some(
                    RowConverter::new(
                        self.order_columns
                            .iter()
                            .map(|&i| SortField::new(fields[i].1.clone()))
                            .collect(),
                    )
                    .map_err(|e| e.to_string())?,
                );
            }
            self.schema = Some(schema);
        }
        let columns = batch
            .columns()
            .iter()
            .map(|array| match array.data_type() {
                DataType::Dictionary(_, ty) => datatypes::arrow::compute::cast(array, ty),
                _ => Ok(array.clone()),
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        let rows = self
            .converter
            .as_ref()
            .ok_or("missing row converter")?
            .convert_columns(&columns)
            .map_err(|e| e.to_string())?;
        if let Some(converter) = &self.order_converter {
            let order = converter
                .convert_columns(
                    &self
                        .order_columns
                        .iter()
                        .map(|&i| columns[i].clone())
                        .collect::<Vec<_>>(),
                )
                .map_err(|e| e.to_string())?;
            for row in order.iter() {
                if self
                    .previous_order
                    .as_ref()
                    .is_some_and(|previous| previous.as_slice() > row.as_ref())
                {
                    return Err(format!(
                        "series/time ordering decreased near row {}",
                        self.rows
                    ));
                }
                self.previous_order = Some(row.as_ref().to_vec());
            }
        }
        for row in rows.iter() {
            self.frame(row.as_ref())?;
            self.rows += 1;
        }
        Ok(())
    }

    fn frame(&mut self, value: &[u8]) -> Result<(), String> {
        if let Some(reader) = &mut self.reader {
            let mut length = [0u8; 8];
            reader
                .read_exact(&mut length)
                .map_err(|e| format!("reference ended at row {}: {e}", self.rows))?;
            if u64::from_le_bytes(length) != value.len() as u64 {
                return Err(format!("logical row length mismatch at row {}", self.rows));
            }
            // Allocate only the current row, never a reference-controlled size.
            let mut expected = vec![0; value.len()];
            reader
                .read_exact(&mut expected)
                .map_err(|e| e.to_string())?;
            if expected != value {
                return Err(format!("logical value mismatch at row {}", self.rows));
            }
        }
        if let Some(writer) = &mut self.writer {
            writer
                .write_all(&(value.len() as u64).to_le_bytes())
                .and_then(|_| writer.write_all(value))
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<(), String> {
        if let Some(reader) = &mut self.reader {
            let mut trailing = [0];
            if reader.read(&mut trailing).map_err(|e| e.to_string())? != 0 {
                return Err("reference has extra rows".into());
            }
        }
        if let Some(writer) = &mut self.writer {
            writer.flush().map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::arrow::array::{ArrayRef, Int64Array, StringArray, StringDictionaryBuilder};
    use datatypes::arrow::datatypes::UInt32Type;
    use std::sync::Arc;

    fn batch(dictionary: bool) -> RecordBatch {
        let strings: ArrayRef = if dictionary {
            let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
            builder.append("beta").unwrap();
            builder.append("alpha").unwrap();
            Arc::new(builder.finish())
        } else {
            Arc::new(StringArray::from(vec!["beta", "alpha"]))
        };
        RecordBatch::try_from_iter(vec![
            ("tag", strings),
            ("value", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        ])
        .unwrap()
    }

    #[test]
    fn dictionary_and_batch_boundaries_are_logical() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rows");
        let mut writer = ExactRowComparison::new(Some(&path), None, false).unwrap();
        writer.consume(&batch(true)).unwrap();
        writer.finish().unwrap();
        let mut reader = ExactRowComparison::new(None, Some(&path), false).unwrap();
        for i in 0..2 {
            reader.consume(&batch(false).slice(i, 1)).unwrap();
        }
        reader.finish().unwrap();
    }

    #[test]
    fn changed_values_missing_and_extra_rows_fail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rows");
        let mut writer = ExactRowComparison::new(Some(&path), None, false).unwrap();
        writer.consume(&batch(false)).unwrap();
        writer.finish().unwrap();
        let mut missing = ExactRowComparison::new(None, Some(&path), false).unwrap();
        missing.consume(&batch(false).slice(0, 1)).unwrap();
        assert!(missing.finish().is_err());
        let mut extra = ExactRowComparison::new(None, Some(&path), false).unwrap();
        extra.consume(&batch(false)).unwrap();
        assert!(extra.consume(&batch(false)).is_err());
        let mut changed = ExactRowComparison::new(None, Some(&path), false).unwrap();
        assert!(changed.consume(&batch(false).slice(1, 1)).is_err());
    }

    #[test]
    fn ordering_is_checked_across_batches() {
        let batch = RecordBatch::try_from_iter(vec![
            ("__tsid", Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef),
            (
                "greptime_timestamp",
                Arc::new(Int64Array::from(vec![2, 1])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut comparison = ExactRowComparison::new(None, None, true).unwrap();
        comparison.consume(&batch.slice(0, 1)).unwrap();
        assert!(comparison.consume(&batch.slice(1, 1)).is_err());
    }
}
