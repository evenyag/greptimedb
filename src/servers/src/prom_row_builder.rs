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

use std::string::ToString;

use api::pool::PROM_ROWS_POOL;
use api::prom_store::remote::Sample;
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnSchema, Row, RowInsertRequest, RowInsertRequests, Rows, SemanticType,
    Value,
};
use bytes::Bytes;
use common_query::prelude::{GREPTIME_TIMESTAMP, GREPTIME_VALUE};
use datafusion::parquet::data_type::AsBytes;
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use prost::DecodeError;

use crate::proto::PromLabel;
use crate::repeated_field::Clear;

/// [TablesBuilder] serves as an intermediate container to build [RowInsertRequests].
#[derive(Default, Debug)]
pub(crate) struct TablesBuilder {
    tables: HashMap<String, TableBuilder>,
}

impl Clear for TablesBuilder {
    fn clear(&mut self) {
        self.tables.clear();
    }
}

impl TablesBuilder {
    /// Gets table builder with given table name. Creates an empty [TableBuilder] if not exist.
    pub(crate) fn get_or_create_table_builder(
        &mut self,
        table_name: String,
        label_num: usize,
        row_num: usize,
    ) -> &mut TableBuilder {
        self.tables
            .entry(table_name)
            .or_insert_with(|| TableBuilder::with_capacity(label_num + 2, row_num))
    }

    /// Converts [TablesBuilder] to [RowInsertRequests] and row numbers and clears inner states.
    pub(crate) fn as_insert_requests(&mut self) -> (RowInsertRequests, usize) {
        let mut total_rows = 0;
        let inserts = self
            .tables
            .drain()
            .map(|(name, mut table)| {
                total_rows += table.num_rows();
                table.as_row_insert_request(name)
            })
            .collect();
        (RowInsertRequests { inserts }, total_rows)
    }
}

/// [PoolTablesBuilder] serves as an intermediate container to build [RowInsertRequests].
#[derive(Default, Debug)]
pub(crate) struct PoolTablesBuilder {
    tables: HashMap<String, PoolTableBuilder>,
}

impl Clear for PoolTablesBuilder {
    fn clear(&mut self) {
        self.tables.clear();
    }
}

impl PoolTablesBuilder {
    /// Gets table builder with given table name. Creates an empty [TableBuilder] if not exist.
    pub(crate) fn get_or_create_table_builder(
        &mut self,
        table_name: String,
        label_num: usize,
        row_num: usize,
    ) -> &mut PoolTableBuilder {
        self.tables.entry(table_name).or_insert_with(|| {
            if let Some(rows) = PROM_ROWS_POOL.try_pull() {
                PoolTableBuilder::from_rows(rows)
            } else {
                PoolTableBuilder::with_capacity(label_num + 2, row_num)
            }
        })
    }

    /// Converts [TablesBuilder] to [RowInsertRequests] and row numbers and clears inner states.
    pub(crate) fn as_insert_requests(&mut self) -> (RowInsertRequests, usize) {
        let mut total_rows = 0;
        let inserts = self
            .tables
            .drain()
            .map(|(name, mut table)| {
                total_rows += table.num_rows();
                table.as_row_insert_request(name)
            })
            .collect();
        (RowInsertRequests { inserts }, total_rows)
    }
}

/// Builder for one table.
///
/// It reuses the same buffer for rows and schema.
/// It will try to use a fast mode that assumes all rows have the same schema.
/// If it finds a row with a different schema, it will switch to the slow mode.
#[derive(Debug)]
pub(crate) struct PoolTableBuilder {
    /// Column schemas.
    /// The builder will reuse the same schema buffer.
    /// The schema always contains at least two columns: timestamp and value.
    schema: Vec<ColumnSchema>,
    /// Rows written.
    /// The builder will reuse the same rows buffer.
    /// To get the number of rows written, we need to use `num_rows`.
    /// It may contains more rows than `num_rows`.
    rows: Vec<Row>,
    /// Number of rows written.
    num_rows: usize,

    /// Indices of columns inside `schema` for slow mode.
    /// By default this is empty. Once we switch to the slow mode, we will
    /// fill this map and use the column name as the index.
    col_indexes: HashMap<String, usize>,
}

impl PoolTableBuilder {
    /// Creates a new `PoolTableBuilder` with the given capacity for columns and rows.
    pub(crate) fn with_capacity(cols: usize, rows: usize) -> Self {
        let mut schema = Vec::with_capacity(cols);
        schema.push(ColumnSchema {
            column_name: GREPTIME_TIMESTAMP.to_string(),
            datatype: ColumnDataType::TimestampMillisecond as i32,
            semantic_type: SemanticType::Timestamp as i32,
            datatype_extension: None,
            options: None,
        });

        schema.push(ColumnSchema {
            column_name: GREPTIME_VALUE.to_string(),
            datatype: ColumnDataType::Float64 as i32,
            semantic_type: SemanticType::Field as i32,
            datatype_extension: None,
            options: None,
        });

        Self {
            schema,
            rows: Vec::with_capacity(rows),
            num_rows: 0,
            col_indexes: HashMap::new(),
        }
    }

    /// Creates a `PoolTableBuilder` from the given `Rows`.
    /// Reuses the memory.
    pub fn from_rows(rows: Rows) -> Self {
        Self {
            schema: rows.schema,
            rows: rows.rows,
            num_rows: 0,
            col_indexes: HashMap::new(),
        }
    }

    /// Adds a set of labels and samples to table builder.
    pub(crate) fn add_labels_and_samples(
        &mut self,
        labels: &[PromLabel],
        samples: &[Sample],
        is_strict_mode: bool,
    ) -> Result<(), DecodeError> {
        if samples.is_empty() {
            return Ok(());
        }

        if self.ensure_schema(labels, is_strict_mode)? {
            self.add_labels_and_samples_fast(labels, samples, is_strict_mode)?;
        } else {
            self.add_labels_and_samples_slow(labels, samples, is_strict_mode)?;
        }

        Ok(())
    }

    /// Converts [PoolTableBuilder] to [RowInsertRequest] and clears buffered data.
    pub(crate) fn as_row_insert_request(&mut self, table_name: String) -> RowInsertRequest {
        if self.num_rows == 0 {
            // The builder is empty, return an empty request.
            return RowInsertRequest {
                table_name,
                rows: None,
            };
        }

        self.rows.truncate(self.num_rows);
        let mut rows = std::mem::take(&mut self.rows);
        self.num_rows = 0;
        let schema = std::mem::take(&mut self.schema);

        if !self.col_indexes.is_empty() {
            // Slow mode, we have different schema.
            let col_num = schema.len();
            for row in &mut rows {
                if row.values.len() < col_num {
                    row.values.resize(col_num, Value { value_data: None });
                }
            }
            self.col_indexes.clear();
        }

        RowInsertRequest {
            table_name,
            rows: Some(Rows { schema, rows }),
        }
    }

    /// Returns the number of rows in the table builder.
    fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Adds a set of labels and samples to table builder in slow mode.
    fn add_labels_and_samples_slow(
        &mut self,
        labels: &[PromLabel],
        samples: &[Sample],
        is_strict_mode: bool,
    ) -> Result<(), DecodeError> {
        debug_assert!(!self.col_indexes.is_empty());

        self.start_new_row();

        for PromLabel { name, value } in labels {
            // Safety: `Self::ensure_schema()` already checked that the name is valid UTF-8.
            let tag_name = unsafe { std::str::from_utf8_unchecked(name.as_bytes()) };
            let tag_value = Self::bytes_to_str(value, is_strict_mode)?;

            if let Some(col_idx) = self.col_indexes.get(tag_name) {
                // Name already exists, sets the value.
                // In slow mode, the col idx already consider the timestamp and value.
                self.set_column_value(*col_idx, tag_value);
            } else {
                // A new column.
                let tag_num = self.col_indexes.len();
                debug_assert_eq!(tag_num, self.schema.len());
                self.col_indexes.insert(tag_name.to_string(), tag_num);
                self.schema.push(ColumnSchema {
                    column_name: tag_name.to_string(),
                    datatype: ColumnDataType::String as i32,
                    semantic_type: SemanticType::Tag as i32,
                    datatype_extension: None,
                    options: None,
                });
                self.push_column_value(tag_value);
            }
        }

        self.add_samples(samples);

        Ok(())
    }

    /// Adds a set of labels and samples to table builder in fast mode.
    fn add_labels_and_samples_fast(
        &mut self,
        labels: &[PromLabel],
        samples: &[Sample],
        is_strict_mode: bool,
    ) -> Result<(), DecodeError> {
        debug_assert!(self.col_indexes.is_empty());

        self.start_new_row();

        for (label_idx, label) in labels.iter().enumerate() {
            let tag_value = Self::bytes_to_str(&label.value, is_strict_mode)?;
            // Put tags behind timestamp and value.
            self.set_column_value(label_idx + 2, tag_value);
        }

        self.add_samples(samples);

        Ok(())
    }

    /// Allocates a new row to write.
    /// It ensures the new row has the same number of columns as the schema.
    fn start_new_row(&mut self) {
        if self.num_rows >= self.rows.len() {
            self.rows.push(Row {
                values: vec![Value { value_data: None }; self.schema.len()],
            });
        } else {
            self.rows[self.num_rows]
                .values
                .resize(self.schema.len(), Value { value_data: None });
        }
    }

    /// Sets the value of a tag in the current row.
    fn set_column_value(&mut self, col_idx: usize, tag_value: &str) {
        let row = &mut self.rows[self.num_rows];
        Self::update_tag_value(&mut row.values[col_idx], tag_value);
    }

    /// Pushes the value of a tag to the current row.
    fn push_column_value(&mut self, tag_value: &str) {
        let row = &mut self.rows[self.num_rows];
        // Pushes a new value to the row.
        row.values.push(Value {
            value_data: Some(ValueData::StringValue(tag_value.to_string())),
        });
    }

    /// Adds samples to rows and bumps `self.num_rows`.
    fn add_samples(&mut self, samples: &[Sample]) {
        debug_assert!(!samples.is_empty());

        let sample = &samples[0];
        let row = &mut self.rows[self.num_rows].values;
        row[0].value_data = Some(ValueData::TimestampMillisecondValue(sample.timestamp));
        row[1].value_data = Some(ValueData::F64Value(sample.value));
        self.num_rows += 1;
        if samples.len() == 1 {
            // Fast path: only has one sample to add.
            return;
        }

        // Slow path: multiple samples to add. We need to repeat all tag values.
        // `Self::start_new_row()` ensures that the buffer >= self.num_rows.
        if self.rows.len() - self.num_rows < samples.len() - 1 {
            self.rows.resize(
                self.rows.len() + samples.len() - 1,
                Row { values: Vec::new() },
            );
        }
        let (left, right) = self.rows.split_at_mut(self.num_rows);
        for sample_idx in 1..samples.len() {
            let row = &mut right[sample_idx - 1].values;
            row.clone_from(&left[left.len() - 1].values);
            let sample = &samples[sample_idx];
            row[0].value_data = Some(ValueData::TimestampMillisecondValue(sample.timestamp));
            row[1].value_data = Some(ValueData::F64Value(sample.value));
            self.num_rows += 1;
        }
    }

    fn update_tag_value(value: &mut Value, tag_value: &str) {
        if let Some(ValueData::StringValue(existing_value)) = &mut value.value_data {
            existing_value.clear();
            existing_value.push_str(tag_value);
        } else {
            *value = Value {
                value_data: Some(ValueData::StringValue(tag_value.to_string())),
            };
        }
    }

    /// Checks if the schema still matches the current row.
    /// If the schema matches, returns true.
    /// If the schema does not match, switches to the slow mode.
    /// If labels have invalid label name, returns an error.
    fn ensure_schema(
        &mut self,
        labels: &[PromLabel],
        is_strict_mode: bool,
    ) -> Result<bool, DecodeError> {
        if self.schema.is_empty() {
            // This is an empty builder.
            self.schema.reserve(labels.len() + 2);
            self.schema.push(ColumnSchema {
                column_name: GREPTIME_TIMESTAMP.to_string(),
                datatype: ColumnDataType::TimestampMillisecond as i32,
                semantic_type: SemanticType::Timestamp as i32,
                datatype_extension: None,
                options: None,
            });
            self.schema.push(ColumnSchema {
                column_name: GREPTIME_VALUE.to_string(),
                datatype: ColumnDataType::Float64 as i32,
                semantic_type: SemanticType::Field as i32,
                datatype_extension: None,
                options: None,
            });

            for label in labels {
                let column_name = Self::bytes_to_str(&label.name, is_strict_mode)?;

                self.schema.push(ColumnSchema {
                    column_name: column_name.to_string(),
                    datatype: ColumnDataType::String as i32,
                    semantic_type: SemanticType::Tag as i32,
                    datatype_extension: None,
                    options: None,
                });
            }

            return Ok(true);
        }

        if self.num_rows == 0 {
            // This is a builder with buffer to reuse, the schema is actually empty. We can
            // reset the schema.
            self.schema.resize(
                labels.len() + 2,
                ColumnSchema {
                    column_name: String::new(),
                    datatype: ColumnDataType::String as i32,
                    semantic_type: SemanticType::Tag as i32,
                    datatype_extension: None,
                    options: None,
                },
            );
            // Now row modifier will reset timestamp and value column schema.
            self.schema[0] = ColumnSchema {
                column_name: GREPTIME_TIMESTAMP.to_string(),
                datatype: ColumnDataType::TimestampMillisecond as i32,
                semantic_type: SemanticType::Timestamp as i32,
                datatype_extension: None,
                options: None,
            };
            self.schema[1] = ColumnSchema {
                column_name: GREPTIME_VALUE.to_string(),
                datatype: ColumnDataType::Float64 as i32,
                semantic_type: SemanticType::Field as i32,
                datatype_extension: None,
                options: None,
            };
            for (label_idx, label) in labels.iter().enumerate() {
                let column_name = unsafe { std::str::from_utf8_unchecked(label.name.as_bytes()) };
                self.schema[label_idx + 2].column_name.clear();
                self.schema[label_idx + 2].column_name.push_str(column_name);
            }

            return Ok(true);
        }

        // Already has schema, checks whether the schema matches the given labels.
        if !self.has_same_schema(labels, is_strict_mode)? {
            // We already have existing schema for previous rows.
            // Switch to slow mode.
            self.switch_to_slow_mode();

            Ok(false)
        } else {
            // We have the same schema.
            Ok(true)
        }
    }

    /// Returns true if the schema matches the given labels.
    /// If labels have invalid label name, returns an error.
    fn has_same_schema(
        &self,
        labels: &[PromLabel],
        is_strict_mode: bool,
    ) -> Result<bool, DecodeError> {
        // TODO(yingwen): +2
        if labels.len() != self.schema.len() - 2 {
            return Ok(false);
        }

        for (col_idx, label) in labels.iter().enumerate() {
            let tag_name = Self::bytes_to_str(&label.name, is_strict_mode)?;
            // Skip the first two columns which are reserved for the timestamp and sample value.
            if tag_name != self.schema[col_idx + 2].column_name {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn switch_to_slow_mode(&mut self) {
        // TODO(yingwen): maybe we can reuse the strings.
        self.col_indexes.clear();
        for (col_idx, column) in self.schema.iter().enumerate() {
            self.col_indexes.insert(column.column_name.clone(), col_idx);
        }
    }

    fn bytes_to_str(bytes: &Bytes, is_strict_mode: bool) -> Result<&str, DecodeError> {
        let v = if is_strict_mode {
            match std::str::from_utf8(bytes.as_ref()) {
                Ok(s) => s,
                Err(_) => return Err(DecodeError::new("invalid utf-8")),
            }
        } else {
            unsafe { std::str::from_utf8_unchecked(bytes.as_ref()) }
        };
        Ok(v)
    }
}

/// Builder for one table.
#[derive(Debug)]
pub(crate) struct TableBuilder {
    /// Column schemas.
    schema: Vec<ColumnSchema>,
    /// Rows written.
    rows: Vec<Row>,
    /// Indices of columns inside `schema`.
    col_indexes: HashMap<String, usize>,
}

impl Default for TableBuilder {
    fn default() -> Self {
        Self::with_capacity(2, 0)
    }
}

impl TableBuilder {
    pub(crate) fn with_capacity(cols: usize, rows: usize) -> Self {
        let mut col_indexes = HashMap::with_capacity_and_hasher(cols, Default::default());
        col_indexes.insert(GREPTIME_TIMESTAMP.to_string(), 0);
        col_indexes.insert(GREPTIME_VALUE.to_string(), 1);

        let mut schema = Vec::with_capacity(cols);
        schema.push(ColumnSchema {
            column_name: GREPTIME_TIMESTAMP.to_string(),
            datatype: ColumnDataType::TimestampMillisecond as i32,
            semantic_type: SemanticType::Timestamp as i32,
            datatype_extension: None,
            options: None,
        });

        schema.push(ColumnSchema {
            column_name: GREPTIME_VALUE.to_string(),
            datatype: ColumnDataType::Float64 as i32,
            semantic_type: SemanticType::Field as i32,
            datatype_extension: None,
            options: None,
        });

        Self {
            schema,
            rows: Vec::with_capacity(rows),
            col_indexes,
        }
    }

    /// Total number of rows inside table builder.
    fn num_rows(&self) -> usize {
        self.rows.len()
    }

    /// Adds a set of labels and samples to table builder.
    pub(crate) fn add_labels_and_samples(
        &mut self,
        labels: &[PromLabel],
        samples: &[Sample],
        is_strict_mode: bool,
    ) -> Result<(), DecodeError> {
        let mut row = vec![Value { value_data: None }; self.col_indexes.len()];

        for PromLabel { name, value } in labels {
            let (tag_name, tag_value) = if is_strict_mode {
                let tag_name = match String::from_utf8(name.to_vec()) {
                    Ok(s) => s,
                    Err(_) => return Err(DecodeError::new("invalid utf-8")),
                };
                let tag_value = match String::from_utf8(value.to_vec()) {
                    Ok(s) => s,
                    Err(_) => return Err(DecodeError::new("invalid utf-8")),
                };
                (tag_name, tag_value)
            } else {
                let tag_name = unsafe { String::from_utf8_unchecked(name.to_vec()) };
                let tag_value = unsafe { String::from_utf8_unchecked(value.to_vec()) };
                (tag_name, tag_value)
            };

            let tag_value = Some(ValueData::StringValue(tag_value));
            let tag_num = self.col_indexes.len();

            match self.col_indexes.entry(tag_name) {
                Entry::Occupied(e) => {
                    row[*e.get()].value_data = tag_value;
                }
                Entry::Vacant(e) => {
                    let column_name = e.key().clone();
                    e.insert(tag_num);
                    self.schema.push(ColumnSchema {
                        column_name,
                        datatype: ColumnDataType::String as i32,
                        semantic_type: SemanticType::Tag as i32,
                        datatype_extension: None,
                        options: None,
                    });
                    row.push(Value {
                        value_data: tag_value,
                    });
                }
            }
        }

        if samples.len() == 1 {
            let sample = &samples[0];
            row[0].value_data = Some(ValueData::TimestampMillisecondValue(sample.timestamp));
            row[1].value_data = Some(ValueData::F64Value(sample.value));
            self.rows.push(Row { values: row });
            return Ok(());
        }
        for sample in samples {
            row[0].value_data = Some(ValueData::TimestampMillisecondValue(sample.timestamp));
            row[1].value_data = Some(ValueData::F64Value(sample.value));
            self.rows.push(Row {
                values: row.clone(),
            });
        }

        Ok(())
    }

    /// Converts [TableBuilder] to [RowInsertRequest] and clears buffered data.
    pub(crate) fn as_row_insert_request(&mut self, table_name: String) -> RowInsertRequest {
        let mut rows = std::mem::take(&mut self.rows);
        let schema = std::mem::take(&mut self.schema);
        let col_num = schema.len();
        for row in &mut rows {
            if row.values.len() < col_num {
                row.values.resize(col_num, Value { value_data: None });
            }
        }

        RowInsertRequest {
            table_name,
            rows: Some(Rows { schema, rows }),
        }
    }
}

#[cfg(test)]
mod tests {
    use api::prom_store::remote::Sample;
    use api::v1::value::ValueData;
    use api::v1::{Row, Value};
    use arrow::datatypes::ToByteSlice;
    use bytes::Bytes;
    use prost::DecodeError;

    use crate::prom_row_builder::{PoolTableBuilder, TableBuilder};
    use crate::proto::PromLabel;

    #[test]
    fn test_table_builder() {
        let mut builder = TableBuilder::default();
        let is_strict_mode = true;
        let _ = builder.add_labels_and_samples(
            &[
                PromLabel {
                    name: Bytes::from("tag0"),
                    value: Bytes::from("v0"),
                },
                PromLabel {
                    name: Bytes::from("tag1"),
                    value: Bytes::from("v1"),
                },
            ],
            &[Sample {
                value: 0.0,
                timestamp: 0,
            }],
            is_strict_mode,
        );

        let _ = builder.add_labels_and_samples(
            &[
                PromLabel {
                    name: Bytes::from("tag0"),
                    value: Bytes::from("v0"),
                },
                PromLabel {
                    name: Bytes::from("tag2"),
                    value: Bytes::from("v2"),
                },
            ],
            &[Sample {
                value: 0.1,
                timestamp: 1,
            }],
            is_strict_mode,
        );

        let request = builder.as_row_insert_request("test".to_string());
        let rows = request.rows.unwrap().rows;
        assert_eq!(2, rows.len());

        assert_eq!(
            vec![
                Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(0))
                },
                Value {
                    value_data: Some(ValueData::F64Value(0.0))
                },
                Value {
                    value_data: Some(ValueData::StringValue("v0".to_string()))
                },
                Value {
                    value_data: Some(ValueData::StringValue("v1".to_string()))
                },
                Value { value_data: None },
            ],
            rows[0].values
        );

        assert_eq!(
            vec![
                Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(1))
                },
                Value {
                    value_data: Some(ValueData::F64Value(0.1))
                },
                Value {
                    value_data: Some(ValueData::StringValue("v0".to_string()))
                },
                Value { value_data: None },
                Value {
                    value_data: Some(ValueData::StringValue("v2".to_string()))
                },
            ],
            rows[1].values
        );

        let invalid_utf8_bytes = &[0xFF, 0xFF, 0xFF];

        let res = builder.add_labels_and_samples(
            &[PromLabel {
                name: Bytes::from("tag0"),
                value: invalid_utf8_bytes.to_byte_slice().into(),
            }],
            &[Sample {
                value: 0.1,
                timestamp: 1,
            }],
            is_strict_mode,
        );
        assert_eq!(res, Err(DecodeError::new("invalid utf-8")));
    }

    #[test]
    fn test_pool_table_builder_diff_schema() {
        common_telemetry::init_default_ut_logging();

        let mut builder = PoolTableBuilder::with_capacity(4, 2);
        let is_strict_mode = true;
        let _ = builder.add_labels_and_samples(
            &[
                PromLabel {
                    name: Bytes::from("tag0"),
                    value: Bytes::from("v0"),
                },
                PromLabel {
                    name: Bytes::from("tag1"),
                    value: Bytes::from("v1"),
                },
            ],
            &[Sample {
                value: 0.0,
                timestamp: 0,
            }],
            is_strict_mode,
        );

        let _ = builder.add_labels_and_samples(
            &[
                PromLabel {
                    name: Bytes::from("tag0"),
                    value: Bytes::from("v0"),
                },
                PromLabel {
                    name: Bytes::from("tag2"),
                    value: Bytes::from("v2"),
                },
            ],
            &[Sample {
                value: 0.1,
                timestamp: 1,
            }],
            is_strict_mode,
        );

        let request = builder.as_row_insert_request("test".to_string());
        let rows = request.rows.unwrap();
        assert_eq!(5, rows.schema.len());
        assert_eq!(2, rows.rows.len());

        assert_eq!(
            vec![
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(0))
                        },
                        Value {
                            value_data: Some(ValueData::F64Value(0.0))
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("v0".to_string()))
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("v1".to_string()))
                        },
                        Value { value_data: None },
                    ]
                },
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(1))
                        },
                        Value {
                            value_data: Some(ValueData::F64Value(0.1))
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("v0".to_string()))
                        },
                        Value { value_data: None },
                        Value {
                            value_data: Some(ValueData::StringValue("v2".to_string()))
                        },
                    ],
                }
            ],
            rows.rows
        );

        let invalid_utf8_bytes = &[0xFF, 0xFF, 0xFF];

        let res = builder.add_labels_and_samples(
            &[PromLabel {
                name: Bytes::from("tag0"),
                value: invalid_utf8_bytes.to_byte_slice().into(),
            }],
            &[Sample {
                value: 0.1,
                timestamp: 1,
            }],
            is_strict_mode,
        );
        assert_eq!(res, Err(DecodeError::new("invalid utf-8")));
    }

    #[test]
    fn test_pool_table_builder_same_schema() {
        common_telemetry::init_default_ut_logging();

        let mut builder = PoolTableBuilder::with_capacity(4, 2);
        let is_strict_mode = true;
        let _ = builder.add_labels_and_samples(
            &[
                PromLabel {
                    name: Bytes::from("tag0"),
                    value: Bytes::from("v0"),
                },
                PromLabel {
                    name: Bytes::from("tag1"),
                    value: Bytes::from("v1"),
                },
            ],
            &[Sample {
                value: 0.0,
                timestamp: 0,
            }],
            is_strict_mode,
        );

        let _ = builder.add_labels_and_samples(
            &[
                PromLabel {
                    name: Bytes::from("tag0"),
                    value: Bytes::from("v0"),
                },
                PromLabel {
                    name: Bytes::from("tag1"),
                    value: Bytes::from("v2"),
                },
            ],
            &[Sample {
                value: 0.1,
                timestamp: 1,
            }],
            is_strict_mode,
        );

        let request = builder.as_row_insert_request("test".to_string());
        let rows = request.rows.unwrap();
        assert_eq!(4, rows.schema.len());
        assert_eq!(2, rows.rows.len());

        assert_eq!(
            vec![
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(0))
                        },
                        Value {
                            value_data: Some(ValueData::F64Value(0.0))
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("v0".to_string()))
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("v1".to_string()))
                        },
                    ],
                },
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(1))
                        },
                        Value {
                            value_data: Some(ValueData::F64Value(0.1))
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("v0".to_string()))
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("v2".to_string()))
                        },
                    ],
                }
            ],
            rows.rows
        );
    }

    #[test]
    fn test_pool_table_builder_reuse_less() {
        common_telemetry::init_default_ut_logging();

        let mut builder = PoolTableBuilder::with_capacity(4, 2);
        let is_strict_mode = true;
        let _ = builder.add_labels_and_samples(
            &[
                PromLabel {
                    name: Bytes::from("tag0"),
                    value: Bytes::from("v0"),
                },
                PromLabel {
                    name: Bytes::from("tag1"),
                    value: Bytes::from("v1"),
                },
            ],
            &[Sample {
                value: 0.0,
                timestamp: 0,
            }],
            is_strict_mode,
        );

        let _ = builder.add_labels_and_samples(
            &[
                PromLabel {
                    name: Bytes::from("tag0"),
                    value: Bytes::from("v0"),
                },
                PromLabel {
                    name: Bytes::from("tag1"),
                    value: Bytes::from("v2"),
                },
            ],
            &[Sample {
                value: 0.1,
                timestamp: 1,
            }],
            is_strict_mode,
        );

        let request = builder.as_row_insert_request("test".to_string());
        let rows = request.rows.unwrap();
        assert_eq!(4, rows.schema.len());
        assert_eq!(2, rows.rows.len());

        let mut builder = PoolTableBuilder::from_rows(rows);
        let _ = builder.add_labels_and_samples(
            &[PromLabel {
                name: Bytes::from("tag00"),
                value: Bytes::from("v0"),
            }],
            &[Sample {
                value: 0.0,
                timestamp: 2,
            }],
            is_strict_mode,
        );

        let request = builder.as_row_insert_request("test".to_string());
        let rows = request.rows.unwrap();
        assert_eq!(3, rows.schema.len());
        let column_names: Vec<_> = rows
            .schema
            .iter()
            .map(|col| col.column_name.clone())
            .collect();
        assert_eq!(
            column_names,
            vec!["greptime_timestamp", "greptime_value", "tag00"]
        );
        assert_eq!(1, rows.rows.len());

        assert_eq!(
            vec![Row {
                values: vec![
                    Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(2))
                    },
                    Value {
                        value_data: Some(ValueData::F64Value(0.0))
                    },
                    Value {
                        value_data: Some(ValueData::StringValue("v0".to_string()))
                    },
                ],
            }],
            rows.rows
        );
    }

    #[test]
    fn test_pool_table_builder_reuse_partial() {
        common_telemetry::init_default_ut_logging();

        let mut builder = PoolTableBuilder::with_capacity(4, 2);
        let is_strict_mode = true;
        let _ = builder.add_labels_and_samples(
            &[PromLabel {
                name: Bytes::from("tag0"),
                value: Bytes::from("v0"),
            }],
            &[Sample {
                value: 0.0,
                timestamp: 0,
            }],
            is_strict_mode,
        );

        let _ = builder.add_labels_and_samples(
            &[PromLabel {
                name: Bytes::from("tag0"),
                value: Bytes::from("v0"),
            }],
            &[Sample {
                value: 0.1,
                timestamp: 1,
            }],
            is_strict_mode,
        );

        let request = builder.as_row_insert_request("test".to_string());
        let rows = request.rows.unwrap();
        assert_eq!(3, rows.schema.len());
        assert_eq!(2, rows.rows.len());

        let mut builder = PoolTableBuilder::from_rows(rows);
        let _ = builder.add_labels_and_samples(
            &[
                PromLabel {
                    name: Bytes::from("tag00"),
                    value: Bytes::from("v00"),
                },
                PromLabel {
                    name: Bytes::from("tag01"),
                    value: Bytes::from("v01"),
                },
            ],
            &[Sample {
                value: 0.0,
                timestamp: 2,
            }],
            is_strict_mode,
        );

        let _ = builder.add_labels_and_samples(
            &[
                PromLabel {
                    name: Bytes::from("tag00"),
                    value: Bytes::from("v02"),
                },
                PromLabel {
                    name: Bytes::from("tag01"),
                    value: Bytes::from("v03"),
                },
            ],
            &[Sample {
                value: 0.1,
                timestamp: 3,
            }],
            is_strict_mode,
        );
        let request = builder.as_row_insert_request("test".to_string());
        let rows = request.rows.unwrap();
        assert_eq!(4, rows.schema.len());
        let column_names: Vec<_> = rows
            .schema
            .iter()
            .map(|col| col.column_name.clone())
            .collect();
        assert_eq!(
            column_names,
            vec!["greptime_timestamp", "greptime_value", "tag00", "tag01"]
        );
        assert_eq!(2, rows.rows.len());

        assert_eq!(
            vec![
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(2))
                        },
                        Value {
                            value_data: Some(ValueData::F64Value(0.0))
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("v00".to_string()))
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("v01".to_string()))
                        },
                    ],
                },
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(3))
                        },
                        Value {
                            value_data: Some(ValueData::F64Value(0.1))
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("v02".to_string()))
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("v03".to_string()))
                        },
                    ],
                }
            ],
            rows.rows
        );
    }
}
