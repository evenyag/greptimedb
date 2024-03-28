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

//! Rewrite SST files.

use std::fs::File;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common_datasource::file_format::parquet::BufferedWriter;
use datatypes::arrow::array::{ArrayRef, DictionaryArray, UInt32Array};
use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema, SchemaRef};
use datatypes::arrow::record_batch::{RecordBatch, RecordBatchReader};
use datatypes::data_type::DataType;
use datatypes::vectors::{MutableVector, Vector};
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::{OP_TYPE_COLUMN_NAME, SEQUENCE_COLUMN_NAME};

use crate::error::{ReadParquetSnafu, Result, WriteBufferSnafu};
use crate::read::{Batch, BatchReader};
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};
use crate::sst::parquet::reader::{ParquetReader, ParquetReaderBuilder};
use crate::sst::parquet::{DEFAULT_READ_BATCH_SIZE, DEFAULT_ROW_GROUP_SIZE};
use crate::sst::split_kv::new_file_handle;
use crate::sst::{DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_CONCURRENCY};

/// A writer that decodes primary key columns and stores them separately.
pub struct SplitPkWriter {
    path: String,

    batch_size: usize,
    row_group_size: usize,
    tag_use_dictionary: bool,
}

impl SplitPkWriter {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            batch_size: DEFAULT_READ_BATCH_SIZE,
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            tag_use_dictionary: false,
        }
    }

    pub async fn write_to_store(
        &self,
        mut reader: ParquetReader,
        store: &ObjectStore,
    ) -> Result<WriterMetrics> {
        let mut metrics = WriterMetrics::default();

        let metadata = reader.metadata().clone();
        let codec = McmpRowCodec::new(
            metadata
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        );
        let schema_with_tags = self.new_schema_with_tags(&metadata);

        let mut writer = self
            .new_buffered_writer_with_schema(store, schema_with_tags.clone(), &metadata)
            .await?;
        let mut builders: Vec<_> = metadata
            .primary_key_columns()
            .map(|meta| {
                meta.column_schema
                    .data_type
                    .create_mutable_vector(self.batch_size)
            })
            .collect();

        while let Some(batch) = reader.next_batch().await? {
            metrics.num_batches += 1;
            metrics.num_rows += batch.num_rows();
            let convert_start = Instant::now();
            let record_batch = Self::batch_to_record_batch(
                &batch,
                &codec,
                &metadata,
                &mut builders,
                schema_with_tags.clone(),
                self.tag_use_dictionary,
            );
            metrics.convert_cost += convert_start.elapsed();
            let write_start = Instant::now();
            writer
                .write(&record_batch)
                .await
                .context(WriteBufferSnafu)?;
            metrics.write_cost += write_start.elapsed();
        }
        let write_start = Instant::now();
        let (_, file_size) = writer.close().await.context(WriteBufferSnafu)?;
        metrics.file_size = file_size as usize;
        metrics.write_cost += write_start.elapsed();

        Ok(metrics)
    }

    async fn new_buffered_writer_with_schema(
        &self,
        store: &ObjectStore,
        schema: SchemaRef,
        metadata: &RegionMetadataRef,
    ) -> Result<BufferedWriter> {
        BufferedWriter::try_new(
            self.path.clone(),
            store.clone(),
            schema,
            Some(self.new_writer_props(metadata)),
            DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize,
            DEFAULT_WRITE_CONCURRENCY,
        )
        .await
        .context(WriteBufferSnafu)
    }

    fn new_writer_props(&self, metadata: &RegionMetadataRef) -> WriterProperties {
        let ts_col = ColumnPath::new(vec![metadata
            .time_index_column()
            .column_schema
            .name
            .clone()]);
        let seq_col = ColumnPath::new(vec![SEQUENCE_COLUMN_NAME.to_string()]);

        WriterProperties::builder()
            .set_max_row_group_size(self.row_group_size)
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_column_encoding(seq_col.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(seq_col, false)
            .set_column_encoding(ts_col.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(ts_col, false)
            .build()
    }

    fn new_schema_with_tags(&self, metadata: &RegionMetadataRef) -> SchemaRef {
        let mut fields = Vec::with_capacity(metadata.column_metadatas.len() + 2);
        // Primary keys.
        for column_metadata in metadata.primary_key_columns() {
            if self.tag_use_dictionary && column_metadata.column_schema.data_type.is_string() {
                fields.push(Field::new_dictionary(
                    &column_metadata.column_schema.name,
                    ArrowDataType::UInt32,
                    column_metadata.column_schema.data_type.as_arrow_type(),
                    column_metadata.column_schema.is_nullable(),
                ));
            } else {
                fields.push(
                    metadata
                        .schema
                        .arrow_schema()
                        .field_with_name(&column_metadata.column_schema.name)
                        .unwrap()
                        .clone(),
                );
            }
        }
        // Fields.
        for column_metadata in metadata.field_columns() {
            fields.push(
                metadata
                    .schema
                    .arrow_schema()
                    .field_with_name(&column_metadata.column_schema.name)
                    .unwrap()
                    .clone(),
            );
        }
        // time index
        fields.push(
            metadata
                .schema
                .arrow_schema()
                .field_with_name(&metadata.time_index_column().column_schema.name)
                .unwrap()
                .clone(),
        );
        fields.extend(Self::internal_fields());

        let fields = Fields::from(fields);
        Arc::new(Schema::new(fields))
    }

    fn internal_fields() -> [Field; 2] {
        [
            Field::new(SEQUENCE_COLUMN_NAME, ArrowDataType::UInt64, false),
            Field::new(OP_TYPE_COLUMN_NAME, ArrowDataType::UInt8, false),
        ]
    }

    fn batch_to_record_batch(
        batch: &Batch,
        codec: &McmpRowCodec,
        metadata: &RegionMetadataRef,
        builders: &mut [Box<dyn MutableVector>],
        schema: SchemaRef,
        tag_use_dictionary: bool,
    ) -> RecordBatch {
        let columns = Self::batch_to_arrays(batch, codec, metadata, builders, tag_use_dictionary);

        RecordBatch::try_new(schema.clone(), columns).unwrap()
    }

    fn batch_to_arrays(
        batch: &Batch,
        codec: &McmpRowCodec,
        metadata: &RegionMetadataRef,
        builders: &mut [Box<dyn MutableVector>],
        tag_use_dictionary: bool,
    ) -> Vec<ArrayRef> {
        let tags = codec.decode(batch.primary_key()).unwrap();
        for (value, builder) in tags.into_iter().zip(builders.iter_mut()) {
            for _ in 0..batch.num_rows() {
                builder.push_value_ref(value.as_value_ref());
            }
        }

        let mut arrays = Vec::with_capacity(metadata.column_metadatas.len() + 2);
        // tags
        for builder in builders {
            let tag_array = builder.to_vector().to_arrow_array();
            if tag_use_dictionary {
                let key_array = UInt32Array::from_iter_values(0..tag_array.len() as u32);
                let dictionary_array: ArrayRef =
                    Arc::new(DictionaryArray::new(key_array, tag_array));
                arrays.push(dictionary_array);
            } else {
                arrays.push(tag_array);
            }
        }
        // fields
        for column in batch.fields() {
            arrays.push(column.data.to_arrow_array());
        }
        // time index
        arrays.push(batch.timestamps().to_arrow_array());
        arrays.push(batch.sequences().to_arrow_array());
        arrays.push(batch.op_types().to_arrow_array());

        arrays
    }
}

/// Metrics for writing a SST file.
#[derive(Debug, Default)]
pub struct WriterMetrics {
    /// Number of batches.
    pub num_batches: usize,
    /// Number of rows.
    pub num_rows: usize,
    /// Output file size.
    pub file_size: usize,
    /// Duration to convert batches.
    pub convert_cost: Duration,
    /// Duration to write the output file.
    pub write_cost: Duration,
}

/// Split primary keys in a file.
pub async fn split_key(
    input_dir: &str,
    file_id: &str,
    output_path: &str,
    object_store: &ObjectStore,
) -> Result<WriterMetrics> {
    let file_handle = new_file_handle(file_id)?;
    let reader =
        ParquetReaderBuilder::new(input_dir.to_string(), file_handle, object_store.clone())
            .build()
            .await?;
    let writer = SplitPkWriter::new(output_path);

    writer.write_to_store(reader, object_store).await
}

/// A writer that rewrites a parquet file.
pub struct ParquetRewriter {
    path: String,

    row_group_size: usize,
    tag_use_dictionary: bool,
}

impl ParquetRewriter {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            tag_use_dictionary: false,
        }
    }

    pub fn with_tag_use_dictionary(mut self, tag_use_dictionary: bool) -> Self {
        self.tag_use_dictionary = tag_use_dictionary;
        self
    }

    pub async fn write_to_store(
        &self,
        mut reader: ParquetRecordBatchReader,
        store: &ObjectStore,
    ) -> Result<WriterMetrics> {
        let mut metrics = WriterMetrics::default();

        let schema = reader.schema();
        let schema_with_tags = self.new_schema_with_tags(&schema);

        let mut writer = self
            .new_buffered_writer_with_schema(store, &schema_with_tags)
            .await?;
        while let Some(batch) = reader.next() {
            let batch = batch.unwrap();
            metrics.num_batches += 1;
            metrics.num_rows += batch.num_rows();
            let convert_start = Instant::now();
            let record_batch =
                Self::convert_record_batch(&schema_with_tags, &batch, self.tag_use_dictionary);
            metrics.convert_cost += convert_start.elapsed();
            let write_start = Instant::now();
            writer
                .write(&record_batch)
                .await
                .context(WriteBufferSnafu)?;
            metrics.write_cost += write_start.elapsed();
        }
        let write_start = Instant::now();
        let (_, file_size) = writer.close().await.context(WriteBufferSnafu)?;
        metrics.file_size = file_size as usize;
        metrics.write_cost += write_start.elapsed();

        Ok(metrics)
    }

    async fn new_buffered_writer_with_schema(
        &self,
        store: &ObjectStore,
        schema: &SchemaRef,
    ) -> Result<BufferedWriter> {
        BufferedWriter::try_new(
            self.path.clone(),
            store.clone(),
            schema.clone(),
            Some(self.new_writer_props(&schema)),
            DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize,
            DEFAULT_WRITE_CONCURRENCY,
        )
        .await
        .context(WriteBufferSnafu)
    }

    fn new_writer_props(&self, schema: &SchemaRef) -> WriterProperties {
        let ts_column = schema
            .fields
            .iter()
            .find(|field| matches!(field.data_type(), ArrowDataType::Timestamp(_, _)))
            .unwrap();
        let ts_col = ColumnPath::new(vec![ts_column.name().to_string()]);
        let seq_col = ColumnPath::new(vec![SEQUENCE_COLUMN_NAME.to_string()]);

        WriterProperties::builder()
            .set_max_row_group_size(self.row_group_size)
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_column_encoding(seq_col.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(seq_col, false)
            .set_column_encoding(ts_col.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(ts_col, false)
            .build()
    }

    fn new_schema_with_tags(&self, schema: &SchemaRef) -> SchemaRef {
        let mut fields = Vec::with_capacity(schema.fields.len());
        for field in &schema.fields {
            if self.tag_use_dictionary
                && (*field.data_type() == ArrowDataType::Utf8
                    || *field.data_type() == ArrowDataType::LargeUtf8)
            {
                fields.push(Arc::new(Field::new_dictionary(
                    field.name(),
                    ArrowDataType::UInt32,
                    field.data_type().clone(),
                    field.is_nullable(),
                )));
            } else {
                fields.push(field.clone());
            }
        }

        let fields = Fields::from(fields);
        Arc::new(Schema::new(fields))
    }

    fn convert_record_batch(
        schema: &SchemaRef,
        batch: &RecordBatch,
        tag_use_dictionary: bool,
    ) -> RecordBatch {
        let mut columns = Vec::with_capacity(batch.num_columns());
        for column in batch.columns() {
            if tag_use_dictionary
                && (*column.data_type() == ArrowDataType::Utf8
                    || *column.data_type() == ArrowDataType::LargeUtf8)
            {
                let key_array = UInt32Array::from_iter_values(0..column.len() as u32);
                let dictionary_array: ArrayRef =
                    Arc::new(DictionaryArray::new(key_array, column.clone()));
                columns.push(dictionary_array);
            } else {
                columns.push(column.clone());
            }
        }

        RecordBatch::try_new(schema.clone(), columns).unwrap()
    }
}

/// Rewrites a file.
pub async fn rewrite_file(
    input_path: &str,
    output_path: &str,
    tag_use_dictionary: bool,
    object_store: &ObjectStore,
) -> Result<WriterMetrics> {
    let file = File::open(input_path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .context(ReadParquetSnafu { path: input_path })?;
    let writer = ParquetRewriter::new(output_path).with_tag_use_dictionary(tag_use_dictionary);

    writer.write_to_store(reader, object_store).await
}
