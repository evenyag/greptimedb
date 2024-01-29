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

//! Split key values.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use api::v1::SemanticType;
use common_datasource::file_format::parquet::BufferedWriter;
use common_time::Timestamp;
use datatypes::arrow::array::{ArrayRef, BinaryArray, UInt64Array};
use datatypes::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::vectors::Vector;
use object_store::ObjectStore;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::{
    OP_TYPE_COLUMN_NAME, PRIMARY_KEY_COLUMN_NAME, SEQUENCE_COLUMN_NAME,
};

use crate::error::{InvalidParquetSnafu, NewRecordBatchSnafu, Result, WriteBufferSnafu};
use crate::read::{Batch, BatchReader};
use crate::sst::file::{FileHandle, FileId, FileMeta};
use crate::sst::file_purger::{FilePurger, FilePurgerRef, PurgeRequest};
use crate::sst::parquet::reader::ParquetReaderBuilder;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::{DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_CONCURRENCY};

const PK_ROW_GROUP_SIZE: usize = 8192;
const DATA_ROW_GROUP_SIZE: usize = 102400;
const PKID_COLUMN_NAME: &str = "__pkid";

type PkId = u64;

/// Row value in a primary key file.
struct PkFileRowValue {
    /// Id for the primary key.
    pkid: PkId,
}

/// A writer to write a primary key file from batches.
struct PrimaryKeyFileWriter {
    path: String,
    primary_keys: BTreeMap<Vec<u8>, PkFileRowValue>,
    next_pkid: PkId,
    schema: SchemaRef,

    batch_size: usize,
    row_group_size: usize,
}

impl PrimaryKeyFileWriter {
    fn new(path: &str) -> PrimaryKeyFileWriter {
        PrimaryKeyFileWriter {
            path: path.to_string(),
            primary_keys: BTreeMap::new(),
            next_pkid: 0,
            schema: Self::new_schema(),
            batch_size: DEFAULT_READ_BATCH_SIZE,
            row_group_size: PK_ROW_GROUP_SIZE,
        }
    }

    fn add_primary_key(&mut self, key: &[u8]) {
        if self.primary_keys.contains_key(key) {
            return;
        }

        let pkid = self.allocate_pk_id();
        let value = PkFileRowValue { pkid };
        self.primary_keys.insert(key.to_vec(), value);
    }

    fn allocate_pk_id(&mut self) -> PkId {
        let next = self.next_pkid;
        self.next_pkid += 1;
        next
    }

    async fn write_to_store(&self, store: &ObjectStore) -> Result<PkWriterMetrics> {
        let mut metrics = PkWriterMetrics {
            num_pk: self.primary_keys.len(),
            ..Default::default()
        };

        let mut writer = self.new_buffered_writer(store).await?;

        let key_values: Vec<_> = self.primary_keys.iter().collect();
        for kv_batch in key_values.chunks(self.batch_size) {
            let key_bytes: usize = kv_batch.iter().map(|kv| kv.0.len()).sum();
            metrics.pk_bytes += key_bytes;

            let record_batch = self.kv_batch_to_record_batch(kv_batch);
            writer
                .write(&record_batch)
                .await
                .context(WriteBufferSnafu)?;
        }
        let (_, file_size) = writer.close().await.context(WriteBufferSnafu)?;
        metrics.file_size = file_size as usize;

        Ok(metrics)
    }

    async fn new_buffered_writer(&self, store: &ObjectStore) -> Result<BufferedWriter> {
        BufferedWriter::try_new(
            self.path.clone(),
            store.clone(),
            self.schema.clone(),
            Some(self.new_writer_props()),
            DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize,
            DEFAULT_WRITE_CONCURRENCY,
        )
        .await
        .context(WriteBufferSnafu)
    }

    fn new_writer_props(&self) -> WriterProperties {
        let pk_column = ColumnPath::new(vec![PRIMARY_KEY_COLUMN_NAME.to_string()]);
        let pkid_column = ColumnPath::new(vec![PKID_COLUMN_NAME.to_string()]);

        WriterProperties::builder()
            .set_max_row_group_size(self.row_group_size)
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_column_dictionary_enabled(pk_column, false)
            .set_column_dictionary_enabled(pkid_column.clone(), false)
            .set_column_encoding(pkid_column, Encoding::DELTA_BINARY_PACKED)
            .build()
    }

    fn new_schema() -> SchemaRef {
        let fields = Fields::from(vec![
            Field::new(PRIMARY_KEY_COLUMN_NAME, DataType::Binary, false),
            Field::new(PKID_COLUMN_NAME, DataType::UInt64, false),
        ]);
        Arc::new(Schema::new(fields))
    }

    fn kv_batch_to_record_batch(&self, kv_batch: &[(&Vec<u8>, &PkFileRowValue)]) -> RecordBatch {
        let pk_array = BinaryArray::from_iter_values(kv_batch.iter().map(|kv| kv.0.as_slice()));
        let pkid_array = UInt64Array::from_iter_values(kv_batch.iter().map(|kv| kv.1.pkid));
        let columns = vec![Arc::new(pk_array) as _, Arc::new(pkid_array) as _];

        RecordBatch::try_new(self.schema.clone(), columns).unwrap()
    }
}

/// Metrics for writing the primary key file.
#[derive(Debug, Default)]
pub struct PkWriterMetrics {
    /// Number of primary keys.
    pub num_pk: usize,
    /// Total bytes of primary keys.
    pub pk_bytes: usize,
    /// Output file size.
    pub file_size: usize,
}

/// Metrics for writing the data file.
#[derive(Debug, Default)]
pub struct DataWriterMetrics {
    /// Number of primary keys.
    pub num_pk: usize,
    /// Number of rows.
    pub num_rows: usize,
    /// Output file size.
    pub file_size: usize,
}

/// A writer to write a data file from batches.
struct DataFileWriter {
    sst_schema: SchemaRef,
    writer: BufferedWriter,
    last_primary_key: Option<Vec<u8>>,
    current_pkid: PkId,

    metrics: DataWriterMetrics,
}

impl DataFileWriter {
    async fn new(
        path: &str,
        metadata: &RegionMetadataRef,
        object_store: &ObjectStore,
        props: Option<WriterProperties>,
    ) -> Result<DataFileWriter> {
        let sst_schema = Self::to_sst_schema(&metadata);
        let writer = BufferedWriter::try_new(
            path.to_string(),
            object_store.clone(),
            sst_schema.clone(),
            props,
            DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize,
            DEFAULT_WRITE_CONCURRENCY,
        )
        .await
        .context(WriteBufferSnafu)?;

        Ok(DataFileWriter {
            sst_schema,
            writer,
            last_primary_key: None,
            current_pkid: 0,
            metrics: DataWriterMetrics::default(),
        })
    }

    async fn write_data(&mut self, batch: &Batch) -> Result<()> {
        let pkid = self.get_or_bump_pkid(batch.primary_key());
        let record_batch = self.convert_batch(pkid, batch)?;
        self.metrics.num_rows += record_batch.num_rows();

        self.writer
            .write(&record_batch)
            .await
            .context(WriteBufferSnafu)
    }

    async fn finish(mut self) -> Result<DataWriterMetrics> {
        let (_, file_size) = self.writer.close().await.context(WriteBufferSnafu)?;
        self.metrics.file_size = file_size as usize;

        Ok(self.metrics)
    }

    fn convert_batch(&self, pkid: PkId, batch: &Batch) -> Result<RecordBatch> {
        // Store all fields. (time index, pkid, seq, op type)
        let mut columns = Vec::with_capacity(batch.fields().len() + 4);
        // Store all fields first.
        for column in batch.fields().iter() {
            // TODO(yingwen): validate column id.
            columns.push(column.data.to_arrow_array());
        }
        // Add time index column.
        columns.push(batch.timestamps().to_arrow_array());
        // Add internal columns: pkid, sequences, op types.
        columns.push(Self::new_pkid_array(pkid, batch.num_rows()));
        columns.push(batch.sequences().to_arrow_array());
        columns.push(batch.op_types().to_arrow_array());

        RecordBatch::try_new(self.sst_schema.clone(), columns).context(NewRecordBatchSnafu)
    }

    fn get_or_bump_pkid(&mut self, pk: &[u8]) -> PkId {
        let Some(current_key) = &self.last_primary_key else {
            self.last_primary_key = Some(pk.to_vec());
            self.metrics.num_pk += 1;
            return self.current_pkid;
        };

        if current_key == pk {
            return self.current_pkid;
        }

        assert!(pk > current_key.as_slice());
        self.last_primary_key = Some(pk.to_vec());
        self.current_pkid += 1;
        self.metrics.num_pk += 1;

        self.current_pkid
    }

    fn to_sst_schema(metadata: &RegionMetadataRef) -> SchemaRef {
        let fields = Fields::from_iter(
            metadata
                .schema
                .arrow_schema()
                .fields()
                .iter()
                .zip(&metadata.column_metadatas)
                .filter_map(|(field, column_meta)| {
                    if column_meta.semantic_type == SemanticType::Field {
                        Some(field.clone())
                    } else {
                        // We have fixed positions for tags (primary key) and time index.
                        None
                    }
                })
                .chain([metadata.time_index_field()])
                .chain(Self::internal_fields()),
        );

        Arc::new(Schema::new(fields))
    }

    /// Fields for internal columns.
    fn internal_fields() -> [FieldRef; 3] {
        // Internal columns are always not null.
        [
            Arc::new(Field::new(PKID_COLUMN_NAME, DataType::UInt64, false)),
            Arc::new(Field::new(SEQUENCE_COLUMN_NAME, DataType::UInt64, false)),
            Arc::new(Field::new(OP_TYPE_COLUMN_NAME, DataType::UInt8, false)),
        ]
    }

    fn new_pkid_array(pkid: PkId, num_rows: usize) -> ArrayRef {
        Arc::new(UInt64Array::from_iter_values(
            std::iter::repeat(pkid).take(num_rows),
        ))
    }

    fn new_writer_props(metadata: &RegionMetadataRef) -> WriterProperties {
        let pkid_column = ColumnPath::new(vec![PKID_COLUMN_NAME.to_string()]);
        let ts_col = ColumnPath::new(vec![metadata
            .time_index_column()
            .column_schema
            .name
            .clone()]);
        let seq_col = ColumnPath::new(vec![SEQUENCE_COLUMN_NAME.to_string()]);

        WriterProperties::builder()
            .set_max_row_group_size(DATA_ROW_GROUP_SIZE)
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_column_dictionary_enabled(pkid_column.clone(), false)
            .set_column_encoding(pkid_column, Encoding::DELTA_BINARY_PACKED)
            .set_column_encoding(seq_col.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(seq_col, false)
            .set_column_encoding(ts_col.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(ts_col, false)
            .build()
    }
}

#[derive(Debug)]
struct NoopFilePurger;

impl FilePurger for NoopFilePurger {
    fn send_request(&self, _request: PurgeRequest) {}
}

fn new_noop_file_purger() -> FilePurgerRef {
    Arc::new(NoopFilePurger {})
}

/// Creates a mock file handle to converting files.
fn new_file_handle(file_id: &str) -> Result<FileHandle> {
    let file_purger = new_noop_file_purger();
    let file_id = FileId::from_str(file_id).map_err(|e| {
        InvalidParquetSnafu {
            file: file_id,
            reason: e.to_string(),
        }
        .build()
    })?;

    Ok(FileHandle::new(
        FileMeta {
            region_id: 0.into(),
            file_id,
            time_range: (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(3600000),
            ),
            level: 0,
            file_size: 0,
            available_indexes: Default::default(),
            index_file_size: 0,
        },
        file_purger,
    ))
}

/// Creates a pk file under `input_dir` to `output_path`.
pub async fn create_pk_file(
    input_dir: &str,
    file_id: &str,
    output_path: &str,
    object_store: &ObjectStore,
) -> Result<PkWriterMetrics> {
    let mut writer = PrimaryKeyFileWriter::new(output_path);

    let file_handle = new_file_handle(file_id)?;
    let mut reader =
        ParquetReaderBuilder::new(input_dir.to_string(), file_handle, object_store.clone())
            .build()
            .await?;
    while let Some(batch) = reader.next_batch().await? {
        writer.add_primary_key(batch.primary_key());
    }

    writer.write_to_store(object_store).await
}

/// Creates a data file under `input_dir` to `output_path`.
pub async fn create_data_file(
    input_dir: &str,
    file_id: &str,
    output_path: &str,
    object_store: &ObjectStore,
) -> Result<DataWriterMetrics> {
    let file_handle = new_file_handle(file_id)?;
    let mut reader =
        ParquetReaderBuilder::new(input_dir.to_string(), file_handle, object_store.clone())
            .build()
            .await?;
    let props = Some(DataFileWriter::new_writer_props(reader.metadata()));
    let mut writer =
        DataFileWriter::new(output_path, reader.metadata(), object_store, props).await?;

    while let Some(batch) = reader.next_batch().await? {
        writer.write_data(&batch).await?;
    }

    writer.finish().await
}
