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

use common_datasource::file_format::parquet::BufferedWriter;
use common_time::Timestamp;
use datatypes::arrow::array::{BinaryArray, UInt64Array};
use datatypes::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use parquet::file::properties::WriterProperties;
use snafu::ResultExt;

use crate::error::{InvalidParquetSnafu, Result, WriteBufferSnafu};
use crate::read::BatchReader;
use crate::sst::file::{FileHandle, FileId, FileMeta};
use crate::sst::file_purger::{FilePurger, FilePurgerRef, PurgeRequest};
use crate::sst::parquet::reader::ParquetReaderBuilder;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::{DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_CONCURRENCY};

const PK_ROW_GROUP_SIZE: usize = 8192;

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

    async fn write_to_store(&self, store: &ObjectStore) -> Result<WriterMetrics> {
        let mut metrics = WriterMetrics {
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
        writer.close().await.context(WriteBufferSnafu)?;

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
        WriterProperties::builder()
            .set_max_row_group_size(self.row_group_size)
            .build()
    }

    fn new_schema() -> SchemaRef {
        let fields = Fields::from(vec![
            Field::new("__primary_key", DataType::Binary, false),
            Field::new("__pkid", DataType::UInt64, false),
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
pub struct WriterMetrics {
    /// Number of primary keys.
    pub num_pk: usize,
    /// Total bytes of primary keys.
    pub pk_bytes: usize,
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

/// Writes file under `input_dir` to `output_path`.
pub async fn create_pk_file(
    input_dir: &str,
    file_id: &str,
    output_path: &str,
    object_store: &ObjectStore,
) -> Result<WriterMetrics> {
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
