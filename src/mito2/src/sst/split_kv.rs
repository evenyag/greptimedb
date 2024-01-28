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
use std::sync::Arc;

use common_datasource::file_format::parquet::BufferedWriter;
use datatypes::arrow::array::{BinaryArray, UInt64Array};
use datatypes::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use parquet::file::properties::WriterProperties;
use snafu::ResultExt;

use crate::error::{Result, WriteBufferSnafu};
use crate::sst::{DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_CONCURRENCY};

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

    async fn write_to_store(&self, store: &ObjectStore) -> Result<()> {
        let mut writer = self.new_buffered_writer(store).await?;

        let key_values: Vec<_> = self.primary_keys.iter().collect();
        for kv_batch in key_values.chunks(self.batch_size) {
            let record_batch = self.kv_batch_to_record_batch(kv_batch);
            writer
                .write(&record_batch)
                .await
                .context(WriteBufferSnafu)?;
        }
        writer.close().await.context(WriteBufferSnafu)?;

        Ok(())
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
