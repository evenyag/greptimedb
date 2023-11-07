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

use std::future::Future;
use std::pin::Pin;

use common_datasource::buffered_writer::LazyBufferedWriter;
use common_datasource::share_buffer::SharedBuffer;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::format::FileMetaData;
use snafu::ResultExt;

use crate::error;
use crate::error::WriteParquetSnafu;

/// Parquet writer that buffers row groups in memory and writes buffered data to an underlying
/// storage by chunks to reduce memory consumption.
pub struct BufferedWriter {
    inner: InnerBufferedWriter,
}

type InnerBufferedWriter = LazyBufferedWriter<
    object_store::Writer,
    ArrowWriter<SharedBuffer>,
    Box<
        dyn FnMut(
                String,
            ) -> Pin<
                Box<
                    dyn Future<Output = common_datasource::error::Result<object_store::Writer>>
                        + Send,
                >,
            > + Send,
    >,
>;

impl BufferedWriter {
    pub async fn try_new(
        path: String,
        store: ObjectStore,
        arrow_schema: SchemaRef,
        props: Option<WriterProperties>,
        buffer_threshold: usize,
    ) -> error::Result<Self> {
        let buffer = SharedBuffer::with_capacity(buffer_threshold);

        let arrow_writer = ArrowWriter::try_new(buffer.clone(), arrow_schema.clone(), props)
            .context(WriteParquetSnafu { path: &path })?;

        Ok(Self {
            inner: LazyBufferedWriter::new(
                buffer_threshold,
                buffer,
                arrow_writer,
                &path,
                Box::new(move |path| {
                    let store = store.clone();
                    Box::pin(async move {
                        store
                            .writer(&path)
                            .await
                            .context(common_datasource::error::WriteObjectSnafu { path })
                    })
                }),
            ),
        })
    }

    /// Write a record batch to stream writer.
    pub async fn write(&mut self, arrow_batch: &RecordBatch) -> error::Result<()> {
        self.inner
            .write(arrow_batch)
            .await
            .context(error::WriteBufferSnafu)?;
        self.inner
            .try_flush(false)
            .await
            .context(error::WriteBufferSnafu)?;

        Ok(())
    }

    /// Close parquet writer.
    pub async fn close(self) -> error::Result<(FileMetaData, u64)> {
        self.inner
            .close_with_arrow_writer()
            .await
            .context(error::WriteBufferSnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::Arc;

    use datatypes::arrow::array::{BinaryArray, UInt64Array};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use object_store::layers::LoggingLayer;
    use object_store::services::S3;

    use super::*;

    fn new_s3_store(bucket: &str) -> ObjectStore {
        let mut builder = S3::default();
        builder
            .root("/mito2-test")
            .access_key_id(&env::var("GT_S3_ACCESS_KEY_ID").unwrap())
            .secret_access_key(&env::var("GT_S3_ACCESS_KEY").unwrap())
            .region(&env::var("GT_S3_REGION").unwrap())
            .bucket(&bucket);

        ObjectStore::new(builder).unwrap().finish().layer(
            LoggingLayer::default()
                .with_error_level(Some("debug"))
                .unwrap(),
        )
    }

    fn new_kv_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, false),
            Field::new("value", DataType::Binary, false),
        ]))
    }

    fn new_test_record_batch(
        schema: SchemaRef,
        start: u64,
        num_rows: u64,
        value_size: usize,
    ) -> RecordBatch {
        let keys = Arc::new(UInt64Array::from_iter_values(
            (start..start + num_rows).map(|v| v),
        ));
        let value = vec![b'g'; value_size];
        let values = Arc::new(BinaryArray::from_iter_values(
            (start..start + num_rows).map(|_i| &value),
        ));

        RecordBatch::try_new(schema, vec![keys, values]).unwrap()
    }

    async fn write_to_s3(bucket: &str) {
        let object_store = new_s3_store(bucket);
        let writer_props = WriterProperties::builder()
            // Use a small row group size for test.
            .set_max_row_group_size(50)
            // Disable dictionary.
            .set_dictionary_enabled(false)
            .build();

        let path = "write_s3_test.parquet".to_string();
        let schema = new_kv_schema();
        let buffer_threshold = 1024 * 1024 * 8;
        let mut writer = BufferedWriter::try_new(
            path,
            object_store,
            schema.clone(),
            Some(writer_props),
            buffer_threshold,
        )
        .await
        .unwrap();

        for i in 0..7 {
            let batch = new_test_record_batch(schema.clone(), i * 1024, i * 1024 + 1024, 1024);
            writer.write(&batch).await.unwrap();
        }

        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_write_to_s3() {
        if let Ok(bucket) = env::var("GT_S3_BUCKET") {
            if !bucket.is_empty() {
                write_to_s3(&bucket).await;
            }
        }
    }
}
