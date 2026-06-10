// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use object_store::{FuturesAsyncWriter, ObjectStore};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{
    AsyncFileReader, MetadataFetch, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use snafu::ResultExt;
use tokio_util::compat::{Compat, FuturesAsyncWriteCompatExt};

use crate::aggr_index::schema::{IndexKind, validate_schema};
use crate::error::{OpenDalSnafu, ReadParquetSnafu, Result, WriteParquetSnafu};
use crate::sst::{DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_CONCURRENCY};

/// The estimated size of the footer and metadata to prefetch from the end of a
/// parquet index file.
const DEFAULT_PREFETCH_SIZE: usize = 64 * 1024;

/// Async parquet writer for an aggregate index file backed by an [`ObjectStore`].
pub struct IndexWriter {
    writer: AsyncArrowWriter<Compat<FuturesAsyncWriter>>,
    rows: usize,
}

impl IndexWriter {
    pub async fn try_new(
        object_store: &ObjectStore,
        path: &str,
        schema: SchemaRef,
    ) -> Result<Self> {
        let writer = object_store
            .writer_with(path)
            .chunk(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
            .concurrent(DEFAULT_WRITE_CONCURRENCY)
            .await
            .context(OpenDalSnafu)?
            .into_futures_async_write()
            .compat_write();
        let writer = AsyncArrowWriter::try_new(writer, schema, None).context(WriteParquetSnafu)?;
        Ok(Self { writer, rows: 0 })
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.rows += batch.num_rows();
        self.writer.write(batch).await.context(WriteParquetSnafu)
    }

    pub async fn close(self) -> Result<usize> {
        self.writer.close().await.context(WriteParquetSnafu)?;
        Ok(self.rows)
    }
}

/// Async parquet reader for an aggregate index file backed by an [`ObjectStore`].
///
/// Yields [`RecordBatch`]es as a [`Stream`].
pub struct IndexReader {
    inner: ParquetRecordBatchStream<ObjectStoreReader>,
    schema: SchemaRef,
    path: String,
}

impl IndexReader {
    pub async fn try_new(object_store: &ObjectStore, path: &str, kind: IndexKind) -> Result<Self> {
        let reader = ObjectStoreReader::new(object_store.clone(), path.to_string());
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .context(ReadParquetSnafu { path })?;
        validate_schema(kind, builder.schema().as_ref())?;
        let schema = builder.schema().clone();
        let inner = builder.build().context(ReadParquetSnafu { path })?;
        Ok(Self {
            inner,
            schema,
            path: path.to_string(),
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for IndexReader {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let path = self.path.clone();
        self.inner
            .poll_next_unpin(cx)
            .map(|opt| opt.map(|res| res.context(ReadParquetSnafu { path })))
    }
}

/// Bridges an opendal [`ObjectStore`] to parquet's [`AsyncFileReader`].
///
/// Mirrors mito2's `ObjectStoreFetch`/`MetadataLoader` (see
/// `crate::sst::parquet::metadata`): bytes are fetched with
/// `object_store.read_with(path).range(range)` and metadata is loaded with
/// [`ParquetMetaDataReader`].
struct ObjectStoreReader {
    object_store: ObjectStore,
    path: String,
}

impl ObjectStoreReader {
    fn new(object_store: ObjectStore, path: String) -> Self {
        Self { object_store, path }
    }
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: std::ops::Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move {
            let data = self
                .object_store
                .read_with(&self.path)
                .range(range)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            Ok(data.to_bytes())
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        async move {
            let file_size = self
                .object_store
                .stat(&self.path)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?
                .content_length();
            let fetch = ObjectStoreFetch {
                object_store: &self.object_store,
                path: &self.path,
            };
            let metadata = ParquetMetaDataReader::new()
                .with_prefetch_hint(Some(DEFAULT_PREFETCH_SIZE))
                .load_and_finish(fetch, file_size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

struct ObjectStoreFetch<'a> {
    object_store: &'a ObjectStore,
    path: &'a str,
}

impl MetadataFetch for ObjectStoreFetch<'_> {
    fn fetch(&mut self, range: std::ops::Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move {
            let data = self
                .object_store
                .read_with(self.path)
                .range(range)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            Ok(data.to_bytes())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use datatypes::arrow::array::{BinaryArray, Int64Array, UInt64Array};
    use object_store::services::Fs;

    use super::*;
    use crate::aggr_index::schema::IndexKind;

    fn temp_store(root: &str) -> ObjectStore {
        ObjectStore::new(Fs::default().root(root)).unwrap().finish()
    }

    fn pk_batch() -> RecordBatch {
        RecordBatch::try_new(
            IndexKind::Pk.schema(),
            vec![
                Arc::new(BinaryArray::from_iter_values([b"a".as_slice(), b"bb"])) as _,
                Arc::new(Int64Array::from(vec![1, 3])) as _,
                Arc::new(Int64Array::from(vec![2, 5])) as _,
                Arc::new(UInt64Array::from(vec![10u64, 20])) as _,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_index_writer_reader_round_trip() {
        let dir = create_temp_dir("aggr_index_io");
        let store = temp_store(dir.path().to_str().unwrap());
        let batch = pk_batch();

        let mut writer = IndexWriter::try_new(&store, "sub/pk.parquet", IndexKind::Pk.schema())
            .await
            .unwrap();
        writer.write(&batch).await.unwrap();
        let rows = writer.close().await.unwrap();
        assert_eq!(rows, batch.num_rows());

        let mut reader = IndexReader::try_new(&store, "sub/pk.parquet", IndexKind::Pk)
            .await
            .unwrap();
        assert_eq!(reader.schema().as_ref(), IndexKind::Pk.schema().as_ref());

        let mut read = Vec::new();
        while let Some(b) = reader.next().await {
            read.push(b.unwrap());
        }
        assert_eq!(read.len(), 1);
        assert_eq!(read[0], batch);
    }

    #[tokio::test]
    async fn test_index_reader_rejects_wrong_schema() {
        let dir = create_temp_dir("aggr_index_io_bad");
        let store = temp_store(dir.path().to_str().unwrap());

        let mut writer = IndexWriter::try_new(&store, "pk.parquet", IndexKind::Pk.schema())
            .await
            .unwrap();
        writer.write(&pk_batch()).await.unwrap();
        writer.close().await.unwrap();

        // Reading a Pk file as a Tag index must fail schema validation.
        let err = match IndexReader::try_new(&store, "pk.parquet", IndexKind::Tag).await {
            Ok(_) => panic!("expected schema validation error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("index schema"), "{err}");
    }
}
