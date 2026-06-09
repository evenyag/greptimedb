// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::fs::File;
use std::path::Path;

use datatypes::arrow::array::RecordBatchReader;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use snafu::ResultExt;

use crate::aggr_index::schema::{IndexKind, validate_schema};
use crate::error::{Error, ExternalSnafu, Result};

pub struct IndexWriter {
    writer: ArrowWriter<File>,
    rows: usize,
}

impl IndexWriter {
    pub fn try_new(path: impl AsRef<Path>, schema: SchemaRef) -> Result<Self> {
        let file = File::create(path.as_ref()).map_err(|e| {
            external_error(e, format!("create index file {}", path.as_ref().display()))
        })?;
        let writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| external_error(e, "create parquet arrow writer".to_string()))?;
        Ok(Self { writer, rows: 0 })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.rows += batch.num_rows();
        self.writer
            .write(batch)
            .map_err(|e| external_error(e, "write index parquet batch".to_string()))
    }

    pub fn close(self) -> Result<usize> {
        let rows = self.rows;
        self.writer
            .close()
            .map_err(|e| external_error(e, "close index parquet writer".to_string()))?;
        Ok(rows)
    }
}

pub struct IndexReader {
    inner: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
}

impl IndexReader {
    pub fn try_new(path: impl AsRef<Path>, kind: IndexKind) -> Result<Self> {
        let file = File::open(path.as_ref()).map_err(|e| {
            external_error(e, format!("open index file {}", path.as_ref().display()))
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| external_error(e, "open parquet record batch reader".to_string()))?;
        validate_schema(kind, builder.schema().as_ref())?;
        let inner = builder
            .build()
            .map_err(|e| external_error(e, "build parquet record batch reader".to_string()))?;
        Ok(Self { inner })
    }

    pub fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Iterator for IndexReader {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|r| r.map_err(|e| external_error(e, "read index parquet batch".to_string())))
    }
}

fn external_error(error: impl std::fmt::Display, context: String) -> Error {
    let boxed = common_error::ext::BoxedError::new(common_error::ext::PlainError::new(
        error.to_string(),
        common_error::status_code::StatusCode::Unexpected,
    ));
    let result: std::result::Result<(), common_error::ext::BoxedError> = Err(boxed);
    result.context(ExternalSnafu { context }).unwrap_err()
}
