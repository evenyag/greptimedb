// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::sync::Arc;

use datatypes::arrow::datatypes::SchemaRef;
use futures::stream;
use object_store::ObjectStore;
use snafu::{OptionExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::PathType;

use crate::error::{InvalidMetaSnafu, Result};
use crate::read::BoxedRecordBatchStream;
use crate::read::flat_merge::FlatMergeReader;
use crate::read::read_columns::ReadColumns;
use crate::sst::file::{FileHandle, FileMeta};
use crate::sst::file_purger::NoopFilePurger;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::flat_format::FlatReadFormat;
use crate::sst::parquet::reader::ParquetReaderBuilder;

pub struct SstStream {
    pub stream: BoxedRecordBatchStream,
    pub metadata: RegionMetadataRef,
    pub schema: SchemaRef,
}

pub async fn open_sst_stream(
    table_dir: String,
    path_type: PathType,
    object_store: ObjectStore,
    file_meta: FileMeta,
) -> Result<SstStream> {
    let handle = FileHandle::new(file_meta, Arc::new(NoopFilePurger));
    let file_path = handle.file_path(&table_dir, path_type);
    let reader = ParquetReaderBuilder::new(table_dir, path_type, handle, object_store)
        .projection(Some(ReadColumns::from_deduped_column_ids(
            std::iter::empty(),
        )))
        .build()
        .await?
        .context(InvalidMetaSnafu {
            reason: "SST has no readable row groups",
        })?;
    let metadata = reader.metadata().clone();
    validate_sparse_flat_metadata(&metadata)?;
    reject_legacy_format(
        &metadata,
        reader
            .parquet_metadata()
            .file_metadata()
            .schema_descr()
            .num_columns(),
        &file_path,
    )?;
    let mut reader = reader;
    let first = reader.next_record_batch().await?;
    let schema = if let Some(batch) = &first {
        batch.schema()
    } else {
        metadata.schema.arrow_schema().clone()
    };
    let stream = Box::pin(stream::try_unfold(
        (first, reader),
        |(pending, mut reader)| async move {
            if let Some(batch) = pending {
                return Ok(Some((batch, (None, reader))));
            }
            let batch = reader.next_record_batch().await?;
            Ok(batch.map(|batch| (batch, (None, reader))))
        },
    ));
    Ok(SstStream {
        stream,
        metadata,
        schema,
    })
}

pub fn validate_sparse_flat_metadata(metadata: &RegionMetadataRef) -> Result<()> {
    ensure!(
        metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse,
        InvalidMetaSnafu {
            reason: "aggregate index only supports sparse primary-key encoding"
        }
    );
    Ok(())
}

pub fn reject_legacy_format(
    metadata: &RegionMetadataRef,
    num_columns: usize,
    file_path: &str,
) -> Result<()> {
    let legacy = FlatReadFormat::is_legacy_format(metadata, num_columns, file_path)?;
    ensure!(
        !legacy,
        InvalidMetaSnafu {
            reason: format!("legacy primary-key SST format is not supported: {file_path}")
        }
    );
    Ok(())
}

pub async fn merge_sources(
    schema: SchemaRef,
    sources: Vec<BoxedRecordBatchStream>,
) -> Result<BoxedRecordBatchStream> {
    let reader = FlatMergeReader::new(schema, sources, DEFAULT_READ_BATCH_SIZE, None).await?;
    Ok(Box::pin(stream::try_unfold(
        reader,
        |mut reader| async move {
            let batch = reader.next_batch().await?;
            Ok(batch.map(|batch| (batch, reader)))
        },
    )))
}

pub fn validate_same_schema(expected: &SchemaRef, actual: &SchemaRef) -> Result<()> {
    ensure!(
        expected.as_ref() == actual.as_ref(),
        InvalidMetaSnafu {
            reason: "mixed sparse-flat SST schemas are not supported"
        }
    );
    Ok(())
}
