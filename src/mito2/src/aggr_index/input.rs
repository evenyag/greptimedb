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
use crate::sst::{FlatSchemaOptions, flat_sst_arrow_schema_column_num};

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
    validate_sparse_flat_sst_schema(
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

pub fn validate_sparse_flat_sst_schema(
    metadata: &RegionMetadataRef,
    num_columns: usize,
    file_path: &str,
) -> Result<()> {
    ensure!(
        metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse,
        InvalidMetaSnafu {
            reason: "aggregate index only supports sparse primary-key encoding"
        }
    );

    // Sparse flat SSTs intentionally omit raw primary-key columns, so their
    // physical schema has the same column count as the legacy primary-key SST
    // format. The aggregate index builder only needs the sparse __primary_key
    // payload and timestamp, so this schema is valid here.
    let sparse_flat_columns = flat_sst_arrow_schema_column_num(
        metadata,
        &FlatSchemaOptions::from_encoding(metadata.primary_key_encoding),
    );
    if num_columns == sparse_flat_columns {
        return Ok(());
    }

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::storage::RegionId;

    use super::*;

    fn build_metadata(encoding: PrimaryKeyEncoding) -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_0".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_1".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field_0".to_string(),
                    ConcreteDataType::uint64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts".to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![0, 1])
            .primary_key_encoding(encoding);

        Arc::new(builder.build().unwrap())
    }

    #[test]
    fn test_validate_sparse_flat_sst_schema_accepts_sparse_flat_schema() {
        let metadata = build_metadata(PrimaryKeyEncoding::Sparse);
        let num_columns = flat_sst_arrow_schema_column_num(
            &metadata,
            &FlatSchemaOptions::from_encoding(PrimaryKeyEncoding::Sparse),
        );

        validate_sparse_flat_sst_schema(&metadata, num_columns, "test.parquet").unwrap();
    }

    #[test]
    fn test_validate_sparse_flat_sst_schema_accepts_full_flat_schema() {
        let metadata = build_metadata(PrimaryKeyEncoding::Sparse);
        let num_columns =
            flat_sst_arrow_schema_column_num(&metadata, &FlatSchemaOptions::default());

        validate_sparse_flat_sst_schema(&metadata, num_columns, "test.parquet").unwrap();
    }

    #[test]
    fn test_validate_sparse_flat_sst_schema_rejects_dense_encoding() {
        let metadata = build_metadata(PrimaryKeyEncoding::Dense);
        let num_columns =
            flat_sst_arrow_schema_column_num(&metadata, &FlatSchemaOptions::default());

        let err =
            validate_sparse_flat_sst_schema(&metadata, num_columns, "test.parquet").unwrap_err();
        assert!(
            err.to_string()
                .contains("aggregate index only supports sparse primary-key encoding"),
            "{err:?}"
        );
    }

    #[test]
    fn test_validate_sparse_flat_sst_schema_rejects_invalid_column_count() {
        let metadata = build_metadata(PrimaryKeyEncoding::Sparse);
        let num_columns =
            flat_sst_arrow_schema_column_num(&metadata, &FlatSchemaOptions::default()) - 1;

        let err =
            validate_sparse_flat_sst_schema(&metadata, num_columns, "test.parquet").unwrap_err();
        assert!(
            err.to_string().contains("Column number difference"),
            "{err:?}"
        );
    }
}
