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

//! Authoritative native samples SST writing.

use std::sync::Arc;

use datatypes::arrow::array::{Array, BinaryArray};
use datatypes::arrow::compute::{cast, max, min};
use datatypes::arrow::datatypes::DataType;
use snafu::ResultExt;
use store_api::storage::FileId;

use crate::access_layer::SstInfoArray;
use crate::access_layer::{AccessLayer, SstWriteRequest};
use crate::error::{ComputeArrowSnafu, InvalidRecordBatchSnafu, Result};
use crate::series_index::format::SamplesFormat;
use crate::series_index::samples::ColumnarSamplesFormat;
use crate::sst::file::{
    FileFormat, NativeFileMeta, NativeObjectMeta, NativeSamplesPart, RegionFileId,
};
use crate::sst::parquet::SstInfo;
use crate::sst::parquet::writer::SstWriteGuard;

/// Physical artifact ID encoded in an authoritative native object path.
/// Sidecar paths deliberately do not match this namespace.
pub(crate) fn file_id_from_path(path: &str) -> Option<FileId> {
    let (_, suffix) = path.rsplit_once("/native/")?;
    let (id, object) = suffix.split_once('/')?;
    if object.is_empty()
        || object
            .split('/')
            .any(|part| part.is_empty() || part == "..")
    {
        return None;
    }
    id.parse().ok()
}

/// Reuses the native codec and the caller's merged, sequence-normalized source.
pub(crate) async fn write_samples(
    layer: &AccessLayer,
    mut request: SstWriteRequest,
    guard: Option<Arc<dyn SstWriteGuard>>,
) -> Result<SstInfoArray> {
    let id = FileId::random();
    let region_id = request.metadata.region_id;
    if let Some(guard) = guard {
        guard
            .before_create(RegionFileId::new(region_id, id))
            .await?;
    }
    let prefix = format!("{}native/{id}/", layer.build_region_dir(region_id));
    let mut writer = ColumnarSamplesFormat::default()
        .writer(
            request.metadata.clone(),
            layer.object_store().clone(),
            &prefix,
            id,
            id,
        )
        .await?;
    let mut rows = 0;
    let mut series = 0;
    let mut previous_key = Vec::new();
    let mut lower = i64::MAX;
    let mut upper = i64::MIN;
    let timestamp = &request.metadata.time_index_column().column_schema.name;
    while let Some(batch) = request.source.next_batch().await? {
        if batch.num_rows() == 0 {
            continue;
        }
        let ts = batch.column_by_name(timestamp).ok_or_else(|| {
            InvalidRecordBatchSnafu {
                reason: "native samples lack timestamp".to_string(),
            }
            .build()
        })?;
        let (values, _) =
            datatypes::timestamp::timestamp_array_to_primitive(ts).ok_or_else(|| {
                InvalidRecordBatchSnafu {
                    reason: "invalid native timestamp".to_string(),
                }
                .build()
            })?;
        lower = lower.min(min(&values).unwrap_or(lower));
        upper = upper.max(max(&values).unwrap_or(upper));
        let keys = batch.column_by_name("__primary_key").ok_or_else(|| {
            InvalidRecordBatchSnafu {
                reason: "native samples lack primary key".to_string(),
            }
            .build()
        })?;
        let keys = cast(keys, &DataType::Binary).context(ComputeArrowSnafu)?;
        let keys = keys.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
            InvalidRecordBatchSnafu {
                reason: "invalid native primary key".to_string(),
            }
            .build()
        })?;
        for key in keys.iter().flatten() {
            if key != previous_key {
                series += 1;
                previous_key.clear();
                previous_key.extend_from_slice(key);
            }
        }
        rows += batch.num_rows();
        // Native files always retain row sequences; import barriers are applied by readers.
        writer.write(&batch).await?;
    }
    if rows == 0 {
        writer.abort().await?;
        return Ok(Default::default());
    }
    let artifact = writer.finish().await?;
    let mut manifest = None;
    let mut parts = Vec::new();
    for object in artifact.objects {
        let native = NativeObjectMeta {
            path: object.path,
            file_size: object.file_size,
        };
        if native.path.ends_with("manifest.json") {
            manifest = Some(native);
        } else {
            parts.push(NativeSamplesPart {
                part_id: parts.len() as u32,
                object: native,
            });
        }
    }
    let manifest = manifest.ok_or_else(|| {
        InvalidRecordBatchSnafu {
            reason: "native writer did not return a manifest".to_string(),
        }
        .build()
    })?;
    let unit = request.metadata.time_index_type().unit();
    Ok(smallvec::smallvec![SstInfo {
        format: FileFormat::Native(NativeFileMeta {
            use_file_sequence: false,
            manifest,
            parts
        }),
        file_id: id,
        time_range: (
            common_time::Timestamp::new(lower, unit),
            common_time::Timestamp::new(upper, unit)
        ),
        file_size: artifact.file_size,
        num_rows: rows,
        num_series: series,
        num_row_groups: 0,
        max_row_group_uncompressed_size: 0,
        file_metadata: None,
        index_metadata: Default::default(),
    }])
}

/// Checks that an artifact can only own objects inside its immutable owner namespace.
pub(crate) fn validate_inventory(
    layer: &AccessLayer,
    file: &crate::sst::file::FileMeta,
) -> Result<()> {
    use crate::series_index::columnar::require;
    let Some(objects) = file.format.native_objects() else {
        return Ok(());
    };
    let prefix = format!(
        "{}native/{}/",
        layer.build_region_dir(file.region_id),
        file.file_id
    );
    let manifest = match &file.format {
        FileFormat::Native(meta) => &meta.manifest,
        FileFormat::NativeSeries(meta) => &meta.manifest,
        FileFormat::Parquet => return Ok(()),
    };
    require(
        manifest.path == format!("{prefix}manifest.json"),
        "native manifest is outside its owner namespace",
    )?;
    let mut paths = std::collections::HashSet::new();
    let mut size = 0u64;
    for object in objects {
        require(
            object.path.starts_with(&prefix)
                && !object.path.split('/').any(|p| p == "..")
                && paths.insert(&object.path),
            "invalid or duplicate native artifact object",
        )?;
        size = size.checked_add(object.file_size).ok_or_else(|| {
            crate::series_index::columnar::invalid("native artifact size overflow")
        })?;
    }
    require(
        size == file.file_size,
        "native artifact byte count mismatch",
    )
}

/// Explicit PoC builder: merge sample keys with the existing SeqScan before writing
/// a durable series index. Publication is a separate manifest edit that rechecks coverage.
#[cfg(any(test, feature = "test"))]
pub(crate) async fn write_series(
    layer: &AccessLayer,
    metadata: store_api::metadata::RegionMetadataRef,
    sources: &[crate::sst::file::FileHandle],
    mut batches: crate::read::FlatSource,
) -> Result<crate::sst::file::FileMeta> {
    use crate::series_index::columnar::{ColumnarSeriesFormat, require};
    use crate::sst::file::{FileMeta, NativeSeriesIndexMeta};
    require(!sources.is_empty(), "native series index requires samples")?;
    require(
        sources
            .iter()
            .all(|source| matches!(source.meta_ref().format, FileFormat::Native(_))),
        "native series index requires native sample sources",
    )?;
    let id = FileId::random();
    let prefix = format!("{}native/{id}/", layer.build_region_dir(metadata.region_id));
    let mut file = FileMeta {
        region_id: metadata.region_id,
        file_id: id,
        time_range: sources
            .iter()
            .map(|file| file.meta_ref().time_range)
            .reduce(|left, right| (left.0.min(right.0), left.1.max(right.1)))
            .ok_or_else(|| {
                crate::series_index::columnar::invalid("empty native index source set")
            })?,
        format: FileFormat::NativeSeries(NativeSeriesIndexMeta {
            manifest: NativeObjectMeta {
                path: format!("{prefix}manifest.json"),
                file_size: 0,
            },
            parts: Vec::new(),
            sources: sources.iter().map(|file| file.file_id()).collect(),
        }),
        ..Default::default()
    };
    let entry = crate::series_index::native_series_entry(&file)?;
    let mut writer = ColumnarSeriesFormat::default().native_writer(
        metadata,
        layer.object_store().clone(),
        &entry,
        prefix,
    )?;
    while let Some(batch) = batches.next_batch().await? {
        writer.write(&batch).await?;
    }
    let artifact = writer.finish().await?;
    file.file_size = artifact.file_size;
    if let FileFormat::NativeSeries(index) = &mut file.format {
        for object in artifact.objects {
            let object = NativeObjectMeta {
                path: object.path,
                file_size: object.file_size,
            };
            if object.path == index.manifest.path {
                index.manifest = object;
            } else {
                index.parts.push(object);
            }
        }
    }
    validate_inventory(layer, &file)?;
    Ok(file)
}
