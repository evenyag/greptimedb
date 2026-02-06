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

use std::collections::HashMap;

use datatypes::arrow::array::ArrayRef;
use datatypes::arrow::record_batch::RecordBatch;
use mito_codec::row_converter::PrimaryKeyCodec;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::error::Result;
use crate::sst::parquet::flat_format::{DecodedPrimaryKeys, decode_primary_keys};

pub(crate) struct TagDecodeState {
    decoded_pks: Option<DecodedPrimaryKeys>,
    decoded_tag_cache: HashMap<ColumnId, ArrayRef>,
}

impl TagDecodeState {
    pub(crate) fn new() -> Self {
        Self {
            decoded_pks: None,
            decoded_tag_cache: HashMap::new(),
        }
    }
}

/// Returns the decoded tag column for `column_id`, or `None` if it's not a tag.
pub(crate) fn maybe_decode_tag_column(
    metadata: &RegionMetadataRef,
    column_id: ColumnId,
    input: &RecordBatch,
    tag_decode_state: &mut TagDecodeState,
    codec: &dyn PrimaryKeyCodec,
) -> Result<Option<ArrayRef>> {
    let Some(pk_index) = metadata.primary_key_index(column_id) else {
        return Ok(None);
    };

    if let Some(cached_column) = tag_decode_state.decoded_tag_cache.get(&column_id) {
        return Ok(Some(cached_column.clone()));
    }

    if tag_decode_state.decoded_pks.is_none() {
        tag_decode_state.decoded_pks = Some(decode_primary_keys(codec, input)?);
    }

    let pk_index = if codec.encoding() == PrimaryKeyEncoding::Sparse {
        None
    } else {
        Some(pk_index)
    };

    let Some(column_index) = metadata.column_index_by_id(column_id) else {
        return Ok(None);
    };
    let Some(decoded) = tag_decode_state.decoded_pks.as_ref() else {
        return Ok(None);
    };

    let column_metadata = &metadata.column_metadatas[column_index];
    let tag_column = decoded.get_tag_column(
        column_id,
        pk_index,
        &column_metadata.column_schema.data_type,
    )?;
    tag_decode_state
        .decoded_tag_cache
        .insert(column_id, tag_column.clone());

    Ok(Some(tag_column))
}
