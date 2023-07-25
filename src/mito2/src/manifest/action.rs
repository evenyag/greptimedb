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

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use storage::metadata::VersionNumber;
use storage::sst::{FileId, FileMeta};
use store_api::manifest::action::{ProtocolAction, ProtocolVersion};
use store_api::manifest::ManifestVersion;
use store_api::storage::{RegionId, SequenceNumber};

use crate::error::{RegionMetadataNotFoundSnafu, Result, SerdeJsonSnafu, Utf8Snafu};
use crate::metadata::RegionMetadataRef;

/// Actions that can be applied to region manifest.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum RegionMetaAction {
    /// Set the min/max supported protocol version
    Protocol(ProtocolAction),
    /// Change region's metadata for request like ALTER
    Change(RegionChange),
    /// Edit region's state for changing options or file list.
    Edit(RegionEdit),
    /// Remove the region.
    Remove(RegionRemove),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionChange {
    /// The metadata after changed.
    pub metadata: RegionMetadataRef,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionEdit {
    pub region_version: VersionNumber,
    pub files_to_add: Vec<FileMeta>,
    pub files_to_remove: Vec<FileMeta>,
    pub compaction_time_window: Option<i64>,
    pub flushed_sequence: Option<SequenceNumber>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionRemove {
    pub region_id: RegionId,
}

/// The region manifest data.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionManifest {
    /// Metadata of the region.
    pub metadata: RegionMetadataRef,
    /// SST files.
    pub files: HashMap<FileId, FileMeta>,
    /// Current manifest version.
    pub manifest_version: ManifestVersion,
}

#[derive(Debug, Default)]
pub struct RegionManifestBuilder {
    metadata: Option<RegionMetadataRef>,
    files: HashMap<FileId, FileMeta>,
    manifest_version: ManifestVersion,
}

impl RegionManifestBuilder {
    /// Start with a checkpoint.
    pub fn with_checkpoint(checkpoint: Option<RegionManifest>) -> Self {
        if let Some(s) = checkpoint {
            Self {
                metadata: Some(s.metadata),
                files: s.files,
                manifest_version: s.manifest_version,
            }
        } else {
            Default::default()
        }
    }

    pub fn apply_change(&mut self, change: RegionChange) {
        self.metadata = Some(change.metadata);
    }

    pub fn apply_edit(&mut self, manifest_version: ManifestVersion, edit: RegionEdit) {
        self.manifest_version = manifest_version;
        for file in edit.files_to_add {
            self.files.insert(file.file_id, file);
        }
        for file in edit.files_to_remove {
            self.files.remove(&file.file_id);
        }
    }

    /// Check if the builder keeps a [RegionMetadata]
    pub fn contains_metadata(&self) -> bool {
        self.metadata.is_some()
    }

    pub fn try_build(self) -> Result<RegionManifest> {
        let metadata = self.metadata.context(RegionMetadataNotFoundSnafu)?;
        Ok(RegionManifest {
            metadata,
            files: self.files,
            manifest_version: self.manifest_version,
        })
    }
}

// The checkpoint of region manifest, generated by checkpointer.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct RegionCheckpoint {
    /// The snasphot protocol
    pub protocol: ProtocolAction,
    /// The last manifest version that this checkpoint compacts(inclusive).
    pub last_version: ManifestVersion,
    // The number of manifest actions that this checkpoint compacts.
    pub compacted_actions: usize,
    // The checkpoint data
    pub checkpoint: Option<RegionManifest>,
}

impl RegionCheckpoint {
    pub fn set_protocol(&mut self, action: ProtocolAction) {
        self.protocol = action;
    }

    pub fn last_version(&self) -> ManifestVersion {
        self.last_version
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        todo!()
    }

    pub fn decode(bs: &[u8]) -> Result<Self> {
        // helper::decode_checkpoint(bs, reader_version)
        todo!()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionMetaActionList {
    pub actions: Vec<RegionMetaAction>,
    pub prev_version: ManifestVersion,
}

impl RegionMetaActionList {
    pub fn with_action(action: RegionMetaAction) -> Self {
        Self {
            actions: vec![action],
            prev_version: 0,
        }
    }

    pub fn new(actions: Vec<RegionMetaAction>) -> Self {
        Self {
            actions,
            prev_version: 0,
        }
    }
}

impl RegionMetaActionList {
    fn set_protocol(&mut self, action: ProtocolAction) {
        // The protocol action should be the first action in action list by convention.
        self.actions.insert(0, RegionMetaAction::Protocol(action));
    }

    pub fn set_prev_version(&mut self, version: ManifestVersion) {
        self.prev_version = version;
    }

    /// Encode self into json in the form of string lines, starts with prev_version and then action json list.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let json = serde_json::to_string(&self).context(SerdeJsonSnafu)?;

        Ok(json.into_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let data = std::str::from_utf8(bytes).context(Utf8Snafu)?;

        serde_json::from_str(data).context(SerdeJsonSnafu)
    }
}

pub struct RegionMetaActionIter {
    // log_iter: ObjectStoreLogIterator,
    reader_version: ProtocolVersion,
    last_protocol: Option<ProtocolAction>,
}

impl RegionMetaActionIter {
    pub fn last_protocol(&self) -> Option<ProtocolAction> {
        self.last_protocol.clone()
    }

    async fn next_action(&mut self) -> Result<Option<(ManifestVersion, RegionMetaActionList)>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use storage::sst::FileId;

    use super::*;

    #[test]
    fn test_encode_decode_action_list() {
        // TODO(ruihang): port this test case
    }

    // These tests are used to ensure backward compatibility of manifest files.
    // DO NOT modify the serialized string when they fail, check if your
    // modification to manifest-related structs is compatible with older manifests.
    #[test]
    fn test_region_manifest_compatibility() {
        let region_edit = r#"{"region_version":0,"flushed_sequence":null,"files_to_add":[{"region_id":4402341478400,"file_name":"4b220a70-2b03-4641-9687-b65d94641208.parquet","time_range":[{"value":1451609210000,"unit":"Millisecond"},{"value":1451609520000,"unit":"Millisecond"}],"level":1}],"files_to_remove":[{"region_id":4402341478400,"file_name":"34b6ebb9-b8a5-4a4b-b744-56f67defad02.parquet","time_range":[{"value":1451609210000,"unit":"Millisecond"},{"value":1451609520000,"unit":"Millisecond"}],"level":0}]}"#;
        let _ = serde_json::from_str::<RegionEdit>(region_edit).unwrap();

        let region_change = r#" {"committed_sequence":42,"metadata":{"column_metadatas":[{"column_schema":{"name":"a","data_type":{"Int64":{}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Tag","column_id":1},{"column_schema":{"name":"b","data_type":{"Float64":{}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Field","column_id":2},{"column_schema":{"name":"c","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Timestamp","column_id":3}],"version":9,"primary_key":[1],"region_id":5299989648942}}"#;
        let _ = serde_json::from_str::<RegionChange>(region_change).unwrap();

        let region_remove = r#"{"region_id":42}"#;
        let _ = serde_json::from_str::<RegionRemove>(region_remove).unwrap();

        let protocol_action = r#"{"min_reader_version":1,"min_writer_version":2}"#;
        let _ = serde_json::from_str::<ProtocolAction>(protocol_action).unwrap();
    }

    fn mock_file_meta() -> FileMeta {
        FileMeta {
            region_id: 0.into(),
            file_id: FileId::random(),
            time_range: None,
            level: 0,
            file_size: 1024,
        }
    }

    #[test]
    fn test_region_manifest_builder() {
        // TODO(ruihang): port this test case
    }

    #[test]
    fn test_encode_decode_region_checkpoint() {
        // TODO(ruihang): port this test case
    }
}
