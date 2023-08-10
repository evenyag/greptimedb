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

mod object_store_util;

use datanode::datanode::StorageConfig;

pub type Result<T, E = snafu::Whatever> = std::result::Result<T, E>;

/// DB repairer.
#[derive(Debug)]
pub struct Repairer {
    /// Storage config of the db.
    storage_config: StorageConfig,
}

impl Repairer {
    /// Creates a new repairer.
    pub fn new(storage_config: StorageConfig) -> Repairer {
        Repairer { storage_config }
    }
}
