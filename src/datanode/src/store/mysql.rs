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

use common_base::secrets::ExposeSecret;
use object_store::services::Mysql;
use object_store::{util, ObjectStore};
use snafu::ResultExt;

use crate::config::MysqlConfig;
use crate::error::{InitBackendSnafu, Result};

/// Creates a mysql store for the given config.
pub(crate) async fn new_mysql_object_store(mysql_config: &MysqlConfig) -> Result<ObjectStore> {
    let root = util::normalize_dir(&mysql_config.root);

    common_telemetry::info!(
        "The mysql storage table is: {}, root is: {}, key_field is: {}, value_field is: {}",
        mysql_config.table,
        &root,
        mysql_config.key_field,
        mysql_config.value_field
    );

    let builder = Mysql::default()
        .connection_string(mysql_config.connection_string.expose_secret())
        .root(&root)
        .table(&mysql_config.table)
        .key_field(&mysql_config.key_field)
        .value_field(&mysql_config.value_field);

    Ok(ObjectStore::new(builder)
        .context(InitBackendSnafu)?
        .finish())
}
