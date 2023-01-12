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

use async_trait::async_trait;

use std::collections::HashMap;
use crate::error::{Result, LoaderConflictSnafu};
use crate::procedure::{BoxedProcedure, BoxedProcedureLoader, Handle, ProcedureManager};
use std::sync::Mutex;
use snafu::ensure;

// TODO(yingwen): 1. lock manager; 2. procedure store.

/// Standalone [ProcedureManager] that maintains state on current machine.
pub struct StandaloneManager {
    /// Procedure loaders. The key is the type name of the procedure which the loader returns.
    loaders: Mutex<HashMap<String, BoxedProcedureLoader>>,
}

impl StandaloneManager {
    /// Create a new StandaloneManager with default configurations.
    pub fn new() -> StandaloneManager {
        StandaloneManager {
            loaders: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl ProcedureManager for StandaloneManager {
    fn register_loader(&self, name: &str, loader: BoxedProcedureLoader) -> Result<()> {
        let mut loaders = self.loaders.lock().unwrap();
        ensure!(!loaders.contains_key(name), LoaderConflictSnafu {
            name,
        });

        loaders.insert(name.to_string(), loader);

        Ok(())
    }

    async fn submit(&self, procedure: BoxedProcedure) -> Result<Handle> {
        unimplemented!()
    }
}
