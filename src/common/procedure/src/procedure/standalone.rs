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

use crate::error::Result;
use crate::procedure::{BoxedProcedure, BoxedProcedureLoader, Handle, ProcedureManager};

/// Standalone [ProcedureManager] that maintains state on current machine.
#[derive(Debug)]
pub struct StandaloneManager {}

impl StandaloneManager {
    /// Create a new StandaloneManager with default configurations.
    pub fn new() -> StandaloneManager {
        StandaloneManager {}
    }
}

#[async_trait]
impl ProcedureManager for StandaloneManager {
    fn register_loader(&self, _name: &str, _loader: BoxedProcedureLoader) -> Result<()> {
        unimplemented!()
    }

    async fn submit(&self, procedure: BoxedProcedure) -> Result<Handle> {
        unimplemented!()
    }
}
