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

//! Common traits and structures for the procedure framework.

use std::any::Any;
use std::sync::Arc;

use common_error::prelude::*;
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum Error {}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        StatusCode::Internal
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Procedure execution status.
#[derive(Debug)]
pub enum Status {
    Executing,
    Done,
}

/// Procedure execution context.
#[derive(Debug)]
pub struct Context {}

/// A `Procedure` represents an operation or a set of operations to be performed step-by-step.
pub trait Procedure {
    fn execute(&mut self, ctx: &Context) -> Result<Status>;
}

/// Boxed [Procedure].
pub type BoxedProcedure = Box<dyn Procedure>;

/// `ProcedureManager` executes [Procedure] submitted to it.
pub trait ProcedureManager: Send + Sync + 'static {
    /// Submit a [Procedure] to execute.
    fn submit(&self, procedure: BoxedProcedure) -> Result<()>;
}

/// Ref-counted pointer to the [ProcedureManager].
pub type ProcedureManagerRef = Arc<dyn ProcedureManager>;

/// Unique id for [Procedure].
#[derive(Debug)]
pub struct ProcedureId(Uuid);

/// Standalone [ProcedureManager].
#[derive(Debug)]
pub struct StandaloneManager {}

impl StandaloneManager {
    /// Create a new StandaloneManager with default configurations.
    pub fn new() -> StandaloneManager {
        StandaloneManager {}
    }
}

impl ProcedureManager for StandaloneManager {
    fn submit(&self, procedure: BoxedProcedure) -> Result<()> {
        unimplemented!()
    }
}
