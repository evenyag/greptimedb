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

use async_trait::async_trait;
use common_error::prelude::*;
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to execute procedure, source: {}", source))]
    Execute {
        #[snafu(backtrace)]
        source: BoxedError,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::Execute { source } => source.status_code(),
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Error {
    /// Creates a new [Error::Execute] error from source `err`.
    pub fn execute<E: ErrorExt + Send + Sync + 'static>(err: E) -> Error {
        Error::Execute {
            source: BoxedError::new(err),
        }
    }
}

/// Procedure execution status.
#[derive(Debug)]
pub enum Status {
    /// The procedure is still executing.
    Executing {
        /// Whether the framework need to persist the procedure.
        persist: bool,
    },
    /// the procedure is done.
    Done,
}

impl Status {
    /// Returns a [Status::Executing] with given `persist` flag.
    pub fn executing(persist: bool) -> Status {
        Status::Executing { persist }
    }
}

/// Procedure execution context.
#[derive(Debug)]
pub struct Context {}

/// A `Procedure` represents an operation or a set of operations to be performed step-by-step.
#[async_trait]
pub trait Procedure {
    /// Execute the procedure.
    ///
    /// The implementation must be idempotent.
    async fn execute(&mut self, ctx: &Context) -> Result<Status>;
}

/// Boxed [Procedure].
pub type BoxedProcedure = Box<dyn Procedure>;

/// `ProcedureManager` executes [Procedure] submitted to it.
pub trait ProcedureManager: Send + Sync + 'static {
    /// Submit a [Procedure] to execute.
    // TODO(yingwen): Returns a joinable handle.
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
