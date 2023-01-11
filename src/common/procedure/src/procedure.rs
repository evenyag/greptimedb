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

mod standalone;

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use snafu::ResultExt;
pub use standalone::StandaloneManager;
use tokio::sync::oneshot::{self, Receiver, Sender};
use uuid::Uuid;

use crate::error::{JoinSnafu, Result};

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
pub trait Procedure: Send + Sync {
    /// Type name of the procedure.
    fn type_name(&self) -> &str;

    /// Execute the procedure.
    ///
    /// The implementation must be idempotent.
    async fn execute(&mut self, ctx: &Context) -> Result<Status>;

    /// Dump the state of the procedure to a string.
    fn dump(&self) -> Result<String>;
}

/// Boxed [Procedure].
pub type BoxedProcedure = Box<dyn Procedure>;

/// Unique id for [Procedure].
#[derive(Debug, Clone, Copy)]
pub struct ProcedureId(Uuid);

impl fmt::Display for ProcedureId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Handle to join on a procedure.
pub struct Handle {
    procedure_id: ProcedureId,
    receiver: Receiver<Result<()>>,
}

impl Handle {
    /// Returns a sender and a handle to join the procedure with specific
    /// `procedure_id`.
    ///
    /// The [ProcedureManager] could use the sender to notify the handle.
    pub fn new(procedure_id: ProcedureId) -> (Sender<Result<()>>, Handle) {
        let (sender, receiver) = oneshot::channel();
        let handle = Handle {
            procedure_id,
            receiver,
        };
        (sender, handle)
    }

    /// Returns the id of the procedure to join.
    #[inline]
    pub fn procedure_id(&self) -> ProcedureId {
        self.procedure_id
    }

    /// Joins the result from the sender.
    pub async fn join(self) -> Result<()> {
        self.receiver.await.context(JoinSnafu {
            procedure_id: self.procedure_id,
        })?
    }
}

/// Loader to recover the [Procedure] instance from serialized data.
pub type BoxedProcedureLoader = Box<dyn Fn(&str) -> Result<BoxedProcedure>>;

/// `ProcedureManager` executes [Procedure] submitted to it.
#[async_trait]
pub trait ProcedureManager: Send + Sync + 'static {
    fn register_loader(&self, name: &str, loader: BoxedProcedureLoader) -> Result<()>;

    /// Submit a [Procedure] to execute.
    async fn submit(&self, procedure: BoxedProcedure) -> Result<Handle>;
}

/// Ref-counted pointer to the [ProcedureManager].
pub type ProcedureManagerRef = Arc<dyn ProcedureManager>;
