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
use serde::{Deserialize, Serialize};
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
    /// The procedure has suspended itself and is waiting for subprocedures.
    Suspended {
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

    /// Returns `true` if the procedure needs the framework to persist its state.
    pub fn need_persist(&self) -> bool {
        match self {
            Status::Executing { persist } | Status::Suspended { persist } => *persist,
            Status::Done => false,
        }
    }
}

/// Procedure execution context.
#[derive(Debug)]
pub struct Context {
    /// Id of the procedure.
    pub procedure_id: ProcedureId,
}

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

    /// Returns the [LockKey] if this procedure needs to acquire lock.
    fn lock_key(&self) -> Option<LockKey>;
}

/// A key to identify the lock.
// We might hold multiple keys in this struct. When there are multiple keys, we need to sort the
// keys and use the first key as primary key.
pub struct LockKey(String);

impl LockKey {
    /// Returns a new [LockKey].
    pub fn new(key: impl Into<String>) -> LockKey {
        LockKey(key.into())
    }

    /// Returns the primary lock key.
    pub fn primary_lock_key(&self) -> &str {
        &self.0
    }
}

/// Boxed [Procedure].
pub type BoxedProcedure = Box<dyn Procedure>;

/// Unique id for [Procedure].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcedureId(Uuid);

impl ProcedureId {
    /// Returns a new ProcedureId randomly.
    fn random() -> ProcedureId {
        ProcedureId(Uuid::new_v4())
    }

    /// Parses id from string.
    fn parse_str(input: &str) -> Option<ProcedureId> {
        Uuid::parse_str(input).map(ProcedureId).ok()
    }
}

impl fmt::Display for ProcedureId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// TODO(yingwen): The handle is a bit useless, we could remove it.
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
    pub fn new(procedure_id: ProcedureId) -> (Handle, Sender<Result<()>>) {
        let (sender, receiver) = oneshot::channel();
        let handle = Handle {
            procedure_id,
            receiver,
        };
        (handle, sender)
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
pub type BoxedProcedureLoader = Box<dyn Fn(&str) -> Result<BoxedProcedure> + Send>;

/// Options to submit a procedure.
#[derive(Debug, Default)]
pub struct SubmitOptions {
    /// Parent procedure id.
    pub parent_id: Option<ProcedureId>,
    /// Current procedure id.
    ///
    /// If this id is specific, the procedure manager can use this id to load the
    /// cached procedure data from the procedure store.
    pub procedure_id: Option<ProcedureId>,
}

/// `ProcedureManager` executes [Procedure] submitted to it.
#[async_trait]
pub trait ProcedureManager: Send + Sync + 'static {
    /// Registers loader for specific procedure type `name`.
    fn register_loader(&self, name: &str, loader: BoxedProcedureLoader) -> Result<()>;

    /// Submits a [Procedure] to execute.
    async fn submit(&self, opts: SubmitOptions, procedure: BoxedProcedure) -> Result<Handle>;

    /// Recovers unfinished procedures and reruns them.
    ///
    /// Callers should ensure all loaders are registered.
    async fn recover(&self) -> Result<()>;
}

/// Ref-counted pointer to the [ProcedureManager].
pub type ProcedureManagerRef = Arc<dyn ProcedureManager>;

/// Serialized data of a procedure.
#[derive(Debug, Serialize, Deserialize)]
struct ProcedureMessage {
    /// Type name of the procedure. The procedure framework also use the type name to
    /// find a loader to load the procedure.
    type_name: String,
    /// The data of the procedure.
    data: String,
    /// Parent procedure id.
    parent_id: Option<ProcedureId>,
}
