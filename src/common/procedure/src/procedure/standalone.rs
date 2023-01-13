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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use common_telemetry::logging;
use snafu::ensure;
use tokio::sync::oneshot::Sender;
use tokio::sync::{Barrier, Notify};
use tokio::task::JoinHandle;

use crate::error::{LoaderConflictSnafu, Result};
use crate::procedure::{
    BoxedProcedure, BoxedProcedureLoader, Context, Handle, LockKey, ProcedureId, ProcedureManager,
    Status,
};

/// Key-value store for persisting procedure's state.
#[async_trait]
trait StateStore {
    /// Puts `key` and `value` into the store.
    async fn put(&self, key: &str, value: &str) -> Result<()>;

    /// Returns the key-value pairs that have the same key `prefix`.
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, String)>>;

    /// Returns all key-value pairs in the store.
    async fn scan_all(&self) -> Result<Vec<(String, String)>>;

    /// Deletes key-value pairs by `keys`.
    async fn delete(&self, keys: &[String]) -> Result<()>;
}

/// Standalone [ProcedureManager] that maintains state on current machine.
pub struct StandaloneManager {
    /// Procedure loaders. The key is the type name of the procedure which the loader returns.
    loaders: Mutex<HashMap<String, BoxedProcedureLoader>>,
    manager_ctx: Arc<ManagerContext>,
}

impl StandaloneManager {
    /// Create a new StandaloneManager with default configurations.
    pub fn new() -> StandaloneManager {
        StandaloneManager {
            loaders: Mutex::new(HashMap::new()),
            manager_ctx: Arc::new(ManagerContext::new()),
        }
    }
}

#[async_trait]
impl ProcedureManager for StandaloneManager {
    fn register_loader(&self, name: &str, loader: BoxedProcedureLoader) -> Result<()> {
        let mut loaders = self.loaders.lock().unwrap();
        ensure!(!loaders.contains_key(name), LoaderConflictSnafu { name });

        loaders.insert(name.to_string(), loader);

        Ok(())
    }

    async fn submit(&self, procedure: BoxedProcedure) -> Result<Handle> {
        let procedure_id = ProcedureId::random();
        let notify = Arc::new(Notify::new());
        let (handle, sender) = Handle::new(procedure_id);
        let mut runner = Runner {
            procedure_id,
            procedure,
            notify: notify.clone(),
            manager_ctx: self.manager_ctx.clone(),
            sender,
        };

        let barrier = Arc::new(Barrier::new(2));
        let task_barrier = barrier.clone();
        common_runtime::spawn_bg(async move {
            // We need to wait until we inserts the meta into the manager context.
            task_barrier.wait().await;

            runner.run().await
        });

        let meta = ProcedureMeta {
            procedure_id,
            notify,
        };
        self.manager_ctx.insert_procedure(procedure_id, meta);
        // Notify the task to run the runner.
        barrier.wait().await;

        Ok(handle)
    }
}

struct ProcedureMeta {
    procedure_id: ProcedureId,
    /// Notify to wake up the runner.
    notify: Arc<Notify>,
}

/// Shared context of the manager.
struct ManagerContext {
    lock_map: LockMap,
    procedures: RwLock<HashMap<ProcedureId, ProcedureMeta>>,
}

impl ManagerContext {
    fn insert_procedure(&self, procedure_id: ProcedureId, meta: ProcedureMeta) {
        let mut procedures = self.procedures.write().unwrap();
        procedures.insert(procedure_id, meta);
    }

    /// Acquire the lock for the procedure or wait for the lock.
    async fn acquire_lock(&self, lock_key: &str, procedure_id: ProcedureId, notify: &Notify) {
        while !self.lock_map.acquire_lock(lock_key, procedure_id) {
            // Wait for notification and try again.
            notify.notified().await;
        }
    }

    /// Release the lock for the procedure and notify the new owner of the lock.
    fn release_lock(&self, lock_key: &str) {
        while let Some(owner) = self.lock_map.release_lock(lock_key) {
            // Try to notify the owner.
            let procedures = self.procedures.write().unwrap();
            if let Some(meta) = procedures.get(&owner) {
                meta.notify.notify_one();
                return;
            }
            // If the owner doesn't exist, release the lock again.
        }
    }

    /// Remove metadata of the procedure.
    ///
    /// The procedure MUST release the lock it holds before invoking this method.
    fn remove_procedure(&self, procedure_id: ProcedureId) {
        let mut procedures = self.procedures.write().unwrap();
        procedures.remove(&procedure_id);
    }
}

impl ManagerContext {
    fn new() -> ManagerContext {
        ManagerContext {
            lock_map: LockMap::new(),
            procedures: RwLock::new(HashMap::new()),
        }
    }
}

struct Runner {
    procedure_id: ProcedureId,
    procedure: BoxedProcedure,
    notify: Arc<Notify>,
    manager_ctx: Arc<ManagerContext>,
    sender: Sender<Result<()>>,
}

impl Runner {
    async fn run(mut self) {
        let lock_key = self.procedure.lock_key();
        let primary_lock_key = lock_key.as_ref().map(|k| k.primary_lock_key());

        // Acquire lock if necessary.
        if let Some(lock_key) = primary_lock_key {
            self.manager_ctx
                .acquire_lock(lock_key, self.procedure_id, &self.notify)
                .await;
        }
        // Execute the procedure.
        let execute_result = self.execute_procedure().await;

        if let Some(lock_key) = primary_lock_key {
            self.manager_ctx.release_lock(lock_key);
        }

        self.sender.send(execute_result);
        self.manager_ctx.remove_procedure(self.procedure_id);
    }

    async fn execute_procedure(&mut self) -> Result<()> {
        let ctx = Context {
            procedure_id: self.procedure_id,
        };

        loop {
            match self.procedure.execute(&ctx).await {
                Ok(status) => match status {
                    Status::Executing { persist } => {
                        if persist {
                            // TODO(yingwen): Persist the procedure.
                            todo!("Persist the procedure");
                        }
                    }
                    Status::Done => {
                        logging::info!(
                            "Procedure {}-{} done",
                            self.procedure.type_name(),
                            self.procedure_id
                        );
                        return Ok(());
                    }
                },
                Err(e) => {
                    logging::error!(e; "Failed to execute procedure {}-{}", self.procedure.type_name(), self.procedure_id);
                    // TODO(yingwen): Retry and rollback if it can't proceed.
                    return Err(e);
                }
            }
        }
    }
}

/// Data of the lock entry.
#[derive(Debug)]
struct Lock {
    /// Procedure id of current lock owner.
    owner: ProcedureId,
    /// Procedure ids of the waiter procedures.
    waiters: HashSet<ProcedureId>,
}

impl Lock {
    /// Returns a [Lock] with specific `owner` procedure.
    fn from_owner(owner: ProcedureId) -> Lock {
        Lock {
            owner,
            waiters: HashSet::new(),
        }
    }

    /// Try to pop a waiter from the waiter list and set it as owner.
    ///
    /// Returns false if there is no waiter in the waiter list.
    fn move_waiter_to_owner(&mut self) -> bool {
        let first = self.waiters.iter().copied().next();
        if let Some(waiter) = first {
            self.waiters.remove(&waiter);
            self.owner = waiter;

            true
        } else {
            false
        }
    }
}

/// Manages lock status for procedures.
struct LockMap {
    locks: RwLock<HashMap<String, Lock>>,
}

impl LockMap {
    /// Returns a new [LockMap].
    fn new() -> LockMap {
        LockMap {
            locks: RwLock::new(HashMap::new()),
        }
    }

    /// Acquire lock by `key` for procedure with specific `procedure_id`.
    ///
    /// Returns `true` the lock is acquired or holding by the procedure.
    fn acquire_lock(&self, key: &str, procedure_id: ProcedureId) -> bool {
        if self.hold_lock(key, procedure_id) {
            // Already holds this lock.
            return true;
        }

        let mut locks = self.locks.write().unwrap();
        if let Some(lock) = locks.get_mut(key) {
            // Lock already exists.
            if lock.owner == procedure_id {
                // The procedure is holding this lock.
                true
            } else {
                // Add this procedure to the waiter list.
                lock.waiters.insert(procedure_id);
                false
            }
        } else {
            locks.insert(key.to_string(), Lock::from_owner(procedure_id));
            true
        }
    }

    /// Release lock by `key`, returns the new owner of the lock.
    fn release_lock(&self, key: &str) -> Option<ProcedureId> {
        let mut locks = self.locks.write().unwrap();
        if let Some(lock) = locks.get_mut(key) {
            if !lock.move_waiter_to_owner() {
                // No body waits for this lock, we can remove the lock entry.
                locks.remove(key);
            } else {
                // Returns the new owner.
                return Some(lock.owner);
            }
        }

        None
    }

    /// Returns true if the procedure with specific `procedure_id` holds the
    /// lock of `key`.
    fn hold_lock(&self, key: &str, procedure_id: ProcedureId) -> bool {
        let locks = self.locks.read().unwrap();
        locks
            .get(key)
            .map(|lock| lock.owner == procedure_id)
            .unwrap_or(false)
    }
}
