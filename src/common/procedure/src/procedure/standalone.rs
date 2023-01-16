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

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use common_telemetry::logging;
use snafu::{ensure, ResultExt};
use tokio::sync::oneshot::Sender;
use tokio::sync::{Barrier, Notify};
use tokio::time;

use crate::error::{LoaderConflictSnafu, Result, ToJsonSnafu};
use crate::procedure::{
    BoxedProcedure, BoxedProcedureLoader, Context, Handle, ProcedureId, ProcedureManager,
    ProcedureMessage, Status,
};

const ERR_WAIT_DURATION: Duration = Duration::from_secs(30);
const KEY_VERSION: &str = "v1";

/// Key-value store for persisting procedure's state.
#[async_trait]
trait StateStore: Send + Sync {
    /// Puts `key` and `value` into the store.
    // TODO(yingwen): Maybe move the key/value?
    async fn put(&self, key: &str, value: &str) -> Result<()>;

    /// Returns the key-value pairs that have the same key `prefix`.
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, String)>>;

    /// Deletes key-value pairs by `keys`.
    async fn delete(&self, keys: &[String]) -> Result<()>;
}

type StateStoreRef = Arc<dyn StateStore>;

/// Standalone [ProcedureManager] that maintains state on current machine.
pub struct StandaloneManager {
    /// Procedure loaders. The key is the type name of the procedure which the loader returns.
    loaders: Mutex<HashMap<String, BoxedProcedureLoader>>,
    manager_ctx: Arc<ManagerContext>,
    state_store: StateStoreRef,
}

impl StandaloneManager {
    /// Create a new StandaloneManager with default configurations.
    pub fn new() -> StandaloneManager {
        StandaloneManager {
            loaders: Mutex::new(HashMap::new()),
            manager_ctx: Arc::new(ManagerContext::new()),
            state_store: Arc::new(MemStateStore::default()),
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
        let meta = Arc::new(ProcedureMeta {
            id: procedure_id,
            notify: Notify::new(),
        });
        let (handle, sender) = Handle::new(procedure_id);
        let runner = Runner {
            meta: meta.clone(),
            procedure,
            manager_ctx: self.manager_ctx.clone(),
            sender,
            step: 0,
            store: ProcedureStore(self.state_store.clone()),
        };

        let barrier = Arc::new(Barrier::new(2));
        let task_barrier = barrier.clone();
        common_runtime::spawn_bg(async move {
            // We need to wait until we inserts the meta into the manager context.
            task_barrier.wait().await;

            runner.run().await
        });

        self.manager_ctx.insert_procedure(procedure_id, meta);
        // Notify the task to run the runner.
        barrier.wait().await;

        Ok(handle)
    }

    async fn recover(&self) -> Result<()> {
        let procedure_store = ProcedureStore(self.state_store.clone());
        let procedures = procedure_store.load_procedures(&self.loaders).await?;

        for procedure in procedures {
            self.submit(procedure).await?;
        }

        Ok(())
    }
}

#[derive(Default)]
struct MemStateStore(Mutex<BTreeMap<String, String>>);

#[async_trait]
impl StateStore for MemStateStore {
    async fn put(&self, key: &str, value: &str) -> Result<()> {
        let mut tree = self.0.lock().unwrap();
        tree.insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, String)>> {
        let tree = self.0.lock().unwrap();
        let key_values = tree
            .range(prefix.to_string()..)
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Ok(key_values)
    }

    async fn delete(&self, keys: &[String]) -> Result<()> {
        let mut tree = self.0.lock().unwrap();
        for key in keys {
            tree.remove(key);
        }
        Ok(())
    }
}

// We can add a cancelation flag here.
/// Shared metadata of a procedure.
#[derive(Debug)]
struct ProcedureMeta {
    /// Id of this procedure.
    id: ProcedureId,
    /// Notify to wake up the runner.
    notify: Notify,
}

type ProcedureMetaRef = Arc<ProcedureMeta>;

/// Shared context of the manager.
struct ManagerContext {
    lock_map: LockMap,
    procedures: RwLock<HashMap<ProcedureId, ProcedureMetaRef>>,
}

impl ManagerContext {
    fn insert_procedure(&self, procedure_id: ProcedureId, meta: ProcedureMetaRef) {
        let mut procedures = self.procedures.write().unwrap();
        procedures.insert(procedure_id, meta);
    }

    /// Acquire the lock for the procedure or wait for the lock.
    async fn acquire_lock(&self, lock_key: &str, meta: &ProcedureMetaRef) {
        while !self.lock_map.acquire_lock(lock_key, meta.clone()).await {}
    }

    /// Release the lock for the procedure and notify the new owner of the lock.
    fn release_lock(&self, lock_key: &str) {
        self.lock_map.release_lock(lock_key);
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

#[derive(Debug)]
struct ParsedKey {
    procedure_id: ProcedureId,
    step: u32,
    is_committed: bool,
}

impl fmt::Display for ParsedKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}_{}_{}.{}",
            KEY_VERSION,
            self.procedure_id,
            self.step,
            if self.is_committed { "commit" } else { "step" }
        )
    }
}

impl ParsedKey {
    fn parse_str(input: &str) -> Option<ParsedKey> {
        let mut parts = input.split('.');
        let prefix = parts.next()?;
        let suffix = parts.next()?;
        let is_committed = match suffix {
            "commit" => true,
            "step" => false,
            _ => return None,
        };

        let mut substrings = prefix.split('_');
        let procedure_id = ProcedureId::parse_str(substrings.next()?)?;
        let step = substrings.next()?.parse().ok()?;
        if substrings.next().is_some() {
            return None;
        }

        Some(ParsedKey {
            procedure_id,
            step,
            is_committed,
        })
    }
}

struct ProcedureStore(StateStoreRef);

impl ProcedureStore {
    async fn store_procedure(
        &self,
        procedure_id: ProcedureId,
        step: u32,
        procedure: &BoxedProcedure,
    ) -> Result<()> {
        let type_name = procedure.type_name();
        let data = procedure.dump()?;

        let message = ProcedureMessage {
            type_name: type_name.to_string(),
            data,
        };
        let key = ParsedKey {
            procedure_id,
            step,
            is_committed: false,
        }
        .to_string();
        let value = serde_json::to_string(&message).context(ToJsonSnafu)?;

        self.0.put(&key, &value).await?;

        Ok(())
    }

    async fn commit_procedure(&self, procedure_id: ProcedureId, step: u32) -> Result<()> {
        let key = ParsedKey {
            procedure_id,
            step,
            is_committed: true,
        }
        .to_string();
        self.0.put(&key, "").await?;

        Ok(())
    }

    async fn load_procedures(
        &self,
        loaders: &Mutex<HashMap<String, BoxedProcedureLoader>>,
    ) -> Result<Vec<BoxedProcedure>> {
        let key_values = self.0.scan_prefix(KEY_VERSION).await?;
        let mut procedures = Vec::new();
        let mut procedure_key_value = None;
        for (key, value) in key_values {
            let Some(curr_key) = ParsedKey::parse_str(&key) else {
                logging::info!("Unknown key while loading procedures, key: {}", key);
                continue;
            };
            let Some((prev_key, prev_value)) = &procedure_key_value else {
                procedure_key_value = Some((curr_key, value));
                continue;
            };
            if prev_key.procedure_id == curr_key.procedure_id && !curr_key.is_committed {
                // The same procedure, update value.
                procedure_key_value = Some((curr_key, value));
            } else if prev_key.procedure_id == curr_key.procedure_id {
                // The procedure is committed
                procedure_key_value = None;
            } else {
                // A new procedure, now we can load previous procedure.
                let Some(procedure) = self.load_one_procedure(prev_key, prev_value, loaders) else {
                    // We don't abort the loading process and just ignore errors to ensure all remaining
                    // procedures are loaded.
                    continue;
                };
                procedures.push(procedure);

                procedure_key_value = Some((curr_key, value));
            }
        }

        if let Some((last_key, last_value)) = &procedure_key_value {
            if let Some(procedure) = self.load_one_procedure(last_key, last_value, loaders) {
                procedures.push(procedure);
            }
        }

        Ok(procedures)
    }

    fn load_one_procedure(
        &self,
        key: &ParsedKey,
        value: &str,
        loader_map: &Mutex<HashMap<String, BoxedProcedureLoader>>,
    ) -> Option<BoxedProcedure> {
        let message: ProcedureMessage = serde_json::from_str(&value)
            .map_err(|e| {
                // `e` doesn't impl ErrorExt so we print it as normal error.
                logging::error!("Failed to parse value, key: {:?}, source: {}", key, e);
                e
            })
            .ok()?;
        let loaders = loader_map.lock().unwrap();
        let loader = loaders.get(&message.type_name).or_else(|| {
            logging::error!(
                "Loader not found, key: {:?}, type_name: {}",
                key,
                message.type_name
            );
            None
        })?;
        loader(&message.data)
            .map_err(|e| {
                logging::error!(
                    "Failed to load procedure data, key: {:?}, source: {}",
                    key,
                    e
                );
                e
            })
            .ok()
    }
}

struct Runner {
    meta: ProcedureMetaRef,
    procedure: BoxedProcedure,
    manager_ctx: Arc<ManagerContext>,
    sender: Sender<Result<()>>,
    step: u32,
    store: ProcedureStore,
}

impl Runner {
    async fn run(mut self) {
        let lock_key = self.procedure.lock_key();
        let primary_lock_key = lock_key.as_ref().map(|k| k.primary_lock_key());

        // Acquire lock if necessary.
        if let Some(lock_key) = primary_lock_key {
            self.manager_ctx.acquire_lock(lock_key, &self.meta).await;
        }
        // Execute the procedure.
        let execute_result = self.execute_procedure().await;

        if let Some(lock_key) = primary_lock_key {
            self.manager_ctx.release_lock(lock_key);
        }

        // Ignore the result since the callers can drop the handle if they don't
        // need the return value.
        let _ = self.sender.send(execute_result);
        self.manager_ctx.remove_procedure(self.meta.id);
    }

    async fn execute_procedure(&mut self) -> Result<()> {
        let ctx = Context {
            procedure_id: self.meta.id,
        };

        loop {
            match self.procedure.execute(&ctx).await {
                Ok(status) => match status {
                    Status::Executing { persist } => {
                        if persist {
                            if let Err(e) = self.persist_procedure().await {
                                logging::error!(
                                    e; "Failed to persist procedure {}-{}",
                                    self.procedure.type_name(),
                                    self.meta.id
                                );

                                time::sleep(ERR_WAIT_DURATION).await;
                                continue;
                            }
                        }
                    }
                    Status::Done => {
                        if let Err(e) = self.commit_procedure().await {
                            logging::error!(
                                e; "Failed to commit procedure {}-{}",
                                self.procedure.type_name(),
                                self.meta.id
                            );

                            time::sleep(ERR_WAIT_DURATION).await;
                            continue;
                        }

                        // TODO(yingwen): Add files to remove list.
                        logging::info!(
                            "Procedure {}-{} done",
                            self.procedure.type_name(),
                            self.meta.id
                        );
                        return Ok(());
                    }
                },
                Err(e) => {
                    logging::error!(e; "Failed to execute procedure {}-{}", self.procedure.type_name(), self.meta.id);
                    // TODO(yingwen): Retry and rollback if it can't proceed.
                    return Err(e);
                }
            }
        }
    }

    async fn persist_procedure(&mut self) -> Result<()> {
        self.store
            .store_procedure(self.meta.id, self.step, &self.procedure)
            .await?;
        self.step += 1;
        Ok(())
    }

    async fn commit_procedure(&mut self) -> Result<()> {
        self.store.commit_procedure(self.meta.id, self.step).await?;
        self.step += 1;
        Ok(())
    }
}

/// Data of the lock entry.
#[derive(Debug)]
struct Lock {
    /// Current lock owner.
    owner: ProcedureMetaRef,
    /// Waiter procedures.
    waiters: VecDeque<ProcedureMetaRef>,
}

impl Lock {
    /// Returns a [Lock] with specific `owner` procedure.
    fn from_owner(owner: ProcedureMetaRef) -> Lock {
        Lock {
            owner,
            waiters: VecDeque::new(),
        }
    }

    /// Try to pop a waiter from the waiter list, set it as owner
    /// and wake up the new owner.
    ///
    /// Returns false if there is no waiter in the waiter list.
    fn switch_owner(&mut self) -> bool {
        if let Some(waiter) = self.waiters.pop_front() {
            waiter.notify.notify_one();
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

    /// Acquire lock by `key` for procedure with specific `meta`.
    ///
    /// Returns `true` if the lock is acquired or holding by the procedure.
    ///
    /// Thought `meta` is clonable, callers must ensure that only one `meta`
    /// is acquiring and holding the lock at the same time.
    async fn acquire_lock(&self, key: &str, meta: ProcedureMetaRef) -> bool {
        {
            let mut locks = self.locks.write().unwrap();
            if let Some(lock) = locks.get_mut(key) {
                // Lock already exists, but we don't expect that a procedure acquires
                // the same lock again.
                assert_ne!(lock.owner.id, meta.id);

                // Add this procedure to the waiter list. Here we don't check
                // whether the procedure is already in the waiter list as we
                // expect that a procedure should not wait for two lock simultaneously.
                lock.waiters.push_back(meta.clone());
            } else {
                locks.insert(key.to_string(), Lock::from_owner(meta));

                return true;
            }
        }

        // Wait for notify.
        meta.notify.notified().await;

        // It is possible that the procedure is notified by other signal so
        // we check the lock again.
        self.hold_lock(key, meta.id)
    }

    /// Release lock by `key`.
    fn release_lock(&self, key: &str) {
        let mut locks = self.locks.write().unwrap();
        if let Some(lock) = locks.get_mut(key) {
            if !lock.switch_owner() {
                // No body waits for this lock, we can remove the lock entry.
                locks.remove(key);
            }
        }
    }

    /// Returns true if the procedure with specific `procedure_id` holds the
    /// lock of `key`.
    fn hold_lock(&self, key: &str, procedure_id: ProcedureId) -> bool {
        let locks = self.locks.read().unwrap();
        locks
            .get(key)
            .map(|lock| lock.owner.id == procedure_id)
            .unwrap_or(false)
    }
}
