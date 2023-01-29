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
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use common_telemetry::logging;
use snafu::{ensure, ResultExt};
use tokio::sync::Notify;
use tokio::time;

use crate::error::{LoaderConflictSnafu, Result, ToJsonSnafu};
use crate::procedure::{
    BoxedProcedure, BoxedProcedureLoader, Context, LockKey, ProcedureId, ProcedureManager,
    ProcedureMessage, ProcedureState, ProcedureWithId, Status,
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
/// Procedure and its parent procedure id.
struct ProcedureAndParent(BoxedProcedure, Option<ProcedureId>);

/// Standalone [ProcedureManager] that maintains state on current machine.
pub struct StandaloneManager {
    manager_ctx: Arc<ManagerContext>,
    state_store: StateStoreRef,
}

impl Default for StandaloneManager {
    fn default() -> Self {
        Self::new()
    }
}

impl StandaloneManager {
    /// Create a new StandaloneManager with default configurations.
    pub fn new() -> StandaloneManager {
        StandaloneManager {
            manager_ctx: Arc::new(ManagerContext::new()),
            state_store: Arc::new(MemStateStore::default()),
        }
    }

    /// Submit a root procedure with given `procedure_id`.
    fn submit_root(&self, procedure_id: ProcedureId, procedure: BoxedProcedure) {
        let meta = Arc::new(ProcedureMeta {
            id: procedure_id,
            lock_notify: Notify::new(),
            parent_id: None,
            child_notify: Notify::new(),
            state: AtomicState::new(),
            parent_locks: Vec::new(),
            lock_key: procedure.lock_key(),
        });
        let runner = Runner {
            meta: meta.clone(),
            procedure,
            manager_ctx: self.manager_ctx.clone(),
            step: 0,
            store: ProcedureStore(self.state_store.clone()),
        };

        self.manager_ctx.insert_procedure(procedure_id, meta);

        common_runtime::spawn_bg(async move {
            // Run the root procedure.
            runner.run().await
        });
    }
}

#[async_trait]
impl ProcedureManager for StandaloneManager {
    fn register_loader(&self, name: &str, loader: BoxedProcedureLoader) -> Result<()> {
        let mut loaders = self.manager_ctx.loaders.lock().unwrap();
        ensure!(!loaders.contains_key(name), LoaderConflictSnafu { name });

        loaders.insert(name.to_string(), loader);

        Ok(())
    }

    async fn submit(&self, procedure: ProcedureWithId) -> Result<()> {
        self.submit_root(procedure.id, procedure.procedure);

        Ok(())
    }

    async fn recover(&self) -> Result<()> {
        let procedure_store = ProcedureStore(self.state_store.clone());
        let messages = procedure_store.load_messages().await?;

        for (procedure_id, message) in &messages {
            if message.parent_id.is_none() {
                // This is the root procedure. We only submit the root procedure as it can
                // submit sub-procedures to the manager.
                let Some(procedure_and_parent) = self.manager_ctx.load_one_procedure(*procedure_id) else {
                    // Try to load other procedures.
                    continue;
                };

                self.submit_root(*procedure_id, procedure_and_parent.0);
            }
        }

        Ok(())
    }

    async fn procedure_state(&self, procedure_id: ProcedureId) -> Result<Option<ProcedureState>> {
        Ok(self.manager_ctx.state(procedure_id))
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

#[derive(Debug)]
struct AtomicState(AtomicU8);

impl AtomicState {
    fn new() -> AtomicState {
        AtomicState(AtomicU8::new(ProcedureState::Running as u8))
    }

    fn set(&self, state: ProcedureState) {
        self.0.store(state as u8, Ordering::Relaxed);
    }

    fn get(&self) -> ProcedureState {
        match self.0.load(Ordering::Relaxed) {
            v if v == ProcedureState::Running as u8 => ProcedureState::Running,
            v if v == ProcedureState::Done as u8 => ProcedureState::Done,
            v if v == ProcedureState::Failed as u8 => ProcedureState::Failed,
            _ => unreachable!(),
        }
    }
}

/// Shared metadata of a procedure.
#[derive(Debug)]
struct ProcedureMeta {
    /// Id of this procedure.
    id: ProcedureId,
    /// Notify to waiting for a lock.
    lock_notify: Notify,
    /// Parent procedure id.
    parent_id: Option<ProcedureId>,
    /// Notify to waiting for subprocedures.
    child_notify: Notify,
    /// State of the procedure.
    state: AtomicState,
    /// Locks inherted from the parent procedure.
    parent_locks: Vec<LockKey>,
    /// Lock this procedure needs additionally.
    ///
    /// If the parent procedure already owns the lock this procedure
    /// needs, then we also set this field to `None`.
    lock_key: Option<LockKey>,
}

impl ProcedureMeta {
    /// Return all locks the procedure needs.
    fn locks_needed(&self) -> Vec<LockKey> {
        let num_locks = self.parent_locks.len() + if self.lock_key.is_some() { 1 } else { 0 };
        let mut locks = Vec::with_capacity(num_locks);
        locks.extend_from_slice(&self.parent_locks);
        if let Some(key) = &self.lock_key {
            locks.push(key.clone());
        }

        locks
    }
}

type ProcedureMetaRef = Arc<ProcedureMeta>;

/// Shared context of the manager.
struct ManagerContext {
    /// Procedure loaders. The key is the type name of the procedure which the loader returns.
    loaders: Mutex<HashMap<String, BoxedProcedureLoader>>,
    lock_map: LockMap,
    procedures: RwLock<HashMap<ProcedureId, ProcedureMetaRef>>,
    // TODO(yingwen): Now we never clean the messages. But when the root procedure is done, we
    // should be able to remove the its message and all its child messages.
    /// Messages loaded from the procedure store.
    messages: Mutex<HashMap<ProcedureId, ProcedureMessage>>,
}

impl ManagerContext {
    fn new() -> ManagerContext {
        ManagerContext {
            loaders: Mutex::new(HashMap::new()),
            lock_map: LockMap::new(),
            procedures: RwLock::new(HashMap::new()),
            messages: Mutex::new(HashMap::new()),
        }
    }

    fn contains_procedure(&self, procedure_id: ProcedureId) -> bool {
        let procedures = self.procedures.read().unwrap();
        procedures.contains_key(&procedure_id)
    }

    fn insert_procedure(&self, procedure_id: ProcedureId, meta: ProcedureMetaRef) {
        let mut procedures = self.procedures.write().unwrap();
        let old = procedures.insert(procedure_id, meta);
        assert!(old.is_none());
    }

    fn state(&self, procedure_id: ProcedureId) -> Option<ProcedureState> {
        let procedures = self.procedures.read().unwrap();
        procedures.get(&procedure_id).map(|meta| meta.state.get())
    }

    /// Acquire the lock for the procedure or wait for the lock.
    async fn acquire_lock(&self, lock_key: &LockKey, meta: &ProcedureMetaRef) {
        while !self
            .lock_map
            .acquire_lock(lock_key.key(), meta.clone())
            .await
        {}
    }

    /// Release the lock for the procedure and notify the new owner of the lock.
    fn release_lock(&self, lock_key: &LockKey) {
        self.lock_map.release_lock(lock_key.key());
    }

    /// Notify a parent procedure with given `procedure_id` by its subprocedure.
    fn notify_by_subprocedure(&self, procedure_id: ProcedureId) {
        let procedures = self.procedures.read().unwrap();
        if let Some(meta) = procedures.get(&procedure_id) {
            meta.child_notify.notify_one();
        }
    }

    /// Load procedure with specific `procedure_id`.
    fn load_one_procedure(&self, procedure_id: ProcedureId) -> Option<ProcedureAndParent> {
        let messages = self.messages.lock().unwrap();
        let message = messages.get(&procedure_id)?;

        let loaders = self.loaders.lock().unwrap();
        let loader = loaders.get(&message.type_name).or_else(|| {
            logging::error!(
                "Loader not found, procedure_id: {}, type_name: {}",
                procedure_id,
                message.type_name
            );
            None
        })?;

        let procedure = loader(&message.data)
            .map_err(|e| {
                logging::error!(
                    "Failed to load procedure data, key: {}, source: {}",
                    procedure_id,
                    e
                );
                e
            })
            .ok()?;

        Some(ProcedureAndParent(procedure, message.parent_id))
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

#[derive(Clone)]
struct ProcedureStore(StateStoreRef);

impl ProcedureStore {
    async fn store_procedure(
        &self,
        procedure_id: ProcedureId,
        step: u32,
        procedure: &BoxedProcedure,
        parent_id: Option<ProcedureId>,
    ) -> Result<()> {
        let type_name = procedure.type_name();
        let data = procedure.dump()?;

        let message = ProcedureMessage {
            type_name: type_name.to_string(),
            data,
            parent_id,
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

    async fn load_messages(&self) -> Result<HashMap<ProcedureId, ProcedureMessage>> {
        let key_values = self.0.scan_prefix(KEY_VERSION).await?;
        let mut messages = HashMap::new();
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
                let Some(message) = self.load_one_message(prev_key, prev_value) else {
                    // We don't abort the loading process and just ignore errors to ensure all remaining
                    // procedures are loaded.
                    continue;
                };
                messages.insert(prev_key.procedure_id, message);

                procedure_key_value = Some((curr_key, value));
            }
        }

        if let Some((last_key, last_value)) = &procedure_key_value {
            if let Some(message) = self.load_one_message(last_key, last_value) {
                messages.insert(last_key.procedure_id, message);
            }
        }

        Ok(messages)
    }

    fn load_one_message(&self, key: &ParsedKey, value: &str) -> Option<ProcedureMessage> {
        serde_json::from_str(value)
            .map_err(|e| {
                // `e` doesn't impl ErrorExt so we print it as normal error.
                logging::error!("Failed to parse value, key: {:?}, source: {}", key, e);
                e
            })
            .ok()
    }
}

struct Runner {
    meta: ProcedureMetaRef,
    procedure: BoxedProcedure,
    manager_ctx: Arc<ManagerContext>,
    step: u32,
    store: ProcedureStore,
}

impl Runner {
    /// Run the procedure.
    async fn run(mut self) {
        // We use the lock key in ProcedureMeta as it considers locks inherited from
        // its parent.
        let lock_key = self.meta.lock_key.clone();

        // TODO(yingwen): Support multiple lock keys.
        // Acquire lock if necessary.
        if let Some(key) = &lock_key {
            self.manager_ctx.acquire_lock(key, &self.meta).await;
        }
        // Execute the procedure.
        if let Err(e) = self.execute_procedure().await {
            logging::error!(
                e; "Failed to execute procedure {}-{}",
                self.procedure.type_name(),
                self.meta.id
            );
        }

        if let Some(key) = &lock_key {
            self.manager_ctx.release_lock(key);
        }
        // We can't remove the metadata of the procedure now as users and its parent might
        // need to query its state.
        // TODO(yingwen): 1. Add TTL to the metadata; 2. Only keep state in the procedure store
        // so we don't need to always store the metadata in memory after the procedure is done.
    }

    /// Submit a subprocedure.
    fn submit_sub(&self, procedure_id: ProcedureId, mut procedure: BoxedProcedure) {
        if let Some(procedure_and_parent) = self.manager_ctx.load_one_procedure(procedure_id) {
            // Try to load procedure state from the message to avoid re-run the subprocedure
            // from initial state.
            assert_eq!(self.meta.id, procedure_and_parent.1.unwrap());

            // Use the dumped procedure from the procedure store.
            procedure = procedure_and_parent.0;
        }

        if self.manager_ctx.contains_procedure(procedure_id) {
            // If the parent has already submitted this procedure, don't submit it again.
            return;
        }

        // Inherit locks from the parent procedure. This procedure can submit a subprocedure,
        // which indicates the procedure already owns the locks and is executing now.
        let parent_locks = self.meta.locks_needed();
        let mut child_lock = procedure.lock_key();
        if let Some(lock) = &child_lock {
            if parent_locks.contains(lock) {
                // If the parent procedure already holds this lock, we set this lock to None
                // so the subprocedure don't need to acquire lock again.
                child_lock = None;
            }
        }

        let meta = Arc::new(ProcedureMeta {
            id: procedure_id,
            lock_notify: Notify::new(),
            parent_id: Some(self.meta.id),
            child_notify: Notify::new(),
            state: AtomicState::new(),
            parent_locks,
            lock_key: child_lock,
        });
        let runner = Runner {
            meta: meta.clone(),
            procedure,
            manager_ctx: self.manager_ctx.clone(),
            step: 0,
            store: self.store.clone(),
        };

        self.manager_ctx.insert_procedure(procedure_id, meta);

        common_runtime::spawn_bg(async move {
            // Run the root procedure.
            runner.run().await
        });
    }

    async fn execute_procedure(&mut self) -> Result<()> {
        let ctx = Context {
            procedure_id: self.meta.id,
        };

        loop {
            match self.procedure.execute(&ctx).await {
                Ok(status) => {
                    if status.need_persist() {
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

                    match status {
                        Status::Executing { .. } => (),
                        Status::Suspended { subprocedures, .. } => {
                            self.on_suspended(subprocedures).await;
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

                            self.done();
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    logging::error!(e; "Failed to execute procedure {}-{}", self.procedure.type_name(), self.meta.id);

                    self.meta.state.set(ProcedureState::Failed);
                    // TODO(yingwen): Retry and rollback if it can't proceed.
                    return Err(e);
                }
            }
        }
    }

    async fn on_suspended(&self, subprocedures: Vec<ProcedureWithId>) {
        for subprocedure in subprocedures {
            self.submit_sub(subprocedure.id, subprocedure.procedure);
        }

        logging::info!(
            "Procedure {}-{} is waiting for subprocedures",
            self.procedure.type_name(),
            self.meta.id,
        );

        // Wait for subprocedures.
        self.meta.child_notify.notified().await;

        logging::info!(
            "Procedure {}-{} is waked up",
            self.procedure.type_name(),
            self.meta.id,
        );
    }

    async fn persist_procedure(&mut self) -> Result<()> {
        self.store
            .store_procedure(
                self.meta.id,
                self.step,
                &self.procedure,
                self.meta.parent_id,
            )
            .await?;
        self.step += 1;
        Ok(())
    }

    async fn commit_procedure(&mut self) -> Result<()> {
        self.store.commit_procedure(self.meta.id, self.step).await?;
        self.step += 1;
        Ok(())
    }

    fn done(&self) {
        // TODO(yingwen): Add files to remove list.
        logging::info!(
            "Procedure {}-{} done",
            self.procedure.type_name(),
            self.meta.id
        );

        // Mark the state of this procedure to done.
        self.meta.state.set(ProcedureState::Done);

        // Notify parent procedure.
        if let Some(parent_id) = self.meta.parent_id {
            self.manager_ctx.notify_by_subprocedure(parent_id);
        }
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
            // We need to use notify_one() since the waiter may not been registered yet.
            waiter.lock_notify.notify_one();
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
        meta.lock_notify.notified().await;

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
