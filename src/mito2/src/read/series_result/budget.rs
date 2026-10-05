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

//! Hierarchical admission and prepaid replay capacity.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion_common::{DataFusionError, Result};

/// A query budget charges its parent as allocations grow. A replay budget buys
/// its full capacity before publication and suballocates it without double charging.
#[derive(Debug)]
pub(crate) struct BudgetPool {
    state: Mutex<State>,
    limit: usize,
    prepaid: bool,
    stage: &'static str,
    peak: AtomicUsize,
}

#[derive(Debug)]
struct State {
    parent: MemoryReservation,
    used: usize,
    closed: bool,
}

impl BudgetPool {
    pub(crate) fn query(parent: &Arc<dyn MemoryPool>, limit: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                parent: MemoryConsumer::new("BufferedSeriesScan::query").register(parent),
                used: 0,
                closed: false,
            }),
            limit,
            prepaid: false,
            stage: "query",
            peak: AtomicUsize::new(0),
        })
    }

    pub(crate) fn replay(parent: &Arc<dyn MemoryPool>, bytes: usize) -> Result<Arc<Self>> {
        let reservation = MemoryConsumer::new("BufferedSeriesScan::publication").register(parent);
        reservation.try_grow(bytes)?;
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                parent: reservation,
                used: 0,
                closed: false,
            }),
            limit: bytes,
            prepaid: true,
            stage: "replay",
            peak: AtomicUsize::new(0),
        }))
    }

    pub(crate) fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Unused credit is released now; escaped allocation leases retain only their
    /// live bytes and release them to the query when the final owner disappears.
    pub(crate) fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if self.prepaid && !state.closed {
            state.closed = true;
            let unused = state.parent.size() - state.used;
            state.parent.shrink(unused);
        }
    }
}

impl fmt::Display for BudgetPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BufferedSeriesScan {} budget", self.stage)
    }
}

impl MemoryPool for BudgetPool {
    fn name(&self) -> &str {
        "BufferedSeriesScan"
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.used += additional;
        self.peak.fetch_max(state.used, Ordering::Relaxed);
        if !self.prepaid || state.used > state.parent.size() {
            let extra = state.used - state.parent.size();
            state.parent.grow(extra);
        }
    }

    fn shrink(&self, _reservation: &MemoryReservation, bytes: usize) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.used -= bytes;
        if !self.prepaid || state.closed {
            state.parent.shrink(bytes);
        }
    }

    fn try_grow(&self, _reservation: &MemoryReservation, bytes: usize) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let available = self.limit.saturating_sub(state.used);
        if state.closed || bytes > available {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "buffered admission: stage={}, required={bytes}, available={available}, limit={}",
                self.stage, self.limit,
            )));
        }
        if !self.prepaid {
            state.parent.try_grow(bytes)?;
        }
        state.used += bytes;
        self.peak.fetch_max(state.used, Ordering::Relaxed);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).used
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;

    #[test]
    fn eight_prepaid_consumers_keep_capacity_until_drop() {
        let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1000));
        let query: Arc<dyn MemoryPool> = BudgetPool::query(&parent, 800);
        let pools = (0..8)
            .map(|_| BudgetPool::replay(&query, 100).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(800, parent.reserved());
        assert!(BudgetPool::replay(&query, 1).is_err());
        for pool in &pools {
            let dynamic: Arc<dyn MemoryPool> = pool.clone();
            let allocation = MemoryConsumer::new("payload").register(&dynamic);
            allocation.try_grow(75).unwrap();
            assert_eq!(
                800,
                parent.reserved(),
                "prepaid bytes must not be charged twice"
            );
            assert!(allocation.try_grow(26).is_err());
            drop(allocation);
            assert_eq!(
                800,
                parent.reserved(),
                "later batches still own promised credit"
            );
        }
        // One live allocation can outlive its abandoned consumer.
        let dynamic: Arc<dyn MemoryPool> = pools[0].clone();
        let escaped = MemoryConsumer::new("escaped").register(&dynamic);
        escaped.try_grow(25).unwrap();
        for pool in &pools {
            pool.close();
        }
        assert_eq!(25, parent.reserved());
        assert!(escaped.try_grow(1).is_err());
        drop(escaped);
        drop(dynamic);
        drop(pools);
        assert_eq!(0, parent.reserved());
    }

    #[test]
    fn parent_limit_and_query_diagnostics() {
        let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(90));
        let query: Arc<dyn MemoryPool> = BudgetPool::query(&parent, 100);
        let charge = MemoryConsumer::new("range").register(&query);
        assert!(charge.try_grow(95).is_err());
        assert_eq!(0, query.reserved());
        charge.try_grow(80).unwrap();
        let error = charge.try_grow(21).unwrap_err().to_string();
        assert!(error.contains("required=21, available=20, limit=100"));
        assert_eq!(80, parent.reserved());
        drop(charge);
        assert_eq!(0, parent.reserved());
    }
}
