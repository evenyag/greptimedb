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

#![feature(let_chains)]

pub mod error;
pub mod helper;

pub mod prom_store {
    pub mod remote {
        pub use greptime_proto::prometheus::remote::*;
    }
}

pub mod region;
pub mod v1;

pub use greptime_proto;
pub use prost::DecodeError;

pub mod pool {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use lazy_static::lazy_static;

    // use object_pool::Pool;
    use crate::v1::Rows;

    pub struct RowsPool {
        /// Rows with capacity == 1, <= 8, <= 64, > 64
        pools: [Mutex<VecDeque<Rows>>; 4],
        capacity: usize,
    }

    impl RowsPool {
        pub fn new(capacity: usize) -> Self {
            Self {
                pools: [
                    Mutex::new(VecDeque::with_capacity(4000)),
                    Mutex::new(VecDeque::with_capacity(400)),
                    Mutex::new(VecDeque::with_capacity(100)),
                    Mutex::new(VecDeque::with_capacity(50)),
                ],
                capacity,
            }
        }

        pub fn try_pull(&self, expect_capacity: usize) -> Option<Rows> {
            match expect_capacity {
                0 => None,
                1 => self.pools[0].lock().unwrap().pop_front(),
                2..=8 => self.pools[1].lock().unwrap().pop_front(),
                9..=64 => self.pools[2].lock().unwrap().pop_front(),
                _ => self.pools[3].lock().unwrap().pop_front(),
            }
        }

        pub fn attach(&self, rows: Rows) {
            match rows.rows.capacity() {
                0 => (),
                1 => self.attach_pool(0, rows),
                2..=8 => self.attach_pool(1, rows),
                9..=64 => self.attach_pool(2, rows),
                _ => self.attach_pool(3, rows),
            }
        }

        fn attach_pool(&self, i: usize, rows: Rows) {
            let mut pools = self.pools[i].lock().unwrap();
            if pools.len() >= self.capacity {
                pools.pop_front();
            }
            pools.push_back(rows);
        }
    }

    lazy_static! {
        pub static ref PROM_ROWS_POOL: RowsPool = RowsPool::new(4000);
    }
}
