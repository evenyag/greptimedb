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
    use lazy_static::lazy_static;
    use object_pool::Pool;

    use crate::v1::Rows;

    pub struct RowsPool {
        /// Pools for rows len:
        /// `<16, <64, <256, >=256
        pools: [Pool<Rows>; 4],
    }

    impl RowsPool {
        pub fn new(capacity: usize) -> Self {
            let pools = std::array::from_fn(|_| Pool::new(capacity, Rows::default));
            Self { pools }
        }

        pub fn try_pull(&self, expect_size: usize) -> Option<Rows> {
            let row_opt = match expect_size {
                0..16 => self.pools[0].try_pull(),
                16..64 => self.pools[1].try_pull(),
                64..256 => self.pools[2].try_pull(),
                _ => self.pools[3].try_pull(),
            };

            row_opt.map(|row| row.detach().1)
        }

        pub fn attach(&self, rows: Rows) {
            let len = rows.rows.len();
            match len {
                0..16 => self.pools[0].attach(rows),
                16..64 => self.pools[1].attach(rows),
                64..256 => self.pools[2].attach(rows),
                _ => self.pools[3].attach(rows),
            };
        }
    }

    lazy_static! {
        pub static ref PROM_ROWS_POOL: RowsPool = RowsPool::new(4000);
    }
}
