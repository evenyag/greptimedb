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

//! Common structs and utilities for reading data.

pub mod batch;
pub mod compat;
pub mod dedup;
pub mod last_row;
pub mod merge;
pub mod plain_batch;
pub mod projection;
pub(crate) mod prune;
pub(crate) mod range;
pub(crate) mod scan_region;
pub(crate) mod scan_util;
pub(crate) mod seq_scan;
pub(crate) mod series_scan;
pub(crate) mod unordered_scan;

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::TryStreamExt;

use crate::error::Result;
use crate::memtable::BoxedBatchIterator;
use crate::read::batch::plain::PlainBatch;
pub use crate::read::batch::{Batch, BatchBuilder, BatchColumn};
use crate::read::prune::PruneReader;

/// Async [Batch] reader and iterator wrapper.
///
/// This is the data source for SST writers or internal readers.
pub enum Source {
    /// Source from a [BoxedBatchReader].
    Reader(BoxedBatchReader),
    /// Source from a [BoxedBatchIterator].
    Iter(BoxedBatchIterator),
    /// Source from a [BoxedBatchStream].
    Stream(BoxedBatchStream),
    /// Source from a [PruneReader].
    PruneReader(PruneReader),

    /// Source from a [BoxedPlainBatchStream].
    PlainStream(BoxedPlainBatchStream),
}

impl Source {
    /// Returns true if this source returns [PlainBatch].
    pub(crate) fn is_plain(&self) -> bool {
        match self {
            Source::Reader(_) | Source::Iter(_) | Source::Stream(_) | Source::PruneReader(_) => {
                false
            }
            Source::PlainStream(_) => true,
        }
    }

    /// Returns next [Batch] from this data source.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            Source::Reader(reader) => reader.next_batch().await,
            Source::Iter(iter) => iter.next().transpose(),
            Source::Stream(stream) => stream.try_next().await,
            Source::PruneReader(reader) => reader.next_batch().await,
            Source::PlainStream(_) => panic!("PlainStream is not supported"),
        }
    }

    /// Returns next [PlainBatch] from this data source.
    pub(crate) async fn next_plain_batch(&mut self) -> Result<Option<PlainBatch>> {
        match self {
            Source::Reader(_) | Source::Iter(_) | Source::Stream(_) | Source::PruneReader(_) => {
                panic!("Next plain batch is not supported")
            }
            Source::PlainStream(stream) => stream.try_next().await,
        }
    }
}

/// Async batch reader.
///
/// The reader must guarantee [Batch]es returned by it have the same schema.
#[async_trait]
pub trait BatchReader: Send {
    /// Fetch next [Batch].
    ///
    /// Returns `Ok(None)` when the reader has reached its end and calling `next_batch()`
    /// again won't return batch again.
    ///
    /// If `Err` is returned, caller should not call this method again, the implementor
    /// may or may not panic in such case.
    async fn next_batch(&mut self) -> Result<Option<Batch>>;
}

/// Pointer to [BatchReader].
pub type BoxedBatchReader = Box<dyn BatchReader>;

/// Pointer to a stream that yields [Batch].
pub type BoxedBatchStream = BoxStream<'static, Result<Batch>>;

#[async_trait::async_trait]
impl<T: BatchReader + ?Sized> BatchReader for Box<T> {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        (**self).next_batch().await
    }
}

/// Pointer to a stream that yields [PlainBatch].
pub type BoxedPlainBatchStream = BoxStream<'static, Result<PlainBatch>>;

/// Local metrics for scanners.
#[derive(Debug, Default)]
pub(crate) struct ScannerMetrics {
    /// Duration to prepare the scan task.
    prepare_scan_cost: Duration,
    /// Duration to build the (merge) reader.
    build_reader_cost: Duration,
    /// Duration to scan data.
    scan_cost: Duration,
    /// Duration to convert batches.
    convert_cost: Duration,
    /// Duration while waiting for `yield`.
    yield_cost: Duration,
    /// Number of batches returned.
    num_batches: usize,
    /// Number of rows returned.
    num_rows: usize,
    /// Number of mem ranges scanned.
    num_mem_ranges: usize,
    /// Number of file ranges scanned.
    num_file_ranges: usize,
}
