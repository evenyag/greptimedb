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

//! Sequential scan.

use std::sync::Arc;

use async_stream::try_stream;
use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{RecordBatchStreamAdaptor, SendableRecordBatchStream};
use common_time::range::TimestampRange;
use snafu::ResultExt;
use store_api::storage::ScanRequest;
use table::predicate::Predicate;

use crate::access_layer::AccessLayerRef;
use crate::error::Result;
use crate::memtable::MemtableRef;
use crate::read::compat::{self, CompatReader};
use crate::read::merge::MergeReaderBuilder;
use crate::read::projection::ProjectionMapper;
use crate::read::BatchReader;
use crate::sst::file::FileHandle;

/// Scans a region and returns rows in a sorted sequence.
///
/// The output order is always `order by primary key, time index`.
pub struct SeqScan {
    /// Region SST access layer.
    access_layer: AccessLayerRef,
    /// Maps projected Batches to RecordBatches.
    mapper: Arc<ProjectionMapper>,
    /// Original scan request to scan memtable.
    // TODO(yingwen): Remove this if memtable::iter() takes another struct.
    request: ScanRequest,

    /// Time range filter for time index.
    time_range: Option<TimestampRange>,
    /// Predicate to push down.
    predicate: Option<Predicate>,
    /// Memtables to scan.
    memtables: Vec<MemtableRef>,
    /// Handles to SST files to scan.
    files: Vec<FileHandle>,
}

impl SeqScan {
    /// Creates a new [SeqScan].
    #[must_use]
    pub(crate) fn new(
        access_layer: AccessLayerRef,
        mapper: ProjectionMapper,
        request: ScanRequest,
    ) -> SeqScan {
        SeqScan {
            access_layer,
            mapper: Arc::new(mapper),
            time_range: None,
            predicate: None,
            memtables: Vec::new(),
            files: Vec::new(),
            request,
        }
    }

    /// Set time range filter for time index.
    #[must_use]
    pub(crate) fn with_time_range(mut self, time_range: Option<TimestampRange>) -> Self {
        self.time_range = time_range;
        self
    }

    /// Set predicate to push down.
    #[must_use]
    pub(crate) fn with_predicate(mut self, predicate: Option<Predicate>) -> Self {
        self.predicate = predicate;
        self
    }

    /// Set memtables to read.
    #[must_use]
    pub(crate) fn with_memtables(mut self, memtables: Vec<MemtableRef>) -> Self {
        self.memtables = memtables;
        self
    }

    /// Set files to read.
    #[must_use]
    pub(crate) fn with_files(mut self, files: Vec<FileHandle>) -> Self {
        self.files = files;
        self
    }

    /// Builds a stream for the query.
    pub async fn build(&self) -> Result<SendableRecordBatchStream> {
        // Scans all memtables and SSTs. Builds a merge reader to merge results.
        let mut builder = MergeReaderBuilder::new();
        for mem in &self.memtables {
            let iter = mem.iter(Some(self.mapper.column_ids()), &self.request.filters);
            builder.push_batch_iter(iter);
        }
        for file in &self.files {
            let reader = self
                .access_layer
                .read_sst(file.clone())
                .predicate(self.predicate.clone())
                .time_range(self.time_range)
                .projection(Some(self.mapper.column_ids().to_vec()))
                .build()
                .await?;
            if compat::has_same_columns(self.mapper.metadata(), reader.metadata()) {
                builder.push_batch_reader(Box::new(reader));
            } else {
                let compat_reader =
                    CompatReader::new(&self.mapper, reader.metadata().clone(), reader)?;
                builder.push_batch_reader(Box::new(compat_reader));
            }
        }
        let mut reader = builder.build().await?;
        // Creates a stream to poll the batch reader and convert batch into record batch.
        let mapper = self.mapper.clone();
        let stream = try_stream! {
            while let Some(batch) = reader.next_batch().await.map_err(BoxedError::new).context(ExternalSnafu)? {
                yield mapper.convert(&batch)?;
            }
        };
        let stream = Box::pin(RecordBatchStreamAdaptor::new(
            self.mapper.output_schema(),
            Box::pin(stream),
        ));

        Ok(stream)
    }
}

#[cfg(test)]
impl SeqScan {
    /// Returns number of SST files to scan.
    pub(crate) fn num_files(&self) -> usize {
        self.files.len()
    }
}
