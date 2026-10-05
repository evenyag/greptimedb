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

//! Bounded, immutable compact results. Independent of scan preparation (Stage 3).
//!
//! This PoC module is available to tests/developer benchmarks until Stage 4.
//! Files are ephemeral, query-owned IPC artifacts, not a persisted engine format.

pub(crate) mod budget;
mod ipc;
pub(crate) mod resources;
#[cfg(test)]
pub(crate) mod tests;

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_ipc::CompressionType;
use datafusion::execution::memory_pool::MemoryPool;
use datatypes::arrow::array::{Array, BinaryArray, DictionaryArray, UInt32Array};
use datatypes::arrow::compute::{cast, concat_batches, take};
use datatypes::arrow::datatypes::{DataType, Schema, SchemaRef, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use snafu::ResultExt;

use crate::error::{JoinSnafu, Result, UnexpectedSnafu};
use crate::read::series_compact::identities;
use crate::read::series_result::ipc::{FilePart, OpenWriter};
use crate::read::series_result::resources::{Charge, Kind};
use crate::series_index::MetricSeriesId;
use crate::sst::parquet::flat_format::primary_key_column_index;

pub use resources::{Snapshot, StoreResources};

/// Batch packing policy. Large series split under either policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    OneSeries,
    MultipleSeries,
}

/// Explicit placement chosen by the preparation caller; this module does not schedule spills.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    Resident,
    File,
}

/// Bounds are enforced independently of diagnostic instrumentation.
#[derive(Debug, Clone)]
pub struct StoreOptions {
    pub layout: Layout,
    pub batch_rows: usize,
    pub batch_bytes: usize,
    pub file_bytes: usize,
    pub file_batches: usize,
    pub file_metadata_bytes: usize,
    pub memory_bytes: usize,
    pub metadata_bytes: usize,
    pub disk_bytes: usize,
    pub compression: Option<CompressionType>,
    /// Complete, sorted, unique 22-byte compact identities. Never extended/replaced.
    pub fixed_keys: Option<Arc<BinaryArray>>,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            layout: Layout::MultipleSeries,
            batch_rows: 8192,
            batch_bytes: 8 * 1024 * 1024,
            file_bytes: 64 * 1024 * 1024,
            file_batches: 1024,
            file_metadata_bytes: 1024 * 1024,
            memory_bytes: 256 * 1024 * 1024,
            metadata_bytes: 32 * 1024 * 1024,
            disk_bytes: 1024 * 1024 * 1024,
            compression: None,
            fixed_keys: None,
        }
    }
}

pub(crate) fn fail(message: impl Into<String>) -> crate::error::Error {
    UnexpectedSnafu {
        reason: format!("series result: {}", message.into()),
    }
    .build()
}

pub(crate) fn checked_add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(|| fail("offset/size overflow"))
}

pub(crate) fn checked_mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b)
        .ok_or_else(|| fail("size multiplication overflow"))
}

pub(crate) fn arrow<T>(
    value: std::result::Result<T, datatypes::arrow::error::ArrowError>,
) -> Result<T> {
    value.map_err(|e| fail(e.to_string()))
}

/// Accounting follows the complete owned batch, including backing rows outside a slice.
struct BatchData {
    batch: RecordBatch,
    _charge: Arc<Charge>,
}

impl BatchData {
    fn new(batch: RecordBatch, charge: Charge) -> Result<Arc<Self>> {
        use datatypes::arrow::array::{ArrayData, make_array};
        use datatypes::arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
        struct PinnedBuffer {
            buffer: Buffer,
            _charge: Arc<Charge>,
        }
        impl AsRef<[u8]> for PinnedBuffer {
            fn as_ref(&self) -> &[u8] {
                self.buffer.as_slice()
            }
        }
        fn pin(data: ArrayData, charge: &Arc<Charge>) -> Result<ArrayData> {
            let wrap = |buffer: &Buffer| {
                Buffer::from(bytes::Bytes::from_owner(PinnedBuffer {
                    buffer: buffer.clone(),
                    _charge: charge.clone(),
                }))
            };
            let buffers = data.buffers().iter().map(wrap).collect();
            let nulls = data.nulls().map(|n| {
                NullBuffer::new(BooleanBuffer::new(wrap(n.buffer()), n.offset(), n.len()))
            });
            let children = data
                .child_data()
                .iter()
                .map(|child| pin(child.clone(), charge))
                .collect::<Result<Vec<_>>>()?;
            arrow(
                data.into_builder()
                    .buffers(buffers)
                    .nulls(nulls)
                    .child_data(children)
                    .build(),
            )
        }
        let charge = Arc::new(charge);
        let columns = batch
            .columns()
            .iter()
            .map(|a| pin(a.to_data(), &charge).map(make_array))
            .collect::<Result<Vec<_>>>()?;
        let batch = arrow(RecordBatch::try_new(batch.schema(), columns))?;
        Ok(Arc::new(Self {
            batch,
            _charge: charge,
        }))
    }
}

/// A replay slice pins its backing batch and reservation. Array buffers also pin the
/// reservation, so a consumer's cloned arrays remain accounted after this lease drops.
#[derive(Clone)]
pub struct BatchLease {
    data: Arc<BatchData>,
    rows: Range<usize>,
}

impl BatchLease {
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    /// Executes a synchronous consumer while retaining the allocation lease.
    pub fn with_batch<T>(&self, consume: impl FnOnce(&RecordBatch) -> T) -> T {
        consume(&self.data.batch.slice(self.rows.start, self.rows.len()))
    }

    pub fn retained_rows(&self) -> usize {
        self.data.batch.num_rows()
    }
    pub fn retained_bytes(&self) -> usize {
        self.data._charge.bytes()
    }
}

#[derive(Clone, Copy)]
struct Span {
    series: MetricSeriesId,
    batch: usize,
    start: usize,
    len: usize,
}

struct DirectoryEntry {
    source: Source,
    rows: usize,
}

enum Source {
    Pending,
    Resident(Arc<BatchData>),
    File { part: Arc<FilePart>, batch: usize },
}

#[derive(Clone, Copy)]
struct SourceBatch {
    rows: usize,
    bytes: usize,
}

struct ResultData {
    plain_schema: SchemaRef,
    source_batches: Vec<SourceBatch>,
    phase: &'static str,
    schema: SchemaRef,
    directory: Vec<DirectoryEntry>,
    spans: Vec<Span>,
    resources: Arc<StoreResources>,
    _metadata: Charge,
}

/// Immutable result; cloning does not copy indexes, payload or file metadata.
#[derive(Clone)]
pub struct ResultHandle(Arc<ResultData>);

impl ResultHandle {
    pub fn cursor(&self) -> Result<ResultCursor> {
        ResultCursor::new(self.clone(), None)
    }

    /// Binary-searches the sorted identity spans, without touching unrelated payload.
    pub(crate) fn cursor_in(&self, pool: Arc<dyn MemoryPool>) -> Result<ResultCursor> {
        ResultCursor::with_pool(self.clone(), None, pool)
    }

    pub fn series_cursor(&self, series: MetricSeriesId) -> Result<ResultCursor> {
        self.0.resources.lookup.measure(|| {
            let spans = &self.0.spans;
            let start = spans.partition_point(|s| s.series < series);
            let end = spans.partition_point(|s| s.series <= series);
            ResultCursor::new(self.clone(), Some(start..end))
        })
    }

    #[cfg(test)]
    pub(crate) fn series_cursor_in(
        &self,
        series: MetricSeriesId,
        pool: Arc<dyn MemoryPool>,
    ) -> Result<ResultCursor> {
        let spans = &self.0.spans;
        let start = spans.partition_point(|s| s.series < series);
        let end = spans.partition_point(|s| s.series <= series);
        ResultCursor::with_pool(self.clone(), Some(start..end), pool)
    }

    /// Replay cached storage using the consuming query's accounting and metrics.
    pub(crate) fn series_cursor_with_resources(
        &self,
        series: MetricSeriesId,
        pool: Arc<dyn MemoryPool>,
        resources: Arc<StoreResources>,
    ) -> Result<ResultCursor> {
        let start = self.0.spans.partition_point(|s| s.series < series);
        let end = self.0.spans.partition_point(|s| s.series <= series);
        ResultCursor::with_resources(self.clone(), Some(start..end), pool, resources)
    }

    /// Creates independent file storage before publication; the original stays usable
    /// if optional admission fails. Staging/decoding use the producer's workspace.
    pub(crate) async fn copy_to_cache(
        &self,
        resources: Arc<StoreResources>,
        query: Arc<StoreResources>,
        pool: Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelOnDrop(cancelled.clone());
        let worker_cancelled = cancelled.clone();
        let original = self.clone();
        let workspace = pool.clone();
        let copied = query
            .blocking(move || {
                let metadata = resources.reserve(Kind::Metadata, original.0._metadata.bytes())?;
                let mut files = BTreeMap::new();
                let mut directory = Vec::with_capacity(original.0.directory.len());
                for entry in &original.0.directory {
                    if worker_cancelled.load(Ordering::SeqCst) {
                        return Err(fail("cache copy cancelled"));
                    }
                    let source = match &entry.source {
                        Source::Pending => return Err(fail("cannot cache unfinished result")),
                        Source::Resident(batch) => Source::Resident(batch.clone()),
                        Source::File { part, batch } => {
                            let id = Arc::as_ptr(part) as usize;
                            if let std::collections::btree_map::Entry::Vacant(entry) =
                                files.entry(id)
                            {
                                entry.insert(part.copy_to_cache(resources.clone(), &workspace)?);
                            }
                            Source::File {
                                part: files[&id].clone(),
                                batch: *batch,
                            }
                        }
                    };
                    directory.push(DirectoryEntry {
                        source,
                        rows: entry.rows,
                    });
                }
                Ok(Self(Arc::new(ResultData {
                    plain_schema: original.0.plain_schema.clone(),
                    source_batches: original.0.source_batches.clone(),
                    phase: "cache",
                    schema: original.0.schema.clone(),
                    directory,
                    spans: original.0.spans.clone(),
                    resources,
                    _metadata: metadata,
                })))
            })
            .await?;
        // This exclusive unpublished copy can safely release borrowed resident
        // buffers as serialization proceeds. Failure leaves the original intact.
        copied.spill_in_with_cancel(pool, Some(cancelled)).await
    }

    pub(crate) fn cache_disk_bytes(&self) -> usize {
        let mut files = std::collections::BTreeSet::new();
        self.0
            .directory
            .iter()
            .filter_map(|entry| match &entry.source {
                Source::File { part, .. } if files.insert(Arc::as_ptr(part) as usize) => {
                    Some(part.disk_bytes())
                }
                _ => None,
            })
            .sum()
    }

    pub(crate) fn externally_pinned(&self) -> bool {
        Arc::strong_count(&self.0) > 1
    }

    pub(crate) fn stored_identities(&self) -> Vec<MetricSeriesId> {
        let mut ids = Vec::new();
        for span in &self.0.spans {
            if ids.last() != Some(&span.series) {
                ids.push(span.series);
            }
        }
        ids
    }

    pub(crate) fn has_series(&self, id: MetricSeriesId) -> bool {
        let spans = &self.0.spans;
        let index = spans.partition_point(|s| s.series < id);
        spans.get(index).is_some_and(|s| s.series == id)
    }

    pub(crate) fn has_resident(&self) -> bool {
        self.0
            .directory
            .iter()
            .any(|entry| matches!(entry.source, Source::Resident(_)))
    }

    /// Maximum incremental cursor/decode memory, including unrelated backing rows.
    pub(crate) fn replay_bytes(&self) -> Result<usize> {
        if self.0.directory.is_empty() {
            return Ok(0);
        }
        self.0.directory.iter().try_fold(1024, |maximum, entry| {
            let bytes = match &entry.source {
                Source::Resident(_) => 1024,
                Source::File { part, batch } => checked_add(part.replay_bytes(*batch)?, 1024)?,
                Source::Pending => return Err(fail("unfinalized result")),
            };
            Ok(maximum.max(bytes))
        })
    }

    /// Bounded reassembly preserves the decoder's batch transitions, which the
    /// existing merge algorithm observes when resolving equal-sequence ties.
    pub(crate) fn source_stream_in(
        self,
        pool: Arc<dyn MemoryPool>,
    ) -> crate::read::BoxedRecordBatchStream {
        Box::pin(async_stream::try_stream! {
            let mut cursor = self.cursor_in(pool.clone())?;
            let mut pending: Option<BatchLease> = None;
            let mut offset = 0;
            let mut directory_index = 0;
            let mut directory_offset = 0;
            for source in &self.0.source_batches {
                let (workspace, _) = self.source_window(source, &mut directory_index, &mut directory_offset)?;
                let charge = self.0.resources.reserve_in(Kind::Workspace, workspace, &pool)?.with_owner("source_reassembly");
                let mut pieces = Vec::new();
                let mut remaining = source.rows;
                while remaining > 0 {
                    if pending.is_none() { pending = cursor.next().await?; offset = 0; }
                    let lease = pending.as_ref().ok_or_else(|| fail("source buffer ended before its original batch"))?;
                    let rows = remaining.min(lease.num_rows() - offset);
                    // Release each decoded IPC allocation before advancing to the
                    // next fragment. Keeping slices here would retain all decoded
                    // windows for one original batch, multiplied by merge fan-in.
                    // Plain arrays also detach dictionary values from IPC backing.
                    let piece = self.0.resources.reconstruction.measure(|| {
                        let plain = lease.with_batch(|batch| convert(&batch.slice(offset, rows), &self.0.plain_schema))?;
                        let empty = RecordBatch::new_empty(self.0.plain_schema.clone());
                        arrow(concat_batches(&self.0.plain_schema, [&plain, &empty]))
                    })?;
                    pieces.push(piece);
                    remaining -= rows;
                    offset += rows;
                    if offset == lease.num_rows() { pending = None; }
                }
                let schema = self.0.schema.clone();
                let plain_schema = self.0.plain_schema.clone();
                let resources = self.0.resources.clone();
                let batch = resources.clone().blocking(move || {
                    let empty = RecordBatch::new_empty(plain_schema.clone());
                    let plain = resources.reconstruction.measure(|| arrow(concat_batches(&plain_schema, pieces.iter().chain(std::iter::once(&empty)))))?;
                    let batch = resources.reconstruction.measure(|| convert(&plain, &schema))?;
                    drop(pieces);
                    drop(plain);
                    let mut charge = charge;
                    charge.resize(unique_batch_bytes(&batch)?)?;
                    pin_batch(batch, charge)
                }).await?;
                yield batch;
            }
            if pending.is_some() || cursor.next().await?.is_some() { Err(fail("source buffer exceeds original batch boundaries"))?; }
            cursor.close().await?;
        })
    }

    /// Reassembly owns normalized fragments and bounded decoder windows, not all
    /// decoded IPC allocations covering an original source batch.
    fn source_window(
        &self,
        source: &SourceBatch,
        index: &mut usize,
        offset: &mut usize,
    ) -> Result<(usize, usize)> {
        let mut remaining = source.rows;
        let mut decoder = 1024;
        let mut pieces = 0;
        while remaining > 0 {
            let entry = self
                .0
                .directory
                .get(*index)
                .ok_or_else(|| fail("invalid source batch directory"))?;
            let bytes = match &entry.source {
                Source::Resident(_) => 1024,
                Source::File { part, batch } => part.replay_bytes(*batch)?,
                Source::Pending => return Err(fail("unfinalized source")),
            };
            decoder = decoder.max(bytes);
            pieces = checked_add(pieces, 1)?;
            let rows = remaining.min(entry.rows - *offset);
            remaining -= rows;
            *offset += rows;
            if *offset == entry.rows {
                *index += 1;
                *offset = 0;
            }
        }
        // Include per-fragment array objects/alignment, as well as overlapping
        // normalized fragments, concatenation, and compact dictionary rebuilding.
        let metadata = checked_mul(
            pieces,
            checked_add(1024, checked_mul(self.0.schema.fields().len(), 512)?)?,
        )?;
        let workspace = checked_add(checked_mul(source.bytes, 4)?, metadata)?;
        Ok((workspace, checked_mul(decoder, 2)?))
    }

    /// Complete simultaneous decoder/reassembly requirement for one source head.
    pub(crate) fn source_replay_bytes(&self) -> Result<usize> {
        let mut maximum = 1024;
        let mut index = 0;
        let mut offset = 0;
        for source in &self.0.source_batches {
            let (workspace, decoder) = self.source_window(source, &mut index, &mut offset)?;
            maximum = maximum.max(checked_add(workspace, decoder)?);
        }
        checked_add(maximum, 1024)
    }

    /// Bounds the backing referenced by any output-sized window, including the
    /// batches straddling its edges. Input windows may start inside a batch.
    pub(crate) fn merge_window_bytes(&self, rows: usize) -> Result<usize> {
        let mut first = 0;
        let mut count = 0;
        let mut bytes = 0;
        let mut maximum = 0;
        for (last, batch) in self.0.source_batches.iter().enumerate() {
            count = checked_add(count, batch.rows)?;
            bytes = checked_add(bytes, batch.bytes)?;
            while first < last && count.saturating_sub(self.0.source_batches[first].rows) > rows {
                count -= self.0.source_batches[first].rows;
                bytes -= self.0.source_batches[first].bytes;
                first += 1;
            }
            maximum = maximum.max(bytes);
        }
        Ok(maximum)
    }

    /// Extra backing capacity when a downstream consumer retains this whole
    /// identity. Resident payload is already charged to the immutable result.
    pub(crate) fn series_output_bytes(&self, id: MetricSeriesId, tags: usize) -> Result<usize> {
        let start = self.0.spans.partition_point(|span| span.series < id);
        self.0.spans[start..]
            .iter()
            .take_while(|span| span.series == id)
            .try_fold(0, |sum, span| {
                let payload = match &self.0.directory[span.batch].source {
                    Source::Resident(_) => 0,
                    Source::File { part, batch } => part.replay_bytes(*batch)?,
                    Source::Pending => return Err(fail("unfinalized result")),
                };
                checked_add(sum, checked_add(payload, tags)?)
            })
    }

    /// Only an unpublished, exclusively owned result may change placement.
    pub(crate) async fn spill(self) -> Result<Self> {
        let pool = self.0.resources.pool();
        self.spill_in(pool).await
    }

    pub(crate) async fn spill_in(self, pool: Arc<dyn MemoryPool>) -> Result<Self> {
        self.spill_in_with_cancel(pool, None).await
    }

    async fn spill_in_with_cancel(
        self,
        pool: Arc<dyn MemoryPool>,
        cancelled: Option<Arc<AtomicBool>>,
    ) -> Result<Self> {
        let resources = self.0.resources.clone();
        resources
            .clone()
            .blocking(move || {
                let mut data = Arc::try_unwrap(self.0)
                    .map_err(|_| fail("cannot spill a published/shared result"))?;
                let helper = ResultBuilder::with_workspace(
                    resources.clone(),
                    data.schema.clone(),
                    pool.clone(),
                )?;
                let mut writer: Option<OpenWriter> = None;
                let mut positions = Vec::new();
                fn finish(
                    writer: &mut Option<OpenWriter>,
                    positions: &mut Vec<usize>,
                    data: &mut ResultData,
                ) -> Result<()> {
                    if let Some(writer) = writer.take() {
                        let part = writer.finish()?;
                        for (batch, index) in positions.drain(..).enumerate() {
                            data.directory[index].source = Source::File {
                                part: part.clone(),
                                batch,
                            };
                        }
                    }
                    Ok(())
                }
                for index in 0..data.directory.len() {
                    if cancelled
                        .as_ref()
                        .is_some_and(|flag| flag.load(Ordering::SeqCst))
                    {
                        return Err(fail("cache conversion cancelled"));
                    }
                    let Source::Resident(batch) = &data.directory[index].source else {
                        continue;
                    };
                    let bytes = normalized_size(&batch.batch)?;
                    let _workspace =
                        resources.reserve_in(Kind::Workspace, checked_mul(bytes, 4)?, &pool)?;
                    let plain = helper.normalize(&batch.batch)?;
                    let storage = helper.storage_batch(&plain)?;
                    let bound = ipc::write_bound(&storage, &resources.options)?;
                    if writer.as_ref().is_some_and(|w| !w.fits(bound)) {
                        finish(&mut writer, &mut positions, &mut data)?;
                    }
                    if writer.is_none() {
                        writer = Some(OpenWriter::with_workspace(
                            resources.clone(),
                            helper.storage_schema.clone(),
                            pool.clone(),
                            data.phase,
                        )?);
                    }
                    writer
                        .as_mut()
                        .ok_or_else(|| fail("missing spill writer"))?
                        .append(&storage)?;
                    positions.push(index);
                    // Serialization has consumed this backing allocation. Pending references
                    // stay private until every corresponding file has finalized successfully.
                    data.directory[index].source = Source::Pending;
                }
                finish(&mut writer, &mut positions, &mut data)?;
                Ok(Self(Arc::new(data)))
            })
            .await
    }

    pub fn num_batches(&self) -> usize {
        self.0.directory.len()
    }
    pub fn num_spans(&self) -> usize {
        self.0.spans.len()
    }
}

/// Mutable positions/read state are never shared between cursors.
pub struct ResultCursor {
    handle: ResultHandle,
    resources: Arc<StoreResources>,
    spans: Option<Range<usize>>,
    position: usize,
    state: Option<CursorState>,
    pool: Arc<dyn MemoryPool>,
}

struct CursorState {
    // Close the file before releasing its payload and budget.
    file: Option<(usize, std::fs::File)>,
    cached: Option<(usize, Arc<BatchData>)>,
    _charge: Charge,
}

impl Drop for ResultCursor {
    fn drop(&mut self) {
        let state = self.state.take();
        // Pin the handle until the blocking worker closes the cursor's file.
        self.resources.drop_blocking((state, self.handle.clone()));
    }
}

impl ResultCursor {
    fn new(handle: ResultHandle, spans: Option<Range<usize>>) -> Result<Self> {
        let pool = handle.0.resources.pool();
        Self::with_pool(handle, spans, pool)
    }

    fn with_pool(
        handle: ResultHandle,
        spans: Option<Range<usize>>,
        pool: Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        let resources = handle.0.resources.clone();
        Self::with_resources(handle, spans, pool, resources)
    }

    fn with_resources(
        handle: ResultHandle,
        spans: Option<Range<usize>>,
        pool: Arc<dyn MemoryPool>,
        resources: Arc<StoreResources>,
    ) -> Result<Self> {
        let charge =
            resources.reserve_in(Kind::Workspace, std::mem::size_of::<Self>() + 256, &pool)?;
        Ok(Self {
            handle,
            resources,
            pool,
            spans,
            position: 0,
            state: Some(CursorState {
                file: None,
                cached: None,
                _charge: charge,
            }),
        })
    }

    /// Await normal cursor destruction before opening another range, so deferred
    /// blocking close work cannot accumulate retained payload across many ranges.
    pub(crate) async fn close(&mut self) -> Result<()> {
        let state = self.state.take();
        self.resources
            .blocking(move || {
                drop(state);
                Ok(())
            })
            .await
    }

    /// Cancellation poisons this cursor and drops its in-flight workspace after the
    /// blocking operation exits. Other cursors and immutable handles stay valid.
    pub async fn next(&mut self) -> Result<Option<BatchLease>> {
        let state = self
            .state
            .take()
            .ok_or_else(|| fail("cursor stopped after cancellation/error"))?;
        let item = match &self.spans {
            Some(range) => {
                if self.position >= range.len() {
                    self.state = Some(state);
                    return Ok(None);
                }
                let span = &self.handle.0.spans[checked_add(range.start, self.position)?];
                (span.batch, span.start..checked_add(span.start, span.len)?)
            }
            None => {
                if self.position >= self.handle.0.directory.len() {
                    self.state = Some(state);
                    return Ok(None);
                }
                (
                    self.position,
                    0..self.handle.0.directory[self.position].rows,
                )
            }
        };
        let result = self.handle.0.clone();
        let resources = self.resources.clone();
        let pool = self.pool.clone();
        let (state, lease) = resources
            .clone()
            .blocking(move || {
                let mut state = state;
                let data = match &state.cached {
                    Some((batch, data)) if *batch == item.0 => {
                        resources.count("cache_hits", 1);
                        data.clone()
                    }
                    _ => {
                        state.cached = None;
                        resources.count("cache_misses", 1);
                        let data = match &result.directory[item.0].source {
                            Source::Pending => return Err(fail("unfinalized result")),
                            Source::Resident(data) => data.clone(),
                            Source::File { part, batch } => part.read(
                                *batch,
                                &result.schema,
                                &mut state.file,
                                &pool,
                                &resources,
                            )?,
                        };
                        state.cached = Some((item.0, data.clone()));
                        data
                    }
                };
                if item.1.end > data.batch.num_rows() {
                    return Err(fail("span exceeds batch"));
                }
                resources.count("requested_rows", item.1.len());
                let full_logical = logical_bytes(&data.batch)?;
                let selected_logical =
                    logical_bytes(&data.batch.slice(item.1.start, item.1.len()))?;
                resources.peak(
                    "lookup_unrelated_logical_bytes",
                    full_logical.saturating_sub(selected_logical),
                );
                resources.peak("lookup_backing_bytes", data._charge.bytes());
                resources.peak(
                    "lookup_unrelated_rows",
                    data.batch.num_rows() - item.1.len(),
                );
                Ok((state, BatchLease { data, rows: item.1 }))
            })
            .await?;
        self.state = Some(state);
        self.position = checked_add(self.position, 1)?;
        Ok(Some(lease))
    }
}

/// Consuming async operations move ownership into the blocking task. Dropping the
/// awaiting future cannot strand a partial writer or its reservations.
pub struct ResultBuilder {
    source_batches: Vec<SourceBatch>,
    spill_on_pressure: bool,
    phase: &'static str,
    resources: Arc<StoreResources>,
    workspace_pool: Arc<dyn MemoryPool>,
    schema: SchemaRef,
    storage_schema: SchemaRef,
    plain_schema: SchemaRef,
    metadata: Charge,
    directory: Vec<DirectoryEntry>,
    spans: Vec<Span>,
    pending: Vec<RecordBatch>,
    pending_charge: Charge,
    pending_rows: usize,
    pending_bytes: usize,
    placement: Placement,
    last_series: Option<MetricSeriesId>,
    writer: Option<OpenWriter>,
}

impl ResultBuilder {
    pub async fn new(root: &Path, schema: SchemaRef, options: StoreOptions) -> Result<Self> {
        let root = root.to_owned();
        common_runtime::spawn_blocking_query(move || {
            let resources = StoreResources::new(&root, options)?;
            Self::with_resources(resources, schema)
        })
        .await
        .context(JoinSnafu)?
    }

    /// Reuses the same query budget across independent builders/results/cursors.
    pub fn with_resources(resources: Arc<StoreResources>, schema: SchemaRef) -> Result<Self> {
        let pool = resources.pool();
        let mut builder = Self::with_workspace(resources, schema, pool)?;
        builder.spill_on_pressure = false;
        Ok(builder)
    }

    /// Serialization and bounded staging consume prepaid preparation capacity.
    pub(crate) fn with_workspace(
        resources: Arc<StoreResources>,
        schema: SchemaRef,
        workspace_pool: Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        if schema.fields().len() < 4 {
            return Err(fail("incomplete compact schema"));
        }
        let mut metadata = resources.reserve(
            Kind::Metadata,
            checked_add(
                checked_mul(schema_memory(&schema)?, 3)?,
                checked_mul(schema.fields().len(), 128)?,
            )?,
        )?;
        let pk = primary_key_column_index(schema.fields().len());
        let fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let typ = if i == pk {
                    DataType::Binary
                } else {
                    match f.data_type() {
                        DataType::Dictionary(_, value) => *value.clone(),
                        t => t.clone(),
                    }
                };
                Arc::new(f.as_ref().clone().with_data_type(typ))
            })
            .collect::<Vec<_>>();
        let plain_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let mut fields = plain_schema.fields().to_vec();
        if resources.options.fixed_keys.is_some() {
            fields[pk] = Arc::new(fields[pk].as_ref().clone().with_data_type(
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
            ));
        }
        let storage_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let shared_bytes = shared_schema_memory(&[&schema, &plain_schema, &storage_schema])?;
        metadata.resize(shared_bytes)?;
        let pending_charge = resources.reserve_in(Kind::Payload, 0, &workspace_pool)?;
        Ok(Self {
            source_batches: Vec::new(),
            spill_on_pressure: true,
            phase: "result",
            resources,
            workspace_pool,
            schema,
            storage_schema,
            plain_schema,
            metadata,
            directory: vec![],
            spans: vec![],
            pending: vec![],
            pending_charge,
            pending_rows: 0,
            pending_bytes: 0,
            placement: Placement::Resident,
            last_series: None,
            writer: None,
        })
    }

    pub(crate) fn with_phase(mut self, phase: &'static str) -> Self {
        self.phase = phase;
        self.metadata = self.metadata.with_owner(phase);
        self.pending_charge = self.pending_charge.with_owner(phase);
        self
    }

    pub fn resources(&self) -> Arc<StoreResources> {
        self.resources.clone()
    }

    pub async fn append(self, batch: RecordBatch, placement: Placement) -> Result<Self> {
        self.append_owned(batch, placement, None).await
    }

    /// The complete merge admission already covers this batch. Pin that admission
    /// through queued/running serialization instead of charging its buffers twice.
    pub(crate) async fn append_prepared(
        self,
        batch: RecordBatch,
        placement: Placement,
        owner: Arc<Charge>,
    ) -> Result<Self> {
        self.append_owned(batch, placement, Some(owner)).await
    }

    async fn append_owned(
        self,
        batch: RecordBatch,
        placement: Placement,
        owner: Option<Arc<Charge>>,
    ) -> Result<Self> {
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelOnDrop(cancelled.clone());
        // Charge before dispatch, including time spent queued on the blocking runtime.
        let input = if owner.is_none() {
            Some(self.resources.reserve_in(
                Kind::Workspace,
                unique_batch_bytes(&batch)?,
                &self.workspace_pool,
            )?)
        } else {
            None
        };
        self.resources
            .clone()
            .blocking(move || {
                let _input = (input, owner);
                let mut builder = self;
                builder.append_blocking(batch, placement, &cancelled)?;
                Ok(builder)
            })
            .await
    }

    pub(crate) async fn spill_resident(self) -> Result<Self> {
        if !self
            .directory
            .iter()
            .any(|entry| matches!(entry.source, Source::Resident(_)))
            && (self.pending_rows == 0 || self.placement == Placement::File)
        {
            // File-bound staging is already bounded. Flushing it on every
            // threshold check would silently defeat multi-series batch packing.
            return Ok(self);
        }
        let workspace_pool = self.workspace_pool.clone();
        let spill_on_pressure = self.spill_on_pressure;
        let handle = self.finish().await?;
        let handle = if handle.has_resident() {
            handle.spill_in(workspace_pool.clone()).await?
        } else {
            handle
        };
        let data = Arc::try_unwrap(handle.0).map_err(|_| fail("shared unpublished builder"))?;
        let mut builder = Self::with_workspace(data.resources, data.schema, workspace_pool)?;
        builder = builder.with_phase(data.phase);
        builder.spill_on_pressure = spill_on_pressure;
        builder.last_series = data.spans.last().map(|s| s.series);
        builder.directory = data.directory;
        builder.spans = data.spans;
        builder.source_batches = data.source_batches;
        builder.metadata = data._metadata;
        builder.placement = Placement::File;
        Ok(builder)
    }

    pub async fn finish(self) -> Result<ResultHandle> {
        self.resources
            .clone()
            .blocking(move || {
                let mut builder = self;
                builder.flush_batch()?;
                builder.finish_file()?;
                Ok(ResultHandle(Arc::new(ResultData {
                    plain_schema: builder.plain_schema,
                    source_batches: builder.source_batches,
                    phase: builder.phase,
                    schema: builder.schema,
                    directory: builder.directory,
                    spans: builder.spans,
                    resources: builder.resources,
                    _metadata: builder.metadata,
                })))
            })
            .await
    }

    fn append_blocking(
        &mut self,
        batch: RecordBatch,
        placement: Placement,
        cancelled: &AtomicBool,
    ) -> Result<()> {
        if batch.schema() != self.schema {
            return Err(fail("compact input schema mismatch"));
        }
        if self.phase == "source" && batch.num_rows() > 0 {
            self.metadata.grow(std::mem::size_of::<SourceBatch>())?;
            self.source_batches
                .try_reserve_exact(1)
                .map_err(|e| fail(e.to_string()))?;
            self.source_batches.push(SourceBatch {
                rows: batch.num_rows(),
                bytes: checked_mul(normalized_size(&batch)?, 2)?,
            });
        }
        if self.placement != placement {
            self.flush_batch()?;
            self.finish_file()?;
            self.placement = placement;
        }
        let pk = primary_key_column_index(batch.num_columns());
        let mut offset = 0;
        while offset < batch.num_rows() {
            if cancelled.load(Ordering::SeqCst) {
                return Err(fail("append cancelled"));
            }
            let mut rows = (batch.num_rows() - offset).min(self.resources.options.batch_rows);
            let (piece, estimate) = loop {
                let piece = batch.slice(offset, rows);
                let estimate = normalized_size(&piece)?;
                if estimate <= self.resources.options.batch_bytes {
                    break (piece, estimate);
                }
                if rows == 1 {
                    return Err(fail("one row exceeds batch byte limit"));
                }
                rows /= 2;
            };
            let _chunk = self.resources.reserve_in(
                Kind::Workspace,
                checked_mul(estimate, 3)?,
                &self.workspace_pool,
            )?;
            // Normalize once per bounded input chunk, avoiding repeated dictionary scans
            // for thousands of tiny series sharing the same input dictionary.
            let piece = self.normalize(&piece)?;
            let keys = piece
                .column(pk)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| fail("compact keys must be Binary"))?;
            if keys.iter().flatten().any(|key| key.len() != 22) {
                return Err(fail("expected 22-byte compact keys"));
            }
            let _ids = self.resources.reserve_in(
                Kind::Workspace,
                checked_mul(rows, 32)?,
                &self.workspace_pool,
            )?;
            let ids = identities(piece.column(pk))?;
            if ids.windows(2).any(|w| w[0] > w[1]) {
                return Err(fail("input series are not ordered"));
            }
            let mut start = 0;
            while start < rows {
                let id = ids[start];
                if self.last_series.is_some_and(|last| last > id) {
                    return Err(fail("input series are not ordered"));
                }
                if self.resources.options.layout == Layout::OneSeries
                    && self.last_series != Some(id)
                {
                    self.flush_batch()?;
                }
                self.last_series = Some(id);
                let end = start + ids[start..].partition_point(|next| *next == id);
                let mut current = start;
                while current < end {
                    if cancelled.load(Ordering::SeqCst) {
                        return Err(fail("append cancelled"));
                    }
                    if self.pending_rows == self.resources.options.batch_rows {
                        self.flush_batch()?;
                    }
                    let mut len =
                        (end - current).min(self.resources.options.batch_rows - self.pending_rows);
                    loop {
                        let candidate = piece.slice(current, len);
                        let bytes = normalized_size(&candidate)?;
                        if checked_add(self.pending_bytes, bytes)?
                            <= self.resources.options.batch_bytes
                        {
                            self.push_piece(&candidate)?;
                            current = checked_add(current, len)?;
                            break;
                        }
                        if self.pending_rows > 0 {
                            self.flush_batch()?;
                            continue;
                        }
                        if len == 1 {
                            return Err(fail("one row exceeds batch byte limit"));
                        }
                        len /= 2;
                    }
                }
                start = end;
            }
            offset = checked_add(offset, rows)?;
        }
        Ok(())
    }

    fn normalize(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        self.resources.conversion.measure(|| {
            let columns = batch
                .columns()
                .iter()
                .zip(self.plain_schema.fields())
                .map(|(array, field)| {
                    let array = arrow(cast(array, field.data_type()))?;
                    // take compacts slices, so pending chunks cannot pin large source batches.
                    let indices = UInt32Array::from_iter_values(
                        0..u32::try_from(batch.num_rows())
                            .map_err(|_| fail("batch row count exceeds u32"))?,
                    );
                    arrow(take(&array, &indices, None))
                })
                .collect::<Result<Vec<_>>>()?;
            arrow(RecordBatch::try_new(self.plain_schema.clone(), columns))
        })
    }

    fn push_piece(&mut self, batch: &RecordBatch) -> Result<()> {
        let estimate = normalized_size(batch)?;
        let _workspace = self.resources.reserve_in(
            Kind::Workspace,
            checked_mul(estimate, 3)?,
            &self.workspace_pool,
        )?;
        let normalized = self.normalize(batch)?;
        let bytes = normalized.get_array_memory_size();
        self.pending_charge
            .grow(checked_add(bytes, std::mem::size_of::<RecordBatch>())?)?;
        self.pending
            .try_reserve_exact(1)
            .map_err(|e| fail(e.to_string()))?;
        self.pending_bytes = checked_add(self.pending_bytes, estimate)?;
        self.pending_rows = checked_add(self.pending_rows, normalized.num_rows())?;
        self.pending.push(normalized);
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.pending_rows == 0 {
            return Ok(());
        }
        let _workspace = self.resources.reserve_in(
            Kind::Workspace,
            checked_mul(self.pending_bytes, 4)?,
            &self.workspace_pool,
        )?;
        // Arrow's single-input concat returns slices. Preparation must detach
        // completed output from decoder/input leases instead of pinning an entire
        // source batch through a tiny result slice. A second, empty input forces
        // the normal concat path for the normalized value arrays.
        let empty = RecordBatch::new_empty(self.plain_schema.clone());
        let plain = if self.spill_on_pressure && self.pending.len() == 1 {
            arrow(concat_batches(
                &self.plain_schema,
                self.pending.iter().chain(std::iter::once(&empty)),
            ))?
        } else {
            arrow(concat_batches(&self.plain_schema, &self.pending))?
        };
        self.pending.clear();
        self.pending.shrink_to_fit();
        self.pending_charge.clear();
        self.pending_rows = 0;
        self.pending_bytes = 0;
        let ids = identities(plain.column(primary_key_column_index(plain.num_columns())))?;
        let index = checked_add(
            self.directory.len(),
            self.writer.as_ref().map_or(0, |w| w.rows.len()),
        )?;
        let mut start = 0;
        while start < ids.len() {
            let end = start + ids[start..].partition_point(|s| *s == ids[start]);
            self.metadata.grow(std::mem::size_of::<Span>())?;
            self.spans
                .try_reserve_exact(1)
                .map_err(|e| fail(e.to_string()))?;
            self.spans.push(Span {
                series: ids[start],
                batch: index,
                start,
                len: end - start,
            });
            start = end;
        }
        self.metadata.grow(std::mem::size_of::<DirectoryEntry>())?;
        self.directory
            .try_reserve_exact(1)
            .map_err(|e| fail(e.to_string()))?;
        let rows = plain.num_rows();
        let source = match self.placement {
            Placement::Resident => {
                let compact = self
                    .resources
                    .reconstruction
                    .measure(|| convert(&plain, &self.schema))?;
                let charge = match self
                    .resources
                    .reserve(Kind::Payload, unique_batch_bytes(&compact)?)
                {
                    Ok(charge) => charge.with_owner(self.phase),
                    Err(_) if self.spill_on_pressure => {
                        // Concurrent preparation may consume free resident capacity
                        // after placement selection. Protected writer credit still
                        // permits this unpublished batch to go straight to disk.
                        drop(compact);
                        self.resources.count("resident_admission_spills", 1);
                        self.placement = Placement::File;
                        self.write_plain(&plain)?;
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                };
                Source::Resident(BatchData::new(compact, charge)?)
            }
            Placement::File => {
                self.write_plain(&plain)?;
                return Ok(());
            }
        };
        self.directory.push(DirectoryEntry { source, rows });
        self.resources.count("batches", 1);
        self.resources.count("rows", rows);
        Ok(())
    }

    fn write_plain(&mut self, plain: &RecordBatch) -> Result<()> {
        let storage = self.storage_batch(plain)?;
        let predicted = ipc::write_bound(&storage, &self.resources.options)?;
        if self.writer.as_ref().is_some_and(|w| !w.fits(predicted)) {
            self.finish_file()?;
        }
        if self.writer.is_none() {
            self.writer = Some(OpenWriter::with_workspace(
                self.resources.clone(),
                self.storage_schema.clone(),
                self.workspace_pool.clone(),
                self.phase,
            )?);
        }
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| fail("missing IPC writer"))?;
        writer.append(&storage)?;
        // File references are installed only by finish_file. The builder is private.
        self.resources.count("batches", 1);
        self.resources.count("rows", plain.num_rows());
        Ok(())
    }

    fn storage_batch(&self, plain: &RecordBatch) -> Result<RecordBatch> {
        let Some(values) = &self.resources.options.fixed_keys else {
            return Ok(plain.clone());
        };
        let pk = primary_key_column_index(plain.num_columns());
        let array = plain
            .column(pk)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| fail("expected Binary compact keys"))?;
        let mut keys = Vec::with_capacity(array.len());
        for key in array.iter() {
            let key = key.ok_or_else(|| fail("null compact key"))?;
            let mut low = 0;
            let mut high = values.len();
            while low < high {
                let mid = low + (high - low) / 2;
                if values.value(mid) < key {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            if low == values.len() || values.value(low) != key {
                return Err(fail("identity absent from fixed dictionary"));
            }
            let idx = low;
            keys.push(u32::try_from(idx).map_err(|_| fail("fixed dictionary exceeds u32"))?);
        }
        let mut columns = plain.columns().to_vec();
        columns[pk] = Arc::new(arrow(DictionaryArray::<UInt32Type>::try_new(
            UInt32Array::from(keys),
            values.clone(),
        ))?);
        arrow(RecordBatch::try_new(self.storage_schema.clone(), columns))
    }

    fn finish_file(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            let part = writer.finish()?;
            self.directory
                .try_reserve_exact(part.rows.len())
                .map_err(|e| fail(e.to_string()))?;
            for batch in 0..part.rows.len() {
                self.directory.push(DirectoryEntry {
                    rows: part.rows[batch],
                    source: Source::File {
                        part: part.clone(),
                        batch,
                    },
                });
            }
        }
        Ok(())
    }
}

fn convert(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(a, f)| {
            // Detach file-wide dictionaries before reconstructing a batch-local merge dictionary.
            let plain = match a.data_type() {
                DataType::Dictionary(_, value) => arrow(cast(a, value))?,
                _ => a.clone(),
            };
            arrow(cast(&plain, f.data_type()))
        })
        .collect::<Result<Vec<_>>>()?;
    arrow(RecordBatch::try_new(schema.clone(), columns))
}

fn schema_memory(schema: &SchemaRef) -> Result<usize> {
    schema
        .fields()
        .iter()
        .try_fold(std::mem::size_of::<Schema>(), |n, f| {
            checked_add(n, f.size())
        })
        .and_then(|n| {
            schema.metadata().iter().try_fold(n, |n, (k, v)| {
                checked_add(n, checked_add(k.capacity(), v.capacity())?)
            })
        })
}

/// Conservative normalized allocation bound, including alignment and dictionary expansion.
fn normalized_size(batch: &RecordBatch) -> Result<usize> {
    batch.columns().iter().try_fold(1024usize, |sum, array| {
        let data = array.to_data();
        let bytes = if matches!(array.data_type(), DataType::Dictionary(_, _)) {
            let values = data
                .child_data()
                .first()
                .ok_or_else(|| fail("missing dictionary values"))?;
            // Maximum single dictionary value bounds expansion without materializing it.
            let max = (0..values.len()).try_fold(0, |max, i| {
                arrow(values.slice(i, 1).get_slice_memory_size()).map(|size| max.max(size))
            })?;
            checked_mul(checked_add(max, 8)?, array.len())?
        } else {
            arrow(data.get_slice_memory_size())?
        };
        checked_add(sum, checked_add(bytes, 256)?)
    })
}

/// Benchmark snapshots expose operation costs separately from logical/storage traffic.
pub fn operation_values(resources: &StoreResources) -> BTreeMap<String, usize> {
    resources
        .operations
        .clone_inner()
        .iter()
        .map(|m| (m.value().name().to_string(), m.value().as_usize()))
        .collect()
}

/// Counts unique backing buffers (IPC arrays can share one whole encoded block).
pub(crate) fn unique_batch_bytes(batch: &RecordBatch) -> Result<usize> {
    fn visit(data: &datatypes::arrow::array::ArrayData, allocations: &mut BTreeMap<usize, usize>) {
        for buffer in data
            .buffers()
            .iter()
            .chain(data.nulls().map(|n| n.buffer()))
        {
            allocations
                .entry(buffer.data_ptr().as_ptr() as usize)
                .or_insert(buffer.capacity());
        }
        for child in data.child_data() {
            visit(child, allocations);
        }
    }
    let mut allocations = BTreeMap::new();
    for array in batch.columns() {
        visit(&array.to_data(), &mut allocations);
    }
    allocations
        .values()
        .try_fold(checked_mul(batch.num_columns(), 256)?, |n, size| {
            checked_add(n, *size)
        })
}

/// Output fields can borrow a resident batch; only newly allocated backing buffers
/// consume replay credit. Existing buffers retain their original ownership leases.
pub(crate) fn incremental_batch_bytes(
    batch: &RecordBatch,
    borrowed: &RecordBatch,
) -> Result<usize> {
    fn buffers(data: &datatypes::arrow::array::ArrayData, output: &mut BTreeMap<usize, usize>) {
        for buffer in data
            .buffers()
            .iter()
            .chain(data.nulls().map(|n| n.buffer()))
        {
            output
                .entry(buffer.data_ptr().as_ptr() as usize)
                .or_insert(buffer.capacity());
        }
        for child in data.child_data() {
            buffers(child, output);
        }
    }
    let mut existing = BTreeMap::new();
    let mut output = BTreeMap::new();
    for array in borrowed.columns() {
        buffers(&array.to_data(), &mut existing);
    }
    for array in batch.columns() {
        buffers(&array.to_data(), &mut output);
    }
    output
        .iter()
        .filter(|(address, _)| !existing.contains_key(address))
        .try_fold(checked_mul(batch.num_columns(), 256)?, |n, (_, bytes)| {
            checked_add(n, *bytes)
        })
}

pub(crate) fn pin_batch(batch: RecordBatch, charge: Charge) -> Result<RecordBatch> {
    Ok(BatchData::new(batch, charge)?.batch.clone())
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn logical_bytes(batch: &RecordBatch) -> Result<usize> {
    batch.columns().iter().try_fold(0, |sum, array| {
        checked_add(sum, arrow(array.to_data().get_slice_memory_size())?)
    })
}

fn shared_schema_memory(schemas: &[&SchemaRef]) -> Result<usize> {
    let mut seen = std::collections::HashSet::new();
    let mut total = 0;
    for schema in schemas {
        total = checked_add(
            total,
            checked_add(
                std::mem::size_of::<Schema>(),
                checked_mul(
                    schema.fields().len(),
                    std::mem::size_of::<Arc<datatypes::arrow::datatypes::Field>>(),
                )?,
            )?,
        )?;
        for (key, value) in schema.metadata() {
            total = checked_add(total, checked_add(key.capacity(), value.capacity())?)?;
        }
        for field in schema.fields() {
            if seen.insert(Arc::as_ptr(field) as usize) {
                total = checked_add(total, field.size())?;
            }
        }
    }
    Ok(total)
}
