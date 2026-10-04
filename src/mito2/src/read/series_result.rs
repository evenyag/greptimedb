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

mod ipc;
mod resources;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_ipc::CompressionType;
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
    Resident(Arc<BatchData>),
    File { part: Arc<FilePart>, batch: usize },
}

struct ResultData {
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
    pub fn series_cursor(&self, series: MetricSeriesId) -> Result<ResultCursor> {
        self.0.resources.lookup.measure(|| {
            let spans = &self.0.spans;
            let start = spans.partition_point(|s| s.series < series);
            let end = spans.partition_point(|s| s.series <= series);
            ResultCursor::new(self.clone(), Some(start..end))
        })
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
    spans: Option<Range<usize>>,
    position: usize,
    state: Option<CursorState>,
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
        self.handle
            .0
            .resources
            .drop_blocking((state, self.handle.clone()));
    }
}

impl ResultCursor {
    fn new(handle: ResultHandle, spans: Option<Range<usize>>) -> Result<Self> {
        let charge = handle
            .0
            .resources
            .reserve(Kind::Workspace, std::mem::size_of::<Self>() + 256)?;
        Ok(Self {
            handle,
            spans,
            position: 0,
            state: Some(CursorState {
                file: None,
                cached: None,
                _charge: charge,
            }),
        })
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
        let resources = result.resources.clone();
        let (state, lease) = resources
            .blocking(move || {
                let mut state = state;
                let data = match &state.cached {
                    Some((batch, data)) if *batch == item.0 => {
                        result.resources.count("cache_hits", 1);
                        data.clone()
                    }
                    _ => {
                        state.cached = None;
                        result.resources.count("cache_misses", 1);
                        let data = match &result.directory[item.0].source {
                            Source::Resident(data) => data.clone(),
                            Source::File { part, batch } => {
                                part.read(*batch, &result.schema, &mut state.file)?
                            }
                        };
                        state.cached = Some((item.0, data.clone()));
                        data
                    }
                };
                if item.1.end > data.batch.num_rows() {
                    return Err(fail("span exceeds batch"));
                }
                result.resources.count("requested_rows", item.1.len());
                let full_logical = logical_bytes(&data.batch)?;
                let selected_logical =
                    logical_bytes(&data.batch.slice(item.1.start, item.1.len()))?;
                result.resources.peak(
                    "lookup_unrelated_logical_bytes",
                    full_logical.saturating_sub(selected_logical),
                );
                result
                    .resources
                    .peak("lookup_backing_bytes", data._charge.bytes());
                result.resources.peak(
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
    resources: Arc<StoreResources>,
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
        let pending_charge = resources.reserve(Kind::Payload, 0)?;
        Ok(Self {
            resources,
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

    pub fn resources(&self) -> Arc<StoreResources> {
        self.resources.clone()
    }

    pub async fn append(self, batch: RecordBatch, placement: Placement) -> Result<Self> {
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelOnDrop(cancelled.clone());
        self.resources
            .clone()
            .blocking(move || {
                let mut builder = self;
                builder.append_blocking(batch, placement, &cancelled)?;
                Ok(builder)
            })
            .await
    }

    pub async fn finish(self) -> Result<ResultHandle> {
        self.resources
            .clone()
            .blocking(move || {
                let mut builder = self;
                builder.flush_batch()?;
                builder.finish_file()?;
                Ok(ResultHandle(Arc::new(ResultData {
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
        // Input is caller-owned, but the store's temporary retention is admitted too.
        let _input = self
            .resources
            .reserve(Kind::Workspace, unique_batch_bytes(&batch)?)?;
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
            let _chunk = self
                .resources
                .reserve(Kind::Workspace, checked_mul(estimate, 3)?)?;
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
            let _ids = self
                .resources
                .reserve(Kind::Workspace, checked_mul(rows, 32)?)?;
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
        let _workspace = self
            .resources
            .reserve(Kind::Workspace, checked_mul(estimate, 3)?)?;
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
        let _workspace = self
            .resources
            .reserve(Kind::Workspace, checked_mul(self.pending_bytes, 4)?)?;
        let plain = arrow(concat_batches(&self.plain_schema, &self.pending))?;
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
                let charge = self
                    .resources
                    .reserve(Kind::Payload, unique_batch_bytes(&compact)?)?;
                Source::Resident(BatchData::new(compact, charge)?)
            }
            Placement::File => {
                let storage = self.storage_batch(&plain)?;
                let predicted = ipc::write_bound(&storage, &self.resources.options)?;
                if self.writer.as_ref().is_some_and(|w| !w.fits(predicted)) {
                    self.finish_file()?;
                }
                if self.writer.is_none() {
                    self.writer = Some(OpenWriter::new(
                        self.resources.clone(),
                        self.storage_schema.clone(),
                    )?);
                }
                let writer = self
                    .writer
                    .as_mut()
                    .ok_or_else(|| fail("missing IPC writer"))?;
                writer.append(&storage)?;
                // File references are installed only by finish_file. The builder is private.
                self.resources.count("batches", 1);
                self.resources.count("rows", rows);
                return Ok(());
            }
        };
        self.directory.push(DirectoryEntry { source, rows });
        self.resources.count("batches", 1);
        self.resources.count("rows", rows);
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
fn unique_batch_bytes(batch: &RecordBatch) -> Result<usize> {
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
