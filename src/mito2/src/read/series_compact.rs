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

//! Query-owned identity mappings, compact batches, and deferred metric tags.

use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use api::v1::SemanticType;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, Time,
};
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, DictionaryArray, UInt32Array,
};
use datatypes::arrow::datatypes::{Schema, SchemaRef, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::DataType;
use datatypes::value::Value;
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec, SparsePrimaryKeyCodec};
use parquet::arrow::arrow_reader::RowSelection;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::PartitionRange;
use store_api::storage::ColumnId;

use crate::error::{
    ComputeArrowSnafu, ComputeVectorSnafu, CreateDefaultSnafu, DecodeSnafu, EncodeSnafu,
    MergeCandidateSeriesSnafu, NewRecordBatchSnafu, Result, UnexpectedSnafu,
};
use crate::read::flat_projection::FlatProjectionMapper;
use crate::read::pruner::PartitionPruner;
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::PartitionMetrics;
use crate::series_index::MetricSeriesId;
use crate::sst::parquet::file_range::FileRange;
use crate::sst::parquet::flat_format::primary_key_column_index;
use crate::sst::parquet::reader::ReaderMetrics;

/// Synchronous operation cost. Thread CPU clocks are sampled on the same thread,
/// never across an await. Elapsed includes descheduling and mutex waits; CPU does not.
#[derive(Clone)]
pub(crate) struct OperationMetrics {
    pub(crate) calls: Count,
    pub(crate) elapsed: Time,
    pub(crate) cpu: Time,
    unavailable: Count,
}

impl OperationMetrics {
    pub(crate) fn new(set: &ExecutionPlanMetricsSet, partition: usize, name: &str) -> Self {
        Self {
            calls: MetricBuilder::new(set).counter(format!("{name}_calls"), partition),
            elapsed: MetricBuilder::new(set).subset_time(format!("{name}_elapsed"), partition),
            cpu: MetricBuilder::new(set).subset_time(format!("{name}_cpu"), partition),
            unavailable: MetricBuilder::new(set)
                .counter(format!("{name}_cpu_unavailable"), partition),
        }
    }

    pub(crate) fn measure<T>(&self, operation: impl FnOnce() -> T) -> T {
        self.calls.add(1);
        let _elapsed = self.elapsed.timer();
        let start = thread_cpu_time();
        let result = operation();
        match (start, thread_cpu_time()) {
            (Some(start), Some(end)) if end >= start => self.cpu.add_duration(end - start),
            _ => self.unavailable.add(1),
        }
        result
    }
}

fn thread_cpu_time() -> Option<std::time::Duration> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        use nix::time::{ClockId, clock_gettime};
        let time = clock_gettime(ClockId::CLOCK_THREAD_CPUTIME_ID).ok()?;
        Some(std::time::Duration::new(
            time.tv_sec().try_into().ok()?,
            time.tv_nsec().try_into().ok()?,
        ))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

pub(crate) fn measure_operation<T>(
    metrics: Option<&OperationMetrics>,
    operation: impl FnOnce() -> T,
) -> T {
    match metrics {
        Some(metrics) => metrics.measure(operation),
        None => operation(),
    }
}

pub(crate) type MeasuredKeyRead = (Count, CompactReadMetrics);

/// Query-owned read-path attribution. Storage time is awaited fetch latency, not
/// physical disk wait; object-store reads may be served by the OS page cache.
#[derive(Clone)]
pub(crate) struct CompactReadMetrics {
    pub(crate) decode: OperationMetrics,
    pub(crate) cache_lookup: OperationMetrics,
    pub(crate) cache_insert: OperationMetrics,
    pub(crate) range_assembly: OperationMetrics,
    pub(crate) fetch_elapsed: Time,
    pub(crate) store_elapsed: Time,
    pub(crate) write_cache_elapsed: Time,
    pub(crate) requested_bytes: Count,
    pub(crate) cache_bytes: Count,
    pub(crate) store_bytes: Count,
    pub(crate) write_cache_bytes: Count,
    pub(crate) fetch_calls: Count,
    pub(crate) store_calls: Count,
    pub(crate) decoded_batches: Count,
    pub(crate) decoded_rows: Count,
}

impl CompactReadMetrics {
    fn new(set: &ExecutionPlanMetricsSet, partition: usize, phase: &str) -> Self {
        let count =
            |suffix| MetricBuilder::new(set).counter(format!("{phase}_{suffix}"), partition);
        let time =
            |suffix| MetricBuilder::new(set).subset_time(format!("{phase}_{suffix}"), partition);
        let op = |suffix| OperationMetrics::new(set, partition, &format!("{phase}_{suffix}"));
        Self {
            decode: op("decode"),
            cache_lookup: op("cache_lookup"),
            cache_insert: op("cache_insert"),
            range_assembly: op("range_assembly"),
            fetch_elapsed: time("fetch_elapsed"),
            store_elapsed: time("store_elapsed"),
            write_cache_elapsed: time("write_cache_elapsed"),
            requested_bytes: count("requested_bytes"),
            cache_bytes: count("page_cache_bytes"),
            store_bytes: count("store_payload_bytes"),
            write_cache_bytes: count("write_cache_bytes"),
            fetch_calls: count("fetch_calls"),
            store_calls: count("store_calls"),
            decoded_batches: count("decoded_batches"),
            decoded_rows: count("decoded_rows"),
        }
    }
}

/// Metrics are query-owned and enabled independently of verbose fetch diagnostics.
#[derive(Clone)]
pub(crate) struct CompactMetrics {
    pub(crate) discovery_read: CompactReadMetrics,
    pub(crate) preflight_read: CompactReadMetrics,
    pub(crate) data_read: CompactReadMetrics,
    pub(crate) mapping_build: OperationMetrics,
    pub(crate) mapping_select: OperationMetrics,
    pub(crate) key_synthesis: OperationMetrics,
    pub(crate) data_filter: OperationMetrics,
    pub(crate) schema_adapt: OperationMetrics,
    pub(crate) discovery_catalog: OperationMetrics,
    pub(crate) catalog_entries: Count,
    pub(crate) tag_fill: OperationMetrics,
    pub(crate) assembly: OperationMetrics,
    pub(crate) assembly_ids: OperationMetrics,
    pub(crate) tag_column: OperationMetrics,
    pub(crate) tag_lock_wait: Time,
    pub(crate) assembly_rows: Count,
    pub(crate) assembly_tag_columns: Count,
    pub(crate) mapping_cache_hits: Count,
    pub(crate) mapping_index_hits: Count,
    pub(crate) mapping_key_reads: Count,
    pub(crate) mapping_runs: Count,
    pub(crate) mapping_index_elapsed: Time,
    pub(crate) preflight_prune_elapsed: Time,
    pub(crate) preflight_keys: Count,
    pub(crate) preflight_key_bytes: Count,
    pub(crate) discovery_key_bytes: Count,
    pub(crate) catalog_bytes: Gauge,
    pub(crate) mapping_bytes: Gauge,
    pub(crate) discovery_keys: Count,
    pub(crate) preflight_groups: Count,
    pub(crate) data_readers: Count,
    pub(crate) audited_decoders: Count,
    pub(crate) key_decode_violations: Count,
    pub(crate) preflight_time: Time,
    pub(crate) tag_time: Time,
}

impl CompactMetrics {
    pub(crate) fn new(set: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let count = |name| MetricBuilder::new(set).counter(name, partition);
        // Every compact decoder is audited before construction and before receiving bytes.
        // Consequently successful audited readers cannot decode a primary-key page.
        count("compact_data_primary_key_pages_decoded");
        Self {
            discovery_read: CompactReadMetrics::new(set, partition, "compact_discovery"),
            preflight_read: CompactReadMetrics::new(set, partition, "compact_preflight"),
            data_read: CompactReadMetrics::new(set, partition, "compact_data"),
            mapping_select: OperationMetrics::new(set, partition, "compact_mapping_select"),
            key_synthesis: OperationMetrics::new(set, partition, "compact_key_synthesis"),
            data_filter: OperationMetrics::new(set, partition, "compact_data_filter"),
            schema_adapt: OperationMetrics::new(set, partition, "compact_schema_adapt"),
            discovery_catalog: OperationMetrics::new(set, partition, "compact_discovery_catalog"),
            catalog_entries: count("compact_catalog_entries"),
            mapping_build: OperationMetrics::new(set, partition, "compact_mapping_build"),
            tag_fill: OperationMetrics::new(set, partition, "compact_preflight_tag_fill"),
            assembly: OperationMetrics::new(set, partition, "compact_assembly"),
            assembly_ids: OperationMetrics::new(set, partition, "compact_assembly_ids"),
            tag_column: OperationMetrics::new(set, partition, "compact_tag_column"),
            tag_lock_wait: MetricBuilder::new(set).subset_time("compact_tag_lock_wait", partition),
            assembly_rows: count("compact_assembly_rows"),
            assembly_tag_columns: count("compact_assembly_tag_columns"),
            mapping_cache_hits: count("compact_mapping_cache_hits"),
            mapping_index_hits: count("compact_mapping_index_hits"),
            mapping_key_reads: count("compact_mapping_key_reads"),
            mapping_runs: count("compact_mapping_runs"),
            mapping_index_elapsed: MetricBuilder::new(set)
                .subset_time("compact_mapping_index_elapsed", partition),
            preflight_prune_elapsed: MetricBuilder::new(set)
                .subset_time("compact_preflight_prune_elapsed", partition),
            preflight_key_bytes: count("preflight_primary_key_bytes"),
            discovery_key_bytes: count("discovery_primary_key_bytes"),
            catalog_bytes: MetricBuilder::new(set).gauge("compact_catalog_peak_bytes", partition),
            mapping_bytes: MetricBuilder::new(set)
                .gauge("compact_mapping_retained_bytes", partition),
            preflight_keys: count("preflight_primary_key_rows"),
            discovery_keys: count("discovery_primary_key_rows"),
            preflight_groups: count("mapping_preflight_row_groups"),
            data_readers: count("compact_data_readers"),
            audited_decoders: count("compact_audited_decoders"),
            key_decode_violations: count("compact_primary_key_decode_violations"),
            preflight_time: MetricBuilder::new(set)
                .subset_time("mapping_preflight_cost", partition),
            tag_time: MetricBuilder::new(set).subset_time("tag_assembly_cost", partition),
        }
    }
}

/// One identity's half-open run in absolute source-row coordinates.
#[derive(Clone, Debug)]
pub(crate) struct SeriesRowRange {
    pub(crate) series: MetricSeriesId,
    pub(crate) rows: Range<usize>,
}

/// Complete source mapping. Construction validates coverage, not just selected identities.
#[derive(Debug)]
pub(crate) struct SeriesRowMapping {
    pub(crate) runs: Vec<SeriesRowRange>,
    pub(crate) num_rows: usize,
}

impl SeriesRowMapping {
    pub(crate) fn try_new(runs: Vec<SeriesRowRange>, num_rows: usize) -> Result<Self> {
        let mut end = 0;
        let mut previous = None;
        for run in &runs {
            ensure!(
                run.rows.start == end
                    && run.rows.end > end
                    && run.rows.end <= num_rows
                    && previous.is_none_or(|id| id <= run.series),
                UnexpectedSnafu {
                    reason: format!(
                        "incomplete or invalid compact identity mapping at row {end}: {run:?}"
                    ),
                }
            );
            end = run.rows.end;
            previous = Some(run.series);
        }
        ensure!(
            end == num_rows,
            UnexpectedSnafu {
                reason: format!("compact identity mapping covers {end} of {num_rows} rows"),
            }
        );
        Ok(Self { runs, num_rows })
    }

    pub(crate) fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.runs.capacity() * size_of::<SeriesRowRange>()
    }

    pub(crate) fn select(
        &self,
        series: &[MetricSeriesId],
        selection: Option<&RowSelection>,
    ) -> Vec<SeriesRowRange> {
        let intervals: Vec<Range<usize>> = match selection {
            None => std::iter::once(0..self.num_rows).collect(),
            Some(selection) => {
                let mut offset = 0;
                selection
                    .iter()
                    .filter_map(|s| {
                        let start = offset;
                        offset += s.row_count;
                        (!s.skip).then_some(start..offset)
                    })
                    .collect()
            }
        };
        let mut result = Vec::new();
        let mut cursor = 0;
        for run in &self.runs {
            if series.binary_search(&run.series).is_err() {
                continue;
            }
            while cursor < intervals.len() && intervals[cursor].end <= run.rows.start {
                cursor += 1;
            }
            for interval in &intervals[cursor..] {
                if interval.start >= run.rows.end {
                    break;
                }
                let rows = interval.start.max(run.rows.start)..interval.end.min(run.rows.end);
                if !rows.is_empty() {
                    result.push(SeriesRowRange {
                        series: run.series,
                        rows,
                    });
                }
            }
        }
        result
    }
}

/// Builds identity runs without retaining full encoded keys.
#[derive(Default)]
pub(crate) struct SeriesRowMappingBuilder {
    runs: Vec<SeriesRowRange>,
    rows: usize,
}

impl SeriesRowMappingBuilder {
    pub(crate) fn append(&mut self, array: &ArrayRef) -> Result<()> {
        let ids = identities(array)?;
        for series in ids {
            match self.runs.last_mut() {
                Some(run) if run.series == series => run.rows.end += 1,
                _ => self.runs.push(SeriesRowRange {
                    series,
                    rows: self.rows..self.rows + 1,
                }),
            }
            self.rows += 1;
        }
        Ok(())
    }

    pub(crate) fn finish(self, rows: usize) -> Result<SeriesRowMapping> {
        SeriesRowMapping::try_new(self.runs, rows)
    }
}

pub(crate) fn identities(array: &ArrayRef) -> Result<Vec<MetricSeriesId>> {
    let codec = SparsePrimaryKeyCodec::schemaless();
    let (values, keys) =
        if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
            (dict.values(), Some(dict.keys()))
        } else {
            (array, None)
        };
    let values = values
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context(UnexpectedSnafu {
            reason: "compact identity requires binary or dictionary-binary keys",
        })?;
    ensure!(
        array.null_count() == 0 && values.null_count() == 0,
        UnexpectedSnafu {
            reason: "null compact identity"
        }
    );
    let decoded = values
        .iter()
        .map(|key| {
            let (table_id, tsid) = codec
                .decode_ids(key.unwrap_or_default())
                .context(DecodeSnafu)?;
            Ok(MetricSeriesId { table_id, tsid })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(match keys {
        Some(keys) => keys.values().iter().map(|i| decoded[*i as usize]).collect(),
        None => decoded,
    })
}

/// Cursor consumes exactly the rows selected for the Parquet decoder.
pub(crate) struct CompactKeyCursor {
    runs: Vec<SeriesRowRange>,
    values: ArrayRef,
    position: usize,
    consumed: usize,
}

impl CompactKeyCursor {
    pub(crate) fn new(runs: Vec<SeriesRowRange>) -> Result<Self> {
        ensure!(
            runs.len() <= u32::MAX as usize,
            UnexpectedSnafu {
                reason: "too many compact identity runs"
            }
        );
        let codec = SparsePrimaryKeyCodec::schemaless();
        let mut builder = BinaryBuilder::new();
        let mut key = Vec::with_capacity(22);
        for run in &runs {
            key.clear();
            codec
                .encode_internal(run.series.table_id, run.series.tsid, &mut key)
                .context(EncodeSnafu)?;
            builder.append_value(&key);
        }
        Ok(Self {
            runs,
            values: Arc::new(builder.finish()),
            position: 0,
            consumed: 0,
        })
    }

    pub(crate) fn next(&mut self, rows: usize) -> Result<ArrayRef> {
        let mut keys = Vec::with_capacity(rows);
        while keys.len() < rows {
            let run = self.runs.get(self.position).context(UnexpectedSnafu {
                reason: "compact data exceeds preflight mapping",
            })?;
            let count = (run.rows.len() - self.consumed).min(rows - keys.len());
            keys.extend(std::iter::repeat_n(self.position as u32, count));
            self.consumed += count;
            if self.consumed == run.rows.len() {
                self.position += 1;
                self.consumed = 0;
            }
        }
        Ok(Arc::new(
            DictionaryArray::<UInt32Type>::try_new(UInt32Array::from(keys), self.values.clone())
                .context(ComputeArrowSnafu)?,
        ))
    }

    pub(crate) fn finish(&self) -> Result<()> {
        ensure!(
            self.position == self.runs.len() && self.consumed == 0,
            UnexpectedSnafu {
                reason: "compact data ended before preflight mapping"
            }
        );
        Ok(())
    }
}

/// Explicit merge schema; output tags never enter range merges or deduplication.
pub(crate) struct CompactSchema {
    pub(crate) schema: SchemaRef,
    output_schema: SchemaRef,
    metadata: RegionMetadataRef,
}

impl CompactSchema {
    pub(crate) fn new(mapper: &FlatProjectionMapper) -> Self {
        let output_schema = mapper.input_arrow_schema(false);
        let fields = output_schema
            .fields()
            .iter()
            .filter(|f| {
                mapper
                    .metadata()
                    .column_by_name(f.name())
                    .is_none_or(|c| c.semantic_type != SemanticType::Tag)
            })
            .cloned()
            .collect::<Vec<_>>();
        Self {
            schema: Arc::new(Schema::new(fields)),
            output_schema,
            metadata: mapper.metadata().clone(),
        }
    }

    /// Adapts fields by name, preserving target types and source-missing defaults.
    pub(crate) fn adapt(&self, batch: &RecordBatch, key: ArrayRef) -> Result<RecordBatch> {
        let pk = primary_key_column_index(self.schema.fields().len());
        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for (idx, field) in self.schema.fields().iter().enumerate() {
            let array = if idx == pk {
                key.clone()
            } else if let Some(array) = batch.column_by_name(field.name()) {
                array.clone()
            } else {
                let column =
                    self.metadata
                        .column_by_name(field.name())
                        .context(UnexpectedSnafu {
                            reason: "missing compact internal column",
                        })?;
                column
                    .column_schema
                    .create_default_vector(batch.num_rows())
                    .context(CreateDefaultSnafu {
                        region_id: self.metadata.region_id,
                        column: field.name(),
                    })?
                    .context(UnexpectedSnafu {
                        reason: "missing compact field has no default",
                    })?
                    .to_arrow_array()
            };
            columns.push(if array.data_type() == field.data_type() {
                array
            } else {
                datatypes::arrow::compute::cast(&array, field.data_type())
                    .context(ComputeArrowSnafu)?
            });
        }
        RecordBatch::try_new(self.schema.clone(), columns).context(NewRecordBatchSnafu)
    }

    pub(crate) fn assemble(&self, batch: RecordBatch, catalog: &TagCatalog) -> Result<RecordBatch> {
        let _timer = catalog.metrics.tag_time.timer();
        catalog.metrics.assembly_rows.add(batch.num_rows());
        catalog
            .metrics
            .assembly
            .measure(|| self.assemble_inner(batch, catalog))
    }

    fn assemble_inner(&self, batch: RecordBatch, catalog: &TagCatalog) -> Result<RecordBatch> {
        let ids = catalog
            .metrics
            .assembly_ids
            .measure(|| identities(batch.column(primary_key_column_index(batch.num_columns()))))?;
        let mut columns = Vec::with_capacity(self.output_schema.fields().len());
        for field in self.output_schema.fields() {
            let array = if let Some(column) = batch.column_by_name(field.name()) {
                column.clone()
            } else {
                let column =
                    self.metadata
                        .column_by_name(field.name())
                        .context(UnexpectedSnafu {
                            reason: "unknown output tag",
                        })?;
                catalog.metrics.assembly_tag_columns.add(1);
                catalog.column(column.column_id, &ids)?
            };
            columns.push(if array.data_type() == field.data_type() {
                array
            } else {
                datatypes::arrow::compute::cast(&array, field.data_type())
                    .context(ComputeArrowSnafu)?
            });
        }
        RecordBatch::try_new(self.output_schema.clone(), columns).context(NewRecordBatchSnafu)
    }
}

/// Decoded tags live once per identity, independently of full source keys.
pub(crate) struct TagCatalog {
    metadata: RegionMetadataRef,
    inner: Mutex<TagCatalogState>,
    pub(crate) metrics: CompactMetrics,
}

struct TagCatalogState {
    values: HashMap<MetricSeriesId, Vec<Value>>,
    missing: HashSet<MetricSeriesId>,
    reservation: MemoryReservation,
}

impl TagCatalog {
    pub(crate) fn new(
        metadata: RegionMetadataRef,
        pool: &Arc<dyn MemoryPool>,
        metrics: CompactMetrics,
    ) -> Self {
        Self {
            metadata,
            inner: Mutex::new(TagCatalogState {
                values: HashMap::new(),
                missing: HashSet::new(),
                reservation: MemoryConsumer::new("SeriesScan::tags").register(pool),
            }),
            metrics,
        }
    }

    /// Series-index discovery emits identity prefixes, which carry no tag payload.
    /// Keep these unresolved until a source key read establishes their tags.
    pub(crate) fn insert_candidate(&self, key: &[u8]) -> Result<()> {
        if key.len() != 22 || self.metadata.primary_key.len() == 2 {
            return self.insert(key);
        }
        let (table_id, tsid) = SparsePrimaryKeyCodec::schemaless()
            .decode_ids(key)
            .context(DecodeSnafu)?;
        let id = MetricSeriesId { table_id, tsid };
        let mut inner = self.inner.lock().unwrap();
        if !inner.values.contains_key(&id) && !inner.missing.contains(&id) {
            inner
                .reservation
                .try_grow(2 * size_of::<MetricSeriesId>())
                .context(MergeCandidateSeriesSnafu)?;
            inner.missing.insert(id);
        }
        Ok(())
    }

    pub(crate) fn needs_source_tags(&self, mapping: &SeriesRowMapping) -> bool {
        let inner = self.inner.lock().unwrap();
        mapping
            .runs
            .iter()
            .any(|run| inner.missing.contains(&run.series))
    }

    pub(crate) fn fill_source_tags(&self, array: &ArrayRef) -> Result<()> {
        let array = array
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
            .map_or(array, |d| d.values());
        let values = array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context(UnexpectedSnafu {
                reason: "source catalog keys must be binary",
            })?;
        let codec = SparsePrimaryKeyCodec::schemaless();
        for key in values.iter().flatten() {
            let (table_id, tsid) = codec.decode_ids(key).context(DecodeSnafu)?;
            let missing = self
                .inner
                .lock()
                .unwrap()
                .missing
                .contains(&MetricSeriesId { table_id, tsid });
            if missing {
                self.insert(key)?;
            }
        }
        Ok(())
    }

    pub(crate) fn insert(&self, key: &[u8]) -> Result<()> {
        let codec = SparsePrimaryKeyCodec::new(&self.metadata);
        let (table_id, tsid) = codec.decode_ids(key).context(DecodeSnafu)?;
        let id = MetricSeriesId { table_id, tsid };
        let mut inner = self.inner.lock().unwrap();
        if inner.values.contains_key(&id) {
            return Ok(());
        }
        let CompositeValues::Sparse(values) = codec.decode(key).context(DecodeSnafu)? else {
            return UnexpectedSnafu {
                reason: "compact catalog requires sparse keys",
            }
            .fail();
        };
        let tags = self
            .metadata
            .primary_key
            .iter()
            .map(|id| values.get_or_null(*id).clone())
            .collect::<Vec<_>>();
        let value_size = tags.capacity() * size_of::<Value>()
            + tags
                .iter()
                .map(|v| v.as_value_ref().data_size())
                .sum::<usize>();
        let old_capacity = inner.values.capacity();
        inner
            .reservation
            .try_grow(value_size + 2 * size_of::<(MetricSeriesId, Vec<Value>)>())
            .context(MergeCandidateSeriesSnafu)?;
        self.metrics.catalog_entries.add(1);
        inner.values.insert(id, tags);
        inner.missing.remove(&id);
        let additional = inner.values.capacity().saturating_sub(old_capacity)
            * size_of::<(MetricSeriesId, Vec<Value>)>();
        inner
            .reservation
            .try_grow(additional)
            .context(MergeCandidateSeriesSnafu)?;
        self.metrics.catalog_bytes.set(inner.reservation.size());
        Ok(())
    }

    pub(crate) fn column(&self, column_id: ColumnId, ids: &[MetricSeriesId]) -> Result<ArrayRef> {
        self.metrics
            .tag_column
            .measure(|| self.column_inner(column_id, ids))
    }

    fn column_inner(&self, column_id: ColumnId, ids: &[MetricSeriesId]) -> Result<ArrayRef> {
        let index = self
            .metadata
            .primary_key_index(column_id)
            .context(UnexpectedSnafu {
                reason: "catalog column is not a tag",
            })?;
        let column = self
            .metadata
            .column_by_id(column_id)
            .context(UnexpectedSnafu {
                reason: "catalog tag missing from metadata",
            })?;
        let mut builder = column
            .column_schema
            .data_type
            .create_mutable_vector(ids.len());
        let wait = self.metrics.tag_lock_wait.timer();
        let inner = self.inner.lock().unwrap();
        drop(wait);
        for id in ids {
            let values = inner.values.get(id).context(UnexpectedSnafu {
                reason: "data identity missing from tag catalog",
            })?;
            builder
                .try_push_value_ref(&values[index].as_value_ref())
                .context(ComputeVectorSnafu)?;
        }
        Ok(builder.to_vector().to_arrow_array())
    }
}

type MappedFileRange = (FileRange, Arc<SeriesRowMapping>);

/// All source handles are established before any partition receives an assignment.
pub(crate) struct CompactReadContext {
    pub(crate) schema: CompactSchema,
    pub(crate) catalog: Arc<TagCatalog>,
    files: HashMap<(usize, i64), Vec<MappedFileRange>>,
    _reservation: MemoryReservation,
    _assignment_reservation: MemoryReservation,
}

impl CompactReadContext {
    pub(crate) async fn preflight(
        ctx: &StreamContext,
        ranges: &[PartitionRange],
        pruner: &PartitionPruner,
        metrics: &PartitionMetrics,
        catalog: Arc<TagCatalog>,
        assignment_reservation: MemoryReservation,
    ) -> Result<Self> {
        let timer_metric = catalog.metrics.preflight_time.clone();
        let _timer = timer_metric.timer();
        let mut files = HashMap::new();
        let reservation =
            MemoryConsumer::new("SeriesScan::mappings").register(&ctx.input.scan_memory_pool);
        for range in ranges {
            for index in &ctx.ranges[range.identifier].row_group_indices {
                let key = (index.index, index.row_group_index);
                if !ctx.is_file_range_index(*index) || files.contains_key(&key) {
                    continue;
                }
                if pruner.try_skip_manifest_pruned_file_range(*index, metrics) {
                    files.insert(key, Vec::new());
                    continue;
                }
                let mut reader_metrics = ReaderMetrics::default();
                let prune_timer = catalog.metrics.preflight_prune_elapsed.timer();
                let sources = pruner
                    .build_file_ranges(*index, metrics, &mut reader_metrics)
                    .await?;
                drop(prune_timer);
                metrics.merge_reader_metrics(&reader_metrics, None);
                let mut prepared = Vec::with_capacity(sources.len());
                for source in sources {
                    let mapping = source.preflight_compact_mapping(&catalog).await?;
                    reservation
                        .try_grow(mapping.estimated_size())
                        .context(MergeCandidateSeriesSnafu)?;
                    prepared.push((source, mapping));
                }
                reservation
                    .try_grow(prepared.capacity() * size_of::<(FileRange, Arc<SeriesRowMapping>)>())
                    .context(MergeCandidateSeriesSnafu)?;
                files.insert(key, prepared);
            }
        }
        reservation
            .try_grow(
                files.capacity()
                    * size_of::<((usize, i64), Vec<(FileRange, Arc<SeriesRowMapping>)>)>(),
            )
            .context(MergeCandidateSeriesSnafu)?;
        catalog.metrics.mapping_bytes.set(reservation.size());
        Ok(Self {
            schema: CompactSchema::new(&ctx.input.mapper),
            catalog,
            files,
            _reservation: reservation,
            _assignment_reservation: assignment_reservation,
        })
    }

    pub(crate) fn files(
        &self,
        index: crate::read::range::RowGroupIndex,
    ) -> Result<&[(FileRange, Arc<SeriesRowMapping>)]> {
        self.files
            .get(&(index.index, index.row_group_index))
            .map(Vec::as_slice)
            .context(UnexpectedSnafu {
                reason: "compact source was not included in preflight",
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::flat_dedup::{FlatDedupReader, FlatLastNonNull};
    use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};
    use api::v1::OpType;
    use datafusion::execution::memory_pool::UnboundedMemoryPool;
    use datatypes::arrow::array::{TimestampMillisecondArray, UInt8Array, UInt64Array};
    use futures::TryStreamExt;
    use parquet::arrow::arrow_reader::RowSelector;
    use store_api::codec::PrimaryKeyEncoding;

    #[test]
    fn compact_operation_metrics_record_cpu_and_errors() {
        let metrics = OperationMetrics::new(&ExecutionPlanMetricsSet::default(), 0, "operation");
        let result: std::result::Result<(), ()> = metrics.measure(|| {
            for i in 0..10000 {
                std::hint::black_box(i);
            }
            Err(())
        });
        assert!(result.is_err());
        assert_eq!(1, metrics.calls.value());
        assert!(metrics.elapsed.value() > 0);
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            assert!(metrics.cpu.value() > 0);
            assert_eq!(0, metrics.unavailable.value());
        }
    }

    #[test]
    fn compact_mapping_coverage_selection_and_cursor() {
        let id = |table_id| MetricSeriesId { table_id, tsid: 7 };
        let run = |table_id, rows| SeriesRowRange {
            series: id(table_id),
            rows,
        };
        for runs in [
            vec![run(1, 0..2)],
            vec![run(1, 1..4)],
            vec![run(1, 0..3), run(2, 2..4)],
        ] {
            assert!(SeriesRowMapping::try_new(runs, 4).is_err());
        }
        let mapping = SeriesRowMapping::try_new(vec![run(1, 0..3), run(2, 3..6)], 6).unwrap();
        let selection = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(3),
            RowSelector::skip(1),
            RowSelector::select(1),
        ]);
        let runs = mapping.select(&[id(1), id(2)], Some(&selection));
        assert_eq!(
            vec![1..3, 3..4, 5..6],
            runs.iter().map(|r| r.rows.clone()).collect::<Vec<_>>()
        );
        let mut cursor = CompactKeyCursor::new(runs).unwrap();
        assert_eq!(vec![id(1)], identities(&cursor.next(1).unwrap()).unwrap());
        assert_eq!(
            vec![id(1), id(2), id(2)],
            identities(&cursor.next(3).unwrap()).unwrap()
        );
        cursor.finish().unwrap();
        assert!(cursor.next(1).is_err());
        let mut incomplete = CompactKeyCursor::new(mapping.runs.clone()).unwrap();
        incomplete.next(5).unwrap();
        assert!(incomplete.finish().is_err());
    }

    #[tokio::test]
    async fn compact_schema_last_non_null_deletes_and_deferred_tags() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let schema = CompactSchema::new(&FlatProjectionMapper::all(&metadata).unwrap());
        assert_eq!(5, schema.schema.fields().len());
        assert_eq!("field_0", schema.schema.field(0).name());
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let set = ExecutionPlanMetricsSet::default();
        let catalog = TagCatalog::new(metadata.clone(), &pool, CompactMetrics::new(&set, 0));
        for (table, tag) in [(1, "a"), (2, "b")] {
            catalog
                .insert(&new_sparse_primary_key(&[tag, "x"], &metadata, table, 7))
                .unwrap();
        }
        let runs = vec![
            SeriesRowRange {
                series: MetricSeriesId {
                    table_id: 1,
                    tsid: 7,
                },
                rows: 0..3,
            },
            SeriesRowRange {
                series: MetricSeriesId {
                    table_id: 2,
                    tsid: 7,
                },
                rows: 3..4,
            },
        ];
        let mut cursor = CompactKeyCursor::new(runs).unwrap();
        let batch = RecordBatch::try_new(
            schema.schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![None, Some(7), Some(8), Some(9)])),
                Arc::new(TimestampMillisecondArray::from(vec![1, 1, 2, 1])),
                cursor.next(4).unwrap(),
                Arc::new(UInt64Array::from(vec![3, 2, 4, 5])),
                Arc::new(UInt8Array::from(vec![
                    OpType::Put as u8,
                    OpType::Put as u8,
                    OpType::Delete as u8,
                    OpType::Put as u8,
                ])),
            ],
        )
        .unwrap();
        // Old SSTs lack newly added nullable fields; adaptation supplies defaults.
        let old = batch.project(&[1, 2, 3, 4]).unwrap();
        let adapted = schema.adapt(&old, batch.column(2).clone()).unwrap();
        assert_eq!(4, adapted.column(0).null_count());
        assert_eq!(batch.column(1), adapted.column(1));
        let stream = Box::pin(futures::stream::iter(vec![
            Ok(batch.slice(0, 1)),
            Ok(batch.slice(1, 3)),
        ]));
        let output = FlatDedupReader::new(stream, FlatLastNonNull::new(0, true), None)
            .into_stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(2, output.iter().map(RecordBatch::num_rows).sum::<usize>());
        let mut values = Vec::new();
        for batch in output {
            assert_eq!(5, batch.num_columns());
            let batch = schema.assemble(batch, &catalog).unwrap();
            let fields = batch
                .column_by_name("field_0")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            values.extend(fields.values().iter().copied());
            assert!(batch.column_by_name("tag_0").is_some());
        }
        assert_eq!(vec![7, 9], values);
        assert!(catalog.metrics.catalog_bytes.value() > 0);
        assert_eq!(2, catalog.metrics.catalog_entries.value());
        assert_eq!(2, catalog.metrics.assembly_rows.value());
        assert!(catalog.metrics.assembly.calls.value() > 0);
        assert_eq!(
            catalog.metrics.assembly.calls.value(),
            catalog.metrics.assembly_ids.calls.value()
        );
        assert_eq!(
            catalog.metrics.assembly_tag_columns.value(),
            catalog.metrics.tag_column.calls.value()
        );
        assert!(catalog.metrics.assembly.elapsed.value() > 0);
    }
}
