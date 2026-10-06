# Compact scan operation metrics

These query-owned metrics appear on SeriesScan in TQL ANALYZE VERBOSE. They
instrument the experimental compact path; they do not change scan semantics.

## Time interpretation

Synchronous operations publish `<name>_calls`, `<name>_elapsed`, `<name>_cpu`,
and `<name>_cpu_unavailable`. Time values in JSON are nanoseconds. Elapsed time
includes scheduling and mutex waits. CPU time uses the calling thread's CPU
clock on Linux/macOS, sampled immediately around synchronous work on that same
thread, never across an await. Unsupported/failed clocks increment
`cpu_unavailable`; zero CPU with unavailable samples must not be read as free
work. Clock sampling and metric updates add diagnostic overhead.

Times are cumulative across calls and partitions, not exclusive query latency.
Parent/child measurements overlap: assembly includes identity extraction, tag
column construction and lock waits; fetch includes cache work, storage awaits
and byte-range assembly. Do not add parent and child times. CPU samples from
native flamegraphs provide a separate attribution check, not elapsed timing.

## Parquet reads

Each of `compact_discovery`, `compact_preflight`, and `compact_data` publishes:

- `decode_{calls,elapsed,cpu,cpu_unavailable}`: synchronous push-decoder calls,
  including decompression, decoding, and pushing fetched ranges. Calls include
  requests for more data and end-of-stream, not just output batches.
- `decoded_batches`, `decoded_rows`: batches/rows produced by that decoder,
  before later source filters, merging, and deduplication.
- `fetch_calls`, `fetch_elapsed`: byte-range fetch invocations and inclusive
  awaited elapsed time. No downstream consumer suspension is included.
- `requested_bytes`, `page_cache_bytes`: requested ranges and fragments served
  by the engine page cache, counting rereads.
- `store_calls`, `store_elapsed`, `store_payload_bytes`: fetch-helper calls,
  their awaited elapsed time, and returned missing-range payload. A helper can
  issue multiple/coalesced backend reads. Payload excludes coalescing gaps;
  it is not physical disk/network traffic. OS caches can serve store reads.
- `write_cache_elapsed`, `write_cache_bytes`: local write-cache probe/read time
  (including misses) and returned payload.
- `cache_lookup_*`, `cache_insert_*`, `range_assembly_*`: synchronous elapsed,
  CPU, calls and unavailable clocks for these operations. Range assembly joins
  cached/fetched fragments, not query output tags.

The existing discovery/preflight primary-key row/byte counters are preserved.
Stage-specific fetch metrics exclude metadata/index I/O outside the decoder.
Storage elapsed is request latency (including scheduling and backend work), not
pure blocked I/O time. It must not be subtracted from CPU as if the two were an
exclusive partition of query latency.

## Mapping and tags

- `mapping_preflight_cost`: full preflight elapsed, before data admission.
- `compact_preflight_prune_elapsed`: obtaining/pruning source ranges, including
  associated metadata work and waits.
- `compact_mapping_cache_hits`, `compact_mapping_index_hits`,
  `compact_mapping_key_reads`: how coverage was obtained for source row groups.
- `compact_mapping_index_elapsed`: range-index lookup/validation elapsed,
  including unsuccessful attempts before key-read fallback.
- `compact_mapping_build_*`: identity extraction, run construction, and final
  coverage validation for newly read mappings. `compact_mapping_runs` counts
  runs in those newly constructed mappings, excluding cached/index mappings.
- `compact_preflight_tag_fill_*`: source-key compatibility and tag-catalog fill
  checks while processing preflight batches.
- `compact_discovery_catalog_*`: candidate tag insertion/checks per input batch.
- `compact_catalog_entries`: distinct identities with decoded catalog values.

## Compact data and output

- `compact_mapping_select_*`: selecting identity runs for source readers.
- `compact_key_synthesis_*`: constructing compact keys for decoded data batches.
- `compact_data_filter_*`: sequence and residual source filtering.
- `compact_schema_adapt_*`: compact SST batch schema/default/type adaptation.
- `compact_assembly_*`: output assembly calls, inclusive elapsed and CPU.
- `compact_assembly_rows`: rows passed to output assembly.
- `compact_assembly_tag_columns`: tag arrays requested by output assembly.
- `compact_assembly_ids_*`: identity extraction during assembly.
- `compact_tag_column_*`: catalog lookup and tag-array construction, including
  calls from residual partition filtering as well as output assembly.
- `compact_tag_lock_wait`: elapsed acquiring the catalog mutex for tag columns;
  included in tag-column/assembly elapsed, not a separate additive phase.

Mapping selection, data filtering and schema-adaptation counters describe the
SST compact path, not every memtable operation. Merge/deduplication retains its
existing metrics; these counters are not a complete exclusive CPU accounting
of every task in the query. Use process CPU deltas and native CPU profiles to
identify uninstrumented work. The original primary-key exclusion audit remains
active and independent of these diagnostics.

## Buffered preparation and replay (Stage 4)

Development buffered mode adds `buffered_settings`, `buffered_resources`, and
`buffered_operations` to the verbose scanner explanation. Scanbench records
effective mode, query budget, spill threshold, layout, IPC/source batch bounds,
compression, preparation concurrency, prefetch, and consumer retention profile.
The Stage 4 profile funds one complete identity plus the next batch for existing
downstream consumers. These explicit experiment settings are not defaults.

`buffered_resources.counts` contains cumulative nanosecond elapsed timers:

- `preparation_ns`: complete successful preparation through funded manifests,
  including discovery, preflight, range preparation, and publication admission.
- `range_preparation_ns`: admitted complete-range source/merge/append work through
  reader destruction. Reader admission/reclamation before startup is outside it.
- `result_append_ns`: result append, including resident/file preparation work.
  `spill_append_ns` is the subset of append calls choosing file placement.
- `spill_finalization_ns`: builder finish and completed-result spill calls.
  Serialization can occur during append; this timer alone is not total spill
  cost. Resident builder finish is also included.
- `readiness_wait_ns`: sum of consumer waits for manifests. At eight concurrent
  consumers it can approach eight times preparation latency.
- `replay_ns`: cumulative cursor `next` awaits, excluding downstream suspension
  and tag assembly. `tag_assembly_ns` times compact output/tag assembly separately.

These timers overlap and are not CPU seconds or an exclusive latency breakdown.
`buffered_operations` reports synchronous calls/elapsed/CPU/unavailable clocks
for normalization, serialization, metadata initialization, IPC decoding,
reconstruction, index lookup, and owned-file cleanup. Interpret those CPU scopes
using the same-thread rules above. Zero calls can mean a path was not exercised;
it does not establish that the broader operation is free.

Resource counts also record reader starts/destructions, completed ranges with
released sources, published manifests, stored rows/batches/files, decoded
rows/batches, requested rows, cursor cache hits/misses, file opens, seeks,
read/write calls, footer bytes, and batch-directory bytes. With mixed placement,
requested rows include resident output while IPC decoded rows do not: their
ratio alone is not file direct-read amplification. Report unrelated lookup rows
and backing bytes, placement, metadata, and repeated reads together.

IPC `logical_requested_bytes` counts requested batch ranges.
`filesystem_read_bytes` includes data and metadata reads; write bytes count
userspace writes. These are separate from Stage 2 source fetch/cache counters
and from physical storage traffic. OS cache and writeback can make block-device
traffic differ. Process/cgroup I/O sampling includes reference/harness work in
exactness runs and may miss late writeback; record that scope and sampling limit.

Resource snapshots report current payload, metadata, workspace, disk, pending
cleanup, active operations, and failed cleanup. Peaks are per-category maxima,
not necessarily simultaneous. `memory_bytes` is shared query admission,
including estimates and prepaid reservations; it is not RSS. Publication
capacity transfers to live allocation charges without double charging. Important
peaks include:

- `live_readers`, `range_reader_estimate_bytes`, and `merge_batch_bytes`: source
  concurrency, its estimated simultaneous capacity, and observed input size.
- `publication_reserved_bytes`: aggregate capacity promised to all manifests;
  `publication_partition_active_replay_bytes` and
  `publication_partition_output_bytes`: maximum per-partition components.
- `replay_payload_bytes`: current decoded backing bound, distinct from peak
  `payload_bytes`, which can include escaped downstream output.
- `lookup_backing_bytes`, `lookup_unrelated_rows`, and
  `lookup_unrelated_logical_bytes`: backing retained by indexed lookup slices.

Measure reader estimates against retained-reader experiments, allocator/RSS
observations, and shared engine/cache state. Whole-process and accounting peaks
do not isolate decoder estimation error. The spill threshold initiates
reclamation; final publication reservations may exceed that threshold while
remaining within the hard query budget. Completion can retain the bounded
resources/metrics object even when all dynamic payload, workspace, and disk are
released. Pair snapshots with actual scratch/process cleanup checks.

## Independent preparation and shared sources (Stage 5)

Validated at `24d38239ba`; see the external
[Stage 5 report](/Users/evenyag/Documents/test/promql-k8s-memory/reports/buffered-series-scan-poc-stage5-report.md)
for equal-budget comparisons and preserved failed attempts. Development options
add `source_policy` and `candidate_chunk_size`. `preparation_concurrency` is a
per-stage ceiling: shared mode independently schedules up to N source reads
and N complete-range merges. All tasks and output partitions share one budget.
Selected-per-partition mode schedules complete range/assignment tasks instead.

Additional cumulative elapsed counters, in nanoseconds:

- `source_preparation_ns`: source stream materialization through builder finish;
  includes source reading, normalization, append, and finalization. Admission
  before starting the task is outside this scope.
- `source_append_ns` and `source_spill_append_ns`: source-buffer append and the
  subset choosing file placement. `source_finalization_ns` measures finish;
  serialization mainly happens during append, and reclamation can also spill
  completed buffers.
- `range_merge_ns`: shared complete-range merge task, including input replay,
  deduplication/selectors, result materialization, and source release.
- `range_materialization_ns`: result stream materialization through finish.
  Selected mode retains `range_preparation_ns` for combined source/merge work.

These counters overlap, including between the independently scheduled stages;
their sum is neither preparation wall time nor CPU. Synchronous operation CPU
remains in `buffered_operations`, combining source/result stores within each
named scope. It does not provide exclusive source-preparation or merge CPU.

`source_parts_planned`, `source_parts_completed`, `candidate_chunks`,
`source_parts_in_later_chunks`, and `source_rereads_across_chunks` describe
candidate-pruned source scheduling. `source_selected_rows` and
`range_result_rows` count different pipeline stages: summing them does not
measure duplicate query output. Q03 fits one chunk; local fixtures exercise
chunk boundaries. Source-reader starts are not decoded-row counts. Compare
Stage 2 data decoder calls/CPU, selected versus decoded rows, logical requested
bytes, and cache/store payload separately when assessing repeated source work.

IPC counters have `source_` and `result_` variants for spill bytes, filesystem
read/write bytes and calls, logical requested bytes, and decoded rows. Their
unprefixed counterparts aggregate store work. Resident/file placement, unrelated
lookup rows, and repeated decoding must accompany direct-read amplification
comparisons; decoded IPC rows divided by all requested rows is insufficient.

Peaks distinguish `active_source_tasks`, `active_merge_tasks`,
`active_range_tasks`, `merge_inputs`, source/result metadata and payload,
source-reader workspace, and source-reassembly workspace. The latter detaches
each decoded IPC fragment before advancing, then reconstructs original source
batch boundaries. Admission includes retained fragments, per-fragment overhead,
and transient decoder/conversion overlap. Required-byte peaks for source
preparation, range preparation, and range merge are estimates/prepaid requests,
not RSS or proven bounds on opaque decoder allocations. Reader/input counts
remain observations, never admission caps. Deferral counters distinguish
temporarily unavailable capacity from irreducible rejection.

The current publication rejection reports the failed reclamation increment
(`available + 1`), not the full aggregate replay/output reservation. Do not
interpret it as the extra capacity needed for success. Successful publication
peaks record the actual reserved capacity, with placement-specific requirements.

Query-end snapshots may precede asynchronous deletion. Pending deletion retains
disk/metadata charges until removal completes; cleanup elapsed/CPU snapshots can
therefore omit later cleanup work. Quiescent local cleanup tests and verified
remote process/cgroup/scratch cleanup are separate gates. Escaped output charges
likewise survive cursor/manifest destruction. Preserve the Stage 4 distinction
between bounded active replay and complete-identity downstream retention.

## File-backed buffered-data cache (Stage 6)

Validated at `77df493f95` with scanbench shutdown correction `9e71852ef4`;
see the external
[Stage 6 report](/Users/evenyag/Documents/test/promql-k8s-memory/reports/buffered-series-scan-poc-stage6-report.md).
Development settings record the optional cache's disk and metadata capacities
separately from the consuming query's memory/spill/disk budgets. Equal query
budgets do not imply equal total provisioning. Existing engine caches and OS
file-cache memory remain separate.

Query-owned `buffered_resources.counts` adds `buffered_cache_hits`,
`buffered_cache_misses`, `buffered_cache_bypasses`, `buffered_cache_admissions`,
`buffered_cache_admission_skips`, and `buffered_cache_evictions`. These describe
complete range/identity-set results, not rows, IPC batches, or physical files.
For retained p8 Q03, shared mode observes six entries/hits and selected mode
48; these are measurements, never caps. An optional admission skip is not a
query failure. Required spill failure still fails the query.

`buffered_cache_admission_ns` is cumulative successful admission elapsed after
acquiring the serial admission lock, including copying/conversion and any
eviction/retry work. It is not exclusive CPU or a final-replay phase timer.
Cache resource `operations.cache_copy_*` instruments synchronous finalized-file
copy calls with the existing same-thread elapsed/CPU/unavailable-clock rules;
resident conversion also uses normalization/serialization scopes. Copying
finalized IPC records `cache_copy_read_bytes` separately from metadata reads.

`buffered_settings.cache_resources` is an engine-owned cumulative snapshot:

- `visible_entries` and `visible_disk_bytes`: lookup-visible complete results.
- `visible_pinned_disk_bytes`: visible results with external result/file owners.
  Evicted pins are not included in this visible-only field.
- `retired_or_staging_disk_bytes`: charged disk not represented by visible
  entries, including staging, evicted pins, and pending deletion. It is not an
  exclusive measure of pinned bytes.
- `resources` and `operations`: cache-owned capacities, peaks, pending/failed
  cleanup, and cumulative content I/O/operation counters. Cache metadata covers
  keys, indexes, and independent decoded tags. Consumer decoding/output is
  charged to the consuming query, not the cache's metadata allowance.

Compare cumulative cache write counters before/after a warm query, alongside
that query's own content-write counters. Do not attribute the cache's lifetime
totals to every query or interpret filesystem/cleanup metadata traffic as IPC
content writes. Physical I/O and RSS require separate observations. Pins keep
disk/index charges until ownership and deletion permit release; lookup eviction
alone does not reclaim them. Query-end snapshots can precede asynchronous
cleanup, and zero visible pins at that snapshot says nothing about earlier pins.

Scanbench now stops its engine on success and error, joining cancelled partition
tasks first. Engine shutdown invalidates lookup ownership and drains cleanup;
escaped owners still retain their charges. Retained tests pair snapshots with
actual process/timer/cgroup/payload cleanup. Three timing-only repetitions use
disabled-first/repeat controls and cold/warm cache pairs; exact-reference I/O is
kept in separate diagnostics. Timing-only still retains buffered accounting and
operation instrumentation, and its process-pair peaks do not isolate warm RSS.
