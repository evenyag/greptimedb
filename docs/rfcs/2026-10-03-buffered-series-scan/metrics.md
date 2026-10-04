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
