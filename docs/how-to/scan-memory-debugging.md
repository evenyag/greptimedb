# Debugging scan memory on the key-reuse branch

Mito reads default to 1024 rows. Set `GREPTIME_MITO_READ_BATCH_SIZE` before
starting GreptimeDB to select a size from 1 through 8192. An invalid value
warns and falls back to 1024; 8192 restores the previous batch-size setting.
The override also controls runtime merge/channel batch estimates. SST row-group
sizes are unchanged. Restart the process between experiments.

Memory diagnostics are enabled by default. Set
`GREPTIME_MITO_SCAN_MEMORY_DIAGNOSTICS=false` before startup for a control run.

## Metrics

- `greptime_mito_scan_debug_memory_bytes{phase,component,partition}`: current
  estimated buffer footprint held by diagnostic owners.
- `greptime_mito_scan_debug_objects{phase,kind,partition}`: live owners, including
  readers and merges still initializing, retained batches, and queued batches.
- `greptime_mito_scan_debug_retained_rows{phase,partition}`: rows held by owners.
- `greptime_mito_scan_debug_produced_bytes_total{phase,component,partition}`:
  cumulative observed output/fetched bytes, including shared dictionary values;
  this is not an allocation counter.

`partition` is the two-phase series scanner partition number, or `shared` for
shared discovery work and scans without a diagnostic partition context. Metrics aggregate concurrent scans in the
process with the same labels. Logs additionally identify each scan and region.

Components include primary-key values/indices, tag values/indices, primitive
tags, data columns, series filters, candidate assignments, row-run mappings,
merge row indices, cache buffers, and Parquet input bytes. Arrow allocations are
deduplicated within a measured batch, including sliced buffers. Ownership
estimates can overlap across batches and stages: do not sum them as a heap total.
Cached mappings can remain live after a scan ends.

Parquet buffered bytes measure staged input only. They exclude active dictionary
decoders, Zstd state, and allocator overhead. Compare reader counts and the
tracked footprints with heap profiles, process RSS, and the existing jemalloc,
cache, and cgroup metrics to investigate the gap.

## Logs and comparison

`Scan memory progress` INFO records cover candidate construction/discovery,
data construction, final-merge initialization, first output, and context drop.
`Scan memory snapshot` records arrive every second while a series scanner is
alive, even before output. They contain partition stages, ready-range counts,
process component footprints and peaks, live-object/row counts, scan-pool
reservations, and Linux RSS when available. Peaks cover the process lifetime.
Snapshots use weak scan references and do not retain data buffers.

For Q03, compare fresh processes with batch sizes 8192 and 1024, keeping the
query, concurrency, retained dataset, cache policy, and memory guards identical.
Scrape metrics every second and retain INFO logs and threshold heap profiles.
Compare retained primary-key bytes, tag indices, reader/batch counts, and peak
process/cgroup memory. The retained benchmark dataset is needed for this run;
it is not included in the diagnostic artifact package.
