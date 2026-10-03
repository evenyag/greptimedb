# Buffered SeriesScan implementation and experiment plan

Status: Draft revised after first review, 2026-10-03.

Read the [design](design.md) for execution semantics and interfaces. Preparation
runs outside scan partition streams; those streams perform final merge and tag
assembly after notification. The core merge key is (table_id, tsid). Range
results can spill to Arrow IPC files before replay; final replay never spills.

# Working conventions

- Preserve current v2 and existing uncommitted research edits. Record source and
  binary identity for measured implementations.
- Keep development options for preparation concurrency, source policy,
  per-query memory budget/spill threshold, IPC layout, batch size, and compression.
  Reader and merge-input counts are metrics, not admission limits.
  Production option names/defaults follow measured selection.
- Use existing scanbench/parquetbench and bounded JSONL evidence. Store run
  artifacts in a fresh external directory, not the completed research root.
- Read applicable repository instructions before editing. Apply license headers
  to new source files; local debug binary builds use CARGO_PROFILE_DEV_DEBUG=1.
- Use focused tests/microbenchmarks before retained-dataset runs. Do not add
  external merge passes, full-key replay fallback, or engine-wide spill
  scheduling to the initial implementation.

# Stage 1: Baseline and preparation/readiness harness

Extend the tools with explicit settings and report effective mode, source
policy, query memory budget/spill threshold, actual output partitions,
batch size, and IPC layout.

Measure candidate discovery, source reads, range merge, spill/finalization,
readiness wait, final replay/merge, tag assembly, and cleanup. Count live
readers, decoded primary-key pages in the data phase, retained capacities,
catalog/index bytes, workspace, disk usage, rows, storage traffic, and
first-output/complete latency.

Build an exact streaming comparison for logical values, ordering within
partitions, and series assignment. Separate timing runs from diagnostic runs.

Prototype a preparation coordinator and owned readiness manifests. It starts
once, schedules work outside partition streams, explicitly marks empty results
complete, notifies consumers, and propagates errors/cancellation. Test
sequential polling and consumers that are never polled.

Gate: reproduce one/eight partitions and Q03's 51,635,200 rows. Keep the older
54,579,200-row discrepancy unresolved rather than adopting it as the baseline.
Readiness must not depend on every output partition being polled.

# Stage 2: Compact identity reads and deferred tag assembly

Extend row-group data readers to synthesize 22-byte compact __primary_key values
from (table_id, tsid) row mappings/range indexes. Exclude SST __primary_key from
the data projection. Intersect identity runs with selections and keep identity
arrays aligned with filtered field arrays.

Candidate discovery supplies mappings and decoded tag context. Without a range
index, require mappings populated by discovery before data reads; fail clearly
on missing coverage rather than reread full-key pages in the data phase.

Create an accounted tag catalog keyed by identity, normalized to the required
output schema. Data batches carry fields, timestamp, compact key, sequence, and
operation type without expanded projected tags. After merge, join tags by
identity and apply output projection.

Preserve snapshot/sequence rules, trusted overrides, field/time predicates,
source compatibility, deduplication, and selectors. Memtable sources use the
same compact identity model.

Gate: output values/tags/order match reference metric scans. Equal TSIDs in
different table IDs stay distinct. Instrumented data reads decode zero SST
__primary_key pages, including cold-mapping and partial-index cases. Candidate
key reads remain reported separately.

# Stage 3: IPC-file result store and batch-layout experiments

Implement result builders/handles/cursors with resident batches and actual
Arrow IPC files. Record batch indices and per-series row spans, including a
series crossing batches/files. Finish files before publishing random-access
handles and use checked offset arithmetic.

Add query-local capacity accounting, bounded serialization staging, reader
prefetch, disk quota, blocking-worker dispatch, and cleanup. Prepared results
can remain resident, spill fully, or reference completed resident/file parts.

Implement two layouts from the start: one series per batch, and multiple
series per batch. A large series has multiple bounded batches. Index exact
positions for direct series access.

Use plain Binary compact keys in the first IPC benchmark and normalize
dictionary-encoded fields to corresponding value arrays in its storage schema.
Compare a consistent fixed compact-key dictionary separately; do not depend on
IPC dictionary replacement. Reconstruct merge arrays on replay.

Measure:

- Write and sequential/random read throughput, CPU, and serialized size.
- Direct series lookup and memory retained for unrelated series.
- Per-series rows/bytes and distributions, not just average batch size.
- Batch/footer/index overhead, file count, seeks, and file-open costs.
- Dictionary conversion, serialization, and replay workspace.
- Cancellation and cleanup after completed and partial writes.

Start uncompressed; compare LZ4/Zstd after exact round trips pass.

Gate: both layouts round-trip compact data correctly, positions select the
right series, partial files are never published, and all owned storage is
released after cancellation/errors.

# Stage 4: Range preparation, spill threshold, and final merge

Integrate the current selected-series source pattern with the coordinator.
Initially prepare one partition range at a time across the query. Drain its
complete merged result into the store and release all associated SST readers.

Admit readers and merges through per-query memory reservations, including
estimated decoder/fetch state and measured buffers/workspace. Keep reservations
until state destruction; calibrate estimates with the reader experiments.
Do not introduce a reader cap or fan-in limit.

For streaming merges, evaluate the full simultaneous working set and avoid
partial startup that waits for memory retained by the same merge. Defer tasks
when runnable work can release memory and spill eligible unpublished results
before retrying. Fail irreducible memory requests with stage/required/available
bytes; do not retry with external merge passes.

Apply complete range-level deduplication and preserve selector placement.
Characterize equal-sequence ties and verify the non-overlapping complete-range
invariant used by final merge.

Use one per-query tracker for catalogs, results, indexes, buffers, queues, and
workspace. During preparation, crossing the threshold spills eligible range
results. Keep writer workspace available and account for transient overlap.

Finalize result placement and expected replay workspace before readiness
publication. Partition streams then replay/merge by compact identity, attach
tags, and yield. They never write spill files or cause published results to
migrate to disk. Further preparation can spill unpublished results only.

Gate: reader destruction is observed at range completion; readiness and
sequential partition consumption work; spill follows aggregate query accounting;
final replay performs zero spill writes. Large reader/input counts alone never
reject a query. Missing mappings, disk exhaustion, and irreducible memory
requests fail and clean up.

# Stage 5: Independent preparation concurrency and shared SST reads

Vary preparation concurrency outside partitions: 1, 2, and 4, with memory-based
admission under the query budget. These are scheduling experiments, not reader
or merge-input count limits. Preserve the consumer interface; changing
the preparation schedule must not move tasks into output streams.

Add the shared selected-series policy. Read candidate-pruned groups for the
union across output assignments, synthesize compact keys from identity mappings,
buffer source results, and merge ranges in preparation. Independently schedule
bounded row-group reads and range-result merging.

If source buffers spill, finalize their storage before merge consumers read
them. This does not introduce spill during final partition replay. Begin with
candidate unions that fit; retain existing chunked assignments for larger
sets and report rereads across chunks. Pageable candidate storage remains a
follow-up rather than a prerequisite.

Compare source policies at equal query budgets. Measure repeated decoding,
selected versus decoded rows, source/result spill bytes, required merge memory,
memory admission failures, complete latency, and first-output time.

Gate: no duplicated/missing rows across assignments or readiness manifests.
Memory does not acquire a separate budget per output partition; reader and
merge-input counts remain measurements only.
Performance evidence determines whether shared reads belong in the candidate.

# Stage 6: File-backed buffered-data cache

Add representation-aware keys and preserve existing fingerprint/eligibility
rules. Cache complete IPC results and indexes plus independently owned tag
context required for output assembly.

Perform cache admission and any conversion to files in preparation, before
publishing final-replay handles. Do not write cache content in partition merge
streams. Admission failure skips caching; required spill failures fail queries.

Implement disk/metadata capacities, pinning, delayed deletion, quota charging,
and startup cleanup. Preserve current v2, candidate, mapping, and SeqScan caches.

Compare disabled, cold, and warm cache runs. Test schema/sequence/filter
changes, dynamic-filter bypass, memtable exclusion, concurrent cursors, eviction
with active pins, and producing-query destruction.

Gate: cached compact rows retain enough tag context to reproduce output,
replay never depends on an expired query catalog, and final replay writes
neither spill nor cache files.

# Stage 7: Integrated evaluation and rollout decision

Combine independently validated components, rerun correctness/resource tests,
and preserve tool options for attributing regressions.

Select IPC batch layout/encoding, source policy, preparation concurrency, and
experimental limits from measured evidence. Add opt-in configuration with
examples, loading/serialization tests, generated configuration documentation,
and matching user documentation. Keep existing defaults until a separate
promotion decision.

Produce a result summary with raw evidence, source/binary bindings, effective
settings, phase memory, complete/first-output latency, spill amplification,
memory admission coverage, and cleanup. A low-memory run rejected before
execution is not a successful benchmark.

Gate: correct output, enforced accepted-workload resource contracts, clean
failure paths, no data-phase full-key decoding, no final-replay spill, and
reproducible comparison evidence.

# Experiment matrix and execution order

Sweep focused variables before combinations.

| Variable | Initial values |
| --- | --- |
| Source count (workload variable) | Synthetic 8/16/32/64 inputs plus retained-dataset ranges; no count limit |
| Source width/decoder footprint | Small and large readers at equal source counts |
| Hard tracked-memory budget | Explicit sweep based on measured working sets; record required/available bytes |
| Per-query spill threshold | 128, 256, 512, 1024 MiB |
| Workspace | Record minimum working size and transient peak separately |
| Preparation concurrency | 1, 2, 4, independent of output partition count |
| Output partitions | 1 and 8 |
| Batch rows | 1024 and 8192, plus smaller per-series samples |
| Source policy | Current selected-series streams; shared selected-series union |
| IPC batch layout | One series; multiple series |
| IPC batch target | Row/byte targets informed by measured per-series size; split large series |
| Result retention | Resident when it fits; forced spill; threshold-based spill before replay |
| Storage key encoding | Plain Binary compact prefix; consistent fixed dictionary |
| Compression | Uncompressed, then LZ4 and Zstd |
| Data-result cache | Disabled, then cold and warm |

For one-series layouts, measure small-batch overhead and lookup latency; for
multiple-series layouts, measure unrelated rows retained by final cursors.
Include a series present in every range, very large series, many short series,
skew, multiple table IDs, and low/high selection fractions.

Progress from synthetic fixtures to representative retained row groups,
complete Q03, and the remaining query suite. Use exact requests; Q11's display
regex must not be silently replaced by an unverified SQL translation.

Fresh processes are required for effective batch settings and comparable
peaks. Separate diagnostic/correctness and timing runs. Report tracked memory,
allocator, RSS, cgroup anonymous/file memory, effective caches, and pressure
separately; do not add overlapping measurements together.

Retained-dataset runs follow the research handoff: verified read-only data,
scratch outside it, maintenance disabled, appropriate WAL handling, new evidence
roots, owned-process controls, and bounded guards. The original 12 GiB setup is
a guarded environment, not a chosen production budget.

Reject incorrect, incomplete, or over-budget variants first. Compare successful
variants at equal budgets and report complete-scan latency, first-output
latency, and the Pareto curve. Include failures/rejections in coverage reports
so comparisons cannot improve by silently omitting difficult workloads.

# Correctness and validation

Exact fixtures cover:

- Series spanning batches, IPC files, row groups, SSTs, and partition ranges.
- Identity synthesis after row selection/filtering, missing mappings, partial
  index coverage, and data projections excluding __primary_key.
- Duplicate timestamps/sequences, deletes, append mode, LastRow, LastNonNull,
  snapshot/exact-sequence reads, and selector placement.
- Memtable/SST overlap, schema evolution, timestamp units, defaults/null tags,
  and deferred tag attachment.
- Equal TSIDs across different table IDs and stable partition assignment.
- Resident/file mixtures, random batch seeks, per-series spans, changing input
  dictionaries, large single series, and unrelated rows in mixed batches.
- Memory-based admission across concurrent tasks, preparation-time reclamation,
  irreducible working-set errors, and final replay with no spill writes.
- Many small inputs that fit are accepted; fewer large inputs that cannot fit
  fail for memory. Counts alone never decide admission.
- Sequential partition polling, never-polled/abandoned consumers, notifications,
  cancellation, corrupt files, disk exhaustion, and cache pinning.
- Candidate chunk boundaries, catalog ownership, and cached output after the
  producer query has been destroyed.

Compare logical values with v2 and appropriate SeqScan references. Batch
boundaries and dictionary encoding may differ. Run focused stage tests,
cargo nextest run -p mito2 for integration, relevant command tests, and focused
SQLness PromQL regressions. Follow repository PR validation for formatting,
lint, license, full tests, dependencies, and public configuration.

# Review checkpoints

1. Review compact-identity synthesis and the coordinator/partition boundary.
2. Review IPC one-series/multiple-series measurements before choosing layout.
3. Review range preparation, memory admission, and per-query spill accounting.
4. Review shared reads and independent concurrency before selecting a policy.
5. Review integrated evidence before selecting defaults or promoting the mode.

Full-key data-phase prototypes, collision-based rescans, packed IPC streams,
external merge passes, final-replay spill, and global spill coordination from
the initial draft have been removed.
