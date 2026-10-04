# Buffered SeriesScan implementation and experiment plan

Status: Draft revised after VictoriaMetrics review, 2026-10-03.

Read the [design](design.md) for execution semantics and interfaces. Preparation
runs outside scan partition streams; those streams lazily concatenate complete
ranges per identity and assemble tags after notification. The core merge key is
(table_id, tsid). Range results can spill to Arrow IPC files before replay;
final replay never spills. Keep the initial complete-preparation barrier and
reserve aggregate replay workspace before publishing manifests. The design's
VictoriaMetrics comparison records the source revisions behind this review;
its compressed-block staging is distinct from our prepared Arrow results.

# Progress

Update this checklist as work lands. Mark a stage complete only after its
implementation and gate pass, and record the validating commit and evidence
path alongside the completed item. Documentation updates alone do not complete
an implementation stage.

- [x] Create `perf/buffered-series-scan-poc` from main at `3e86422ef8` in the
  existing checkout and carry over the reviewed RFCs from `2939662e6a`.
- [x] Record the external artifact directory and progress-tracking convention.
- [x] Record the external remote build/experiment workflow and link it below.
- [x] Verify the existing seven-day query bench and loaded dataset; record
  setup evidence and the [fresh-thread handoff](/Users/evenyag/Documents/test/promql-k8s-memory/reports/buffered-series-scan-poc-setup-handoff.md)
  (2026-10-03; setup only, no new measurements).
- [x] Stage 1: Baseline tooling, exact comparison, and preparation/readiness
  harness implemented at `1cca645d66`; normal-profile local validation at
  `bf692aa28b` passed 1,702 tests, Clippy, and formatting. Polling-independence
  and one-partition exact comparison passed. Current baseline accepted by the
  user on 2026-10-03: main eight-partition timeout is an expected outcome,
  while research V6 reproduced 51,635,200 rows at one/eight partitions.
  Main eight-partition exactness and cross-partition comparison remain
  unvalidated; acceptance does not claim these checks passed. See the
  [Stage 1 report](/Users/evenyag/Documents/test/promql-k8s-memory/reports/buffered-series-scan-poc-stage1-report.md)
  and external evidence under
  `/Users/evenyag/Documents/test/promql-k8s-memory/buffered-series-scan-poc/20261003-stage1/`.
- [x] Stage 2: Mapping preflight, compact identity/schema, and deferred tags
  implemented and validated at `2603f14aea` (2026-10-04). Local focused tests
  (23), reference/compact SQLness, Clippy, formatting, and license checks passed.
  Retained Q03 p1 and p8 each completed 51,635,200 rows; exact comparison against
  Stage 1's p1 reference passed, including cross-partition ordering and unique
  series ownership. Every compact decoder passed the primary-key projection/
  byte-request audit, with zero data-phase key-page decoding and violations.
  Warm candidates/cold mappings, partial indexes, multi-table identities, and
  preflight failure before data reads passed local synthetic gates. See the
  [Stage 2 report](/Users/evenyag/Documents/test/promql-k8s-memory/reports/buffered-series-scan-poc-stage2-report.md)
  and evidence under
  `/Users/evenyag/Documents/test/promql-k8s-memory/buffered-series-scan-poc/20261004-stage2-remote/`.
  Main v2's historical p8 exactness gap remains unchanged.
- [x] Stage 2 operation diagnostics added at `8b33dd7373` (2026-10-04).
  Local focused tests (11), Clippy, and formatting passed. Six seven-day Q03
  captures passed operation-count/byte reconciliation and zero data-phase
  primary-key audits; verbose plans, two native CPU graphs, and eight heap
  graphs were retrieved and verified. See [metric definitions](metrics.md)
  and the external
  [operation-cost report](/Users/evenyag/Documents/test/promql-k8s-memory/reports/buffered-series-scan-poc-stage2-operation-metrics.md).
  These diagnostic captures do not replace Stage 2's exact-output evidence.
- [ ] Stage 3: Resident/IPC result store, both batch layouts, shared metadata,
  and cleanup; pass exact round-trip and independent-cursor gates.
- [ ] Stage 4: Complete range preparation, spill admission, publication
  reservations, and lazy replay; pass resource and selector correctness gates.
- [ ] Stage 5: Independent preparation concurrency and shared selected-series
  reads; pass correctness gates and compare equal-budget measurements.
- [ ] Stage 6: File-backed buffered-data cache; pass ownership, invalidation,
  pinning, and replay-without-writes gates.
- [ ] Stage 7: Integrated evaluation and rollout decision; record reproducible
  evidence and select experimental settings without changing existing defaults.

# Working conventions

- Edit code and tests locally on `perf/buffered-series-scan-poc` in the existing
  checkout; do not create another local worktree. Keep
  `perf/series-scan-key-reuse` at `2939662e6a` as the research baseline. Preserve
  unrelated edits and record source and binary identity for measurements.
- Compile, test, and lint locally using the normal development/test profiles.
  Push passing code, synchronize the existing remote checkout, and build the
  measurement binary remotely using its existing build cache. Run retained-data
  experiments remotely; do not run routine tests with the remote nightly profile.
  Machine-specific connection details and the exact build command are recorded
  only in the external
  [remote workflow document](/Users/evenyag/Documents/test/promql-k8s-memory/reports/buffered-series-scan-poc-remote-workflow.md),
  not in tracked files. Do not run retained-data PoC experiments locally.
- Carry over only the row-mapping/reader primitives and benchmark tooling
  required by the relevant stage. Research-branch diagnostics and fixes are not
  automatically PoC dependencies. Compare main v2, research-branch v2, and the
  buffered mode with their effective settings recorded.
- Keep development options for preparation concurrency, source policy,
  per-query memory budget/spill threshold, IPC layout, batch size, and compression.
  Reader and merge-input counts are metrics, not admission limits.
  Production option names/defaults follow measured selection.
- Use existing scanbench/parquetbench and bounded JSONL evidence. Keep remote
  results and scratch outside the Git checkout and read-only dataset, in fresh
  run directories. Store retrieved results, logs, profiles, and other local
  uncommitted artifacts under
  `/Users/evenyag/Documents/test/promql-k8s-memory/buffered-series-scan-poc/`.
  Give each run a fresh subdirectory; preserve completed research artifacts
  elsewhere under the parent directory. Commit implementation, tests, RFC
  progress, and concise evidence summaries/references, not raw run artifacts.
- Read applicable repository instructions before editing. Apply license headers
  to new source files; follow the external workflow for remote builds.
- Use focused tests/microbenchmarks before retained-dataset runs. Do not add
  external merge passes, full-key replay fallback, or engine-wide spill
  scheduling to the initial implementation.

# Fresh-thread resumption

Stage 2 passed at `2603f14aea`; Stage 3 is next. Read the external
[Stage 2 report](/Users/evenyag/Documents/test/promql-k8s-memory/reports/buffered-series-scan-poc-stage2-report.md)
and its linked evidence/checkpoints before continuing. Compact p1/p8 exactness
and decoder audits passed; timing samples were 60.482s/46.430s with sampled RSS
3.025/7.771 GiB under the unchanged controlled-cache guards. These are diagnostic
samples, not production defaults or a statistical speedup claim. Source state,
research artifacts, and owned-process cleanup were verified; raw evidence was
retrieved and hash-checked. The user may stop the remote machine; obtain startup
confirmation before any later remote work if it has been stopped.

The subsequent diagnostics at `8b33dd7373` quantify CPU, fetch latency, read
amplification, assembly calls, and tag-catalog lock waits. Read the linked
operation-cost report before optimizing: p8 assembly included 90.148 cumulative
seconds of catalog-lock waits, and requested data payload was 4.31 times p1's.
These are individual diagnostic samples, not exclusive query-time shares.
All evidence was retrieved and owned-process cleanup verified.

Stage 1's current baseline remains accepted as recorded in the
[Stage 1 report](/Users/evenyag/Documents/test/promql-k8s-memory/reports/buffered-series-scan-poc-stage1-report.md).
Its main eight-partition exactness remains unvalidated. Stage 2 compared compact
p8 against the accepted p1 oracle and did not retune that baseline. Reuse the
existing seven-day query bench and loaded eight-day dataset; do not regenerate
or reload. Verify actual request windows because historical plan-collection SQL
used one hour despite seven-day dispatch parameters.

# Stage 1: Baseline and preparation/readiness harness

Extend the tools with explicit settings and report effective mode, source
policy, query memory budget/spill threshold, actual output partitions,
batch size, and IPC layout.

Measure candidate discovery and mapping preflight, source reads, range merge,
spill/finalization, readiness wait, final replay, tag assembly, and cleanup.
Count live readers, decoded primary-key pages in the data phase, retained
capacities, catalog/index bytes, workspace, disk usage, rows, storage traffic,
and first-output/complete latency.

Build an exact streaming comparison for logical values, ordering within
partitions, and series assignment. Separate timing runs from diagnostic runs.

Prototype a preparation coordinator and owned readiness manifests. It starts
once, schedules work outside partition streams, explicitly marks empty results
complete, notifies consumers, and propagates errors/cancellation. Test
sequential polling and consumers that are never polled.

Gate: reproduce one/eight partitions and Q03's 51,635,200 rows. Keep the older
54,579,200-row discrepancy unresolved rather than adopting it as the baseline.
Readiness must not depend on every output partition being polled.

Acceptance recorded on 2026-10-03: the user accepted the current results after
confirming that high memory and eight-partition timeouts are expected baseline
behavior. Main one-partition exact comparison and readiness tests passed;
research V6 reproduced the required row count at one/eight partitions. Main
eight-partition reproduction and exact cross-partition comparison were not
validated. Preserve that gap in later comparisons rather than retuning this
baseline to force a pass. Stage 2's reference-output and zero-key-page gates
remain required.

# Stage 2: Compact identity reads and deferred tag assembly

Extend row-group data readers to synthesize 22-byte compact __primary_key values
from (table_id, tsid) row mappings/range indexes. Exclude SST __primary_key from
the data projection. Intersect identity runs with selections and keep identity
arrays aligned with filtered field arrays.

Preflight mapping coverage for every required source row group, independently
of candidate-cache hits and series-index coverage. Reuse range indexes and
complete cached mappings; populate missing mappings with discovery-phase
key-only reads before admitting data readers. Keep mappings owned through
their readers and report preflight reads separately. Fail with source/row-group
context if coverage cannot be established; never invoke a lazy full-key mapping
read in the data phase.

Define an explicit tag-free compact batch schema and field-column boundary,
preserving existing flat-format internal column conventions. Adapt merge,
deduplication, LastNonNull, source compatibility, and residual field predicates
to this schema rather than the output mapper's tag-bearing schema/offsets.

Create an accounted tag catalog keyed by identity, normalized to the required
output schema. Data batches carry fields, timestamp, compact key, sequence, and
operation type without expanded projected tags. After final replay, join tags by
identity and apply output projection.

Preserve snapshot/sequence rules, trusted overrides, field/time predicates,
source compatibility, deduplication, and selectors. Memtable sources use the
same compact identity model.

Gate: output values/tags/order match reference metric scans. Equal TSIDs in
different table IDs stay distinct. Instrumented data reads decode zero SST
__primary_key pages, including warm candidate caches with cold mappings, partial
index coverage, and series-index coverage without row mappings. Verify that
preflight establishes coverage or fails before data reads. Tag-free schema
fixtures cover LastNonNull, deletes, schema evolution, and selectors. Discovery
and preflight key reads remain reported separately.

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

Share immutable IPC schema, batch-directory, and dictionary metadata across
independent cursors. Decode requested batches using this shared metadata rather
than opening a complete FileReader for every series. Account for metadata once
and preserve its lifetime while handles/cursors need it. Bound files by payload
and metadata growth, including files with many tiny one-series batches.

Measure:

- Write and sequential/random read throughput, CPU, and serialized size.
- Direct series lookup and memory retained for unrelated series.
- Per-series rows/bytes and distributions, not just average batch size.
- Batch/footer/index overhead and footer growth with batch count, file count,
  seeks, file-open costs, and reader initialization count/time.
- Repeated metadata loading, shared metadata accounting, and retained file-wide
  dictionaries during direct series lookups.
- Dictionary conversion, serialization, IPC decoding, compact-array
  reconstruction, and replay workspace.
- Cancellation and cleanup after completed and partial writes.

Start uncompressed; compare LZ4/Zstd after exact round trips pass.

Gate: both layouts round-trip compact data correctly, positions select the
right series, partial files are never published, and all owned storage is
released after cancellation/errors. Many one-series batches exercise shared
metadata initialization and retention; independent cursors cannot interfere
with each other's positions.

# Stage 4: Range preparation, spill threshold, and final replay

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
Characterize equal-sequence ties. Keep complete groups of overlapping sources
without row-group splitting and validate previous maximum timestamp < next
minimum timestamp using inclusive bounds and correct timestamp units. Touching
intervals must share a group. Validate prepared spans against range bounds and
fail clearly on invariant violations before publication.

Use one per-query tracker for catalogs, results, indexes, buffers, queues, and
workspace. During preparation, crossing the threshold spills eligible range
results. Keep writer workspace available and account for transient overlap.

Keep the complete-preparation barrier. Before publishing any manifests, reserve
aggregate replay workspace for all partitions that may run concurrently,
including decoded payload, bounded prefetch, IPC/compact-array conversion,
selector state, output, and tag assembly. Include unrelated backing rows for
mixed-series batches; reserve only incremental workspace beyond retained
allocations already charged. Borrowed resident buffers must not be reserved a
second time. Spill unpublished results until retained results/metadata and
aggregate reservations fit; fail irreducible requirements with the memory
diagnostic contract. Transfer reserved capacity into live allocation charges
without double charging and return it to the partition reservation when
workspace is released. Release remaining reservations on completion/drop.

Partition streams enumerate compact identities in order and lazily concatenate
each identity's complete ranges in timestamp order. Use indexed spans to open
only the current payload and bounded prefetch, skipping empty contributions by
metadata. Apply the final selector across the concatenated identity, attach
tags, and yield bounded output batches. They never collect a whole decoded
series, write spill files, or cause published results to migrate to disk.
Any later early-publication experiment must also reserve remaining preparation
and writer workspace without depending on unpolled partitions releasing memory.

Gate: reader destruction is observed at range completion; readiness and
sequential partition consumption work; spill follows aggregate query accounting;
final replay performs zero spill writes. At fixed batch size, width, and
prefetch, active replay payload remains bounded as one series spans increasing
numbers of complete ranges; index/metadata growth is reported separately.
Eight manifests with concurrent, sequential, slow, and never-polled consumers
cannot overcommit aggregate reservations. Touching intervals and invalid range
spans exercise the complete-range invariant, and final selectors match v2.
Large reader/input counts alone never reject a query. Missing mappings, disk
exhaustion, and irreducible memory requests fail and clean up.

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
publishing final-replay handles. Do not write cache content in partition replay
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
| Contributing complete ranges | Increase ranges for one series at fixed batch size/width; separate active payload from index/metadata growth |
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

For one-series layouts, measure small-batch overhead, footer growth, metadata
initialization, and lookup latency; for multiple-series layouts, measure
unrelated rows retained by final cursors. Measure dictionary retention and
repeated metadata loads for both layouts, including the fixed-dictionary variant.
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
- Mapping preflight with warm candidate caches/cold mapping caches and
  series-index coverage without row mappings; discovery reads stay outside
  the data phase and failed preflight admits no data readers.
- Duplicate timestamps/sequences, deletes, append mode, LastRow, LastNonNull,
  snapshot/exact-sequence reads, and selector placement.
- Memtable/SST overlap, schema evolution, timestamp units, defaults/null tags,
  and deferred tag attachment.
- Explicit compact schema/field offsets with LastNonNull, deletes, source
  compatibility, and final selectors.
- Equal TSIDs across different table IDs and stable partition assignment.
- Resident/file mixtures, random batch seeks, per-series spans, changing input
  dictionaries, large single series, and unrelated rows in mixed batches.
- Many one-series IPC batches, independent cursors sharing file metadata,
  dictionary retention, and initialization count/metadata accounting.
- Inclusive touching time bounds, unsplit complete ranges, invalid range spans,
  lazy replay across increasing range counts, and final selectors across spans.
- Memory-based admission across concurrent tasks, preparation-time reclamation,
  irreducible working-set errors, and final replay with no spill writes.
- Many small inputs that fit are accepted; fewer large inputs that cannot fit
  fail for memory. Counts alone never decide admission.
- Sequential partition polling, never-polled/abandoned consumers, notifications,
  cancellation, corrupt files, disk exhaustion, and cache pinning.
- Aggregate publication reservations for eight manifests under concurrent,
  sequential, slow, and never-polled consumers, including release on drop and
  transfer to allocation charges without double counting.
- Candidate chunk boundaries, catalog ownership, and cached output after the
  producer query has been destroyed.

Compare logical values with v2 and appropriate SeqScan references. Batch
boundaries and dictionary encoding may differ. Run focused stage tests,
cargo nextest run -p mito2 for integration, relevant command tests, and focused
SQLness PromQL regressions. Follow repository PR validation for formatting,
lint, license, full tests, dependencies, and public configuration.

# Review checkpoints

1. Review mapping preflight, compact schema/identity synthesis, and the
   coordinator/partition boundary.
2. Review IPC one-series/multiple-series payload and metadata measurements
   before choosing layout.
3. Review complete-range invariants, lazy replay, publication reservations,
   memory admission, and per-query spill accounting.
4. Review shared reads and independent concurrency before selecting a policy.
5. Review integrated evidence before selecting defaults or promoting the mode.

Full-key data-phase prototypes, collision-based rescans, packed IPC streams,
external merge passes, final-replay spill, and global spill coordination from
the initial draft have been removed.
