# Buffered SeriesScan implementation and experiment plan

Status: Initial draft for review, 2026-10-03.

Read the [design](design.md) for execution semantics, storage interfaces, and
correctness boundaries. This plan separates reusable result-store work from
scanner integration and from optional optimizations.

# Working conventions

- Preserve current v2 and existing uncommitted research changes. Record source
  and binary identity for every measured implementation.
- Keep experimental choices selectable in development tools so each can be
  measured independently. Production option names and defaults are a later
  integration deliverable, not frozen by this draft.
- Use existing scanbench/parquetbench infrastructure and bounded JSONL evidence.
  Put run artifacts in a new external evidence directory, not the completed
  research directory or the repository.
- Read applicable repository instructions before edits. New source files need
  repository license headers. Local debug binary builds use
  `CARGO_PROFILE_DEV_DEBUG=1` per `.local/AGENTS.md`.
- Begin with focused tests and microbenchmarks. Full retained-dataset runs follow
  only after correctness, resource, and cleanup checks pass.

# Stage 1: Baseline and comparison harness

Extend the current tools to accept explicit implementation, source policy,
reader limit, result-memory budget, workspace budget, merge fan-in, spill layout,
segment target, compression, and spill quota. Reject contradictory settings and
record the effective values, actual output partitions, and actual batch sizes.

Add measurements for candidate, source-read, range-merge, spill, replay, final
merge, output conversion, and cleanup phases. Track reader current/peak counts,
resident backing capacity, index/dictionary capacities, workspace, disk usage,
rows decoded/emitted, storage bytes, first-output time, and complete latency.

Implement a streaming comparison that checks logical values, per-partition
ordering, and stable series assignment without retaining the full output.
Separate diagnostic instrumentation from timing runs. Counts and checksums are
useful for large runs but do not replace exact small-fixture comparisons.

Gate: reproduce actual one/eight partition behavior and the complete Q03
reference of 51,635,200 rows. Preserve the unresolved older output-count
discrepancy rather than using its 54,579,200 figure as a new baseline.

# Stage 2: Result-store prototype and microbenchmarks

Implement the private builder, handle, cursor, and resource interfaces from the
design. Support resident segments, packed independent IPC streams, mixed
resident/spilled results, stored row offsets, and complete-result publication.
Initially use full primary keys and preserve current projected tags.

Implement byte-budgeted writes and reads, quota accounting, blocking-runtime
dispatch, error propagation, ownership-based cleanup, and bounded index pages.
Avoid concatenating an entire range for serialization or replay.

Use synthetic flat batches and representative selected row groups. Measure:

- Range-result retained capacity, including shared slices and dictionaries.
- Spill throughput, serialized size, CPU, and peak staging memory.
- Sequential replay and indexed seeks, including dictionary load costs.
- Segment sizes, container count, and file-open overhead.
- Cancellation and cleanup time after completed and partial writes.

Compare packed independent streams with normalized IPC-file segments and larger
packed stream segments. Add compression only after uncompressed round trips
pass. Include changing dictionaries, unused dictionary values, nulls, and schema
metadata in tests.

Gate: exact round trips, no partially published results, observed bounded
staging, and cleanup after errors/cancellation. Record the largest indivisible
batch and minimum working memory separately from configured segment targets.

# Stage 3: Sequential partition-range data phase

Add a buffered mode using v2's eligibility checks, candidate discovery,
assignments, pruning, and current selected-series readers. Share one active
range-processing permit across the scanner's output partitions.

Drain each range stream into the store before releasing the permit. Instrument
reader destruction so a "range complete" event cannot conceal retained decoder
state. Replay the completed results through the existing final flat merger.

Keep this milestone's limitations explicit: the largest range can still have
large source fan-in, and replay can still retain one decoded batch per result.
Do not describe it as a complete bounded-memory implementation.

Gate: output matches v2 on exact fixtures, range-reader lifetime is reduced,
sequential partition consumption works, and output/cancellation errors reach
all relevant consumers without detached producers.

# Stage 4: Bounded source fan-in and final replay

Introduce scanner-wide reader permits held for actual reader lifetime. Acquire
whole source-group allocations together. Add bounded intermediate merge passes
for oversized ranges, without deduplication, selectors, or tombstone removal
until the complete range boundary.

Characterize equal-sequence ties before changing merge topology. Preserve
observable winner behavior; do not assume grouping is associative for every
deduplication mode.

Add indexed key-window replay with bounded decoded batches, byte-prefetch,
open files, and merge fan-in. Use stored merge passes when needed. Keep full
primary keys within a key window, but stream a large series in batches.

Integrate managed-memory reservations with the existing engine-wide scan pool.
Maintain separate scanner-local retention and workspace budgets. Spill on
reservation pressure, and return a resource error for irreducible allocations.
Account for transient input/output coexistence and pinned backing buffers.

Gate: one very large overlapping range, many ranges, and one series spanning
all ranges respect reader and managed-memory limits. Increasing output
partitions does not independently multiply these limits. Disk exhaustion and
memory pressure terminate cleanly rather than deadlock.

# Stage 5: Compact-key experiment

Add fresh 22-byte prefix dictionaries using the existing sparse codec, after
source decoding, filtering, compatibility, and projected tag materialization.
Track the full-key catalog, dictionaries, and transformation workspace.

Add full-key variant detection and explicit full-key fallback for paths that
cannot prove selected identity uniqueness. A late detected conflict discards
compact results and restarts the data phase at the same snapshot before output.
Index-backed reads require the same correctness evidence.

Compare full-key and compact-key runs using identical source/spill policies.
Measure retained capacity and serialized bytes separately; repeated projected
tags may remain a significant cost even after keys shrink.

Gate: values, projected tags, ordering, and deduplication match references.
Variants sharing an identity never collapse silently. Memory savings survive
accounting for catalogs and temporary transformation allocations.

# Stage 6: Shared selected-series SST reads

Add the alternative source policy: consume candidate discovery to completion,
read the selected-series union once across output assignments in the data
phase, materialize sorted source results, then merge complete partition ranges.
Allow bounded parallel row-group reads while readers remain capped.

Store large candidate assignments in accounted pageable sorted storage and
adapt selection to read its pages. Preserve pruning and precise filters.
Avoid unbounded queues for output partitions that have not been polled.

Compare with the current per-assignment SST policy at equal budgets. Measure
repeated row-group decoding, selected versus decoded rows, source spill bytes,
merge passes, complete latency, and first-output latency. Separate costs saved
by sharing reads from costs introduced by extra materialization.

Gate: no duplicated or missing rows across assignments; large candidate sets
and sequential partition consumption work within managed budgets. The measured
tradeoff determines whether this policy belongs in the integrated candidate.

# Stage 7: File-backed cache for buffered data results

Add representation-aware cache keys while preserving current fingerprint and
eligibility rules. Admit only complete self-contained data results. Transfer
artifact ownership where safe, rather than copying the full result again.

Implement disk capacity, metadata-memory limits, cursor pinning, delayed
deletion, admission rejection, and startup cleanup. Pinned bytes remain charged
until deletion. Keep candidate, mapping, SeqScan, and current v2 caches intact.

Compare disabled, cold, and warm cache runs. Validate schema/sequence/filter
changes, dynamic-filter bypass, memtable exclusion, concurrent consumers, and
eviction while a cursor is active.

Gate: cache reuse preserves results, neither cache nor query handles outlive
required dependencies, and admission/eviction cannot exceed accounted capacity.

# Stage 8: Integrated evaluation and rollout decision

Select independently validated components and rerun correctness and resource
tests for their combination. Preserve explicit modes in the tools so a
regression can be attributed to a component.

Add documented opt-in experimental configuration only after the measured
selection. Keep current defaults until a separate decision to promote or
replace v2. Configuration work includes example TOMLs, loading/serialization
tests, generated configuration documentation, and matching user documentation.

Produce a result summary with raw evidence links, source/binary bindings,
effective settings, phase memory, latency, first-output time, spill
amplification, cleanup, and remaining limitations. Report the Pareto curve
rather than declaring one policy universally fastest.

Gate: the integrated candidate is correct, respects its resource contracts,
cleans up, and has reproducible comparison evidence. A deployment or default
change is a separate reviewed action.

# Experiment matrix and execution order

Run focused sweeps before combinations; do not start with the full Cartesian
product.

| Variable | Initial values |
| --- | --- |
| Reader cap | 8, 16, 32, 64 |
| Resident result budget | 128, 256, 512, 1024 MiB |
| Workspace | Explicit, recorded independently; determine minimum viable size in microbenchmarks |
| Range concurrency | 1, 2, 4, constrained by reader/workspace limits |
| Output partitions | 1 and 8 |
| Batch rows | 1024 and 8192; smaller batches for storage microbenchmarks |
| Source policy | Current filtered reads; shared selected-series union |
| Result retention | Resident when it fits; forced spill; adaptive spill |
| Keys | Full; compact where eligibility is established |
| IPC segment target | 1, 8, 32 MiB; record oversized indivisible batches separately |
| Compression | Uncompressed first, then LZ4 and Zstd |
| Data-result cache | Disabled first, then cold and warm |

Progress from synthetic overlap/selectivity cases to representative retained
row groups, complete Q03, and then the remaining query suite. Include skew,
many logical table IDs, high selected-series cardinality, and concurrent scans.
Use exact requests for correctness; Q11's displayed regex must not be silently
replaced by an unverified SQL translation.

Fresh processes are required for runtime batch configuration and comparable
peaks. Separate correctness/instrumented runs from timing runs. Record allocator,
RSS, cgroup anonymous/file memory, effective caches, and pressure events; do not
sum overlapping measurements as independent memory components.

Retained-dataset runs follow the research handoff: verified read-only data,
scratch outside the dataset, no maintenance or WAL replay that modifies it,
unique evidence roots, owned-process controls, and bounded guards. The original
12 GiB setup is a useful guarded environment, not the chosen production budget.

For selection, first reject incorrect, incomplete, or over-budget variants.
Compare surviving variants at equal reader, managed-memory, and disk budgets.
Use complete-scan latency as the primary speed comparison and report first-row
latency separately. If policies trade wins across workloads, retain their
measured Pareto results for review rather than inventing an automatic heuristic.

# Correctness and validation

Exact fixtures must cover:

- Series crossing batches, segments, row groups, SSTs, and partition ranges.
- Duplicate timestamps and sequences, deletes, append mode, `LastRow`,
  `LastNonNull`, and selector placement.
- Memtable/SST overlap, snapshot and exact-sequence reads.
- Schema evolution, timestamp units, missing/null tags, and compatibility.
- Table IDs, TSID boundaries, sparse selections, full-key variants, and unused
  dictionary values.
- Resident/spilled mixtures, repeated seeks, oversized batches, and multiple
  merge passes.
- Sequential partition consumption, abandoned consumers, cancellation, disk
  exhaustion, corrupt segments, and cache eviction with active pins.
- Assignments crossing the existing candidate chunking threshold.

Compare against v2 and appropriate SeqScan references, checking logical values,
per-partition ordering, and stable assignment. Batch boundaries and dictionary
layout may differ; compare their logical content rather than encoded equality.

Run narrow tests as stages land, then `cargo nextest run -p mito2` for integrated
read-path changes, focused command tests for tool changes, and relevant SQLness
regressions for PromQL behavior. Before a PR, follow the repository's formatting,
lint, license, full-test, dependency, and configuration checks.

# Review checkpoints

1. Review this draft before implementing the harness and result-store prototype.
2. Review spill/replay microbenchmarks before selecting IPC normalization and
   segment defaults.
3. Review sequential-range results before adding more range concurrency.
4. Review bounded-merge correctness and measurements before combining compact
   keys and shared reads.
5. Review the integrated comparison before choosing configuration defaults or
   changing the default scanner.

The first review should focus on the proposed boundaries: separate mode,
materialization barrier, range/source/replay limits, compact-key correctness,
and how far shared-read assignment storage should extend beyond current v2.
