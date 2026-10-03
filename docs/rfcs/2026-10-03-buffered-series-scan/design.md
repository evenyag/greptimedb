---
Feature Name: Buffered SeriesScan
Date: 2026-10-03
Status: Initial draft for review
---

# Summary

Add an experimental buffered SeriesScan mode while retaining current v2 as a
baseline. Keep candidate discovery and change the series data-reading phase:
fully merge a partition range, store its sorted result, release its readers,
and finally merge indexed results from memory or temporary Arrow IPC storage.

The first implementation processes one partition range at a time across the
scanner. Subsequent experiments vary range concurrency, bound source fan-in,
compact primary keys, and compare the current SST read pattern with reading the
union of selected series once. Choose the combination from measured correctness,
memory, and latency rather than enabling all optimizations together.

The [implementation and experiment plan](implementation-plan.md) describes
independent milestones and their acceptance gates. This document is a proposal;
it does not claim that the algorithms or performance have been validated.

# Motivation and current implementation

The local research report is at
`/Users/evenyag/Documents/test/promql-k8s-memory/reports/series-scan-memory-research-report.md`.
Its reproduction handoff is in the same directory, named
`series-scan-research-collection-handoff.md`. These files and their artifacts are
external research evidence, not checked-in dependencies of this RFC.

The report measured complete Q03 scans with range-result caching disabled:

| Batch rows | Actual output partitions | Peak sampled RSS, GiB | Output rows |
| ---: | ---: | ---: | ---: |
| 1024 | 1 | 3.347 | 51,635,200 |
| 1024 | 8 | 7.621 | 51,635,200 |
| 8192 | 1 | 3.906 | 51,635,200 |
| 8192 | 8 | 9.928 | 51,635,200 |

These runs are diagnostic evidence with tracing and guard overhead. They are
neither scored latency benchmarks nor predictions of a replacement scanner's
memory. The report's conditional reader-memory models exclude important
components and must not be interpreted as total RSS estimates.

Relevant current code:

- `src/mito2/src/read/series_scan.rs`: candidate distribution, TSID assignments,
  scanner mode selection, and partition streams.
- `src/mito2/src/read/series_reader.rs`: selected-series reading and two levels
  of merging.
- `src/mito2/src/read/seq_scan.rs`: flat merge, deduplication, and selectors.
- `src/mito2/src/read/range_cache.rs`: request fingerprints and resident result
  caching.
- `src/mito2/src/read/series_mapping.rs` and
  `src/mito2/src/sst/parquet/file_range.rs`: retained keys, row mappings, and
  selected-series row-group readers.

`SeriesReader::build_stream()` starts range-building tasks under a semaphore,
collects their streams, and then builds the final merge. A completed build task
releases its permit, but its stream can retain initialized readers. The permit
therefore bounds construction rather than the full reader lifetime.

Each output partition reads its selected series across the partition ranges.
Parquet row groups contain multiple series, so partition-local reads can retain
many decoders and repeat decoding across output assignments. Buffering the
merged results separates decoder lifetime from final series consumption.

# Scope and agreed direction

The initial work changes the data phase for native sparse metric scans eligible
for v2. Existing snapshot selection, source pruning, predicates, source-schema
compatibility, and candidate discovery remain the foundation.

Agreed choices from the design discussion:

- Add a separate opt-in experimental mode; preserve v2 for comparison.
- Sweep explicit resource budgets before selecting production defaults.
- The alternative SST policy reads the union of selected series, rather than
  indiscriminately decoding the whole SST.
- Allow a materialization barrier in the first prototype and measure first-row
  latency separately.
- Introduce file-backed data-result caching for the new mode first.
- Compare correct implementations at equal resource budgets, then choose by
  latency and report the memory/latency Pareto curve.

Candidate-discovery redesign, SST persisted-format changes, and migration of
every range-cache consumer are outside this initial scope. Candidate-phase
memory remains separately observable; bounding the data phase does not bound
the entire query.

# Terminology and data flow

| Term | Meaning |
| --- | --- |
| Partition range | Existing grouping of overlapping source time ranges, represented by `PartitionRange` and `RangeMeta`. |
| Output partition | Series assignment based on the TSID integer domain; distinct from a partition range. |
| Live reader | An initialized Parquet data reader retaining decoder/fetch state, including readers paused by a merge. |
| Range result | Sorted output after all sources of a partition range have been merged and range-level deduplication has run where required. |
| Intermediate run | Sorted materialized rows from only a subset of sources; not yet safe for complete range-level deduplication. |
| Segment | Independently readable storage unit containing batches and the dictionaries needed to interpret them. |
| Key window | An ordered interval processed during replay; a full primary key is not divided between windows. |

```text
existing candidate discovery
  -> selected-series assignments
  -> bounded partition-range processing
       current filtered SST streams OR shared selected-series materialization
  -> sorted, indexed range results
       resident RecordBatches OR temporary Arrow IPC segments
  -> bounded replay and merge for each output series assignment
  -> existing output projection
```

# Result-store interfaces

Use private read-path abstractions, with provisional names:

| Interface | Responsibility |
| --- | --- |
| `SeriesScanResources` | Shared reader permits, managed-memory reservations, spill quota, and cancellation. |
| `RangeResultBuilder` | Append sorted batches, build indexes, and spill incrementally. |
| `RangeResultHandle` | Own immutable resident/spilled content, schema, indexes, and completeness metadata. |
| `RangeResultCursor` | Replay selected key spans starting from a stored position. |
| `SeriesSourcePolicy` | Select existing filtered streaming or shared selected-series materialization. |

A result handle can contain both resident and spilled segments. A cursor must
not expose the storage choice to merge logic. Only complete results can be
published to consumers or the cache; intermediate runs carry an explicit stage
distinction so they cannot be mistaken for deduplicated range results.

Record a segment ID, batch index, and row offset for stored positions. Segment
metadata includes row count, key bounds, retained bytes, and serialized bytes;
spilled segments also record byte offset and length in their container. Use
checked arithmetic and offsets that can represent large results.

Build a coarse block index and series spans that cross batch and segment
boundaries. Charge index capacities to the budget. If indexes become large,
store paged index blocks on disk and retain only a small directory.

Output assignment uses TSID ranges, while key ordering starts with table ID.
Selecting one assignment can therefore require several spans across logical
tables. Do not represent it as one composite-key interval.

`RegionScanner` and user-visible output schemas stay unchanged. Explain and
benchmark output identify the selected mode, source policy, effective limits,
and representation.

# Bounded partition-range processing

Start with one active partition-range merge per scanner, shared across all
output partitions. Fully consume its stream into a result store before
releasing the processing permit. Drop readers and merge state at that point;
retain the result handle and only metadata still required by later work.

The range permit alone is insufficient: one overlapping range can contain many
SST sources. Introduce a separate reader cap whose permits cover construction,
consumption, and destruction of actual readers. Include auxiliary readers used
by the data phase in accounting; report candidate readers separately.

Acquire the permits needed for a merge group together before constructing its
readers. Avoid partial allocations in which every worker retains some readers
while waiting for permits held by another worker. Reader, replay, and spill
work must not share permits in a way that creates producer/consumer cycles.

For an oversized range, merge bounded groups into intermediate sorted runs,
release their readers, and merge the runs in bounded passes. Preserve sequence
numbers, operation types, and required fields until every contributing source
has participated. Do not apply partial `LastNonNull` deduplication, tombstone
removal, or last-row selection to these runs.

The existing merge order is primary key, timestamp ascending, and sequence
descending. Equal-sequence ties need characterization before changing merge
topology. Tests must establish whether different grouping can affect observable
winners; preserve existing behavior rather than assuming stable source order.

Apply deduplication at the complete partition-range boundary. Existing range
grouping combines overlapping time ranges, allowing the final merge to skip
deduplication as v2 does today. Assert this invariant in tests. Preserve selector
placement and avoid invoking a helper that adds selectors to intermediate
passes.

# SST read-policy alternatives

## A. Current filtered source streams

Keep per-output-assignment series filtering, source pruning, and current
row-group reader construction. Change only lifetime and result storage first.

This offers the smallest correctness delta and establishes how much memory can
be saved without changing SST access. It can still repeat decoding for different
output partitions.

## B. Shared selected-series materialization

Read candidate-pruned row groups for the union of selected series once during
the data phase. Preserve precise time/field/sequence filtering and schema
compatibility. Materialize sorted source results, then perform the complete
partition-range merge. Output partitions use indexed spans of shared results.

This separates SST reading from merging and allows bounded parallel row-group
reads. Reader results can spill before participating in the range merge.

"Once" refers to data reads across output assignments. Candidate discovery may
still read primary keys separately. Also distinguish one object read from one
row-group decode in metrics; they are different costs.

This policy waits for candidate completion. The candidate union must not become
an unbounded hash set or full-key allocation. For large assignments, use
accounted, pageable sorted storage, and extend source selection to consume its
pages. Adapting assignment consumption is in scope; rewriting discovery itself
is not required for the first comparison.

An all-rows-of-pruned-groups policy is not an initial experiment. Add it only if
measurements show that selected-series filtering or lookup dominates reading.

# Memory policy

Budgets are scanner-wide across output partitions. Increasing the partition
count must not independently multiply result-buffer or reader limits.

Track these ownership classes:

- Unique Arrow backing allocations retained by result segments, cursors, and
  internal queues.
- Dictionaries, catalogs, indexes, and container capacities.
- Merge output, IPC staging, and prefetch workspace.
- Live readers and their peak count.

Reuse the research allocation-base/capacity accounting approach, but keep
enforcement independent of diagnostic instrumentation. Shared slices and
dictionaries must not be double charged, and reservations must not be released
until the final internal owner drops the allocation.

Register retained buffers and workspace with the existing engine-wide scan
memory pool. Also enforce scanner-local retention and workspace limits. Spill
before retaining data that would exceed those limits; reservation pressure
from the engine-wide pool can trigger spilling earlier.

Keep spill workspace available when resident storage is full. Bound queues by
bytes as well as batch count. Account for input/output coexistence during
compaction or serialization and for batches pinned by active cursors.

Decoder allocations are not completely represented by Arrow buffer accounting.
Reader caps bound their multiplicity, not the size of each decoder. Measure
temporary overshoot and the largest indivisible batch. If a required allocation
cannot fit, fail with a resource error rather than wait indefinitely or claim a
false hard bound. Caller-owned yielded batches are outside retained-result
ownership; internal output queues remain accounted.

Report retained-buffer thresholds, workspace, allocator usage, RSS, and cgroup
memory separately. A threshold for buffered results is not a total RSS ceiling.
Metadata, candidate discovery, allocator behavior, and filesystem page cache
remain material contributors.

# Spill representation and lifecycle

The initial representation is an append-only temporary container of complete,
self-contained Arrow IPC stream segments. Start with one batch per segment,
including its required dictionaries. Store each segment's byte offset and
length so replay does not read earlier segments or the entire container.

Arrow IPC files support batch random access but prohibit dictionary replacement.
Current batches can have different dictionaries, so a single file writer needs
normalization or a segmentation boundary. See the
[Arrow IPC specification](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format).

Compare these alternatives behind the same result-store interface:

| Representation | Tradeoff to measure |
| --- | --- |
| Packed independent IPC streams | Simple dictionary lifetime and independent reads; repeated dictionaries and schema metadata. |
| IPC files with normalized segment dictionaries | Footer-based batch access and dictionary reuse; normalization memory and CPU. |
| Larger packed stream segments | Less metadata duplication; more dictionary state and larger replay units. |

Trim unused dictionary entries when beneficial, with accounted transformation
workspace. Write uncompressed IPC first, then compare LZ4 and Zstd. Memory
mapping is a later experiment because page residency and file pinning require
separate accounting.

Spill incrementally while producing a range. Prefer the oldest completed,
unpinned resident segments. Do not wait for the whole range to fit before
writing it. Release backing allocations only after successful writes and after
remaining owners have dropped them.

Use the shared blocking runtime for synchronous IPC encoding and filesystem
operations. Publish offsets only for complete segments. Scratch files are
ephemeral and require no cross-version persistence contract.

Use a dedicated scan scratch namespace outside SST data paths. Track disk usage
for source materialization, intermediate passes, complete range results, and
cache artifacts. Disk quota exhaustion or mandatory spill failure fails the
scan. Optional cache admission failure skips caching.

Cancellation and errors stop owned producers, close cursors, and release scratch
handles. Asynchronous cleanup must remain observable; disk usage is charged
until deletion. Startup cleanup removes abandoned scan scratch artifacts
without touching unrelated index or auxiliary storage.

# Final replay and merge

First replay stored ranges through the existing flat merger to validate the
result-store interface. This milestone may retain one decoded batch per range
and therefore does not yet establish bounded replay memory.

The bounded implementation reads indexed ordered key windows. Bound decoded
batches, prefetch bytes, open files, and merge fan-in across output partitions.
If a window still has too many contributing results, use bounded materialized
merge passes rather than initializing every cursor with a full batch.

Keep a full primary key inside one window, preserving complete series output.
A large series still streams in batches; a key window is not a request to load
the entire series. Test a single series spanning every range.

The initial mode uses a materialization barrier. Parallelism exists in bounded
source reading and final replay/merge, not in retaining all live source readers.
Measure first-row latency explicitly. Progressive readiness and earlier output
are follow-up scheduling experiments.

Output partitions can be consumed sequentially. Neither materialization nor
candidate dispatch may wait for every partition to be polled. Cancellation
releases work for abandoned consumers.

# Compact primary keys

Implement key compaction as a separate optimization after full-key buffering
and spilling are correct. Existing sparse encoding has a 22-byte reserved
prefix for `(table_id, tsid)`; use the codec rather than reproducing its encoding.

Before replacing dictionary values, finish source-schema decoding, predicate
evaluation, and compatibility handling. Preserve projected tags in the batch.
Build a fresh compact dictionary; slicing the full-key payload would keep its
backing allocation alive.

Prefix equality must not silently collapse distinct full keys. Extend key
retention to record conflicting full-key variants instead of silently keeping
the first key for an identity. Compact only paths that can establish selected
key uniqueness, including the coverage needed for index-backed reads. Paths
without that evidence remain in full-key mode.

If a conflict appears before output, discard compact data-phase results and
restart against the same snapshot with full keys. Preserve the baseline
behavior and tags. Tests must include schema-associated variants and multiple
full keys sharing an identity; the report's unique Q03 identities are evidence
for that dataset, not a universal invariant.

Keep the supporting full-key catalog accounted and store each required key
once where possible. Deferred tag expansion is a later experiment; the first
compact-key prototype retains current projected tag columns so its benefit can
be isolated.

# File-backed range-result cache

Introduce a new namespace for buffered-mode data results. Preserve existing
fingerprint rules for projections, predicates, sequence ranges, schema and
partition versions, and immutable SST identities. Preserve restrictions on
dynamic filters and memtable-backed ranges. Include result representation and
compact-key policy in cache identity.

Cache entries own complete artifacts and indexes. They cannot depend on a
query-local key or tag catalog after the producing query ends. Admit only
self-contained results; transferring artifact ownership should not require a
second full copy where safe.

Track disk capacity separately from metadata memory. Active cursors pin files.
Eviction removes lookup visibility immediately, but physical deletion waits
for the final pin. Pinned bytes remain charged until deletion, and admission
must account for that delayed reclamation.

Leave current v2, SeqScan, candidate-result, and row-mapping cache consumers
unchanged. Broader migration follows only after the new cache proves useful.

# Decisions reserved for measurement

| Question | Initial prototype | How to decide |
| --- | --- | --- |
| Range concurrency | One range per scanner | Compare complete latency and peak managed memory at equal reader limits. |
| SST policy | Current filtered reads | Compare shared selected-series reads for repeated decoding, spill traffic, and latency. |
| IPC layout | Packed one-batch streams | Compare normalization cost, dictionary duplication, replay latency, and workspace. |
| Compression | Uncompressed | Compare CPU, disk capacity, and read/write throughput with LZ4 and Zstd. |
| Compact keys | Full keys first | Enable only after correctness checks and independent memory measurements. |
| Replay strategy | Existing merger for correctness | Add bounded indexed windows/passes before claiming a complete memory bound. |
| Production defaults | Not selected | Use budget sweeps and representative queries; preserve opt-in rollout. |

These are experiment choices, not decisions an implementation should make
implicitly. Promotion requires the gates in the implementation plan and a
separate review of the resulting configuration and default policy.
