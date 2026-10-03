---
Feature Name: Buffered SeriesScan
Date: 2026-10-03
Updated: 2026-10-03
Status: Draft revised after first review
---

# Summary

Add an experimental buffered SeriesScan mode while retaining current v2 as a
baseline. Keep candidate discovery and change the series data-reading phase:
prepare and merge partition-range results in independently scheduled tasks,
buffer or spill those results, release their SST readers, and notify output
partitions when their final-merge inputs are ready.

Scan partition streams perform only final merge/replay and output assembly.
Source reading, range-result preparation, spilling, and readiness coordination
run outside those streams. This keeps preparation concurrency independent of
the number and polling order of output partitions.

Use (table_id, tsid) as the series identity throughout the new data phase,
merging, and replay. Synthesize a compact encoded __primary_key from row mappings
or range indexes without reading the SST __primary_key column in that phase.
Keep decoded tags separately and join them to merged rows during output
RecordBatch assembly.

Spill prepared range results to Arrow IPC files before final replay. Final
replay does not write spill files or perform external merge passes. Resource
admission and rejection depend on memory, not reader or merge-input counts.

The [implementation and experiment plan](implementation-plan.md) separates
prototype work and comparisons. The preferred spill format is decided; whether
IPC batches contain one series or multiple series remains an experiment.

# Motivation and current implementation

The local research report is at
/Users/evenyag/Documents/test/promql-k8s-memory/reports/series-scan-memory-research-report.md.
Its reproduction handoff is in the same directory, named
series-scan-research-collection-handoff.md. These are external evidence, not
checked-in dependencies of this RFC.

The report measured complete Q03 scans with range-result caching disabled:

| Batch rows | Actual output partitions | Peak sampled RSS, GiB | Output rows |
| ---: | ---: | ---: | ---: |
| 1024 | 1 | 3.347 | 51,635,200 |
| 1024 | 8 | 7.621 | 51,635,200 |
| 8192 | 1 | 3.906 | 51,635,200 |
| 8192 | 8 | 9.928 | 51,635,200 |

These are diagnostic runs with tracing and guard overhead, not scored latency
benchmarks or predictions of a replacement scanner's footprint.

Current SeriesReader::build_stream() starts range-building tasks under a
semaphore, collects their streams, and builds a final merge. Build-task
completion releases a permit but can leave initialized readers retained by the
returned stream. The permit limits construction rather than reader lifetime.

Each output partition reads its selected series across partition ranges.
Parquet row groups contain multiple series, so these reads can retain many
decoders and repeat work across assignments. Prepared range results separate
SST-reader lifetime from final series consumption.

Relevant implementation areas are the read modules series_scan, series_reader,
series_mapping, seq_scan, and range_cache, plus SST parquet file_range and
reader. The existing reader_by_series path already uses range-index or cached
row mappings to select absolute row runs; the new path changes key synthesis
and postpones tag attachment.

# Review decisions and scope

The first review establishes these requirements:

- Preparation tasks run outside scan partition streams. Notify a partition only
  when the inputs needed for its final merge are complete and replayable.
- Limit memory rather than reader count or merge fan-in. Fail a merge only if
  its required working memory cannot fit after preparation-time reclamation.
  Do not add intermediate external merge passes.
- Use only (table_id, tsid) for merge identity. Full encoded tag keys are not an
  alternative data-phase merge representation.
- Attach decoded tags after merge instead of retaining expanded tag columns in
  every range result.
- Spill range results before final replay; final replay never spills.
- Start with per-query memory accounting for spill decisions.
- Use Arrow IPC files with random batch access. Compare one-series batches with
  batches containing multiple series.
- Do not read SST __primary_key pages in the new series data phase. Obtain IDs
  from range indexes or cached row mappings and encode the compact key.

Preserve v2 as a baseline, opt-in rollout, explicit budget sweeps, the shared
selected-series read experiment, and file-backed data-result caching for the
new mode first. Compare correct implementations at equal resource budgets, then
compare latency and report the Pareto curve.

The initial scope is native sparse metric scans eligible for v2. Existing
snapshot selection, pruning, predicates, source compatibility, and candidate
discovery remain the foundation. Candidate discovery may read full keys to
evaluate tags and build mappings. Avoiding primary-key page decoding applies
to the series data phase, not to the entire query.

Persisted SST-format changes, broad candidate-discovery redesign, automatic
external merge, and migration of every cache consumer are outside this work.

# Terminology and execution model

| Term | Meaning |
| --- | --- |
| Partition range | Existing grouping of overlapping source time ranges. |
| Output partition | Scan stream assigned an interval of the TSID domain. |
| Series identity | The pair (table_id, tsid), also used for compact merge keys. |
| Range result | Sorted, completely merged output for one partition range and its selected-series scope. |
| Series span | Positions of one identity within resident batches or IPC-file batches. |
| Readiness manifest | Complete result handles and tag context needed by an output partition for an assignment. |
| Live reader | Initialized Parquet reader retaining decoder/fetch state, including a paused reader. |

    existing candidate discovery and decoded tag catalog
      -> preparation coordinator outside scan partition streams
           bounded SST reads and partition-range merges
           compact keys + fields + timestamp + sequence + operation
           query-local accounting -> resident results or Arrow IPC files
      -> complete readiness manifest for each output assignment
      -> notification to scan partition
           indexed final replay/merge using (table_id, tsid)
           attach decoded tags and apply output projection
           yield output RecordBatches

Start the preparation coordinator once. It schedules source reads and
range-result merges independently of output stream polling. Initially schedule
one partition-range preparation at a time, then vary concurrency without moving
tasks into partition streams.

A partition waits for a manifest covering every partition-range result needed
by its current assignment. A missing/empty contribution must be explicitly
complete, not mistaken for work still in progress. Manifest publication is the
boundary after which those results are immutable and no longer spillable.

A first prototype can publish after all preparation completes. Later it can
publish complete assignments earlier; it must not treat one finished range as
permission to emit a series whose other contributing ranges are still pending.

Use an owned readiness state or notification mechanism that does not block
preparation on an unpolled output partition. Notifications carry handles and
completion/error state, not queues of decoded output batches. Sequential
partition consumption, abandoned partitions, and cancellation must work.

# Identity, source reads, and deferred tags

## Index-backed compact key synthesis

Read table ID, TSID, and absolute row runs from the existing range index or
cached series-row mappings. Intersect runs with pruning selections, then map
the returned selected rows to their identities. Apply the same row selection
to identities and field arrays so filtering cannot misalign them.

Use the existing sparse codec to encode its 22-byte (table_id, tsid) prefix as
__primary_key. Build compact dictionaries from identities, without constructing
or slicing a full encoded tag key. Data merge order is compact key, timestamp
ascending, and sequence descending; deduplication uses identity and timestamp.

Candidate discovery supplies decoded projected tags and row mappings. Without
a range index, candidate discovery can populate mappings while it reads keys.
Before data reads start, require mapping coverage for their source row groups.
If required identity metadata is unavailable, fail the buffered-mode query
with a clear error. Do not silently read large primary-key pages in the new
data phase or switch its merge identity to full keys.

Memtable inputs likewise expose or derive identity before entering the compact
merge. Reuse snapshot and sequence filtering, trusted sequence overrides,
time/field predicates, and source-schema compatibility. Candidate tag
predicates stay in discovery; required residual tag predicates are evaluated
with decoded tag context at the correct existing semantic boundary.

## Tag catalog and output assembly

Keep an accounted tag catalog keyed by (table_id, tsid), containing normalized
decoded values for projected tags and any tags needed by remaining predicates.
Preserve source-schema/default/null handling during catalog preparation.

Range-result batches contain fields, timestamp, compact identity, sequence, and
operation type, but no per-row projected tag arrays. After the final merge,
look up the identities of the resulting rows and join their decoded tag values
to assemble the output RecordBatch, then apply the existing output projection.

The pair is the series identity by design. Do not add full-key variant catalogs,
collision-based rescan, or full-key replay fallback. Test that equal TSIDs in
different table IDs remain distinct and that output tags match the existing
metric scan semantics.

# Preparation concurrency and memory admission

Use per-query memory reservations to admit active range preparations, SST
readers, and spill writers. There is no configured live-reader cap or merge
fan-in limit. Their counts remain diagnostic metrics, not rejection criteria.
Admission is shared across output partitions and independent of partition count.

Reserve estimated reader decoder/fetch state and merge workspace before
constructing them, and reconcile reservations as measurable allocations become
known. Keep reservations until their state is actually destroyed. Calibrate
reader-memory estimates with the direct-reader experiments and report their
coverage and estimation error separately from exact Arrow buffer accounting.

A streaming merge may need all of its participating readers to progress.
Evaluate its complete required working set instead of starting readers that
retain memory while waiting for the remaining inputs. Defer preparation work
when other runnable work can release memory; spill eligible unpublished results
before retrying admission. Never wait for memory held by the same blocked merge.

If the merge's irreducible working set cannot fit the query memory budget after
eligible preparation-time spilling and reclamation, fail with the stage,
required bytes, available bytes, and memory limit. A large input count alone
does not fail the query, and a small input count does not guarantee admission.
There are no external regrouping passes. Materialized source reading can release
decoder state before merging, reducing its required working memory.

The prototype starts with one active range preparation. Development tools can
vary scheduling concurrency for experiments, but these settings do not impose
count-based query admission limits; memory determines which tasks can run.

For each accepted partition range, completely merge its contributing sources,
apply range-level deduplication where required, and append its result to the
store. Release source readers at completion. Preserve selector placement,
tombstone behavior, append mode, LastRow, and LastNonNull; do not apply these
operations to incomplete subsets of a range.

Existing grouping combines overlapping source time ranges, allowing final
merge to skip duplicate resolution between complete ranges as v2 does today.
Assert that invariant. Preserve existing equal-sequence winner behavior and
characterize it with fixtures rather than assuming stable source order.

# SST read-policy alternatives

## A. Current selected-series streaming

Keep the current per-assignment source selection and row-group read pattern,
but synthesize keys from mappings and omit decoded tags from data batches.
Preparation runs outside partitions and completely consumes each range before
publishing its result.

This is the initial integration policy. It can still decode the same row group
for different output assignments, but no longer keeps completed range readers
alive for final replay.

## B. Shared read of the selected-series union

Read candidate-pruned row groups for the combined candidate identities across
output partitions. Buffer source results and merge partition ranges outside
partition streams. Output partitions select their series spans from shared
prepared results.

For example, assignments {A, B} and {C, D} read an SST for {A, B, C, D}
together, rather than separately decoding overlapping groups for each
assignment. Selection still excludes unrelated series and preserves precise
filters. "Once" refers to data decoding across assignments; discovery may read
primary-key pages separately.

This decouples row-group decoding from merging, permitting independent bounded
read tasks. Account for source buffers in the same query tracker. If source
materialization needs disk, complete that spill before its consumers read it;
there is no read-and-spill feedback loop during final replay.

Start the union experiment on candidate sets that fit the query budget. For
larger sets, preserve existing assignment chunks and report repeated reads
between chunks. Pageable candidate storage is a follow-up if measurements
justify it, not a prerequisite for the initial data-phase rewrite.

# Per-query memory and spill threshold

Start with one query-local tracker shared by all preparation tasks, output
partitions, tag catalogs, result stores, cursors, and internal queues. Increasing
partition count must not create an independent spill threshold per partition.

Track unique retained Arrow backing allocations, index/catalog/container
capacities, reader decoder/fetch reservations, merge buffers, and IPC/prefetch
workspace. Shared dictionaries and
slices must not be charged repeatedly or released while another internal owner
still retains them. Enforcement is independent of diagnostic instrumentation.

Distinguish the preparation spill threshold from the hard tracked-memory
budget. The threshold starts reclamation; exceeding it alone is not a query
error. The hard budget determines whether required working memory can be
admitted after reclamation.

When tracked query usage reaches the spill threshold during preparation, spill
eligible completed range-result batches, preferably from older unpinned
results. Append/spill incrementally while producing a range rather than requiring
a whole range to fit. Keep serialization workspace available and account for
input/output coexistence.

Before publishing a readiness manifest, finalize the resident/disk placement
of its results. Account for expected final-merge cursor/output workspace and
tag assembly. Further preparation can spill unpublished results, but published
results do not migrate to disk during replay.

Final replay contributes to query accounting but never triggers new spill
writes. Bound prefetch and queues. Fail on an allocation that cannot fit an
applicable limit; do not spill or wait indefinitely to recover. A series that
spans many ranges can still require one input batch per contributing range,
which motivates the batch-layout experiments.

Per-query accounting drives the first spill policy. Existing engine resource
checks remain respected, but a new engine-wide spill coordinator or global
fairness policy is not part of the initial implementation.

The threshold is a tracked-memory policy, not a total RSS limit. Decoder
reservations are estimates where exact allocation tracking is unavailable;
metadata, candidate discovery, allocator behavior, and filesystem page cache
are separately measured. Record temporary overshoot, estimation error, and the
largest indivisible batch rather than asserting that the threshold removes all
peaks.

# Arrow IPC file storage and batch-layout experiment

Use actual Arrow IPC files and their footer-based random access to record
batches. Do not start with concatenated independent IPC streams. See the
[Arrow IPC specification](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format).

A result store records file handle, batch index, and row offset for each series
span. A series spanning several batches maps to several positions. Resident
results expose equivalent positions. Final consumers seek directly to needed
batches rather than decode preceding batches.

Compare these layouts:

| Layout | Expected benefit | Cost to measure |
| --- | --- | --- |
| Batches containing one series | Load a series without unrelated row payload; smaller active merge inputs. | Small batch/footer overhead, more seeks, and write/read throughput. |
| Batches containing multiple series | Larger sequential transfers and less per-batch metadata. | Unrelated rows retained on lookup and larger input-batch memory. |

One series may have several bounded batches. Do not collect an arbitrarily
large series into one batch. For multiple-series batches, record exact series
spans within each batch; slicing does not free unrelated backing allocation.

IPC files do not allow dictionary replacement for the same dictionary ID.
Choose a consistent storage schema before writing each file. For the first
microbenchmark, store compact keys as plain Binary values and materialize any
dictionary-encoded field values to their corresponding logical value arrays;
reconstruct the merge representation on replay. This avoids growing
per-file dictionaries and keeps unrelated series dictionaries out of a direct
series read. Compare fixed compact-key dictionaries as a separate storage
encoding experiment. Preserve logical types, nulls, and schema metadata.

Start uncompressed, then compare LZ4 and Zstd. Measure dictionary normalization
workspace, footer/index size, random lookup, complete replay, file count, and
serialized bytes, not just sequential bandwidth.

Files must be finished before their result handles are published for random
access. During preparation, write incrementally and finish bounded files as
needed; a result can reference multiple IPC files. Use shared blocking workers
for synchronous IPC/filesystem work and avoid serializing an entire range in
one staging allocation.

Use a dedicated query scratch namespace outside SST paths, with disk quota
and ownership-based cleanup. Mandatory spill failures fail the query. On
cancellation/error stop preparation, close cursors, and release owned files.
Disk usage remains charged until deletion. Startup cleanup targets only
abandoned scan scratch artifacts.

# Final merge and output in scan partitions

Each scan partition receives complete readiness manifests for its assigned
series. It opens resident/IPC cursors, performs the final merge using compact
identities, attaches decoded tags, and yields output batches.

Start with indexed per-series replay so only batches covering the current
identity participate. The one-series IPC layout avoids unrelated row payload;
the multiple-series layout may still retain it and must be measured.

Keep final cursor prefetch and merge workspace within the query memory budget.
There is no final-merge input-count limit. Fail if required replay memory cannot
fit. No spill writes, external regrouping passes, or compact-to-full-key fallback
occur here. The
first implementation can keep the preparation barrier; partition readiness
allows later scheduling changes without putting preparation into the streams.

# File-backed data-result cache

Add a separate namespace for buffered-mode complete data results. Preserve
fingerprint rules for projection, predicates, sequence range, schema/partition
versions, immutable SST identity, dynamic-filter bypass, and memtable exclusion.
Include representation and batch-layout versions in cache identity.

Cache entries own IPC files, series-span indexes, and the decoded tag information
or independent references needed to assemble output. They cannot depend on a
query-local tag catalog after that query ends. Compact data-only artifacts
cannot by themselves reproduce tag projections.

Cache admission and conversion to cache files occur during preparation, before
publishing replay handles. The final merge does not write cache/spill content.
An optional cache failure skips admission; mandatory range spill failure fails
the query.

Track disk capacity and metadata memory separately. Active cursors pin files;
eviction removes lookup visibility immediately and deletes content after the
last pin drops. Keep pinned bytes charged. Leave current v2, candidate, mapping,
and SeqScan caches unchanged.

# Decisions reserved for measurement

| Question | Initial choice | Experiment |
| --- | --- | --- |
| Preparation concurrency | One active partition-range preparation | Compare 1/2/4 independently of output partition count. |
| Source policy | Current selected-series streaming | Compare shared selected-series reads and source buffering. |
| Spill format | Arrow IPC file | Format is fixed; compare encoding and batch layouts. |
| IPC batch scope | Prototype both layouts | Measure one-series versus multiple-series batches. |
| Compact key storage | Plain Binary in first IPC benchmark | Compare a consistent fixed dictionary encoding. |
| Compression | Uncompressed | Compare LZ4/Zstd CPU, size, and read/write throughput. |
| Final replay spill | Disabled | No spill experiment in final replay. |
| Reader/merge-input counts | Metrics only | Vary source counts and widths; memory determines admission. |
| Required merge memory exceeds budget | Query error after eligible reclamation | Report required/available bytes; do not add external merge passes. |
| Spill accounting | Per query | Global spill coordination is follow-up work. |
| Production defaults | Not selected | Decide from correctness and equal-budget measurements. |

Compact merge identity and deferred tag attachment are core behavior, not
optional optimizations. Full-key data-phase modes and collision-based retries
are removed from this proposal.
