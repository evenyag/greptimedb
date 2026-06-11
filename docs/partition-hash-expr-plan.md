# Support `partition_hash(col)` in Partition Rules

## Context

Today a table can only be partitioned by **raw schema columns**: every partition
rule compares a column directly against a literal (`host >= 'a' AND host < 'm'`).
For high-cardinality keys (host, trace_id, user_id) this forces users to hand-pick
string/integer range boundaries that evenly split the data — tedious and fragile.

Users want to partition by the **hash of a column** so a key space can be split
into even ranges mechanically:

```sql
PARTITION ON COLUMNS (host) (
  partition_hash(host) <  1431655765,
  partition_hash(host) >= 1431655765 AND partition_hash(host) < 2863311530,
  partition_hash(host) >= 2863311530
)
```

The partition machinery assumes "operand = schema column" almost everywhere
(SQL parse/validate, `PartitionExpr` serialization, ingest routing, validation,
pruning, distribution metadata). This plan introduces a restricted, built-in
hash operand and threads it through every path.

### Decisions (confirmed with user)
- **Reserved function name:** `partition_hash(col)` (single arg, a partition column).
- **Hash domain:** bounded **`UInt32` `[0, 2^32)`**. Users write range bounds against this.
- **Syntax form:** the source column stays in `PARTITION ON COLUMNS (host)`;
  `partition_hash(host)` only appears **inside** the rule exprs. **No** table-metadata
  schema change (`partition_key_indices` keeps pointing at real columns).
- **Pruning scope:** equality and positive `IN` predicates on the source column
  translate to hash-equality constraints and prune precisely. All other predicates
  on a hashed column (range, `LIKE`, `IS NULL`, `!=`) cannot map through the hash
  and **fall back to scanning all regions** (correct, just unpruned).

### Key invariant
The hash must be **deterministic, fixed-seed, version-pinned, and identical** in
(a) the Rust row evaluator, (b) the DataFusion physical-expr (batch) evaluator, and
(c) the planner that hashes query literals. A mismatch in any path silently
misroutes or mis-prunes rows. The workspace `ahash` uses `compile-time-rng` and is
**unusable** here — it changes per build.

---

## Design summary

### New operand
Extend `Operand` in `src/partition/src/expr.rs`:

```rust
pub enum Operand {
    Column(String),
    Value(Value),
    Expr(PartitionExpr),
    Hash(String),  // partition_hash(<column>)  -> UInt32 virtual key
}
```

A hash rule is represented as `PartitionExpr { lhs: Hash("host"), op: Lt, rhs: Value(UInt32(..)) }`.
`Operand::Hash` is a *dedicated* variant (not a generic function call) to keep
validation/collider/pruning tractable; the serde tag is forward-compatible if we
later add more functions.

### Virtual column key
Throughout collider/checker/pruner, a hash operand is keyed by the **distinct string
`partition_hash(host)`** (helper `Operand::Hash` → `hash_column_key()`), so a raw
`host` predicate (string domain) and a `partition_hash(host)` predicate (u32 domain)
never collide in normalization. The real column `host` is recorded separately for
column-collection purposes (`collect_column_names` still returns `host`, so routing
loads the right Arrow column).

### Stable hash module — `src/partition/src/hash.rs` (new)
- `pub fn partition_hash_value(v: &Value) -> Option<u32>` — canonical-bytes hash of a
  single `datatypes::value::Value`. `None` for `Value::Null`.
- Define a **canonical byte encoding** per value type (fixed: type-tag byte +
  little-endian payload / UTF-8 bytes) so the Rust path and array path agree byte-for-byte.
- Algorithm: a portable, spec-stable hash with a **fixed seed**, truncated to `u32`.
  Recommend `xxhash-rust` (xxh3/xxh64, low 32 bits, seed 0) — const-friendly, portable,
  stable spec; pin the version. (Alternative already in tree: `crc32fast`, a fixed
  standard giving u32 directly — acceptable but weaker avalanche.)
- `pub fn partition_hash_udf() -> Arc<ScalarUDF>` — UDF named `partition_hash`,
  signature `(Any) -> UInt32`, implemented by converting the input array to a
  `Vector` and calling `partition_hash_value` per element (correctness over speed;
  guarantees identical results to the row path). NULL in → NULL out.

### Null semantics
`partition_hash(NULL)` yields NULL ⇒ all comparisons are false ⇒ the row routes to
`DEFAULT_REGION` (consistent with existing null handling, where unmatched rows fall
through). Document this.

---

## PR breakdown (merge order)

### PR 1 — Core representation + stable hash + UDF
Files: `src/partition/src/expr.rs`, new `src/partition/src/hash.rs`, `src/partition/src/lib.rs`, `src/partition/Cargo.toml`, `src/partition/src/error.rs`.

- Add `Operand::Hash(String)` + builder helper (e.g. `pub fn hash_col(name) -> Operand`).
- `hash.rs`: canonical encoding, `partition_hash_value`, `partition_hash_udf`, fixed seed const. Add hash crate dep.
- Update in `expr.rs`:
  - `Display for Operand` → `partition_hash(host)`.
  - `to_parser_expr` → emit `ParserExpr::Function` named `partition_hash` with the column ident (round-trips SHOW CREATE).
  - `Operand::try_as_logical_expr` → `Expr::ScalarFunction(partition_hash_udf(), [col(host)])` (UDF embedded directly, so no registry needed by `try_as_physical_expr`).
  - `canonicalize` / `canonicalize_operand` → treat `Hash` like `Column` for the value-swap rule (`Value op Hash` → `Hash op Value`).
  - `collect_operand_columns` → `Hash(c)` inserts the **real** column `c`.
- Serde derive is automatic; add a serialization round-trip test (`{"Hash":"host"}`).
- Unit tests: display, to_parser round-trip, logical-expr shape, hash determinism, NULL.

### PR 2 — Validation (collider + checker)
Files: `src/partition/src/collider.rs`, `src/partition/src/checker.rs` (verify only), `src/partition/src/overlap.rs` (verify only).

- `collider.rs`:
  - `collect_column_values_from_expr` → add arms for `(Hash, Value)` / `(Value, Hash)`, keying by `hash_column_key("host")`.
  - `try_create_nucleon` → same arms; `NucleonExpr.column` holds the virtual key.
  - `NucleonExpr::to_physical_expr` keeps using `col(self.column, schema)` — the
    checker builds its synthetic test schema from `normalized_values` keys, so the
    virtual key is present and consistent (no real hashing in the checker; it
    validates coverage/overlap purely in normalized u32 space).
- Confirm `PartitionChecker` matrix testing now validates that the declared hash
  ranges are exhaustive + non-overlapping over the u32 line. The source column
  `host` never appears as a checker column — only `partition_hash(host)` does.
- Tests: coverage gap detection, overlap detection, mixed (one hashed key + one raw key) rule.

### PR 3 — Ingest routing
Files: `src/partition/src/multi_dim.rs`.

- `evaluate_expr` → add arms:
  - `(Hash(name), Value(r))`: `partition_hash_value(&values[idx]).map(|h| perform_op(UInt32(h), op, r)).unwrap_or(false)`.
  - `(Value(l), Hash(name))`: symmetric.
- Row path (`find_region` → `evaluate_expr`) and batch path (`split_record_batch` →
  `try_as_physical_expr`, which now embeds the UDF) both work. `record_batch_to_cols`
  already loads the real partition columns (via `collect_column_names` returning `host`).
- Tests: route rows with a known hash table; assert row path and batch path agree.

### PR 4 — SQL parsing + DDL conversion + SHOW CREATE
Files: `src/sql/src/parsers/create_parser.rs`, `src/operator/src/statement/ddl.rs`.
(`src/operator/src/statement/show.rs` works unchanged via `to_parser_expr`.)

- `create_parser.rs::ensure_one_expr` → accept `Expr::Function` when the name is
  `partition_hash` and the single arg is an identifier that is one of the declared
  partition columns; reject any other function/shape with a clear error.
- `ddl.rs::convert_one_expr` → add operand arms:
  - `(Function(partition_hash, [ident]), Value)` → `(Operand::Hash(col), op, Operand::Value)`.
  - `(Value, Function(...))` → symmetric.
  - `(Function(...), UnaryOp{Value})` for negative literals.
  Validate the function name + arg-is-partition-column; reuse `convert_identifier`
  to resolve the column type (so `convert_value` parses the RHS as `UInt32`).
- After this PR the feature is **end-to-end for writes**: create a hash-partitioned
  table, insert, rows route correctly; `SHOW CREATE TABLE` reproduces `partition_hash(host)`.
  Validation (PR2) runs at create time. Queries are correct but scan all regions.
- Tests: parser accept/reject cases; ddl conversion; a sqlness create+show round-trip.

### PR 5 — Query pruning + pushdown
Files: `src/query/src/dist_plan/predicate_extractor.rs`, `src/query/src/dist_plan/region_pruner.rs` (verify), `src/query/src/dist_plan/merge_scan.rs`, `src/query/src/dist_plan/planner.rs`.

- `planner.rs`: derive the set of hashed source columns by scanning the loaded
  partition exprs for `Operand::Hash`; pass a `hashed_columns` set into
  `PredicateExtractor::extract_partition_expressions`.
- `predicate_extractor.rs` (`DataFusionExprConverter::convert` + helpers): when a
  filter touches a hashed source column,
  - `host = lit` → `Hash(host) = Value(partition_hash_value(lit))` (hash the literal **now**, in Rust — same module as ingest).
  - `host IN (a,b,..)` → OR of `Hash(host) = hash(x)`.
  - any other op on `host` → **drop** the constraint (omit ⇒ no pruning ⇒ all regions).
  Non-hashed columns are unchanged.
- `region_pruner.rs`: works unchanged — both query and partition exprs now reference
  the same `partition_hash(host)` virtual key, so the collider/overlap path lines up.
- `merge_scan.rs`: when building `Partitioning::Hash`, for a hashed partition column
  wrap the column physical expr with the `partition_hash` UDF so the declared
  distribution matches the storage layout. (Lower priority within this PR; correctness
  of results does not depend on it, only redundant-repartition avoidance.)
- Tests: sqlness — `WHERE host='a'` prunes to one region; `host IN (...)` prunes to a
  subset; `host LIKE 'a%'` / range scans all regions and still returns correct rows.

---

## Critical files (reference)
- Representation: `src/partition/src/expr.rs` (`Operand`, `PartitionExpr::{to_parser_expr, try_as_logical_expr, canonicalize, collect_operand_columns}`).
- Hash: new `src/partition/src/hash.rs`.
- Validation: `src/partition/src/collider.rs` (`collect_column_values_from_expr`, `try_create_nucleon`, `NucleonExpr`), `src/partition/src/checker.rs`.
- Ingest: `src/partition/src/multi_dim.rs` (`evaluate_expr`, `find_region`, `split_record_batch`).
- SQL/DDL: `src/sql/src/parsers/create_parser.rs` (`ensure_one_expr`), `src/operator/src/statement/ddl.rs` (`convert_one_expr`).
- Query: `src/query/src/dist_plan/{predicate_extractor.rs, region_pruner.rs, merge_scan.rs, planner.rs}`.

## Reuse notes
- `PartitionExpr` serde, `PartitionBound::Expr` wrapping, and `manager.rs` metadata
  flow need **no change** — the new operand rides through automatically once serde
  derives cover it.
- `convert_identifier` / `convert_value` in `ddl.rs` are reused to type the RHS.
- Pruning normalization (`Collider`, `overlap::atomic_exprs_overlap`) is reused as-is
  once both sides speak the virtual hash key.

---

## Verification

Per PR:
- **PR1–PR3:** `cargo test -p partition`. Add a focused test asserting
  `partition_hash_value(&Value::String("a".into()))` is stable across runs and that
  the row evaluator and the UDF batch evaluator produce identical region assignments.
- **PR4 onward:** sqlness (`cargo sqlness bare`). New cases under `tests/cases/`:
  - create a `PARTITION ON COLUMNS (host)` table with three `partition_hash(host)` ranges,
  - `SHOW CREATE TABLE` reproduces `partition_hash(host)`,
  - insert rows spanning many hosts and confirm they spread across regions,
  - `SELECT ... WHERE host='x'` returns correct rows.
- **PR5:** sqlness with `EXPLAIN`/region-count assertions (or a unit test on
  `extract_partition_expressions` + `prune_regions`) showing `host='a'` and
  `host IN (...)` prune to a subset while range/`LIKE` scan all regions, all
  returning correct results.

End-to-end manual check after PR4: build, start a standalone datanode, create the
hash table, insert, query, `SHOW CREATE TABLE`. After PR5: compare scanned-region
counts for equality vs range predicates.

## Risks / watch-outs
- **Hash stability:** pin the hash crate version; the canonical value encoding is a
  wire/format contract — changing it later silently reshuffles existing tables.
  Add a unit test with hardcoded expected hashes as a regression guard.
- **Type of RHS:** parser must coerce the bound literal to `UInt32`; reject out-of-range
  or non-integer bounds at DDL time with a clear message.
- **UDF performance** on the ingest batch path (per-element Value conversion) — fine
  for v1; optimize with a typed array fast-path later if needed.
- **NULL routing** to `DEFAULT_REGION` must be covered by a test so it can't regress.
