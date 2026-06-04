# Plan: Changelog Collapse for Upsert Sinks

- **Status:** Phases 0–3 implemented (Delta). Phase 4 (Iceberg MOR reuse) deferred.
- **Date:** 2026-05-26 (impl 2026-05-27)
- **Scope:** Make "aggregating MV → upsert lakehouse table" a correct, supported topology.
- **Primary consumer:** Delta Lake upsert sink. **Second consumer:** Iceberg MOR upsert sink (in flight).

## Implementation notes (2026-05-27)

- **Phase 0** — `laminar-core::changelog` module: `WEIGHT_COLUMN`, `OP_COLUMN`, and
  `op::{INSERT,UPDATE,DELETE,READ,UPDATE_AFTER,UPDATE_BEFORE}`. `aggregate_state.rs`
  re-exports `WEIGHT_COLUMN` from it (`crate::aggregate_state::WEIGHT_COLUMN` still resolves).
- **Phase 1** — `laminar-connectors::changelog::collapse_changelog`, behind a new
  `changelog-collapse` feature (just pulls `arrow-row`; `delta-lake` includes it) so the
  helper builds/tests without the deltalake stack. 16 unit tests.
- **Phase 2** — wired into `flush_staged_to_delta` (concat → collapse → MERGE). Also fixed a
  latent bug: `target_schema` (and `with_schema`) only stripped `_op`/`_ts_ms` from the
  created table, leaking `__weight` as a phantom column; the strip list is now
  `CHANGELOG_METADATA_COLUMNS = [_op, _ts_ms, __weight]`, matching what collapse strips from
  the source. End-to-end integration test (`upsert_collapses_aggregating_mv_to_current_state`)
  covers update, group-drop, and multi-update-per-key-in-one-epoch (the cardinality case).
- **Phase 3** — collapse metrics (`delta_sink_collapse_{rows_in,upserts_out,deletes_out}_total`
  + `delta_sink_collapse_duration_seconds`). The retry-bound item was already satisfied: the
  flush loop only retries `is_conflict_error`; non-conflict errors (incl. any cardinality
  violation) already propagate immediately. Collapse eliminates the cardinality poison batch.
- **CDC normalization deviation from the plan:** collapse normalizes the output `_op` to
  `{U, D}` (D for `_op ∈ {D, U-}`, U otherwise) rather than retaining the raw op, so the MERGE
  (which only understands `I/U/r/D`) handles the full CDC vocabulary including `U+`/`U-`.

## Problem

An aggregating materialized view emits a **Z-set changelog** keyed by its output row,
carrying an Int64 `__weight` column (`crates/laminar-db/src/aggregate_state.rs:254`,
emitted at `:1117-1133`): a value change for key `K` arrives as
`{(K, V_old): -1, (K, V_new): +1}`, a new group as `{(K, V_new): +1}`, a dropped
group as `{(K, V_old): -1}`.

For a changelog-capable sink, `prepare_for_sink` passes this batch through unchanged
(`crates/laminar-db/src/changelog_filter.rs:71-72`). But the Delta upsert path
(`merge_changelog`, `crates/laminar-connectors/src/lakehouse/delta_io.rs:951`):

1. References `source._op` and only strips `["_op","_ts_ms"]` (`:965`) — it does **not**
   understand `__weight`. Result: missing-`_op` MERGE failure, or `__weight` leaks as a
   phantom target column.
2. Hands the **whole epoch** to delta-rs MERGE with no per-key dedup
   (`delta.rs:602-610`). Multiple events per key in one epoch (every aggregate update is
   retract+insert) → delta-rs cardinality violation ("multiple source rows matched a
   target row") → the epoch fails and retries forever.

Net: the most natural topology silently breaks or wedges the pipeline.

## Goals

- Aggregating MV → upsert table produces a table holding **current** per-key state.
- One **shared, sink-agnostic** collapse operation; Delta and Iceberg MOR both consume it.
- Cardinality-safe: exactly one row per merge key reaches the underlying writer per commit.
- Idempotent under replay (at-least-once) and exactly-once epoch skipping.
- No change to the existing `merge_changelog` MERGE clauses — collapse produces the
  `_op`-encoded, key-unique batch they already expect.

## Non-goals (explicit anti-over-engineering boundaries)

- **No sequence/LSN column plumbing.** Z-set weight summation is order-independent within
  an epoch and epochs are serialized — ordering metadata is unnecessary for this path.
- **No merge-key-range pruning predicate.** CoW MERGE scaling is addressed by data layout
  (docs), not connector code.
- **No deletion-vectors / merge-on-read engine.** CoW MERGE is correct for bounded
  group-key cardinality; revisit only if a scale requirement appears.
- **No dead-letter queue.** Poison batches fail fast with a clear error (see retry bound).
- **No pluggable dedup-strategy framework / trait hierarchy.** One function, one contract.
- **No out-of-order external-CDC ordering.** Last-arrival-wins only (single-writer causal
  order); explicit LSN ordering is a future item if/when needed.

## Design

### Changelog contract (shared)

Move the changelog encoding constants to `laminar-core` so the producer (laminar-db MV)
and consumers (connectors) agree — this formalizes today's hardcoded, drifting strings:

- `WEIGHT_COLUMN = "__weight"` (Int64; +n insert, -n retract).
- `_op` values: `I`, `U`, `D`, `r`, `U+`, `U-`.

`aggregate_state.rs` switches to the shared `WEIGHT_COLUMN` (drops its `pub(crate)` copy).

### The collapse operation

`collapse_changelog(batch, merge_key: &[String]) -> Result<RecordBatch>` — pure Arrow,
operates on the **already-concatenated epoch batch**. Detects encoding:

**Z-set mode (`__weight` present):**
1. Consolidate: group by full row (all columns except `__weight`), sum `__weight`.
2. Drop net-zero rows.
3. Group survivors by `merge_key`:
   - rows with net weight `> 0` = live values → **assert ≤ 1 per key** (else error: merge
     key is not unique) → emit with `_op = "U"`.
   - key with only net-negative rows → emit one row (carries the key) with `_op = "D"`.
4. Output: key-unique batch, `__weight` stripped, `_op` added.

**CDC mode (`_op` present, no `__weight`):**
1. Per merge key, keep the **last-arriving** row (buffer/row order = arrival order).
2. `_op == "D"` → delete; else upsert (`_op` retained).
3. Output: key-unique `_op` batch.

Both outputs feed the existing `merge_changelog` unchanged (it already handles
`_op ∈ {I,U,r,D}` and is now cardinality-safe).

**Why this is correct without sequencing or float-equality assumptions:** the retract of a
prior-committed value appears as a lone net-negative row (its matching insert was a prior
epoch); per-key live-row selection picks the new value; upsert-by-key subsumes the stale
retract. Intra-epoch oscillation cancels at full-row granularity. Correctness never depends
on a retract's old value matching what's currently in the table.

### Placement

- **Contract constants:** `laminar-core`.
- **Collapse helper:** `crates/laminar-connectors/src/changelog/collapse.rs` (new).
- **Invocation:** sink-side, at commit, on the concatenated epoch batch — Delta upsert
  branch of `flush_staged_to_delta` / `attempt_delta_write`
  (`delta.rs:594` / `:505`), before `merge_changelog`.
- `prepare_for_sink` is **unchanged** (still passes the raw weighted changelog to the sink).
- laminar-db needs no behavior change beyond adopting the shared contract constant.

### Merge key source

The sink's declared `merge.key.columns` (already threaded from SQL `WITH(...)`,
`delta_config.rs:47`). **Not** derived from the query plan — the key is a target-table
property and many changelog MVs have no GROUP BY. Generalize the same field to other
upsert sinks.

### Guardrails

- Runtime: `> 1` live row per merge key after collapse → loud error (`merge.key.columns`
  is not a unique key of the changelog output).
- DDL (where statically knowable): validate `merge.key.columns` is non-empty for upsert
  mode (already enforced, `delta_config.rs:388`); extend with a uniqueness note.

## Component changes

| Area | File | Change |
|---|---|---|
| Contract | `laminar-core` (new `changelog` mod) | `WEIGHT_COLUMN`, `_op` consts |
| Producer | `laminar-db/src/aggregate_state.rs` | use shared const |
| Collapse | `laminar-connectors/src/changelog/collapse.rs` (new) | `collapse_changelog()` + tests |
| Delta wiring | `laminar-connectors/src/lakehouse/delta.rs` (`:505`/`:594`) | collapse concatenated batch before merge |
| Metrics | `laminar-connectors/src/lakehouse/delta_metrics.rs` | rows_in, upserts_out, deletes_out, collapse duration |
| Retry bound | `delta.rs:621` loop | bound **non-conflict** errors (today only conflicts are bounded) → fail fast |
| Iceberg | Iceberg MOR sink (separate effort) | call the same helper |
| Docs | `laminar-connectors/README.md` | upsert requires unique `merge.key.columns`; partition/Z-order by it for MERGE perf |

## Phases

- **Phase 0 — contract:** shared constants in `laminar-core`; `aggregate_state` adopts.
  No behavior change. (Small.)
- **Phase 1 — collapse helper:** `collapse_changelog` + exhaustive unit tests, standalone,
  no wiring. (Core deliverable.)
- **Phase 2 — Delta wiring:** call collapse in the upsert flush; integration test
  (aggregating MV → Delta upsert table, force updates + group disappearance, read back and
  assert current aggregates; assert no cardinality error with multi-update-per-key epochs).
- **Phase 3 — hardening:** bounded non-conflict retries + collapse/merge metrics.
- **Phase 4 — generalization (separate):** Iceberg MOR reuse; docs.

## Testing

- **Unit (collapse):** new-group insert; value update (retract+insert) → single `U`;
  group disappearance → `D`; multi-update-per-key in one epoch → final value only;
  insert+delete within epoch → no-op; non-unique key → error; CDC I/U/D dedup; empty batch.
- **Integration (Phase 2):** end-to-end `COUNT`/`SUM ... GROUP BY` MV into a Delta upsert
  table; mutate source to drive updates and group removal; assert table == expected current
  aggregates. Gated on `delta-lake`; object-store cases `#[ignore]`.
- **Recovery:** re-apply an epoch; assert idempotent table state.

## Risks & mitigations

- Full-row consolidation cost for wide rows / large epochs → bounded by the existing epoch
  buffer cap (`delta.rs:1125`). Acceptable.
- CoW MERGE cost at scale → out of scope; documented data-layout requirement
  (partition/Z-order by merge key).
- Misdeclared (non-unique) merge key → loud runtime error, never silent loss.

## Decisions

1. **Collapse output shape:** a single `_op`-tagged, key-unique batch — reuses
   `merge_changelog` unchanged; the Iceberg MOR sink splits it by `_op` into
   equality-deletes + inserts. (Not a structured `{upserts, delete_keys}` result.)
2. **CDC `_op` path in v1:** in scope. The CDC path also lacks per-key dedup today (the
   same latent cardinality bug), so `collapse_changelog` covers both encodings (`__weight`
   Z-set and `_op` CDC).
3. **Collapse module home:** a new top-level `changelog` module in `laminar-connectors`
   (`src/changelog/collapse.rs`), not under `serde`/`lakehouse`.
