# A1-emit Stage 2 — changelog propagation to chained readers

Status: **investigated / sized (2026-06-27). Replaces the snapshot-serving sketch** that was
prototyped first and rejected. Decision: **ADR-007 — chained readers consume the retraction
changelog via the retraction-aware path** (owner-confirmed). This doc is the grounded plan.

## Why snapshot-serving was rejected

The first sketch had `route_output` materialize the incremental MV's snapshot from the upsert store
and route *that* to chained readers (parity with a full-emit MV). A diagnostic proved this is wrong:

- **Full-emit baseline is already broken for chained reads.** A full-emit aggregate re-emits its
  whole snapshot *every cycle*; a chained `SUM(total)` over it returned **60** for a true value of
  30 — double-counted by cycle count. So "parity with full-emit" = parity with a bug.
- **Snapshot-serving is correct only for inserts.** Because incremental emit fires only on change,
  a chained agg saw the data once (`SUM` = 30, correct) *for an insert-only run*. But routing the
  **full snapshot on every change** means under UPDATES a chained agg re-adds the whole snapshot
  (double-count) and a chained projection (Append store) keeps stale rows. Silent wrong results.

The only correct model is the ADR's: emit a **retraction changelog** and have downstreams **net the
retractions**. `SELECT * FROM mv` keeps returning the snapshot (Stage 1 upsert store).

## What already exists (diagnostic findings)

Incremental MVs emit the **same `__weight` Z-set changelog as `EMIT CHANGES`** (both via
`emit_changelog_delta`). So the question was: does `EMIT CHANGES` chaining already net correctly?
Tested (`diag_emit_changes_chaining`): **no, it's broken/fragile** —
- **Aggregate consumes weights** (`update_group_accumulators` appends `weight_col_idx` to the
  accumulator input, so `+1/−1` net) — the core mechanism is present and correct.
- **But `source_has_weight` detection races emit.** It reads the column from
  `ctx.table_provider(src).schema()`; for a chained source that schema is the graph-ctx
  `LiveSourceProvider`, whose schema is only set on the source's **first emit** (`route_output` →
  `ensure_live_provider`). A downstream that plans before the source emits sees no `__weight` → the
  roll-up came back **empty**.
- **Projections/filters do not pass `__weight` through.** The compiled-projection / cached-plan
  paths select named columns and drop `__weight`; `changelog_filter.rs` only handles CDC `_op`
  source columns, not the Z-set weight. So a projection over a changelog loses retractions (stale
  `(1,10)` survived).

So Stage 2 is "make changelog chaining actually work," reused by both `EMIT CHANGES` and A1-emit.

## Plan (phased)

### Phase 1 — chained aggregate roll-ups — DONE (2026-06-27)

**Root cause (pinned by instrumentation), revised from the Phase-0 hypothesis:** the chained-agg
roll-up returned empty/0 NOT because of a `source_has_weight` typing race (detection actually
self-heals: a downstream defers until its source is resolvable, and a changelog producer's live
provider always carries `__weight`). The real bug: a changelog aggregate ran its **cached pre-agg
physical plan, which re-scans the upstream's per-cycle live provider** (the compiled, `inputs`-
consuming path was deliberately disabled when `source_has_weight`, see `aggregate_state.rs:794`).
That live-provider scan is swapped/cleared per cycle and **desyncs from the changelog actually
routed to the operator's `input_bufs`** → rows missed/duplicated → weighted `SUM` netted to 0.

**Fix (contained to `aggregate_state.rs` + `ddl.rs`):**
- **Weight-preserving compiled pre-agg.** Build the compiled pre-agg projection for changelog
  sources too (no `WHERE`), appending a `__weight` passthrough column (at `weight_col_idx`). The
  agg then consumes its routed `inputs` via `try_evaluate_compiled` instead of re-scanning the live
  provider → in sync → retractions net. The unoptimized plan's input schema keeps `__weight`, so the
  compiled exprs index the routed batch correctly. (No Phase-0 up-front typing needed.)
- **Default routing.** Dropped the snapshot-serving prototype; `route_output` routes the changelog
  by default, and post-cycle `update_mv_stores` maintains the MV's own upsert snapshot for
  `SELECT *`.
- **Agg-aware guard** (`reject_non_agg_reading_incremental_mv`): a chained **aggregate** (keyed or
  global) over an incremental MV is allowed (nets the changelog); a non-aggregate projection/filter
  MV or stream is rejected (Phase 2), and sinks/`SUBSCRIBE` stay rejected.
- A chained **keyed** agg is itself incremental (its `GROUP BY` → upsert store); a global agg stays
  full-emit (single-row snapshot). Both net correctly.

**Validated:** `chained_aggregate_over_incremental_nets_under_updates` (`SUM`=35 and per-key roll-up
correct after an update), `terminality_guard_allows_chained_agg_rejects_nonagg_and_sink`, plus the
Stage-1 tests; 777 db lib tests green (no regression to the existing changelog/EMIT-CHANGES path);
clippy `--features cluster --tests -D warnings` + fmt clean. This also fixes `EMIT CHANGES`
aggregate chaining (was broken/empty; same machinery).

### Phase 2 — projection / filter pass-through — DONE (2026-06-27)

**Upsert-key decision (owner): Z-set multiset.** A chained projection/filter has no declared key,
so its snapshot store keys on the **full output row** with an integer **multiplicity** (`+weight`).
Correct for every projection including key-dropping ones that produce duplicate rows (a retraction
decrements multiplicity, so a row another upstream key still produces survives); matches SQL
multiset semantics. Simpler than keying on the upstream GROUP BY (no key-column mapping); a plain
set (key-on-all-cols, no count) is **wrong** (a retraction would delete a row another key produces).

**Implemented:**
- **`MvStorageMode::Multiset`** (`mv_store.rs`): `MultisetState` = full-row `RowConverter` +
  `HashMap<OwnedRow, i64>`. `apply(changelog)` strips `__weight`, `counts[row] += weight`, drops at
  0; `to_record_batch` emits each row `count` times (`convert_rows`); checkpoint = materialize→IPC,
  restore = replay counting occurrences. Unit tests incl. the duplicate/key-dropping case.
- **`__weight` pass-through** (`operator/sql_query.rs::try_build_compiled_projection`): when the
  source schema carries `__weight`, append a `__weight` passthrough column to the compiled
  projection (unless already projected). The compiled path consumes the routed `inputs` (no
  live-provider desync), and a `WHERE` filters each changelog row by its own values (retracts carry
  old values, inserts new) so the multiset nets correctly — **filters work for free**.
- **DDL** (`ddl.rs`): `incremental_emit_mode` → `IncEmit::{Upsert(keyed agg) | Multiset
  (projection/filter over a changelog) | None}`; `register_mv_provider` maps it to the store mode.
  Guard broadened (`reject_unsupported_reading_incremental_mv`): a chained **aggregate or simple
  projection/filter** over an incremental MV is allowed; a **complex shape (join)** and
  sinks/`SUBSCRIBE` are rejected (Phase 3 / later).

**Validated:** `chained_projection_over_incremental_is_correct_under_updates` (projection AND a
`WHERE` filter track an update with no stale row), updated guard test (agg + projection allowed,
join + sink rejected); 3 `MultisetState` unit tests; 780 db lib tests green; clippy
`--features cluster --tests -D warnings` + fmt clean.

### Phase 3 — joins / multi-input over changelogs
- Scope TBD: a join with a changelog input must net both sides. Likely its own effort. Currently
  rejected at DDL by the Stage-2 guard.

## Validation
Deterministic `incremental_emit` tests per phase: agg roll-up nets under updates; projection drops
stale rows after retraction; crash+restart. Add equivalent `EMIT CHANGES` chaining tests (same
machinery — currently untested, and the diagnostic shows it's broken).

## Status
Phase 1 done + committed (snapshot-serving prototype fully reverted; `operator_graph.rs` is back to
its Stage-1 state). Remaining: Phase 2 (projection/filter `__weight` pass-through, with the upsert-
key design question) and Phase 3 (joins). Both still gated by the agg-only guard.
