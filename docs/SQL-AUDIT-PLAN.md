# LaminarDB SQL Implementation Audit & Fix Plan

> Generated: 2026-02-20
> Branch: `feature/sql_fixes`
> Status: Audit complete, fixes pending

---

## Summary Table

| # | Area | Status | Critical Gaps | Priority |
|---|------|--------|---------------|----------|
| 1 | Watermark Semantics | Mostly Correct | No processing-time watermark; no UNION operator; `on_periodic()` not called from reactor | P1 |
| 2 | Window Functions | Mostly Correct | No CUMULATE windows; no offsets/timezone alignment; SESSION column name mismatch | P1 |
| 3 | Emit & Materialized Views | Gap | MV not wired to streaming pipeline; changelog not propagated between MV levels | P0 |
| 4 | Stream Joins | Gap | Semi/Anti joins silently wrong; temporal join not SQL-wired; no window join | P0 |
| 5 | Time-Series & Advanced SQL | Gap | Top-K/dedup parsed but not executed; MATCH_RECOGNIZE missing; no public UDF API | P1 |
| 6 | DDL & Source/Sink | Gap | No ALTER; no SET; DESCRIBE only works for sources; SHOW output is thin | P1 |
| 7 | Error Handling | **Broken** | Raw DataFusion errors leak via `DbError::Pipeline`; `suggest_column()` dead code; LDB-2xxx/3xxx empty | **P0** |

---

## P0 — Broken / Wrong Behavior (Fix Immediately)

### P0-1: Semi/Anti Joins Produce Wrong Results

**Problem:** `LeftSemi` and `LeftAnti` are silently collapsed to `JoinType::Left` (outer join). A semi-join returns all left rows + nulls instead of only matched rows. An anti-join returns all left rows instead of only unmatched rows.

**File:** `crates/laminar-sql/src/parser/join_parser.rs:280-288`

**Current code:**
```rust
JoinOperator::LeftSemi(_)
| JoinOperator::LeftAnti(_)
| JoinOperator::Semi(_) => JoinType::Left,

JoinOperator::RightSemi(_)
| JoinOperator::RightAnti(_)
| JoinOperator::Anti(_) => JoinType::Right,
```

**Fix:**
1. Add `Semi` and `Anti` variants to `JoinType` enum in `stream_join.rs`:
```rust
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    LeftSemi,
    LeftAnti,
    RightSemi,
    RightAnti,
}
```
2. Update `join_parser.rs` to map correctly:
```rust
JoinOperator::LeftSemi(_) | JoinOperator::Semi(_) => JoinType::LeftSemi,
JoinOperator::LeftAnti(_) | JoinOperator::Anti(_) => JoinType::LeftAnti,
JoinOperator::RightSemi(_) => JoinType::RightSemi,
JoinOperator::RightAnti(_) => JoinType::RightAnti,
```
3. Update `StreamJoinOperator::process_event()` to:
   - **Semi:** emit left row on FIRST match only (track `matched` flag per key), don't emit right columns
   - **Anti:** emit left row only on timer expiry (watermark passes time_bound) if no match was found
4. Add `emits_unmatched_left()` / `emits_unmatched_right()` logic for semi/anti.
5. Add tests: `test_left_semi_join_only_emits_matched`, `test_left_anti_join_only_emits_unmatched`.

**Files to modify:**
- `crates/laminar-core/src/operator/stream_join.rs` (JoinType enum, process_event, on_timer)
- `crates/laminar-sql/src/parser/join_parser.rs` (mapping)

---

### P0-2: NTH_VALUE Silently Produces Wrong Results

**Problem:** `AnalyticFunctionType::NthValue` is parsed but the `LagLeadOperator` only branches on `is_lag: bool`. NTH_VALUE is silently treated as LAG with offset=1.

**File:** `crates/laminar-core/src/operator/lag_lead.rs`

**Fix:**
1. Add `function_type: AnalyticFunctionType` field to `LagLeadFunctionSpec` (replacing the `is_lag: bool`):
```rust
pub enum AnalyticFunctionType {
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue(usize),
}
```
2. In the operator's `process()`:
   - `Lag` / `Lead` — existing buffer logic (unchanged)
   - `FirstValue` — return the first value seen in the partition (store once, emit always)
   - `LastValue` — return current event's value (trivial for append-only streams)
   - `NthValue(n)` — buffer up to N events per partition, emit the Nth

**Files to modify:**
- `crates/laminar-core/src/operator/lag_lead.rs`
- `crates/laminar-sql/src/translator/analytic_translator.rs` (map parsed type to operator spec)

---

### P0-3: SESSION Window Column Name Mismatch

**Problem:** `SessionWindowOperator` output schema uses `session_start` / `session_end`, but the SQL `WindowRewriter` injects `window_start` / `window_end` references. Queries fail with column-not-found.

**File:** `crates/laminar-core/src/operator/session_window.rs:256-257`

**Fix:** Rename the output schema fields from `session_start`/`session_end` to `window_start`/`window_end` for consistency with TUMBLE and HOP:
```rust
// session_window.rs output schema construction
Field::new("window_start", DataType::Int64, false),
Field::new("window_end", DataType::Int64, false),
```
Update all internal references and tests that use `session_start`/`session_end`.

**Files to modify:**
- `crates/laminar-core/src/operator/session_window.rs` (schema + all references)
- Any tests referencing `session_start`/`session_end` columns

---

### P0-4: Raw DataFusion Errors Leak to Users (22+ Locations)

**Problem:** `DbError::Pipeline(format!("...{e}..."))` embeds raw `DataFusionError` text. Users see: `Pipeline error: Stream 'my_stream' planning failed: DataFusion error: Plan("No field named 'foo'")`.

**Affected files and lines:**

#### `crates/laminar-db/src/stream_executor.rs`
| Lines | Context |
|-------|---------|
| 365-366 | `ctx.sql()` planning |
| 368-370 | `.collect()` execution |
| 413-415 | `MemTable::try_new` |
| 421-424 | `register_table` |
| 435-437 | `MemTable::try_new` |
| 440-443 | `register_table` |
| 640-642 | EOWC `MemTable::try_new` |
| 645-648 | EOWC `register_table` |
| 664-666 | EOWC `ctx.sql()` planning |
| 667-669 | EOWC `.collect()` execution |
| 735-737 | Lookup `ctx.sql()` |
| 738-741 | Lookup `.collect()` |
| 768-770 | Background `ctx.sql()` |
| 772-773 | Background `.collect()` |

#### `crates/laminar-db/src/db.rs`
| Lines | Context |
|-------|---------|
| 1357-1359 | ASOF left `ctx.sql()` |
| 1364-1367 | ASOF left `.collect()` |
| 1375-1379 | ASOF right `ctx.sql()` |
| 1383-1387 | ASOF right `.collect()` |
| 1407-1409 | `MemTable::try_new` |
| 1414 | `register_table` |
| 1419-1421 | `ctx.sql()` |
| 1423-1425 | `execute_stream()` |

#### `crates/laminar-db/src/asof_batch.rs`
| Lines | Context |
|-------|---------|
| 111, 124 | Arrow `take` errors |
| 335, 354, 378 | Arrow compute errors |

**Fix strategy — two options:**

**Option A (minimal):** Replace `format!("...{e}")` with `translate_datafusion_error` inline:
```rust
// Before:
.map_err(|e| DbError::Pipeline(format!("Stream '{}' planning failed: {e}", query_name)))?;

// After:
.map_err(|e| {
    let translated = translate_datafusion_error(&e.to_string());
    DbError::Pipeline(format!(
        "Stream '{}' planning failed: [{}] {}",
        query_name, translated.code, translated.message
    ))
})?;
```

**Option B (better):** Change these to `DbError::DataFusion(e)` where `e` is a `DataFusionError`, letting the existing `Display` impl handle translation:
```rust
// Before:
.map_err(|e| DbError::Pipeline(format!("Stream '{}' planning failed: {e}", query_name)))?;

// After:
.map_err(DbError::DataFusion)?;
```
Then add context via a new `DbError` variant:
```rust
QueryPlanningFailed { query_name: String, source: DataFusionError },
```
And implement Display with translation.

**Also fix:** `DbError::Pipeline` → `ApiError::internal(900)` catch-all should route planning failures to `ApiError::query(400)`:
```rust
// api/error.rs: Add explicit match arm before the catch-all
DbError::Pipeline(msg) if msg.contains("planning failed") => {
    Self::query(msg)
}
```

**Files to modify:**
- `crates/laminar-db/src/stream_executor.rs` (14 locations)
- `crates/laminar-db/src/db.rs` (8 locations)
- `crates/laminar-db/src/asof_batch.rs` (5 locations)
- `crates/laminar-db/src/error.rs` (new variant or Display change)
- `crates/laminar-db/src/api/error.rs` (Pipeline → query routing)

---

### P0-5: Wire `suggest_column()` Into Error Translation

**Problem:** `suggest_column()` in `laminar-sql/src/error/mod.rs:65` is fully implemented with Levenshtein distance but has ZERO production call sites. Column-not-found errors (`LDB-1100`) always have `hint: None`.

**Fix:**
1. Add an `available_columns: &[&str]` parameter to `translate_datafusion_error()`:
```rust
pub fn translate_datafusion_error(
    msg: &str,
    available_columns: Option<&[&str]>,
) -> TranslatedError {
    // ... existing logic ...

    // For LDB-1100 (column not found), extract the column name and suggest:
    if code == codes::COLUMN_NOT_FOUND {
        if let Some(cols) = available_columns {
            if let Some(missing_col) = extract_column_name(&sanitized) {
                hint = suggest_column(&missing_col, cols);
            }
        }
    }
}
```
2. At call sites in `db.rs` and `stream_executor.rs`, pass the current schema's column names:
```rust
let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
let translated = translate_datafusion_error(&e.to_string(), Some(&cols));
```
3. Users will then see: `[LDB-1100] Column 'prce' not found. Did you mean 'price'?`

**Files to modify:**
- `crates/laminar-sql/src/error/mod.rs` (add parameter, wire suggest_column)
- `crates/laminar-db/src/db.rs` (pass schema at call sites)
- `crates/laminar-db/src/stream_executor.rs` (pass schema at call sites)

---

## P1 — Missing Industry-Standard Features

### P1-1: MV Not Wired to Streaming Pipeline

**Problem:** `handle_create_materialized_view()` registers the MV in `MvRegistry` and resolves schema but does NOT call `executor.add_query()`. The MV will not receive streaming updates.

**File:** `crates/laminar-db/src/db.rs:2977-3057`

**Fix:** After registry registration, wire the MV's backing query into the `StreamExecutor`:
```rust
// After MvRegistry registration:
self.stream_executor.register_mv_query(
    &mv_name,
    &query_sql,
    &source_refs,
    emit_clause,
)?;
```
This requires adding a `register_mv_query()` method to `StreamExecutor` that:
1. Plans the query via DataFusion
2. Creates a streaming operator chain
3. Connects source channels to the MV's input
4. Registers the MV's output as a queryable table

**Files to modify:**
- `crates/laminar-db/src/db.rs` (wire MV to executor)
- `crates/laminar-db/src/stream_executor.rs` (add `register_mv_query`)

---

### P1-2: Changelog Not Propagated Between MV Levels

**Problem:** In `MvPipelineExecutor.process_mv_inputs()`, `Output::Changelog` goes to the final output, not to the next MV level's input queue.

**File:** `crates/laminar-core/src/mv/executor.rs:287-296`

**Current code:**
```rust
for output in op_outputs {
    match output {
        Output::Event(event) => { /* queue for downstream */ }
        other => outputs.push(other),  // Changelog goes to final sink
    }
}
```

**Fix:**
```rust
for output in op_outputs {
    match output {
        Output::Event(event) => {
            // Queue for downstream MVs
            // ...existing code...
        }
        Output::Changelog(changelog) => {
            // Convert changelog to events for downstream MVs
            let events = changelog.to_events();
            for event in events {
                for downstream in self.registry.dependents(&mv_name) {
                    self.queues.entry(downstream).or_default().push_back(event.clone());
                }
            }
            // Also send to final output for sinks
            outputs.push(Output::Changelog(changelog));
        }
        other => outputs.push(other),
    }
}
```

**Files to modify:**
- `crates/laminar-core/src/mv/executor.rs`

---

### P1-3: Temporal Join Not SQL-Wired

**Problem:** `TemporalJoinOperator` in `laminar-core` is fully implemented and tested but has zero references in `laminar-sql` or `laminar-db`.

**Fix:**
1. Add `FOR SYSTEM_TIME AS OF` parsing to `join_parser.rs`:
```rust
// Detect temporal join syntax:
// JOIN table FOR SYSTEM_TIME AS OF stream.time_col ON ...
JoinOperator::Inner(JoinConstraint::On(expr)) => {
    if has_system_time_as_of(&join_factor) {
        return Ok(JoinAnalysis::temporal(...));
    }
    // ... existing stream-stream join analysis
}
```
2. Add `JoinAnalysis::Temporal` variant with `time_column`, `key_columns`, `semantics`
3. Wire through `join_translator.rs` → `TemporalJoinConfig`
4. In `stream_executor.rs`, detect temporal joins and instantiate `TemporalJoinOperator`

**Files to modify:**
- `crates/laminar-sql/src/parser/join_parser.rs` (parse FOR SYSTEM_TIME AS OF)
- `crates/laminar-sql/src/translator/join_translator.rs` (add TemporalJoinConfig)
- `crates/laminar-sql/src/planner/mod.rs` (plan temporal join)
- `crates/laminar-db/src/stream_executor.rs` (instantiate operator)

---

### P1-4: Top-K / PerGroupTopK Not SQL-Wired

**Problem:** Both operators exist. `order_analyzer.rs` detects `ORDER BY ... LIMIT N` and `ROW_NUMBER() WHERE rn <= N` patterns. `order_translator.rs` builds configs. But no execution path instantiates the operators.

**File:** `crates/laminar-sql/src/translator/order_translator.rs` (configs built but unused)

**Fix:**
1. In `stream_executor.rs`, after query planning, check for `OrderOperatorConfig`:
```rust
if let Some(order_config) = plan.order_config {
    match order_config {
        OrderOperatorConfig::TopK(config) => {
            let op = TopKOperator::new(config);
            pipeline.add_operator(op);
        }
        OrderOperatorConfig::PerGroupTopK(config) => {
            let op = PartitionedTopKOperator::new(config);
            pipeline.add_operator(op);
        }
        // ...
    }
}
```

**Files to modify:**
- `crates/laminar-db/src/stream_executor.rs`
- `crates/laminar-sql/src/planner/mod.rs` (ensure order_config is in QueryPlan)

---

### P1-5: No ALTER DDL

**Problem:** No ALTER SOURCE/SINK/TABLE exists. Schema evolution requires destructive DROP + CREATE.

**Fix (minimal viable):**
1. Add `StreamingStatement::AlterSource { name, alterations }` to `statements.rs`
2. Parse `ALTER SOURCE name ADD COLUMN col TYPE` and `ALTER SOURCE name SET ('key' = 'value')`
3. In `db.rs`, implement by re-registering schema in catalog without dropping the channel

**Files to modify:**
- `crates/laminar-sql/src/parser/statements.rs` (new variant)
- `crates/laminar-sql/src/parser/mod.rs` (parse ALTER)
- `crates/laminar-db/src/db.rs` (execute ALTER)

---

### P1-6: No SET Session Properties

**Problem:** No runtime `SET parallelism = 4` or `SET state_ttl = '1 hour'`.

**Fix:**
1. Add `StreamingStatement::Set { key, value }`
2. Parse `SET key = 'value'`
3. Store in a `SessionConfig` map on `LaminarDB`
4. Read from operators at construction time

**Files to modify:**
- `crates/laminar-sql/src/parser/statements.rs`
- `crates/laminar-sql/src/parser/mod.rs`
- `crates/laminar-db/src/db.rs`

---

### P1-7: No Processing-Time Watermark / PROCTIME()

**Problem:** Only event-time watermark strategies exist. No `PROCTIME()` function or processing-time watermark.

**Fix:**
1. Add `WatermarkStrategy::ProcessingTime` variant to watermark.rs
2. Register `PROCTIME()` as a ScalarUDF returning `TimestampMillisecond(now())`
3. When `WATERMARK FOR col AS PROCTIME()` is specified, use `PeriodicGenerator` with wall-clock time

**Files to modify:**
- `crates/laminar-core/src/time/watermark.rs`
- `crates/laminar-sql/src/datafusion/mod.rs` (register UDF)
- `crates/laminar-sql/src/translator/streaming_ddl.rs` (detect PROCTIME)

---

### P1-8: EmitStrategy Mismatch Between SQL and Core

**Problem:** SQL layer has `EmitStrategy::FinalOnly`, core has `EmitStrategy::Final`. Two independent enums with no mapping.

**Fix:** Add `From<sql::EmitStrategy> for core::EmitStrategy` or unify the enums:
```rust
// In a bridge module:
impl From<laminar_sql::EmitStrategy> for laminar_core::EmitStrategy {
    fn from(s: laminar_sql::EmitStrategy) -> Self {
        match s {
            laminar_sql::EmitStrategy::FinalOnly => laminar_core::EmitStrategy::Final,
            laminar_sql::EmitStrategy::OnUpdate => laminar_core::EmitStrategy::OnUpdate,
            // ...
        }
    }
}
```

**Files to modify:**
- `crates/laminar-sql/src/parser/statements.rs` or new bridge module
- `crates/laminar-db/src/stream_executor.rs` (use the mapping)

---

### P1-9: DESCRIBE Only Works for Sources

**Problem:** `DESCRIBE my_table` returns `SourceNotFound`.

**File:** `crates/laminar-db/src/db.rs:3302`

**Fix:** Check catalog sources, then DataFusion tables, then MVs:
```rust
fn handle_describe(&self, name: &str) -> Result<RecordBatch> {
    // Try source catalog first
    if let Some(schema) = self.catalog.describe_source(name) {
        return Ok(schema_to_batch(schema));
    }
    // Try DataFusion registered tables
    if let Some(table) = self.session_ctx.table_provider(name).await? {
        return Ok(schema_to_batch(table.schema()));
    }
    // Try MVs
    if let Some(mv) = self.mv_registry.get(name) {
        return Ok(schema_to_batch(mv.schema()));
    }
    Err(DbError::TableNotFound(name.to_string()))
}
```

**Files to modify:**
- `crates/laminar-db/src/db.rs` (handle_describe)

---

### P1-10: DROP SOURCE CASCADE Not Supported

**Problem:** Dropping a source leaves dependent MVs/streams broken silently.

**Fix:**
1. Add `cascade: bool` field to `StreamingStatement::DropSource`
2. Track source→stream/MV dependencies (already partially in MvRegistry)
3. On CASCADE, drop dependent streams and MVs before dropping source
4. Without CASCADE, return error if dependents exist

**Files to modify:**
- `crates/laminar-sql/src/parser/statements.rs`
- `crates/laminar-sql/src/parser/mod.rs`
- `crates/laminar-db/src/db.rs`

---

### P1-11: No CUMULATE Windows

**Problem:** Flink-style cumulating windows entirely absent.

**Fix:** Create `CumulatingWindowAssigner` and `CumulatingWindowOperator`:
```rust
pub struct CumulatingWindowAssigner {
    step_ms: i64,    // Emit interval
    max_size_ms: i64, // Maximum window size
}

impl CumulatingWindowAssigner {
    fn assign(&self, ts: i64) -> Vec<WindowId> {
        // Window resets at max_size boundaries
        // Within a max_size period, windows grow by step_ms
        let epoch_start = (ts / self.max_size_ms) * self.max_size_ms;
        let steps = ((ts - epoch_start) / self.step_ms) + 1;
        (0..steps).map(|i| WindowId {
            start: epoch_start,
            end: epoch_start + (i + 1) * self.step_ms,
        }).collect()
    }
}
```
Add SQL syntax: `CUMULATE(col, INTERVAL step, INTERVAL max_size)`

**Files to create/modify:**
- `crates/laminar-core/src/operator/cumulating_window.rs` (new)
- `crates/laminar-sql/src/parser/window_rewriter.rs` (parse CUMULATE)
- `crates/laminar-sql/src/datafusion/window_udf.rs` (CUMULATE UDF)

---

### P1-12: Public UDF/UDAF Registration API

**Problem:** `LaminarDbBuilder` has `register_connector()` but no `register_udf()`.

**Fix:**
```rust
// In builder.rs:
pub fn register_udf(mut self, udf: ScalarUDF) -> Self {
    self.custom_udfs.push(udf);
    self
}

pub fn register_udaf(mut self, udaf: AggregateUDF) -> Self {
    self.custom_udafs.push(udaf);
    self
}
```
Apply in `build()` by calling `ctx.register_udf()` / `ctx.register_udaf()` for each.

**Files to modify:**
- `crates/laminar-db/src/builder.rs`

---

### P1-13: Source Discovery Uses Fragile Substring Matching

**Problem:** `query_sql.contains(s.as_str())` for MV source discovery.

**File:** `crates/laminar-db/src/db.rs:3023-3025`

**Fix:** Use DataFusion's logical plan to extract table references:
```rust
let plan = ctx.sql(&query_sql).await?.into_optimized_plan()?;
let table_refs: Vec<String> = plan
    .table_references()
    .map(|r| r.to_string())
    .collect();
```

**Files to modify:**
- `crates/laminar-db/src/db.rs`

---

## P1-14: Populate LDB-2xxx and LDB-3xxx Error Codes

**Problem:** Module doc promises window (2xxx) and join (3xxx) error codes but only generic codes exist.

**File:** `crates/laminar-sql/src/error/codes.rs`

**Fix:** Add constants:
```rust
// Window/Watermark errors (2000-2099)
pub const WATERMARK_REQUIRED: &str = "LDB-2001";
pub const WINDOW_TYPE_INVALID: &str = "LDB-2002";
pub const WINDOW_SIZE_INVALID: &str = "LDB-2003";
pub const LATE_DATA_REJECTED: &str = "LDB-2004";

// Join errors (3000-3099)
pub const JOIN_KEY_MISSING: &str = "LDB-3001";
pub const JOIN_TIME_BOUND_MISSING: &str = "LDB-3002";
pub const TEMPORAL_JOIN_NO_PK: &str = "LDB-3003";
pub const JOIN_TYPE_UNSUPPORTED: &str = "LDB-3004";
```

Wire into `translate_datafusion_error()` by pattern-matching on window/join-related error messages.

**Files to modify:**
- `crates/laminar-sql/src/error/codes.rs`
- `crates/laminar-sql/src/error/mod.rs` (translation logic)

---

## P2 — Nice-to-Have

### P2-1: Window Offsets / Timezone Alignment

Add optional 3rd argument to TUMBLE/HOP:
```sql
TUMBLE(ts, INTERVAL '1' DAY, INTERVAL '8' HOUR)  -- offset
```
- Modify `TumblingWindowAssigner` to add `offset_ms: i64`
- Modify `window_rewriter.rs` to accept 3 arguments
- Assignment: `floor((ts - offset) / size) * size + offset`

### P2-2: SHOW Output Enrichment

Expand SHOW SOURCES to return: `name, schema, watermark_column, connector_type, format, buffer_size`

### P2-3: EXPLAIN ANALYZE

Add execution statistics collection and `EXPLAIN ANALYZE` DDL variant.

### P2-4: MATCH_RECOGNIZE / CEP

Future feature — requires full custom implementation as DataFusion doesn't support it.

### P2-5: UNNEST Streaming Operator

Create an operator that expands array columns into multiple rows for streaming SQL.

### P2-6: Window Frames (ROWS BETWEEN)

Implement `WindowFrameOperator` to consume the existing `WindowFrameConfig` from the planner.

### P2-7: date_trunc / date_bin Registration

Register DataFusion built-in `date_trunc` and `date_bin` as aliases in LaminarDB's session context.

### P2-8: ASOF Direction::Nearest

Implement `Nearest` direction in `asof_batch.rs` batch execution path.

### P2-9: CREATE SINK FROM (subquery)

Wire inline subquery support in CREATE SINK (currently parsed but rejected).

### P2-10: SHOW CREATE SOURCE/SINK

Store original DDL and return it on `SHOW CREATE SOURCE name`.

---

## Implementation Order (Recommended)

### Phase 1: Error Handling (P0-4, P0-5, P1-14) — 1-2 days
Fix all 22+ raw DataFusion error leaks, wire `suggest_column()`, populate error codes. This is the highest-impact change for user experience.

### Phase 2: Correctness Fixes (P0-1, P0-2, P0-3) — 1-2 days
Fix semi/anti joins, NTH_VALUE, SESSION column mismatch. These produce silently wrong results.

### Phase 3: SQL Wiring (P1-1, P1-2, P1-3, P1-4, P1-8) — 2-3 days
Wire MVs, changelog propagation, temporal joins, Top-K to SQL execution. Large architectural but the operators already exist.

### Phase 4: DDL Completeness (P1-5, P1-6, P1-9, P1-10, P1-13) — 1-2 days
ALTER, SET, DESCRIBE for all objects, DROP CASCADE, fix MV source discovery.

### Phase 5: Feature Gaps (P1-7, P1-11, P1-12) — 2-3 days
PROCTIME(), CUMULATE windows, public UDF API.

### Phase 6: Polish (P2-*) — ongoing
Window offsets, SHOW enrichment, EXPLAIN ANALYZE, etc.

---

## Test Strategy

Each fix should include:
1. **Unit test** in the operator/parser file
2. **Integration test** via `LaminarDB::execute()` with SQL input
3. **Negative test** verifying the correct error message/code for invalid input
4. **Regression test** for the specific bug being fixed (e.g., semi-join returning wrong rows)

Run after each phase:
```bash
cargo test                           # All tests pass
cargo clippy -- -D warnings          # No warnings
cargo test --features kafka,rocksdb  # Feature-gated tests
```
