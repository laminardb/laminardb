# Phase E: Changelog/Retraction for OperatorGraph

## Problem

`IncrementalAggState.emit()` outputs running totals — the full current state of every group, every cycle. A downstream operator or sink receiving `[AAPL, SUM=300]` on cycle N and `[AAPL, SUM=450]` on cycle N+1 has no way to know that the first row was superseded. This breaks:

- **Cascaded aggregation**: `agg1 → agg2` where agg2 must subtract the old agg1 value and add the new one
- **Materialized view maintenance**: sinks that maintain a table must DELETE the old row before INSERTing the new one
- **State TTL**: evicting a group silently drops it from output — the sink never learns the group is gone

Window operators (EOWC) and joins are NOT affected — they emit append-only results by design.

## Scope

Only `IncrementalAggState` (non-windowed GROUP BY) needs retractions. All other operators pass through or produce append-only output.

## Design choices

### 1. Representation: `__op` column vs Z-set weights

| Approach | Pros | Cons |
|----------|------|------|
| `__op: Int8` column (+1=insert, -1=retract) | Flink-compatible, explicit, sinks can filter | Schema pollution, 1 byte per row overhead |
| Z-set `weight: Int32` column (+1, -1, or higher for duplicates) | Materialize/DBSP-compatible, mathematically composable | More complex, sinks must understand weight algebra |
| Side-channel metadata (not in RecordBatch) | No schema change | Can't flow through DataFusion operators, lossy on serialization |

**Recommendation**: `__op: Int8` column. Reasons:
- Flink ecosystem familiarity
- Passes through DataFusion projections/filters unchanged (just another column)
- Sinks can ignore it (append-only mode) or use it (changelog mode)
- Simpler than Z-set weight algebra

### 2. Column position

Append `__op` as the **last column** in the output schema. This avoids breaking existing column-index-based access patterns. Operators that don't produce retractions emit all rows with `__op = 1` (insert).

### 3. Retraction tracking in `IncrementalAggState`

Add `last_emitted: AHashMap<OwnedRow, Vec<ScalarValue>>` — the previous cycle's output per group. On `emit()`:

```
for each group in self.groups:
    current_value = evaluate accumulators
    if group in last_emitted:
        old_value = last_emitted[group]
        if old_value != current_value:
            emit row with __op = -1 (retract old_value)
            emit row with __op = +1 (insert current_value)
            last_emitted[group] = current_value
        else:
            // Value unchanged — emit nothing (or emit with __op = 1 if always-emit mode)
    else:
        emit row with __op = +1 (insert)
        last_emitted[group] = current_value

for each group in last_emitted but NOT in self.groups:
    // Group was evicted (TTL) or dropped
    emit row with __op = -1 (retract)
    remove from last_emitted
```

This makes `emit()` produce a **changelog stream** instead of a **full state dump**.

### 4. Downstream operator behavior

**Stateless operators** (filter, project, compiled): Pass `__op` column through unchanged. A retraction of a row that doesn't match the filter is harmlessly dropped.

**Stateful operators** (cascaded agg): Must consume `__op`:
- `__op = +1`: normal accumulator update
- `__op = -1`: reverse the accumulator (subtract from SUM, decrement COUNT, etc.)
- Requires `Accumulator` to support `retract_batch` (DataFusion supports this)

**This is the hard part.** Cascaded aggregation with retractions requires every downstream aggregate to handle retraction correctly. Not all aggregates support `retract_batch` (e.g., MIN/MAX can't efficiently retract). For those, a full recomputation is needed.

### 5. Sink behavior

Sinks declare `changelog: bool` in their capabilities:
- `changelog = false` (default): Sink receives only `__op = +1` rows. Retractions are filtered out before delivery. Running-state aggregates emit full state (no changelog, current behavior).
- `changelog = true`: Sink receives both `__op = +1` and `__op = -1` rows. Postgres sink translates to DELETE+INSERT. Kafka sink includes `__op` in the record.
- `upsert = true`: Sink receives `__op = +1` only but uses the key columns to overwrite existing rows. No explicit retraction needed — the sink handles it via primary key.

### 6. Emit mode configuration

Per-query configuration via SQL DDL or pipeline config:
- `EMIT CHANGELOG` — emit retraction pairs (requires `last_emitted` tracking)
- `EMIT RUNNING STATE` — emit full group state every cycle (current behavior, no tracking overhead)
- Default: `RUNNING STATE` for backward compatibility

### 7. Checkpoint implications

`last_emitted` must be checkpointed. On recovery, if `last_emitted` is not restored, the first emit after recovery produces `__op = +1` for all groups (effectively a "snapshot" — correct for exactly-once with aligned barriers).

## Implementation order

1. Add `__op` column to `IncrementalAggState` output schema
2. Add `last_emitted` tracking and changelog emit mode
3. Wire `EMIT CHANGELOG` DDL → emit mode config
4. Update sinks to declare and handle `changelog` capability
5. Add `retract_batch` support for cascaded aggregation
6. Wire TTL eviction to produce retractions before group removal
7. Checkpoint `last_emitted` state

## Files to modify

- `crates/laminar-db/src/aggregate_state.rs` — `emit()`, `GroupEntry`, checkpoint/restore
- `crates/laminar-db/src/operator/sql_query.rs` — emit mode config, __op column handling
- `crates/laminar-db/src/operator_graph.rs` — __op column propagation
- `crates/laminar-connectors/src/connector.rs` — SinkConnectorCapabilities wiring
- `crates/laminar-sql/src/parser/statements.rs` — EMIT CHANGELOG DDL
- Sink implementations — changelog column handling

## Open questions (need design review)

1. Should `__op` be visible in `SELECT *` queries or hidden?
2. For sinks with `upsert = true`, should we skip changelog tracking entirely (no `last_emitted` overhead)?
3. How to handle MIN/MAX retraction? Full recomputation from state, or reject cascaded agg with non-retractable functions?
4. Should the first emit after startup always be `__op = +1` (snapshot), or should we track "first emit" separately?
