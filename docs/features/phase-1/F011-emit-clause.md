# F011: EMIT Clause

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F011 |
| **Status** | ✅ Done |
| **Priority** | P2 |
| **Phase** | 1 |
| **Effort** | S (1-2 days) |
| **Dependencies** | F010 |
| **Owner** | TBD |
| **Completed** | 2026-01-24 |

## Summary

Implement the EMIT clause to control when window results are output. Options include emitting on watermark, periodically, or on every update.

## Goals

- EMIT ON WATERMARK (default, most efficient)
- EMIT EVERY interval (periodic updates)
- EMIT ON UPDATE (every change)

## Implementation

### Core Engine (`laminar-core`)

The `EmitStrategy` enum in `crates/laminar-core/src/operator/window.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum EmitStrategy {
    /// Emit final results when watermark passes window end (default).
    #[default]
    OnWatermark,

    /// Emit intermediate results at fixed intervals.
    Periodic(Duration),

    /// Emit updated results after every state change.
    OnUpdate,
}
```

Window operators support configurable emit strategies:

```rust
let mut operator = TumblingWindowOperator::new(
    assigner,
    aggregator,
    Duration::from_secs(5),
);

// Emit every 10 seconds instead of waiting for watermark
operator.set_emit_strategy(EmitStrategy::Periodic(Duration::from_secs(10)));
```

### SQL Parser (`laminar-sql`)

The `EmitClause` enum in `crates/laminar-sql/src/parser/statements.rs`:

```rust
pub enum EmitClause {
    AfterWatermark,  // EMIT AFTER WATERMARK or EMIT ON WATERMARK
    OnWindowClose,   // EMIT ON WINDOW CLOSE
    Periodically { interval: Expr },  // EMIT EVERY INTERVAL '10' SECOND
    OnUpdate,        // EMIT ON UPDATE
}
```

### SQL Syntax

```sql
-- Default: emit when watermark passes window end
SELECT ... FROM stream
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
EMIT ON WATERMARK;

-- Periodic: emit intermediate results every 10 seconds
SELECT ... FROM stream
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
EMIT EVERY INTERVAL '10' SECOND;

-- On update: emit after every state change (lowest latency, highest overhead)
SELECT ... FROM stream
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
EMIT ON UPDATE;
```

## Key Files

- `crates/laminar-core/src/operator/window.rs` - `EmitStrategy` enum and window operator implementation
- `crates/laminar-sql/src/parser/statements.rs` - `EmitClause` SQL AST
- `crates/laminar-sql/src/parser/parser_simple.rs` - EMIT clause parsing

## Tests

- 9 new tests for `EmitStrategy` in `operator::window::tests`
- 3 new tests for EMIT clause parsing in `parser::parser_simple::tests`

## Completion Checklist

- [x] All emit strategies implemented (OnWatermark, Periodic, OnUpdate)
- [x] SQL parsing working (EMIT ON WATERMARK, EMIT EVERY, EMIT ON UPDATE)
- [x] Unit tests passing (12 new tests)
- [x] Clippy clean
