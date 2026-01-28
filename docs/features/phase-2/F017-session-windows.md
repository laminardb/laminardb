# F017: Session Windows

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F017 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F016 |
| **Owner** | TBD |

## Summary

Implement session windows that group events by activity periods separated by gaps. Sessions are dynamic windows that grow with activity and close after inactivity.

## Goals

- Gap-based session detection
- Per-key session tracking
- Session merging on late data (prepared, not fully implemented)
- Configurable gap timeout

## Implementation

**New Module**: `crates/laminar-core/src/operator/session_window.rs`

### Key Components

#### `SessionState`
```rust
pub struct SessionState {
    /// Session start timestamp (inclusive)
    pub start: i64,
    /// Session end timestamp (exclusive, = last event time + gap)
    pub end: i64,
    /// Key bytes for this session
    pub key: Vec<u8>,
}
```

#### `SessionWindowOperator<A: Aggregator>`
```rust
pub struct SessionWindowOperator<A: Aggregator> {
    gap_ms: i64,                              // Gap timeout in milliseconds
    aggregator: A,                            // Aggregation function
    allowed_lateness_ms: i64,                 // Grace period for late data
    active_sessions: FxHashMap<u64, SessionState>,  // Per-key sessions
    pending_timers: FxHashMap<u64, i64>,      // Closure timers
    emit_strategy: EmitStrategy,              // Emission strategy
    late_data_config: LateDataConfig,         // Late data handling
    key_column: Option<usize>,                // Key column for partitioning
    // ...
}
```

### Features

- **Gap-based detection**: Sessions close when no events arrive within the gap period
- **Per-key tracking**: Each unique key maintains independent session state
- **Timer-based closure**: Timers fire to close sessions after gap + allowed lateness
- **Emit strategies**: All F011B strategies supported (OnWatermark, OnUpdate, Changelog, OnWindowClose, Final)
- **Late data handling**: Configurable drop, side output, or silent drop (Final)
- **Checkpoint/restore**: Full state serialization via rkyv

### Usage

```rust
use laminar_core::operator::session_window::SessionWindowOperator;
use laminar_core::operator::window::CountAggregator;
use std::time::Duration;

// Create a session window with 30-second gap
let mut operator = SessionWindowOperator::new(
    Duration::from_secs(30),    // gap timeout
    CountAggregator::new(),
    Duration::from_secs(60),    // allowed lateness
);

// Optional: key by a specific column for per-key sessions
operator.set_key_column(0);

// Optional: set emit strategy
operator.set_emit_strategy(EmitStrategy::OnUpdate);
```

### Session Lifecycle

1. **Start**: First event for a key creates a new session
2. **Extend**: Events within gap period extend the session end time
3. **Close**: Timer fires when gap expires, emitting aggregation results
4. **Cleanup**: Session state is deleted after emission

### State Keys

- `ses:<key_hash>` - Session metadata (start, end, key)
- `sac:<key_hash>` - Accumulator state

## SQL Syntax

```sql
SELECT
    SESSION_START(ts, INTERVAL '30' MINUTE) as session_start,
    SESSION_END(ts, INTERVAL '30' MINUTE) as session_end,
    user_id,
    COUNT(*) as events
FROM clickstream
GROUP BY SESSION(ts, INTERVAL '30' MINUTE), user_id;
```

> **Note**: SQL syntax parsing is not yet implemented (requires F006B). The operator can be used programmatically.

## Tests

23 unit tests covering:
- Session state creation, contains, extend, merge
- Operator creation and configuration
- Single event processing
- Multiple events in same session
- Gap-based new session creation
- Timer-triggered emission
- Per-key tracking
- Late event handling (drop, side output, Final)
- Emit strategies (OnUpdate, Changelog)
- Checkpoint/restore
- Stale timer handling
- Sum aggregation

## Completion Checklist

- [x] Gap detection working
- [x] Per-key tracking correct
- [x] Timer-based closure
- [x] All emit strategies supported
- [x] Late data handling
- [x] Checkpoint/restore
- [x] 23 unit tests passing
- [ ] Session merging on late data (prepared, not triggered)
- [ ] SQL syntax working (requires F006B)
