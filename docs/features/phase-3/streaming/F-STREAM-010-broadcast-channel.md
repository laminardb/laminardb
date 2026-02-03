# F-STREAM-010: Broadcast Channel

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-STREAM-010 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-STREAM-001, F-STREAM-005 |
| **Owner** | Claude |
| **Created** | 2026-01-28 |
| **Updated** | 2026-02-03 |

## Summary

Broadcast channel for multi-consumer scenarios. Automatically derived when multiple materialized views read from the same source - users never specify broadcast mode.

**Key Design Principle**: Broadcast is derived from query plan analysis, not user configuration.

## Implementation

### Files Created

| File | Purpose | Tests |
|------|---------|-------|
| `laminar-core/src/streaming/broadcast.rs` | `BroadcastChannel<T>`, `BroadcastConfig`, `SlowSubscriberPolicy` | 31 |
| `laminar-sql/src/planner/channel_derivation.rs` | `derive_channel_types()`, `DerivedChannelType` | 11 |

### Key Types

```rust
// Slow subscriber handling policy
pub enum SlowSubscriberPolicy {
    Block,       // Block producer until subscriber catches up
    DropSlow,    // Drop the slowest subscriber
    SkipForSlow, // Skip messages for slow subscribers
}

// Broadcast channel configuration
pub struct BroadcastConfig {
    pub capacity: usize,
    pub max_subscribers: usize,
    pub slow_subscriber_policy: SlowSubscriberPolicy,
    pub slow_subscriber_timeout: Duration,
    pub lag_warning_threshold: u64,
}

// Derived channel type from query analysis
pub enum DerivedChannelType {
    Spsc,                             // Single consumer
    Broadcast { consumer_count: usize }, // Multiple consumers
}
```

### API

```rust
// Create channel
let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());

// Subscribe (returns subscriber ID)
let id1 = channel.subscribe("mv1")?;
let id2 = channel.subscribe("mv2")?;

// Broadcast (single producer)
channel.broadcast(42)?;

// Read (each subscriber)
assert_eq!(channel.read(id1), Some(42));
assert_eq!(channel.read(id2), Some(42));

// Metrics
assert_eq!(channel.subscriber_count(), 2);
assert_eq!(channel.subscriber_lag(id1), 0);

// Channel derivation from query plan
let types = derive_channel_types(&sources, &mvs);
// trades consumed by 2 MVs → Broadcast
assert_eq!(types["trades"], DerivedChannelType::Broadcast { consumer_count: 2 });
```

## Goals

- Single producer, multiple consumers
- Independent read cursors per subscriber
- Slowest consumer determines retention
- Automatic derivation from topology

## Non-Goals

- User-specified broadcast mode (always automatic)
- Consumer-side filtering (use SQL WHERE)
- Acknowledgment semantics (separate feature)

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path)
**Crate**: `laminar-core`, `laminar-sql`
**Module**: `streaming/broadcast.rs`, `planner/channel_derivation.rs`

```
┌─────────────────────────────────────────────────────────────────┐
│                   Broadcast Channel Derivation                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Query Plan Analysis:                                            │
│                                                                  │
│  CREATE SOURCE trades (...);                                     │
│                                                                  │
│  -- Single consumer → SPSC                                       │
│  CREATE MATERIALIZED VIEW ohlc AS SELECT ... FROM trades;        │
│                                                                  │
│  -- Adding second consumer → Upgrade to Broadcast                │
│  CREATE MATERIALIZED VIEW vwap AS SELECT ... FROM trades;        │
│                                                                  │
│                      trades (Source)                             │
│                           │                                      │
│                           ▼                                      │
│              ┌────────────────────────┐                         │
│              │    Broadcast Channel   │◄── Auto-derived         │
│              │                        │    when 2+ consumers     │
│              │  ┌──────────────────┐  │                         │
│              │  │   Ring Buffer    │  │                         │
│              │  │   (shared data)  │  │                         │
│              │  └──────────────────┘  │                         │
│              │                        │                         │
│              │  Cursor[0]: ohlc MV    │                         │
│              │  Cursor[1]: vwap MV    │                         │
│              └────────────────────────┘                         │
│                     │           │                                │
│                     ▼           ▼                                │
│                ┌────────┐  ┌────────┐                           │
│                │  ohlc  │  │  vwap  │                           │
│                │   MV   │  │   MV   │                           │
│                └────────┘  └────────┘                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `MaxSubscribersReached` | Too many MVs from source | Increase limit or consolidate |
| `SlowSubscriberTimeout` | Subscriber too slow | Check subscriber, increase buffer |
| `NoSubscribers` | All subscribers dropped | Normal shutdown |
| `SubscriberNotFound` | Invalid subscriber ID | Check subscription |
| `Closed` | Channel closed | Stop broadcasting |

## Test Plan

### Unit Tests (31 broadcast + 11 derivation = 42 total)

- [x] `test_default_config`
- [x] `test_config_with_capacity`
- [x] `test_config_effective_capacity_rounds_up`
- [x] `test_config_builder`
- [x] `test_slow_subscriber_policy_default`
- [x] `test_slow_subscriber_policy_variants`
- [x] `test_channel_creation`
- [x] `test_channel_custom_capacity`
- [x] `test_subscribe`
- [x] `test_subscribe_max_limit`
- [x] `test_unsubscribe`
- [x] `test_unsubscribe_nonexistent`
- [x] `test_broadcast_no_subscribers`
- [x] `test_broadcast_and_read_single_subscriber`
- [x] `test_broadcast_and_read_multiple_subscribers`
- [x] `test_read_unsubscribed`
- [x] `test_read_nonexistent_subscriber`
- [x] `test_try_read`
- [x] `test_try_read_nonexistent`
- [x] `test_subscriber_lag`
- [x] `test_slowest_cursor`
- [x] `test_is_lagging`
- [x] `test_skip_for_slow_policy`
- [x] `test_drop_slow_policy`
- [x] `test_channel_close`
- [x] `test_subscriber_info`
- [x] `test_list_subscribers`
- [x] `test_debug_format`
- [x] `test_error_display`
- [x] `test_concurrent_subscribe_read`
- [x] `test_multiple_concurrent_readers`
- [x] `test_derive_single_consumer_spsc`
- [x] `test_derive_multiple_consumers_broadcast`
- [x] `test_derive_mixed_sources`
- [x] `test_derive_no_consumers`
- [x] `test_derive_mv_with_multiple_sources`
- [x] `test_derived_channel_type_methods`
- [x] `test_source_definition`
- [x] `test_mv_definition`
- [x] `test_analyze_mv_sources`
- [x] `test_detailed_derivation`
- [x] `test_three_consumers`

### Performance Targets

| Metric | Target |
|--------|--------|
| Broadcast (2 subs) | < 100ns |
| Broadcast (4 subs) | < 150ns |
| Read | < 50ns |

## Completion Checklist

- [x] Code implemented
- [x] Unit tests passing (42 tests, 100% core coverage)
- [x] Clippy clean (`-D warnings`)
- [x] Documentation updated
- [x] Feature spec updated

---

## Notes

**Zero-Config**: Users never write `channel = 'broadcast'`. The query planner analyzes how many MVs read from each source and automatically upgrades to broadcast when needed.

**Clone Semantics**: In broadcast mode, values are cloned for each subscriber. For `Arc<RecordBatch>`, this is cheap (~2ns atomic increment).

**Cache Alignment**: Write sequence uses `CachePadded` to avoid false sharing.

## References

- [F-STREAM-001: Ring Buffer](F-STREAM-001-ring-buffer.md)
- [F-STREAM-005: Sink](F-STREAM-005-sink.md)
