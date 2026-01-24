# F020: Lookup Joins

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F020 |
| **Status** | ✅ Done |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F003 |
| **Owner** | - |

## Summary

Join streaming data with reference tables (dimension tables). Lookup joins enrich events with data from slowly-changing external tables.

## Goals

- Cached reference table lookups
- Configurable cache TTL
- Async cache refresh
- Support multiple backends (Redis, PostgreSQL)

## Implementation

### Module Structure

```
crates/laminar-core/src/operator/lookup_join.rs
├── LookupJoinOperator     # Main operator
├── LookupJoinConfig       # Configuration with builder
├── LookupJoinType         # Inner, Left join types
├── CacheEntry             # Cached lookup with timestamp
└── LookupJoinMetrics      # Operational metrics

crates/laminar-connectors/src/lookup.rs
├── TableLoader            # Async trait for external data sources
├── LookupResult           # Found/NotFound enum
├── LookupError            # Error types
├── InMemoryTableLoader    # Testing/static data implementation
└── NoOpTableLoader        # No-op implementation
```

### Technical Design

```rust
use laminar_core::operator::lookup_join::{
    LookupJoinOperator, LookupJoinConfig, LookupJoinType,
};
use std::time::Duration;

// Create a lookup join that enriches orders with customer data
let config = LookupJoinConfig::builder()
    .stream_key_column("customer_id".to_string())
    .lookup_key_column("id".to_string())
    .cache_ttl(Duration::from_secs(300))  // 5 minute cache
    .join_type(LookupJoinType::Left)
    .build();

let mut operator = LookupJoinOperator::new(config);

// Synchronous lookup with closure
let outputs = operator.process_with_lookup(&event, &mut ctx, |key| {
    lookup_table.get(key).cloned()
});

// Or async flow: process -> check pending_lookups -> provide_lookup
let outputs = operator.process(&event, &mut ctx);
for key in operator.pending_lookups() {
    // Perform async lookup...
    let result = loader.lookup(key).await?;
    operator.provide_lookup(key, result.into_batch().as_ref(), &mut ctx);
}
```

### TableLoader Trait

```rust
use laminar_connectors::lookup::{TableLoader, LookupResult, LookupError};
use async_trait::async_trait;

#[async_trait]
pub trait TableLoader: Send + Sync {
    async fn lookup(&self, key: &[u8]) -> Result<LookupResult, LookupError>;
    async fn lookup_batch(&self, keys: &[&[u8]]) -> Result<Vec<LookupResult>, LookupError>;
    fn name(&self) -> &str;
    async fn health_check(&self) -> bool;
    async fn close(&self) -> Result<(), LookupError>;
}
```

### Cache Behavior

- Cache stored in state store using keys `lkc:<key_hash>`
- Each entry stores: `inserted_at`, `found` flag, serialized batch data
- TTL-based expiration using processing time (not event time)
- Timer-based cleanup of expired entries
- Caches both "found" and "not found" results to avoid repeated lookups

### Join Types

- **Inner**: Only emit events where lookup succeeds
- **Left**: Emit all events, with nulls for failed lookups

## SQL Syntax

```sql
SELECT o.*, c.name, c.tier
FROM orders o
JOIN customers c  -- Reference table
    ON o.customer_id = c.id;
```

## Completion Checklist

- [x] Cache working (state store based with TTL)
- [x] TTL refresh implemented (timer-based expiration)
- [x] Multiple backends supported (TableLoader trait with InMemoryTableLoader)
- [x] Tests passing (16 tests)
- [x] Clippy clean

## Test Coverage

- `test_lookup_join_type_properties` - Join type behavior
- `test_config_builder` - Configuration builder
- `test_inner_join_basic` - Basic inner join
- `test_inner_join_no_match` - Inner join drops unmatched
- `test_left_join_no_match` - Left join emits with nulls
- `test_cache_hit` - Cache lookup performance
- `test_cache_expiry` - TTL expiration
- `test_cache_timer_cleanup` - Timer-based cleanup
- `test_checkpoint_restore` - Fault tolerance
- `test_integer_key_lookup` - Int64 key support
- `test_async_lookup_flow` - Async lookup pattern
- `test_cache_entry_serialization` - Arrow IPC round-trip
- `test_cache_entry_expiry` - Expiration logic
- `test_not_found_cache_entry` - Caching negative results
- `test_metrics_reset` - Metrics tracking
- `test_multiple_events_same_key` - Cache efficiency

## Future Enhancements (Phase 3)

- Redis TableLoader implementation (F030)
- PostgreSQL TableLoader implementation
- Cache size limits with LRU eviction
- Background cache refresh
- Batch lookup optimization
