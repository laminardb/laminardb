# F019: Stream-Stream Joins

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F019 |
| **Status** | ✅ Done |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F003, F010 |
| **Owner** | TBD |

## Summary

Implement joins between two event streams within a time window. Events from one stream are matched with events from another stream if they share a key and fall within a time bound.

## Goals

- Time-bounded stream joins
- Inner, left, right, full outer joins
- Efficient state management
- Watermark-based cleanup

## Technical Design

```rust
pub struct StreamJoinOperator {
    left_state: StateStore,
    right_state: StateStore,
    time_bound: Duration,
    join_type: JoinType,
}

impl StreamJoinOperator {
    pub fn process_left(&mut self, event: &Event) -> Vec<o> {
        // Store in left state
        self.left_state.put(&event.key, event);
        
        // Probe right state for matches
        let matches = self.right_state.range_scan(
            &event.key,
            event.timestamp - self.time_bound.as_millis() as u64,
            event.timestamp + self.time_bound.as_millis() as u64,
        );
        
        matches.map(|right| self.join(event, right)).collect()
    }
}
```

## SQL Syntax

```sql
SELECT o.*, p.status
FROM orders o
JOIN payments p
    ON o.order_id = p.order_id
    AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR;
```

## Completion Checklist

- [x] All join types working (Inner, Left, Right, Full)
- [x] State cleanup on watermark
- [ ] SQL syntax complete (deferred to Phase 2 SQL work)
- [ ] Performance benchmarks passing (deferred)
