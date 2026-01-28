# F056: ASOF Joins

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F056 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F019, F003 |
| **Owner** | Implemented |
| **Research** | [Stream Joins Research 2026](../../research/laminardb-stream-joins-research-review-2026.md) |

## Summary

Implement ASOF (temporal proximity) joins that match events based on the closest timestamp rather than exact equality. Essential for time-series data like financial applications where trades need to be enriched with the most recent quote.

## Motivation

From research review:
- **DuckDB (Feb 2025)**: Adaptive plan selection - loop join for small left tables, sorted merge for large
- **Apache Pinot (Sep 2025)**: Hybrid hash + binary search - hash on join key, binary search on time
- **RisingWave (v2.1+)**: Streaming ASOF with continuous state maintenance
- **Snowflake/DolphinDB**: Process-time vs event-time flags

**Current Gap**: LaminarDB has no ASOF join implementation. This is critical for financial/time-series use cases.

## Goals

- Backward, Forward, and Nearest proximity modes
- Tolerance-based matching (optional max time difference)
- Efficient BTreeMap-based state for range queries
- Adaptive plan selection based on cardinality
- SQL syntax compatible with Snowflake/DuckDB standards

## SQL Syntax

```sql
-- Backward ASOF (default): t_left >= t_right (most recent prior)
SELECT t.symbol, t.price, q.bid, q.ask
FROM trades t
ASOF JOIN quotes q
ON t.symbol = q.symbol
AND t.trade_time >= q.quote_time;

-- With tolerance (max 5 second gap)
SELECT t.*, q.bid
FROM trades t
ASOF JOIN quotes q
MATCH_CONDITION(t.trade_time >= q.quote_time)
TOLERANCE INTERVAL '5' SECOND
ON t.symbol = q.symbol;

-- Forward ASOF: t_left <= t_right (next future value)
SELECT o.*, d.delivery_time
FROM orders o
ASOF JOIN deliveries d
MATCH_CONDITION(o.order_time <= d.scheduled_time)
ON o.order_id = d.order_id;

-- Nearest: min absolute time difference
SELECT s.*, r.rate
FROM samples s
ASOF JOIN reference r
MATCH_CONDITION NEAREST
TOLERANCE INTERVAL '1' MINUTE
ON s.sensor_id = r.sensor_id;
```

## Technical Design

### Core Types

```rust
/// Direction for ASOF matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AsofDirection {
    /// Match the most recent prior event (t_left >= t_right).
    /// Most common for enrichment with latest known value.
    #[default]
    Backward,
    /// Match the next future event (t_left <= t_right).
    /// Useful for forward-looking joins.
    Forward,
    /// Match the closest event by absolute time difference.
    /// Useful when direction doesn't matter.
    Nearest,
}

/// Configuration for ASOF joins.
#[derive(Debug, Clone)]
pub struct AsofJoinConfig {
    /// Column name for join key (equality match).
    pub key_column: String,
    /// Column name for time matching in left stream.
    pub left_time_column: String,
    /// Column name for time matching in right stream.
    pub right_time_column: String,
    /// Direction for time matching.
    pub direction: AsofDirection,
    /// Maximum allowed time difference (None = unlimited).
    pub tolerance: Option<Duration>,
    /// Join type (Inner or Left outer).
    pub join_type: AsofJoinType,
}

/// State for each join key - maintains time-ordered events.
pub struct KeyState {
    /// Sorted tree of (timestamp -> JoinRow) for range queries.
    events: BTreeMap<i64, Vec<JoinRow>>,
    /// Minimum timestamp in state (for cleanup).
    min_timestamp: i64,
    /// Maximum timestamp in state.
    max_timestamp: i64,
}
```

### Operator Structure

```rust
pub struct AsofJoinOperator {
    config: AsofJoinConfig,
    operator_id: String,

    /// Right-side state indexed by key.
    /// BTreeMap enables O(log n) range queries.
    right_state: HashMap<Vec<u8>, KeyState>,

    /// Metrics for monitoring.
    metrics: AsofJoinMetrics,

    /// Current watermark for state cleanup.
    watermark: i64,

    /// Output schema.
    output_schema: Option<SchemaRef>,

    // Adaptive planning hints
    left_cardinality_hint: usize,
    right_cardinality_hint: usize,
}
```

### Matching Algorithm

```rust
impl AsofJoinOperator {
    /// Find the best match for a left event in the right state.
    fn find_match(
        &self,
        left_key: &[u8],
        left_timestamp: i64,
    ) -> Option<&JoinRow> {
        let key_state = self.right_state.get(left_key)?;

        match self.config.direction {
            AsofDirection::Backward => {
                // Find largest timestamp <= left_timestamp
                let range = key_state.events.range(..=left_timestamp);
                let (ts, rows) = range.last()?;

                // Check tolerance
                if let Some(tol) = self.config.tolerance {
                    if left_timestamp - ts > tol.as_millis() as i64 {
                        return None;
                    }
                }
                rows.last()
            }

            AsofDirection::Forward => {
                // Find smallest timestamp >= left_timestamp
                let range = key_state.events.range(left_timestamp..);
                let (ts, rows) = range.next()?;

                // Check tolerance
                if let Some(tol) = self.config.tolerance {
                    if ts - left_timestamp > tol.as_millis() as i64 {
                        return None;
                    }
                }
                rows.first()
            }

            AsofDirection::Nearest => {
                // Find closest by absolute difference
                let before = key_state.events.range(..=left_timestamp).last();
                let after = key_state.events.range(left_timestamp..).next();

                let candidate = match (before, after) {
                    (Some((t1, r1)), Some((t2, r2))) => {
                        let d1 = left_timestamp - t1;
                        let d2 = t2 - left_timestamp;
                        if d1 <= d2 { (t1, r1) } else { (t2, r2) }
                    }
                    (Some(x), None) | (None, Some(x)) => x,
                    (None, None) => return None,
                };

                // Check tolerance
                if let Some(tol) = self.config.tolerance {
                    let diff = (left_timestamp - candidate.0).abs();
                    if diff > tol.as_millis() as i64 {
                        return None;
                    }
                }
                candidate.1.first()
            }
        }
    }
}
```

### State Management

```rust
impl AsofJoinOperator {
    /// Store a right-side event in the time-ordered state.
    fn store_right_event(&mut self, key: Vec<u8>, row: JoinRow) {
        let key_state = self.right_state.entry(key).or_insert_with(|| KeyState {
            events: BTreeMap::new(),
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
        });

        let timestamp = row.timestamp;
        key_state.events
            .entry(timestamp)
            .or_insert_with(Vec::new)
            .push(row);

        key_state.min_timestamp = key_state.min_timestamp.min(timestamp);
        key_state.max_timestamp = key_state.max_timestamp.max(timestamp);
    }

    /// Cleanup old state based on watermark.
    fn cleanup_state(&mut self, watermark: i64) {
        // For backward ASOF, keep state that might match future left events
        // For forward ASOF, aggressively clean old state
        let cleanup_threshold = match self.config.direction {
            AsofDirection::Backward => {
                // Keep longer - left events may need old right values
                watermark - self.config.tolerance
                    .map(|t| t.as_millis() as i64)
                    .unwrap_or(0)
            }
            AsofDirection::Forward => {
                // Can clean more aggressively
                watermark
            }
            AsofDirection::Nearest => {
                // Need to keep both directions
                watermark - self.config.tolerance
                    .map(|t| t.as_millis() as i64)
                    .unwrap_or(0)
            }
        };

        for key_state in self.right_state.values_mut() {
            key_state.events = key_state.events.split_off(&cleanup_threshold);
        }

        // Remove empty key entries
        self.right_state.retain(|_, v| !v.events.is_empty());
    }
}
```

### Adaptive Plan Selection (DuckDB Innovation)

```rust
impl AsofJoinOperator {
    /// Select optimal join strategy based on cardinality hints.
    fn select_strategy(&self) -> AsofStrategy {
        const LOOP_JOIN_THRESHOLD: usize = 64;

        if self.left_cardinality_hint <= LOOP_JOIN_THRESHOLD {
            // Small left table: loop join is more efficient
            // Stream large right table through, keep only latest matches
            AsofStrategy::LoopJoin
        } else if self.right_cardinality_hint <= LOOP_JOIN_THRESHOLD {
            // Small right table: swap sides
            AsofStrategy::SwappedLoopJoin
        } else {
            // Both large: use standard BTreeMap approach
            AsofStrategy::SortedMerge
        }
    }
}

enum AsofStrategy {
    /// Loop through small left, stream right
    LoopJoin,
    /// Swap sides, loop through small right
    SwappedLoopJoin,
    /// Standard sorted merge for large tables
    SortedMerge,
}
```

## Implementation Phases

### Phase 1: Basic ASOF (3-4 days)

1. Create `AsofJoinOperator` with basic backward matching
2. Implement BTreeMap-based state storage per key
3. Add tolerance support
4. Unit tests for basic scenarios

### Phase 2: All Directions (2-3 days)

1. Implement Forward direction
2. Implement Nearest direction
3. Add comprehensive tests for all modes
4. Edge case handling (no match, empty state)

### Phase 3: Optimizations (3-4 days)

1. Watermark-based state cleanup
2. Adaptive plan selection hints
3. Metrics and observability
4. Checkpoint/restore support

### Phase 4: SQL Integration (2-3 days)

1. SQL parser support for ASOF JOIN
2. MATCH_CONDITION clause parsing
3. TOLERANCE interval parsing
4. Integration tests with full SQL

## Test Cases

```rust
#[test]
fn test_backward_asof_basic() {
    // Trade at t=1000, quotes at t=900, t=950, t=1050
    // Should match quote at t=950 (most recent prior)
}

#[test]
fn test_forward_asof_basic() {
    // Order at t=1000, deliveries at t=900, t=1100, t=1200
    // Should match delivery at t=1100 (next future)
}

#[test]
fn test_nearest_asof() {
    // Sample at t=1000, refs at t=800, t=1100
    // Should match ref at t=1100 (closer: 100 vs 200)
}

#[test]
fn test_tolerance_exceeded() {
    // Trade at t=1000, nearest quote at t=500
    // With 100ms tolerance, should return no match
}

#[test]
fn test_multiple_keys() {
    // Trades for AAPL and GOOG, verify independent matching
}

#[test]
fn test_state_cleanup() {
    // Verify old state is cleaned up after watermark advances
}
```

## Acceptance Criteria

- [x] Backward ASOF join working with BTreeMap state
- [x] Forward ASOF join working
- [x] Nearest ASOF join working
- [x] Tolerance-based filtering implemented
- [x] Watermark-based state cleanup
- [x] Inner and Left outer join types
- [x] Checkpoint/restore working
- [ ] SQL syntax parsing (ASOF JOIN, MATCH_CONDITION, TOLERANCE) - Phase 4/F006B
- [x] 15+ unit tests passing (22 tests)
- [x] Performance: O(log n) lookup per event

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Lookup per event | O(log n) | BTreeMap range query |
| Memory per key | O(events in window) | Bounded by cleanup |
| State cleanup | O(k) per watermark | k = keys with expired data |

## Future Enhancements (Phase 3+)

1. **Hash+Binary Search** (Apache Pinot pattern)
   - Hash partition on join key
   - Binary search within partitions
   - Better cache locality

2. **GPU Acceleration**
   - Batch multiple lookups
   - Parallel range queries

3. **Incremental Updates**
   - Handle right-side updates/deletes
   - Retraction support

## References

- DuckDB ASOF Implementation (Feb 2025): Adaptive plan selection
- Apache Pinot ASOF (Sep 2025): Hash+binary search hybrid
- RisingWave Streaming ASOF: Continuous state maintenance
- Snowflake MATCH_CONDITION syntax
