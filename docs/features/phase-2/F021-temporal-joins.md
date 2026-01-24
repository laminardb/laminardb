# F021: Temporal Joins

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F021 |
| **Status** | 📝 Draft |
| **Priority** | P2 |
| **Phase** | 2 |
| **Effort** | M (5-7 days) |
| **Dependencies** | F020 |
| **Owner** | TBD |
| **Research** | [Stream Joins Research 2026](../../research/laminardb-stream-joins-research-review-2026.md) |

## Summary

Join streaming events with versioned tables using point-in-time lookups. Temporal joins return the table value that was valid at the event's timestamp, enabling consistent enrichment with time-varying dimension data.

## Motivation

From research review (RisingWave 2025):
- **Append-Only vs Non-Append-Only**: Different code paths for performance
- **Process-Time vs Event-Time**: Clear semantic distinction
- **Index-Based Lookup**: Performance optimization

**Use Cases**:
- Currency rate lookup at transaction time
- Product price lookup at order time
- User tier lookup at event time
- Regulatory compliance (audit trail)

## Goals

1. Point-in-time lookup (FOR SYSTEM_TIME AS OF)
2. Append-only optimization (no state for static tables)
3. Non-append-only support with retraction handling
4. Event-time and process-time semantics
5. Index-based lookup performance

## SQL Syntax

```sql
-- Event-time temporal join (deterministic)
SELECT o.*, r.rate
FROM orders o
JOIN currency_rates FOR SYSTEM_TIME AS OF o.order_time r
    ON o.currency = r.currency;

-- Process-time temporal join (non-deterministic, latest value)
SELECT o.*, c.tier
FROM orders o
JOIN customers FOR SYSTEM_TIME AS OF PROCTIME() c
    ON o.customer_id = c.id;

-- With explicit version column
SELECT t.*, p.price
FROM transactions t
JOIN products FOR SYSTEM_TIME AS OF t.ts VERSIONED ON version_col p
    ON t.product_id = p.id;
```

## Technical Design

### Temporal Join Types

```rust
/// Type of temporal join semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalJoinSemantics {
    /// Event-time: Lookup value valid at event timestamp.
    /// Deterministic, requires versioned table.
    EventTime,

    /// Process-time: Lookup current value at processing time.
    /// Non-deterministic, simpler but less predictable.
    ProcessTime,
}

/// Table update characteristics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableCharacteristics {
    /// Append-only: Only inserts, no updates/deletes.
    /// Optimization: No state needed on streaming side.
    AppendOnly,

    /// Non-append-only: Has updates and/or deletes.
    /// Requires state to track which rows were joined.
    NonAppendOnly,
}
```

### Append-Only Temporal Join (Optimized)

For tables that only receive inserts (no updates/deletes):

```rust
/// Optimized temporal join for append-only tables.
///
/// No state needed on the streaming side because:
/// - Table rows never change after insertion
/// - Join result is deterministic based on event timestamp
/// - No need to track previous join results for retractions
pub struct AppendOnlyTemporalJoinOperator {
    config: TemporalJoinConfig,
    operator_id: String,

    /// Versioned table state: key -> BTreeMap<version_ts, row>
    /// BTreeMap allows efficient lookup of version valid at event time.
    table_state: HashMap<Vec<u8>, BTreeMap<i64, TableRow>>,

    /// Table schema.
    table_schema: Option<SchemaRef>,

    /// Output schema.
    output_schema: Option<SchemaRef>,

    /// Metrics.
    metrics: TemporalJoinMetrics,
}

impl AppendOnlyTemporalJoinOperator {
    /// Find the table row valid at the given timestamp.
    ///
    /// Returns the row with the largest version_ts <= event_ts.
    fn lookup_at_time(&self, key: &[u8], event_ts: i64) -> Option<&TableRow> {
        let versions = self.table_state.get(key)?;

        // Find largest version <= event_ts
        let (_, row) = versions.range(..=event_ts).last()?;
        Some(row)
    }

    /// Process a streaming event.
    fn process_stream_event(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        let mut output = OutputVec::new();

        let key = self.extract_key(&event.data);
        let event_ts = match self.config.semantics {
            TemporalJoinSemantics::EventTime => event.timestamp,
            TemporalJoinSemantics::ProcessTime => ctx.processing_time,
        };

        if let Some(table_row) = self.lookup_at_time(&key, event_ts) {
            let joined = self.create_joined_event(event, table_row);
            output.push(Output::Event(joined));
        } else if self.config.join_type == TemporalJoinType::Left {
            let joined = self.create_unmatched_event(event);
            output.push(Output::Event(joined));
        }
        // Inner join: drop if no match

        output
    }

    /// Process a table update (insert only for append-only).
    fn process_table_insert(&mut self, row: &TableRow) {
        let key = self.extract_table_key(row);
        let version_ts = row.version_timestamp;

        self.table_state
            .entry(key)
            .or_insert_with(BTreeMap::new)
            .insert(version_ts, row.clone());
    }
}
```

### Non-Append-Only Temporal Join

For tables with updates and deletes:

```rust
/// Temporal join for non-append-only tables.
///
/// Requires state on streaming side to handle retractions:
/// - Track which events joined with which table rows
/// - When table row is updated/deleted, emit retraction + new result
pub struct NonAppendOnlyTemporalJoinOperator {
    config: TemporalJoinConfig,
    operator_id: String,

    /// Versioned table state.
    table_state: HashMap<Vec<u8>, BTreeMap<i64, TableRow>>,

    /// Stream event state: tracks join results for potential retraction.
    /// key -> (event_ts, joined_table_version, original_event)
    stream_state: HashMap<Vec<u8>, Vec<JoinedEventRecord>>,

    /// Metrics.
    metrics: TemporalJoinMetrics,
}

/// Record of a joined event for retraction tracking.
struct JoinedEventRecord {
    /// Original stream event.
    event: Event,
    /// Table row version that was joined.
    table_version: i64,
    /// Timestamp of the join result.
    result_timestamp: i64,
}

impl NonAppendOnlyTemporalJoinOperator {
    /// Process a table update (insert, update, or delete).
    fn process_table_change(
        &mut self,
        change: &TableChange,
        ctx: &mut OperatorContext,
    ) -> OutputVec {
        let mut output = OutputVec::new();

        match change {
            TableChange::Insert(row) => {
                self.handle_table_insert(row, &mut output);
            }
            TableChange::Update { old, new } => {
                // Emit retractions for events that joined with old version
                self.emit_retractions_for_version(&old.key, old.version, &mut output);
                // Update state
                self.update_table_version(old, new);
                // Re-join affected events with new version
                self.rejoin_affected_events(&new.key, new.version, &mut output);
            }
            TableChange::Delete(row) => {
                // Emit retractions for events that joined with deleted row
                self.emit_retractions_for_version(&row.key, row.version, &mut output);
                // Remove from state
                self.remove_table_version(row);
            }
        }

        output
    }

    /// Emit retractions for all events that joined with a specific table version.
    fn emit_retractions_for_version(
        &self,
        key: &[u8],
        version: i64,
        output: &mut OutputVec,
    ) {
        if let Some(records) = self.stream_state.get(key) {
            for record in records {
                if record.table_version == version {
                    // Emit retraction (negative weight in Z-set model)
                    output.push(Output::Retraction(record.event.clone()));
                }
            }
        }
    }
}
```

### Configuration

```rust
#[derive(Debug, Clone)]
pub struct TemporalJoinConfig {
    /// Stream key column.
    pub stream_key_column: String,

    /// Table key column.
    pub table_key_column: String,

    /// Table version/timestamp column.
    pub table_version_column: String,

    /// Join semantics (event-time or process-time).
    pub semantics: TemporalJoinSemantics,

    /// Table characteristics (append-only or non-append-only).
    pub table_characteristics: TableCharacteristics,

    /// Join type (inner or left outer).
    pub join_type: TemporalJoinType,

    /// Operator ID for checkpointing.
    pub operator_id: Option<String>,
}
```

### Index-Based Lookup Optimization

```rust
/// Indexed temporal table for fast lookups.
pub struct IndexedTemporalTable {
    /// Primary index: key -> versions
    primary_index: HashMap<Vec<u8>, BTreeMap<i64, TableRow>>,

    /// Secondary indices for common lookup patterns.
    secondary_indices: HashMap<String, SecondaryIndex>,

    /// Statistics for query optimization.
    stats: TableStats,
}

impl IndexedTemporalTable {
    /// Lookup with index selection.
    fn lookup_with_index(
        &self,
        key: &[u8],
        timestamp: i64,
        index_hint: Option<&str>,
    ) -> Option<&TableRow> {
        // Use primary index by default
        let versions = self.primary_index.get(key)?;
        versions.range(..=timestamp).last().map(|(_, row)| row)
    }

    /// Add secondary index on a column.
    pub fn create_index(&mut self, column: &str) {
        // Build secondary index for faster lookups
        let index = SecondaryIndex::build(&self.primary_index, column);
        self.secondary_indices.insert(column.to_string(), index);
    }
}
```

## Implementation Phases

### Phase 1: Append-Only Temporal Join (2-3 days)

1. Implement `AppendOnlyTemporalJoinOperator`
2. Versioned table state with BTreeMap
3. Point-in-time lookup logic
4. Event-time semantics
5. Unit tests

### Phase 2: Non-Append-Only Support (2-3 days)

1. Implement `NonAppendOnlyTemporalJoinOperator`
2. Stream-side state for retraction tracking
3. Table change handling (insert/update/delete)
4. Retraction emission
5. Tests for update/delete scenarios

### Phase 3: Process-Time Support (1 day)

1. Add process-time semantics
2. Current-value lookup mode
3. Tests for non-deterministic behavior

### Phase 4: SQL Integration (1-2 days)

1. Parser support for `FOR SYSTEM_TIME AS OF`
2. `VERSIONED ON` clause
3. `PROCTIME()` function
4. Integration tests

## Test Cases

```rust
#[test]
fn test_event_time_temporal_join() {
    // Order at t=1000, rates at t=500 (1.1), t=800 (1.2), t=1200 (1.3)
    // Should join with rate 1.2 (valid at t=1000)
}

#[test]
fn test_append_only_no_state_needed() {
    // Verify no stream-side state for append-only tables
}

#[test]
fn test_table_update_retraction() {
    // Join event, then update table row
    // Should emit retraction + new join result
}

#[test]
fn test_table_delete_retraction() {
    // Join event, then delete table row
    // Should emit retraction only
}

#[test]
fn test_process_time_lookup() {
    // Verify process-time uses current table value
}

#[test]
fn test_left_join_no_match() {
    // No table row for event key - should emit with nulls
}
```

## Acceptance Criteria

- [ ] Append-only temporal join working
- [ ] Event-time point-in-time lookup correct
- [ ] Non-append-only with retraction support
- [ ] Process-time semantics implemented
- [ ] SQL syntax parsed correctly
- [ ] Checkpoint/restore working
- [ ] 12+ unit tests passing

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Point-in-time lookup | O(log v) | v = versions per key |
| Table insert | O(log v) | BTreeMap insert |
| Retraction scan | O(n) | n = joined events for key |

## Comparison: Temporal vs Lookup vs ASOF

| Aspect | Lookup (F020) | Temporal (F021) | ASOF (F056) |
|--------|---------------|-----------------|-------------|
| **Time Semantics** | Current value | Point-in-time | Nearest match |
| **Table State** | Latest only | Versioned history | Time-ordered |
| **Determinism** | Non-deterministic | Deterministic | Deterministic |
| **Use Case** | Dimension lookup | Versioned data | Time-series |
| **State Size** | O(keys) | O(keys × versions) | O(keys × window) |

## References

- RisingWave Temporal Join Documentation (2025)
- Flink FOR SYSTEM_TIME AS OF Semantics
- SQL:2011 Temporal Tables Standard
