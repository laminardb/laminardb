# F063: Changelog and Retraction Support (Z-Sets)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F063 |
| **Status** | ✅ Done |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F011, F012 |
| **Blocks** | F023, F060 |
| **Owner** | TBD |
| **Research** | [Emit Patterns Research 2026](../../research/emit-patterns-research-2026.md) |
| **Completed** | 2026-01-25 |

## Summary

Implement Z-set style changelog records with integer weights for incremental computation. This is the foundation for exactly-once sinks, cascading materialized views, and CDC connectors.

## Motivation

From the 2026 emit patterns research:

> "The DBSP framework provides a **mathematical foundation** for incremental computation... Z-sets where elements have integer weights. Weight > 0 → insert, weight < 0 → delete."

**Critical Gaps Identified:**
1. F023 (Exactly-Once Sinks) cannot handle late data corrections without retractions
2. F060 (Cascading MVs) needs changelog format for MV-to-MV propagation
3. Phase 3 CDC connectors need standard changelog envelope format

**Industry Implementations:**
- **DBSP/Feldera**: Z-sets with automatic incrementalization (VLDB 2025 Best Paper)
- **Apache Flink**: Retraction mode with +I/-D/+U/-U operations
- **RisingWave**: Changelog with emit-on-window-close for append-only sinks
- **Debezium**: Standard CDC envelope format

## Goals

1. Z-set style `ChangelogRecord<T>` with `weight: i64` field
2. CDC operation types: INSERT, DELETE, UPDATE_BEFORE, UPDATE_AFTER
3. Retraction generation for late data (emit `-old, +new` pairs)
4. Standard changelog serialization format (Debezium-compatible)
5. Aggregate operators track insert/delete weights
6. Zero-allocation changelog references for Ring 0

## Non-Goals

- Full DBSP automatic incrementalization (Phase 3+)
- Multi-way join retraction optimization (Phase 3)
- Changelog compaction/deduplication (Phase 3)
- External CDC source parsing (F027/F028)

## Technical Design

### Core Types

```rust
/// CDC operation type (Flink/Debezium compatible).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum CdcOperation {
    /// New record insertion (+I)
    Insert,
    /// Record deletion (-D)
    Delete,
    /// Value before update (-U) - paired with UpdateAfter
    UpdateBefore,
    /// Value after update (+U) - paired with UpdateBefore
    UpdateAfter,
}

impl CdcOperation {
    /// Returns the Z-set weight for this operation.
    #[inline]
    pub const fn weight(&self) -> i64 {
        match self {
            Self::Insert | Self::UpdateAfter => 1,
            Self::Delete | Self::UpdateBefore => -1,
        }
    }

    /// Returns true if this is an additive operation (insert/update-after).
    #[inline]
    pub const fn is_insert(&self) -> bool {
        matches!(self, Self::Insert | Self::UpdateAfter)
    }
}

/// Changelog record with Z-set weight.
///
/// The weight represents the multiplicity change:
/// - `weight > 0`: Insert (add `weight` copies)
/// - `weight < 0`: Delete (remove `|weight|` copies)
/// - `weight == 0`: No change (no-op)
///
/// For most streaming operations, weights are +1 (insert) or -1 (delete).
#[derive(Debug, Clone, PartialEq)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ChangelogRecord<T> {
    /// The data payload
    pub data: T,
    /// Z-set weight: +1 insert, -1 delete, 0 no-op
    pub weight: i64,
    /// Event timestamp (for watermark coordination)
    pub timestamp: i64,
    /// CDC operation type (for sink compatibility)
    pub operation: CdcOperation,
}

impl<T> ChangelogRecord<T> {
    /// Creates an insert record.
    pub fn insert(data: T, timestamp: i64) -> Self {
        Self {
            data,
            weight: 1,
            timestamp,
            operation: CdcOperation::Insert,
        }
    }

    /// Creates a delete record.
    pub fn delete(data: T, timestamp: i64) -> Self {
        Self {
            data,
            weight: -1,
            timestamp,
            operation: CdcOperation::Delete,
        }
    }

    /// Creates a retraction pair (update-before, update-after).
    pub fn retraction(old: T, new: T, timestamp: i64) -> (Self, Self)
    where
        T: Clone,
    {
        (
            Self {
                data: old,
                weight: -1,
                timestamp,
                operation: CdcOperation::UpdateBefore,
            },
            Self {
                data: new,
                weight: 1,
                timestamp,
                operation: CdcOperation::UpdateAfter,
            },
        )
    }

    /// Returns true if this is an insert operation.
    #[inline]
    pub fn is_insert(&self) -> bool {
        self.weight > 0
    }

    /// Returns true if this is a delete operation.
    #[inline]
    pub fn is_delete(&self) -> bool {
        self.weight < 0
    }
}
```

### Zero-Allocation Changelog Reference (Ring 0)

```rust
/// Zero-allocation changelog reference for Ring 0 hot path.
///
/// Instead of allocating a full ChangelogRecord, this stores
/// offsets into the event batch with the operation type.
#[derive(Debug, Clone, Copy)]
pub struct ChangelogRef {
    /// Offset into the current batch
    pub batch_offset: u32,
    /// Row index within the batch
    pub row_index: u32,
    /// Z-set weight
    pub weight: i16,
    /// Operation type
    pub operation: CdcOperation,
}

/// Ring 0 changelog buffer (pre-allocated, reused per epoch).
pub struct ChangelogBuffer {
    /// Pre-allocated changelog references
    refs: Vec<ChangelogRef>,
    /// Current write position
    len: usize,
    /// Capacity
    capacity: usize,
}

impl ChangelogBuffer {
    /// Creates a new buffer with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            refs: Vec::with_capacity(capacity),
            len: 0,
            capacity,
        }
    }

    /// Pushes a changelog reference (no allocation if under capacity).
    #[inline]
    pub fn push(&mut self, changelog_ref: ChangelogRef) -> bool {
        if self.len < self.capacity {
            if self.len >= self.refs.len() {
                self.refs.push(changelog_ref);
            } else {
                self.refs[self.len] = changelog_ref;
            }
            self.len += 1;
            true
        } else {
            false // Buffer full - backpressure signal
        }
    }

    /// Drains references for Ring 1 processing.
    pub fn drain(&mut self) -> impl Iterator<Item = ChangelogRef> + '_ {
        let len = self.len;
        self.len = 0;
        self.refs.drain(..len)
    }

    /// Returns current count.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}
```

### Retraction-Enabled Aggregator Trait

```rust
/// Extension trait for aggregators that support retractions.
pub trait RetractableAggregator: Aggregator {
    /// Retracts (un-applies) a value from the aggregate.
    ///
    /// This is the inverse of `accumulate`. For example:
    /// - Count: decrement by 1
    /// - Sum: subtract the value
    /// - Min/Max: may need to recompute from remaining values
    fn retract(&mut self, value: &Self::Input);

    /// Returns true if this aggregator can efficiently retract.
    ///
    /// Some aggregators (like Min/Max) may need to scan all values
    /// on retraction if the retracted value was the current min/max.
    fn supports_efficient_retraction(&self) -> bool;
}

/// Count aggregator with retraction support.
#[derive(Debug, Clone, Default)]
pub struct RetractableCountAggregator {
    count: i64, // Signed to support negative from retractions
}

impl Aggregator for RetractableCountAggregator {
    type Input = ();
    type Output = i64;

    fn accumulate(&mut self, _value: &Self::Input) {
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        self.count += other.count;
    }

    fn finalize(&self) -> Self::Output {
        self.count
    }

    fn reset(&mut self) {
        self.count = 0;
    }
}

impl RetractableAggregator for RetractableCountAggregator {
    fn retract(&mut self, _value: &Self::Input) {
        self.count -= 1;
    }

    fn supports_efficient_retraction(&self) -> bool {
        true // O(1) retraction
    }
}

/// Sum aggregator with retraction support.
#[derive(Debug, Clone, Default)]
pub struct RetractableSumAggregator {
    sum: i64,
}

impl Aggregator for RetractableSumAggregator {
    type Input = i64;
    type Output = i64;

    fn accumulate(&mut self, value: &Self::Input) {
        self.sum += value;
    }

    fn merge(&mut self, other: &Self) {
        self.sum += other.sum;
    }

    fn finalize(&self) -> Self::Output {
        self.sum
    }

    fn reset(&mut self) {
        self.sum = 0;
    }
}

impl RetractableAggregator for RetractableSumAggregator {
    fn retract(&mut self, value: &Self::Input) {
        self.sum -= value;
    }

    fn supports_efficient_retraction(&self) -> bool {
        true // O(1) retraction
    }
}
```

### Late Data Retraction Generation

```rust
/// Generates retractions for late data corrections.
///
/// When late data arrives and updates an already-emitted window result,
/// we need to emit:
/// 1. A retraction (-1 weight) for the old result
/// 2. An insert (+1 weight) for the new result
pub struct LateDataRetractionGenerator {
    /// Previously emitted results (for generating retractions)
    emitted_results: HashMap<WindowId, EmittedResult>,
    /// Whether retraction generation is enabled
    enabled: bool,
}

#[derive(Debug, Clone)]
struct EmittedResult {
    /// The emitted data (serialized for comparison)
    data: Vec<u8>,
    /// Timestamp when emitted
    emit_time: i64,
    /// Number of times re-emitted (for metrics)
    version: u32,
}

impl LateDataRetractionGenerator {
    /// Creates a new generator.
    pub fn new(enabled: bool) -> Self {
        Self {
            emitted_results: HashMap::new(),
            enabled,
        }
    }

    /// Checks if we need to generate a retraction for this window.
    ///
    /// Returns `Some((old_data, new_data))` if retraction needed.
    pub fn check_retraction(
        &mut self,
        window_id: &WindowId,
        new_data: &[u8],
        timestamp: i64,
    ) -> Option<(Vec<u8>, Vec<u8>)> {
        if !self.enabled {
            return None;
        }

        if let Some(prev) = self.emitted_results.get_mut(window_id) {
            if prev.data != new_data {
                let old_data = std::mem::replace(&mut prev.data, new_data.to_vec());
                prev.emit_time = timestamp;
                prev.version += 1;
                return Some((old_data, new_data.to_vec()));
            }
        } else {
            self.emitted_results.insert(
                window_id.clone(),
                EmittedResult {
                    data: new_data.to_vec(),
                    emit_time: timestamp,
                    version: 1,
                },
            );
        }

        None
    }

    /// Cleans up state for closed windows.
    pub fn cleanup_window(&mut self, window_id: &WindowId) {
        self.emitted_results.remove(window_id);
    }
}
```

### CDC Envelope Format (Debezium-Compatible)

```rust
/// CDC envelope for sink serialization.
///
/// Compatible with Debezium envelope format for interoperability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEnvelope<T> {
    /// Operation type: "c" (create), "u" (update), "d" (delete), "r" (read/snapshot)
    pub op: String,
    /// Timestamp in milliseconds
    pub ts_ms: i64,
    /// Source metadata
    pub source: CdcSource,
    /// Value before change (for updates/deletes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<T>,
    /// Value after change (for inserts/updates)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<T>,
}

/// Source metadata for CDC envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSource {
    /// Source name (e.g., "laminardb")
    pub name: String,
    /// Database/schema
    pub db: String,
    /// Table/view name
    pub table: String,
    /// Sequence number for ordering
    pub sequence: u64,
}

impl<T> CdcEnvelope<T> {
    /// Creates an insert envelope.
    pub fn insert(after: T, source: CdcSource, ts_ms: i64) -> Self {
        Self {
            op: "c".to_string(),
            ts_ms,
            source,
            before: None,
            after: Some(after),
        }
    }

    /// Creates a delete envelope.
    pub fn delete(before: T, source: CdcSource, ts_ms: i64) -> Self {
        Self {
            op: "d".to_string(),
            ts_ms,
            source,
            before: Some(before),
            after: None,
        }
    }

    /// Creates an update envelope.
    pub fn update(before: T, after: T, source: CdcSource, ts_ms: i64) -> Self {
        Self {
            op: "u".to_string(),
            ts_ms,
            source,
            before: Some(before),
            after: Some(after),
        }
    }
}
```

### Extended EmitStrategy

See F011B (EMIT Clause Extension) for the following additions:

```rust
pub enum EmitStrategy {
    OnWatermark,           // Existing
    Periodic(Duration),    // Existing
    OnUpdate,              // Existing
    OnWindowClose,         // NEW: For append-only sinks
    Changelog,             // NEW: Emit with Z-set weights
    Final,                 // NEW: Suppress intermediate
}
```

## Implementation Phases

### Phase 1: Core Types (2-3 days)

1. Implement `CdcOperation` enum
2. Implement `ChangelogRecord<T>` struct
3. Implement `ChangelogRef` and `ChangelogBuffer` for Ring 0
4. Add rkyv serialization
5. Unit tests for all types

### Phase 2: Retractable Aggregators (3-4 days)

1. Define `RetractableAggregator` trait
2. Implement `RetractableCountAggregator`
3. Implement `RetractableSumAggregator`
4. Implement `RetractableAvgAggregator`
5. Tests for retraction correctness

### Phase 3: Late Data Retraction (2-3 days)

1. Implement `LateDataRetractionGenerator`
2. Integrate with `TumblingWindowOperator`
3. Integrate with `SlidingWindowOperator`
4. Tests for late data retraction scenarios

### Phase 4: CDC Envelope (2-3 days)

1. Implement `CdcEnvelope<T>` struct
2. Implement `CdcSource` metadata
3. JSON serialization (Debezium-compatible)
4. Integration with sink connectors
5. Format compatibility tests

## Test Cases

```rust
#[test]
fn test_changelog_record_insert() {
    let record = ChangelogRecord::insert("hello", 1000);
    assert_eq!(record.weight, 1);
    assert!(record.is_insert());
    assert_eq!(record.operation, CdcOperation::Insert);
}

#[test]
fn test_changelog_record_delete() {
    let record = ChangelogRecord::<&str>::delete("goodbye", 2000);
    assert_eq!(record.weight, -1);
    assert!(record.is_delete());
    assert_eq!(record.operation, CdcOperation::Delete);
}

#[test]
fn test_retraction_pair() {
    let (before, after) = ChangelogRecord::retraction("old", "new", 3000);
    assert_eq!(before.weight, -1);
    assert_eq!(after.weight, 1);
    assert_eq!(before.operation, CdcOperation::UpdateBefore);
    assert_eq!(after.operation, CdcOperation::UpdateAfter);
}

#[test]
fn test_changelog_buffer_zero_alloc() {
    let mut buffer = ChangelogBuffer::with_capacity(1024);

    // Push should not allocate once pre-warmed
    for i in 0..100 {
        buffer.push(ChangelogRef {
            batch_offset: i,
            row_index: 0,
            weight: 1,
            operation: CdcOperation::Insert,
        });
    }

    assert_eq!(buffer.len(), 100);

    // Drain and reuse
    let drained: Vec<_> = buffer.drain().collect();
    assert_eq!(drained.len(), 100);
    assert!(buffer.is_empty());
}

#[test]
fn test_retractable_count() {
    let mut agg = RetractableCountAggregator::default();

    agg.accumulate(&());
    agg.accumulate(&());
    agg.accumulate(&());
    assert_eq!(agg.finalize(), 3);

    agg.retract(&());
    assert_eq!(agg.finalize(), 2);
}

#[test]
fn test_retractable_sum() {
    let mut agg = RetractableSumAggregator::default();

    agg.accumulate(&10);
    agg.accumulate(&20);
    agg.accumulate(&30);
    assert_eq!(agg.finalize(), 60);

    agg.retract(&20);
    assert_eq!(agg.finalize(), 40);
}

#[test]
fn test_late_data_retraction_generation() {
    let mut gen = LateDataRetractionGenerator::new(true);
    let window_id = WindowId { start: 0, end: 60000 };

    // First emission - no retraction
    let result1 = gen.check_retraction(&window_id, b"count=5", 1000);
    assert!(result1.is_none());

    // Late data causes different result - generates retraction
    let result2 = gen.check_retraction(&window_id, b"count=7", 2000);
    assert!(result2.is_some());
    let (old, new) = result2.unwrap();
    assert_eq!(old, b"count=5");
    assert_eq!(new, b"count=7");

    // Same result - no retraction
    let result3 = gen.check_retraction(&window_id, b"count=7", 3000);
    assert!(result3.is_none());
}

#[proptest]
fn test_changelog_weights_sum_to_count(inserts: Vec<i64>, deletes: Vec<i64>) {
    let mut agg = RetractableSumAggregator::default();

    let mut expected = 0i64;
    for v in &inserts {
        agg.accumulate(v);
        expected += v;
    }
    for v in &deletes {
        agg.retract(v);
        expected -= v;
    }

    assert_eq!(agg.finalize(), expected);
}

#[test]
fn test_cdc_envelope_debezium_compatible() {
    let source = CdcSource {
        name: "laminardb".to_string(),
        db: "default".to_string(),
        table: "orders".to_string(),
        sequence: 42,
    };

    let envelope = CdcEnvelope::insert(
        serde_json::json!({"id": 1, "amount": 100}),
        source,
        1706140800000,
    );

    let json = serde_json::to_string(&envelope).unwrap();
    assert!(json.contains("\"op\":\"c\""));
    assert!(json.contains("\"after\""));
    assert!(!json.contains("\"before\""));
}
```

## Acceptance Criteria

- [ ] `ChangelogRecord<T>` with Z-set weight implemented
- [ ] `CdcOperation` enum with Insert/Delete/UpdateBefore/UpdateAfter
- [ ] `ChangelogBuffer` for zero-allocation Ring 0 path
- [ ] `RetractableAggregator` trait defined
- [ ] Count, Sum, Avg aggregators support retraction
- [ ] `LateDataRetractionGenerator` for late data corrections
- [ ] `CdcEnvelope` Debezium-compatible format
- [ ] Integration with F012 (Late Data Handling)
- [ ] 20+ unit tests passing
- [ ] Property tests for weight summation correctness

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| ChangelogRef creation | < 10ns | Zero allocation |
| ChangelogBuffer push | < 5ns | Pre-allocated |
| Retraction check | < 100ns | HashMap lookup |
| CDC envelope serialize | < 1μs | JSON encoding |
| Retractable count acc/ret | < 5ns | Atomic increment |

## Ring Architecture Integration

```
┌────────────────────────────────────────────────────────────────────┐
│                          RING 0: HOT PATH                          │
│  ChangelogBuffer (pre-allocated)                                   │
│  ChangelogRef (zero-copy offset references)                        │
│  RetractableAggregator.accumulate/retract (no allocation)          │
├────────────────────────────────────────────────────────────────────┤
│                       RING 1: BACKGROUND                           │
│  LateDataRetractionGenerator (HashMap for emitted results)         │
│  CdcEnvelope serialization                                         │
│  Sink writing with changelog format                                │
├────────────────────────────────────────────────────────────────────┤
│                      RING 2: CONTROL PLANE                         │
│  Changelog configuration                                           │
│  CDC format selection                                              │
│  Retraction policy management                                      │
└────────────────────────────────────────────────────────────────────┘
```

## Future Enhancements

1. **Min/Max Retraction**: Efficient retraction for Min/Max (requires value tracking)
2. **Changelog Compaction**: Merge +1/-1 pairs for same key
3. **DBSP Incrementalization**: Automatic query incrementalization
4. **Multi-way Join Retractions**: Optimized retraction for complex joins

## References

- [Emit Patterns Research 2026](../../research/emit-patterns-research-2026.md)
- [DBSP: Automatic IVM for Rich Query Languages](https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf) - VLDB 2023
- [Feldera Documentation](https://docs.feldera.com/)
- [Debezium CDC Envelope Format](https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-events)
- [Flink Retraction Mode](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/concepts/dynamic_tables/#table-to-stream-conversion)
