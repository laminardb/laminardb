# F009: Event Time Processing

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F009 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 1 |
| **Effort** | S (1-2 days) |
| **Dependencies** | F001 |
| **Owner** | TBD |
| **Completed** | 2026-01-24 |

## Summary

Implement event-time semantics where events are processed according to their embedded timestamps rather than arrival time. This is essential for correct windowing and out-of-order event handling.

## Goals

- Extract timestamps from event payloads
- Support configurable timestamp fields
- Track event-time progress
- Enable event-time windowing

## Technical Design

```rust
pub enum TimestampFormat {
    UnixMillis,    // i64 milliseconds (default)
    UnixSeconds,   // i64 seconds -> millis
    UnixMicros,    // i64 micros -> millis
    UnixNanos,     // i64 nanos -> millis
    Iso8601,       // String parsing
    ArrowNative,   // Auto-detect from Arrow Timestamp type
}

pub enum TimestampField {
    Name(String),  // Column name (cached after first lookup)
    Index(usize),  // Column index (most efficient)
}

pub enum ExtractionMode {
    First,  // Default, O(1)
    Last,   // O(1)
    Max,    // O(n)
    Min,    // O(n)
}

pub struct EventTimeExtractor {
    field: TimestampField,
    format: TimestampFormat,
    mode: ExtractionMode,
    cached_index: Option<usize>,
}

impl EventTimeExtractor {
    pub fn from_column(name: &str, format: TimestampFormat) -> Self;
    pub fn from_index(index: usize, format: TimestampFormat) -> Self;
    pub fn with_mode(self, mode: ExtractionMode) -> Self;
    pub fn extract(&mut self, batch: &RecordBatch) -> Result<i64, EventTimeError>;
    pub fn validate_schema(&self, schema: &Schema) -> Result<(), EventTimeError>;
}
```

## Implementation

| File | Description |
|------|-------------|
| `crates/laminar-core/src/time/event_time.rs` | EventTimeExtractor implementation |
| `crates/laminar-core/src/time/mod.rs` | Module exports and re-exports |

## Completion Checklist

- [x] Timestamp extraction working
- [x] Multiple formats supported (6 formats)
- [x] Multiple extraction modes (First, Last, Max, Min)
- [x] Column name caching for performance
- [x] Schema validation
- [x] Comprehensive error handling
- [x] Unit tests passing (27 tests)
