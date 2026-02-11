//! Schema-aware event time extraction for compiled queries.
//!
//! [`RowEventTimeExtractor`] reads the event timestamp directly from an
//! [`EventRow`] field, avoiding the overhead of columnar extraction used
//! by the batch-level [`EventTimeExtractor`](crate::time::EventTimeExtractor).
//!
//! When no timestamp column is present (or the schema has no suitable field),
//! the caller should fall back to using the row index as a monotonic surrogate.

use super::row::{EventRow, FieldType, RowSchema};

// ────────────────────────────── EventTimeConfig ─────────────────────────

/// Configuration for event time extraction in compiled queries.
#[derive(Debug, Clone, Default)]
pub struct EventTimeConfig {
    /// Explicit column name for the event timestamp.
    /// When `None`, auto-detection scans for well-known names.
    pub column: Option<String>,
    /// Watermark delay in microseconds (subtracted from max observed timestamp).
    pub watermark_delay_us: i64,
}

// ────────────────────────────── RowEventTimeExtractor ────────────────────

/// Extracts event timestamps from [`EventRow`] fields.
///
/// Supports `Int64` and `TimestampMicros` field types. Tracks the maximum
/// observed timestamp and computes a watermark with a configurable delay.
///
/// # Auto-Detection
///
/// When no explicit column is specified, [`from_schema`](Self::from_schema)
/// scans the schema for:
/// 1. A `TimestampMicros` field (first one found)
/// 2. A field named `ts`, `event_time`, or `timestamp` (case-insensitive)
///
/// If neither heuristic matches, returns `None` — the caller should use
/// a monotonic surrogate (row index).
pub struct RowEventTimeExtractor {
    /// Index of the timestamp field in the `RowSchema`.
    field_idx: usize,
    /// Type of the timestamp field.
    field_type: FieldType,
    /// Maximum observed timestamp.
    max_timestamp: i64,
    /// Configured watermark delay (microseconds).
    delay: i64,
}

impl RowEventTimeExtractor {
    /// Creates an extractor for a known field index and type.
    ///
    /// # Panics
    ///
    /// Debug-asserts that `field_type` is `Int64` or `TimestampMicros`.
    #[must_use]
    pub fn new(field_idx: usize, field_type: FieldType, delay: i64) -> Self {
        debug_assert!(
            field_type == FieldType::Int64 || field_type == FieldType::TimestampMicros,
            "event time field must be Int64 or TimestampMicros, got {field_type:?}"
        );
        Self {
            field_idx,
            field_type,
            max_timestamp: i64::MIN,
            delay,
        }
    }

    /// Attempts to create an extractor by scanning the schema.
    ///
    /// Uses the following strategy:
    /// 1. If `config.column` is `Some(name)`, look up that column exactly.
    /// 2. Otherwise, pick the first `TimestampMicros` field.
    /// 3. Otherwise, pick a field named `ts`, `event_time`, or `timestamp`.
    ///
    /// Returns `None` if no suitable field is found.
    #[must_use]
    pub fn from_schema(schema: &RowSchema, config: &EventTimeConfig) -> Option<Self> {
        let delay = config.watermark_delay_us;

        // 1. Explicit column name.
        if let Some(ref name) = config.column {
            return Self::find_by_name(schema, name, delay);
        }

        // 2. First TimestampMicros field.
        for (idx, layout) in schema.fields().iter().enumerate() {
            if layout.field_type == FieldType::TimestampMicros {
                return Some(Self::new(idx, FieldType::TimestampMicros, delay));
            }
        }

        // 3. Well-known names.
        for well_known in &["ts", "event_time", "timestamp"] {
            if let Some(ext) = Self::find_by_name(schema, well_known, delay) {
                return Some(ext);
            }
        }

        None
    }

    /// Extracts the event time from a row and updates the max timestamp.
    ///
    /// Returns the raw timestamp value (microseconds or plain `i64`).
    #[inline]
    pub fn extract(&mut self, row: &EventRow<'_>) -> i64 {
        if row.is_null(self.field_idx) {
            return self.max_timestamp;
        }

        let ts = match self.field_type {
            FieldType::Int64 | FieldType::TimestampMicros => row.get_i64(self.field_idx),
            _ => unreachable!("validated in constructor"),
        };

        if ts > self.max_timestamp {
            self.max_timestamp = ts;
        }
        ts
    }

    /// Returns the current watermark: `max_timestamp - delay`.
    ///
    /// Returns `i64::MIN` if no timestamps have been observed.
    #[inline]
    #[must_use]
    pub fn watermark(&self) -> i64 {
        self.max_timestamp.saturating_sub(self.delay)
    }

    /// Returns the maximum observed timestamp.
    #[inline]
    #[must_use]
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    /// Returns the field index used for extraction.
    #[inline]
    #[must_use]
    pub fn field_idx(&self) -> usize {
        self.field_idx
    }

    /// Returns the field type used for extraction.
    #[inline]
    #[must_use]
    pub fn field_type(&self) -> FieldType {
        self.field_type
    }

    // ── internal ─────────────────────────────────────────────────

    fn find_by_name(schema: &RowSchema, name: &str, delay: i64) -> Option<Self> {
        let arrow_schema = schema.arrow_schema();
        let lower = name.to_ascii_lowercase();
        for (idx, field) in arrow_schema.fields().iter().enumerate() {
            if field.name().to_ascii_lowercase() == lower {
                let ft = schema.fields()[idx].field_type;
                if ft == FieldType::Int64 || ft == FieldType::TimestampMicros {
                    return Some(Self::new(idx, ft, delay));
                }
            }
        }
        None
    }
}

impl std::fmt::Debug for RowEventTimeExtractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowEventTimeExtractor")
            .field("field_idx", &self.field_idx)
            .field("field_type", &self.field_type)
            .field("max_timestamp", &self.max_timestamp)
            .field("delay", &self.delay)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::row::{MutableEventRow, RowSchema};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use bumpalo::Bump;
    use std::sync::Arc;

    fn make_schema(fields: Vec<(&str, DataType)>) -> Arc<Schema> {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt)| Field::new(name, dt, true))
                .collect::<Vec<_>>(),
        ))
    }

    // ── Constructor tests ────────────────────────────────────────

    #[test]
    fn new_with_int64() {
        let ext = RowEventTimeExtractor::new(0, FieldType::Int64, 1000);
        assert_eq!(ext.field_idx(), 0);
        assert_eq!(ext.field_type(), FieldType::Int64);
        assert_eq!(ext.max_timestamp(), i64::MIN);
        assert_eq!(ext.watermark(), i64::MIN); // MIN - 1000 saturates
    }

    #[test]
    fn new_with_timestamp_micros() {
        let ext = RowEventTimeExtractor::new(1, FieldType::TimestampMicros, 0);
        assert_eq!(ext.field_idx(), 1);
        assert_eq!(ext.field_type(), FieldType::TimestampMicros);
    }

    // ── Auto-detection tests ────────────────────────────────────

    #[test]
    fn auto_detect_timestamp_micros() {
        let arrow = make_schema(vec![
            ("x", DataType::Int64),
            (
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
        ]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let config = EventTimeConfig::default();
        let ext = RowEventTimeExtractor::from_schema(&rs, &config).unwrap();
        assert_eq!(ext.field_idx(), 1);
        assert_eq!(ext.field_type(), FieldType::TimestampMicros);
    }

    #[test]
    fn auto_detect_well_known_name_ts() {
        let arrow = make_schema(vec![("value", DataType::Float64), ("ts", DataType::Int64)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let config = EventTimeConfig::default();
        let ext = RowEventTimeExtractor::from_schema(&rs, &config).unwrap();
        assert_eq!(ext.field_idx(), 1);
        assert_eq!(ext.field_type(), FieldType::Int64);
    }

    #[test]
    fn auto_detect_well_known_name_event_time() {
        let arrow = make_schema(vec![
            ("event_time", DataType::Int64),
            ("val", DataType::Float64),
        ]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let config = EventTimeConfig::default();
        let ext = RowEventTimeExtractor::from_schema(&rs, &config).unwrap();
        assert_eq!(ext.field_idx(), 0);
    }

    #[test]
    fn auto_detect_well_known_name_timestamp() {
        let arrow = make_schema(vec![
            ("id", DataType::Int64),
            ("timestamp", DataType::Int64),
        ]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let config = EventTimeConfig::default();
        let ext = RowEventTimeExtractor::from_schema(&rs, &config).unwrap();
        assert_eq!(ext.field_idx(), 1);
    }

    #[test]
    fn auto_detect_case_insensitive() {
        let arrow = make_schema(vec![("id", DataType::Float64), ("TS", DataType::Int64)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let config = EventTimeConfig::default();
        let ext = RowEventTimeExtractor::from_schema(&rs, &config).unwrap();
        assert_eq!(ext.field_idx(), 1);
    }

    #[test]
    fn auto_detect_none_when_no_match() {
        let arrow = make_schema(vec![("x", DataType::Float64), ("y", DataType::Float64)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let config = EventTimeConfig::default();
        assert!(RowEventTimeExtractor::from_schema(&rs, &config).is_none());
    }

    #[test]
    fn explicit_column_name() {
        let arrow = make_schema(vec![("x", DataType::Int64), ("my_ts", DataType::Int64)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let config = EventTimeConfig {
            column: Some("my_ts".to_string()),
            watermark_delay_us: 5000,
        };
        let ext = RowEventTimeExtractor::from_schema(&rs, &config).unwrap();
        assert_eq!(ext.field_idx(), 1);
        assert_eq!(ext.delay, 5000);
    }

    #[test]
    fn explicit_column_not_found() {
        let arrow = make_schema(vec![("x", DataType::Int64)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let config = EventTimeConfig {
            column: Some("nonexistent".to_string()),
            watermark_delay_us: 0,
        };
        assert!(RowEventTimeExtractor::from_schema(&rs, &config).is_none());
    }

    #[test]
    fn explicit_column_wrong_type() {
        let arrow = make_schema(vec![("label", DataType::Utf8)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let config = EventTimeConfig {
            column: Some("label".to_string()),
            watermark_delay_us: 0,
        };
        assert!(RowEventTimeExtractor::from_schema(&rs, &config).is_none());
    }

    // ── Extraction tests ─────────────────────────────────────────

    #[test]
    fn extract_from_int64() {
        let arrow = make_schema(vec![("ts", DataType::Int64), ("val", DataType::Float64)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut ext = RowEventTimeExtractor::new(0, FieldType::Int64, 0);

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_i64(0, 42_000);
        row.set_f64(1, 1.0);
        let row = row.freeze();

        assert_eq!(ext.extract(&row), 42_000);
        assert_eq!(ext.max_timestamp(), 42_000);
    }

    #[test]
    fn extract_from_timestamp_micros() {
        let arrow = make_schema(vec![
            ("x", DataType::Float64),
            ("ts", DataType::Timestamp(TimeUnit::Microsecond, None)),
        ]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut ext = RowEventTimeExtractor::new(1, FieldType::TimestampMicros, 0);

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_f64(0, 3.14);
        row.set_i64(1, 1_000_000);
        let row = row.freeze();

        assert_eq!(ext.extract(&row), 1_000_000);
    }

    #[test]
    fn extract_null_returns_max() {
        let arrow = make_schema(vec![("ts", DataType::Int64)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut ext = RowEventTimeExtractor::new(0, FieldType::Int64, 0);

        // First: set a non-null value.
        let arena = Bump::new();
        let mut row1 = MutableEventRow::new_in(&arena, &rs, 0);
        row1.set_i64(0, 100);
        let row1 = row1.freeze();
        ext.extract(&row1);

        // Second: null field — should return current max (100).
        let mut row2 = MutableEventRow::new_in(&arena, &rs, 0);
        row2.set_null(0, true);
        let row2 = row2.freeze();
        assert_eq!(ext.extract(&row2), 100);
    }

    // ── Watermark tests ──────────────────────────────────────────

    #[test]
    fn watermark_monotonic_advancement() {
        let arrow = make_schema(vec![("ts", DataType::Int64)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut ext = RowEventTimeExtractor::new(0, FieldType::Int64, 1000);

        let arena = Bump::new();
        let timestamps = [5000_i64, 3000, 7000, 6000, 10_000];
        for &ts in &timestamps {
            let mut row = MutableEventRow::new_in(&arena, &rs, 0);
            row.set_i64(0, ts);
            let row = row.freeze();
            ext.extract(&row);
        }

        // max = 10_000, delay = 1000 → watermark = 9000
        assert_eq!(ext.max_timestamp(), 10_000);
        assert_eq!(ext.watermark(), 9_000);
    }

    #[test]
    fn watermark_delay_applied() {
        let arrow = make_schema(vec![("ts", DataType::Int64)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut ext = RowEventTimeExtractor::new(0, FieldType::Int64, 5000);

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_i64(0, 10_000);
        let row = row.freeze();
        ext.extract(&row);

        assert_eq!(ext.watermark(), 5_000);
    }

    #[test]
    fn watermark_saturates_at_min() {
        let ext = RowEventTimeExtractor::new(0, FieldType::Int64, 1000);
        // No rows observed → max = i64::MIN → watermark saturates at i64::MIN
        assert_eq!(ext.watermark(), i64::MIN);
    }

    // ── Debug ────────────────────────────────────────────────────

    #[test]
    fn debug_format() {
        let ext = RowEventTimeExtractor::new(0, FieldType::Int64, 100);
        let s = format!("{ext:?}");
        assert!(s.contains("RowEventTimeExtractor"));
        assert!(s.contains("field_idx"));
    }
}
