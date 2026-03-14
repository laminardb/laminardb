//! CSV format decoder implementing [`FormatDecoder`].
//!
//! Converts raw CSV byte payloads into Arrow `RecordBatch`es.
//! Constructed once at `CREATE SOURCE` time with a frozen Arrow schema
//! and CSV format configuration. The decoder is stateless after
//! construction so the Ring 1 hot path has zero configuration lookups.
//!
//! Uses the `csv` crate's `ByteRecord` API for zero-copy field access
//! where possible. Type coercion (string → int, string → timestamp, etc.)
//! is performed during the Arrow builder append phase.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
    TimestampNanosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef, TimeUnit};

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::FormatDecoder;
use crate::schema::types::RawRecord;

/// Strategy for rows with incorrect field count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldCountMismatchStrategy {
    /// Pad missing fields with null, ignore extra fields. Default.
    Null,
    /// Skip the malformed row entirely.
    Skip,
    /// Return a decode error on the first malformed row.
    Reject,
}

/// CSV decoder configuration.
///
/// Maps directly to the SQL `FORMAT CSV (...)` options.
/// All fields have sensible defaults matching RFC 4180.
#[derive(Debug, Clone)]
pub struct CsvDecoderConfig {
    /// Field delimiter character. Default: `','` (comma).
    /// Common alternatives: `'\t'` (tab), `'|'` (pipe), `';'` (semicolon).
    pub delimiter: u8,

    /// Quote character for fields containing delimiters or newlines.
    /// Default: `'"'` (double quote). Set to `None` to disable quoting.
    pub quote: Option<u8>,

    /// Escape character within quoted fields.
    /// Default: `None` (RFC 4180 uses doubled quote chars for escaping).
    /// Set to `Some(b'\\')` for backslash-escaped CSVs.
    pub escape: Option<u8>,

    /// Whether the first row is a header row with column names.
    /// Default: `true`.
    pub has_header: bool,

    /// String value to interpret as SQL NULL.
    /// Default: `""` (empty string). Common alternatives: `"NA"`, `"null"`, `"\\N"`.
    pub null_string: String,

    /// Comment line prefix. Lines starting with this character are skipped.
    /// Default: `None` (no comment support).
    pub comment: Option<u8>,

    /// Number of rows to skip at the beginning of the data (after header).
    /// Default: `0`.
    pub skip_rows: usize,

    /// Timestamp format pattern for parsing timestamp columns.
    /// Default: `"%Y-%m-%d %H:%M:%S%.f"`.
    pub timestamp_format: String,

    /// Date format pattern for parsing date columns.
    /// Default: `"%Y-%m-%d"`.
    pub date_format: String,

    /// How to handle rows with wrong number of fields.
    /// Default: `Null` (pad missing fields with null, truncate extra).
    pub field_count_mismatch: FieldCountMismatchStrategy,
}

impl Default for CsvDecoderConfig {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: Some(b'"'),
            escape: None,
            has_header: true,
            null_string: String::new(),
            comment: None,
            skip_rows: 0,
            timestamp_format: "%Y-%m-%d %H:%M:%S%.f".into(),
            date_format: "%Y-%m-%d".into(),
            field_count_mismatch: FieldCountMismatchStrategy::Null,
        }
    }
}

/// Pre-computed coercion strategy for a single CSV column.
#[derive(Debug, Clone)]
enum CsvCoercion {
    /// Parse as boolean (`"true"`/`"false"`, `"1"`/`"0"`, `"yes"`/`"no"`).
    Boolean,
    /// Parse as i64.
    Int64,
    /// Parse as f64.
    Float64,
    /// Parse as `Timestamp(Nanosecond, UTC)` using the configured format.
    Timestamp(String),
    /// Parse as `Date32` using the configured format.
    Date(String),
    /// No coercion needed — keep as UTF-8 string.
    Utf8,
}

/// Decodes CSV byte payloads into Arrow `RecordBatch`es.
///
/// # Ring Placement
///
/// - **Ring 1**: `decode_batch()` — parse CSV, build columnar Arrow output
/// - **Ring 2**: Construction (`new` / `with_config`) — one-time setup
pub struct CsvDecoder {
    /// Frozen output schema.
    schema: SchemaRef,
    /// CSV format configuration.
    config: CsvDecoderConfig,
    /// Per-column type coercion functions, indexed by column position.
    /// Pre-computed at construction time to avoid per-record dispatch.
    coercions: Vec<CsvCoercion>,
    /// Cumulative count of parse errors (for diagnostics).
    parse_error_count: AtomicU64,
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for CsvDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvDecoder")
            .field("schema", &self.schema)
            .field("config", &self.config)
            .field(
                "parse_error_count",
                &self.parse_error_count.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl CsvDecoder {
    /// Creates a new CSV decoder for the given Arrow schema with default config.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self::with_config(schema, CsvDecoderConfig::default())
    }

    /// Creates a new CSV decoder with custom configuration.
    #[must_use]
    pub fn with_config(schema: SchemaRef, config: CsvDecoderConfig) -> Self {
        let coercions: Vec<CsvCoercion> = schema
            .fields()
            .iter()
            .map(|field| Self::coercion_for_type(field.data_type(), &config))
            .collect();

        Self {
            schema,
            config,
            coercions,
            parse_error_count: AtomicU64::new(0),
        }
    }

    /// Returns the cumulative parse error count.
    pub fn parse_error_count(&self) -> u64 {
        self.parse_error_count.load(Ordering::Relaxed)
    }

    /// Determines the coercion strategy for an Arrow data type.
    fn coercion_for_type(data_type: &DataType, config: &CsvDecoderConfig) -> CsvCoercion {
        match data_type {
            DataType::Boolean => CsvCoercion::Boolean,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => CsvCoercion::Int64,
            DataType::Float16 | DataType::Float32 | DataType::Float64 => CsvCoercion::Float64,
            DataType::Timestamp(_, _) => CsvCoercion::Timestamp(config.timestamp_format.clone()),
            DataType::Date32 | DataType::Date64 => CsvCoercion::Date(config.date_format.clone()),
            _ => CsvCoercion::Utf8,
        }
    }

    /// Builds a `csv::ReaderBuilder` from the decoder config.
    fn make_reader_builder(&self) -> csv::ReaderBuilder {
        let mut rb = csv::ReaderBuilder::new();
        rb.delimiter(self.config.delimiter)
            .has_headers(false) // We handle headers ourselves
            .flexible(true); // Allow variable field counts

        if let Some(q) = self.config.quote {
            rb.quote(q);
        }
        if let Some(e) = self.config.escape {
            rb.escape(Some(e));
        }
        if let Some(c) = self.config.comment {
            rb.comment(Some(c));
        }

        rb
    }
}

impl FormatDecoder for CsvDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Decodes a batch of raw CSV records into an Arrow `RecordBatch`.
    ///
    /// Each `RawRecord.value` contains one or more CSV lines (typically one
    /// line per record for streaming sources; may contain multiple lines
    /// for file-based sources).
    ///
    /// # Algorithm
    ///
    /// 1. Initialize one Arrow `ArrayBuilder` per schema column.
    /// 2. Concatenate all raw record bytes into a single buffer.
    /// 3. Create a `csv::Reader` with pre-configured settings.
    /// 4. For each CSV row:
    ///    - Skip rows per `skip_rows` config.
    ///    - For each field: apply the pre-computed `CsvCoercion`.
    ///    - Handle field count mismatches per config.
    /// 5. Finish all builders and assemble into `RecordBatch`.
    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let num_fields = self.schema.fields().len();
        let capacity = records.len();

        // Initialize one builder per schema column.
        let mut builders = create_builders(&self.schema, capacity);

        // Concatenate all raw record bytes, ensuring newline separation.
        let mut combined = Vec::with_capacity(records.iter().map(|r| r.value.len() + 1).sum());
        for record in records {
            combined.extend_from_slice(&record.value);
            if !record.value.ends_with(b"\n") {
                combined.push(b'\n');
            }
        }

        let rb = self.make_reader_builder();
        let mut reader = rb.from_reader(combined.as_slice());

        let mut rows_skipped = 0usize;
        let mut header_skipped = false;
        let mut row_count = 0usize;

        let mut byte_record = csv::ByteRecord::new();
        while reader
            .read_byte_record(&mut byte_record)
            .map_err(|e| SchemaError::DecodeError(format!("CSV parse error: {e}")))?
        {
            // Skip header row if configured.
            if self.config.has_header && !header_skipped {
                header_skipped = true;
                continue;
            }

            // Skip initial data rows per config.
            if rows_skipped < self.config.skip_rows {
                rows_skipped += 1;
                continue;
            }

            let field_count = byte_record.len();

            // Handle field count mismatch.
            if field_count != num_fields {
                match self.config.field_count_mismatch {
                    FieldCountMismatchStrategy::Reject => {
                        return Err(SchemaError::DecodeError(format!(
                            "field count mismatch: expected {num_fields}, got {field_count}"
                        )));
                    }
                    FieldCountMismatchStrategy::Skip => {
                        self.parse_error_count.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    FieldCountMismatchStrategy::Null => {
                        // Will pad/truncate below.
                    }
                }
            }

            // Process each column.
            for col_idx in 0..num_fields {
                if col_idx >= field_count {
                    // Missing field — append null.
                    append_null(&mut builders[col_idx]);
                    continue;
                }

                let raw_field = &byte_record[col_idx];
                let field_str = std::str::from_utf8(raw_field).unwrap_or("");
                let trimmed = field_str.trim();

                // Check for null string.
                if trimmed == self.config.null_string {
                    append_null(&mut builders[col_idx]);
                    continue;
                }

                // Apply coercion.
                let ok = append_coerced(&mut builders[col_idx], &self.coercions[col_idx], trimmed);

                if !ok {
                    self.parse_error_count.fetch_add(1, Ordering::Relaxed);
                    append_null(&mut builders[col_idx]);
                }
            }

            row_count += 1;
        }

        // If no data rows were processed, return empty batch.
        if row_count == 0 {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // Finish all builders into arrays.
        let columns: Vec<ArrayRef> = builders.into_iter().map(|mut b| b.finish()).collect();

        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| SchemaError::DecodeError(format!("RecordBatch construction: {e}")))
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn format_name(&self) -> &str {
        "csv"
    }
}

// ── Builder helpers ────────────────────────────────────────────────

/// Trait-object wrapper so we can store heterogeneous builders in a `Vec`.
trait ColumnBuilder: Send {
    fn finish(&mut self) -> ArrayRef;
    fn append_null_value(&mut self);
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

macro_rules! impl_column_builder {
    ($builder:ty) => {
        impl ColumnBuilder for $builder {
            fn finish(&mut self) -> ArrayRef {
                Arc::new(<$builder>::finish(self))
            }
            fn append_null_value(&mut self) {
                self.append_null();
            }
            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }
        }
    };
}

impl_column_builder!(BooleanBuilder);
impl_column_builder!(Int64Builder);
impl_column_builder!(Float64Builder);
impl_column_builder!(StringBuilder);
impl_column_builder!(TimestampNanosecondBuilder);
impl_column_builder!(Date32Builder);

fn create_builders(schema: &SchemaRef, capacity: usize) -> Vec<Box<dyn ColumnBuilder>> {
    schema
        .fields()
        .iter()
        .map(|f| create_builder(f.data_type(), capacity))
        .collect()
}

fn create_builder(data_type: &DataType, capacity: usize) -> Box<dyn ColumnBuilder> {
    match data_type {
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            Box::new(Float64Builder::with_capacity(capacity))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let builder =
                TimestampNanosecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone());
            Box::new(builder)
        }
        DataType::Date32 | DataType::Date64 => Box::new(Date32Builder::with_capacity(capacity)),
        // Fallback: store as UTF-8 string.
        _ => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
    }
}

fn append_null(builder: &mut Box<dyn ColumnBuilder>) {
    builder.append_null_value();
}

/// Appends a coerced value to the appropriate builder. Returns `true` on
/// success, `false` if the value could not be parsed.
fn append_coerced(
    builder: &mut Box<dyn ColumnBuilder>,
    coercion: &CsvCoercion,
    value: &str,
) -> bool {
    match coercion {
        CsvCoercion::Boolean => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            match value.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "t" | "y" => {
                    b.append_value(true);
                    true
                }
                "false" | "0" | "no" | "f" | "n" => {
                    b.append_value(false);
                    true
                }
                _ => false,
            }
        }
        CsvCoercion::Int64 => {
            let b = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            match value.parse::<i64>() {
                Ok(v) => {
                    b.append_value(v);
                    true
                }
                Err(_) => false,
            }
        }
        CsvCoercion::Float64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap();
            match value.parse::<f64>() {
                Ok(v) => {
                    b.append_value(v);
                    true
                }
                Err(_) => false,
            }
        }
        CsvCoercion::Timestamp(fmt) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampNanosecondBuilder>()
                .unwrap();
            // Try the configured format first.
            if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(value, fmt) {
                let nanos = ndt.and_utc().timestamp_nanos_opt().unwrap_or(0);
                b.append_value(nanos);
                return true;
            }
            // Try ISO 8601 fallback.
            if let Ok(nanos) = arrow_cast::parse::string_to_timestamp_nanos(value) {
                b.append_value(nanos);
                return true;
            }
            false
        }
        CsvCoercion::Date(fmt) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .unwrap();
            if let Ok(date) = chrono::NaiveDate::parse_from_str(value, fmt) {
                // Date32 stores days since epoch (1970-01-01).
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days = (date - epoch).num_days();
                #[allow(clippy::cast_possible_truncation)]
                {
                    b.append_value(days as i32);
                }
                return true;
            }
            false
        }
        CsvCoercion::Utf8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            b.append_value(value);
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_schema::{Field, Schema};

    fn make_schema(fields: Vec<(&str, DataType, bool)>) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt, nullable)| Field::new(name, dt, nullable))
                .collect::<Vec<_>>(),
        ))
    }

    fn csv_record(line: &str) -> RawRecord {
        RawRecord::new(line.as_bytes().to_vec())
    }

    fn csv_block(lines: &str) -> RawRecord {
        RawRecord::new(lines.as_bytes().to_vec())
    }

    // ── Basic decode tests ────────────────────────────────────

    #[test]
    fn test_decode_empty_batch() {
        let schema = make_schema(vec![("id", DataType::Int64, false)]);
        let decoder = CsvDecoder::new(schema.clone());
        let batch = decoder.decode_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), schema);
    }

    #[test]
    fn test_decode_single_row_with_header() {
        let schema = make_schema(vec![
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);
        let decoder = CsvDecoder::new(schema);
        let records = vec![csv_block("id,name\n42,Alice")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            42
        );
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "Alice");
    }

    #[test]
    fn test_decode_multiple_rows() {
        let schema = make_schema(vec![
            ("x", DataType::Int64, false),
            ("y", DataType::Float64, false),
        ]);
        let decoder = CsvDecoder::new(schema);
        let records = vec![csv_block("x,y\n1,1.5\n2,2.5\n3,3.5")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 3);
        let x_col = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(x_col.value(0), 1);
        assert_eq!(x_col.value(1), 2);
        assert_eq!(x_col.value(2), 3);
    }

    #[test]
    fn test_decode_all_types() {
        let schema = make_schema(vec![
            ("bool_col", DataType::Boolean, false),
            ("int_col", DataType::Int64, false),
            ("float_col", DataType::Float64, false),
            ("str_col", DataType::Utf8, false),
        ]);
        let decoder = CsvDecoder::new(schema);
        let records = vec![csv_block(
            "bool_col,int_col,float_col,str_col\ntrue,42,3.14,hello",
        )];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column(0).as_boolean().value(0));
        assert_eq!(
            batch
                .column(1)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            42
        );
        let f = batch
            .column(2)
            .as_primitive::<arrow_array::types::Float64Type>()
            .value(0);
        assert!((f - 3.14).abs() < f64::EPSILON);
        assert_eq!(batch.column(3).as_string::<i32>().value(0), "hello");
    }

    // ── Null handling ─────────────────────────────────────────

    #[test]
    fn test_decode_null_string_default() {
        // Default null_string is empty string.
        let schema = make_schema(vec![
            ("a", DataType::Int64, true),
            ("b", DataType::Utf8, true),
        ]);
        let decoder = CsvDecoder::new(schema);
        let records = vec![csv_block("a,b\n,")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert!(batch.column(0).is_null(0));
        assert!(batch.column(1).is_null(0));
    }

    #[test]
    fn test_decode_null_string_custom() {
        let schema = make_schema(vec![("val", DataType::Int64, true)]);
        let config = CsvDecoderConfig {
            null_string: "NA".into(),
            ..Default::default()
        };
        let decoder = CsvDecoder::with_config(schema, config);
        let records = vec![csv_block("val\nNA\n42")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert!(batch.column(0).is_null(0));
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(1),
            42
        );
    }

    // ── Field count mismatch strategies ───────────────────────

    #[test]
    fn test_mismatch_null_strategy() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, true),
            ("b", DataType::Utf8, true),
            ("c", DataType::Int64, true),
        ]);
        let decoder = CsvDecoder::new(schema);
        // Row only has 2 fields, schema expects 3.
        let records = vec![csv_block("a,b,c\n1,hello")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            1
        );
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "hello");
        assert!(batch.column(2).is_null(0)); // padded with null
    }

    #[test]
    fn test_mismatch_skip_strategy() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Int64, false),
        ]);
        let config = CsvDecoderConfig {
            field_count_mismatch: FieldCountMismatchStrategy::Skip,
            ..Default::default()
        };
        let decoder = CsvDecoder::with_config(schema, config);
        // One good row, one bad row (too few fields).
        let records = vec![csv_block("a,b\n1,2\n3")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 1); // bad row skipped
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            1
        );
    }

    #[test]
    fn test_mismatch_reject_strategy() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Int64, false),
        ]);
        let config = CsvDecoderConfig {
            field_count_mismatch: FieldCountMismatchStrategy::Reject,
            ..Default::default()
        };
        let decoder = CsvDecoder::with_config(schema, config);
        let records = vec![csv_block("a,b\n1")]; // too few fields
        let result = decoder.decode_batch(&records);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("field count mismatch"));
    }

    // ── Delimiter options ─────────────────────────────────────

    #[test]
    fn test_pipe_delimiter() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Utf8, false),
        ]);
        let config = CsvDecoderConfig {
            delimiter: b'|',
            ..Default::default()
        };
        let decoder = CsvDecoder::with_config(schema, config);
        let records = vec![csv_block("a|b\n42|hello")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            42
        );
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "hello");
    }

    #[test]
    fn test_tab_delimiter() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Utf8, false),
        ]);
        let config = CsvDecoderConfig {
            delimiter: b'\t',
            ..Default::default()
        };
        let decoder = CsvDecoder::with_config(schema, config);
        let records = vec![csv_block("a\tb\n42\thello")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            42
        );
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "hello");
    }

    #[test]
    fn test_semicolon_delimiter() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Utf8, false),
        ]);
        let config = CsvDecoderConfig {
            delimiter: b';',
            ..Default::default()
        };
        let decoder = CsvDecoder::with_config(schema, config);
        let records = vec![csv_block("a;b\n99;world")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            99
        );
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "world");
    }

    // ── Comment lines ─────────────────────────────────────────

    #[test]
    fn test_comment_lines() {
        let schema = make_schema(vec![("val", DataType::Int64, false)]);
        let config = CsvDecoderConfig {
            comment: Some(b'#'),
            ..Default::default()
        };
        let decoder = CsvDecoder::with_config(schema, config);
        let records = vec![csv_block("val\n# this is a comment\n42\n# another\n99")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 2);
        let col = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(col.value(0), 42);
        assert_eq!(col.value(1), 99);
    }

    // ── Skip rows ─────────────────────────────────────────────

    #[test]
    fn test_skip_rows() {
        let schema = make_schema(vec![("val", DataType::Int64, false)]);
        let config = CsvDecoderConfig {
            skip_rows: 2,
            ..Default::default()
        };
        let decoder = CsvDecoder::with_config(schema, config);
        let records = vec![csv_block("val\nskip1\nskip2\n42\n99")];
        let batch = decoder.decode_batch(&records).unwrap();

        // "skip1" and "skip2" are skipped (parse errors counted), then 42 and 99.
        // Actually skip_rows skips first N data rows; skip1/skip2 aren't valid i64
        // so they'd be parse errors. But the skip_rows logic skips before type
        // coercion, so they won't generate errors.
        assert_eq!(batch.num_rows(), 2);
    }

    // ── No header mode ────────────────────────────────────────

    #[test]
    fn test_no_header() {
        let schema = make_schema(vec![
            ("col0", DataType::Int64, false),
            ("col1", DataType::Utf8, false),
        ]);
        let config = CsvDecoderConfig {
            has_header: false,
            ..Default::default()
        };
        let decoder = CsvDecoder::with_config(schema, config);
        let records = vec![csv_block("1,alpha\n2,beta")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 2);
        let col0 = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(col0.value(0), 1);
        assert_eq!(col0.value(1), 2);
    }

    // ── Multiple records (streaming) ──────────────────────────

    #[test]
    fn test_multiple_raw_records() {
        // Simulate streaming: each RawRecord is one CSV line (no header).
        let schema = make_schema(vec![
            ("id", DataType::Int64, false),
            ("val", DataType::Float64, false),
        ]);
        let config = CsvDecoderConfig {
            has_header: false,
            ..Default::default()
        };
        let decoder = CsvDecoder::with_config(schema, config);
        let records = vec![
            csv_record("1,1.5"),
            csv_record("2,2.5"),
            csv_record("3,3.5"),
        ];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 3);
        let id_col = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        let val_col = batch
            .column(1)
            .as_primitive::<arrow_array::types::Float64Type>();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(2), 3);
        assert!((val_col.value(1) - 2.5).abs() < f64::EPSILON);
    }

    // ── Quoted fields ─────────────────────────────────────────

    #[test]
    fn test_quoted_fields_with_delimiter() {
        let schema = make_schema(vec![
            ("name", DataType::Utf8, false),
            ("desc", DataType::Utf8, false),
        ]);
        let decoder = CsvDecoder::new(schema);
        let records = vec![csv_block("name,desc\n\"Smith, John\",\"A, B\"")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(0).as_string::<i32>().value(0), "Smith, John");
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "A, B");
    }

    #[test]
    fn test_quoted_fields_with_newline() {
        let schema = make_schema(vec![
            ("id", DataType::Int64, false),
            ("text", DataType::Utf8, false),
        ]);
        let decoder = CsvDecoder::new(schema);
        let records = vec![csv_block("id,text\n1,\"line1\nline2\"")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "line1\nline2");
    }

    #[test]
    fn test_escaped_quotes_rfc4180() {
        // RFC 4180: doubled quotes within quoted field.
        let schema = make_schema(vec![("val", DataType::Utf8, false)]);
        let decoder = CsvDecoder::new(schema);
        let records = vec![csv_block("val\n\"She said \"\"hello\"\"\"")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch.column(0).as_string::<i32>().value(0),
            "She said \"hello\""
        );
    }

    // ── Timestamp parsing ─────────────────────────────────────

    #[test]
    fn test_decode_timestamp() {
        let schema = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        )]);
        let decoder = CsvDecoder::new(schema);
        let records = vec![csv_block("ts\n2025-01-15 10:30:00.000")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert!(!batch.column(0).is_null(0));
    }

    #[test]
    fn test_decode_timestamp_iso8601_fallback() {
        let schema = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]);
        let decoder = CsvDecoder::new(schema);
        let records = vec![csv_block("ts\n2025-01-15T10:30:00Z")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert!(!batch.column(0).is_null(0));
    }

    // ── Date parsing ──────────────────────────────────────────

    #[test]
    fn test_decode_date() {
        let schema = make_schema(vec![("d", DataType::Date32, false)]);
        let decoder = CsvDecoder::new(schema);
        let records = vec![csv_block("d\n2025-06-15")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert!(!batch.column(0).is_null(0));
        // 2025-06-15 is day 20254 since epoch.
        let days = batch
            .column(0)
            .as_primitive::<arrow_array::types::Date32Type>()
            .value(0);
        let expected = chrono::NaiveDate::from_ymd_opt(2025, 6, 15)
            .unwrap()
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days();
        #[allow(clippy::cast_possible_truncation)]
        {
            assert_eq!(days, expected as i32);
        }
    }

    // ── Boolean parsing ───────────────────────────────────────

    #[test]
    fn test_decode_boolean_variants() {
        let schema = make_schema(vec![("b", DataType::Boolean, false)]);
        let config = CsvDecoderConfig {
            has_header: false,
            ..Default::default()
        };
        let decoder = CsvDecoder::with_config(schema, config);
        let records = vec![csv_block("true\nfalse\n1\n0\nyes\nno\nt\nf\ny\nn")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 10);
        let col = batch.column(0).as_boolean();
        assert!(col.value(0)); // true
        assert!(!col.value(1)); // false
        assert!(col.value(2)); // 1
        assert!(!col.value(3)); // 0
        assert!(col.value(4)); // yes
        assert!(!col.value(5)); // no
        assert!(col.value(6)); // t
        assert!(!col.value(7)); // f
        assert!(col.value(8)); // y
        assert!(!col.value(9)); // n
    }

    // ── Parse error counting ──────────────────────────────────

    #[test]
    fn test_parse_error_count() {
        let schema = make_schema(vec![("val", DataType::Int64, true)]);
        let decoder = CsvDecoder::new(schema);
        let records = vec![csv_block("val\nnot_a_number\n42\nalso_bad")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert!(batch.column(0).is_null(0));
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(1),
            42
        );
        assert!(batch.column(0).is_null(2));
        assert_eq!(decoder.parse_error_count(), 2);
    }

    // ── Extra fields ignored ──────────────────────────────────

    #[test]
    fn test_extra_fields_truncated() {
        let schema = make_schema(vec![("a", DataType::Int64, false)]);
        let decoder = CsvDecoder::new(schema);
        // Row has 3 fields but schema only has 1.
        let records = vec![csv_block("a\n42,extra1,extra2")];
        let batch = decoder.decode_batch(&records).unwrap();

        // Extra fields silently ignored (flexible mode).
        // field_count (3) != num_fields (1), but Null strategy just pads/truncates.
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            42
        );
    }

    // ── FormatDecoder trait ───────────────────────────────────

    #[test]
    fn test_format_name() {
        let schema = make_schema(vec![("a", DataType::Int64, false)]);
        let decoder = CsvDecoder::new(schema);
        assert_eq!(decoder.format_name(), "csv");
    }

    #[test]
    fn test_output_schema() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Utf8, true),
        ]);
        let decoder = CsvDecoder::new(schema.clone());
        assert_eq!(decoder.output_schema(), schema);
    }

    #[test]
    fn test_decode_one() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);
        let config = CsvDecoderConfig {
            has_header: false,
            ..Default::default()
        };
        let decoder = CsvDecoder::with_config(schema, config);
        let record = csv_record("99");
        let batch = decoder.decode_one(&record).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            99
        );
    }

    // ── Edge cases ────────────────────────────────────────────

    #[test]
    fn test_mixed_line_endings() {
        let schema = make_schema(vec![("val", DataType::Int64, false)]);
        let config = CsvDecoderConfig {
            has_header: false,
            ..Default::default()
        };
        let decoder = CsvDecoder::with_config(schema, config);
        let records = vec![csv_block("1\r\n2\n3\r\n")];
        let batch = decoder.decode_batch(&records).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_unicode_values() {
        let schema = make_schema(vec![("name", DataType::Utf8, false)]);
        let decoder = CsvDecoder::new(schema);
        let records = vec![csv_block("name\nこんにちは\nüber\nnaïve")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.column(0).as_string::<i32>().value(0), "こんにちは");
        assert_eq!(batch.column(0).as_string::<i32>().value(1), "über");
        assert_eq!(batch.column(0).as_string::<i32>().value(2), "naïve");
    }

    #[test]
    fn test_trailing_comma() {
        // Trailing comma creates an extra empty field.
        let schema = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Int64, true),
        ]);
        let decoder = CsvDecoder::new(schema);
        let records = vec![csv_block("a,b\n1,")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            1
        );
        // Empty string matches default null_string → null.
        assert!(batch.column(1).is_null(0));
    }

    #[test]
    fn test_backslash_escape() {
        let schema = make_schema(vec![("val", DataType::Utf8, false)]);
        let config = CsvDecoderConfig {
            escape: Some(b'\\'),
            ..Default::default()
        };
        let decoder = CsvDecoder::with_config(schema, config);
        let records = vec![csv_block("val\n\"hello \\\"world\\\"\"")];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch.column(0).as_string::<i32>().value(0),
            "hello \"world\""
        );
    }
}
