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
use crate::schema::traits::{FormatDecoder, FormatEncoder};
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

    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        let values: Vec<&[u8]> = records
            .iter()
            .map(|record| record.value.as_slice())
            .collect();
        self.decode_slices(&values)
    }

    fn format_name(&self) -> &'static str {
        "csv"
    }
}

impl CsvDecoder {
    /// Decodes borrowed CSV payloads without first copying them into raw records.
    ///
    /// # Errors
    ///
    /// Returns a decode error for malformed CSV or values incompatible with the schema.
    pub fn decode_slices(&self, records: &[&[u8]]) -> SchemaResult<RecordBatch> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let num_fields = self.schema.fields().len();
        let capacity = records.len();

        // Initialize one builder per schema column.
        let mut builders = create_builders(&self.schema, capacity);

        // Concatenate all raw record bytes, ensuring newline separation.
        let mut combined = Vec::with_capacity(records.iter().map(|record| record.len() + 1).sum());
        for record in records {
            combined.extend_from_slice(record);
            if !record.ends_with(b"\n") {
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

/// Configuration for [`CsvEncoder`].
#[derive(Debug, Clone)]
pub struct CsvEncoderConfig {
    /// Field delimiter. Default: `','`.
    pub delimiter: u8,
    /// Whether to include a header row. Default: `false`.
    pub has_header: bool,
}

impl Default for CsvEncoderConfig {
    fn default() -> Self {
        Self {
            delimiter: b',',
            has_header: false,
        }
    }
}

/// Encodes Arrow `RecordBatch`es into CSV byte records via `arrow_csv::writer`.
#[derive(Debug)]
pub struct CsvEncoder {
    schema: SchemaRef,
    config: CsvEncoderConfig,
}

impl CsvEncoder {
    /// Creates a new CSV encoder for the given schema with default config.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self::with_config(schema, CsvEncoderConfig::default())
    }

    /// Creates a new CSV encoder with custom configuration.
    #[must_use]
    pub fn with_config(schema: SchemaRef, config: CsvEncoderConfig) -> Self {
        Self { schema, config }
    }
}

impl FormatEncoder for CsvEncoder {
    fn input_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn encode_batch(&self, batch: &RecordBatch) -> SchemaResult<Vec<Vec<u8>>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        let mut buf = Vec::new();
        {
            let writer = arrow_csv::writer::WriterBuilder::new()
                .with_header(self.config.has_header)
                .with_delimiter(self.config.delimiter);
            let mut csv_writer = writer.build(&mut buf);
            csv_writer
                .write(batch)
                .map_err(|e| SchemaError::DecodeError(format!("CSV encode error: {e}")))?;
        }

        let output: Vec<Vec<u8>> = buf
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .map(<[u8]>::to_vec)
            .collect();

        Ok(output)
    }

    fn format_name(&self) -> &'static str {
        "csv"
    }
}

#[cfg(test)]
mod tests;
