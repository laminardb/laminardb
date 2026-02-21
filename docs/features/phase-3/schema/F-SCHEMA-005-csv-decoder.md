# F-SCHEMA-005: CSV Format Decoder & Inference

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-005 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-SCHEMA-001 (FormatDecoder/FormatEncoder traits), F-SCHEMA-003 (FormatInference registry) |
| **Blocks** | F-CONN-005 (Kafka CSV integration), File CSV source |
| **Owner** | TBD |
| **Created** | 2026-02-21 |
| **Crate** | `laminar-formats` |
| **Module** | `laminar-formats/src/csv/decoder.rs`, `laminar-formats/src/csv/inference.rs` |

## Summary

Implement the CSV format decoder (`CsvDecoder`) and inference engine (`CsvFormatInference`) for LaminarDB. The decoder converts raw CSV byte payloads into Arrow RecordBatches using the `csv` crate (BurntSushi) for zero-copy parsing on the Ring 1 path. The inference engine follows DuckDB's type detection approach: for each column, it attempts to parse all sampled values in a fixed type priority order (BOOLEAN -> BIGINT -> DOUBLE -> TIMESTAMP -> DATE -> TIME -> VARCHAR), and the narrowest type that successfully parses every value wins. CSV format options (delimiter, quote character, escape character, header row, null string, comment prefix, timestamp format, skip rows) are fully configurable via SQL DDL `FORMAT CSV (...)` syntax.

## Goals

- `CsvDecoder` implementing `FormatDecoder` with batch-oriented CSV-to-Arrow conversion
- `CsvFormatInference` implementing `FormatInference` with DuckDB-style column type detection
- Zero-copy CSV parsing via the `csv` crate with `ByteRecord` API
- Full `FORMAT CSV (...)` SQL option mapping: delimiter, quote, escape, has_header, null_string, comment, timestamp_format, skip_rows
- Configurable batch size with columnar Arrow output
- Support for multi-byte delimiters (tab, pipe, semicolon, custom)
- Header row auto-detection when not explicitly specified
- Proper handling of quoted fields, embedded newlines, and escaped quotes

## Non-Goals

- CSV encoding for sinks (`CsvEncoder`) -- separate feature
- Multi-file CSV source with directory watching (handled by File source connector)
- CSV schema evolution (CSV has no schema versioning)
- Schema registry integration (CSV does not support schema registries)
- Compression handling (gzip, zstd) -- handled by the source connector layer
- Excel/TSV-specific parsing (TSV is just CSV with `delimiter = '\t'`)

## Technical Design

### Architecture

```
                    Ring 2 (One-Time)
                    ┌─────────────────────────────────────────────┐
                    │  CsvFormatInference                          │
                    │  ┌─────────────┐   ┌──────────────────────┐ │
                    │  │ Sample      │──>│ DuckDB-Style Type    │ │
                    │  │ CSV Records │   │ Detection Engine     │ │
                    │  └─────────────┘   └──────────┬───────────┘ │
                    │                               │              │
                    │                    ┌──────────▼───────────┐  │
                    │                    │ InferredSchema        │  │
                    │                    │ (Arrow SchemaRef)     │  │
                    │                    └──────────┬───────────┘  │
                    └───────────────────────────────┼──────────────┘
                                                    │ frozen
                    Ring 1 (Per-Batch)               │
                    ┌───────────────────────────────┼──────────────┐
                    │  CsvDecoder                    ▼              │
                    │  ┌────────────────┐   ┌──────────────────┐  │
                    │  │ csv::Reader    │──>│ Arrow Builder     │  │
                    │  │ (ByteRecord)   │   │ (columnar output) │  │
                    │  └────────────────┘   └──────────┬───────┘  │
                    │                                  │           │
                    │                       ┌──────────▼───────┐  │
                    │                       │ RecordBatch       │  │
                    │                       └──────────────────┘  │
                    └─────────────────────────────────────────────┘
```

### Ring Integration

| Ring | Operation | Latency Budget | Component |
|------|-----------|----------------|-----------|
| Ring 0 | Arrow column access (pre-decoded) | <50ns | Standard Arrow access |
| Ring 1 | CSV parse + Arrow build (per batch) | <500us / 1024 records | `CsvDecoder::decode_batch()` |
| Ring 1 | CSV field parsing + type coercion | <200ns per field | `csv` crate `ByteRecord` |
| Ring 2 | Schema inference (one-time) | Seconds | `CsvFormatInference::infer()` |
| Ring 2 | Decoder construction (one-time) | Microseconds | `CsvDecoder::new()` |
| Ring 2 | Header detection (one-time) | Milliseconds | `CsvFormatInference::detect_header()` |

### API Design

#### CsvDecoder

```rust
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// CSV decoder configuration.
///
/// Maps directly to the SQL `FORMAT CSV (...)` options.
/// All fields have sensible defaults matching RFC 4180.
#[derive(Debug, Clone)]
pub struct CsvDecoderConfig {
    /// Field delimiter character. Default: ',' (comma).
    /// Common alternatives: '\t' (tab), '|' (pipe), ';' (semicolon).
    pub delimiter: u8,

    /// Quote character for fields containing delimiters or newlines.
    /// Default: '"' (double quote). Set to None to disable quoting.
    pub quote: Option<u8>,

    /// Escape character within quoted fields.
    /// Default: None (RFC 4180 uses doubled quote chars for escaping).
    /// Set to Some(b'\\') for backslash-escaped CSVs.
    pub escape: Option<u8>,

    /// Whether the first row is a header row with column names.
    /// Default: true.
    pub has_header: bool,

    /// String value to interpret as SQL NULL.
    /// Default: "" (empty string). Common alternatives: "NA", "null", "\\N".
    pub null_string: String,

    /// Comment line prefix. Lines starting with this character are skipped.
    /// Default: None (no comment support).
    pub comment: Option<u8>,

    /// Number of rows to skip at the beginning of the data (after header).
    /// Default: 0.
    pub skip_rows: usize,

    /// Timestamp format pattern for parsing timestamp columns.
    /// Default: "%Y-%m-%d %H:%M:%S%.f".
    /// The inference engine tries multiple patterns; this is for explicit schema.
    pub timestamp_format: String,

    /// Date format pattern for parsing date columns.
    /// Default: "%Y-%m-%d".
    pub date_format: String,

    /// Time format pattern for parsing time columns.
    /// Default: "%H:%M:%S%.f".
    pub time_format: String,

    /// Maximum number of records per decoded batch.
    /// Default: 1024.
    pub batch_size: usize,

    /// How to handle rows with wrong number of fields.
    /// Default: Null (pad missing fields with null, truncate extra).
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
            time_format: "%H:%M:%S%.f".into(),
            batch_size: 1024,
            field_count_mismatch: FieldCountMismatchStrategy::Null,
        }
    }
}

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

/// Decodes CSV byte payloads into Arrow RecordBatches.
///
/// Constructed once at CREATE SOURCE time with a frozen Arrow schema
/// and CSV format configuration. The decoder is stateless after
/// construction -- all schema and format information is baked in
/// so the Ring 1 path has zero configuration lookups.
///
/// Uses the `csv` crate's `ByteRecord` API for zero-copy field access
/// where possible. Type coercion (string -> int, string -> timestamp, etc.)
/// is performed during the Arrow builder append phase.
#[derive(Debug)]
pub struct CsvDecoder {
    /// The frozen output schema.
    schema: SchemaRef,

    /// CSV format configuration.
    config: CsvDecoderConfig,

    /// Pre-compiled csv::ReaderBuilder for reuse across batches.
    /// Encodes delimiter, quote, escape, comment settings.
    reader_builder: csv::ReaderBuilder,

    /// Pre-compiled timestamp parser (chrono format string).
    timestamp_parser: TimestampParser,

    /// Per-column type coercion functions, indexed by column position.
    /// Pre-computed at construction time to avoid per-record dispatch.
    coercions: Vec<CsvCoercion>,

    /// Cumulative count of parse errors (for diagnostics).
    parse_error_count: std::sync::atomic::AtomicU64,
}

/// Pre-computed coercion strategy for a single CSV column.
#[derive(Debug, Clone)]
enum CsvCoercion {
    /// Parse as boolean ("true"/"false", "1"/"0", "yes"/"no").
    Boolean,
    /// Parse as i64.
    Int64,
    /// Parse as f64.
    Float64,
    /// Parse as Timestamp(Nanosecond, UTC) using the configured format.
    Timestamp(String),
    /// Parse as Date32 using the configured format.
    Date(String),
    /// Parse as Time64(Nanosecond) using the configured format.
    Time(String),
    /// No coercion needed -- keep as UTF-8 string.
    Utf8,
}

impl CsvDecoder {
    /// Create a new CSV decoder for the given Arrow schema.
    pub fn new(schema: SchemaRef, config: CsvDecoderConfig) -> Self {
        let mut reader_builder = csv::ReaderBuilder::new();
        reader_builder
            .delimiter(config.delimiter)
            .has_headers(false) // We handle headers ourselves
            .flexible(true);   // Allow variable field counts

        if let Some(q) = config.quote {
            reader_builder.quote(q);
        }
        if let Some(e) = config.escape {
            reader_builder.escape(Some(e));
        }
        if let Some(c) = config.comment {
            reader_builder.comment(Some(c));
        }

        let coercions: Vec<CsvCoercion> = schema.fields().iter()
            .map(|field| Self::coercion_for_type(field.data_type(), &config))
            .collect();

        let timestamp_parser = TimestampParser::new(&config.timestamp_format);

        Self {
            schema,
            config,
            reader_builder,
            timestamp_parser,
            coercions,
            parse_error_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Determine the coercion strategy for an Arrow data type.
    fn coercion_for_type(data_type: &DataType, config: &CsvDecoderConfig) -> CsvCoercion {
        match data_type {
            DataType::Boolean => CsvCoercion::Boolean,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                CsvCoercion::Int64
            }
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                CsvCoercion::Float64
            }
            DataType::Timestamp(_, _) => {
                CsvCoercion::Timestamp(config.timestamp_format.clone())
            }
            DataType::Date32 | DataType::Date64 => {
                CsvCoercion::Date(config.date_format.clone())
            }
            DataType::Time32(_) | DataType::Time64(_) => {
                CsvCoercion::Time(config.time_format.clone())
            }
            _ => CsvCoercion::Utf8,
        }
    }

    /// Return the cumulative parse error count.
    pub fn parse_error_count(&self) -> u64 {
        self.parse_error_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl FormatDecoder for CsvDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Decode a batch of raw CSV records into an Arrow RecordBatch.
    ///
    /// Each RawRecord.value contains one or more CSV lines (typically one
    /// line per record for streaming sources; may contain multiple lines
    /// for file-based sources).
    ///
    /// # Algorithm
    ///
    /// 1. Initialize one Arrow ArrayBuilder per schema column.
    /// 2. For each raw record:
    ///    a. Create a csv::Reader over the raw bytes.
    ///    b. For each CSV row:
    ///       - Skip rows per skip_rows config.
    ///       - For each field (by column index):
    ///         * If field value equals null_string: append null.
    ///         * Else: apply the pre-computed CsvCoercion for this column.
    ///         * On parse failure: apply FieldCountMismatchStrategy.
    ///       - Handle row field count mismatches per config.
    /// 3. Finish all builders and assemble into RecordBatch.
    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        // Implementation: parse CSV lines, coerce to Arrow types, build RecordBatch
        todo!()
    }

    fn format_name(&self) -> &str {
        "csv"
    }
}

/// Pre-compiled timestamp parser for efficient repeated parsing.
#[derive(Debug)]
struct TimestampParser {
    format: String,
}

impl TimestampParser {
    fn new(format: &str) -> Self {
        Self { format: format.to_string() }
    }

    /// Parse a timestamp string into nanoseconds since epoch.
    fn parse_nanos(&self, s: &str) -> Option<i64> {
        chrono::NaiveDateTime::parse_from_str(s, &self.format)
            .ok()
            .map(|dt| dt.and_utc().timestamp_nanos_opt().unwrap_or(0))
    }
}
```

#### CsvFormatInference

```rust
use std::collections::HashMap;

/// CSV-specific type inference engine.
///
/// Implements the `FormatInference` trait using DuckDB's type detection
/// approach: for each column, attempt to parse ALL sampled values through
/// a fixed type priority chain. The narrowest type that succeeds for
/// every non-null value wins.
///
/// # Type Detection Priority Chain
///
/// ```text
/// BOOLEAN -> BIGINT -> DOUBLE -> TIMESTAMP -> DATE -> TIME -> VARCHAR
/// ```
///
/// Each type is tested by attempting to parse every sampled value for
/// that column. If all values parse successfully, that type is chosen.
/// If any value fails, the next wider type is tried. VARCHAR always
/// succeeds as the terminal fallback.
#[derive(Debug)]
pub struct CsvFormatInference;

/// CSV-specific inference configuration, extending the base InferenceConfig.
#[derive(Debug, Clone)]
pub struct CsvInferenceConfig {
    /// Base inference settings (sample_size, timeout, etc.).
    pub base: InferenceConfig,

    /// Delimiter to use when parsing samples.
    /// If None, auto-detect from the first few lines.
    pub delimiter: Option<u8>,

    /// Whether to auto-detect the presence of a header row.
    /// Default: true.
    pub detect_header: bool,

    /// Timestamp format patterns to try during inference.
    /// Tried in order; first pattern where ALL values parse wins.
    pub timestamp_formats: Vec<String>,

    /// Date format patterns to try during inference.
    pub date_formats: Vec<String>,

    /// Time format patterns to try during inference.
    pub time_formats: Vec<String>,

    /// String values to treat as boolean true.
    /// Default: ["true", "1", "yes", "t", "y"].
    pub true_values: Vec<String>,

    /// String values to treat as boolean false.
    /// Default: ["false", "0", "no", "f", "n"].
    pub false_values: Vec<String>,

    /// String values to treat as null.
    /// Default: ["", "NA", "null", "NULL", "None", "\\N"].
    pub null_values: Vec<String>,
}

impl Default for CsvInferenceConfig {
    fn default() -> Self {
        Self {
            base: InferenceConfig::default(),
            delimiter: None,
            detect_header: true,
            timestamp_formats: vec![
                "%Y-%m-%d %H:%M:%S%.f".into(),
                "%Y-%m-%dT%H:%M:%S%.fZ".into(),
                "%Y-%m-%dT%H:%M:%S%.f%:z".into(),
                "%Y-%m-%d %H:%M:%S".into(),
                "%m/%d/%Y %H:%M:%S".into(),
                "%d/%m/%Y %H:%M:%S".into(),
            ],
            date_formats: vec![
                "%Y-%m-%d".into(),
                "%m/%d/%Y".into(),
                "%d/%m/%Y".into(),
                "%Y/%m/%d".into(),
            ],
            time_formats: vec![
                "%H:%M:%S%.f".into(),
                "%H:%M:%S".into(),
                "%H:%M".into(),
            ],
            true_values: vec![
                "true".into(), "1".into(), "yes".into(), "t".into(), "y".into(),
            ],
            false_values: vec![
                "false".into(), "0".into(), "no".into(), "f".into(), "n".into(),
            ],
            null_values: vec![
                String::new(), "NA".into(), "null".into(), "NULL".into(),
                "None".into(), "\\N".into(),
            ],
        }
    }
}

/// Per-column inference state collected during type detection.
#[derive(Debug)]
struct ColumnDetection {
    /// Column name (from header or generated: col_0, col_1, ...).
    name: String,
    /// All non-null string values observed for this column.
    values: Vec<String>,
    /// Number of null values observed.
    null_count: usize,
    /// Total number of rows observed.
    total_count: usize,
}

impl FormatInference for CsvFormatInference {
    fn format_name(&self) -> &str {
        "csv"
    }

    /// Infer an Arrow schema from sampled CSV records.
    ///
    /// # Algorithm
    ///
    /// ```text
    /// Phase 0 -- Format Detection:
    ///   If delimiter not specified:
    ///     Count occurrences of ',', '\t', '|', ';' in first 5 lines
    ///     Pick the most frequent candidate as delimiter
    ///   If detect_header:
    ///     Parse first row -- if all fields are non-numeric strings
    ///     and subsequent rows have different type patterns, treat
    ///     first row as header
    ///
    /// Phase 1 -- Value Collection:
    ///   Parse all sample records using detected/configured CSV settings
    ///   For each column, collect all non-null string values
    ///   Track null counts per column
    ///
    /// Phase 2 -- Type Detection (DuckDB-style):
    ///   For each column:
    ///     For each type in [BOOLEAN, BIGINT, DOUBLE, TIMESTAMP, DATE, TIME]:
    ///       Attempt to parse EVERY non-null value as that type
    ///       If ALL succeed: this column's type is determined, stop
    ///       If ANY fails: try next type
    ///     Fallback: VARCHAR
    ///
    /// Phase 3 -- Schema Assembly:
    ///   Build Arrow Schema from detected column types
    ///   Set nullable = true for columns with any null values
    /// ```
    fn infer(
        &self,
        samples: &[RawRecord],
        config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        let csv_config = CsvInferenceConfig {
            base: config.clone(),
            ..Default::default()
        };

        // Phase 0: Detect format
        let delimiter = self.detect_delimiter(samples, &csv_config)?;
        let has_header = self.detect_header(samples, delimiter, &csv_config)?;

        // Phase 1: Collect values
        let columns = self.collect_column_values(
            samples, delimiter, has_header, &csv_config
        )?;

        // Phase 2: Detect types
        let resolved = self.detect_column_types(&columns, &csv_config)?;

        // Phase 3: Assemble schema
        self.assemble_schema(resolved, samples.len())
    }
}

impl CsvFormatInference {
    /// Phase 0a: Auto-detect the field delimiter.
    ///
    /// Strategy: Count occurrences of candidate delimiters in the
    /// first 5 lines. The candidate with the most consistent count
    /// across lines (lowest variance) and highest total count wins.
    fn detect_delimiter(
        &self,
        samples: &[RawRecord],
        config: &CsvInferenceConfig,
    ) -> SchemaResult<u8> {
        if let Some(d) = config.delimiter {
            return Ok(d);
        }

        let candidates: &[u8] = &[b',', b'\t', b'|', b';'];
        let mut best_delimiter = b',';
        let mut best_score = 0.0f64;

        // Concatenate first few samples for line analysis
        let combined: Vec<u8> = samples.iter()
            .take(5)
            .flat_map(|s| s.value.iter().copied())
            .collect();

        let lines: Vec<&[u8]> = combined.split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .take(5)
            .collect();

        for &candidate in candidates {
            let counts: Vec<usize> = lines.iter()
                .map(|line| line.iter().filter(|&&b| b == candidate).count())
                .collect();

            if counts.is_empty() || counts.iter().all(|&c| c == 0) {
                continue;
            }

            let mean = counts.iter().sum::<usize>() as f64 / counts.len() as f64;
            let variance = counts.iter()
                .map(|&c| (c as f64 - mean).powi(2))
                .sum::<f64>() / counts.len() as f64;

            // Score: prefer high count with low variance
            let score = if variance < 0.01 { mean * 2.0 } else { mean / (1.0 + variance) };

            if score > best_score {
                best_score = score;
                best_delimiter = candidate;
            }
        }

        Ok(best_delimiter)
    }

    /// Phase 0b: Auto-detect whether the first row is a header.
    ///
    /// Heuristic: If the first row's fields are all non-numeric
    /// and the second row has at least one numeric field, the first
    /// row is likely a header.
    fn detect_header(
        &self,
        samples: &[RawRecord],
        delimiter: u8,
        config: &CsvInferenceConfig,
    ) -> SchemaResult<bool> {
        if !config.detect_header {
            return Ok(false);
        }

        // Parse first two rows
        let combined: Vec<u8> = samples.iter()
            .take(2)
            .flat_map(|s| {
                let mut v = s.value.clone();
                if !v.ends_with(b"\n") {
                    v.push(b'\n');
                }
                v
            })
            .collect();

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(false)
            .flexible(true)
            .from_reader(combined.as_slice());

        let mut records = reader.byte_records();

        let first_row = match records.next() {
            Some(Ok(r)) => r,
            _ => return Ok(false),
        };
        let second_row = match records.next() {
            Some(Ok(r)) => r,
            _ => return Ok(false),
        };

        // Check: are all first-row fields non-numeric?
        let first_all_non_numeric = first_row.iter().all(|field| {
            let s = std::str::from_utf8(field).unwrap_or("");
            s.parse::<f64>().is_err()
        });

        // Check: does second row have at least one numeric field?
        let second_has_numeric = second_row.iter().any(|field| {
            let s = std::str::from_utf8(field).unwrap_or("");
            s.parse::<f64>().is_ok()
        });

        Ok(first_all_non_numeric && second_has_numeric)
    }

    /// Phase 1: Collect all column values from samples.
    fn collect_column_values(
        &self,
        samples: &[RawRecord],
        delimiter: u8,
        has_header: bool,
        config: &CsvInferenceConfig,
    ) -> SchemaResult<Vec<ColumnDetection>> {
        let mut columns: Vec<ColumnDetection> = Vec::new();
        let mut header_names: Option<Vec<String>> = None;

        // Concatenate all sample bytes
        let combined: Vec<u8> = samples.iter()
            .flat_map(|s| {
                let mut v = s.value.clone();
                if !v.ends_with(b"\n") {
                    v.push(b'\n');
                }
                v
            })
            .collect();

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(false)
            .flexible(true)
            .from_reader(combined.as_slice());

        let mut is_first = true;
        for result in reader.records() {
            let record = result.map_err(|e| {
                SchemaError::InferenceFailed(format!("CSV parse error: {e}"))
            })?;

            // Handle header row
            if is_first && has_header {
                is_first = false;
                header_names = Some(
                    record.iter().map(|f| f.to_string()).collect()
                );
                continue;
            }
            is_first = false;

            // Ensure we have enough columns
            while columns.len() < record.len() {
                let idx = columns.len();
                let name = header_names
                    .as_ref()
                    .and_then(|h| h.get(idx))
                    .cloned()
                    .unwrap_or_else(|| format!("col_{idx}"));
                columns.push(ColumnDetection {
                    name,
                    values: Vec::new(),
                    null_count: 0,
                    total_count: 0,
                });
            }

            // Collect values
            for (i, field) in record.iter().enumerate() {
                if i >= columns.len() {
                    break; // Extra fields beyond detected columns
                }
                columns[i].total_count += 1;
                let trimmed = field.trim();
                if config.null_values.iter().any(|nv| nv == trimmed) {
                    columns[i].null_count += 1;
                } else {
                    columns[i].values.push(trimmed.to_string());
                }
            }
        }

        if columns.is_empty() {
            return Err(SchemaError::InferenceFailed(
                "No data rows found in CSV samples".into()
            ));
        }

        Ok(columns)
    }

    /// Phase 2: Detect the type of each column using DuckDB-style
    /// parse-attempt ordering.
    ///
    /// For each column, try parsing ALL non-null values as each type
    /// in the priority chain. The first type where every value parses
    /// successfully wins. VARCHAR always succeeds.
    fn detect_column_types(
        &self,
        columns: &[ColumnDetection],
        config: &CsvInferenceConfig,
    ) -> SchemaResult<Vec<ResolvedCsvColumn>> {
        let mut resolved = Vec::with_capacity(columns.len());

        for col in columns {
            let (data_type, confidence, warnings) = if col.values.is_empty() {
                // All values were null
                (DataType::Utf8, 0.5, vec![InferenceWarning {
                    field_name: col.name.clone(),
                    message: format!(
                        "Column '{}' had no non-null values; defaulting to VARCHAR",
                        col.name
                    ),
                    severity: WarningSeverity::Warning,
                }])
            } else {
                self.detect_single_column_type(col, config)
            };

            let nullable = col.null_count > 0;

            resolved.push(ResolvedCsvColumn {
                name: col.name.clone(),
                data_type,
                nullable,
                confidence,
                non_null_count: col.values.len(),
                total_count: col.total_count,
                warnings,
            });
        }

        Ok(resolved)
    }

    /// Detect the type of a single column by attempting to parse
    /// all values through the type priority chain.
    fn detect_single_column_type(
        &self,
        col: &ColumnDetection,
        config: &CsvInferenceConfig,
    ) -> (DataType, f64, Vec<InferenceWarning>) {
        let values = &col.values;

        // 1. Try BOOLEAN
        if self.all_parse_as_boolean(values, config) {
            return (DataType::Boolean, 1.0, vec![]);
        }

        // 2. Try BIGINT (i64)
        if values.iter().all(|v| v.parse::<i64>().is_ok()) {
            return (DataType::Int64, 1.0, vec![]);
        }

        // 3. Try DOUBLE (f64)
        if values.iter().all(|v| v.parse::<f64>().is_ok()) {
            // Check if some values were integers -- still DOUBLE since
            // at least one value failed BIGINT parse above
            return (DataType::Float64, 1.0, vec![]);
        }

        // 4. Try TIMESTAMP (with multiple format patterns)
        for fmt in &config.timestamp_formats {
            if values.iter().all(|v| {
                chrono::NaiveDateTime::parse_from_str(v, fmt).is_ok()
                    || chrono::DateTime::parse_from_str(v, fmt).is_ok()
            }) {
                return (
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    0.95,
                    vec![],
                );
            }
        }

        // 5. Try DATE
        for fmt in &config.date_formats {
            if values.iter().all(|v| {
                chrono::NaiveDate::parse_from_str(v, fmt).is_ok()
            }) {
                return (DataType::Date32, 0.95, vec![]);
            }
        }

        // 6. Try TIME
        for fmt in &config.time_formats {
            if values.iter().all(|v| {
                chrono::NaiveTime::parse_from_str(v, fmt).is_ok()
            }) {
                return (
                    DataType::Time64(TimeUnit::Nanosecond),
                    0.95,
                    vec![],
                );
            }
        }

        // 7. Fallback: VARCHAR
        (DataType::Utf8, 1.0, vec![])
    }

    /// Check if all values can be parsed as boolean.
    fn all_parse_as_boolean(
        &self,
        values: &[String],
        config: &CsvInferenceConfig,
    ) -> bool {
        values.iter().all(|v| {
            let lower = v.to_lowercase();
            config.true_values.iter().any(|t| t.to_lowercase() == lower)
                || config.false_values.iter().any(|f| f.to_lowercase() == lower)
        })
    }

    /// Phase 3: Assemble the final InferredSchema from resolved columns.
    fn assemble_schema(
        &self,
        columns: Vec<ResolvedCsvColumn>,
        sample_count: usize,
    ) -> SchemaResult<InferredSchema> {
        let arrow_fields: Vec<Field> = columns.iter()
            .map(|c| Field::new(&c.name, c.data_type.clone(), c.nullable))
            .collect();

        let schema = Arc::new(Schema::new(arrow_fields));

        let field_details: Vec<FieldInferenceDetail> = columns.iter()
            .map(|c| FieldInferenceDetail {
                field_name: c.name.clone(),
                inferred_type: c.data_type.clone(),
                appearance_rate: c.non_null_count as f64 / c.total_count.max(1) as f64,
                confidence: c.confidence,
                observed_types: vec![c.data_type.clone()],
            })
            .collect();

        let warnings: Vec<InferenceWarning> = columns.into_iter()
            .flat_map(|c| c.warnings)
            .collect();

        Ok(InferredSchema {
            schema,
            field_details,
            sample_count,
            warnings,
        })
    }
}

/// Internal resolved column representation.
#[derive(Debug)]
struct ResolvedCsvColumn {
    name: String,
    data_type: DataType,
    nullable: bool,
    confidence: f64,
    non_null_count: usize,
    total_count: usize,
    warnings: Vec<InferenceWarning>,
}
```

### SQL Interface

#### FORMAT CSV Options Mapping

```sql
-- Full CSV options specification
CREATE SOURCE csv_trades
FROM KAFKA (
    brokers = 'localhost:9092',
    topic   = 'csv-trades'
)
FORMAT CSV (
    delimiter        = ',',           -- Field separator (default: ',')
    quote            = '"',           -- Quote character (default: '"')
    escape           = '\\',          -- Escape character (default: none)
    has_header       = true,          -- First row is header (default: true)
    null_string      = 'NA',          -- Interpret as NULL (default: '')
    comment          = '#',           -- Comment line prefix (default: none)
    skip_rows        = 0,             -- Skip N data rows (default: 0)
    timestamp_format = '%Y-%m-%d %H:%M:%S%.f'  -- Timestamp parse format
);

-- CSV from file source with explicit schema
CREATE SOURCE logs (
    timestamp   TIMESTAMP NOT NULL,
    level       VARCHAR NOT NULL,
    message     VARCHAR,
    trace_id    VARCHAR
) FROM FILE (
    path  = '/data/logs.csv',
    watch = true
)
FORMAT CSV (
    delimiter   = '|',
    has_header  = true,
    null_string = '\\N'
);

-- CSV with auto-inference
CREATE SOURCE unknown_csv
FROM FILE (
    path = '/data/unknown.csv'
)
FORMAT CSV (
    delimiter  = ',',
    has_header = true
)
INFER SCHEMA (
    sample_size = 2000,
    null_as     = 'VARCHAR'
);

-- TSV (tab-separated values) -- just CSV with tab delimiter
CREATE SOURCE tsv_data
FROM FILE (
    path = '/data/export.tsv'
)
FORMAT CSV (
    delimiter  = '\t',
    has_header = true
);

-- CSV with partial schema (wildcard inference for remaining columns)
CREATE SOURCE sensor_csv (
    device_id   VARCHAR NOT NULL,
    reading     DOUBLE NOT NULL,
    *
) FROM FILE (
    path = '/data/sensors.csv'
)
FORMAT CSV (has_header = true)
INFER SCHEMA (
    sample_size     = 500,
    wildcard_prefix = 'extra_'
);
```

#### SQL Option to Config Field Mapping

| SQL Option | `CsvDecoderConfig` Field | Type | Default |
|------------|--------------------------|------|---------|
| `delimiter` | `delimiter` | `u8` | `b','` |
| `quote` | `quote` | `Option<u8>` | `Some(b'"')` |
| `escape` | `escape` | `Option<u8>` | `None` |
| `has_header` | `has_header` | `bool` | `true` |
| `null_string` | `null_string` | `String` | `""` |
| `comment` | `comment` | `Option<u8>` | `None` |
| `skip_rows` | `skip_rows` | `usize` | `0` |
| `timestamp_format` | `timestamp_format` | `String` | `"%Y-%m-%d %H:%M:%S%.f"` |

### Data Structures

#### CSV Type Detection Priority

| Priority | Type | Parse Test | Arrow Type | Example Values |
|----------|------|-----------|------------|----------------|
| 1 | BOOLEAN | matches true/false value lists | Boolean | `true`, `false`, `1`, `0`, `yes`, `no` |
| 2 | BIGINT | `str::parse::<i64>()` succeeds | Int64 | `42`, `-100`, `0`, `9999999999` |
| 3 | DOUBLE | `str::parse::<f64>()` succeeds | Float64 | `3.14`, `-0.5`, `1e10`, `NaN` |
| 4 | TIMESTAMP | chrono parse with format patterns | Timestamp(ns, UTC) | `2025-01-01 12:00:00`, `2025-01-01T12:00:00Z` |
| 5 | DATE | chrono parse with date formats | Date32 | `2025-01-01`, `01/15/2025` |
| 6 | TIME | chrono parse with time formats | Time64(ns) | `12:00:00`, `23:59:59.999` |
| 7 | VARCHAR | always succeeds (terminal fallback) | Utf8 | any string |

### Algorithm / Flow

#### decode_batch Flow

```
Input: &[RawRecord]  (N raw CSV byte payloads, each containing one or more lines)
Output: RecordBatch   (columnar Arrow format)

1. Initialize ArrayBuilders (one per schema column):
   - BooleanBuilder, Int64Builder, Float64Builder, StringBuilder,
     TimestampNanosecondBuilder, Date32Builder, Time64NanosecondBuilder

2. Concatenate all raw record bytes into a single byte buffer
   (CSV records may span multiple RawRecords for file sources)

3. Create csv::Reader with pre-configured ReaderBuilder settings

4. Skip `skip_rows` data rows (header already handled by has_headers=false
   + manual first-row skip logic)

5. For each CSV row from the reader:
   a. Check field count:
      - If fields < schema columns and strategy != Reject:
        pad missing fields with null
      - If fields > schema columns: ignore extra fields
      - If strategy == Reject and mismatch: return Err
   b. For each (col_idx, field_value) in row:
      - trimmed = field_value.trim()
      - If trimmed == null_string:
          builder[col_idx].append_null()
      - Else match self.coercions[col_idx]:
          Boolean:   parse against true/false value sets -> append
          Int64:     str::parse::<i64>() -> append
          Float64:   str::parse::<f64>() -> append
          Timestamp: timestamp_parser.parse_nanos(trimmed) -> append
          Date:      chrono::NaiveDate::parse_from_str() -> append
          Time:      chrono::NaiveTime::parse_from_str() -> append
          Utf8:      builder.append_value(trimmed)
      - On parse failure:
          Null strategy: append_null(), increment parse_error_count
          Skip strategy: skip entire row, continue
          Reject strategy: return Err(SchemaError::DecodeError)

6. Finish all builders:
   columns = builders.iter_mut().map(|b| b.finish()).collect()

7. RecordBatch::try_new(self.schema.clone(), columns)?
```

#### infer() Flow

```
Input:  &[RawRecord], &InferenceConfig
Output: InferredSchema

Phase 0 -- Format Detection:
  delimiter = detect_delimiter():
    If configured: use it
    Else: count ',', '\t', '|', ';' in first 5 lines
          pick candidate with highest consistent count
  has_header = detect_header():
    Parse first two rows
    If first row is all non-numeric AND second row has numeric fields:
      has_header = true
    Else: has_header = false

Phase 1 -- Value Collection:
  For each sample record:
    Parse as CSV with detected delimiter
    If has_header and first row: extract column names
    For each row (skipping header):
      For each field by column index:
        If field matches null_values: increment null_count
        Else: add to column's value list

Phase 2 -- Type Detection:
  For each column:
    values = column.values (non-null strings)
    If values is empty: type = VARCHAR, confidence = 0.5, warn
    Else try in order:
      BOOLEAN: all values in true_values or false_values?
      BIGINT:  all values.parse::<i64>() succeed?
      DOUBLE:  all values.parse::<f64>() succeed?
      TIMESTAMP: for each format_pattern:
                   all values parse as timestamp?
      DATE:    for each format_pattern:
                   all values parse as date?
      TIME:    for each format_pattern:
                   all values parse as time?
      VARCHAR: (always succeeds)

Phase 3 -- Assembly:
  Build Arrow Schema from detected types
  Set nullable = true for columns with any null values
  Return InferredSchema { schema, field_details, sample_count, warnings }
```

### Error Handling

| Error | Cause | SchemaError Variant | Recovery |
|-------|-------|-------------------|----------|
| CSV parse failure | Malformed CSV (unmatched quotes, encoding) | `DecodeError` | Dead-letter or skip per source ON ERROR config |
| Field count mismatch | Row has too few or too many fields | `DecodeError` | Depends on `FieldCountMismatchStrategy`: null / skip / reject |
| Type coercion failure | Value doesn't parse as target type | `DecodeError` | Insert null, skip row, or reject (per config) |
| No data rows | All samples are empty or comments | `InferenceFailed` | User must check source data |
| All values null | Column has no non-null values | Warning only | Defaults to VARCHAR |
| Delimiter ambiguity | Multiple delimiters have equal scores | Warning only | Falls back to comma |
| Header detection false positive | First row looks like header but isn't | Warning only | User should set `has_header` explicitly |
| Encoding error | Non-UTF-8 bytes in CSV | `DecodeError` | Replace invalid bytes or reject |

## Implementation Plan

### Phase 1: Core CsvDecoder (2 days)

1. Create `laminar-formats/src/csv/mod.rs` module structure
2. Implement `CsvDecoderConfig` with SQL option mapping
3. Implement `CsvDecoder` with `FormatDecoder` trait
4. Implement `CsvCoercion` enum and per-column coercion logic
5. Support all primitive types: Boolean, Int64, Float64, Utf8
6. Support temporal types: Timestamp, Date32, Time64
7. Implement `FieldCountMismatchStrategy` handling
8. Implement `null_string` matching
9. Unit tests for all type coercions and edge cases

### Phase 2: CsvFormatInference (1.5 days)

1. Implement `CsvFormatInference` with four-phase algorithm
2. Implement delimiter auto-detection
3. Implement header auto-detection
4. Implement DuckDB-style type detection priority chain
5. Implement configurable boolean, null, and temporal value patterns
6. Register in `FormatInferenceRegistry`
7. Unit tests for each detection phase

### Phase 3: SQL Integration & Edge Cases (1.5 days)

1. Wire `FORMAT CSV (...)` DDL parsing to `CsvDecoderConfig`
2. Implement `INFER SCHEMA` integration for CSV sources
3. Handle quoted fields with embedded delimiters and newlines
4. Handle escaped quotes (both RFC 4180 doubled-quote and backslash)
5. Integration test: Kafka CSV source -> inference -> decode roundtrip
6. Integration test: File CSV source with various formats
7. Performance benchmarks

## Testing Strategy

### Unit Tests (25+)

| Module | Tests | Description |
|--------|-------|-------------|
| `csv::decoder` | 10 | Decode all primitive types, nullable fields, batch decoding, null_string matching, field count mismatch strategies (null/skip/reject), skip_rows, comment lines, custom delimiter (pipe, tab, semicolon), quoted fields with embedded delimiters, empty batch |
| `csv::inference` | 10 | Delimiter auto-detection (comma, tab, pipe), header auto-detection (with/without header), type detection for each priority level (boolean, bigint, double, timestamp, date, time, varchar), all-null column default, multiple timestamp format patterns, boolean value list customization |
| `csv::format_detection` | 5 | Delimiter detection with multiple candidates, header detection edge cases (all-string data, single row, numeric header names), mixed encoding, comment-heavy files, empty files |

### Integration Tests (6)

- CSV decode from Kafka source with explicit schema
- CSV inference from file source (auto-detect delimiter + header + types)
- CSV with explicit `FORMAT CSV (delimiter = '|', ...)` options from SQL
- CSV with wildcard partial schema inference
- TSV file (tab-delimited) end-to-end
- Malformed CSV rows with dead-letter handling

### Edge Case Tests (8)

- Quoted fields containing the delimiter character
- Quoted fields containing newline characters
- Escaped quotes within quoted fields (RFC 4180: doubled quote)
- Escaped quotes within quoted fields (backslash escape)
- CSV with BOM (byte order mark) at file start
- CSV with mixed line endings (CRLF and LF)
- CSV with trailing comma (extra empty field)
- CSV with unicode column names and values

### Benchmarks

| Benchmark | Target | Description |
|-----------|--------|-------------|
| `bench_csv_decode_batch_1024` | <500us | Decode 1024 CSV records (~100 bytes each, 5 columns) to RecordBatch |
| `bench_csv_decode_per_record` | <500ns | Per-record decode latency |
| `bench_csv_type_coercion` | <100ns/field | Per-field type coercion overhead (vs raw string copy) |
| `bench_csv_inference_1000` | <200ms | Infer schema from 1000 CSV rows |
| `bench_csv_delimiter_detection` | <1ms | Auto-detect delimiter from 5 sample lines |
| `bench_csv_throughput_per_core` | >800K rec/s | Sustained decode throughput (simple 5-column schema) |

## Success Criteria

- `CsvDecoder` passes all unit and integration tests
- `CsvFormatInference` correctly detects types for all priority levels
- Delimiter auto-detection correctly identifies comma, tab, pipe, semicolon
- Header auto-detection works for numeric-data CSVs
- All `FORMAT CSV (...)` SQL options correctly map to `CsvDecoderConfig`
- CSV decode throughput >800K records/sec/core for simple schemas
- `cargo clippy -- -D warnings` clean
- Zero unsafe code
- Integration with `SchemaResolver` produces identical results whether schema is declared or inferred

## Competitive Comparison

| Feature | LaminarDB | Apache Flink | RisingWave | Materialize | DuckDB | ClickHouse |
|---------|-----------|-------------|------------|-------------|--------|------------|
| CSV auto-inference | DuckDB-style type detection | DDL only | DDL only | DDL only | DuckDB sniffer | TabSeparated auto |
| Delimiter auto-detect | Frequency analysis | No (configured) | No (configured) | No | Yes | No |
| Header auto-detect | Heuristic | No (configured) | No (configured) | No | Yes | No |
| Custom null strings | Configurable list | "null" only | "null" only | No | Configurable | Configurable |
| Boolean value sets | Configurable true/false lists | true/false only | true/false only | true/false only | Configurable | 1/0 only |
| Temporal type detection | TIMESTAMP + DATE + TIME | TIMESTAMP only | TIMESTAMP only | TIMESTAMP only | All temporal | TIMESTAMP only |
| Multi-format timestamps | Try N patterns in order | Single format | Single format | Single format | Auto-detect | Single format |
| Quoted field handling | Full RFC 4180 + backslash | RFC 4180 | RFC 4180 | RFC 4180 | Full | Basic |
| Streaming CSV source | Kafka, WebSocket, File | Kafka, File | Kafka, File | File only | File only | File, Kafka |
| Batch decode output | Columnar Arrow | Row-based | Row-based | Row-based | Columnar | Columnar |

## Files

### New Files

- `crates/laminar-formats/src/csv/mod.rs` -- Module declaration and re-exports
- `crates/laminar-formats/src/csv/decoder.rs` -- `CsvDecoder`, `CsvDecoderConfig`, `CsvCoercion`
- `crates/laminar-formats/src/csv/inference.rs` -- `CsvFormatInference`, `CsvInferenceConfig`, type detection
- `crates/laminar-formats/src/csv/tests.rs` -- Unit tests

### Modified Files

- `crates/laminar-formats/src/lib.rs` -- Register `csv` module, add to `FormatInferenceRegistry`
- `crates/laminar-formats/Cargo.toml` -- Add `csv` crate dependency
- `crates/laminar-sql/src/parser/` -- Parse `FORMAT CSV (...)` options in DDL

### Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `csv` | 1.x (BurntSushi) | Zero-copy CSV parsing with `ByteRecord` API |
| `chrono` | 0.4+ | Temporal type parsing (timestamp, date, time formats) |
| `arrow` | (workspace) | RecordBatch construction, ArrayBuilder APIs |

## References

- [Schema Inference Design](../../../research/schema-inference-design.md) -- Sections 4.3, 8.1-8.3
- [Extensible Schema Traits](../../../research/extensible-schema-traits.md) -- FormatDecoder, FormatInference traits
- F-SCHEMA-001 -- FormatDecoder / FormatEncoder trait definitions
- F-SCHEMA-003 -- FormatInferenceRegistry and pluggable inference
- [DuckDB CSV Sniffer](https://duckdb.org/docs/data/csv/auto_detection) -- Type detection algorithm reference
- [RFC 4180](https://tools.ietf.org/html/rfc4180) -- CSV format specification
