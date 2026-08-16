//! JSON format decoder implementing [`FormatDecoder`].
//!
//! Converts raw JSON byte payloads into Arrow `RecordBatch`es.
//! Constructed once at `CREATE SOURCE` time with a frozen Arrow schema;
//! the decoder is stateless after construction so the Ring 1 hot path
//! has zero schema lookups.
#![allow(clippy::disallowed_types)] // cold path: schema management

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::FormatDecoder;
use crate::schema::types::RawRecord;

mod batch;
mod value;

/// Strategy for JSON fields not in the Arrow schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownFieldStrategy {
    /// Silently ignore unknown fields (default).
    Ignore,
    /// Collect unknown fields into an `_extra` `LargeBinary` (JSONB) column.
    CollectExtra,
    /// Return a decode error if any unknown field is encountered.
    Reject,
}

/// Strategy for JSON values that don't match the expected Arrow type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeMismatchStrategy {
    /// Insert null and increment the mismatch counter (default).
    Null,
    /// Attempt coercion (e.g., `"123"` → `123` for Int64 columns).
    Coerce,
    /// Return a decode error on the first mismatch.
    Reject,
}

impl TypeMismatchStrategy {
    /// Parse from a `WITH` option value (`schema.enforcement`).
    #[must_use]
    pub fn from_enforcement_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "coerce" => Some(Self::Coerce),
            "strict" => Some(Self::Reject),
            "permissive" => Some(Self::Null),
            _ => None,
        }
    }
}

/// Epoch unit for *numeric* JSON timestamps feeding a `Timestamp` column
/// (strings still go through `timestamp_formats`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EpochUnit {
    /// Epoch seconds.
    Seconds,
    /// Epoch milliseconds (default).
    #[default]
    Millis,
    /// Epoch microseconds.
    Micros,
    /// Epoch nanoseconds.
    Nanos,
}

impl EpochUnit {
    /// Parse a `json.column.<col>.epoch_unit` value.
    #[must_use]
    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "seconds" => Some(Self::Seconds),
            "millis" => Some(Self::Millis),
            "micros" => Some(Self::Micros),
            "nanos" => Some(Self::Nanos),
            _ => None,
        }
    }

    /// Nanoseconds per one unit (used for lossless scaling).
    const fn nanos_per(self) -> i64 {
        match self {
            Self::Seconds => 1_000_000_000,
            Self::Millis => 1_000_000,
            Self::Micros => 1_000,
            Self::Nanos => 1,
        }
    }
}

/// Per-column extraction strategy, pre-computed at Ring 2.
#[derive(Debug, Clone)]
enum ColumnExtraction {
    /// Extract field from the default path target (json.path or root).
    DefaultPath,
    /// Extract field from a custom absolute path from root.
    CustomPath { segments: Vec<String> },
}

/// JSON decoder configuration.
#[derive(Debug, Clone)]
pub struct JsonDecoderConfig {
    /// How to handle fields present in JSON but absent from the schema.
    pub unknown_fields: UnknownFieldStrategy,

    /// How to handle type mismatches.
    pub type_mismatch: TypeMismatchStrategy,

    /// Timestamp format patterns to try when parsing string values
    /// into Timestamp columns. Tried in order; first match wins.
    /// Use `"iso8601"` for RFC 3339 / ISO 8601 auto-detection.
    pub timestamp_formats: Vec<String>,

    /// Whether to encode nested objects as JSONB binary format
    /// instead of JSON-serialized Utf8. When true, nested objects
    /// become `LargeBinary` columns with JSONB encoding.
    pub nested_as_jsonb: bool,

    /// Dot-separated path to navigate before field extraction.
    /// e.g. `"data"` → navigate into `{"data": {...}}` before extraction.
    /// e.g. `"data.trade"` → navigate into `{"data":{"trade":{...}}}`.
    pub json_path: Option<Vec<String>>,

    /// Per-column absolute path overrides: `column_name` → path segments.
    /// Parsed from `json.column.<name> = 'path.to.field'` options.
    /// These paths are absolute from the root, ignoring `json_path`.
    pub json_column_paths: HashMap<String, Vec<String>>,

    /// Per-column numeric-timestamp epoch unit, from
    /// `json.column.<name>.epoch_unit`. Absent ⇒ [`EpochUnit::Millis`].
    pub numeric_timestamp_units: HashMap<String, EpochUnit>,

    /// Column names for array-to-rows expansion.
    /// When set, the target (after `json_path`) must be an array.
    /// Each element becomes a row; positional names map array elements to columns.
    pub json_explode: Option<Vec<String>>,
}

impl Default for JsonDecoderConfig {
    fn default() -> Self {
        Self {
            unknown_fields: UnknownFieldStrategy::Ignore,
            type_mismatch: TypeMismatchStrategy::Coerce,
            timestamp_formats: vec![
                "iso8601".into(),
                "%Y-%m-%dT%H:%M:%S%.fZ".into(),
                "%Y-%m-%dT%H:%M:%S%.f%:z".into(),
                "%Y-%m-%d %H:%M:%S%.f".into(),
                "%Y-%m-%d %H:%M:%S".into(),
            ],
            nested_as_jsonb: false,
            json_path: None,
            json_column_paths: HashMap::new(),
            numeric_timestamp_units: HashMap::new(),
            json_explode: None,
        }
    }
}

impl JsonDecoderConfig {
    /// Build config from a [`ConnectorConfig`](crate::config::ConnectorConfig).
    ///
    /// Parses `json.path`, `json.column.*`, `json.explode`,
    /// `schema.enforcement`, and `nested.as.jsonb` properties.
    /// Epoch-unit options are validated against the frozen output schema.
    /// Called once at Ring 2 (`CREATE SOURCE` time).
    ///
    /// # Errors
    ///
    /// Returns an invalid-configuration error when an epoch unit is unknown or
    /// targets a missing or non-timestamp column.
    pub fn from_connector_config(
        config: &crate::config::ConnectorConfig,
        schema: &SchemaRef,
    ) -> SchemaResult<Self> {
        let mut cfg = Self::default();

        if let Some(path) = config.get("json.path") {
            cfg.json_path = Some(parse_path_option("json.path", path)?);
        }

        let mut col_props: Vec<_> = config
            .properties_with_prefix("json.column.")
            .into_iter()
            .collect();
        col_props.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        for (col_name, val) in col_props {
            // `json.column.<col>.epoch_unit = '<unit>'` configures numeric
            // timestamp scaling for <col>; everything else is a path.
            // (Column names are SQL identifiers — no dots — so the suffix
            // split is unambiguous against dotted path *values*.)
            if let Some(col) = col_name.strip_suffix(".epoch_unit") {
                let unit =
                    EpochUnit::from_str_opt(&val).ok_or_else(|| SchemaError::InvalidConfig {
                        key: format!("json.column.{col}.epoch_unit"),
                        message: format!(
                            "invalid epoch unit '{val}'; expected seconds, millis, micros, or nanos"
                        ),
                    })?;
                let field =
                    schema
                        .field_with_name(col)
                        .map_err(|_| SchemaError::InvalidConfig {
                            key: format!("json.column.{col}.epoch_unit"),
                            message: format!("column '{col}' is not present in the source schema"),
                        })?;
                if !matches!(field.data_type(), DataType::Timestamp(_, _)) {
                    return Err(SchemaError::InvalidConfig {
                        key: format!("json.column.{col}.epoch_unit"),
                        message: format!(
                            "column '{col}' must be a Timestamp, found {:?}",
                            field.data_type()
                        ),
                    });
                }
                cfg.numeric_timestamp_units.insert(col.to_string(), unit);
                continue;
            }
            schema
                .field_with_name(&col_name)
                .map_err(|_| SchemaError::InvalidConfig {
                    key: format!("json.column.{col_name}"),
                    message: format!("column '{col_name}' is not present in the source schema"),
                })?;
            cfg.json_column_paths.insert(
                col_name.clone(),
                parse_path_option(&format!("json.column.{col_name}"), &val)?,
            );
        }

        if let Some(explode) = config.get("json.explode") {
            let columns: Vec<String> = explode
                .split(',')
                .map(str::trim)
                .map(ToString::to_string)
                .collect();
            if columns.is_empty() || columns.iter().any(String::is_empty) {
                return Err(SchemaError::InvalidConfig {
                    key: "json.explode".into(),
                    message: "expected one or more comma-separated schema columns".into(),
                });
            }
            let mut seen = std::collections::HashSet::with_capacity(columns.len());
            for column in &columns {
                if !seen.insert(column) {
                    return Err(SchemaError::InvalidConfig {
                        key: "json.explode".into(),
                        message: format!("column '{column}' is listed more than once"),
                    });
                }
                schema
                    .field_with_name(column)
                    .map_err(|_| SchemaError::InvalidConfig {
                        key: "json.explode".into(),
                        message: format!("column '{column}' is not present in the source schema"),
                    })?;
            }
            cfg.json_explode = Some(columns);
        }

        if let Some(enforcement) = config.get("schema.enforcement") {
            cfg.type_mismatch = TypeMismatchStrategy::from_enforcement_str(enforcement)
                .ok_or_else(|| SchemaError::InvalidConfig {
                    key: "schema.enforcement".into(),
                    message: format!(
                        "invalid value '{enforcement}'; expected lenient, coerce, or strict"
                    ),
                })?;
        }
        if let Some(v) = config.get("nested.as.jsonb") {
            cfg.nested_as_jsonb = match v.to_ascii_lowercase().as_str() {
                "true" => true,
                "false" => false,
                _ => {
                    return Err(SchemaError::InvalidConfig {
                        key: "nested.as.jsonb".into(),
                        message: format!("invalid boolean '{v}'; expected true or false"),
                    });
                }
            };
        }
        Ok(cfg)
    }
}

fn parse_path_option(key: &str, value: &str) -> Result<Vec<String>, SchemaError> {
    let segments: Vec<String> = value
        .split('.')
        .map(str::trim)
        .map(ToString::to_string)
        .collect();
    if segments.is_empty() || segments.iter().any(String::is_empty) {
        return Err(SchemaError::InvalidConfig {
            key: key.into(),
            message: "path must contain non-empty dot-separated segments".into(),
        });
    }
    Ok(segments)
}

/// Decodes JSON byte payloads into Arrow `RecordBatch`es.
///
/// # Ring Placement
///
/// - **Ring 1**: `decode_batch()` — parse JSON, build columnar Arrow output
/// - **Ring 2**: Construction (`new` / `with_config`) — one-time setup
pub struct JsonDecoder {
    /// Frozen output schema.
    schema: SchemaRef,
    /// Decoder configuration.
    config: JsonDecoderConfig,
    /// Pre-computed field index map: field name → column index.
    field_indices: Vec<(String, usize)>,
    /// Cumulative type mismatch count (diagnostics).
    mismatch_count: AtomicU64,
    /// Ring 2 pre-computed: extraction strategy per schema column.
    column_extractions: Vec<ColumnExtraction>,
    /// Ring 2 pre-computed: numeric epoch unit per schema column
    /// (aligned to `schema.fields()`; default [`EpochUnit::Millis`]).
    column_epoch_units: Vec<EpochUnit>,
    /// Ring 2 pre-computed: explode position → schema column index.
    /// `Some` when `json.explode` is configured; each entry maps an
    /// explode position to the schema column index (or `None` if
    /// the explode name doesn't match any schema column).
    explode_col_indices: Option<Vec<Option<usize>>>,
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for JsonDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonDecoder")
            .field("schema", &self.schema)
            .field("config", &self.config)
            .field(
                "mismatch_count",
                &self.mismatch_count.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl JsonDecoder {
    /// Creates a new JSON decoder for the given Arrow schema.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self::with_config(schema, JsonDecoderConfig::default())
    }

    /// Creates a new JSON decoder with custom configuration.
    #[must_use]
    pub fn with_config(schema: SchemaRef, config: JsonDecoderConfig) -> Self {
        let field_indices: Vec<(String, usize)> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().clone(), i))
            .collect();

        let column_extractions: Vec<ColumnExtraction> = schema
            .fields()
            .iter()
            .map(|f| {
                let col_name = f.name();
                if let Some(path_segments) = config.json_column_paths.get(col_name.as_str()) {
                    ColumnExtraction::CustomPath {
                        segments: path_segments.clone(),
                    }
                } else {
                    ColumnExtraction::DefaultPath
                }
            })
            .collect();

        let column_epoch_units: Vec<EpochUnit> = schema
            .fields()
            .iter()
            .map(|f| {
                config
                    .numeric_timestamp_units
                    .get(f.name().as_str())
                    .copied()
                    .unwrap_or_default()
            })
            .collect();

        let explode_col_indices = config.json_explode.as_ref().map(|names| {
            names
                .iter()
                .map(|name| {
                    field_indices
                        .iter()
                        .find(|(n, _)| n == name)
                        .map(|(_, idx)| *idx)
                })
                .collect()
        });

        Self {
            schema,
            config,
            field_indices,
            mismatch_count: AtomicU64::new(0),
            column_extractions,
            column_epoch_units,
            explode_col_indices,
        }
    }

    /// Returns the cumulative type mismatch count.
    pub fn mismatch_count(&self) -> u64 {
        self.mismatch_count.load(Ordering::Relaxed)
    }
}

impl FormatDecoder for JsonDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        let values: Vec<&[u8]> = records.iter().map(|r| r.value.as_slice()).collect();
        self.decode_slices(&values)
    }

    fn format_name(&self) -> &'static str {
        "json"
    }
}

#[cfg(test)]
mod tests;
