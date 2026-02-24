//! JSON format decoder implementing [`FormatDecoder`].
//!
//! Converts raw JSON byte payloads into Arrow `RecordBatch`es.
//! Constructed once at `CREATE SOURCE` time with a frozen Arrow schema;
//! the decoder is stateless after construction so the Ring 1 hot path
//! has zero schema lookups.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder, LargeBinaryBuilder, LargeStringBuilder, StringBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    TimestampSecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef, TimeUnit};

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::json::jsonb::JsonbEncoder;
use crate::schema::traits::FormatDecoder;
use crate::schema::types::RawRecord;

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
        }
    }
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

        Self {
            schema,
            config,
            field_indices,
            mismatch_count: AtomicU64::new(0),
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
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let num_fields = self.schema.fields().len();
        let capacity = records.len();

        // Initialise one builder per schema column.
        let mut builders = create_builders(&self.schema, capacity);

        // Optional _extra JSONB column for CollectExtra strategy.
        let collect_extra = matches!(
            self.config.unknown_fields,
            UnknownFieldStrategy::CollectExtra
        );
        let mut extra_builder = if collect_extra {
            Some(LargeBinaryBuilder::with_capacity(capacity, capacity * 64))
        } else {
            None
        };

        let mut jsonb_encoder = if self.config.nested_as_jsonb {
            Some(JsonbEncoder::new())
        } else {
            None
        };

        for record in records {
            let value: serde_json::Value = serde_json::from_slice(&record.value)
                .map_err(|e| SchemaError::DecodeError(format!("JSON parse error: {e}")))?;

            let obj = value.as_object().ok_or_else(|| {
                SchemaError::DecodeError("top-level JSON value must be an object".into())
            })?;

            // Track which schema fields were populated for this record.
            let mut populated = vec![false; num_fields];

            // Collect unknown fields for CollectExtra.
            let mut extra_fields: Option<serde_json::Map<String, serde_json::Value>> =
                if collect_extra {
                    Some(serde_json::Map::new())
                } else {
                    None
                };

            for (key, val) in obj {
                if let Some(col_idx) = self.field_index(key) {
                    populated[col_idx] = true;
                    let field = &self.schema.fields()[col_idx];
                    append_value(
                        &mut builders[col_idx],
                        field.data_type(),
                        val,
                        &self.config,
                        &self.mismatch_count,
                        jsonb_encoder.as_mut(),
                    )?;
                } else {
                    match self.config.unknown_fields {
                        UnknownFieldStrategy::Ignore => {}
                        UnknownFieldStrategy::CollectExtra => {
                            if let Some(ref mut extra) = extra_fields {
                                extra.insert(key.clone(), val.clone());
                            }
                        }
                        UnknownFieldStrategy::Reject => {
                            return Err(SchemaError::DecodeError(format!(
                                "unknown field '{key}' not in schema"
                            )));
                        }
                    }
                }
            }

            // Append nulls for missing fields.
            for (col_idx, was_populated) in populated.iter().enumerate() {
                if !was_populated {
                    append_null(&mut builders[col_idx]);
                }
            }

            // Append _extra column.
            if let Some(ref mut eb) = extra_builder {
                if let Some(ref extra) = extra_fields {
                    if extra.is_empty() {
                        eb.append_null();
                    } else {
                        let mut enc = jsonb_encoder.as_mut().map_or_else(JsonbEncoder::new, |_| {
                            // Borrow-safe: take a fresh encoder for extra.
                            JsonbEncoder::new()
                        });
                        let bytes = enc.encode(&serde_json::Value::Object(extra.clone()));
                        eb.append_value(&bytes);
                    }
                } else {
                    eb.append_null();
                }
            }
        }

        // Finish all builders into arrays.
        let mut columns: Vec<ArrayRef> = builders.into_iter().map(|mut b| b.finish()).collect();

        // Append _extra column if present.
        let final_schema = if let Some(mut eb) = extra_builder {
            columns.push(Arc::new(eb.finish()));
            let mut fields = self.schema.fields().to_vec();
            fields.push(Arc::new(arrow_schema::Field::new(
                "_extra",
                DataType::LargeBinary,
                true,
            )));
            Arc::new(arrow_schema::Schema::new(fields))
        } else {
            self.schema.clone()
        };

        RecordBatch::try_new(final_schema, columns)
            .map_err(|e| SchemaError::DecodeError(format!("RecordBatch construction: {e}")))
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn format_name(&self) -> &str {
        "json"
    }
}

impl JsonDecoder {
    /// O(n) field lookup. For schemas with many fields, consider switching
    /// to a `HashMap`; for typical schemas (<50 fields) linear scan is faster.
    fn field_index(&self, name: &str) -> Option<usize> {
        self.field_indices
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, idx)| *idx)
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
    ($builder:ty, $array:ty) => {
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

impl_column_builder!(BooleanBuilder, arrow_array::BooleanArray);
impl_column_builder!(Int8Builder, arrow_array::Int8Array);
impl_column_builder!(Int16Builder, arrow_array::Int16Array);
impl_column_builder!(Int32Builder, arrow_array::Int32Array);
impl_column_builder!(Int64Builder, arrow_array::Int64Array);
impl_column_builder!(UInt8Builder, arrow_array::UInt8Array);
impl_column_builder!(UInt16Builder, arrow_array::UInt16Array);
impl_column_builder!(UInt32Builder, arrow_array::UInt32Array);
impl_column_builder!(UInt64Builder, arrow_array::UInt64Array);
impl_column_builder!(Float32Builder, arrow_array::Float32Array);
impl_column_builder!(Float64Builder, arrow_array::Float64Array);
impl_column_builder!(StringBuilder, arrow_array::StringArray);
impl_column_builder!(LargeStringBuilder, arrow_array::LargeStringArray);
impl_column_builder!(LargeBinaryBuilder, arrow_array::LargeBinaryArray);
impl_column_builder!(TimestampSecondBuilder, arrow_array::TimestampSecondArray);
impl_column_builder!(
    TimestampMillisecondBuilder,
    arrow_array::TimestampMillisecondArray
);
impl_column_builder!(
    TimestampMicrosecondBuilder,
    arrow_array::TimestampMicrosecondArray
);
impl_column_builder!(
    TimestampNanosecondBuilder,
    arrow_array::TimestampNanosecondArray
);

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
        DataType::Int8 => Box::new(Int8Builder::with_capacity(capacity)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::UInt8 => Box::new(UInt8Builder::with_capacity(capacity)),
        DataType::UInt16 => Box::new(UInt16Builder::with_capacity(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::with_capacity(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::LargeUtf8 => Box::new(LargeStringBuilder::with_capacity(capacity, capacity * 32)),
        DataType::LargeBinary => {
            Box::new(LargeBinaryBuilder::with_capacity(capacity, capacity * 64))
        }
        DataType::Timestamp(TimeUnit::Second, tz) => {
            let builder =
                TimestampSecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone());
            Box::new(builder)
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let builder =
                TimestampMillisecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone());
            Box::new(builder)
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let builder =
                TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone());
            Box::new(builder)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let builder =
                TimestampNanosecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone());
            Box::new(builder)
        }
        // Fallback: serialize as JSON string.
        _ => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
    }
}

fn append_null(builder: &mut Box<dyn ColumnBuilder>) {
    builder.append_null_value();
}

/// Append a JSON value to the appropriate builder column.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn append_value(
    builder: &mut Box<dyn ColumnBuilder>,
    target_type: &DataType,
    value: &serde_json::Value,
    config: &JsonDecoderConfig,
    mismatch_count: &AtomicU64,
    jsonb_encoder: Option<&mut JsonbEncoder>,
) -> SchemaResult<()> {
    if value.is_null() {
        builder.append_null_value();
        return Ok(());
    }

    match target_type {
        DataType::Boolean => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            match extract_bool(value, config) {
                Ok(v) => b.append_value(v),
                Err(e) => handle_mismatch(builder, config, mismatch_count, &e)?,
            }
        }
        DataType::Int8 => {
            let b = builder.as_any_mut().downcast_mut::<Int8Builder>().unwrap();
            match extract_i8(value, config) {
                Ok(v) => b.append_value(v),
                Err(e) => handle_mismatch(builder, config, mismatch_count, &e)?,
            }
        }
        DataType::Int16 => {
            let b = builder.as_any_mut().downcast_mut::<Int16Builder>().unwrap();
            match extract_i16(value, config) {
                Ok(v) => b.append_value(v),
                Err(e) => handle_mismatch(builder, config, mismatch_count, &e)?,
            }
        }
        DataType::Int32 => {
            let b = builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
            match extract_i32(value, config) {
                Ok(v) => b.append_value(v),
                Err(e) => handle_mismatch(builder, config, mismatch_count, &e)?,
            }
        }
        DataType::Int64 => {
            let b = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            match extract_i64(value, config) {
                Ok(v) => b.append_value(v),
                Err(e) => handle_mismatch(builder, config, mismatch_count, &e)?,
            }
        }
        DataType::UInt8 => {
            let b = builder.as_any_mut().downcast_mut::<UInt8Builder>().unwrap();
            match extract_u8(value, config) {
                Ok(v) => b.append_value(v),
                Err(e) => handle_mismatch(builder, config, mismatch_count, &e)?,
            }
        }
        DataType::UInt16 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt16Builder>()
                .unwrap();
            match extract_u16(value, config) {
                Ok(v) => b.append_value(v),
                Err(e) => handle_mismatch(builder, config, mismatch_count, &e)?,
            }
        }
        DataType::UInt32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt32Builder>()
                .unwrap();
            match extract_u32(value, config) {
                Ok(v) => b.append_value(v),
                Err(e) => handle_mismatch(builder, config, mismatch_count, &e)?,
            }
        }
        DataType::UInt64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .unwrap();
            match extract_u64(value, config) {
                Ok(v) => b.append_value(v),
                Err(e) => handle_mismatch(builder, config, mismatch_count, &e)?,
            }
        }
        DataType::Float32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .unwrap();
            match extract_f32(value, config) {
                Ok(v) => b.append_value(v),
                Err(e) => handle_mismatch(builder, config, mismatch_count, &e)?,
            }
        }
        DataType::Float64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap();
            match extract_f64(value, config) {
                Ok(v) => b.append_value(v),
                Err(e) => handle_mismatch(builder, config, mismatch_count, &e)?,
            }
        }
        DataType::LargeUtf8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<LargeStringBuilder>()
                .unwrap();
            let s = value_to_string(value);
            b.append_value(&s);
        }
        DataType::LargeBinary => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<LargeBinaryBuilder>()
                .unwrap();
            if let Some(enc) = jsonb_encoder {
                let bytes = enc.encode(value);
                b.append_value(&bytes);
            } else {
                // Fallback: serialize as JSON bytes.
                let bytes = serde_json::to_vec(value).unwrap_or_default();
                b.append_value(&bytes);
            }
        }
        DataType::Timestamp(unit, _) => match extract_timestamp(value, config, *unit) {
            Ok(ts) => append_timestamp(builder, *unit, ts),
            Err(e) => handle_mismatch(builder, config, mismatch_count, &e)?,
        },
        // Unsupported types: serialize as JSON string.
        _ => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            let s = value_to_string(value);
            b.append_value(&s);
        }
    }

    Ok(())
}

fn handle_mismatch(
    builder: &mut Box<dyn ColumnBuilder>,
    config: &JsonDecoderConfig,
    mismatch_count: &AtomicU64,
    error_msg: &str,
) -> SchemaResult<()> {
    match config.type_mismatch {
        TypeMismatchStrategy::Null => {
            mismatch_count.fetch_add(1, Ordering::Relaxed);
            builder.append_null_value();
            Ok(())
        }
        TypeMismatchStrategy::Coerce => {
            // Coercion already failed in the extractor — this is a real error.
            Err(SchemaError::DecodeError(format!(
                "type coercion failed: {error_msg}"
            )))
        }
        TypeMismatchStrategy::Reject => Err(SchemaError::DecodeError(format!(
            "type mismatch: {error_msg}"
        ))),
    }
}

// ── Value extractors ───────────────────────────────────────────────

fn extract_bool(value: &serde_json::Value, config: &JsonDecoderConfig) -> Result<bool, String> {
    if let Some(b) = value.as_bool() {
        return Ok(b);
    }
    if matches!(config.type_mismatch, TypeMismatchStrategy::Coerce) {
        if let Some(s) = value.as_str() {
            match s.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => return Ok(true),
                "false" | "0" | "no" => return Ok(false),
                _ => {}
            }
        }
        if let Some(n) = value.as_i64() {
            return Ok(n != 0);
        }
    }
    Err(format!("expected boolean, got {}", json_type_name(value)))
}

fn extract_i8(value: &serde_json::Value, config: &JsonDecoderConfig) -> Result<i8, String> {
    if let Some(n) = value.as_i64() {
        if let Ok(v) = i8::try_from(n) {
            return Ok(v);
        }
        return Err(format!("integer {n} out of i8 range"));
    }
    if matches!(config.type_mismatch, TypeMismatchStrategy::Coerce) {
        if let Some(s) = value.as_str() {
            if let Ok(v) = s.parse::<i8>() {
                return Ok(v);
            }
        }
        if let Some(f) = value.as_f64() {
            #[allow(clippy::cast_possible_truncation)]
            let v = f as i8;
            return Ok(v);
        }
    }
    Err(format!("expected i8, got {}", json_type_name(value)))
}

fn extract_i16(value: &serde_json::Value, config: &JsonDecoderConfig) -> Result<i16, String> {
    if let Some(n) = value.as_i64() {
        if let Ok(v) = i16::try_from(n) {
            return Ok(v);
        }
        return Err(format!("integer {n} out of i16 range"));
    }
    if matches!(config.type_mismatch, TypeMismatchStrategy::Coerce) {
        if let Some(s) = value.as_str() {
            if let Ok(v) = s.parse::<i16>() {
                return Ok(v);
            }
        }
        if let Some(f) = value.as_f64() {
            #[allow(clippy::cast_possible_truncation)]
            let v = f as i16;
            return Ok(v);
        }
    }
    Err(format!("expected i16, got {}", json_type_name(value)))
}

fn extract_i32(value: &serde_json::Value, config: &JsonDecoderConfig) -> Result<i32, String> {
    if let Some(n) = value.as_i64() {
        if let Ok(v) = i32::try_from(n) {
            return Ok(v);
        }
        return Err(format!("integer {n} out of i32 range"));
    }
    if matches!(config.type_mismatch, TypeMismatchStrategy::Coerce) {
        if let Some(s) = value.as_str() {
            if let Ok(v) = s.parse::<i32>() {
                return Ok(v);
            }
        }
        if let Some(f) = value.as_f64() {
            #[allow(clippy::cast_possible_truncation)]
            return Ok(f as i32);
        }
    }
    Err(format!("expected i32, got {}", json_type_name(value)))
}

fn extract_i64(value: &serde_json::Value, config: &JsonDecoderConfig) -> Result<i64, String> {
    if let Some(n) = value.as_i64() {
        return Ok(n);
    }
    if let Some(n) = value.as_u64() {
        if let Ok(v) = i64::try_from(n) {
            return Ok(v);
        }
        return Err(format!("u64 {n} out of i64 range"));
    }
    if matches!(config.type_mismatch, TypeMismatchStrategy::Coerce) {
        if let Some(s) = value.as_str() {
            if let Ok(v) = s.parse::<i64>() {
                return Ok(v);
            }
        }
        if let Some(f) = value.as_f64() {
            #[allow(clippy::cast_possible_truncation)]
            return Ok(f as i64);
        }
    }
    Err(format!("expected i64, got {}", json_type_name(value)))
}

fn extract_f32(value: &serde_json::Value, config: &JsonDecoderConfig) -> Result<f32, String> {
    if let Some(f) = value.as_f64() {
        #[allow(clippy::cast_possible_truncation)]
        return Ok(f as f32);
    }
    if let Some(n) = value.as_i64() {
        #[allow(clippy::cast_precision_loss)]
        return Ok(n as f32);
    }
    if matches!(config.type_mismatch, TypeMismatchStrategy::Coerce) {
        if let Some(s) = value.as_str() {
            if let Ok(v) = s.parse::<f32>() {
                return Ok(v);
            }
        }
    }
    Err(format!("expected f32, got {}", json_type_name(value)))
}

fn extract_f64(value: &serde_json::Value, config: &JsonDecoderConfig) -> Result<f64, String> {
    if let Some(f) = value.as_f64() {
        return Ok(f);
    }
    if let Some(n) = value.as_i64() {
        #[allow(clippy::cast_precision_loss)]
        return Ok(n as f64);
    }
    if matches!(config.type_mismatch, TypeMismatchStrategy::Coerce) {
        if let Some(s) = value.as_str() {
            if let Ok(v) = s.parse::<f64>() {
                return Ok(v);
            }
        }
    }
    Err(format!("expected f64, got {}", json_type_name(value)))
}

fn extract_u8(value: &serde_json::Value, config: &JsonDecoderConfig) -> Result<u8, String> {
    if let Some(n) = value.as_u64() {
        if let Ok(v) = u8::try_from(n) {
            return Ok(v);
        }
        return Err(format!("integer {n} out of u8 range"));
    }
    if let Some(n) = value.as_i64() {
        if let Ok(v) = u8::try_from(n) {
            return Ok(v);
        }
        return Err(format!("integer {n} out of u8 range"));
    }
    if matches!(config.type_mismatch, TypeMismatchStrategy::Coerce) {
        if let Some(s) = value.as_str() {
            if let Ok(v) = s.parse::<u8>() {
                return Ok(v);
            }
        }
    }
    Err(format!("expected u8, got {}", json_type_name(value)))
}

fn extract_u16(value: &serde_json::Value, config: &JsonDecoderConfig) -> Result<u16, String> {
    if let Some(n) = value.as_u64() {
        if let Ok(v) = u16::try_from(n) {
            return Ok(v);
        }
        return Err(format!("integer {n} out of u16 range"));
    }
    if let Some(n) = value.as_i64() {
        if let Ok(v) = u16::try_from(n) {
            return Ok(v);
        }
        return Err(format!("integer {n} out of u16 range"));
    }
    if matches!(config.type_mismatch, TypeMismatchStrategy::Coerce) {
        if let Some(s) = value.as_str() {
            if let Ok(v) = s.parse::<u16>() {
                return Ok(v);
            }
        }
    }
    Err(format!("expected u16, got {}", json_type_name(value)))
}

fn extract_u32(value: &serde_json::Value, config: &JsonDecoderConfig) -> Result<u32, String> {
    if let Some(n) = value.as_u64() {
        if let Ok(v) = u32::try_from(n) {
            return Ok(v);
        }
        return Err(format!("integer {n} out of u32 range"));
    }
    if let Some(n) = value.as_i64() {
        if let Ok(v) = u32::try_from(n) {
            return Ok(v);
        }
        return Err(format!("integer {n} out of u32 range"));
    }
    if matches!(config.type_mismatch, TypeMismatchStrategy::Coerce) {
        if let Some(s) = value.as_str() {
            if let Ok(v) = s.parse::<u32>() {
                return Ok(v);
            }
        }
    }
    Err(format!("expected u32, got {}", json_type_name(value)))
}

fn extract_u64(value: &serde_json::Value, config: &JsonDecoderConfig) -> Result<u64, String> {
    if let Some(n) = value.as_u64() {
        return Ok(n);
    }
    if let Some(n) = value.as_i64() {
        if let Ok(v) = u64::try_from(n) {
            return Ok(v);
        }
        return Err(format!("integer {n} out of u64 range"));
    }
    if matches!(config.type_mismatch, TypeMismatchStrategy::Coerce) {
        if let Some(s) = value.as_str() {
            if let Ok(v) = s.parse::<u64>() {
                return Ok(v);
            }
        }
    }
    Err(format!("expected u64, got {}", json_type_name(value)))
}

/// Extracts a timestamp value as an i64 in the specified [`TimeUnit`].
///
/// For numeric JSON values, treats them as epoch milliseconds and converts.
/// For string values, tries the configured timestamp format patterns.
fn extract_timestamp(
    value: &serde_json::Value,
    config: &JsonDecoderConfig,
    unit: TimeUnit,
) -> Result<i64, String> {
    // Numeric values: treat as epoch milliseconds.
    if let Some(n) = value.as_i64() {
        return Ok(millis_to_unit(n, unit));
    }
    if let Some(f) = value.as_f64() {
        #[allow(clippy::cast_possible_truncation)]
        let ms = f as i64;
        return Ok(millis_to_unit(ms, unit));
    }

    // String values: try configured timestamp formats.
    if let Some(s) = value.as_str() {
        for fmt in &config.timestamp_formats {
            if fmt == "iso8601" {
                if let Ok(nanos) = arrow_cast::parse::string_to_timestamp_nanos(s) {
                    return Ok(nanos_to_unit(nanos, unit));
                }
                continue;
            }
            if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
                let nanos = ndt.and_utc().timestamp_nanos_opt().unwrap_or(0);
                return Ok(nanos_to_unit(nanos, unit));
            }
        }
        return Err(format!("cannot parse timestamp from string: {s}"));
    }

    Err(format!("expected timestamp, got {}", json_type_name(value)))
}

/// Converts epoch milliseconds to the target time unit.
fn millis_to_unit(ms: i64, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => ms / 1_000,
        TimeUnit::Millisecond => ms,
        TimeUnit::Microsecond => ms * 1_000,
        TimeUnit::Nanosecond => ms * 1_000_000,
    }
}

/// Converts nanoseconds to the target time unit.
fn nanos_to_unit(nanos: i64, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => nanos / 1_000_000_000,
        TimeUnit::Millisecond => nanos / 1_000_000,
        TimeUnit::Microsecond => nanos / 1_000,
        TimeUnit::Nanosecond => nanos,
    }
}

/// Appends a timestamp value to the appropriate builder based on [`TimeUnit`].
fn append_timestamp(builder: &mut Box<dyn ColumnBuilder>, unit: TimeUnit, value: i64) {
    match unit {
        TimeUnit::Second => {
            builder
                .as_any_mut()
                .downcast_mut::<TimestampSecondBuilder>()
                .unwrap()
                .append_value(value);
        }
        TimeUnit::Millisecond => {
            builder
                .as_any_mut()
                .downcast_mut::<TimestampMillisecondBuilder>()
                .unwrap()
                .append_value(value);
        }
        TimeUnit::Microsecond => {
            builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap()
                .append_value(value);
        }
        TimeUnit::Nanosecond => {
            builder
                .as_any_mut()
                .downcast_mut::<TimestampNanosecondBuilder>()
                .unwrap()
                .append_value(value);
        }
    }
}

fn value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
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

    fn json_record(json: &str) -> RawRecord {
        RawRecord::new(json.as_bytes().to_vec())
    }

    // ── Basic decode tests ────────────────────────────────────

    #[test]
    fn test_decode_empty_batch() {
        let schema = make_schema(vec![("id", DataType::Int64, false)]);
        let decoder = JsonDecoder::new(schema.clone());
        let batch = decoder.decode_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), schema);
    }

    #[test]
    fn test_decode_single_record() {
        let schema = make_schema(vec![
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"id": 42, "name": "Alice"}"#)];
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
    fn test_decode_multiple_records() {
        let schema = make_schema(vec![
            ("x", DataType::Int64, false),
            ("y", DataType::Float64, false),
        ]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![
            json_record(r#"{"x": 1, "y": 1.5}"#),
            json_record(r#"{"x": 2, "y": 2.5}"#),
            json_record(r#"{"x": 3, "y": 3.5}"#),
        ];
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
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(
            r#"{"bool_col": true, "int_col": 42, "float_col": 3.14, "str_col": "hello"}"#,
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
    fn test_decode_null_values() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, true),
            ("b", DataType::Utf8, true),
        ]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"a": null, "b": null}"#)];
        let batch = decoder.decode_batch(&records).unwrap();

        assert!(batch.column(0).is_null(0));
        assert!(batch.column(1).is_null(0));
    }

    #[test]
    fn test_decode_missing_field_becomes_null() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, true),
            ("b", DataType::Utf8, true),
        ]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"a": 1}"#)]; // "b" missing
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            1
        );
        assert!(batch.column(1).is_null(0));
    }

    // ── Type mismatch strategies ──────────────────────────────

    #[test]
    fn test_mismatch_null_strategy() {
        let schema = make_schema(vec![("x", DataType::Int64, true)]);
        let config = JsonDecoderConfig {
            type_mismatch: TypeMismatchStrategy::Null,
            ..Default::default()
        };
        let decoder = JsonDecoder::with_config(schema, config);
        let records = vec![json_record(r#"{"x": "not_a_number"}"#)];
        let batch = decoder.decode_batch(&records).unwrap();

        assert!(batch.column(0).is_null(0));
        assert_eq!(decoder.mismatch_count(), 1);
    }

    #[test]
    fn test_mismatch_coerce_strategy() {
        let schema = make_schema(vec![("x", DataType::Int64, true)]);
        let config = JsonDecoderConfig {
            type_mismatch: TypeMismatchStrategy::Coerce,
            ..Default::default()
        };
        let decoder = JsonDecoder::with_config(schema, config);
        let records = vec![json_record(r#"{"x": "123"}"#)];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            123
        );
    }

    #[test]
    fn test_mismatch_reject_strategy() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);
        let config = JsonDecoderConfig {
            type_mismatch: TypeMismatchStrategy::Reject,
            ..Default::default()
        };
        let decoder = JsonDecoder::with_config(schema, config);
        let records = vec![json_record(r#"{"x": "not_a_number"}"#)];
        let result = decoder.decode_batch(&records);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("type mismatch"));
    }

    // ── Unknown field strategies ──────────────────────────────

    #[test]
    fn test_unknown_fields_ignore() {
        let schema = make_schema(vec![("a", DataType::Int64, false)]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"a": 1, "unknown": "value"}"#)];
        let batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_unknown_fields_reject() {
        let schema = make_schema(vec![("a", DataType::Int64, false)]);
        let config = JsonDecoderConfig {
            unknown_fields: UnknownFieldStrategy::Reject,
            ..Default::default()
        };
        let decoder = JsonDecoder::with_config(schema, config);
        let records = vec![json_record(r#"{"a": 1, "unknown": "value"}"#)];
        let result = decoder.decode_batch(&records);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown field"));
    }

    #[test]
    fn test_unknown_fields_collect_extra() {
        let schema = make_schema(vec![("a", DataType::Int64, false)]);
        let config = JsonDecoderConfig {
            unknown_fields: UnknownFieldStrategy::CollectExtra,
            ..Default::default()
        };
        let decoder = JsonDecoder::with_config(schema, config);
        let records = vec![json_record(r#"{"a": 1, "extra1": "v1", "extra2": 42}"#)];
        let batch = decoder.decode_batch(&records).unwrap();

        // Schema should have an extra `_extra` column.
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(1).name(), "_extra");
        assert!(!batch.column(1).is_null(0));
    }

    // ── Timestamp parsing ─────────────────────────────────────

    #[test]
    fn test_decode_timestamp_iso8601() {
        let schema = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        )]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"ts": "2025-01-15T10:30:00Z"}"#)];
        let batch = decoder.decode_batch(&records).unwrap();

        assert!(!batch.column(0).is_null(0));
    }

    #[test]
    fn test_decode_timestamp_epoch_millis() {
        let schema = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"ts": 1705312200000}"#)];
        let batch = decoder.decode_batch(&records).unwrap();

        let ts_col = batch
            .column(0)
            .as_primitive::<arrow_array::types::TimestampNanosecondType>();
        // 1705312200000 ms * 1_000_000 = nanos
        assert_eq!(ts_col.value(0), 1_705_312_200_000_000_000);
    }

    // ── Nested objects as LargeBinary ─────────────────────────

    #[test]
    fn test_decode_nested_object_as_json_string() {
        let schema = make_schema(vec![("data", DataType::LargeBinary, true)]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"data": {"nested": true}}"#)];
        let batch = decoder.decode_batch(&records).unwrap();

        assert!(!batch.column(0).is_null(0));
    }

    // ── Error cases ───────────────────────────────────────────

    #[test]
    fn test_decode_invalid_json() {
        let schema = make_schema(vec![("a", DataType::Int64, false)]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![RawRecord::new(b"not json".to_vec())];
        let result = decoder.decode_batch(&records);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("JSON parse error"));
    }

    #[test]
    fn test_decode_non_object_json() {
        let schema = make_schema(vec![("a", DataType::Int64, false)]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record("[1, 2, 3]")];
        let result = decoder.decode_batch(&records);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be an object"));
    }

    // ── FormatDecoder trait ───────────────────────────────────

    #[test]
    fn test_format_name() {
        let schema = make_schema(vec![("a", DataType::Int64, false)]);
        let decoder = JsonDecoder::new(schema);
        assert_eq!(decoder.format_name(), "json");
    }

    #[test]
    fn test_output_schema() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Utf8, true),
        ]);
        let decoder = JsonDecoder::new(schema.clone());
        assert_eq!(decoder.output_schema(), schema);
    }

    #[test]
    fn test_decode_one() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);
        let decoder = JsonDecoder::new(schema);
        let record = json_record(r#"{"x": 99}"#);
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

    // ── Int/Float numeric coercion ────────────────────────────

    #[test]
    fn test_decode_int_from_float_json() {
        // JSON number 42.0 is parsed as f64 by serde_json. With the default
        // Coerce strategy, it is coerced to Int64 = 42.
        let schema = make_schema(vec![("x", DataType::Int64, true)]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"x": 42.0}"#)];
        let batch = decoder.decode_batch(&records).unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            42
        );
    }

    #[test]
    fn test_decode_float_from_int_json() {
        // JSON integer 42 should decode as Float64 = 42.0.
        let schema = make_schema(vec![("x", DataType::Float64, false)]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"x": 42}"#)];
        let batch = decoder.decode_batch(&records).unwrap();
        let val = batch
            .column(0)
            .as_primitive::<arrow_array::types::Float64Type>()
            .value(0);
        assert!((val - 42.0).abs() < f64::EPSILON);
    }

    // ── Coercion tests (string→numeric, int→float, etc.) ────

    #[test]
    fn test_decode_string_number_to_float64() {
        let schema = make_schema(vec![("price", DataType::Float64, false)]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"price": "187.52"}"#)];
        let batch = decoder.decode_batch(&records).unwrap();
        let val = batch
            .column(0)
            .as_primitive::<arrow_array::types::Float64Type>()
            .value(0);
        assert!((val - 187.52).abs() < f64::EPSILON);
    }

    #[test]
    fn test_decode_string_to_int() {
        let schema = make_schema(vec![("qty", DataType::Int32, false)]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"qty": "100"}"#)];
        let batch = decoder.decode_batch(&records).unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>()
                .value(0),
            100
        );
    }

    #[test]
    fn test_decode_epoch_millis_to_timestamp_millis() {
        let schema = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"ts": 1705312200000}"#)];
        let batch = decoder.decode_batch(&records).unwrap();
        let ts_col = batch
            .column(0)
            .as_primitive::<arrow_array::types::TimestampMillisecondType>();
        assert_eq!(ts_col.value(0), 1_705_312_200_000);
    }

    #[test]
    fn test_decode_int_to_float_promotion() {
        let schema = make_schema(vec![("val", DataType::Float64, false)]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"val": 100}"#)];
        let batch = decoder.decode_batch(&records).unwrap();
        let val = batch
            .column(0)
            .as_primitive::<arrow_array::types::Float64Type>()
            .value(0);
        assert!((val - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_decode_string_boolean() {
        let schema = make_schema(vec![("active", DataType::Boolean, false)]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"active": "true"}"#)];
        let batch = decoder.decode_batch(&records).unwrap();
        assert!(batch.column(0).as_boolean().value(0));
    }

    #[test]
    fn test_coerce_fails_on_unconvertible() {
        // With default Coerce, a string that can't be parsed as Int64 should error.
        let schema = make_schema(vec![("x", DataType::Int64, true)]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"x": "not_a_number"}"#)];
        let result = decoder.decode_batch(&records);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("type coercion failed"));
    }

    #[test]
    fn test_enforcement_str_parsing() {
        assert_eq!(
            TypeMismatchStrategy::from_enforcement_str("coerce"),
            Some(TypeMismatchStrategy::Coerce)
        );
        assert_eq!(
            TypeMismatchStrategy::from_enforcement_str("STRICT"),
            Some(TypeMismatchStrategy::Reject)
        );
        assert_eq!(
            TypeMismatchStrategy::from_enforcement_str("Permissive"),
            Some(TypeMismatchStrategy::Null)
        );
        assert_eq!(TypeMismatchStrategy::from_enforcement_str("unknown"), None);
    }

    // ── Small integer types ──────────────────────────────────

    #[test]
    fn test_decode_i8_and_u8() {
        let schema = make_schema(vec![
            ("signed", DataType::Int8, false),
            ("unsigned", DataType::UInt8, false),
        ]);
        let decoder = JsonDecoder::new(schema);
        let records = vec![json_record(r#"{"signed": -5, "unsigned": 200}"#)];
        let batch = decoder.decode_batch(&records).unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int8Type>()
                .value(0),
            -5
        );
        assert_eq!(
            batch
                .column(1)
                .as_primitive::<arrow_array::types::UInt8Type>()
                .value(0),
            200
        );
    }
}
