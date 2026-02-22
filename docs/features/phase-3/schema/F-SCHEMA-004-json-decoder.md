# F-SCHEMA-004: JSON Format Decoder & Inference

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-004 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-SCHEMA-001 (FormatDecoder/FormatEncoder traits), F-SCHEMA-003 (FormatInference registry) |
| **Blocks** | F-SCHEMA-006 (JSONB functions), F-CONN-004 (Kafka JSON integration) |
| **Owner** | TBD |
| **Created** | 2026-02-21 |
| **Crate** | `laminar-formats` |
| **Module** | `laminar-formats/src/json/decoder.rs`, `laminar-formats/src/json/inference.rs`, `laminar-formats/src/json/jsonb.rs` |

## Summary

Implement the JSON format decoder (`JsonDecoder`) and inference engine (`JsonFormatInference`) for LaminarDB. The decoder converts raw JSON bytes into Arrow RecordBatches using simd-json for SIMD-accelerated parsing on the Ring 1 hot path. The inference engine samples JSON records and infers Arrow schemas with configurable type promotion, nested object handling, and ambiguity resolution. A companion JSONB binary format provides sub-100ns field access on the Ring 0 hot path by pre-computing byte offsets during Ring 1 decode.

## Goals

- `JsonDecoder` implementing `FormatDecoder` with batch-oriented JSON-to-Arrow conversion
- `JsonFormatInference` implementing `FormatInference` with sample-based type inference
- simd-json SIMD-accelerated parsing for Ring 1 throughput (target: >500K records/sec/core)
- serde_json fallback for platforms without SIMD support
- JSONB binary format for Ring 0 hot-path field access (<100ns per field)
- Configurable batch size (default 1024) with columnar Arrow output
- Nested object handling: STRUCT for low-cardinality keys, JSONB for high-cardinality (configurable threshold)
- Type inference priority chain: null -> BOOLEAN -> BIGINT -> DOUBLE -> TIMESTAMP -> VARCHAR
- Ambiguity resolution rules for mixed-type fields
- JSON-to-Arrow type mapping covering all JSON value types

## Non-Goals

- JSON Schema Registry integration (handled by `SchemaRegistryAware` trait in F-SCHEMA-002)
- JSON path queries and SQL functions (`->`, `->>`, `json_typeof`, etc.) (F-SCHEMA-006)
- JSON encoding for sinks (`JsonEncoder`) -- separate feature
- Protobuf or MessagePack decoding (separate format features)
- Schema evolution on live streams (handled by `SchemaEvolvable` trait)
- Full JSONPath/SQL-JSON standard implementation

## Technical Design

### Architecture

```
                    Ring 2 (One-Time)
                    ┌─────────────────────────────────────────┐
                    │  JsonFormatInference                     │
                    │  ┌───────────┐   ┌──────────────────┐  │
                    │  │ Sample    │──>│ Type Inference    │  │
                    │  │ Records   │   │ Engine            │  │
                    │  └───────────┘   └────────┬─────────┘  │
                    │                           │             │
                    │                  ┌────────▼─────────┐  │
                    │                  │ InferredSchema    │  │
                    │                  │ (Arrow SchemaRef) │  │
                    │                  └────────┬─────────┘  │
                    └──────────────────────────┼─────────────┘
                                               │ frozen
                    Ring 1 (Per-Batch)          │
                    ┌──────────────────────────┼─────────────┐
                    │  JsonDecoder              ▼             │
                    │  ┌──────────┐   ┌──────────────────┐  │
                    │  │ simd-json│──>│ Arrow Builder     │  │
                    │  │ parse    │   │ (columnar output) │  │
                    │  └──────────┘   └────────┬─────────┘  │
                    │                          │             │
                    │                 ┌────────▼─────────┐  │
                    │                 │ RecordBatch       │  │
                    │                 │ + JSONB columns   │  │
                    │                 └────────┬─────────┘  │
                    └─────────────────────────┼─────────────┘
                                              │
                    Ring 0 (Hot Path)          │
                    ┌─────────────────────────┼─────────────┐
                    │                         ▼              │
                    │  ┌──────────────────────────────────┐ │
                    │  │ JSONB binary: O(1) field access   │ │
                    │  │ via pre-computed byte offsets     │ │
                    │  │ Target: <100ns per field lookup   │ │
                    │  └──────────────────────────────────┘ │
                    └───────────────────────────────────────┘
```

### Ring Integration

| Ring | Operation | Latency Budget | Component |
|------|-----------|----------------|-----------|
| Ring 0 | JSONB field access (offset lookup) | <100ns | `JsonbAccessor` |
| Ring 0 | Arrow column read (pre-decoded) | <50ns | Standard Arrow access |
| Ring 1 | JSON parse + Arrow build (per batch) | <1ms / 1024 records | `JsonDecoder::decode_batch()` |
| Ring 1 | JSONB binary encoding (per field) | <500ns | `JsonbEncoder::encode_value()` |
| Ring 2 | Schema inference (one-time) | Seconds | `JsonFormatInference::infer()` |
| Ring 2 | Decoder construction (one-time) | Microseconds | `JsonDecoder::new()` |

### API Design

#### JsonDecoder

```rust
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// JSON decoder configuration.
#[derive(Debug, Clone)]
pub struct JsonDecoderConfig {
    /// Maximum number of records per decoded batch.
    /// Default: 1024. Must be a power of 2.
    pub batch_size: usize,

    /// Whether to use simd-json for SIMD-accelerated parsing.
    /// Falls back to serde_json if false or if SIMD is unavailable.
    /// Default: true.
    pub use_simd: bool,

    /// How to handle fields present in the JSON but absent from the schema.
    /// Default: Ignore.
    pub unknown_fields: UnknownFieldStrategy,

    /// How to handle type mismatches (e.g., string where int expected).
    /// Default: Null (insert null and increment error counter).
    pub type_mismatch: TypeMismatchStrategy,

    /// Timestamp format patterns to try when parsing string values
    /// into Timestamp columns. Tried in order; first match wins.
    pub timestamp_formats: Vec<String>,

    /// Whether to encode nested objects as JSONB binary format
    /// instead of Arrow Struct. Default: false (use Struct).
    /// When true, nested objects become LargeBinary columns
    /// with JSONB encoding for O(1) field access on Ring 0.
    pub nested_as_jsonb: bool,
}

impl Default for JsonDecoderConfig {
    fn default() -> Self {
        Self {
            batch_size: 1024,
            use_simd: true,
            unknown_fields: UnknownFieldStrategy::Ignore,
            type_mismatch: TypeMismatchStrategy::Null,
            timestamp_formats: vec![
                "%Y-%m-%dT%H:%M:%S%.fZ".into(),
                "%Y-%m-%dT%H:%M:%S%.f%:z".into(),
                "%Y-%m-%d %H:%M:%S%.f".into(),
                "%Y-%m-%d %H:%M:%S".into(),
            ],
            nested_as_jsonb: false,
        }
    }
}

/// Strategy for JSON fields not in the Arrow schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownFieldStrategy {
    /// Silently ignore unknown fields. Default.
    Ignore,
    /// Collect unknown fields into a catch-all JSONB column named `_extra`.
    CollectExtra,
    /// Return a decode error if any unknown field is encountered.
    Reject,
}

/// Strategy for JSON values that don't match the expected Arrow type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeMismatchStrategy {
    /// Insert null and increment the mismatch counter. Default.
    Null,
    /// Attempt coercion (e.g., "123" -> 123 for Int64 columns).
    Coerce,
    /// Return a decode error on the first mismatch.
    Reject,
}

/// Decodes JSON byte payloads into Arrow RecordBatches.
///
/// Constructed once at CREATE SOURCE time with a frozen Arrow schema.
/// The decoder is stateless after construction -- all schema information
/// is baked in so the Ring 1 hot path has zero schema lookups.
///
/// Uses simd-json for SIMD-accelerated parsing when available,
/// falling back to serde_json on unsupported platforms.
#[derive(Debug)]
pub struct JsonDecoder {
    /// The frozen output schema. Set at construction, never changes.
    schema: SchemaRef,

    /// Decoder configuration.
    config: JsonDecoderConfig,

    /// Pre-computed field index map: JSON field name -> Arrow column index.
    /// Avoids per-record HashMap lookups during decode.
    field_indices: Vec<(String, usize)>,

    /// JSONB encoder for nested object columns (if nested_as_jsonb is true).
    jsonb_encoder: Option<JsonbEncoder>,

    /// Cumulative count of type mismatch events (for diagnostics).
    mismatch_count: std::sync::atomic::AtomicU64,
}

impl JsonDecoder {
    /// Create a new JSON decoder for the given Arrow schema.
    ///
    /// The schema is frozen at this point. All subsequent `decode_batch`
    /// calls produce RecordBatches conforming to this schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self::with_config(schema, JsonDecoderConfig::default())
    }

    /// Create a new JSON decoder with custom configuration.
    pub fn with_config(schema: SchemaRef, config: JsonDecoderConfig) -> Self {
        let field_indices: Vec<(String, usize)> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().clone(), i))
            .collect();

        let jsonb_encoder = if config.nested_as_jsonb {
            Some(JsonbEncoder::new())
        } else {
            None
        };

        Self {
            schema,
            config,
            field_indices,
            jsonb_encoder,
            mismatch_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Return the cumulative type mismatch count.
    pub fn mismatch_count(&self) -> u64 {
        self.mismatch_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl FormatDecoder for JsonDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Decode a batch of raw JSON records into an Arrow RecordBatch.
    ///
    /// # Algorithm
    ///
    /// 1. Initialize one Arrow ArrayBuilder per schema column.
    /// 2. For each raw record:
    ///    a. Parse JSON bytes via simd-json (or serde_json fallback).
    ///    b. For each schema field, extract the corresponding JSON value.
    ///    c. Convert JSON value to the target Arrow type and append to builder.
    ///    d. If field is missing or null, append null.
    ///    e. If type mismatch, apply the configured TypeMismatchStrategy.
    /// 3. Finish all builders and assemble into RecordBatch.
    ///
    /// # Performance
    ///
    /// - simd-json parse: ~500ns per 1KB JSON document (with SIMD)
    /// - Builder append: ~10ns per field (amortized)
    /// - Total per 1024-record batch: <1ms typical
    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        // Implementation: parse JSON, map to Arrow columns, build RecordBatch
        todo!()
    }

    fn format_name(&self) -> &str {
        "json"
    }
}
```

#### JsonFormatInference

```rust
use std::collections::{HashMap, HashSet};

/// JSON-specific type inference engine.
///
/// Implements the `FormatInference` trait to infer Arrow schemas
/// from sampled JSON records. Registered in the global
/// `FormatInferenceRegistry` at startup.
///
/// # Type Inference Priority Chain
///
/// For each field, values are tested against types in this order:
///
/// ```text
/// null -> BOOLEAN -> BIGINT -> DOUBLE -> TIMESTAMP -> VARCHAR
/// ```
///
/// The narrowest type that accommodates ALL sampled values wins.
/// If values span multiple types, ambiguity resolution rules apply.
#[derive(Debug)]
pub struct JsonFormatInference;

/// Per-field type observation collected during inference.
#[derive(Debug, Clone)]
struct FieldObservation {
    /// Types observed across all samples for this field.
    observed_types: HashSet<ObservedJsonType>,
    /// Number of samples where this field was present (non-missing).
    present_count: usize,
    /// Number of samples where this field was null.
    null_count: usize,
    /// For object fields: set of all keys seen across samples.
    nested_keys: Option<HashSet<String>>,
    /// For array fields: element type observations (recursive).
    array_elements: Option<Box<FieldObservation>>,
    /// Maximum nesting depth observed for this field.
    max_depth: usize,
}

/// JSON value types observed during sampling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ObservedJsonType {
    Null,
    Boolean,
    Integer,
    Float,
    String,
    TimestampString,
    Array,
    Object,
}

impl FormatInference for JsonFormatInference {
    fn format_name(&self) -> &str {
        "json"
    }

    /// Infer an Arrow schema from sampled JSON records.
    ///
    /// # Algorithm
    ///
    /// ```text
    /// Phase 1 -- Observation (O(n * m) where n=samples, m=fields):
    ///   For each sample record:
    ///     Parse JSON into serde_json::Value
    ///     For each top-level key:
    ///       Record the observed type in FieldObservation
    ///       If object: record nested keys (up to max_depth)
    ///       If array: record element types
    ///       If string: test timestamp format patterns
    ///
    /// Phase 2 -- Resolution (O(m)):
    ///   For each observed field:
    ///     Apply type inference priority chain
    ///     Apply ambiguity resolution rules
    ///     Determine nullability from appearance_rate
    ///     For objects: STRUCT if distinct_keys <= map_threshold,
    ///                  else JSONB (LargeBinary)
    ///     For arrays: infer element type via array_strategy config
    ///
    /// Phase 3 -- Schema Assembly:
    ///   Build Arrow Schema from resolved field types
    ///   Attach field metadata (confidence, observed types)
    ///   Generate InferenceWarnings for ambiguous fields
    /// ```
    fn infer(
        &self,
        samples: &[RawRecord],
        config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        // Phase 1: Observe
        let observations = self.observe_samples(samples, config)?;

        // Phase 2: Resolve
        let resolved_fields = self.resolve_types(&observations, config)?;

        // Phase 3: Assemble
        self.assemble_schema(resolved_fields, samples.len())
    }
}

impl JsonFormatInference {
    /// Phase 1: Parse each sample and collect type observations per field.
    fn observe_samples(
        &self,
        samples: &[RawRecord],
        config: &InferenceConfig,
    ) -> SchemaResult<HashMap<String, FieldObservation>> {
        let mut observations: HashMap<String, FieldObservation> = HashMap::new();

        for sample in samples {
            let value: serde_json::Value = serde_json::from_slice(&sample.value)
                .map_err(|e| SchemaError::InferenceFailed(
                    format!("Failed to parse JSON sample: {e}")
                ))?;

            let obj = value.as_object().ok_or_else(|| {
                SchemaError::InferenceFailed(
                    "Top-level JSON value must be an object".into()
                )
            })?;

            for (key, val) in obj {
                let obs = observations.entry(key.clone()).or_insert_with(|| {
                    FieldObservation {
                        observed_types: HashSet::new(),
                        present_count: 0,
                        null_count: 0,
                        nested_keys: None,
                        array_elements: None,
                        max_depth: 0,
                    }
                });
                obs.present_count += 1;
                self.observe_value(val, obs, config, 0);
            }
        }

        Ok(observations)
    }

    /// Observe a single JSON value and update the field observation.
    fn observe_value(
        &self,
        value: &serde_json::Value,
        obs: &mut FieldObservation,
        config: &InferenceConfig,
        depth: usize,
    ) {
        match value {
            serde_json::Value::Null => {
                obs.observed_types.insert(ObservedJsonType::Null);
                obs.null_count += 1;
            }
            serde_json::Value::Bool(_) => {
                obs.observed_types.insert(ObservedJsonType::Boolean);
            }
            serde_json::Value::Number(n) => {
                if n.is_f64() && n.as_f64().map_or(false, |f| f.fract() != 0.0) {
                    obs.observed_types.insert(ObservedJsonType::Float);
                } else {
                    obs.observed_types.insert(ObservedJsonType::Integer);
                }
            }
            serde_json::Value::String(s) => {
                // Test timestamp patterns before falling back to String
                let is_ts = config.timestamp_formats.iter().any(|fmt| {
                    if fmt == "iso8601" {
                        s.parse::<chrono::DateTime<chrono::Utc>>().is_ok()
                    } else {
                        chrono::NaiveDateTime::parse_from_str(s, fmt).is_ok()
                    }
                });
                if is_ts {
                    obs.observed_types.insert(ObservedJsonType::TimestampString);
                } else {
                    obs.observed_types.insert(ObservedJsonType::String);
                }
            }
            serde_json::Value::Array(arr) => {
                obs.observed_types.insert(ObservedJsonType::Array);
                if depth < config.max_depth {
                    let elem_obs = obs.array_elements.get_or_insert_with(|| {
                        Box::new(FieldObservation {
                            observed_types: HashSet::new(),
                            present_count: 0,
                            null_count: 0,
                            nested_keys: None,
                            array_elements: None,
                            max_depth: 0,
                        })
                    });
                    for elem in arr {
                        elem_obs.present_count += 1;
                        self.observe_value(elem, elem_obs, config, depth + 1);
                    }
                }
            }
            serde_json::Value::Object(obj) => {
                obs.observed_types.insert(ObservedJsonType::Object);
                obs.max_depth = obs.max_depth.max(depth + 1);
                let keys = obs.nested_keys.get_or_insert_with(HashSet::new);
                for key in obj.keys() {
                    keys.insert(key.clone());
                }
            }
        }
    }

    /// Phase 2: Resolve each field's observations into an Arrow DataType.
    ///
    /// # Ambiguity Resolution Rules
    ///
    /// 1. Mixed int + float -> DOUBLE
    /// 2. Mixed string + timestamp_string -> VARCHAR (user may have
    ///    non-timestamp strings matching a pattern)
    /// 3. Mixed numeric + string -> VARCHAR (safest common type)
    /// 4. Mixed boolean + anything_else -> VARCHAR
    /// 5. null + exactly_one_type -> that type (nullable)
    /// 6. All nulls -> config.null_type (default VARCHAR)
    /// 7. Object with distinct_keys > map_threshold -> JSONB (LargeBinary)
    /// 8. Object with distinct_keys <= map_threshold -> STRUCT
    fn resolve_types(
        &self,
        observations: &HashMap<String, FieldObservation>,
        config: &InferenceConfig,
    ) -> SchemaResult<Vec<ResolvedField>> {
        let mut fields = Vec::with_capacity(observations.len());

        for (name, obs) in observations {
            let non_null_types: HashSet<_> = obs.observed_types.iter()
                .filter(|t| **t != ObservedJsonType::Null)
                .copied()
                .collect();

            let (data_type, confidence, warnings) = match non_null_types.len() {
                0 => {
                    // All null
                    (config.null_type.clone(), 0.5, vec![
                        InferenceWarning {
                            field_name: name.clone(),
                            message: format!(
                                "Field '{}' was null in all {} samples; \
                                 defaulting to {:?}",
                                name, obs.present_count, config.null_type
                            ),
                            severity: WarningSeverity::Warning,
                        }
                    ])
                }
                1 => {
                    // Unambiguous single type
                    let t = non_null_types.into_iter().next().unwrap();
                    let dt = self.json_type_to_arrow(t, obs, config);
                    (dt, 1.0, vec![])
                }
                _ => {
                    // Ambiguous -- apply resolution rules
                    self.resolve_ambiguity(name, &non_null_types, obs, config)
                }
            };

            let nullable = obs.null_count > 0
                || (obs.present_count as f64 / config.sample_size as f64)
                    < config.field_appearance_threshold;

            fields.push(ResolvedField {
                name: name.clone(),
                data_type,
                nullable,
                confidence,
                appearance_rate: obs.present_count as f64 / config.sample_size as f64,
                observed_types: obs.observed_types.iter().copied().collect(),
                warnings,
            });
        }

        // Sort fields alphabetically for deterministic schema ordering
        fields.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(fields)
    }

    /// Map a single unambiguous JSON type to an Arrow DataType.
    fn json_type_to_arrow(
        &self,
        json_type: ObservedJsonType,
        obs: &FieldObservation,
        config: &InferenceConfig,
    ) -> DataType {
        match json_type {
            ObservedJsonType::Null => config.null_type.clone(),
            ObservedJsonType::Boolean => DataType::Boolean,
            ObservedJsonType::Integer => match config.number_type {
                NumberInference::BigInt => DataType::Int64,
                NumberInference::Double => DataType::Float64,
                NumberInference::Decimal(p, s) => DataType::Decimal128(p, s as i8),
            },
            ObservedJsonType::Float => DataType::Float64,
            ObservedJsonType::TimestampString => {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some("UTC".into()))
            }
            ObservedJsonType::String => DataType::Utf8,
            ObservedJsonType::Array => {
                // Infer element type from array_elements observations
                match &obs.array_elements {
                    Some(elem_obs) => {
                        let elem_types: HashSet<_> = elem_obs.observed_types.iter()
                            .filter(|t| **t != ObservedJsonType::Null)
                            .copied()
                            .collect();
                        let elem_type = if elem_types.len() == 1 {
                            self.json_type_to_arrow(
                                elem_types.into_iter().next().unwrap(),
                                elem_obs,
                                config,
                            )
                        } else {
                            match config.array_strategy {
                                ArrayInference::FirstElement => {
                                    // Use first non-null type
                                    elem_types.into_iter().next()
                                        .map(|t| self.json_type_to_arrow(t, elem_obs, config))
                                        .unwrap_or(DataType::Utf8)
                                }
                                ArrayInference::Union => DataType::Utf8,
                                ArrayInference::Jsonb => DataType::LargeBinary,
                            }
                        };
                        DataType::List(Arc::new(Field::new("item", elem_type, true)))
                    }
                    None => {
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
                    }
                }
            }
            ObservedJsonType::Object => {
                let distinct_keys = obs.nested_keys.as_ref()
                    .map_or(0, |k| k.len());
                if distinct_keys > config.map_threshold || obs.max_depth >= config.max_depth {
                    // Too many keys or too deep -- store as JSONB binary
                    DataType::LargeBinary
                } else {
                    // Low cardinality -- infer as STRUCT
                    // NOTE: nested field types require recursive inference;
                    // for the initial implementation, nested structs are
                    // stored as JSONB and promoted to STRUCT in a future pass.
                    DataType::LargeBinary
                }
            }
        }
    }

    /// Resolve ambiguous field types when multiple JSON types are observed.
    fn resolve_ambiguity(
        &self,
        name: &str,
        types: &HashSet<ObservedJsonType>,
        obs: &FieldObservation,
        config: &InferenceConfig,
    ) -> (DataType, f64, Vec<InferenceWarning>) {
        let mut warnings = Vec::new();

        // Rule 1: int + float -> DOUBLE
        if types.contains(&ObservedJsonType::Integer)
            && types.contains(&ObservedJsonType::Float)
            && types.len() == 2
        {
            warnings.push(InferenceWarning {
                field_name: name.to_string(),
                message: format!(
                    "Field '{}' has mixed integer and float values; widened to DOUBLE",
                    name
                ),
                severity: WarningSeverity::Info,
            });
            return (DataType::Float64, 0.9, warnings);
        }

        // Rule 2: string + timestamp_string -> VARCHAR
        if types.contains(&ObservedJsonType::String)
            && types.contains(&ObservedJsonType::TimestampString)
            && types.len() == 2
        {
            warnings.push(InferenceWarning {
                field_name: name.to_string(),
                message: format!(
                    "Field '{}' has mixed string and timestamp-like values; \
                     defaulting to VARCHAR",
                    name
                ),
                severity: WarningSeverity::Warning,
            });
            return (DataType::Utf8, 0.7, warnings);
        }

        // Rule 3+: anything else -> VARCHAR (safest fallback)
        warnings.push(InferenceWarning {
            field_name: name.to_string(),
            message: format!(
                "Field '{}' has mixed types ({:?}); defaulting to VARCHAR",
                name, types
            ),
            severity: WarningSeverity::Warning,
        });
        (DataType::Utf8, 0.5, warnings)
    }

    /// Phase 3: Assemble the final InferredSchema from resolved fields.
    fn assemble_schema(
        &self,
        fields: Vec<ResolvedField>,
        sample_count: usize,
    ) -> SchemaResult<InferredSchema> {
        let arrow_fields: Vec<Field> = fields.iter()
            .map(|f| Field::new(&f.name, f.data_type.clone(), f.nullable))
            .collect();

        let schema = Arc::new(Schema::new(arrow_fields));

        let field_details: Vec<FieldInferenceDetail> = fields.iter()
            .map(|f| FieldInferenceDetail {
                field_name: f.name.clone(),
                inferred_type: f.data_type.clone(),
                appearance_rate: f.appearance_rate,
                confidence: f.confidence,
                observed_types: f.observed_types.iter()
                    .map(|t| self.json_type_to_arrow(*t, &FieldObservation {
                        observed_types: HashSet::new(),
                        present_count: 0,
                        null_count: 0,
                        nested_keys: None,
                        array_elements: None,
                        max_depth: 0,
                    }, &InferenceConfig::default()))
                    .collect(),
            })
            .collect();

        let warnings: Vec<InferenceWarning> = fields.into_iter()
            .flat_map(|f| f.warnings)
            .collect();

        Ok(InferredSchema {
            schema,
            field_details,
            sample_count,
            warnings,
        })
    }
}

/// Internal resolved field representation.
#[derive(Debug)]
struct ResolvedField {
    name: String,
    data_type: DataType,
    nullable: bool,
    confidence: f64,
    appearance_rate: f64,
    observed_types: Vec<ObservedJsonType>,
    warnings: Vec<InferenceWarning>,
}
```

### SQL Interface

```sql
-- Explicit schema with JSON format (no inference)
CREATE SOURCE trades (
    symbol      VARCHAR NOT NULL,
    price       DOUBLE NOT NULL,
    quantity    BIGINT NOT NULL,
    timestamp   TIMESTAMP NOT NULL,
    metadata    JSONB
) FROM KAFKA (
    brokers     = 'broker1:9092',
    topic       = 'trades',
    group_id    = 'laminar-trades'
)
FORMAT JSON;

-- JSON with auto-inference
CREATE SOURCE raw_events
FROM KAFKA (
    brokers = 'localhost:9092',
    topic   = 'events'
)
FORMAT JSON
INFER SCHEMA (
    sample_size       = 1000,
    sample_timeout    = '5s',
    null_as           = 'VARCHAR',
    number_as         = 'DOUBLE',
    timestamp_formats = ('iso8601', '%Y-%m-%d %H:%M:%S', 'epoch_millis'),
    max_depth         = 5,
    array_strategy    = 'first_element'
);

-- JSON with partial schema (wildcard inference)
CREATE SOURCE sensor_data (
    device_id   VARCHAR NOT NULL,
    temperature DOUBLE,
    *
) FROM KAFKA (
    brokers = 'localhost:9092',
    topic   = 'sensors'
)
FORMAT JSON
INFER SCHEMA (
    sample_size      = 500,
    wildcard_prefix  = 'extra_'
);

-- JSON from WebSocket
CREATE SOURCE ws_feed
FROM WEBSOCKET (
    url             = 'wss://feed.example.com/v1/stream',
    message_format  = 'json_lines'
)
FORMAT JSON
INFER SCHEMA (sample_size = 100);
```

### Data Structures

#### JSON-to-Arrow Type Mapping

| JSON Value | SQL Type (Default) | Arrow Type | Notes |
|------------|-------------------|------------|-------|
| `true`/`false` | BOOLEAN | Boolean | |
| `123` (integer) | BIGINT | Int64 | Configurable via `number_as` |
| `1.5` (float) | DOUBLE | Float64 | |
| `"hello"` | VARCHAR | Utf8 | |
| `"2025-01-01T00:00:00Z"` | TIMESTAMP | Timestamp(ns, UTC) | Only if matches timestamp pattern |
| `null` | NULL (inferred later) | Null -> fallback type | Configurable via `null_as` |
| `[1, 2, 3]` | ARRAY\<BIGINT\> | List\<Int64\> | Element type inferred |
| `{"a": 1}` (few keys) | STRUCT | Struct | When distinct keys <= `map_threshold` |
| `{"a": 1}` (many keys) | JSONB | LargeBinary | When distinct keys > `map_threshold` |

#### JSONB Binary Format

The JSONB binary format provides O(1) field access for Ring 0 hot-path operations. It is inspired by PostgreSQL's JSONB wire format but optimized for Arrow columnar storage.

```
JSONB Binary Layout (variable length):
┌────────────┬─────────────────────────────────────────────────────┐
│  Header    │  Data Region                                        │
├────────────┼─────────────────────────────────────────────────────┤
│ type_tag   │                                                     │
│ (1 byte)   │  (varies by type)                                   │
└────────────┴─────────────────────────────────────────────────────┘

Type Tags:
  0x00  Null
  0x01  Boolean false
  0x02  Boolean true
  0x03  Int64 (8 bytes, little-endian)
  0x04  Float64 (8 bytes, IEEE 754)
  0x05  String (4-byte length prefix + UTF-8 bytes)
  0x06  Array  (header + elements)
  0x07  Object (header + key-value pairs)

Object Layout (type_tag = 0x07):
┌──────────┬───────────────────────────────┬──────────────────────┐
│ field_cnt │ offset_table                  │ key_data + val_data  │
│ (4 bytes) │ (field_cnt * 8 bytes)         │ (variable)           │
│ u32 LE    │ [key_off: u32, val_off: u32]  │                      │
└──────────┴───────────────────────────────┴──────────────────────┘

  - field_cnt: Number of key-value pairs (u32 little-endian).
  - offset_table: For each field, a pair of u32 offsets:
      key_off: byte offset from start of data region to the key string.
      val_off: byte offset from start of data region to the value (another JSONB).
  - Keys are stored as length-prefixed UTF-8 strings (u16 length + bytes).
  - Keys are sorted alphabetically for binary-search field lookup.
  - Values are recursively encoded JSONB.

Array Layout (type_tag = 0x06):
┌──────────┬───────────────────────────────┬──────────────────────┐
│ elem_cnt  │ offset_table                  │ element_data         │
│ (4 bytes) │ (elem_cnt * 4 bytes)          │ (variable)           │
│ u32 LE    │ [elem_off: u32]               │                      │
└──────────┴───────────────────────────────┴──────────────────────┘

  - elem_cnt: Number of elements (u32 little-endian).
  - offset_table: For each element, a u32 byte offset to its JSONB value.
  - Elements are recursively encoded JSONB.
```

**Field Access Algorithm** (O(log n) via binary search on sorted keys):

```rust
/// JSONB accessor for Ring 0 hot-path field lookups.
///
/// All operations are zero-allocation: they return byte slices
/// into the original JSONB binary buffer.
pub struct JsonbAccessor;

impl JsonbAccessor {
    /// Access a field by name in a JSONB object.
    /// Returns a byte slice pointing to the field's JSONB value,
    /// or None if the field does not exist.
    ///
    /// Performance: O(log n) binary search on sorted keys.
    /// Typical: <100ns for objects with <100 fields.
    #[inline]
    pub fn get_field<'a>(jsonb: &'a [u8], field_name: &str) -> Option<&'a [u8]> {
        if jsonb.is_empty() || jsonb[0] != 0x07 {
            return None; // Not an object
        }

        let field_count = u32::from_le_bytes(
            jsonb[1..5].try_into().ok()?
        ) as usize;

        let offset_table_start = 5;
        let offset_table_end = offset_table_start + field_count * 8;
        let data_start = offset_table_end;

        // Binary search on sorted keys
        let mut lo = 0usize;
        let mut hi = field_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_offset = offset_table_start + mid * 8;
            let key_off = u32::from_le_bytes(
                jsonb[entry_offset..entry_offset + 4].try_into().ok()?
            ) as usize;

            let key_abs = data_start + key_off;
            let key_len = u16::from_le_bytes(
                jsonb[key_abs..key_abs + 2].try_into().ok()?
            ) as usize;
            let key_bytes = &jsonb[key_abs + 2..key_abs + 2 + key_len];
            let key_str = std::str::from_utf8(key_bytes).ok()?;

            match key_str.cmp(field_name) {
                std::cmp::Ordering::Equal => {
                    let val_off = u32::from_le_bytes(
                        jsonb[entry_offset + 4..entry_offset + 8].try_into().ok()?
                    ) as usize;
                    let val_abs = data_start + val_off;
                    // Return slice from val_abs to end of this value
                    // (caller uses type tag to determine extent)
                    return Some(&jsonb[val_abs..]);
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    /// Extract a scalar string from a JSONB value slice.
    /// Returns None if the value is not a string.
    #[inline]
    pub fn as_str<'a>(jsonb_value: &'a [u8]) -> Option<&'a str> {
        if jsonb_value.is_empty() || jsonb_value[0] != 0x05 {
            return None;
        }
        let len = u32::from_le_bytes(
            jsonb_value[1..5].try_into().ok()?
        ) as usize;
        std::str::from_utf8(&jsonb_value[5..5 + len]).ok()
    }

    /// Extract an i64 from a JSONB value slice.
    #[inline]
    pub fn as_i64(jsonb_value: &[u8]) -> Option<i64> {
        if jsonb_value.is_empty() || jsonb_value[0] != 0x03 {
            return None;
        }
        Some(i64::from_le_bytes(
            jsonb_value[1..9].try_into().ok()?
        ))
    }

    /// Extract an f64 from a JSONB value slice.
    #[inline]
    pub fn as_f64(jsonb_value: &[u8]) -> Option<f64> {
        if jsonb_value.is_empty() || jsonb_value[0] != 0x04 {
            return None;
        }
        Some(f64::from_le_bytes(
            jsonb_value[1..9].try_into().ok()?
        ))
    }

    /// Get array element count from a JSONB array.
    #[inline]
    pub fn array_len(jsonb_value: &[u8]) -> Option<usize> {
        if jsonb_value.is_empty() || jsonb_value[0] != 0x06 {
            return None;
        }
        Some(u32::from_le_bytes(
            jsonb_value[1..5].try_into().ok()?
        ) as usize)
    }
}

/// Encodes serde_json::Value into JSONB binary format.
///
/// Used in Ring 1 during JSON decode to pre-compute the binary
/// representation that Ring 0 will access.
#[derive(Debug)]
pub struct JsonbEncoder {
    /// Reusable buffer to avoid per-encode allocation.
    buf: Vec<u8>,
}

impl JsonbEncoder {
    pub fn new() -> Self {
        Self { buf: Vec::with_capacity(4096) }
    }

    /// Encode a serde_json::Value into JSONB binary format.
    /// Returns the encoded bytes.
    pub fn encode(&mut self, value: &serde_json::Value) -> Vec<u8> {
        self.buf.clear();
        self.encode_value(value);
        self.buf.clone()
    }

    fn encode_value(&mut self, value: &serde_json::Value) {
        match value {
            serde_json::Value::Null => self.buf.push(0x00),
            serde_json::Value::Bool(false) => self.buf.push(0x01),
            serde_json::Value::Bool(true) => self.buf.push(0x02),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    self.buf.push(0x03);
                    self.buf.extend_from_slice(&i.to_le_bytes());
                } else if let Some(f) = n.as_f64() {
                    self.buf.push(0x04);
                    self.buf.extend_from_slice(&f.to_le_bytes());
                }
            }
            serde_json::Value::String(s) => {
                self.buf.push(0x05);
                self.buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                self.buf.extend_from_slice(s.as_bytes());
            }
            serde_json::Value::Array(arr) => {
                self.buf.push(0x06);
                self.buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());
                // Reserve space for offset table
                let offset_table_pos = self.buf.len();
                self.buf.resize(self.buf.len() + arr.len() * 4, 0);
                let data_start = self.buf.len();
                for (i, elem) in arr.iter().enumerate() {
                    let elem_offset = (self.buf.len() - data_start) as u32;
                    let entry_pos = offset_table_pos + i * 4;
                    self.buf[entry_pos..entry_pos + 4]
                        .copy_from_slice(&elem_offset.to_le_bytes());
                    self.encode_value(elem);
                }
            }
            serde_json::Value::Object(obj) => {
                self.buf.push(0x07);
                // Sort keys for binary search
                let mut keys: Vec<_> = obj.keys().collect();
                keys.sort();
                self.buf.extend_from_slice(&(keys.len() as u32).to_le_bytes());
                // Reserve space for offset table (key_off + val_off per field)
                let offset_table_pos = self.buf.len();
                self.buf.resize(self.buf.len() + keys.len() * 8, 0);
                let data_start = self.buf.len();

                for (i, key) in keys.iter().enumerate() {
                    // Write key offset
                    let key_offset = (self.buf.len() - data_start) as u32;
                    let entry_pos = offset_table_pos + i * 8;
                    self.buf[entry_pos..entry_pos + 4]
                        .copy_from_slice(&key_offset.to_le_bytes());
                    // Write key (u16 length + UTF-8 bytes)
                    self.buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
                    self.buf.extend_from_slice(key.as_bytes());
                    // Write value offset
                    let val_offset = (self.buf.len() - data_start) as u32;
                    self.buf[entry_pos + 4..entry_pos + 8]
                        .copy_from_slice(&val_offset.to_le_bytes());
                    // Write value
                    self.encode_value(&obj[*key]);
                }
            }
        }
    }
}
```

### Algorithm / Flow

#### decode_batch Flow

```
Input: &[RawRecord]  (N raw JSON byte payloads)
Output: RecordBatch   (columnar Arrow format)

1. Initialize ArrayBuilders (one per schema column):
   - Int64Builder, Float64Builder, StringBuilder, etc.
   - Builders are reused across batches via take() + new() pattern.

2. For i in 0..N:
   a. raw_bytes = records[i].value
   b. json_value = simd_json::to_borrowed_value(&mut raw_bytes)
      - On SIMD failure: fallback to serde_json::from_slice()
   c. For each (field_name, col_idx) in self.field_indices:
      - value = json_value.get(field_name)
      - If value is None or Null:
          builder[col_idx].append_null()
      - Else:
          Match target Arrow type:
            Int64:      builder.append_value(value.as_i64()?)
            Float64:    builder.append_value(value.as_f64()?)
            Utf8:       builder.append_value(value.as_str()?)
            Boolean:    builder.append_value(value.as_bool()?)
            Timestamp:  parse string -> epoch nanos -> append
            LargeBinary (JSONB): jsonb_encoder.encode(value) -> append
            List<T>:    iterate array elements -> append to list builder
          On type mismatch:
            Null strategy:   append_null(), increment mismatch_count
            Coerce strategy: attempt string-to-target coercion
            Reject strategy: return Err(SchemaError::DecodeError)

3. Finish all builders:
   columns = builders.iter_mut().map(|b| b.finish()).collect()

4. RecordBatch::try_new(self.schema.clone(), columns)?
```

#### infer() Flow

```
Input:  &[RawRecord], &InferenceConfig
Output: InferredSchema

Phase 1 -- Observation:
  observations: HashMap<String, FieldObservation> = {}
  For each sample in samples:
    json = serde_json::from_slice(sample.value)
    For each (key, value) in json.as_object():
      obs = observations.entry(key).or_default()
      obs.present_count += 1
      Match value:
        Null    -> obs.null_count += 1
        Bool    -> obs.observed_types.insert(Boolean)
        Number  -> if has_decimal_part: insert(Float) else insert(Integer)
        String  -> if matches_timestamp_pattern: insert(TimestampString)
                   else: insert(String)
        Array   -> insert(Array), recurse on elements
        Object  -> insert(Object), collect nested keys

Phase 2 -- Resolution:
  For each (field, obs) in observations:
    non_null_types = obs.observed_types - {Null}
    If |non_null_types| == 0:  -> config.null_type (default VARCHAR)
    If |non_null_types| == 1:  -> direct mapping
    If {Integer, Float} only:  -> DOUBLE (widen)
    If {String, TimestampString}: -> VARCHAR (ambiguous)
    Otherwise:                 -> VARCHAR (safest)

    Nullability:
      nullable = obs.null_count > 0 OR appearance_rate < threshold

    Objects:
      If distinct_keys > map_threshold: -> JSONB (LargeBinary)
      If distinct_keys <= map_threshold: -> STRUCT (recursive)

Phase 3 -- Assembly:
  Sort fields alphabetically
  Build Arrow Schema from resolved types
  Collect warnings
  Return InferredSchema { schema, field_details, sample_count, warnings }
```

### Error Handling

| Error | Cause | SchemaError Variant | Recovery |
|-------|-------|-------------------|----------|
| JSON parse failure | Malformed JSON bytes | `DecodeError` | Dead-letter or skip (per source ON ERROR config) |
| Type mismatch | JSON value doesn't match schema type | `DecodeError` | Depends on `TypeMismatchStrategy`: null / coerce / reject |
| All samples null | Every sample has null for a field | `InferenceFailed` (warning) | Uses `null_as` config default type |
| Non-object top-level | JSON root is array or scalar | `InferenceFailed` | Reject sample or wrap in `{"value": ...}` |
| Max depth exceeded | Nesting deeper than `max_depth` | None (downgrades to JSONB) | Object stored as JSONB binary |
| Empty sample set | Zero records sampled in timeout | `InferenceFailed` | User must increase `sample_timeout` or check source |
| JSONB encode overflow | Value exceeds LargeBinary capacity | `DecodeError` | Should not occur in practice (LargeBinary supports 2GB) |
| SIMD parse failure | CPU does not support required SIMD instructions | None (transparent fallback) | Automatic fallback to serde_json |

## Implementation Plan

### Phase 1: Core JsonDecoder (2 days)

1. Create `laminar-formats/src/json/mod.rs` module structure
2. Implement `JsonDecoder` with serde_json parsing (no SIMD yet)
3. Implement `FormatDecoder` trait: `decode_batch()`, `decode_one()`, `output_schema()`
4. Support primitive types: Boolean, Int64, Float64, Utf8, Timestamp
5. Support nullable fields with null propagation
6. Add `JsonDecoderConfig` with `UnknownFieldStrategy` and `TypeMismatchStrategy`
7. Unit tests for all primitive type conversions

### Phase 2: JsonFormatInference (2 days)

1. Implement `JsonFormatInference` with three-phase algorithm
2. Implement observation phase with recursive value walking
3. Implement resolution phase with ambiguity rules
4. Implement assembly phase with confidence scoring
5. Register in `FormatInferenceRegistry`
6. Unit tests for each ambiguity resolution rule
7. Integration test: sample -> infer -> decode roundtrip

### Phase 3: JSONB Binary Format (2 days)

1. Implement `JsonbEncoder` for Ring 1 encode
2. Implement `JsonbAccessor` for Ring 0 field access
3. Binary format: type tags, offset tables, sorted keys
4. Wire JSONB encoding into `JsonDecoder` for nested object columns
5. Benchmark JSONB field access (<100ns target)
6. Benchmark JSONB encode throughput

### Phase 4: simd-json Integration (1 day)

1. Add simd-json dependency with feature flag
2. Replace serde_json parse with simd-json on the decode path
3. Keep serde_json for inference (one-time, correctness matters more)
4. Benchmark comparison: simd-json vs serde_json throughput
5. Fallback path when SIMD is unavailable at runtime

### Phase 5: Complex Types & Edge Cases (1 day)

1. Array type inference and decoding (List\<T\>)
2. Nested object -> STRUCT conversion (below map_threshold)
3. Mixed-type array handling per `array_strategy`
4. Timestamp format detection with configurable patterns
5. `CollectExtra` strategy for unknown fields -> `_extra` JSONB column
6. Edge case tests: empty objects, empty arrays, deeply nested, unicode keys

## Testing Strategy

### Unit Tests (30+)

| Module | Tests | Description |
|--------|-------|-------------|
| `json::decoder` | 12 | Decode primitives (bool, int, float, string, timestamp, null), nullable fields, batch of N records, type mismatch strategies (null/coerce/reject), unknown field strategies, empty batch, schema validation |
| `json::inference` | 10 | Single-type fields, mixed int/float -> DOUBLE, mixed string/timestamp -> VARCHAR, all-null -> default type, object below threshold -> STRUCT, object above threshold -> JSONB, array element inference, appearance rate / nullability, confidence scoring, deterministic field ordering |
| `json::jsonb` | 8 | Encode/decode roundtrip for all type tags, object field access by name, binary search on sorted keys, nested object access, array element access, scalar extraction (str, i64, f64), empty object, large object (100+ fields) |
| `json::simd` | 4 | simd-json parse equivalence with serde_json, fallback path, SIMD availability detection, batch throughput parity |

### Integration Tests (8)

- JSON decode from Kafka source end-to-end (explicit schema)
- JSON inference from Kafka topic (auto-infer mode)
- JSON inference with wildcard partial schema
- JSON decode with JSONB columns + Ring 0 field access
- WebSocket JSON source with inference
- Dead-letter handling for malformed JSON records
- Schema mismatch detection and error reporting
- Timestamp format auto-detection across multiple patterns

### Property Tests (4)

- `prop_decode_encode_roundtrip`: Any valid JSON object decodes to RecordBatch that re-encodes to equivalent JSON
- `prop_inference_subsumes_all_samples`: Inferred schema can decode every sample without type errors
- `prop_jsonb_field_access_matches_serde`: JSONB field access returns same values as serde_json parse
- `prop_sorted_keys_binary_search`: JSONB binary search finds every key that exists and returns None for absent keys

### Benchmarks

| Benchmark | Target | Description |
|-----------|--------|-------------|
| `bench_json_decode_batch_1024` | <1ms | Decode 1024 JSON records (~500 bytes each) to RecordBatch |
| `bench_json_decode_per_record` | <1us | Per-record decode latency |
| `bench_json_simd_vs_serde` | >2x speedup | simd-json throughput vs serde_json baseline |
| `bench_jsonb_field_access` | <100ns | Single field lookup in JSONB binary (10-field object) |
| `bench_jsonb_field_access_large` | <200ns | Single field lookup in JSONB binary (100-field object) |
| `bench_jsonb_encode` | <500ns | Encode a 10-field JSON object to JSONB binary |
| `bench_inference_1000_samples` | <500ms | Infer schema from 1000 JSON samples |
| `bench_json_throughput_per_core` | >500K rec/s | Sustained decode throughput on a single core |

## Success Criteria

- `JsonDecoder` passes all unit and integration tests
- `JsonFormatInference` correctly infers schemas for all JSON type combinations
- JSONB field access consistently <100ns (10-field objects) in benchmarks
- JSON decode throughput >500K records/sec/core with simd-json
- All ambiguity resolution rules produce correct Arrow types
- `cargo clippy -- -D warnings` clean
- Zero unsafe code (all parsing via safe crate APIs)
- Integration with `SchemaResolver` produces identical results whether schema is declared or inferred

## Competitive Comparison

| Feature | LaminarDB | Apache Flink | RisingWave | Materialize | DuckDB | ClickHouse |
|---------|-----------|-------------|------------|-------------|--------|------------|
| JSON auto-inference | Sample-based with confidence | DDL only | DDL only | DDL only | File-based | File-based |
| SIMD JSON parsing | simd-json (Ring 1) | Jackson | serde_json | serde_json | yyjson | simdjson |
| JSONB binary format | Custom (offset-based, <100ns) | None | PostgreSQL JSONB | PostgreSQL JSONB | None | None |
| Nested object handling | Configurable STRUCT/JSONB threshold | Explicit ROW type | Explicit STRUCT | Explicit JSONB | Auto-infer | Nested/JSON |
| Mixed-type resolution | Widening rules with warnings | Error | Error | Error | Widening | VARCHAR fallback |
| Wildcard schema | Partial DDL + inference | No | No | No | No | No |
| Batch decode | Columnar Arrow output | Row-based | Row-based | Row-based | Columnar | Columnar |
| Dead letter support | Configurable per-source | Side output | No | No | N/A (batch) | N/A (batch) |
| Timestamp detection | Configurable pattern list | Explicit format | Explicit format | Explicit format | Auto-detect | Explicit format |

## Files

### New Files

- `crates/laminar-formats/src/json/mod.rs` -- Module declaration and re-exports
- `crates/laminar-formats/src/json/decoder.rs` -- `JsonDecoder`, `JsonDecoderConfig`
- `crates/laminar-formats/src/json/inference.rs` -- `JsonFormatInference`, observation/resolution logic
- `crates/laminar-formats/src/json/jsonb.rs` -- `JsonbEncoder`, `JsonbAccessor`, JSONB binary format
- `crates/laminar-formats/src/json/tests.rs` -- Unit tests

### Modified Files

- `crates/laminar-formats/src/lib.rs` -- Register `json` module, add to `FormatInferenceRegistry`
- `crates/laminar-formats/Cargo.toml` -- Add `simd-json`, `serde_json`, `chrono` dependencies

### Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `simd-json` | 0.14+ | SIMD-accelerated JSON parsing (Ring 1 hot path) |
| `serde_json` | 1.x | Fallback parser, inference phase parsing |
| `chrono` | 0.4+ | Timestamp format detection and parsing |
| `arrow` | (workspace) | RecordBatch construction, ArrayBuilder APIs |

## References

- [Schema Inference Design](../../../research/schema-inference-design.md) -- Sections 4.2, 8.1-8.3
- [Extensible Schema Traits](../../../research/extensible-schema-traits.md) -- FormatDecoder, FormatInference traits
- F-SCHEMA-001 -- FormatDecoder / FormatEncoder trait definitions
- F-SCHEMA-003 -- FormatInferenceRegistry and pluggable inference
