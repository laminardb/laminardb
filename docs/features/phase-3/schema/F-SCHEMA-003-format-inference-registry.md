# F-SCHEMA-003: Format Inference Registry

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-003 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-SCHEMA-001 (Core Schema Traits) |
| **Blocks** | F-SCHEMA-002 uses this at priority 4 (sample inference) |
| **Owner** | TBD |
| **Created** | 2026-02-21 |
| **Crate** | `laminar-connectors` |
| **Module** | `laminar-connectors/src/schema/inference.rs`, `laminar-connectors/src/schema/inference/` |

## Summary

Implement the pluggable `FormatInferenceRegistry` that hosts format-specific schema inference logic. Each wire format (JSON, CSV, raw bytes) registers a `FormatInference` implementation that can examine raw byte samples and produce an `InferredSchema` with per-field confidence scores, appearance rates, and diagnostic warnings. The registry is initialized once via `LazyLock` and is immutable at runtime.

The `SchemaInferable` trait's default `infer_from_samples()` implementation delegates to this registry, so any source that can collect samples automatically gets inference for all registered formats without format-specific code. New formats (MessagePack, CBOR, Protobuf, custom binary) register their inference logic independently -- no source code changes required.

## Goals

- Define the `FormatInference` trait with `format_name()` and `infer()` methods
- Implement `FormatInferenceRegistry` with `register()` and `get()` operations
- Define `InferenceConfig` with all tuning parameters accessible from SQL
- Define `InferredSchema`, `FieldInferenceDetail`, `InferenceWarning`, `WarningSeverity`
- Define `NumberInference` and `ArrayInference` strategy enums
- Implement built-in `JsonFormatInference` with type detection from raw bytes
- Implement built-in `CsvFormatInference` with column type sniffing
- Implement built-in `RawFormatInference` (passthrough: single `BINARY` column)
- Initialize the global registry via `LazyLock` with all built-in formats
- Wire `SchemaInferable::infer_from_samples()` default impl to the registry
- Parse `INFER SCHEMA (...)` SQL options into `InferenceConfig`

## Non-Goals

- Avro inference (Avro is self-describing or registry-backed; inference is not applicable)
- Protobuf inference (Protobuf requires a descriptor; inference from raw bytes is not feasible)
- Parquet/Iceberg inference (these are self-describing via `SchemaProvider`)
- Format decoding on the hot path (inference is Ring 2 only)
- Schema evolution (F-SCHEMA-004)
- Dynamic registry modification after startup (the registry is immutable once initialized)

## Technical Design

### FormatInference Trait

```rust
use arrow_schema::SchemaRef;

use crate::schema::traits::{
    InferenceConfig, InferredSchema, RawRecord, SchemaError, SchemaResult,
};

/// Format-specific inference logic.
///
/// Implementations examine raw byte samples and produce an Arrow schema
/// with confidence metadata. This is a Ring 2 one-time operation, separate
/// from `FormatDecoder` which handles the continuous Ring 1 hot path.
///
/// # Implementing a New Format
///
/// To add inference for a new format (e.g., MessagePack):
///
/// ```rust
/// pub struct MsgpackFormatInference;
///
/// impl FormatInference for MsgpackFormatInference {
///     fn format_name(&self) -> &str { "msgpack" }
///
///     fn infer(
///         &self,
///         samples: &[RawRecord],
///         config: &InferenceConfig,
///     ) -> SchemaResult<InferredSchema> {
///         // Parse samples as MessagePack, collect field types, merge
///         todo!()
///     }
/// }
/// ```
///
/// Then register it:
///
/// ```rust
/// registry.register(Box::new(MsgpackFormatInference));
/// ```
pub trait FormatInference: Send + Sync {
    /// The format name this inferrer handles (e.g., "json", "csv", "raw").
    ///
    /// Must be lowercase and match the format name used in SQL DDL
    /// (`FORMAT JSON`, `FORMAT CSV`, etc.).
    fn format_name(&self) -> &str;

    /// Infer an Arrow schema from raw record samples.
    ///
    /// Examines up to `config.sample_size` records (caller may provide
    /// fewer if the source ran out or timed out). Returns an
    /// `InferredSchema` with the schema, per-field details, and warnings.
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::InferenceFailed` if the samples cannot be
    /// parsed in this format (e.g., invalid JSON, malformed CSV).
    fn infer(
        &self,
        samples: &[RawRecord],
        config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema>;
}
```

### InferenceConfig

All tuning parameters for inference, corresponding 1:1 with SQL `INFER SCHEMA (...)` options.

```rust
use std::time::Duration;

use arrow_schema::DataType;

/// Configuration for sample-based schema inference.
///
/// Every field maps to a SQL option in `INFER SCHEMA (...)`.
/// Defaults are chosen for a balance of accuracy and speed.
#[derive(Debug, Clone)]
pub struct InferenceConfig {
    /// Number of records to sample. More samples improve accuracy
    /// but increase latency.
    ///
    /// SQL: `sample_size = 1000`
    pub sample_size: usize,

    /// Maximum time to spend collecting samples. The sampler stops
    /// early if this deadline is reached, even if `sample_size` records
    /// have not been collected.
    ///
    /// SQL: `sample_timeout = '5s'`
    pub sample_timeout: Duration,

    /// Default type for fields that are null in all samples.
    /// Common choices: `Utf8` (safest), `Null` (strict).
    ///
    /// SQL: `null_as = 'VARCHAR'`
    pub null_type: DataType,

    /// How to infer numeric types.
    ///
    /// SQL: `number_as = 'DOUBLE'` | `'BIGINT'` | `'DECIMAL(38,18)'`
    pub number_type: NumberInference,

    /// Timestamp format patterns to try, in order. The first pattern
    /// that successfully parses a string value wins.
    ///
    /// SQL: `timestamp_formats = ('iso8601', '%Y-%m-%d %H:%M:%S')`
    pub timestamp_formats: Vec<String>,

    /// Maximum nesting depth to infer for JSON objects. Beyond this
    /// depth, the entire subtree is stored as JSONB (opaque binary).
    ///
    /// SQL: `max_depth = 5`
    pub max_depth: usize,

    /// Strategy for inferring array element types.
    ///
    /// SQL: `array_strategy = 'first_element'` | `'union'` | `'jsonb'`
    pub array_strategy: ArrayInference,

    /// If a JSON object has more than this many distinct keys across
    /// all samples, infer `MAP<VARCHAR, ...>` instead of `STRUCT`.
    /// Inspired by DuckDB's `map_inference_threshold`.
    ///
    /// SQL: `map_threshold = 50`
    pub map_threshold: usize,

    /// Minimum fraction of samples in which a field must appear to
    /// be included in the schema. Fields below this threshold are
    /// omitted and a warning is emitted.
    ///
    /// SQL: `field_appearance_threshold = 0.1`
    pub field_appearance_threshold: f64,
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            sample_size: 1000,
            sample_timeout: Duration::from_secs(5),
            null_type: DataType::Utf8,
            number_type: NumberInference::Double,
            timestamp_formats: vec![
                "iso8601".into(),
                "%Y-%m-%d %H:%M:%S".into(),
                "%Y-%m-%dT%H:%M:%S%.fZ".into(),
            ],
            max_depth: 5,
            array_strategy: ArrayInference::FirstElement,
            map_threshold: 50,
            field_appearance_threshold: 0.1,
        }
    }
}
```

### NumberInference and ArrayInference Enums

```rust
/// Strategy for inferring numeric types from samples.
#[derive(Debug, Clone)]
pub enum NumberInference {
    /// Integers default to BIGINT, floats to DOUBLE.
    /// This is the most type-safe option.
    BigInt,
    /// All numbers default to DOUBLE (including integers).
    /// Simplest, avoids int/float ambiguity. This is the default.
    Double,
    /// All numbers default to DECIMAL(precision, scale).
    /// Best for financial data where exact precision matters.
    Decimal(u8, u8),
}

/// Strategy for inferring array element types.
#[derive(Debug, Clone)]
pub enum ArrayInference {
    /// Use the type of the first element for the entire array.
    /// Fast, but may be wrong if elements have mixed types.
    FirstElement,
    /// Compute the union of all element types across all samples.
    /// Most accurate, but may produce wide types (e.g., Utf8 if
    /// integers and strings are mixed).
    Union,
    /// Store the entire array as JSONB (opaque binary).
    /// Safest fallback when element types are unpredictable.
    Jsonb,
}
```

### InferredSchema and Per-Field Details

```rust
use arrow_schema::DataType;

/// Result of schema inference -- includes confidence information.
///
/// This is the output of `FormatInference::infer()`, consumed by
/// `SchemaResolver::resolve()` to produce the final `ResolvedSchema`.
#[derive(Debug, Clone)]
pub struct InferredSchema {
    /// The inferred Arrow schema.
    pub schema: SchemaRef,
    /// Per-field inference details (one entry per schema field).
    pub field_details: Vec<FieldInferenceDetail>,
    /// Number of samples actually used (may be fewer than requested).
    pub sample_count: usize,
    /// Fields that were ambiguous, low-confidence, or problematic.
    pub warnings: Vec<InferenceWarning>,
}

/// Per-field inference metadata.
///
/// Provides confidence scoring and diagnostic information for each
/// inferred field. Useful for `DESCRIBE SOURCE` output and debugging.
#[derive(Debug, Clone)]
pub struct FieldInferenceDetail {
    /// Field name as it will appear in the Arrow schema.
    pub field_name: String,
    /// The type chosen for this field.
    pub inferred_type: DataType,
    /// Fraction of samples where this field was present and non-null.
    /// Range: 0.0 (never seen) to 1.0 (always present).
    pub appearance_rate: f64,
    /// Confidence in the inferred type. Range: 0.0 to 1.0.
    /// - 1.0: all samples had a consistent type
    /// - 0.5: mixed types, picked the widest
    /// - 0.0: all samples were null (used `null_type` fallback)
    pub confidence: f64,
    /// All distinct types observed across samples (for diagnostics).
    /// If this contains more than one type, the field had mixed types.
    pub observed_types: Vec<DataType>,
}

/// A warning generated during schema inference.
#[derive(Debug, Clone)]
pub struct InferenceWarning {
    /// The field this warning relates to, or empty for global warnings.
    pub field_name: String,
    /// Human-readable warning message.
    pub message: String,
    /// Severity level.
    pub severity: WarningSeverity,
}

/// Severity levels for inference warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarningSeverity {
    /// Informational: inference made a reasonable choice.
    Info,
    /// Warning: inference made a choice that may need user review.
    Warning,
    /// Error: inference could not determine a type (used fallback).
    Error,
}
```

### FormatInferenceRegistry

```rust
use std::collections::HashMap;
use std::sync::LazyLock;

/// Global registry of format inference implementations.
///
/// Populated once at startup with all built-in formats. Read-only
/// afterwards (no runtime mutation). New formats register by adding
/// entries to the `new()` constructor or via feature-flag-gated
/// initialization blocks.
///
/// # Thread Safety
///
/// The registry is behind `LazyLock` and is immutable after init.
/// All methods take `&self` -- no locking required.
pub struct FormatInferenceRegistry {
    inferrers: HashMap<String, Box<dyn FormatInference>>,
}

impl FormatInferenceRegistry {
    /// Create a new registry with all built-in format inferrers.
    pub fn new() -> Self {
        let mut reg = Self {
            inferrers: HashMap::new(),
        };

        // Built-in registrations
        reg.register(Box::new(JsonFormatInference));
        reg.register(Box::new(CsvFormatInference));
        reg.register(Box::new(RawFormatInference));

        // Feature-gated registrations
        #[cfg(feature = "msgpack")]
        reg.register(Box::new(MsgpackFormatInference));

        reg
    }

    /// Register a format inference implementation.
    ///
    /// If a format with the same name is already registered,
    /// the new implementation replaces it.
    pub fn register(&mut self, inferrer: Box<dyn FormatInference>) {
        self.inferrers
            .insert(inferrer.format_name().to_string(), inferrer);
    }

    /// Look up the inference implementation for a format.
    ///
    /// Returns `None` if no inferrer is registered for the format.
    pub fn get(&self, format: &str) -> Option<&dyn FormatInference> {
        self.inferrers.get(format).map(|b| b.as_ref())
    }

    /// List all registered format names.
    pub fn registered_formats(&self) -> Vec<&str> {
        self.inferrers.keys().map(String::as_str).collect()
    }
}

/// Lazy-initialized global registry.
///
/// Thread-safe: populated once at first access, read-only afterwards.
/// All built-in formats are registered during initialization.
pub static FORMAT_INFERENCE_REGISTRY: LazyLock<FormatInferenceRegistry> =
    LazyLock::new(FormatInferenceRegistry::new);
```

### Default `infer_from_samples()` Implementation

The `SchemaInferable` trait (defined in F-SCHEMA-001) has a default implementation for `infer_from_samples()` that delegates to the registry. Sources only need to implement `sample_records()` -- format-specific inference is handled automatically.

```rust
/// Sources that support schema inference by sampling data.
///
/// The framework calls `sample_records()` to get raw data, then passes
/// it to `infer_from_samples()`. This two-step design lets the source
/// handle its own I/O (Kafka consumer, file read, WebSocket connect)
/// while the inference logic is reusable across formats.
#[async_trait]
pub trait SchemaInferable: Send + Sync {
    /// Collect a sample of raw records from the source.
    ///
    /// Implementations handle their own connection/disconnection.
    /// The returned records are raw bytes -- format-specific parsing
    /// happens in `infer_from_samples()`.
    async fn sample_records(
        &self,
        config: &SourceConfig,
        inference_config: &InferenceConfig,
    ) -> SchemaResult<Vec<RawRecord>>;

    /// Infer Arrow schema from the sampled records.
    ///
    /// Default implementation delegates to format-specific inference
    /// via the `FormatInferenceRegistry`. Override this only if your
    /// source needs custom inference logic (rare).
    async fn infer_from_samples(
        &self,
        samples: &[RawRecord],
        format: &str,
        inference_config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        let inferrer = FORMAT_INFERENCE_REGISTRY
            .get(format)
            .ok_or_else(|| {
                SchemaError::InferenceFailed(format!(
                    "No inference support for format '{format}'. \
                     Registered formats: {:?}",
                    FORMAT_INFERENCE_REGISTRY.registered_formats()
                ))
            })?;
        inferrer.infer(samples, inference_config)
    }
}
```

### Built-In Format Implementations

#### JSON Inference

```rust
/// JSON format inference.
///
/// Parses each sample as a JSON object, collects field names and types
/// across all samples, and merges them into an Arrow schema.
///
/// # Type Detection Rules
///
/// | JSON Value | Arrow Type |
/// |------------|------------|
/// | `true`/`false` | Boolean |
/// | Integer (no decimal) | Depends on `number_type` config |
/// | Float (has decimal) | Float64 |
/// | String matching timestamp pattern | Timestamp(Nanosecond, None) |
/// | Other string | Utf8 |
/// | `null` | Depends on `null_type` config |
/// | Array | List<element_type> (per `array_strategy`) |
/// | Object (keys < `map_threshold`) | Struct (recurse up to `max_depth`) |
/// | Object (keys >= `map_threshold`) | Map<Utf8, inferred_value_type> |
/// | Object (depth > `max_depth`) | LargeBinary (JSONB) |
///
/// # Ambiguity Resolution
///
/// When a field has different types across samples:
/// 1. Int + Float in different samples -> Float64
/// 2. String + non-string -> Utf8 (widest safe type)
/// 3. Null + any type -> the non-null type (nullable)
/// 4. Different object shapes -> JSONB fallback
pub struct JsonFormatInference;

impl FormatInference for JsonFormatInference {
    fn format_name(&self) -> &str {
        "json"
    }

    fn infer(
        &self,
        samples: &[RawRecord],
        config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        if samples.is_empty() {
            return Err(SchemaError::InferenceFailed(
                "No samples available for JSON inference".into(),
            ));
        }

        // Phase 1: Parse all samples and collect per-field type sets
        let mut field_types: HashMap<String, Vec<ObservedType>> = HashMap::new();
        let mut parse_errors = 0usize;

        for record in samples {
            match serde_json::from_slice::<serde_json::Value>(&record.value) {
                Ok(serde_json::Value::Object(map)) => {
                    collect_field_types(&map, "", config.max_depth, &mut field_types);
                }
                Ok(_) => {
                    parse_errors += 1; // Not a JSON object
                }
                Err(_) => {
                    parse_errors += 1;
                }
            }
        }

        let valid_samples = samples.len() - parse_errors;
        if valid_samples == 0 {
            return Err(SchemaError::InferenceFailed(
                "All samples failed JSON parsing".into(),
            ));
        }

        // Phase 2: Resolve each field to a single Arrow type
        let mut fields = Vec::new();
        let mut details = Vec::new();
        let mut warnings = Vec::new();

        for (name, types) in &field_types {
            let appearance_rate = types.len() as f64 / valid_samples as f64;
            if appearance_rate < config.field_appearance_threshold {
                warnings.push(InferenceWarning {
                    field_name: name.clone(),
                    message: format!(
                        "Field '{}' appeared in {:.0}% of samples \
                         (below {:.0}% threshold); excluded",
                        name,
                        appearance_rate * 100.0,
                        config.field_appearance_threshold * 100.0,
                    ),
                    severity: WarningSeverity::Info,
                });
                continue;
            }

            let (arrow_type, confidence, observed) =
                resolve_json_type(types, config);

            let nullable = appearance_rate < 1.0;
            fields.push(Field::new(name, arrow_type.clone(), nullable));
            details.push(FieldInferenceDetail {
                field_name: name.clone(),
                inferred_type: arrow_type,
                appearance_rate,
                confidence,
                observed_types: observed,
            });
        }

        // Sort fields alphabetically for deterministic output
        fields.sort_by(|a, b| a.name().cmp(b.name()));
        details.sort_by(|a, b| a.field_name.cmp(&b.field_name));

        if parse_errors > 0 {
            warnings.push(InferenceWarning {
                field_name: String::new(),
                message: format!(
                    "{parse_errors}/{} samples failed JSON parsing",
                    samples.len()
                ),
                severity: WarningSeverity::Warning,
            });
        }

        Ok(InferredSchema {
            schema: Arc::new(Schema::new(fields)),
            field_details: details,
            sample_count: valid_samples,
            warnings,
        })
    }
}
```

#### CSV Inference

```rust
/// CSV format inference.
///
/// Follows DuckDB's approach: for each column, attempt parsing in order
/// BOOLEAN -> BIGINT -> DOUBLE -> TIMESTAMP -> DATE -> TIME -> VARCHAR.
/// The first type that successfully parses ALL sampled values wins.
///
/// Header detection: if the first row contains non-numeric strings
/// and subsequent rows contain numeric values, the first row is
/// treated as a header.
pub struct CsvFormatInference;

impl FormatInference for CsvFormatInference {
    fn format_name(&self) -> &str {
        "csv"
    }

    fn infer(
        &self,
        samples: &[RawRecord],
        config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        // Concatenate all sample bytes (CSV records may span multiple raw records)
        // Parse with csv crate, detect header, infer per-column types
        // using the DuckDB type-ladder approach
        todo!("CSV inference implementation")
    }
}
```

#### Raw/Passthrough Inference

```rust
/// Raw format inference.
///
/// Produces a single-column schema with type `LargeBinary`. This is
/// the fallback for formats where no meaningful structure can be inferred.
pub struct RawFormatInference;

impl FormatInference for RawFormatInference {
    fn format_name(&self) -> &str {
        "raw"
    }

    fn infer(
        &self,
        samples: &[RawRecord],
        _config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::LargeBinary, false),
        ]));

        Ok(InferredSchema {
            schema,
            field_details: vec![FieldInferenceDetail {
                field_name: "payload".into(),
                inferred_type: DataType::LargeBinary,
                appearance_rate: 1.0,
                confidence: 1.0,
                observed_types: vec![DataType::LargeBinary],
            }],
            sample_count: samples.len(),
            warnings: vec![],
        })
    }
}
```

### SQL Mapping: INFER SCHEMA Options -> InferenceConfig

The `INFER SCHEMA (...)` clause in `CREATE SOURCE` maps directly to `InferenceConfig` fields.

```sql
CREATE SOURCE events
FROM KAFKA (
    brokers = 'localhost:9092',
    topic   = 'events'
)
FORMAT JSON
INFER SCHEMA (
    sample_size             = 1000,
    sample_timeout          = '5s',
    null_as                 = 'VARCHAR',
    number_as               = 'DOUBLE',
    timestamp_formats       = ('iso8601', '%Y-%m-%d %H:%M:%S', 'epoch_millis'),
    max_depth               = 5,
    array_strategy          = 'first_element',
    map_threshold           = 50,
    field_appearance_threshold = 0.1
);
```

Parser mapping:

```rust
/// Parse INFER SCHEMA options into InferenceConfig.
///
/// Each SQL option maps to exactly one InferenceConfig field.
/// Unknown options produce a parse error with available option names.
pub fn parse_inference_config(
    options: &[(String, SqlValue)],
) -> Result<InferenceConfig, ParseError> {
    let mut config = InferenceConfig::default();

    for (key, value) in options {
        match key.as_str() {
            "sample_size" => {
                config.sample_size = value.as_usize()?;
            }
            "sample_timeout" => {
                config.sample_timeout = parse_duration(value.as_str()?)?;
            }
            "null_as" => {
                config.null_type = parse_sql_type(value.as_str()?)?;
            }
            "number_as" => {
                config.number_type = match value.as_str()? {
                    "BIGINT" => NumberInference::BigInt,
                    "DOUBLE" => NumberInference::Double,
                    s if s.starts_with("DECIMAL") => {
                        let (p, s) = parse_decimal_params(s)?;
                        NumberInference::Decimal(p, s)
                    }
                    other => {
                        return Err(ParseError::InvalidOption {
                            option: "number_as".into(),
                            value: other.into(),
                            expected: "BIGINT, DOUBLE, or DECIMAL(p,s)".into(),
                        });
                    }
                };
            }
            "timestamp_formats" => {
                config.timestamp_formats = value.as_string_list()?;
            }
            "max_depth" => {
                config.max_depth = value.as_usize()?;
            }
            "array_strategy" => {
                config.array_strategy = match value.as_str()? {
                    "first_element" => ArrayInference::FirstElement,
                    "union" => ArrayInference::Union,
                    "jsonb" => ArrayInference::Jsonb,
                    other => {
                        return Err(ParseError::InvalidOption {
                            option: "array_strategy".into(),
                            value: other.into(),
                            expected: "first_element, union, or jsonb".into(),
                        });
                    }
                };
            }
            "map_threshold" => {
                config.map_threshold = value.as_usize()?;
            }
            "field_appearance_threshold" => {
                config.field_appearance_threshold = value.as_f64()?;
            }
            unknown => {
                return Err(ParseError::UnknownOption {
                    option: unknown.into(),
                    available: vec![
                        "sample_size",
                        "sample_timeout",
                        "null_as",
                        "number_as",
                        "timestamp_formats",
                        "max_depth",
                        "array_strategy",
                        "map_threshold",
                        "field_appearance_threshold",
                    ],
                });
            }
        }
    }

    Ok(config)
}
```

### Ring Integration

All inference is Ring 2 (one-time at `CREATE SOURCE`). No inference code is ever called on the Ring 0 or Ring 1 hot path.

| Operation | Ring | Latency Budget | Frequency |
|-----------|------|----------------|-----------|
| `FORMAT_INFERENCE_REGISTRY` init | Ring 2 | Microseconds (once) | Process startup |
| `FormatInference::infer()` | Ring 2 | Milliseconds-seconds | Once per CREATE SOURCE |
| `SchemaInferable::sample_records()` | Ring 2 | Seconds (network I/O) | Once per CREATE SOURCE |
| Resulting `Arc<Schema>` in decoder | Ring 1 | Zero (pre-baked) | Every batch |

### How New Formats Register

A new format registers its inference logic independently of sources. For example, a hypothetical MessagePack format:

```rust
// In a feature-gated module or external crate:
pub struct MsgpackFormatInference;

impl FormatInference for MsgpackFormatInference {
    fn format_name(&self) -> &str { "msgpack" }

    fn infer(
        &self,
        samples: &[RawRecord],
        config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        // Use rmp-serde to parse samples, detect types, build schema
        todo!()
    }
}
```

Registration happens in `FormatInferenceRegistry::new()` behind a feature gate:

```rust
#[cfg(feature = "msgpack")]
reg.register(Box::new(MsgpackFormatInference));
```

The format is then immediately available to any source:

```sql
CREATE SOURCE events
FROM WEBSOCKET (url = 'wss://feed.example.com/v1/stream')
FORMAT MSGPACK
INFER SCHEMA (sample_size = 100);
```

No changes to `WebSocketSource` are needed -- it already implements `SchemaInferable`, and the default `infer_from_samples()` automatically delegates to the `"msgpack"` entry in the registry.

### Module Structure

```
crates/laminar-connectors/src/schema/
    mod.rs               # Public re-exports (types, registry, built-in inferrers)
    inference/
        mod.rs           # FormatInference trait, FormatInferenceRegistry, LazyLock
        config.rs        # InferenceConfig, NumberInference, ArrayInference
        types.rs         # InferredSchema, FieldInferenceDetail,
                         #   InferenceWarning, WarningSeverity
        json.rs          # JsonFormatInference
        csv.rs           # CsvFormatInference
        raw.rs           # RawFormatInference
```

## Implementation Plan

### Step 1: Core Types (0.5 day)

- Define `InferenceConfig` with `Default` impl in `schema/inference/config.rs`
- Define `NumberInference`, `ArrayInference` enums
- Define `InferredSchema`, `FieldInferenceDetail`, `InferenceWarning`, `WarningSeverity` in `schema/inference/types.rs`
- Unit tests for default config construction and enum variants

### Step 2: FormatInference Trait & Registry (0.5 day)

- Define `FormatInference` trait in `schema/inference/mod.rs`
- Implement `FormatInferenceRegistry` with `register()`, `get()`, `registered_formats()`
- Set up `LazyLock` static with empty built-in list initially
- Unit tests for registry lookup, missing format, duplicate registration

### Step 3: Raw Format Inference (0.5 day)

- Implement `RawFormatInference` in `schema/inference/raw.rs`
- Register in `FormatInferenceRegistry::new()`
- Unit tests: single-column schema, correct field details

### Step 4: JSON Format Inference (1.5 days)

- Implement `JsonFormatInference` in `schema/inference/json.rs`
- JSON value type detection (bool, int, float, string, null, array, object)
- Timestamp pattern matching on string values
- Field appearance rate calculation
- Type conflict resolution (int+float -> float, string+any -> string)
- Nested object handling (struct vs. map vs. JSONB based on config)
- Array element type inference (per `array_strategy`)
- Unit tests for each type detection rule, mixed types, nested objects, arrays, edge cases

### Step 5: CSV Format Inference (1 day)

- Implement `CsvFormatInference` in `schema/inference/csv.rs`
- Header detection heuristic
- DuckDB-style type ladder: BOOLEAN -> BIGINT -> DOUBLE -> TIMESTAMP -> DATE -> TIME -> VARCHAR
- Unit tests for each type in the ladder, mixed-type columns, header detection

### Step 6: SQL Parser Integration (0.5 day)

- Implement `parse_inference_config()` in streaming parser
- Wire `INFER SCHEMA (...)` clause to `InferenceConfig` construction
- Unit tests for each SQL option, unknown option error, default values

### Step 7: Wire to SchemaInferable Default Impl (0.5 day)

- Implement the default `infer_from_samples()` on `SchemaInferable` trait
- Verify delegation to `FORMAT_INFERENCE_REGISTRY`
- Integration test: mock source + JSON format -> inferred schema

## Testing Strategy

### Unit Tests (30+)

| Category | Count | Description |
|----------|-------|-------------|
| Config defaults | 3 | Default values correct, custom construction, Duration parsing |
| Registry | 5 | Lookup existing, lookup missing, duplicate overwrite, list formats, empty registry |
| Raw inference | 3 | Single-column output, empty samples error, field details |
| JSON type detection | 8 | Boolean, int, float, string, null, timestamp, nested object, array |
| JSON ambiguity | 4 | Int+float merge, string+int merge, null+type merge, all-null field |
| JSON thresholds | 3 | Appearance threshold filters, map threshold triggers, max depth -> JSONB |
| JSON warnings | 3 | Parse errors counted, low-appearance field warned, mixed types warned |
| CSV inference | 4 | Header detection, type ladder, all-VARCHAR column, numeric column |
| SQL parsing | 4 | Full option set, partial options (defaults), unknown option error, DECIMAL params |

### Integration Tests (5+)

| Test | Description |
|------|-------------|
| `test_kafka_json_inference` | Create Kafka mock, sample 10 JSON messages, verify inferred schema |
| `test_websocket_json_inference` | WebSocket mock, sample records, verify type detection |
| `test_inference_with_wildcard` | Partial DDL + INFER SCHEMA, verify merge produces combined schema |
| `test_inference_config_from_sql` | `CREATE SOURCE ... INFER SCHEMA (sample_size=500, ...)`, verify config |
| `test_unsupported_format_error` | `FORMAT PROTOBUF INFER SCHEMA`, verify clear error message |

### Property-Based Tests

- Inferred schema always has at least one field (if samples are non-empty)
- All `FieldInferenceDetail` entries correspond to fields in the schema
- `appearance_rate` is always in [0.0, 1.0]
- `confidence` is always in [0.0, 1.0]
- `sample_count` is always <= number of input samples
- Raw inference always produces exactly one field named "payload"

## Success Criteria

- `FORMAT_INFERENCE_REGISTRY` initializes with JSON, CSV, and raw formats
- JSON inference correctly detects all primitive types from mixed samples
- CSV inference detects header rows and applies the DuckDB type ladder
- `SchemaInferable::infer_from_samples()` default impl successfully delegates to registry
- `INFER SCHEMA (...)` SQL clause parses all options with correct defaults
- Unknown format names produce a clear error listing registered formats
- `InferenceConfig::default()` produces reasonable defaults (1000 samples, 5s timeout, etc.)
- All inference runs in Ring 2; zero overhead on Ring 0/Ring 1 hot path
- 35+ tests passing across unit and integration suites

## Competitive Comparison

| Feature | LaminarDB | Flink | RisingWave | Materialize | ClickHouse | DuckDB |
|---------|-----------|-------|------------|-------------|------------|--------|
| JSON inference | Yes (sample) | No | No | No | File only | Yes (file) |
| CSV inference | Yes (sample) | No | No | No | File only | Yes (file) |
| Pluggable format registry | Yes | No | No | No | No | No |
| Inference from streaming source | Yes | No | No | No | No | No |
| Tunable inference config (SQL) | Yes | N/A | N/A | N/A | Partial | Partial |
| Appearance threshold | Yes | N/A | N/A | N/A | No | No |
| Map vs struct threshold | Yes | N/A | N/A | N/A | No | Yes |
| Per-field confidence scoring | Yes | No | No | No | No | No |
| Inference warnings | Yes | N/A | N/A | N/A | No | No |
| Third-party format plugins | Yes | No | No | No | No | Yes (extensions) |

LaminarDB is unique in supporting sample-based inference from live streaming sources (Kafka, WebSocket) with full SQL-tunable configuration. Other systems only support inference from static files (ClickHouse, DuckDB) or not at all (Flink, RisingWave, Materialize). The pluggable `FormatInferenceRegistry` pattern enables third-party format support without core code changes.

## References

- [Extensible Schema Framework](../../../research/extensible-schema-traits.md) -- Section 5 (Format Inference Registry)
- [Schema Inference Design](../../../research/schema-inference-design.md) -- Sections 2, 4 (Type Detection Rules)
- F-SCHEMA-001 (Core Schema Traits) -- `SchemaInferable`, `FormatInference` trait definitions
- F-SCHEMA-002 (Schema Resolver) -- Consumes `InferredSchema` at priority 4
- [DuckDB JSON inference](https://duckdb.org/docs/data/json/overview) -- Inspiration for type detection rules
- [DuckDB CSV sniffer](https://duckdb.org/docs/data/csv/overview) -- Inspiration for CSV type ladder

## Files

- `crates/laminar-connectors/src/schema/inference/mod.rs` -- FormatInference trait, FormatInferenceRegistry, LazyLock
- `crates/laminar-connectors/src/schema/inference/config.rs` -- InferenceConfig, NumberInference, ArrayInference
- `crates/laminar-connectors/src/schema/inference/types.rs` -- InferredSchema, FieldInferenceDetail, InferenceWarning
- `crates/laminar-connectors/src/schema/inference/json.rs` -- JsonFormatInference
- `crates/laminar-connectors/src/schema/inference/csv.rs` -- CsvFormatInference
- `crates/laminar-connectors/src/schema/inference/raw.rs` -- RawFormatInference
- `crates/laminar-sql/src/parser/streaming_parser.rs` -- INFER SCHEMA clause parsing, parse_inference_config()
