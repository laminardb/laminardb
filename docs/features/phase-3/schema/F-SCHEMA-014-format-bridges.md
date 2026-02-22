# F-SCHEMA-014: Format Bridge Functions

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-014 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-SCHEMA-001 (Core Schema Traits), F-SCHEMA-004 (JSON Decoder), F-SCHEMA-006 (Avro Decoder) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-21 |
| **Crate** | `laminar-sql` |
| **Module** | `laminar-sql/src/datafusion/format_bridge_udf.rs` |

## Summary

Add SQL-callable format bridge functions that perform inline format conversion
within queries. These functions -- `from_avro`, `to_avro`, `from_json`,
`to_json`, `from_csv`, `from_msgpack`, `parse_timestamp`, and `parse_epoch` --
are registered as DataFusion scalar UDFs and enable users to decode/encode
binary or string payloads without creating intermediate sources or sinks.

## Motivation

Production streaming pipelines frequently carry heterogeneous payloads. A
single Kafka topic may contain raw Avro bytes in one field, JSON strings in
another, and epoch timestamps that need conversion. Today, users must create
separate sources for each format. Format bridge functions let users perform
inline conversion within SQL:

```sql
-- Decode nested Avro payload within a JSON-envelope source
SELECT
    event_id,
    from_avro(payload_bytes, '{"type":"record","name":"Trade",...}') AS trade,
    parse_epoch(ts_millis, 'milliseconds') AS event_time
FROM raw_events;

-- Re-encode enriched data as JSON string for a downstream sink
SELECT to_json(STRUCT(symbol, price, enriched_sector)) AS json_payload
FROM enriched_trades;
```

Without these functions, users must either:
1. Create multiple sources/sinks with different formats
2. Handle format conversion in application code outside the SQL pipeline
3. Pre-process data before ingestion

## Goals

- Register `from_avro`, `to_avro`, `from_json`, `to_json`, `from_csv`,
  `from_msgpack`, `parse_timestamp`, and `parse_epoch` as DataFusion scalar UDFs
- Support inline Avro schema strings and schema registry references
- Support type specification strings (e.g., `'STRUCT<name VARCHAR, age INT>'`)
  for type-directed decoding
- Zero-allocation where possible on the Ring 1 decode path
- Consistent error handling with structured `SchemaError` variants
- Full documentation with SQL examples for each function

## Non-Goals

- Protobuf bridge functions (deferred to F-SCHEMA-017)
- Schema registry credential management (handled by F-CLOUD-001)
- Format auto-detection (users must specify the target format explicitly)
- Streaming format negotiation or content-type sniffing

## Technical Design

### Architecture

Format bridge functions operate in Ring 1 (format decoding layer). They are
registered as DataFusion scalar UDFs at session creation time alongside the
existing streaming UDFs (tumble, hop, session, watermark). Each function
receives its raw input (bytes or string), performs the conversion using the
existing format decoder/encoder infrastructure from `laminar-connectors`, and
returns the decoded Arrow value.

```
Ring 2: Query Planning
  SQL: from_json(payload, 'STRUCT<...>') ──> DataFusion UDF lookup
                                              │
Ring 1: Format Bridge                         ▼
  ScalarUDF::invoke(args) ──> JsonBridgeUdf.invoke()
                                │
                                ├── parse type_spec string → Arrow DataType
                                ├── decode JSON string → Arrow Struct
                                └── return ColumnarValue::Array
                                              │
Ring 0: Hot Path                              ▼
  RecordBatch with decoded struct columns
```

### Ring Integration

| Operation | Ring | Latency Budget | Notes |
|-----------|------|----------------|-------|
| UDF registration | Ring 2 | One-time at startup | `register_format_bridge_functions()` |
| Type spec parsing | Ring 2 | Milliseconds | Cached after first call per query |
| Avro schema parsing | Ring 2 | Milliseconds | Cached by schema string hash |
| JSON decode (`from_json`) | Ring 1 | Microseconds/row | Uses simd-json when available |
| Avro decode (`from_avro`) | Ring 1 | Microseconds/row | Uses apache-avro |
| CSV parse (`from_csv`) | Ring 1 | Microseconds/row | BurntSushi csv crate |
| Timestamp parse | Ring 1 | Nanoseconds/row | chrono::NaiveDateTime |
| Epoch conversion | Ring 0 | Nanoseconds/row | Pure arithmetic, no allocation |

### API Design

#### Registration

```rust
use datafusion::prelude::SessionContext;
use datafusion_expr::ScalarUDF;

/// Register all format bridge UDFs with a DataFusion session context.
///
/// Registers: from_avro, to_avro, from_json, to_json, from_csv,
/// from_msgpack, parse_timestamp, parse_epoch.
pub fn register_format_bridge_functions(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(FromAvroUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ToAvroUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(FromJsonUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ToJsonUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(FromCsvUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(FromMsgpackUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ParseTimestampUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ParseEpochUdf::new()));
}
```

#### UDF Implementations

```rust
use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// `from_json(string, type_spec)` -- Decode a JSON string into a typed struct.
///
/// # Arguments
/// - `string`: Utf8 column containing JSON strings
/// - `type_spec`: Utf8 literal, e.g. `'STRUCT<name VARCHAR, age INT>'`
///
/// # Returns
/// A Struct column with the specified fields.
///
/// # Example
/// ```sql
/// SELECT from_json(payload, 'STRUCT<name VARCHAR, age INT>') FROM raw_events;
/// ```
#[derive(Debug)]
pub struct FromJsonUdf {
    signature: Signature,
}

impl FromJsonUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for FromJsonUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "from_json" }
    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        // Return type depends on the type_spec argument.
        // At planning time we parse the literal to determine the struct schema.
        // If the type_spec is not a literal, fall back to Utf8 (runtime resolve).
        Ok(DataType::Utf8) // Placeholder; overridden by return_type_from_exprs
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let json_col = &args.args[0];
        let type_spec = &args.args[1];
        // Parse type_spec literal → Arrow DataType
        // Iterate rows in json_col, parse each JSON string → struct
        // Build and return ColumnarValue::Array
        todo!("Implementation in Phase 3D")
    }
}

/// `to_json(struct)` -- Encode a struct column as a JSON string.
#[derive(Debug)]
pub struct ToJsonUdf {
    signature: Signature,
}

/// `from_avro(bytes, schema)` -- Decode binary Avro into a typed struct.
///
/// # Arguments
/// - `bytes`: Binary column containing Avro-encoded bytes
/// - `schema`: Utf8 literal containing the Avro schema JSON string,
///   OR a schema registry reference: `'registry:http://host:8081/subject-value'`
///
/// # Example
/// ```sql
/// SELECT from_avro(raw_bytes, '{"type":"record","name":"Trade",...}')
/// FROM raw_source;
/// ```
#[derive(Debug)]
pub struct FromAvroUdf {
    signature: Signature,
}

/// `parse_timestamp(string, format, timezone)` -- Parse a string to TIMESTAMP.
///
/// # Arguments
/// - `string`: Utf8 column containing timestamp strings
/// - `format`: Utf8 literal, e.g. `'%Y-%m-%d %H:%M:%S%.f'`
/// - `timezone`: Utf8 literal, e.g. `'UTC'`, `'America/New_York'`
///
/// # Example
/// ```sql
/// SELECT parse_timestamp(ts_string, '%Y-%m-%d %H:%M:%S%.f', 'UTC')
/// FROM events;
/// ```
#[derive(Debug)]
pub struct ParseTimestampUdf {
    signature: Signature,
}

/// `parse_epoch(number, unit)` -- Convert epoch number to TIMESTAMP.
///
/// # Arguments
/// - `number`: Int64 or Float64 column containing epoch values
/// - `unit`: Utf8 literal: 'seconds', 'milliseconds', 'microseconds', 'nanoseconds'
///
/// # Example
/// ```sql
/// SELECT parse_epoch(ts_millis, 'milliseconds') AS event_time FROM events;
/// ```
#[derive(Debug)]
pub struct ParseEpochUdf {
    signature: Signature,
}
```

### SQL Interface

```sql
-- Decode binary Avro with inline schema
SELECT from_avro(raw_bytes, '{"type":"record","name":"Trade","fields":[...]}')
    AS trade
FROM raw_source;

-- Decode binary Avro with schema registry reference
SELECT from_avro(
    raw_bytes,
    'registry:http://schema-registry:8081/trades-value'
) AS trade
FROM raw_source;

-- Encode struct to Avro binary
SELECT to_avro(
    STRUCT(symbol, price, ts),
    '{"type":"record","name":"Trade","fields":[...]}'
) AS avro_bytes
FROM trades;

-- Decode JSON string to typed struct
SELECT from_json(
    payload,
    'STRUCT<name VARCHAR, age INT, scores ARRAY<DOUBLE>>'
) AS parsed
FROM text_source;

-- Encode any value to JSON string
SELECT to_json(STRUCT(id, name, scores)) AS json_out FROM parsed_data;

-- Parse CSV line to row
SELECT from_csv(line, 'id BIGINT, name VARCHAR, value DOUBLE', ',')
FROM raw_lines;

-- Decode MessagePack binary
SELECT from_msgpack(binary_data, 'STRUCT<id INT, name VARCHAR>')
FROM raw_source;

-- Parse timestamps from string
SELECT parse_timestamp(ts_string, '%Y-%m-%d %H:%M:%S%.f', 'UTC')
FROM events;

-- Parse ISO 8601 (shortcut: format = 'iso8601')
SELECT parse_timestamp(ts_string, 'iso8601', 'UTC') FROM events;

-- Convert epoch to timestamp
SELECT parse_epoch(ts_millis, 'milliseconds') AS event_time FROM events;
SELECT parse_epoch(ts_secs, 'seconds') AS event_time FROM events;
SELECT parse_epoch(ts_micros, 'microseconds') AS event_time FROM events;
SELECT parse_epoch(ts_nanos, 'nanoseconds') AS event_time FROM events;
```

### Data Structures

```rust
/// Parsed type specification from a SQL literal string.
///
/// Converts strings like `'STRUCT<name VARCHAR, age INT>'` into Arrow DataTypes.
/// Cached after first parse for a given query to avoid re-parsing on every row.
pub struct TypeSpec {
    /// The parsed Arrow DataType.
    pub data_type: DataType,
    /// Original string for diagnostics.
    pub raw: String,
}

impl TypeSpec {
    /// Parse a type specification string into an Arrow DataType.
    ///
    /// Supported formats:
    /// - `STRUCT<field1 TYPE, field2 TYPE, ...>`
    /// - `ARRAY<TYPE>`
    /// - `MAP<KEY_TYPE, VALUE_TYPE>`
    /// - Primitive types: `VARCHAR`, `INT`, `BIGINT`, `DOUBLE`, `BOOLEAN`,
    ///   `TIMESTAMP`, `DATE`, `BINARY`
    pub fn parse(spec: &str) -> Result<Self, SchemaError> {
        // Delegate to sqlparser for type parsing
        todo!()
    }
}

/// Epoch time unit for `parse_epoch`.
#[derive(Debug, Clone, Copy)]
pub enum EpochUnit {
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl EpochUnit {
    /// Parse unit string. Case-insensitive.
    pub fn from_str(s: &str) -> Result<Self, SchemaError> {
        match s.to_lowercase().as_str() {
            "seconds" | "s" => Ok(Self::Seconds),
            "milliseconds" | "ms" => Ok(Self::Milliseconds),
            "microseconds" | "us" => Ok(Self::Microseconds),
            "nanoseconds" | "ns" => Ok(Self::Nanoseconds),
            _ => Err(SchemaError::InferenceFailed(
                format!("Unknown epoch unit: '{s}'. Expected: seconds, milliseconds, microseconds, nanoseconds")
            )),
        }
    }

    /// Convert an epoch value to nanoseconds since epoch.
    pub fn to_nanos(self, value: i64) -> i64 {
        match self {
            Self::Seconds => value * 1_000_000_000,
            Self::Milliseconds => value * 1_000_000,
            Self::Microseconds => value * 1_000,
            Self::Nanoseconds => value,
        }
    }
}

/// Avro schema source for `from_avro` / `to_avro`.
#[derive(Debug, Clone)]
pub enum AvroSchemaSource {
    /// Inline Avro schema JSON string.
    Inline(String),
    /// Schema registry reference: URL + subject.
    Registry { url: String, subject: String },
}

impl AvroSchemaSource {
    /// Parse schema argument string.
    ///
    /// If the string starts with `registry:`, treat as a registry reference.
    /// Otherwise, treat as inline Avro schema JSON.
    pub fn parse(arg: &str) -> Self {
        if let Some(rest) = arg.strip_prefix("registry:") {
            // Expected format: "registry:http://host:8081/subject-name"
            if let Some(slash_pos) = rest.rfind('/') {
                let url = &rest[..slash_pos];
                let subject = &rest[slash_pos + 1..];
                Self::Registry {
                    url: url.to_string(),
                    subject: subject.to_string(),
                }
            } else {
                Self::Inline(arg.to_string())
            }
        } else {
            Self::Inline(arg.to_string())
        }
    }
}
```

### Error Handling

New error variants added to `laminar-sql/src/error/mod.rs`:

```rust
pub enum SqlError {
    // ... existing variants ...

    /// Format bridge function failed to decode input.
    #[error("format bridge decode error in {function}(): {message}")]
    FormatBridgeDecode {
        function: String,
        message: String,
    },

    /// Invalid type specification string in format bridge function.
    #[error("invalid type specification '{spec}': {reason}")]
    InvalidTypeSpec {
        spec: String,
        reason: String,
    },

    /// Invalid epoch unit in parse_epoch().
    #[error("invalid epoch unit '{unit}': expected seconds, milliseconds, microseconds, or nanoseconds")]
    InvalidEpochUnit {
        unit: String,
    },

    /// Invalid timestamp format string in parse_timestamp().
    #[error("invalid timestamp format '{format}': {reason}")]
    InvalidTimestampFormat {
        format: String,
        reason: String,
    },

    /// Schema registry unavailable for from_avro/to_avro with registry reference.
    #[error("schema registry error for {function}(): {message}")]
    SchemaRegistryBridge {
        function: String,
        message: String,
    },
}
```

## Implementation Plan

### Step 1: Type Spec Parser (1 day)

- Implement `TypeSpec::parse()` using sqlparser's data type parser
- Support `STRUCT<...>`, `ARRAY<...>`, `MAP<...>`, and all primitive types
- Unit tests for all supported type specs

### Step 2: Epoch and Timestamp Functions (0.5 day)

- Implement `ParseEpochUdf` -- pure arithmetic, no external dependencies
- Implement `ParseTimestampUdf` -- chrono-based parsing with format strings
- These are the simplest functions and serve as the template for the rest

### Step 3: JSON Bridge Functions (1 day)

- Implement `FromJsonUdf` using simd-json or serde_json
- Implement `ToJsonUdf` using serde_json serialization
- Reuse JSON decoding logic from `laminar-connectors/src/serde/json.rs`

### Step 4: Avro Bridge Functions (1 day)

- Implement `FromAvroUdf` with inline schema and registry reference support
- Implement `ToAvroUdf` using apache-avro
- Reuse Avro codec from `laminar-connectors/src/kafka/avro.rs`

### Step 5: CSV and MessagePack Functions (0.5 day)

- Implement `FromCsvUdf` with configurable delimiter
- Implement `FromMsgpackUdf` using rmp-serde
- Wire into `register_format_bridge_functions()`

### Step 6: Integration (0.5 day)

- Call `register_format_bridge_functions()` from `create_streaming_context()`
- Add to `register_streaming_functions()` and `register_streaming_functions_with_watermark()`
- End-to-end test with real Kafka + Avro payloads (feature-gated)

## Testing Strategy

| Module | Tests | What |
|--------|-------|------|
| `type_spec` | 10 | Parse all primitive types, STRUCT, ARRAY, MAP, nested types, error cases |
| `parse_epoch` | 6 | All four units, negative epochs, overflow, invalid unit string |
| `parse_timestamp` | 8 | ISO 8601, custom formats, timezone handling, invalid format, invalid input |
| `from_json` | 8 | Primitive types, nested structs, arrays, maps, null handling, type mismatch |
| `to_json` | 5 | Struct to JSON, nested, arrays, null fields, special characters |
| `from_avro` | 6 | Inline schema, nested records, union types, schema mismatch, malformed bytes |
| `to_avro` | 4 | Struct encode, nested, nullable fields, schema validation |
| `from_csv` | 5 | Basic parsing, custom delimiter, quoted fields, null handling, column mismatch |
| `from_msgpack` | 4 | Primitive types, nested structs, arrays, malformed input |
| `registration` | 2 | All UDFs registered in context, callable via SQL |
| `integration` | 3 | End-to-end: from_json in SELECT, parse_epoch in WHERE, chained bridges |

## Success Criteria

| Metric | Target | Validation |
|--------|--------|------------|
| `parse_epoch` latency | < 5ns/row | Benchmark: pure arithmetic conversion |
| `parse_timestamp` latency | < 50ns/row | Benchmark: chrono parsing with cached format |
| `from_json` throughput | > 500K rows/sec | Benchmark: simple 5-field JSON struct |
| `from_avro` throughput | > 300K rows/sec | Benchmark: 10-field Avro record |
| All 57+ tests passing | 100% | `cargo test` |
| `cargo clippy` clean | Zero warnings | CI gate |
| All public APIs documented | 100% | `#![deny(missing_docs)]` |

## Competitive Comparison

| Feature | LaminarDB | Flink | RisingWave | Materialize | ClickHouse | DuckDB |
|---------|-----------|-------|------------|-------------|------------|--------|
| `from_json()` inline | Planned | Limited (LATERAL) | No | No | `JSONExtract*` | `json_transform` |
| `to_json()` inline | Planned | No | `to_jsonb()` | `to_jsonb()` | `toJSONString` | `to_json` |
| `from_avro()` inline | Planned | No | No | No | No | No |
| `to_avro()` inline | Planned | No | No | No | No | No |
| `from_csv()` inline | Planned | No | No | No | `extractAllGroups` | `read_csv_auto` (table) |
| `from_msgpack()` inline | Planned | No | No | No | No | No |
| `parse_timestamp()` | Planned | `TO_TIMESTAMP` | `to_timestamp` | `to_timestamp` | `parseDateTimeBestEffort` | `strptime` |
| `parse_epoch()` | Planned | `FROM_UNIXTIME` | `to_timestamp` | N/A | `FROM_UNIXTIME` | `epoch_ms` |
| Schema registry bridge | Planned | No | No | No | No | No |

LaminarDB's `from_avro()` with schema registry reference is unique -- no other
streaming database offers inline Avro decoding within SQL expressions. Combined
with `from_json()` and `parse_epoch()`, users can handle heterogeneous payload
formats entirely within a single SQL query.

## Files

- `crates/laminar-sql/src/datafusion/format_bridge_udf.rs` -- NEW: All UDF implementations
- `crates/laminar-sql/src/datafusion/type_spec.rs` -- NEW: TypeSpec parser
- `crates/laminar-sql/src/datafusion/mod.rs` -- Wire into registration functions
- `crates/laminar-sql/src/error/mod.rs` -- New error variants
