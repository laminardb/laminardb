# F-SCHEMA-013: LaminarDB JSON Extensions

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-013 |
| **Status** | Draft |
| **Priority** | P2 |
| **Phase** | 3 |
| **Effort** | M (1-2 weeks) |
| **Dependencies** | F-SCHEMA-011 (JSON scalar functions) |
| **Owner** | TBD |
| **Created** | 2026-02-21 |
| **Updated** | 2026-02-21 |
| **Crate** | `laminar-sql` (DataFusion UDFs) |

---

## Summary

Implement streaming-specific JSON transformation functions that extend beyond the PostgreSQL standard. These functions are purpose-built for common stream processing patterns: flattening nested JSON to typed columns, inferring schemas from data, merging/patching JSON documents, stripping null fields, renaming keys, projecting/excluding key subsets, and converting between nested and dot-notation representations. All functions are registered as DataFusion UDFs. Planning-time functions (`json_to_columns`, `json_infer_schema`) resolve during query compilation in Ring 2, while runtime functions (`jsonb_merge`, `jsonb_flatten`, etc.) execute in Ring 1.

## Goals

- Schema-resolving functions: `json_to_columns` (flatten to typed columns at plan time), `json_infer_schema` (return schema as metadata)
- Document merge functions: `jsonb_merge` (shallow), `jsonb_deep_merge` (recursive)
- Cleanup function: `jsonb_strip_nulls` (remove null-valued fields)
- Key manipulation: `jsonb_rename_keys` (bulk rename via MAP), `jsonb_pick` (select key subset), `jsonb_except` (exclude key subset)
- Structure transformation: `jsonb_flatten` (nested to dot-notation), `jsonb_unflatten` (dot-notation to nested)
- All functions integrate with streaming SQL pipelines including materialized views
- Consistent error handling: return NULL on invalid input, return DataFusion error on type mismatches

## Non-Goals

- PostgreSQL-standard JSON functions (covered by F-SCHEMA-011)
- JSON table-valued / set-returning functions (covered by F-SCHEMA-012)
- Full JSONPath support (covered by F-SCHEMA-012)
- JSON Schema validation
- JSON diff / JSON patch (RFC 6902) -- potential future extension
- Binary JSONB format design (F-SCHEMA-004)
- JSON indexing or acceleration structures (Phase 4)

---

## Technical Design

### Architecture

**Ring placement**: Functions are split between planning-time (Ring 2) and execution-time (Ring 1).

- **Ring 2 (plan time)**: `json_to_columns` resolves the type specification into a concrete Arrow schema and rewrites the query to use typed column extraction. `json_infer_schema` samples data and returns a schema string.
- **Ring 1 (execution)**: All runtime functions (`jsonb_merge`, `jsonb_deep_merge`, `jsonb_strip_nulls`, `jsonb_rename_keys`, `jsonb_pick`, `jsonb_except`, `jsonb_flatten`, `jsonb_unflatten`) allocate and construct new JSONB binary values. These are not Ring 0 because they produce new JSONB values rather than reading existing ones.

**Crate**: `laminar-sql` -- all functions registered as DataFusion UDFs.

**Module**: `laminar-sql/src/datafusion/json_extensions.rs`.

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Ring 2: Query Planning                            │
│                                                                     │
│  json_to_columns(payload, 'STRUCT(user_id BIGINT, name VARCHAR)')   │
│       │                                                             │
│       v                                                             │
│  Plan-time resolution:                                              │
│    1. Parse type_spec string → Arrow schema                         │
│    2. Rewrite query: replace json_to_columns with per-field         │
│       jsonb_get_text + CAST for each column in the type spec        │
│    3. Output plan has typed columns, not JSONB                      │
│                                                                     │
│  json_infer_schema(payload)                                         │
│       │                                                             │
│       v                                                             │
│  Plan-time: sample N rows, infer types, return schema JSON string   │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                    Ring 1: Execution                                 │
│                                                                     │
│  jsonb_merge(a, b)        → allocate new JSONB with merged keys     │
│  jsonb_deep_merge(a, b)   → recursive merge, allocate new JSONB     │
│  jsonb_strip_nulls(obj)   → copy non-null fields to new JSONB       │
│  jsonb_rename_keys(obj, map) → copy with renamed keys               │
│  jsonb_pick(obj, keys)    → copy selected keys to new JSONB         │
│  jsonb_except(obj, keys)  → copy all except listed keys             │
│  jsonb_flatten(obj, sep)  → recursive flatten to dot-notation       │
│  jsonb_unflatten(obj, sep) → split keys and rebuild nested          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Ring Integration

| Ring | Responsibility | Latency Budget |
|------|---------------|----------------|
| **Ring 0** | N/A -- extension functions produce new JSONB values and therefore allocate. Ring 0 extraction operators (F-SCHEMA-011) are used internally where possible. | N/A |
| **Ring 1** | All runtime extension functions: `jsonb_merge`, `jsonb_deep_merge`, `jsonb_strip_nulls`, `jsonb_rename_keys`, `jsonb_pick`, `jsonb_except`, `jsonb_flatten`, `jsonb_unflatten`. Each allocates a new JSONB binary buffer. | < 5us for typical 20-field object |
| **Ring 2** | `json_to_columns` schema resolution and query rewriting. `json_infer_schema` sample-based inference. UDF registration. | Plan time (ms) |

### API Design

**UDF Registration** (in `laminar-sql/src/datafusion/mod.rs`):

```rust
use crate::datafusion::json_extensions::{
    JsonToColumns, JsonInferSchema,
    JsonbMerge, JsonbDeepMerge, JsonbStripNulls,
    JsonbRenameKeys, JsonbPick, JsonbExcept,
    JsonbFlatten, JsonbUnflatten,
};

/// Registers all LaminarDB JSON extension functions with the
/// given `SessionContext`.
///
/// Called from `create_streaming_context()` after the standard
/// JSON functions (F-SCHEMA-011) and TVFs (F-SCHEMA-012).
pub fn register_json_extensions(ctx: &SessionContext) {
    // Schema-resolving functions (Ring 2 plan-time)
    ctx.register_udf(ScalarUDF::new_from_impl(JsonToColumns::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonInferSchema::new()));

    // Document merge functions (Ring 1)
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbMerge::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbDeepMerge::new()));

    // Cleanup functions (Ring 1)
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbStripNulls::new()));

    // Key manipulation functions (Ring 1)
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbRenameKeys::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbPick::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbExcept::new()));

    // Structure transformation functions (Ring 1)
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbFlatten::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbUnflatten::new()));
}
```

**Schema-Resolving Functions**:

```rust
use std::any::Any;
use std::hash::{Hash, Hasher};

use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};

/// `json_to_columns(jsonb, type_spec) -> typed columns`
///
/// Flattens a JSONB value into typed columns based on a type
/// specification string. Schema resolution happens at plan time
/// (Ring 2): the type_spec is parsed into an Arrow schema, and
/// the query is rewritten to extract each field individually
/// using `jsonb_get_text` + `CAST`.
///
/// This is similar to DuckDB's `json_transform` but designed
/// for streaming: the schema is frozen at planning time and
/// never changes during execution.
///
/// # SQL Usage
///
/// ```sql
/// SELECT json_to_columns(
///     payload,
///     'STRUCT(user_id BIGINT, name VARCHAR, address STRUCT(city VARCHAR, zip VARCHAR))'
/// ) AS (user_id, name, address)
/// FROM raw_events;
/// ```
///
/// # Plan-Time Rewriting
///
/// The planner rewrites the above to:
///
/// ```sql
/// SELECT
///     CAST(payload ->> 'user_id' AS BIGINT) AS user_id,
///     payload ->> 'name' AS name,
///     json_to_columns(
///         payload -> 'address',
///         'STRUCT(city VARCHAR, zip VARCHAR)'
///     ) AS address
/// FROM raw_events;
/// ```
#[derive(Debug)]
pub struct JsonToColumns {
    signature: Signature,
}

impl JsonToColumns {
    /// Creates a new `json_to_columns` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::LargeBinary, // JSONB input
                    DataType::Utf8,        // type specification string
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonToColumns {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonToColumns {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonToColumns {}

impl Hash for JsonToColumns {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "json_to_columns".hash(state);
    }
}

impl ScalarUDFImpl for JsonToColumns {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "json_to_columns"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Return type is determined at plan time based on the type_spec.
        // The planner rewrites this call before execution, so this
        // fallback returns Struct with an empty field list.
        Ok(DataType::Struct(arrow::datatypes::Fields::empty()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // This should be rewritten at plan time. If it reaches execution,
        // fall back to runtime extraction using the type spec.
        todo!("Plan-time rewriting should handle this; runtime fallback")
    }
}

/// `json_infer_schema(jsonb) -> text`
///
/// Returns the inferred schema of a JSONB value as a JSON string
/// describing the type structure. This is a planning-time helper
/// for schema exploration.
///
/// # SQL Usage
///
/// ```sql
/// SELECT json_infer_schema(payload) FROM raw_events LIMIT 1;
/// -- Returns: '{"user_id": "BIGINT", "name": "VARCHAR", "tags": "ARRAY<VARCHAR>"}'
/// ```
#[derive(Debug)]
pub struct JsonInferSchema {
    signature: Signature,
}

impl JsonInferSchema {
    /// Creates a new `json_infer_schema` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonInferSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonInferSchema {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonInferSchema {}

impl Hash for JsonInferSchema {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "json_infer_schema".hash(state);
    }
}

impl ScalarUDFImpl for JsonInferSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "json_infer_schema"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8) // Returns schema as JSON text
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // 1. Read JSONB binary value
        // 2. Traverse structure, inferring types for each field
        // 3. Return JSON object mapping field names to SQL type names
        todo!("Schema inference from JSONB binary")
    }
}
```

**Runtime Transformation Functions**:

```rust
/// `jsonb_merge(jsonb, jsonb) -> jsonb`
///
/// Performs a shallow merge of two JSONB objects. Keys from the
/// second argument override keys from the first. Non-object inputs
/// return the second argument (last-value-wins).
///
/// # SQL Usage
///
/// ```sql
/// SELECT jsonb_merge(
///     '{"a": 1, "b": 2}'::jsonb,
///     '{"b": 3, "c": 4}'::jsonb
/// );
/// -- Returns: {"a": 1, "b": 3, "c": 4}
/// ```
#[derive(Debug)]
pub struct JsonbMerge {
    signature: Signature,
}

impl JsonbMerge {
    /// Creates a new `jsonb_merge` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::LargeBinary,
                    DataType::LargeBinary,
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonbMerge {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbMerge {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbMerge {}

impl Hash for JsonbMerge {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_merge".hash(state);
    }
}

impl ScalarUDFImpl for JsonbMerge {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_merge"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary) // Returns JSONB
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // 1. Parse both JSONB binary values
        // 2. Iterate keys from first, copy to output
        // 3. Iterate keys from second, overwrite/add to output
        // 4. Serialize merged object to JSONB binary
        todo!("Shallow merge implementation")
    }
}

/// `jsonb_deep_merge(jsonb, jsonb) -> jsonb`
///
/// Performs a recursive merge of two JSONB objects. When both
/// sides have an object at the same key, the objects are merged
/// recursively. For non-object values, the second argument wins.
///
/// # SQL Usage
///
/// ```sql
/// SELECT jsonb_deep_merge(
///     '{"user": {"name": "Alice", "prefs": {"theme": "dark"}}}'::jsonb,
///     '{"user": {"prefs": {"lang": "en"}, "age": 30}}'::jsonb
/// );
/// -- Returns: {"user": {"name": "Alice", "prefs": {"theme": "dark", "lang": "en"}, "age": 30}}
/// ```
#[derive(Debug)]
pub struct JsonbDeepMerge {
    signature: Signature,
}

impl JsonbDeepMerge {
    /// Creates a new `jsonb_deep_merge` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::LargeBinary,
                    DataType::LargeBinary,
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

/// `jsonb_strip_nulls(jsonb) -> jsonb`
///
/// Removes all object fields whose value is JSON null.
/// Operates recursively on nested objects.
///
/// # SQL Usage
///
/// ```sql
/// SELECT jsonb_strip_nulls('{"a": 1, "b": null, "c": {"d": null, "e": 2}}'::jsonb);
/// -- Returns: {"a": 1, "c": {"e": 2}}
/// ```
#[derive(Debug)]
pub struct JsonbStripNulls {
    signature: Signature,
}

impl JsonbStripNulls {
    /// Creates a new `jsonb_strip_nulls` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                Volatility::Immutable,
            ),
        }
    }
}

/// `jsonb_rename_keys(jsonb, map) -> jsonb`
///
/// Renames top-level keys in a JSONB object according to the
/// provided mapping. Keys not in the map are preserved unchanged.
///
/// The map is provided as alternating old/new key pairs (similar
/// to `json_build_object` argument style).
///
/// # SQL Usage
///
/// ```sql
/// SELECT jsonb_rename_keys(
///     payload,
///     MAP['old_field', 'new_field', 'ts', 'event_time']
/// ) FROM events;
/// ```
#[derive(Debug)]
pub struct JsonbRenameKeys {
    signature: Signature,
}

impl JsonbRenameKeys {
    /// Creates a new `jsonb_rename_keys` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::LargeBinary, // JSONB input
                    DataType::Map(
                        Box::new(arrow::datatypes::Field::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    arrow::datatypes::Field::new("key", DataType::Utf8, false),
                                    arrow::datatypes::Field::new("value", DataType::Utf8, false),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

/// `jsonb_pick(jsonb, keys_array) -> jsonb`
///
/// Returns a new JSONB object containing only the specified keys.
/// Keys not present in the input are omitted from the output.
///
/// # SQL Usage
///
/// ```sql
/// SELECT jsonb_pick(payload, ARRAY['user_id', 'timestamp', 'action'])
/// FROM events;
/// -- Input:  {"user_id": 123, "timestamp": "...", "action": "click", "debug": true}
/// -- Output: {"user_id": 123, "timestamp": "...", "action": "click"}
/// ```
#[derive(Debug)]
pub struct JsonbPick {
    signature: Signature,
}

impl JsonbPick {
    /// Creates a new `jsonb_pick` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::LargeBinary,                    // JSONB input
                    DataType::List(Box::new(
                        arrow::datatypes::Field::new("item", DataType::Utf8, false),
                    ).into()),  // Array of key names
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

/// `jsonb_except(jsonb, keys_array) -> jsonb`
///
/// Returns a new JSONB object with the specified keys removed.
/// The inverse of `jsonb_pick`.
///
/// # SQL Usage
///
/// ```sql
/// SELECT jsonb_except(payload, ARRAY['internal_id', 'debug_info'])
/// FROM events;
/// -- Input:  {"user_id": 123, "internal_id": "xyz", "data": 42, "debug_info": {...}}
/// -- Output: {"user_id": 123, "data": 42}
/// ```
#[derive(Debug)]
pub struct JsonbExcept {
    signature: Signature,
}

impl JsonbExcept {
    /// Creates a new `jsonb_except` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::LargeBinary,
                    DataType::List(Box::new(
                        arrow::datatypes::Field::new("item", DataType::Utf8, false),
                    ).into()),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

/// `jsonb_flatten(jsonb, separator) -> jsonb`
///
/// Flattens a nested JSONB object into a single-level object
/// using the separator to join nested key paths. Arrays are
/// flattened with numeric indices.
///
/// # SQL Usage
///
/// ```sql
/// SELECT jsonb_flatten(
///     '{"user": {"name": "Alice", "address": {"city": "London"}}, "tags": ["a", "b"]}'::jsonb,
///     '.'
/// );
/// -- Returns: {"user.name": "Alice", "user.address.city": "London", "tags.0": "a", "tags.1": "b"}
/// ```
#[derive(Debug)]
pub struct JsonbFlatten {
    signature: Signature,
}

impl JsonbFlatten {
    /// Creates a new `jsonb_flatten` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::LargeBinary, // JSONB input
                    DataType::Utf8,        // separator string
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

/// `jsonb_unflatten(jsonb, separator) -> jsonb`
///
/// Reverses `jsonb_flatten`: takes a flat JSONB object with
/// dot-notation keys and rebuilds the nested structure.
///
/// # SQL Usage
///
/// ```sql
/// SELECT jsonb_unflatten(
///     '{"user.name": "Alice", "user.address.city": "London"}'::jsonb,
///     '.'
/// );
/// -- Returns: {"user": {"name": "Alice", "address": {"city": "London"}}}
/// ```
#[derive(Debug)]
pub struct JsonbUnflatten {
    signature: Signature,
}

impl JsonbUnflatten {
    /// Creates a new `jsonb_unflatten` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::LargeBinary,
                    DataType::Utf8,
                ]),
                Volatility::Immutable,
            ),
        }
    }
}
```

### SQL Interface

**Schema Resolution**:

```sql
-- Flatten JSON to typed columns (schema resolved at plan time)
SELECT json_to_columns(
    payload,
    'STRUCT(user_id BIGINT, name VARCHAR, address STRUCT(city VARCHAR, zip VARCHAR))'
) AS (user_id, name, address)
FROM raw_events;

-- Result columns are typed:
-- | user_id (BIGINT) | name (VARCHAR) | address (STRUCT)         |
-- |------------------|----------------|--------------------------|
-- | 12345            | Alice          | {city: London, zip: SW1} |

-- Infer schema from data
SELECT json_infer_schema(payload) FROM raw_events LIMIT 1;
-- Returns: '{"user_id": "BIGINT", "name": "VARCHAR", "tags": "ARRAY<VARCHAR>"}'

-- Use inferred schema to create a typed view
-- (workflow: infer first, then use explicit schema)
CREATE MATERIALIZED VIEW typed_events AS
SELECT json_to_columns(
    payload,
    'STRUCT(user_id BIGINT, name VARCHAR, tags ARRAY<VARCHAR>)'
) AS (user_id, name, tags)
FROM raw_events;
```

**Document Merge**:

```sql
-- Shallow merge: override defaults with user config
SELECT jsonb_merge(
    '{"theme": "light", "lang": "en", "notifications": true}'::jsonb,
    user_preferences
) AS effective_config
FROM users;
-- If user_preferences = '{"theme": "dark"}'
-- Returns: {"theme": "dark", "lang": "en", "notifications": true}

-- Deep merge: merge nested config hierarchies
SELECT jsonb_deep_merge(base_config, override_config) AS config
FROM config_sources;
-- base_config:     {"db": {"host": "localhost", "port": 5432, "pool": {"min": 5, "max": 20}}}
-- override_config: {"db": {"host": "prod-db", "pool": {"max": 100}}}
-- Returns:         {"db": {"host": "prod-db", "port": 5432, "pool": {"min": 5, "max": 100}}}

-- Streaming pattern: enrich events with default metadata
CREATE MATERIALIZED VIEW enriched_events AS
SELECT
    event_id,
    jsonb_merge(
        '{"source": "laminar", "version": "1.0"}'::jsonb,
        payload
    ) AS enriched_payload
FROM raw_events;
```

**Cleanup**:

```sql
-- Remove null fields before sinking to external system
SELECT jsonb_strip_nulls(payload) AS clean_payload
FROM events;
-- Input:  {"a": 1, "b": null, "c": {"d": null, "e": 2}}
-- Output: {"a": 1, "c": {"e": 2}}

-- Streaming pipeline: clean nulls before Kafka sink
CREATE MATERIALIZED VIEW clean_events AS
SELECT
    event_id,
    jsonb_strip_nulls(payload) AS payload
FROM raw_events;
```

**Key Manipulation**:

```sql
-- Rename fields for downstream compatibility
SELECT jsonb_rename_keys(
    payload,
    MAP['old_field', 'new_field', 'ts', 'event_time', 'uid', 'user_id']
) AS renamed
FROM legacy_events;
-- Input:  {"old_field": 1, "ts": "2026-02-21", "uid": 123, "data": "x"}
-- Output: {"new_field": 1, "event_time": "2026-02-21", "user_id": 123, "data": "x"}

-- Select only needed fields (projection)
SELECT jsonb_pick(payload, ARRAY['user_id', 'action', 'timestamp'])
FROM events;
-- Input:  {"user_id": 123, "action": "click", "timestamp": "...", "debug": true, "internal": {...}}
-- Output: {"user_id": 123, "action": "click", "timestamp": "..."}

-- Remove sensitive/internal fields before external sink
SELECT jsonb_except(payload, ARRAY['internal_id', 'debug_info', 'raw_bytes'])
FROM events;
-- Input:  {"user_id": 123, "internal_id": "xyz", "data": 42, "debug_info": {...}, "raw_bytes": "..."}
-- Output: {"user_id": 123, "data": 42}

-- Streaming pipeline: sanitize before Kafka sink
CREATE MATERIALIZED VIEW sanitized_events AS
SELECT
    event_id,
    jsonb_except(payload, ARRAY['pii_name', 'pii_email', 'pii_phone']) AS payload
FROM raw_events;
```

**Structure Transformation**:

```sql
-- Flatten nested JSON for flat storage (e.g., CSV sink)
SELECT jsonb_flatten(payload, '.') AS flat
FROM events;
-- Input:  {"user": {"name": "Alice", "address": {"city": "London"}}, "tags": ["a", "b"]}
-- Output: {"user.name": "Alice", "user.address.city": "London", "tags.0": "a", "tags.1": "b"}

-- Unflatten dot-notation back to nested (e.g., from flat CSV source)
SELECT jsonb_unflatten(flat_data, '.') AS nested
FROM flat_events;
-- Input:  {"user.name": "Alice", "user.address.city": "London"}
-- Output: {"user": {"name": "Alice", "address": {"city": "London"}}}

-- Streaming pattern: flatten for analytics, unflatten for API
CREATE MATERIALIZED VIEW flat_for_analytics AS
SELECT event_id, jsonb_flatten(payload, '.') AS flat_payload
FROM raw_events;

CREATE MATERIALIZED VIEW nested_for_api AS
SELECT event_id, jsonb_unflatten(flat_payload, '.') AS payload
FROM flat_for_analytics;
```

### Data Structures

```rust
/// Type specification parser for `json_to_columns`.
///
/// Parses strings like `'STRUCT(user_id BIGINT, name VARCHAR,
/// address STRUCT(city VARCHAR, zip VARCHAR))'` into an Arrow
/// schema at plan time.
#[derive(Debug)]
pub struct TypeSpecParser;

impl TypeSpecParser {
    /// Parses a type specification string into an Arrow `Fields`.
    ///
    /// Supports:
    /// - Primitive types: `BIGINT`, `DOUBLE`, `VARCHAR`, `BOOLEAN`, `TIMESTAMP`
    /// - `STRUCT(field1 TYPE1, field2 TYPE2, ...)`
    /// - `ARRAY<TYPE>`
    /// - `MAP<KEY_TYPE, VALUE_TYPE>`
    ///
    /// # Errors
    ///
    /// Returns `JsonExtError::InvalidTypeSpec` if the string cannot be parsed.
    pub fn parse(spec: &str) -> Result<arrow::datatypes::Fields, JsonExtError> {
        todo!("Recursive type spec parser")
    }
}

/// Key rename mapping for `jsonb_rename_keys`.
///
/// Pre-computed at plan time when the MAP argument is a constant
/// literal. At execution time, key lookup is O(1) via `FxHashMap`.
#[derive(Debug, Clone)]
pub struct KeyRenameMap {
    /// Old key -> new key mapping.
    mappings: fxhash::FxHashMap<String, String>,
}

impl KeyRenameMap {
    /// Creates a new rename map from alternating old/new pairs.
    pub fn from_pairs(pairs: &[(String, String)]) -> Self {
        let mappings = pairs.iter().cloned().collect();
        Self { mappings }
    }

    /// Looks up the new name for a key. Returns the original
    /// key if no mapping exists.
    #[inline]
    pub fn rename<'a>(&'a self, key: &'a str) -> &'a str {
        self.mappings.get(key).map_or(key, |v| v.as_str())
    }
}

/// Key set for `jsonb_pick` and `jsonb_except`.
///
/// Pre-computed at plan time when the ARRAY argument is a constant.
/// Uses `FxHashSet` for O(1) membership checks at execution time.
#[derive(Debug, Clone)]
pub struct KeySet {
    keys: fxhash::FxHashSet<String>,
}

impl KeySet {
    /// Creates a new key set from a slice of key names.
    pub fn from_keys(keys: &[String]) -> Self {
        Self {
            keys: keys.iter().cloned().collect(),
        }
    }

    /// Checks if a key is in the set.
    #[inline]
    pub fn contains(&self, key: &str) -> bool {
        self.keys.contains(key)
    }
}

/// Flattening context for `jsonb_flatten`.
///
/// Tracks the current path prefix during recursive flattening.
/// Uses a single `String` buffer that is extended/truncated
/// to avoid per-level allocation.
#[derive(Debug)]
struct FlattenContext {
    /// Current key path prefix (e.g., "user.address.").
    prefix: String,
    /// Separator between path segments.
    separator: String,
    /// Maximum nesting depth to prevent stack overflow.
    max_depth: usize,
    /// Accumulated flat key-value pairs.
    output: Vec<(String, Vec<u8>)>,
}

impl FlattenContext {
    fn new(separator: &str, max_depth: usize) -> Self {
        Self {
            prefix: String::new(),
            separator: separator.to_string(),
            max_depth,
            output: Vec::new(),
        }
    }
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `JsonExtError::InvalidTypeSpec(String)` | `json_to_columns` type spec cannot be parsed | Return DataFusion planning error |
| `JsonExtError::NotAnObject` | Merge/rename/pick/except on non-object JSONB | Return NULL |
| `JsonExtError::NullInput` | Any function called with NULL argument | Return NULL |
| `JsonExtError::MaxDepthExceeded` | `jsonb_flatten` or `jsonb_deep_merge` exceeds max nesting | Truncate at max depth, log warning |
| `JsonExtError::InvalidSeparator` | Empty separator string for flatten/unflatten | Return DataFusion error |
| `JsonExtError::AmbiguousUnflatten` | Conflicting keys during unflatten (e.g., `a.b` and `a.b.c`) | Last-value-wins, log warning |
| `JsonExtError::InvalidJsonb` | Malformed JSONB binary | Return NULL |

```rust
use thiserror::Error;

/// Errors from JSON extension function evaluation.
#[derive(Debug, Error)]
pub enum JsonExtError {
    /// Invalid type specification string.
    #[error("invalid type specification: {0}")]
    InvalidTypeSpec(String),

    /// Expected a JSON object but got another type.
    #[error("expected JSON object, got {0}")]
    NotAnObject(String),

    /// Input is NULL.
    #[error("NULL input")]
    NullInput,

    /// Maximum nesting depth exceeded.
    #[error("maximum nesting depth ({max_depth}) exceeded during {operation}")]
    MaxDepthExceeded {
        max_depth: usize,
        operation: String,
    },

    /// Invalid separator string.
    #[error("separator must be non-empty")]
    InvalidSeparator,

    /// Ambiguous key paths during unflatten.
    #[error("ambiguous unflatten: key '{key}' conflicts with nested path")]
    AmbiguousUnflatten { key: String },

    /// Malformed JSONB binary.
    #[error("invalid JSONB binary data")]
    InvalidJsonb,
}
```

---

## Implementation Plan

### Phase 1: Merge and Cleanup Functions (3 days)

1. Implement `JsonbMerge` -- shallow object merge
2. Implement `JsonbDeepMerge` -- recursive merge with depth limit
3. Implement `JsonbStripNulls` -- recursive null removal
4. Unit tests for all three functions including edge cases
5. Register in `create_streaming_context()`

### Phase 2: Key Manipulation Functions (3 days)

1. Implement `JsonbRenameKeys` with `KeyRenameMap`
2. Implement `JsonbPick` with `KeySet`
3. Implement `JsonbExcept` with `KeySet`
4. Plan-time constant folding for MAP/ARRAY arguments
5. Unit tests for key manipulation

### Phase 3: Structure Transformation (3 days)

1. Implement `JsonbFlatten` with recursive `FlattenContext`
2. Implement `JsonbUnflatten` with key splitting and tree construction
3. Handle arrays in flatten (numeric index keys)
4. Depth limit enforcement
5. Unit tests for flatten/unflatten round-trip

### Phase 4: Schema-Resolving Functions (3 days)

1. Implement `TypeSpecParser` for type specification strings
2. Implement `JsonToColumns` with plan-time query rewriting
3. Implement `JsonInferSchema` with single-value type inference
4. Integration with streaming planner for MV creation
5. Unit tests for schema resolution

### Phase 5: Integration and Optimization (2 days)

1. End-to-end integration tests with streaming SQL pipelines
2. Plan-time constant folding for all constant arguments
3. Criterion benchmarks
4. Documentation

---

## Testing Strategy

### Unit Tests

```sql
-- test_jsonb_merge_basic
SELECT jsonb_merge('{"a": 1, "b": 2}'::jsonb, '{"b": 3, "c": 4}'::jsonb);
-- Expected: {"a": 1, "b": 3, "c": 4}

-- test_jsonb_merge_empty_left
SELECT jsonb_merge('{}'::jsonb, '{"a": 1}'::jsonb);
-- Expected: {"a": 1}

-- test_jsonb_merge_empty_right
SELECT jsonb_merge('{"a": 1}'::jsonb, '{}'::jsonb);
-- Expected: {"a": 1}

-- test_jsonb_merge_null_input
SELECT jsonb_merge(NULL, '{"a": 1}'::jsonb);
-- Expected: NULL

-- test_jsonb_deep_merge_nested
SELECT jsonb_deep_merge(
    '{"a": {"x": 1, "y": 2}, "b": 3}'::jsonb,
    '{"a": {"y": 99, "z": 100}, "c": 4}'::jsonb
);
-- Expected: {"a": {"x": 1, "y": 99, "z": 100}, "b": 3, "c": 4}

-- test_jsonb_deep_merge_array_override
SELECT jsonb_deep_merge(
    '{"tags": [1, 2]}'::jsonb,
    '{"tags": [3, 4, 5]}'::jsonb
);
-- Expected: {"tags": [3, 4, 5]} (arrays are not merged, second wins)

-- test_jsonb_strip_nulls_basic
SELECT jsonb_strip_nulls('{"a": 1, "b": null, "c": 3}'::jsonb);
-- Expected: {"a": 1, "c": 3}

-- test_jsonb_strip_nulls_nested
SELECT jsonb_strip_nulls('{"a": 1, "b": {"c": null, "d": 2}, "e": null}'::jsonb);
-- Expected: {"a": 1, "b": {"d": 2}}

-- test_jsonb_strip_nulls_all_null
SELECT jsonb_strip_nulls('{"a": null, "b": null}'::jsonb);
-- Expected: {}

-- test_jsonb_strip_nulls_no_nulls
SELECT jsonb_strip_nulls('{"a": 1, "b": 2}'::jsonb);
-- Expected: {"a": 1, "b": 2}

-- test_jsonb_rename_keys_basic
SELECT jsonb_rename_keys(
    '{"old_name": 1, "ts": "2026-02-21", "other": 2}'::jsonb,
    MAP['old_name', 'new_name', 'ts', 'event_time']
);
-- Expected: {"new_name": 1, "event_time": "2026-02-21", "other": 2}

-- test_jsonb_rename_keys_no_match
SELECT jsonb_rename_keys(
    '{"a": 1, "b": 2}'::jsonb,
    MAP['x', 'y']
);
-- Expected: {"a": 1, "b": 2} (unchanged)

-- test_jsonb_pick_basic
SELECT jsonb_pick('{"a": 1, "b": 2, "c": 3}'::jsonb, ARRAY['a', 'c']);
-- Expected: {"a": 1, "c": 3}

-- test_jsonb_pick_missing_keys
SELECT jsonb_pick('{"a": 1}'::jsonb, ARRAY['a', 'missing']);
-- Expected: {"a": 1}

-- test_jsonb_pick_empty_array
SELECT jsonb_pick('{"a": 1, "b": 2}'::jsonb, ARRAY[]::TEXT[]);
-- Expected: {}

-- test_jsonb_except_basic
SELECT jsonb_except('{"a": 1, "b": 2, "c": 3}'::jsonb, ARRAY['b']);
-- Expected: {"a": 1, "c": 3}

-- test_jsonb_except_all_keys
SELECT jsonb_except('{"a": 1, "b": 2}'::jsonb, ARRAY['a', 'b']);
-- Expected: {}

-- test_jsonb_except_no_match
SELECT jsonb_except('{"a": 1, "b": 2}'::jsonb, ARRAY['missing']);
-- Expected: {"a": 1, "b": 2}

-- test_jsonb_flatten_basic
SELECT jsonb_flatten(
    '{"user": {"name": "Alice", "address": {"city": "London"}}}'::jsonb,
    '.'
);
-- Expected: {"user.name": "Alice", "user.address.city": "London"}

-- test_jsonb_flatten_with_arrays
SELECT jsonb_flatten(
    '{"tags": ["a", "b"], "data": {"x": 1}}'::jsonb,
    '.'
);
-- Expected: {"tags.0": "a", "tags.1": "b", "data.x": 1}

-- test_jsonb_flatten_custom_separator
SELECT jsonb_flatten(
    '{"a": {"b": 1}}'::jsonb,
    '__'
);
-- Expected: {"a__b": 1}

-- test_jsonb_flatten_already_flat
SELECT jsonb_flatten('{"a": 1, "b": 2}'::jsonb, '.');
-- Expected: {"a": 1, "b": 2}

-- test_jsonb_unflatten_basic
SELECT jsonb_unflatten(
    '{"user.name": "Alice", "user.address.city": "London"}'::jsonb,
    '.'
);
-- Expected: {"user": {"name": "Alice", "address": {"city": "London"}}}

-- test_jsonb_unflatten_with_numeric_keys
SELECT jsonb_unflatten(
    '{"tags.0": "a", "tags.1": "b"}'::jsonb,
    '.'
);
-- Expected: {"tags": {"0": "a", "1": "b"}}
-- Note: numeric keys remain as object keys, not converted to arrays

-- test_jsonb_flatten_unflatten_roundtrip
-- For any flat JSONB object:
-- jsonb_unflatten(jsonb_flatten(obj, '.'), '.') = obj
-- (when obj has no key containing '.')

-- test_json_to_columns_basic
SELECT json_to_columns(
    '{"id": 42, "name": "Alice"}'::jsonb,
    'STRUCT(id BIGINT, name VARCHAR)'
) AS (id, name);
-- Expected: id=42, name='Alice'

-- test_json_to_columns_nested
SELECT json_to_columns(
    '{"user": {"name": "Alice", "age": 30}}'::jsonb,
    'STRUCT(user STRUCT(name VARCHAR, age BIGINT))'
) AS (user);
-- Expected: user={name: Alice, age: 30}

-- test_json_infer_schema_basic
SELECT json_infer_schema('{"id": 42, "name": "Alice", "active": true}'::jsonb);
-- Expected: '{"id": "BIGINT", "name": "VARCHAR", "active": "BOOLEAN"}'

-- test_json_infer_schema_nested
SELECT json_infer_schema('{"user": {"name": "Alice"}, "tags": ["a", "b"]}'::jsonb);
-- Expected: '{"user": "STRUCT(name VARCHAR)", "tags": "ARRAY<VARCHAR>"}'
```

### Integration Tests

- [ ] End-to-end: Merge default config with per-event overrides in a streaming MV
- [ ] Pipeline: `jsonb_strip_nulls` -> `jsonb_rename_keys` -> `jsonb_pick` as a chained transformation MV
- [ ] Flatten for analytics sink: Push nested JSON, flatten in MV, verify flat output in sink
- [ ] Unflatten from flat source: Ingest flat CSV-like JSON, unflatten in MV, verify nested output
- [ ] `json_to_columns` in CREATE MATERIALIZED VIEW: Verify typed columns flow through downstream aggregation
- [ ] Deep merge with streaming config updates: Merge base config with streaming override events

### Property Tests

- [ ] `jsonb_merge(a, b)` always contains all keys from both `a` and `b`
- [ ] `jsonb_pick(obj, all_keys)` equals `obj` when `all_keys` contains all object keys
- [ ] `jsonb_except(obj, ARRAY[])` equals `obj`
- [ ] `jsonb_pick(obj, keys)` and `jsonb_except(obj, complement_keys)` are equivalent when keys + complement = all_keys
- [ ] `jsonb_flatten` then `jsonb_unflatten` is identity for objects without separator characters in keys
- [ ] `jsonb_strip_nulls(jsonb_strip_nulls(obj))` equals `jsonb_strip_nulls(obj)` (idempotent)
- [ ] `jsonb_deep_merge(obj, '{}')` equals `obj`

### Benchmarks

- [ ] `bench_jsonb_merge_10_fields` -- Target: < 2us
- [ ] `bench_jsonb_merge_100_fields` -- Target: < 10us
- [ ] `bench_jsonb_deep_merge_3_levels` -- Target: < 5us
- [ ] `bench_jsonb_strip_nulls_20_fields` -- Target: < 3us
- [ ] `bench_jsonb_rename_keys_10_renames` -- Target: < 2us
- [ ] `bench_jsonb_pick_5_of_20` -- Target: < 2us
- [ ] `bench_jsonb_except_5_of_20` -- Target: < 2us
- [ ] `bench_jsonb_flatten_3_levels_20_leaves` -- Target: < 5us
- [ ] `bench_jsonb_unflatten_20_dot_keys` -- Target: < 5us
- [ ] `bench_json_to_columns_5_fields` -- Target: < 3us

---

## Success Criteria

- [ ] All 10 extension functions implemented and registered as DataFusion UDFs
- [ ] `json_to_columns` performs plan-time query rewriting for constant type specs
- [ ] `json_infer_schema` returns correct type information for all JSON value types
- [ ] Merge functions handle nested objects correctly (shallow vs. deep)
- [ ] `jsonb_flatten` / `jsonb_unflatten` round-trip for valid inputs
- [ ] Key manipulation functions (`pick`, `except`, `rename_keys`) are correct for all edge cases
- [ ] 35+ unit tests passing
- [ ] Integration tests with streaming SQL pipelines passing
- [ ] Benchmarks meet latency targets (< 10us for typical operations)
- [ ] All public APIs documented with `///` doc comments
- [ ] `cargo clippy -- -D warnings` passes

---

## Competitive Comparison

| Feature | LaminarDB | DuckDB | ClickHouse | PostgreSQL | RisingWave | Flink |
|---------|-----------|--------|------------|------------|------------|-------|
| `json_to_columns` (typed flatten) | Yes (plan-time) | `json_transform` | N/A | N/A | N/A | N/A |
| `json_infer_schema` | Yes | `json_structure` | N/A | N/A | N/A | N/A |
| `jsonb_merge` (shallow) | Yes | N/A | N/A | `\|\|` operator | N/A | N/A |
| `jsonb_deep_merge` | Yes | N/A | N/A | N/A | N/A | N/A |
| `jsonb_strip_nulls` | Yes | N/A | N/A | `jsonb_strip_nulls` | N/A | N/A |
| `jsonb_rename_keys` | Yes | N/A | N/A | N/A | N/A | N/A |
| `jsonb_pick` (project) | Yes | N/A | N/A | N/A | N/A | N/A |
| `jsonb_except` (exclude) | Yes | N/A | N/A | N/A | N/A | N/A |
| `jsonb_flatten` | Yes | N/A | N/A | N/A | N/A | N/A |
| `jsonb_unflatten` | Yes | N/A | N/A | N/A | N/A | N/A |
| Streaming integration | Yes | No | No | N/A | Partial | Partial |
| Plan-time optimization | Yes | Yes | No | No | No | No |

**Key differentiators**:
- LaminarDB is the only streaming database offering `jsonb_pick`, `jsonb_except`, `jsonb_rename_keys`, `jsonb_flatten`, and `jsonb_unflatten` as first-class SQL functions
- `json_to_columns` resolves at plan time (like DuckDB's `json_transform`) but works in streaming context
- `jsonb_deep_merge` with depth limiting is unique to LaminarDB
- All functions integrate with streaming watermarks, retractions, and checkpoint/recovery

---

## Module Structure

```
crates/laminar-sql/src/datafusion/
+-- json_extensions.rs        # All 10 extension UDFs
+-- json_type_spec.rs         # TypeSpecParser for json_to_columns
+-- mod.rs                    # Updated: register_json_extensions() call added
```

---

## Completion Checklist

- [ ] Code implemented in `laminar-sql/src/datafusion/json_extensions.rs`
- [ ] Code implemented in `laminar-sql/src/datafusion/json_type_spec.rs`
- [ ] Registered in `create_streaming_context()` and `create_streaming_context_with_watermark()`
- [ ] Plan-time rewriting for `json_to_columns` implemented
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## References

- [DuckDB JSON Functions](https://duckdb.org/docs/extensions/json/overview.html) -- `json_transform`, `json_structure`
- [PostgreSQL jsonb_strip_nulls](https://www.postgresql.org/docs/16/functions-json.html)
- [DataFusion UDF Documentation](https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.ScalarUDFImpl.html)
- [F-SCHEMA-011: JSON Scalar Functions](F-SCHEMA-011-json-functions.md)
- [F-SCHEMA-012: JSON Table-Valued Functions](F-SCHEMA-012-json-tvf.md)
- [Research: Schema Inference Design, Section 6.2](../../../research/schema-inference-design.md)
