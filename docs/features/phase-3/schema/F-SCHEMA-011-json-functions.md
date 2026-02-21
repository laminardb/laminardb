# F-SCHEMA-011: PostgreSQL-Compatible JSON Functions

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-011 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | L (2-3 weeks) |
| **Dependencies** | F-SCHEMA-004 (JSONB binary format) |
| **Owner** | TBD |
| **Created** | 2026-02-21 |
| **Updated** | 2026-02-21 |
| **Crate** | `laminar-sql` (DataFusion UDFs/UDAFs) |

---

## Summary

Implement the full set of PostgreSQL-compatible JSON operators and functions as DataFusion UDFs and UDAFs, registered with LaminarDB's `SessionContext`. This includes extraction operators (`->`, `->>`, `#>`, `#>>`), existence/containment operators (`?`, `?|`, `?&`, `@>`, `<@`), scalar construction/interrogation functions (`json_typeof`, `json_build_object`, `json_build_array`, `to_jsonb`, `row_to_json`), and aggregate functions (`json_agg`, `json_object_agg`). All operators on the JSONB binary format use offset-based lookup (<100ns) rather than string parsing, with field offsets pre-computed in Ring 1 and specialized accessors generated at query compile time.

## Goals

- Full PostgreSQL JSON operator parity: `->`, `->>`, `#>`, `#>>`, `?`, `?|`, `?&`, `@>`, `<@`
- PostgreSQL-compatible scalar functions: `json_typeof`, `json_build_object`, `json_build_array`, `to_jsonb`, `row_to_json`
- Streaming-aware aggregate functions: `json_agg`, `json_object_agg` with retraction support
- Sub-100ns hot path latency for JSONB extraction operators via binary offset lookup
- Seamless integration with DataFusion's query planning and optimization
- Registration in `create_streaming_context()` alongside existing window and watermark UDFs

## Non-Goals

- JSON table-valued functions (set-returning) -- covered by F-SCHEMA-012
- LaminarDB-specific extension functions (merge, flatten, pick) -- covered by F-SCHEMA-013
- SQL/JSON path query language (`jsonb_path_exists`, etc.) -- covered by F-SCHEMA-012
- JSON Schema validation functions
- Full GIN index support for JSON containment queries (Phase 4)
- JSONB binary format design itself (handled by F-SCHEMA-004)

---

## Technical Design

### Architecture

**Ring placement**: Operators execute in Ring 0 on the hot path when accessing pre-parsed JSONB binary data. Construction functions (`json_build_object`, `to_jsonb`) execute in Ring 1 since they allocate. Aggregate functions (`json_agg`, `json_object_agg`) use the DataFusion aggregate bridge (F075) and execute in Ring 1.

**Crate**: `laminar-sql` -- all functions are registered as DataFusion UDFs/UDAFs in the `datafusion` module.

**Module**: `laminar-sql/src/datafusion/json_udf.rs` (scalar operators and functions), `laminar-sql/src/datafusion/json_udaf.rs` (aggregate functions).

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Ring 2: Query Planning                            │
│                                                                     │
│  SQL: payload -> 'user' ->> 'name'                                  │
│       │                                                             │
│       v                                                             │
│  Parser rewrites operators to UDF calls:                            │
│    jsonb_get(payload, 'user')  → jsonb_get_text(_, 'name')          │
│       │                                                             │
│       v                                                             │
│  DataFusion LogicalPlan with UDF nodes                              │
│  Compile-time: generate specialized accessor if path is constant    │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                    Ring 1: Background I/O                            │
│                                                                     │
│  JSON decode → JSONB binary format (pre-computed field offsets)      │
│  json_build_object(), to_jsonb() execute here (allocations OK)      │
│  json_agg(), json_object_agg() accumulate here                      │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                    Ring 0: Hot Path                                  │
│                                                                     │
│  jsonb_get(payload, 'field')    → binary offset lookup, <100ns      │
│  jsonb_get_text(payload, 'field') → offset lookup + type cast       │
│  jsonb_exists(payload, 'key')   → header bitmap check               │
│  jsonb_contains(a, b)          → binary prefix comparison           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Ring Integration

| Ring | Responsibility | Latency Budget |
|------|---------------|----------------|
| **Ring 0** | `jsonb_get`, `jsonb_get_text`, `jsonb_get_path`, `jsonb_get_path_text`, `jsonb_exists`, `jsonb_exists_any`, `jsonb_exists_all`, `jsonb_contains`, `jsonb_contained_by`, `json_typeof` -- binary offset lookups on pre-parsed JSONB | < 100ns per access |
| **Ring 1** | `json_build_object`, `json_build_array`, `to_jsonb`, `row_to_json` -- allocate and serialize JSONB binary. `json_agg`, `json_object_agg` -- accumulate into growing buffers. | < 10us per call |
| **Ring 2** | Operator-to-UDF rewriting during SQL parsing. Constant-path specialization at plan time. UDF registration in `SessionContext`. | Plan time (ms) |

### API Design

Each SQL operator maps to a named DataFusion UDF. The SQL parser rewrites operator syntax into function calls during planning.

**Operator-to-UDF Mapping**:

| SQL Operator | UDF Name | Signature | Return Type |
|-------------|----------|-----------|-------------|
| `->` (text key) | `jsonb_get` | `(Jsonb, Utf8) -> Jsonb` | JSONB |
| `->` (int index) | `jsonb_get_idx` | `(Jsonb, Int32) -> Jsonb` | JSONB |
| `->>` (text key) | `jsonb_get_text` | `(Jsonb, Utf8) -> Utf8` | TEXT |
| `->>` (int index) | `jsonb_get_text_idx` | `(Jsonb, Int32) -> Utf8` | TEXT |
| `#>` | `jsonb_get_path` | `(Jsonb, List<Utf8>) -> Jsonb` | JSONB |
| `#>>` | `jsonb_get_path_text` | `(Jsonb, List<Utf8>) -> Utf8` | TEXT |
| `?` | `jsonb_exists` | `(Jsonb, Utf8) -> Boolean` | BOOLEAN |
| `?|` | `jsonb_exists_any` | `(Jsonb, List<Utf8>) -> Boolean` | BOOLEAN |
| `?&` | `jsonb_exists_all` | `(Jsonb, List<Utf8>) -> Boolean` | BOOLEAN |
| `@>` | `jsonb_contains` | `(Jsonb, Jsonb) -> Boolean` | BOOLEAN |
| `<@` | `jsonb_contained_by` | `(Jsonb, Jsonb) -> Boolean` | BOOLEAN |

**Registration** (in `laminar-sql/src/datafusion/mod.rs`):

```rust
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_expr::ScalarUDF;

use crate::datafusion::json_udf::{
    JsonbGet, JsonbGetIdx, JsonbGetText, JsonbGetTextIdx,
    JsonbGetPath, JsonbGetPathText,
    JsonbExists, JsonbExistsAny, JsonbExistsAll,
    JsonbContains, JsonbContainedBy,
    JsonTypeof, JsonBuildObject, JsonBuildArray,
    ToJsonb, RowToJson,
};
use crate::datafusion::json_udaf::{JsonAgg, JsonObjectAgg};

/// Registers all PostgreSQL-compatible JSON UDFs and UDAFs
/// with the given `SessionContext`.
///
/// Called from `create_streaming_context()` alongside window
/// and watermark UDFs.
pub fn register_json_functions(ctx: &SessionContext) {
    // Extraction operators (Ring 0 hot path)
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbGet::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbGetIdx::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbGetText::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbGetTextIdx::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbGetPath::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbGetPathText::new()));

    // Existence operators (Ring 0 hot path)
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbExists::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbExistsAny::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbExistsAll::new()));

    // Containment operators (Ring 0 hot path)
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbContains::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbContainedBy::new()));

    // Scalar functions
    ctx.register_udf(ScalarUDF::new_from_impl(JsonTypeof::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonBuildObject::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonBuildArray::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ToJsonb::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(RowToJson::new()));

    // Aggregate functions
    ctx.register_udaf(
        datafusion_expr::AggregateUDF::new_from_impl(JsonAgg::new())
    );
    ctx.register_udaf(
        datafusion_expr::AggregateUDF::new_from_impl(JsonObjectAgg::new())
    );
}
```

**Scalar UDF Implementation Pattern** (following `ProcTimeUdf` pattern):

```rust
use std::any::Any;
use std::hash::{Hash, Hasher};

use arrow::datatypes::DataType;
use arrow_array::{ArrayRef, BinaryArray, StringArray};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};

/// `jsonb_get(jsonb, text) -> jsonb`
///
/// Extracts a JSON object field by key, returning JSONB.
/// Maps to the `->` operator in PostgreSQL.
///
/// On the Ring 0 hot path, this performs a binary offset
/// lookup into the pre-parsed JSONB format (<100ns).
/// No string parsing or allocation occurs.
#[derive(Debug)]
pub struct JsonbGet {
    signature: Signature,
}

impl JsonbGet {
    /// Creates a new `jsonb_get` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::LargeBinary, // JSONB stored as LargeBinary
                    DataType::Utf8,        // field key
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonbGet {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbGet {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbGet {}

impl Hash for JsonbGet {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_get".hash(state);
    }
}

impl ScalarUDFImpl for JsonbGet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_get"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary) // Returns JSONB
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Extract JSONB binary and key from arguments
        // Perform binary offset lookup into JSONB header
        // Return the sub-JSONB value at the given key
        todo!("Binary offset lookup into JSONB format")
    }
}

/// `jsonb_get_text(jsonb, text) -> text`
///
/// Extracts a JSON object field by key, returning TEXT.
/// Maps to the `->>` operator in PostgreSQL.
#[derive(Debug)]
pub struct JsonbGetText {
    signature: Signature,
}

impl JsonbGetText {
    /// Creates a new `jsonb_get_text` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::LargeBinary, // JSONB
                    DataType::Utf8,        // field key
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

// ScalarUDFImpl follows the same pattern as JsonbGet...

/// `json_typeof(jsonb) -> text`
///
/// Returns the type of the outermost JSON value as a text string:
/// "object", "array", "string", "number", "boolean", or "null".
///
/// Executes in Ring 0 -- reads the type tag byte from JSONB
/// binary header without parsing the value.
#[derive(Debug)]
pub struct JsonTypeof {
    signature: Signature,
}

impl JsonTypeof {
    /// Creates a new `json_typeof` UDF.
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

/// `json_build_object(key1, value1, key2, value2, ...) -> jsonb`
///
/// Constructs a JSONB object from alternating key-value pairs.
/// Executes in Ring 1 (allocates JSONB binary buffer).
///
/// # Example
///
/// ```sql
/// SELECT json_build_object('name', symbol, 'px', price)
/// FROM trades;
/// -- Returns: {"name": "AAPL", "px": 150.25}
/// ```
#[derive(Debug)]
pub struct JsonBuildObject {
    signature: Signature,
}

impl JsonBuildObject {
    /// Creates a new `json_build_object` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::VariadicAny,
                Volatility::Immutable,
            ),
        }
    }
}
```

**Aggregate UDF Implementation**:

```rust
use std::any::Any;
use std::hash::{Hash, Hasher};

use arrow::datatypes::{DataType, Field};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, GroupsAccumulator,
    Signature, TypeSignature, Volatility,
};

/// `json_agg(expression) -> jsonb`
///
/// Collects all values of an expression into a JSON array.
/// Supports retraction for streaming aggregation: when a
/// retraction arrives, the corresponding element is removed
/// from the accumulated array.
///
/// Executes in Ring 1 via the DataFusion aggregate bridge (F075).
#[derive(Debug)]
pub struct JsonAgg {
    signature: Signature,
}

impl JsonAgg {
    /// Creates a new `json_agg` UDAF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Any(1),
                Volatility::Immutable,
            ),
        }
    }
}

impl PartialEq for JsonAgg {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonAgg {}

impl Hash for JsonAgg {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "json_agg".hash(state);
    }
}

impl AggregateUDFImpl for JsonAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "json_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary) // Returns JSONB array
    }

    fn accumulator(
        &self,
        _args: AccumulatorArgs<'_>,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(JsonAggAccumulator::new()))
    }
}

/// Accumulator for `json_agg`.
///
/// Maintains a Vec of JSONB values. On `update_batch`, appends
/// new values. On retraction (negative weight), removes the
/// matching value. On `evaluate`, serializes to a JSONB array.
struct JsonAggAccumulator {
    values: Vec<Vec<u8>>,
}

impl JsonAggAccumulator {
    fn new() -> Self {
        Self { values: Vec::new() }
    }
}

/// `json_object_agg(key, value) -> jsonb`
///
/// Collects key-value pairs into a JSON object. Duplicate keys
/// use last-value-wins semantics (consistent with PostgreSQL).
/// Supports retraction: when a key is retracted, it is removed
/// from the accumulated object.
#[derive(Debug)]
pub struct JsonObjectAgg {
    signature: Signature,
}

impl JsonObjectAgg {
    /// Creates a new `json_object_agg` UDAF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Any(2),
                Volatility::Immutable,
            ),
        }
    }
}
```

### SQL Interface

The SQL parser rewrites JSON operators into UDF calls during planning. Users write standard PostgreSQL syntax:

**Extraction**:

```sql
-- Extract nested field as JSONB
SELECT payload -> 'user' -> 'address' FROM events;

-- Extract as TEXT
SELECT payload ->> 'user_id' FROM events;

-- Array element by index (0-based)
SELECT payload -> 'items' -> 0 FROM events;
SELECT payload -> 'items' ->> 0 FROM events;

-- Path extraction
SELECT payload #> ARRAY['user', 'address', 'city'] FROM events;
SELECT payload #>> ARRAY['user', 'address', 'city'] FROM events;

-- Chained extraction in WHERE clause
SELECT *
FROM sensor_data
WHERE payload ->> 'device_type' = 'temperature'
  AND (payload -> 'reading' ->> 'value')::DOUBLE > 100.0;
```

**Existence and Containment**:

```sql
-- Key exists
SELECT * FROM events WHERE payload ? 'user_id';

-- Any key exists
SELECT * FROM events WHERE payload ?| ARRAY['email', 'phone'];

-- All keys exist
SELECT * FROM events WHERE payload ?& ARRAY['name', 'email', 'age'];

-- Contains (left contains right)
SELECT * FROM events
WHERE payload @> '{"status": "active", "tier": "premium"}'::jsonb;

-- Contained by (left is contained by right)
SELECT * FROM events
WHERE '{"required_field": true}'::jsonb <@ payload;
```

**Type Interrogation and Construction**:

```sql
-- Type checking
SELECT json_typeof(payload -> 'price') FROM events;
-- Returns: "number"

SELECT json_typeof(payload -> 'tags') FROM events;
-- Returns: "array"

-- Build JSON objects dynamically
SELECT json_build_object(
    'symbol', symbol,
    'price', price,
    'timestamp', ts,
    'metadata', json_build_object('source', 'laminar', 'version', 1)
) AS enriched
FROM trades;
-- Returns: {"symbol":"AAPL","price":150.25,"timestamp":"2026-02-21T...","metadata":{"source":"laminar","version":1}}

-- Build JSON arrays
SELECT json_build_array(symbol, price, volume) FROM trades;
-- Returns: ["AAPL", 150.25, 1000]

-- Convert SQL value to JSONB
SELECT to_jsonb(price) FROM trades;
-- Returns: 150.25

-- Convert entire row to JSON
SELECT row_to_json(t) FROM trades t;
-- Returns: {"symbol":"AAPL","price":150.25,"volume":1000,"ts":"2026-02-21T..."}
```

**Aggregation**:

```sql
-- Collect all symbols in a window into a JSON array
SELECT
    TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_start,
    json_agg(symbol) AS symbols_in_window
FROM trades
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)
EMIT AFTER WATERMARK;
-- Returns: ["AAPL", "GOOG", "MSFT", "AAPL", ...]

-- Build a JSON object from key-value aggregation
SELECT
    TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_start,
    json_object_agg(symbol, price) AS latest_prices
FROM (
    SELECT DISTINCT ON (symbol) symbol, price, ts
    FROM trades
    ORDER BY symbol, ts DESC
)
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)
EMIT AFTER WATERMARK;
-- Returns: {"AAPL": 150.25, "GOOG": 2800.50, "MSFT": 310.75}
```

### Data Structures

```rust
/// JSONB type tag (first byte of binary format).
///
/// Used by `json_typeof` for O(1) type interrogation without
/// parsing the value body.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonbTypeTag {
    /// JSON null
    Null = 0x00,
    /// JSON boolean (true or false)
    Boolean = 0x01,
    /// JSON number (integer or float)
    Number = 0x02,
    /// JSON string
    String = 0x03,
    /// JSON array
    Array = 0x04,
    /// JSON object
    Object = 0x05,
}

impl JsonbTypeTag {
    /// Returns the PostgreSQL-compatible type name string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Boolean => "boolean",
            Self::Number => "number",
            Self::String => "string",
            Self::Array => "array",
            Self::Object => "object",
        }
    }

    /// Reads the type tag from the first byte of a JSONB binary value.
    ///
    /// # Errors
    ///
    /// Returns an error if the byte does not correspond to a valid type tag.
    pub fn from_byte(byte: u8) -> Result<Self, JsonFunctionError> {
        match byte {
            0x00 => Ok(Self::Null),
            0x01 => Ok(Self::Boolean),
            0x02 => Ok(Self::Number),
            0x03 => Ok(Self::String),
            0x04 => Ok(Self::Array),
            0x05 => Ok(Self::Object),
            _ => Err(JsonFunctionError::InvalidTypeTag(byte)),
        }
    }
}

/// Pre-computed field offset index for a JSONB object.
///
/// Built during Ring 1 decode. Enables O(1) field access in Ring 0
/// by mapping field names to byte offsets within the JSONB binary.
///
/// Layout: sorted array of (field_name_hash, offset) pairs for
/// binary search, plus a fallback linear scan for hash collisions.
#[derive(Debug, Clone)]
pub struct JsonbFieldIndex {
    /// Sorted (hash, offset) pairs for binary search lookup.
    entries: Vec<(u64, u32)>,
    /// Total number of fields in the object.
    field_count: u32,
}

impl JsonbFieldIndex {
    /// Looks up a field by name. Returns the byte offset within
    /// the JSONB binary, or `None` if the field does not exist.
    ///
    /// Uses binary search on pre-computed hashes: O(log n) worst case,
    /// but typically O(1) for objects with < 16 fields due to cache
    /// locality of the sorted array.
    #[inline]
    pub fn lookup(&self, field_name: &str) -> Option<u32> {
        let hash = xxhash_rust::xxh3::xxh3_64(field_name.as_bytes());
        self.entries
            .binary_search_by_key(&hash, |(h, _)| *h)
            .ok()
            .map(|idx| self.entries[idx].1)
    }
}

/// Compiled JSON accessor for constant-path extraction.
///
/// When the SQL query uses a constant path (e.g., `payload -> 'user' ->> 'name'`),
/// the planner compiles the path into a `CompiledJsonAccessor` at plan time.
/// At execution time, the accessor traverses the pre-computed offsets in a
/// single pass without branching on operator type.
#[derive(Debug, Clone)]
pub struct CompiledJsonAccessor {
    /// Sequence of access steps (key or index).
    steps: Vec<JsonAccessStep>,
    /// Whether the final step returns text (true) or JSONB (false).
    as_text: bool,
}

/// A single step in a compiled JSON access path.
#[derive(Debug, Clone)]
pub enum JsonAccessStep {
    /// Object field access by name.
    Field(String),
    /// Array element access by index.
    Index(i32),
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `JsonFunctionError::InvalidTypeTag(u8)` | JSONB binary has corrupted type byte | Return NULL (consistent with PostgreSQL) |
| `JsonFunctionError::KeyNotFound(String)` | `->` operator on non-existent key | Return NULL |
| `JsonFunctionError::IndexOutOfBounds(i32)` | `->` with index beyond array length | Return NULL |
| `JsonFunctionError::NotAnObject` | `->` with text key on non-object JSONB | Return NULL |
| `JsonFunctionError::NotAnArray` | `->` with integer index on non-array JSONB | Return NULL |
| `JsonFunctionError::InvalidJsonb` | Malformed JSONB binary data | Return NULL, log warning |
| `JsonFunctionError::OddArgumentCount` | `json_build_object` with odd number of args | Return DataFusion error |
| `JsonFunctionError::NonTextKey` | `json_build_object` key is not text | Return DataFusion error |
| `JsonFunctionError::TypeMismatch` | `@>` with non-JSONB operand | Return DataFusion error |

```rust
use thiserror::Error;

/// Errors from JSON function evaluation.
#[derive(Debug, Error)]
pub enum JsonFunctionError {
    /// Invalid JSONB type tag byte.
    #[error("invalid JSONB type tag: 0x{0:02x}")]
    InvalidTypeTag(u8),

    /// Field not found in JSONB object (returns NULL, not error).
    #[error("key not found: {0}")]
    KeyNotFound(String),

    /// Array index out of bounds (returns NULL, not error).
    #[error("array index out of bounds: {0}")]
    IndexOutOfBounds(i32),

    /// Expected a JSON object but got another type.
    #[error("cannot extract field from non-object JSONB")]
    NotAnObject,

    /// Expected a JSON array but got another type.
    #[error("cannot extract index from non-array JSONB")]
    NotAnArray,

    /// Malformed JSONB binary data.
    #[error("invalid JSONB binary data")]
    InvalidJsonb,

    /// `json_build_object` called with odd number of arguments.
    #[error("json_build_object requires an even number of arguments")]
    OddArgumentCount,

    /// `json_build_object` key is not a text value.
    #[error("json_build_object keys must be text")]
    NonTextKey,

    /// Type mismatch in containment operator.
    #[error("type mismatch: expected JSONB, got {0}")]
    TypeMismatch(String),
}
```

---

## Implementation Plan

### Phase 1: Core Extraction Operators (5 days)

1. Implement `JsonbGet` and `JsonbGetText` UDFs with binary offset lookup
2. Implement `JsonbGetIdx` and `JsonbGetTextIdx` for array index access
3. Implement `JsonbGetPath` and `JsonbGetPathText` for path array access
4. SQL parser rewriting: `->` to `jsonb_get`, `->>` to `jsonb_get_text`, `#>` to `jsonb_get_path`, `#>>` to `jsonb_get_path_text`
5. Register in `create_streaming_context()` and `create_streaming_context_with_watermark()`
6. Unit tests for each operator

### Phase 2: Existence and Containment (3 days)

1. Implement `JsonbExists`, `JsonbExistsAny`, `JsonbExistsAll` UDFs
2. Implement `JsonbContains` and `JsonbContainedBy` UDFs
3. SQL parser rewriting: `?` to `jsonb_exists`, `?|` to `jsonb_exists_any`, `?&` to `jsonb_exists_all`, `@>` to `jsonb_contains`, `<@` to `jsonb_contained_by`
4. Unit tests for each operator

### Phase 3: Scalar Functions (3 days)

1. Implement `JsonTypeof` UDF (reads type tag byte)
2. Implement `JsonBuildObject` UDF (variadic arguments)
3. Implement `JsonBuildArray` UDF (variadic arguments)
4. Implement `ToJsonb` UDF (SQL value to JSONB)
5. Implement `RowToJson` UDF (record to JSON)
6. Unit tests for each function

### Phase 4: Aggregate Functions (3 days)

1. Implement `JsonAgg` UDAF with accumulator
2. Implement `JsonObjectAgg` UDAF with accumulator
3. Retraction support: handle negative-weight changelog records
4. Integration with DataFusion aggregate bridge (F075)
5. Unit tests for windowed aggregation

### Phase 5: Optimization and Benchmarks (2 days)

1. Implement `CompiledJsonAccessor` for constant-path optimization
2. Integrate accessor compilation into query planner
3. Criterion benchmarks: extraction <100ns, typeof <50ns
4. Integration tests with streaming SQL queries

---

## Testing Strategy

### Unit Tests

```sql
-- test_jsonb_get_object_field
SELECT '{"name":"Alice","age":30}'::jsonb -> 'name';
-- Expected: '"Alice"'

-- test_jsonb_get_text_object_field
SELECT '{"name":"Alice","age":30}'::jsonb ->> 'name';
-- Expected: 'Alice'

-- test_jsonb_get_array_index
SELECT '[10, 20, 30]'::jsonb -> 1;
-- Expected: 20

-- test_jsonb_get_text_array_index
SELECT '[10, 20, 30]'::jsonb ->> 1;
-- Expected: '20'

-- test_jsonb_get_nested
SELECT '{"user":{"address":{"city":"London"}}}'::jsonb -> 'user' -> 'address' ->> 'city';
-- Expected: 'London'

-- test_jsonb_get_path
SELECT '{"a":{"b":{"c":"deep"}}}'::jsonb #> ARRAY['a','b','c'];
-- Expected: '"deep"'

-- test_jsonb_get_path_text
SELECT '{"a":{"b":{"c":"deep"}}}'::jsonb #>> ARRAY['a','b','c'];
-- Expected: 'deep'

-- test_jsonb_get_missing_key_returns_null
SELECT '{"a":1}'::jsonb -> 'missing';
-- Expected: NULL

-- test_jsonb_get_index_out_of_bounds_returns_null
SELECT '[1,2,3]'::jsonb -> 10;
-- Expected: NULL

-- test_jsonb_exists_true
SELECT '{"name":"Alice","age":30}'::jsonb ? 'name';
-- Expected: true

-- test_jsonb_exists_false
SELECT '{"name":"Alice"}'::jsonb ? 'missing';
-- Expected: false

-- test_jsonb_exists_any
SELECT '{"a":1,"b":2}'::jsonb ?| ARRAY['b','c'];
-- Expected: true

-- test_jsonb_exists_all_true
SELECT '{"a":1,"b":2,"c":3}'::jsonb ?& ARRAY['a','b'];
-- Expected: true

-- test_jsonb_exists_all_false
SELECT '{"a":1,"b":2}'::jsonb ?& ARRAY['a','missing'];
-- Expected: false

-- test_jsonb_contains
SELECT '{"a":1,"b":2,"c":3}'::jsonb @> '{"a":1,"c":3}'::jsonb;
-- Expected: true

-- test_jsonb_contains_false
SELECT '{"a":1}'::jsonb @> '{"a":1,"b":2}'::jsonb;
-- Expected: false

-- test_jsonb_contained_by
SELECT '{"a":1}'::jsonb <@ '{"a":1,"b":2}'::jsonb;
-- Expected: true

-- test_json_typeof_object
SELECT json_typeof('{"a":1}'::jsonb);
-- Expected: 'object'

-- test_json_typeof_array
SELECT json_typeof('[1,2,3]'::jsonb);
-- Expected: 'array'

-- test_json_typeof_string
SELECT json_typeof('"hello"'::jsonb);
-- Expected: 'string'

-- test_json_typeof_number
SELECT json_typeof('42'::jsonb);
-- Expected: 'number'

-- test_json_typeof_boolean
SELECT json_typeof('true'::jsonb);
-- Expected: 'boolean'

-- test_json_typeof_null
SELECT json_typeof('null'::jsonb);
-- Expected: 'null'

-- test_json_build_object
SELECT json_build_object('name', 'Alice', 'age', 30);
-- Expected: {"name":"Alice","age":30}

-- test_json_build_object_nested
SELECT json_build_object('user', json_build_object('name', 'Alice'));
-- Expected: {"user":{"name":"Alice"}}

-- test_json_build_array
SELECT json_build_array(1, 'two', 3.0, true, null);
-- Expected: [1,"two",3.0,true,null]

-- test_to_jsonb_integer
SELECT to_jsonb(42);
-- Expected: 42

-- test_to_jsonb_text
SELECT to_jsonb('hello');
-- Expected: "hello"

-- test_json_agg_basic
SELECT json_agg(x) FROM (VALUES (1),(2),(3)) AS t(x);
-- Expected: [1,2,3]

-- test_json_object_agg_basic
SELECT json_object_agg(k, v)
FROM (VALUES ('a',1),('b',2),('c',3)) AS t(k, v);
-- Expected: {"a":1,"b":2,"c":3}
```

### Integration Tests

- [ ] End-to-end: Create source with JSONB column, push JSON data, extract fields with `->` and `->>` in streaming SQL, verify output
- [ ] Windowed aggregation: `json_agg` inside `TUMBLE` window with `EMIT AFTER WATERMARK`
- [ ] Nested extraction chain: `payload -> 'level1' -> 'level2' ->> 'field'` through full pipeline
- [ ] Existence filter: `WHERE payload ? 'required_key'` filters records correctly in streaming mode
- [ ] Containment join: `WHERE a.payload @> b.filter_pattern` in stream-stream join

### Property Tests

- [ ] For any valid JSON string, `json_typeof` returns one of the six valid type names
- [ ] For any JSONB object and any key, `(obj -> key) IS NULL` is equivalent to `NOT (obj ? key)`
- [ ] `json_build_object` round-trips: `json_build_object('k', v) -> 'k'` equals `to_jsonb(v)`
- [ ] Containment is reflexive: `obj @> obj` is always true
- [ ] `@>` and `<@` are inverse: `a @> b` equals `b <@ a`

### Benchmarks

- [ ] `bench_jsonb_get_small_object` (10 fields) -- Target: < 100ns
- [ ] `bench_jsonb_get_large_object` (100 fields) -- Target: < 200ns
- [ ] `bench_jsonb_get_text` -- Target: < 100ns
- [ ] `bench_jsonb_get_nested_3_levels` -- Target: < 300ns
- [ ] `bench_jsonb_exists` -- Target: < 50ns
- [ ] `bench_jsonb_contains` -- Target: < 500ns
- [ ] `bench_json_typeof` -- Target: < 30ns
- [ ] `bench_json_build_object_5_fields` -- Target: < 1us
- [ ] `bench_compiled_accessor_3_steps` -- Target: < 150ns
- [ ] `bench_json_agg_1000_values` -- Target: < 100us

---

## Success Criteria

- [ ] All 11 PostgreSQL JSON operators registered and functional as DataFusion UDFs
- [ ] All 5 scalar functions (`json_typeof`, `json_build_object`, `json_build_array`, `to_jsonb`, `row_to_json`) implemented
- [ ] Both aggregate functions (`json_agg`, `json_object_agg`) implemented with retraction support
- [ ] SQL parser correctly rewrites all operator syntax to UDF calls
- [ ] Extraction operators execute in < 100ns on pre-parsed JSONB binary
- [ ] `json_typeof` executes in < 50ns (single byte read)
- [ ] 30+ unit tests passing
- [ ] Integration tests with streaming SQL pipelines passing
- [ ] Benchmarks meet latency targets
- [ ] All public APIs documented with `///` doc comments
- [ ] `cargo clippy -- -D warnings` passes

---

## Competitive Comparison

| Feature | LaminarDB | PostgreSQL | ClickHouse | DuckDB | RisingWave | Flink |
|---------|-----------|------------|------------|--------|------------|-------|
| `->` / `->>` operators | Yes (UDF) | Native | `JSONExtract*` | `->` / `->>` | Yes | N/A |
| `#>` / `#>>` path | Yes (UDF) | Native | N/A | N/A | Yes | N/A |
| `?` / `?|` / `?&` exists | Yes (UDF) | Native | `JSONHas` | N/A | Yes | N/A |
| `@>` / `<@` containment | Yes (UDF) | Native | N/A | N/A | Yes | N/A |
| `json_typeof` | Yes | Native | `JSONType` | `json_type` | Yes | N/A |
| `json_build_object` | Yes | Native | N/A | `to_json` | Yes | N/A |
| `json_agg` | Yes (retractable) | Native | `groupArray` | `json_group_array` | Yes | N/A |
| `json_object_agg` | Yes (retractable) | Native | N/A | `json_group_object` | Yes | N/A |
| Binary JSONB format | Yes (<100ns) | Yes | No | Yes | Yes (JSONB) | No |
| Ring 0 hot path | Yes | N/A | N/A | N/A | No | No |
| Compiled path accessor | Yes | No | No | No | No | No |
| Streaming retraction | Yes | N/A | N/A | N/A | Yes | Yes |

---

## Module Structure

```
crates/laminar-sql/src/datafusion/
+-- json_udf.rs              # Scalar JSON UDFs (11 operators + 5 functions)
+-- json_udaf.rs             # Aggregate JSON UDAFs (json_agg, json_object_agg)
+-- json_types.rs            # JsonbTypeTag, JsonbFieldIndex, CompiledJsonAccessor
+-- json_binary.rs           # JSONB binary format access primitives
+-- mod.rs                   # Updated: register_json_functions() call added
```

---

## Completion Checklist

- [ ] Code implemented in `laminar-sql/src/datafusion/json_udf.rs`
- [ ] Code implemented in `laminar-sql/src/datafusion/json_udaf.rs`
- [ ] Registered in `create_streaming_context()` and `create_streaming_context_with_watermark()`
- [ ] SQL parser operator rewriting implemented
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

- [PostgreSQL JSON Functions](https://www.postgresql.org/docs/16/functions-json.html)
- [DataFusion UDF Documentation](https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.ScalarUDFImpl.html)
- [DataFusion UDAF Documentation](https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.AggregateUDFImpl.html)
- [F-SCHEMA-004: JSONB Binary Format](../schema/F-SCHEMA-004-jsonb-binary.md)
- [F075: DataFusion Aggregate Bridge](../../phase-2/F075-datafusion-aggregate-bridge.md)
- [Research: Schema Inference Design, Section 6.1](../../../research/schema-inference-design.md)
- [ProcTimeUdf implementation](../../../../crates/laminar-sql/src/datafusion/proctime_udf.rs) -- reference UDF pattern
