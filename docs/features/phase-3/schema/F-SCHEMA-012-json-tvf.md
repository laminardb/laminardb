# F-SCHEMA-012: JSON Table-Valued & Path Functions

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-012 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | L (2-3 weeks) |
| **Dependencies** | F-SCHEMA-011 (JSON scalar functions) |
| **Owner** | TBD |
| **Created** | 2026-02-21 |
| **Updated** | 2026-02-21 |
| **Crate** | `laminar-sql` (DataFusion table functions) |

---

## Summary

Implement JSON table-valued functions (TVFs) and SQL/JSON path query functions as DataFusion table functions registered with LaminarDB's `SessionContext`. Table-valued functions are critical for streaming because they unnest JSON arrays and objects into rows, enabling LATERAL join patterns that expand a single event containing nested data into multiple downstream records. This includes `jsonb_array_elements`, `jsonb_array_elements_text`, `jsonb_each`, `jsonb_each_text`, `jsonb_object_keys` (all with `WITH ORDINALITY` support), and the SQL/JSON path query functions `jsonb_path_exists`, `jsonb_path_query`, `jsonb_path_query_array`, and `jsonb_path_match`. All TVFs are registered via DataFusion's `TableFunctionImpl` trait and integrate with LATERAL joins for streaming unnest patterns.

## Goals

- Full set of PostgreSQL JSON table-valued functions: `jsonb_array_elements`, `jsonb_array_elements_text`, `jsonb_each`, `jsonb_each_text`, `jsonb_object_keys`
- `WITH ORDINALITY` support for all TVFs (position tracking in unnested output)
- SQL/JSON path query functions: `jsonb_path_exists`, `jsonb_path_query`, `jsonb_path_query_array`, `jsonb_path_match`
- LATERAL join integration for streaming unnest patterns
- Watermark propagation through TVF expansion (expanded rows inherit source watermark)
- Zero-copy where possible: TVFs read from JSONB binary offsets in Ring 0

## Non-Goals

- Generic SQL UNNEST for non-JSON arrays (separate feature)
- Full JSONPath specification compliance (we target the PostgreSQL SQL/JSON subset)
- JSON Schema validation via path expressions
- Recursive path descent (`.**`) beyond PostgreSQL's supported subset
- Custom path function extensions beyond the SQL/JSON standard
- Index-accelerated path queries (Phase 4)

---

## Technical Design

### Architecture

**Ring placement**: TVF execution spans Ring 0 and Ring 1. The actual JSONB binary traversal (reading array elements, iterating object keys) happens in Ring 0 via offset lookup. Row materialization and RecordBatch construction happen in Ring 1. Path expression compilation happens in Ring 2 at plan time.

**Crate**: `laminar-sql` -- table functions registered via `TableFunctionImpl`, path functions as scalar UDFs.

**Module**: `laminar-sql/src/datafusion/json_tvf.rs` (table-valued functions), `laminar-sql/src/datafusion/json_path.rs` (path query functions).

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Ring 2: Query Planning                            │
│                                                                     │
│  SQL: SELECT elem.value                                             │
│       FROM events,                                                  │
│            LATERAL jsonb_array_elements(payload->'items') AS elem    │
│                                                                     │
│  1. Parse LATERAL + TVF call                                        │
│  2. Compile path expression (if jsonb_path_query)                   │
│  3. Build execution plan with TVF node                              │
│  4. Wire watermark propagation through expansion                    │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                    Ring 1: Batch Materialization                     │
│                                                                     │
│  For each input RecordBatch:                                        │
│    - Read JSONB column via binary offsets (Ring 0 primitive)         │
│    - Expand arrays/objects into output rows                         │
│    - Build output RecordBatch with expanded schema                  │
│    - Attach ordinality column if WITH ORDINALITY                    │
│    - Propagate watermark from source row to all expanded rows       │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                    Ring 0: JSONB Binary Access                       │
│                                                                     │
│  jsonb_array_iter(binary)  → iterator over (index, offset, length)  │
│  jsonb_object_iter(binary) → iterator over (key, offset, length)    │
│  jsonb_path_eval(binary, compiled_path) → matched offsets           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Ring Integration

| Ring | Responsibility | Latency Budget |
|------|---------------|----------------|
| **Ring 0** | JSONB binary iteration primitives: `jsonb_array_iter`, `jsonb_object_iter`. These yield (offset, length) pairs without copying data. Path expression evaluation on binary JSONB. | < 100ns per element |
| **Ring 1** | Row materialization: copy element bytes into output RecordBatch columns. Ordinality tracking. Watermark propagation. RecordBatch construction for expanded rows. | < 1us per output row |
| **Ring 2** | SQL parsing of LATERAL + TVF syntax. Path expression compilation. Execution plan construction. Schema inference for TVF output. | Plan time (ms) |

### API Design

**Table-Valued Function Registration**:

Each TVF implements DataFusion's `TableFunctionImpl` trait, which produces a `TableProvider` when called with arguments. The planner detects LATERAL joins containing these functions and wires them into the streaming execution plan.

```rust
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::function::TableFunctionImpl;
use datafusion_common::Result;
use datafusion_expr::Expr;

/// `jsonb_array_elements(jsonb) -> setof jsonb`
///
/// Expands a JSONB array into a set of JSONB values (one row per element).
/// When used with `WITH ORDINALITY`, adds an `ordinality` column (1-based).
///
/// This is the primary streaming unnest function: a single event
/// containing a JSON array of N items produces N output rows,
/// each carrying the source event's watermark.
///
/// # SQL Usage
///
/// ```sql
/// SELECT elem.value
/// FROM sensor_data,
///      LATERAL jsonb_array_elements(payload -> 'readings') AS elem;
/// ```
#[derive(Debug)]
pub struct JsonbArrayElements {
    /// Whether to include an ordinality column.
    with_ordinality: bool,
}

impl JsonbArrayElements {
    /// Creates a new `jsonb_array_elements` TVF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            with_ordinality: false,
        }
    }

    /// Creates a new `jsonb_array_elements` TVF with ordinality.
    #[must_use]
    pub fn with_ordinality() -> Self {
        Self {
            with_ordinality: true,
        }
    }

    /// Returns the output schema for this TVF.
    fn output_schema(&self) -> SchemaRef {
        let mut fields = vec![
            Field::new("value", DataType::LargeBinary, true), // JSONB element
        ];
        if self.with_ordinality {
            fields.push(Field::new("ordinality", DataType::Int64, false));
        }
        Arc::new(Schema::new(fields))
    }
}

impl TableFunctionImpl for JsonbArrayElements {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        // Validate: exactly one argument of JSONB type
        // Return a TableProvider that, for each input JSONB value,
        // iterates the array via binary offsets and produces
        // one output row per element.
        todo!("Implement array element expansion")
    }
}

/// `jsonb_array_elements_text(jsonb) -> setof text`
///
/// Same as `jsonb_array_elements` but returns each element as TEXT
/// instead of JSONB. Useful when array elements are strings and
/// downstream processing expects text columns.
#[derive(Debug)]
pub struct JsonbArrayElementsText {
    with_ordinality: bool,
}

impl JsonbArrayElementsText {
    /// Creates a new `jsonb_array_elements_text` TVF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            with_ordinality: false,
        }
    }

    /// Creates with ordinality support.
    #[must_use]
    pub fn with_ordinality() -> Self {
        Self {
            with_ordinality: true,
        }
    }

    fn output_schema(&self) -> SchemaRef {
        let mut fields = vec![
            Field::new("value", DataType::Utf8, true), // TEXT element
        ];
        if self.with_ordinality {
            fields.push(Field::new("ordinality", DataType::Int64, false));
        }
        Arc::new(Schema::new(fields))
    }
}

impl TableFunctionImpl for JsonbArrayElementsText {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        todo!("Implement text array element expansion")
    }
}

/// `jsonb_each(jsonb) -> setof (key text, value jsonb)`
///
/// Expands a JSONB object into a set of key-value pairs.
/// Each output row contains the key (as text) and the value (as JSONB).
///
/// # SQL Usage
///
/// ```sql
/// SELECT kv.key, kv.value
/// FROM events,
///      LATERAL jsonb_each(payload -> 'properties') AS kv(key, value);
/// ```
#[derive(Debug)]
pub struct JsonbEach {
    with_ordinality: bool,
}

impl JsonbEach {
    /// Creates a new `jsonb_each` TVF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            with_ordinality: false,
        }
    }

    /// Creates with ordinality support.
    #[must_use]
    pub fn with_ordinality() -> Self {
        Self {
            with_ordinality: true,
        }
    }

    fn output_schema(&self) -> SchemaRef {
        let mut fields = vec![
            Field::new("key", DataType::Utf8, false),         // Object key
            Field::new("value", DataType::LargeBinary, true),  // JSONB value
        ];
        if self.with_ordinality {
            fields.push(Field::new("ordinality", DataType::Int64, false));
        }
        Arc::new(Schema::new(fields))
    }
}

impl TableFunctionImpl for JsonbEach {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        todo!("Implement object key-value expansion")
    }
}

/// `jsonb_each_text(jsonb) -> setof (key text, value text)`
///
/// Same as `jsonb_each` but casts each value to TEXT.
#[derive(Debug)]
pub struct JsonbEachText {
    with_ordinality: bool,
}

impl JsonbEachText {
    /// Creates a new `jsonb_each_text` TVF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            with_ordinality: false,
        }
    }

    /// Creates with ordinality support.
    #[must_use]
    pub fn with_ordinality() -> Self {
        Self {
            with_ordinality: true,
        }
    }

    fn output_schema(&self) -> SchemaRef {
        let mut fields = vec![
            Field::new("key", DataType::Utf8, false),   // Object key
            Field::new("value", DataType::Utf8, true),   // Text value
        ];
        if self.with_ordinality {
            fields.push(Field::new("ordinality", DataType::Int64, false));
        }
        Arc::new(Schema::new(fields))
    }
}

impl TableFunctionImpl for JsonbEachText {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        todo!("Implement object key-value text expansion")
    }
}

/// `jsonb_object_keys(jsonb) -> setof text`
///
/// Returns all keys of a JSONB object as a set of text rows.
#[derive(Debug)]
pub struct JsonbObjectKeys {
    with_ordinality: bool,
}

impl JsonbObjectKeys {
    /// Creates a new `jsonb_object_keys` TVF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            with_ordinality: false,
        }
    }

    /// Creates with ordinality support.
    #[must_use]
    pub fn with_ordinality() -> Self {
        Self {
            with_ordinality: true,
        }
    }

    fn output_schema(&self) -> SchemaRef {
        let mut fields = vec![
            Field::new("key", DataType::Utf8, false), // Object key
        ];
        if self.with_ordinality {
            fields.push(Field::new("ordinality", DataType::Int64, false));
        }
        Arc::new(Schema::new(fields))
    }
}

impl TableFunctionImpl for JsonbObjectKeys {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        todo!("Implement object key extraction")
    }
}
```

**TVF Registration** (in `laminar-sql/src/datafusion/mod.rs`):

```rust
use crate::datafusion::json_tvf::{
    JsonbArrayElements, JsonbArrayElementsText,
    JsonbEach, JsonbEachText, JsonbObjectKeys,
};
use crate::datafusion::json_path::{
    JsonbPathExists, JsonbPathQuery, JsonbPathQueryArray, JsonbPathMatch,
};

/// Registers all JSON table-valued functions and path query
/// functions with the given `SessionContext`.
pub fn register_json_table_functions(ctx: &SessionContext) {
    // Table-valued functions
    ctx.register_udtf(
        "jsonb_array_elements",
        Arc::new(JsonbArrayElements::new()),
    );
    ctx.register_udtf(
        "jsonb_array_elements_text",
        Arc::new(JsonbArrayElementsText::new()),
    );
    ctx.register_udtf(
        "jsonb_each",
        Arc::new(JsonbEach::new()),
    );
    ctx.register_udtf(
        "jsonb_each_text",
        Arc::new(JsonbEachText::new()),
    );
    ctx.register_udtf(
        "jsonb_object_keys",
        Arc::new(JsonbObjectKeys::new()),
    );

    // Path query functions (scalar UDFs)
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbPathExists::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbPathMatch::new()));

    // Path query set-returning functions (table functions)
    ctx.register_udtf(
        "jsonb_path_query",
        Arc::new(JsonbPathQuery::new()),
    );
    ctx.register_udtf(
        "jsonb_path_query_array",
        Arc::new(JsonbPathQueryArray::new()),
    );
}
```

**SQL/JSON Path Query Functions**:

```rust
use std::any::Any;
use std::hash::{Hash, Hasher};

use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};

/// `jsonb_path_exists(jsonb, path_text [, vars_jsonb]) -> boolean`
///
/// Returns true if the SQL/JSON path expression matches any
/// element in the JSONB value.
///
/// The path expression is compiled at plan time (Ring 2) when
/// the path argument is a string literal. At execution time,
/// the compiled path is evaluated against the JSONB binary.
///
/// # SQL Usage
///
/// ```sql
/// SELECT * FROM events
/// WHERE jsonb_path_exists(payload, '$.users[*] ? (@.age > 25)');
/// ```
#[derive(Debug)]
pub struct JsonbPathExists {
    signature: Signature,
}

impl JsonbPathExists {
    /// Creates a new `jsonb_path_exists` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::OneOf(vec![
                    // (jsonb, path)
                    TypeSignature::Exact(vec![
                        DataType::LargeBinary,
                        DataType::Utf8,
                    ]),
                    // (jsonb, path, vars)
                    TypeSignature::Exact(vec![
                        DataType::LargeBinary,
                        DataType::Utf8,
                        DataType::LargeBinary,
                    ]),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl PartialEq for JsonbPathExists {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbPathExists {}

impl Hash for JsonbPathExists {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_path_exists".hash(state);
    }
}

impl ScalarUDFImpl for JsonbPathExists {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_path_exists"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // 1. Extract JSONB binary and path string
        // 2. Compile path if not already cached
        // 3. Evaluate compiled path against JSONB binary
        // 4. Return true if any match found
        todo!("Path expression evaluation")
    }
}

/// `jsonb_path_match(jsonb, path_text [, vars_jsonb]) -> boolean`
///
/// Returns the result of a SQL/JSON path predicate check.
/// Unlike `jsonb_path_exists`, this evaluates the path as a
/// boolean predicate (the path must yield a single boolean).
#[derive(Debug)]
pub struct JsonbPathMatch {
    signature: Signature,
}

impl JsonbPathMatch {
    /// Creates a new `jsonb_path_match` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::OneOf(vec![
                    TypeSignature::Exact(vec![
                        DataType::LargeBinary,
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::LargeBinary,
                        DataType::Utf8,
                        DataType::LargeBinary,
                    ]),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

/// `jsonb_path_query(jsonb, path_text [, vars_jsonb]) -> setof jsonb`
///
/// Returns all JSONB values matched by the SQL/JSON path expression.
/// This is a table-valued function (set-returning).
///
/// # SQL Usage
///
/// ```sql
/// SELECT matched.value
/// FROM events,
///      LATERAL jsonb_path_query(payload, '$.items[*].price') AS matched;
/// ```
#[derive(Debug)]
pub struct JsonbPathQuery;

impl JsonbPathQuery {
    /// Creates a new `jsonb_path_query` TVF.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl TableFunctionImpl for JsonbPathQuery {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        // Returns a table provider that evaluates the path expression
        // and yields one row per matched JSONB value.
        todo!("Path query set-returning implementation")
    }
}

/// `jsonb_path_query_array(jsonb, path_text [, vars_jsonb]) -> jsonb`
///
/// Returns all matched values as a single JSONB array.
/// Unlike `jsonb_path_query`, this is a scalar function that
/// returns one row containing a JSONB array of all matches.
///
/// # SQL Usage
///
/// ```sql
/// SELECT jsonb_path_query_array(payload, '$.items[*] ? (@.quantity > 10)')
/// FROM orders;
/// -- Returns: [{"name":"widget","quantity":20},{"name":"gadget","quantity":15}]
/// ```
#[derive(Debug)]
pub struct JsonbPathQueryArray;

impl JsonbPathQueryArray {
    /// Creates a new `jsonb_path_query_array` TVF.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl TableFunctionImpl for JsonbPathQueryArray {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        todo!("Path query array implementation")
    }
}
```

**Compiled SQL/JSON Path Expression**:

```rust
/// A compiled SQL/JSON path expression.
///
/// Path expressions are compiled at plan time (Ring 2) when the
/// path string is a constant literal. This avoids repeated parsing
/// at execution time. The compiled form is a sequence of path steps
/// that can be evaluated against JSONB binary data.
///
/// # Supported Syntax (PostgreSQL SQL/JSON subset)
///
/// - `$` -- root element
/// - `.key` -- object member access
/// - `[n]` -- array element access
/// - `[*]` -- wildcard array element
/// - `? (predicate)` -- filter expression
/// - `@` -- current element in filter
/// - `==`, `!=`, `<`, `>`, `<=`, `>=` -- comparison in filters
/// - `&&`, `||`, `!` -- logical operators in filters
/// - `exists(path)` -- existence check in filter
#[derive(Debug, Clone)]
pub struct CompiledJsonPath {
    /// Sequence of path steps.
    steps: Vec<JsonPathStep>,
}

/// A single step in a compiled JSON path.
#[derive(Debug, Clone)]
pub enum JsonPathStep {
    /// Root element (`$`).
    Root,
    /// Object member access (`.key`).
    Member(String),
    /// Array element by index (`[n]`).
    ArrayIndex(i64),
    /// Wildcard array element (`[*]`).
    ArrayWildcard,
    /// Filter predicate (`? (...)`).
    Filter(Box<JsonPathPredicate>),
}

/// A predicate expression within a JSON path filter.
#[derive(Debug, Clone)]
pub enum JsonPathPredicate {
    /// Comparison: `@.field op value`.
    Comparison {
        left: Box<JsonPathExpr>,
        op: JsonPathCompOp,
        right: Box<JsonPathExpr>,
    },
    /// Logical AND: `pred1 && pred2`.
    And(Box<JsonPathPredicate>, Box<JsonPathPredicate>),
    /// Logical OR: `pred1 || pred2`.
    Or(Box<JsonPathPredicate>, Box<JsonPathPredicate>),
    /// Logical NOT: `! pred`.
    Not(Box<JsonPathPredicate>),
    /// Existence check: `exists(path)`.
    Exists(CompiledJsonPath),
}

/// An expression within a JSON path predicate.
#[derive(Debug, Clone)]
pub enum JsonPathExpr {
    /// Current element (`@`).
    CurrentItem,
    /// Member access on current element (`@.field`).
    MemberAccess(Vec<String>),
    /// Literal value.
    Literal(JsonPathLiteral),
}

/// A literal value in a JSON path expression.
#[derive(Debug, Clone)]
pub enum JsonPathLiteral {
    /// Numeric literal.
    Number(f64),
    /// String literal.
    String(String),
    /// Boolean literal.
    Boolean(bool),
    /// Null literal.
    Null,
}

/// Comparison operators in JSON path predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonPathCompOp {
    /// `==`
    Eq,
    /// `!=`
    Ne,
    /// `<`
    Lt,
    /// `>`
    Gt,
    /// `<=`
    Le,
    /// `>=`
    Ge,
}

impl CompiledJsonPath {
    /// Compiles a SQL/JSON path string into a `CompiledJsonPath`.
    ///
    /// Called at plan time (Ring 2). Returns an error if the path
    /// syntax is invalid.
    ///
    /// # Errors
    ///
    /// Returns `JsonPathError::InvalidSyntax` if the path cannot be parsed.
    pub fn compile(path_str: &str) -> Result<Self, JsonPathError> {
        let mut steps = Vec::new();
        // Parse the path expression into steps
        // (tokenizer + recursive descent parser)
        todo!("JSON path compiler")
    }

    /// Evaluates the compiled path against a JSONB binary value.
    ///
    /// Returns an iterator of (offset, length) pairs pointing to
    /// matched sub-values within the JSONB binary. No allocation
    /// occurs during evaluation -- results reference the input buffer.
    ///
    /// Ring 0 compatible: no heap allocation, no string parsing.
    pub fn evaluate<'a>(
        &self,
        jsonb: &'a [u8],
    ) -> JsonPathMatches<'a> {
        todo!("Path evaluation against JSONB binary")
    }
}

/// Iterator over JSON path matches.
///
/// Yields references into the original JSONB binary buffer.
/// No allocation during iteration (Ring 0 compatible).
pub struct JsonPathMatches<'a> {
    jsonb: &'a [u8],
    // Internal state for traversal
}
```

### SQL Interface

**Array Unnest**:

```sql
-- Unnest a JSON array into individual rows
SELECT
    s.device_id,
    elem.value ->> 'temperature' AS temperature,
    elem.value ->> 'humidity' AS humidity
FROM sensor_data s,
     LATERAL jsonb_array_elements(s.payload -> 'readings') AS elem;

-- Input row:
-- device_id: "sensor-1"
-- payload: {"readings": [{"temperature": 22.5, "humidity": 45}, {"temperature": 23.0, "humidity": 42}]}
--
-- Output rows:
-- | device_id | temperature | humidity |
-- |-----------|-------------|----------|
-- | sensor-1  | 22.5        | 45       |
-- | sensor-1  | 23.0        | 42       |
```

**Array Unnest with Ordinality**:

```sql
-- Track position within the array
SELECT
    e.event_id,
    item.value ->> 'product_name' AS product,
    item.ordinality AS line_number
FROM orders e,
     LATERAL jsonb_array_elements(e.payload -> 'items')
     WITH ORDINALITY AS item(value, ordinality);

-- Input row:
-- event_id: 1001
-- payload: {"items": [{"product_name":"Widget"}, {"product_name":"Gadget"}, {"product_name":"Doohickey"}]}
--
-- Output rows:
-- | event_id | product   | line_number |
-- |----------|-----------|-------------|
-- | 1001     | Widget    | 1           |
-- | 1001     | Gadget    | 2           |
-- | 1001     | Doohickey | 3           |
```

**Text Array Unnest**:

```sql
-- Unnest array of strings directly to text
SELECT
    e.user_id,
    tag.value AS tag
FROM events e,
     LATERAL jsonb_array_elements_text(e.payload -> 'tags') AS tag;

-- Input: payload: {"tags": ["vip", "early-adopter", "beta"]}
-- Output:
-- | user_id | tag           |
-- |---------|---------------|
-- | u123    | vip           |
-- | u123    | early-adopter |
-- | u123    | beta          |
```

**Object Key-Value Iteration**:

```sql
-- Iterate over all properties as key-value pairs
SELECT
    e.event_id,
    kv.key AS property_name,
    kv.value AS property_value
FROM events e,
     LATERAL jsonb_each(e.payload -> 'properties') AS kv(key, value);

-- Input: payload: {"properties": {"color": "red", "size": 42, "active": true}}
-- Output:
-- | event_id | property_name | property_value |
-- |----------|---------------|----------------|
-- | e1       | color         | "red"          |
-- | e1       | size          | 42             |
-- | e1       | active        | true           |
```

**Object Key-Value as Text**:

```sql
-- Same but values cast to text
SELECT kv.key, kv.value
FROM events e,
     LATERAL jsonb_each_text(e.payload -> 'metadata') AS kv(key, value);

-- Output:
-- | key   | value |
-- |-------|-------|
-- | color | red   |
-- | size  | 42    |
```

**Object Keys**:

```sql
-- Extract all top-level keys
SELECT DISTINCT jsonb_object_keys(payload) AS key
FROM events;

-- Output:
-- | key        |
-- |------------|
-- | user_id    |
-- | timestamp  |
-- | properties |
-- | tags       |
```

**Path Queries**:

```sql
-- Check if any user is over 25
SELECT *
FROM events
WHERE jsonb_path_exists(payload, '$.users[*] ? (@.age > 25)');

-- Extract all prices from items array
SELECT matched.value
FROM orders,
     LATERAL jsonb_path_query(payload, '$.items[*].price') AS matched;

-- Get filtered items as a single array
SELECT
    order_id,
    jsonb_path_query_array(payload, '$.items[*] ? (@.quantity > 10)')
        AS high_quantity_items
FROM orders;

-- Boolean predicate check
SELECT *
FROM users
WHERE jsonb_path_match(profile, 'exists($.settings.notifications ? (@ == true))');
```

**Streaming Unnest Pattern (LATERAL + TVF + Window)**:

```sql
-- Real-world streaming pattern: IoT sensor data with nested readings
CREATE SOURCE iot_events (
    device_id   VARCHAR NOT NULL,
    payload     JSONB NOT NULL,
    event_time  TIMESTAMP NOT NULL,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (buffer_size = 65536);

-- Unnest readings and aggregate per device per minute
CREATE MATERIALIZED VIEW device_avg_temp AS
SELECT
    s.device_id,
    TUMBLE_START(s.event_time, INTERVAL '1' MINUTE) AS window_start,
    AVG(CAST(reading.value ->> 'temperature' AS DOUBLE)) AS avg_temp,
    COUNT(*) AS reading_count
FROM iot_events s,
     LATERAL jsonb_array_elements(s.payload -> 'readings') AS reading
GROUP BY
    s.device_id,
    TUMBLE(s.event_time, INTERVAL '1' MINUTE)
EMIT AFTER WATERMARK;
```

### Data Structures

```rust
/// Configuration for TVF execution in a streaming context.
///
/// Controls how expanded rows interact with watermarks and
/// checkpointing.
#[derive(Debug, Clone)]
pub struct TvfStreamingConfig {
    /// Maximum number of output rows per input row.
    /// Prevents unbounded expansion from malicious input.
    /// Default: 10_000.
    pub max_expansion: usize,

    /// Watermark propagation mode.
    pub watermark_mode: TvfWatermarkMode,
}

/// How watermarks propagate through TVF expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TvfWatermarkMode {
    /// All expanded rows inherit the source row's watermark.
    /// This is the default and correct behavior for most cases.
    Inherit,

    /// The watermark is the minimum across all expanded rows.
    /// Used when expanded rows have their own timestamps.
    Minimum,
}

impl Default for TvfStreamingConfig {
    fn default() -> Self {
        Self {
            max_expansion: 10_000,
            watermark_mode: TvfWatermarkMode::Inherit,
        }
    }
}

/// Output of a TVF expansion for a single input row.
///
/// Contains the expanded rows and metadata about the expansion.
#[derive(Debug)]
pub struct TvfExpansionResult {
    /// Number of rows produced.
    pub row_count: usize,
    /// Whether the expansion was truncated due to `max_expansion`.
    pub truncated: bool,
    /// The Arrow RecordBatch containing the expanded rows.
    pub batch: arrow::record_batch::RecordBatch,
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `JsonTvfError::NotAnArray` | `jsonb_array_elements` called on non-array JSONB | Return empty set (0 rows) |
| `JsonTvfError::NotAnObject` | `jsonb_each` called on non-object JSONB | Return empty set (0 rows) |
| `JsonTvfError::NullInput` | TVF called with NULL argument | Return empty set (0 rows) |
| `JsonTvfError::ExpansionLimitExceeded` | Array has more elements than `max_expansion` | Truncate output, log warning |
| `JsonTvfError::InvalidJsonb` | Malformed JSONB binary | Return empty set, log warning |
| `JsonPathError::InvalidSyntax(String)` | Path expression cannot be parsed | Return DataFusion planning error |
| `JsonPathError::TypeMismatch` | Path filter comparison type mismatch | Return NULL for that match |
| `JsonPathError::DivisionByZero` | Arithmetic error in path filter | Return NULL for that match |

```rust
use thiserror::Error;

/// Errors from JSON table-valued function evaluation.
#[derive(Debug, Error)]
pub enum JsonTvfError {
    /// Input is not a JSON array.
    #[error("jsonb_array_elements requires a JSON array, got {0}")]
    NotAnArray(String),

    /// Input is not a JSON object.
    #[error("jsonb_each requires a JSON object, got {0}")]
    NotAnObject(String),

    /// Input is NULL.
    #[error("TVF input is NULL")]
    NullInput,

    /// Array expansion exceeded the configured limit.
    #[error("expansion limit exceeded: {count} > {limit}")]
    ExpansionLimitExceeded { count: usize, limit: usize },

    /// Malformed JSONB binary.
    #[error("invalid JSONB binary data")]
    InvalidJsonb,
}

/// Errors from JSON path expression compilation and evaluation.
#[derive(Debug, Error)]
pub enum JsonPathError {
    /// Invalid path syntax.
    #[error("invalid JSON path syntax: {0}")]
    InvalidSyntax(String),

    /// Unexpected token in path expression.
    #[error("unexpected token '{token}' at position {position}")]
    UnexpectedToken { token: String, position: usize },

    /// Type mismatch in path filter comparison.
    #[error("type mismatch in path filter: cannot compare {left} with {right}")]
    TypeMismatch { left: String, right: String },

    /// Division by zero in path filter arithmetic.
    #[error("division by zero in path filter")]
    DivisionByZero,
}
```

---

## Implementation Plan

### Phase 1: Array TVFs (5 days)

1. Implement `JsonbArrayElements` TVF with `TableFunctionImpl`
2. Implement `JsonbArrayElementsText` TVF
3. Implement `WITH ORDINALITY` support for both
4. LATERAL join integration in the streaming planner
5. Watermark propagation through array expansion
6. Unit tests for array TVFs

### Phase 2: Object TVFs (3 days)

1. Implement `JsonbEach` TVF
2. Implement `JsonbEachText` TVF
3. Implement `JsonbObjectKeys` TVF
4. `WITH ORDINALITY` support for all three
5. Unit tests for object TVFs

### Phase 3: JSON Path Compiler (4 days)

1. Implement JSON path tokenizer
2. Implement recursive descent parser for JSON path expressions
3. Implement `CompiledJsonPath::compile()` with error reporting
4. Implement `CompiledJsonPath::evaluate()` with zero-copy iteration
5. Plan-time path compilation for constant path literals
6. Unit tests for path parser and evaluator

### Phase 4: Path Query Functions (3 days)

1. Implement `JsonbPathExists` scalar UDF
2. Implement `JsonbPathMatch` scalar UDF
3. Implement `JsonbPathQuery` TVF
4. Implement `JsonbPathQueryArray` TVF (scalar variant)
5. Path variable support (`$vars` parameter)
6. Unit tests for path query functions

### Phase 5: Streaming Integration (3 days)

1. `TvfStreamingConfig` with expansion limits
2. Watermark propagation through LATERAL TVF expansion
3. Retraction handling: when source row is retracted, retract all expanded rows
4. Integration tests with full streaming SQL pipelines
5. Benchmarks for TVF expansion throughput

---

## Testing Strategy

### Unit Tests

```sql
-- test_jsonb_array_elements_basic
SELECT value FROM jsonb_array_elements('[1, 2, 3]'::jsonb);
-- Expected: 3 rows: 1, 2, 3

-- test_jsonb_array_elements_objects
SELECT value ->> 'name' AS name
FROM jsonb_array_elements('[{"name":"Alice"},{"name":"Bob"}]'::jsonb);
-- Expected: 2 rows: 'Alice', 'Bob'

-- test_jsonb_array_elements_empty
SELECT value FROM jsonb_array_elements('[]'::jsonb);
-- Expected: 0 rows

-- test_jsonb_array_elements_null_input
SELECT value FROM jsonb_array_elements(NULL);
-- Expected: 0 rows

-- test_jsonb_array_elements_not_array
SELECT value FROM jsonb_array_elements('{"a":1}'::jsonb);
-- Expected: 0 rows (graceful degradation)

-- test_jsonb_array_elements_with_ordinality
SELECT value, ordinality
FROM jsonb_array_elements('[10, 20, 30]'::jsonb)
WITH ORDINALITY;
-- Expected:
-- | value | ordinality |
-- |-------|------------|
-- | 10    | 1          |
-- | 20    | 2          |
-- | 30    | 3          |

-- test_jsonb_array_elements_text_basic
SELECT value FROM jsonb_array_elements_text('["a", "b", "c"]'::jsonb);
-- Expected: 3 rows: 'a', 'b', 'c'

-- test_jsonb_array_elements_text_numbers
SELECT value FROM jsonb_array_elements_text('[1, 2.5, true]'::jsonb);
-- Expected: 3 rows: '1', '2.5', 'true'

-- test_jsonb_each_basic
SELECT key, value
FROM jsonb_each('{"a":1,"b":"hello","c":true}'::jsonb);
-- Expected:
-- | key | value   |
-- |-----|---------|
-- | a   | 1       |
-- | b   | "hello" |
-- | c   | true    |

-- test_jsonb_each_empty
SELECT key, value FROM jsonb_each('{}'::jsonb);
-- Expected: 0 rows

-- test_jsonb_each_text_basic
SELECT key, value
FROM jsonb_each_text('{"a":1,"b":"hello","c":true}'::jsonb);
-- Expected:
-- | key | value |
-- |-----|-------|
-- | a   | 1     |
-- | b   | hello |
-- | c   | true  |

-- test_jsonb_object_keys_basic
SELECT key FROM jsonb_object_keys('{"a":1,"b":2,"c":3}'::jsonb);
-- Expected: 3 rows: 'a', 'b', 'c'

-- test_jsonb_object_keys_empty
SELECT key FROM jsonb_object_keys('{}'::jsonb);
-- Expected: 0 rows

-- test_lateral_join_array_elements
SELECT t.id, elem.value ->> 'x' AS x
FROM (SELECT 1 AS id, '[{"x":"a"},{"x":"b"}]'::jsonb AS arr) t,
     LATERAL jsonb_array_elements(t.arr) AS elem;
-- Expected:
-- | id | x |
-- |----|---|
-- | 1  | a |
-- | 1  | b |

-- test_jsonb_path_exists_true
SELECT jsonb_path_exists('{"users":[{"age":30},{"age":20}]}'::jsonb,
                         '$.users[*] ? (@.age > 25)');
-- Expected: true

-- test_jsonb_path_exists_false
SELECT jsonb_path_exists('{"users":[{"age":18},{"age":20}]}'::jsonb,
                         '$.users[*] ? (@.age > 25)');
-- Expected: false

-- test_jsonb_path_query_basic
SELECT value
FROM jsonb_path_query('{"items":[{"price":10},{"price":20},{"price":30}]}'::jsonb,
                      '$.items[*].price');
-- Expected: 3 rows: 10, 20, 30

-- test_jsonb_path_query_with_filter
SELECT value
FROM jsonb_path_query(
    '{"items":[{"name":"a","qty":5},{"name":"b","qty":15}]}'::jsonb,
    '$.items[*] ? (@.qty > 10)'
);
-- Expected: 1 row: {"name":"b","qty":15}

-- test_jsonb_path_query_array_basic
SELECT jsonb_path_query_array(
    '{"items":[{"price":10},{"price":20},{"price":30}]}'::jsonb,
    '$.items[*] ? (@.price > 15)'
);
-- Expected: [{"price":20},{"price":30}]

-- test_jsonb_path_match_true
SELECT jsonb_path_match(
    '{"user":{"premium":true}}'::jsonb,
    'exists($.user.premium ? (@ == true))'
);
-- Expected: true

-- test_jsonb_path_match_false
SELECT jsonb_path_match(
    '{"user":{"premium":false}}'::jsonb,
    'exists($.user.premium ? (@ == true))'
);
-- Expected: false
```

### Integration Tests

- [ ] End-to-end streaming unnest: Push JSON events with nested arrays through source, run LATERAL `jsonb_array_elements` in MV, verify expanded output via sink
- [ ] Windowed aggregation over unnested data: `jsonb_array_elements` + `TUMBLE` window + `AVG` aggregate
- [ ] Watermark propagation through TVF: Verify expanded rows trigger correct window closures
- [ ] Retraction through TVF: Retract source row, verify all expanded rows are retracted
- [ ] Path query in streaming filter: `WHERE jsonb_path_exists(...)` filters correctly over streaming data
- [ ] Multi-TVF pipeline: Chain `jsonb_array_elements` with `jsonb_each` for deeply nested structures
- [ ] Expansion limit: Verify `max_expansion` truncates output and logs warning

### Property Tests

- [ ] `jsonb_array_elements` produces exactly `array_length` rows for any valid JSON array
- [ ] `jsonb_each` produces exactly `object_key_count` rows for any valid JSON object
- [ ] `jsonb_object_keys` output is a subset of `jsonb_each` keys
- [ ] With ordinality, ordinality values are consecutive 1..N
- [ ] `jsonb_path_exists(obj, path)` returns true if and only if `jsonb_path_query(obj, path)` returns at least one row

### Benchmarks

- [ ] `bench_array_elements_10_items` -- Target: < 2us total (< 200ns/element)
- [ ] `bench_array_elements_100_items` -- Target: < 15us total
- [ ] `bench_array_elements_text_100_items` -- Target: < 20us total
- [ ] `bench_each_10_fields` -- Target: < 2us total
- [ ] `bench_each_100_fields` -- Target: < 15us total
- [ ] `bench_object_keys_100_fields` -- Target: < 10us total
- [ ] `bench_path_compile` -- Target: < 5us for typical path
- [ ] `bench_path_eval_simple` (e.g., `$.a.b.c`) -- Target: < 200ns
- [ ] `bench_path_eval_wildcard_filter` (e.g., `$[*] ? (@.x > 5)`) -- Target: < 500ns per element
- [ ] `bench_lateral_unnest_throughput` -- Target: > 1M expanded rows/sec

---

## Success Criteria

- [ ] All 5 JSON TVFs implemented and registered as DataFusion table functions
- [ ] `WITH ORDINALITY` support for all TVFs
- [ ] All 4 SQL/JSON path functions implemented (2 scalar, 2 set-returning)
- [ ] JSON path compiler handles the PostgreSQL SQL/JSON subset
- [ ] LATERAL join integration works in streaming SQL pipelines
- [ ] Watermark propagation through TVF expansion verified
- [ ] Retraction propagation through TVF expansion verified
- [ ] Expansion limit prevents unbounded row generation
- [ ] 25+ unit tests passing
- [ ] Integration tests with streaming SQL pipelines passing
- [ ] Benchmarks meet latency targets
- [ ] All public APIs documented with `///` doc comments
- [ ] `cargo clippy -- -D warnings` passes

---

## Competitive Comparison

| Feature | LaminarDB | PostgreSQL | ClickHouse | DuckDB | RisingWave | Flink |
|---------|-----------|------------|------------|--------|------------|-------|
| `jsonb_array_elements` | Yes (TVF) | Native | `arrayJoin` | `unnest` | Yes | `CROSS JOIN UNNEST` |
| `jsonb_each` | Yes (TVF) | Native | N/A | N/A | Yes | N/A |
| `jsonb_object_keys` | Yes (TVF) | Native | `JSONExtractKeys` | `json_keys` | Yes | N/A |
| `WITH ORDINALITY` | Yes | Yes | N/A | Yes | Yes | N/A |
| SQL/JSON path queries | Yes (compiled) | Yes | N/A | `json_extract` | Yes | N/A |
| LATERAL join TVFs | Yes | Yes | N/A | Yes | Yes | N/A |
| Streaming watermark propagation | Yes | N/A | N/A | N/A | Yes | Yes |
| Retraction through TVF | Yes | N/A | N/A | N/A | Yes | Yes |
| Plan-time path compilation | Yes | No | N/A | No | No | N/A |
| Ring 0 binary iteration | Yes (<100ns/elem) | N/A | N/A | N/A | No | No |
| Expansion limit safeguard | Yes (configurable) | No | No | No | No | No |

---

## Module Structure

```
crates/laminar-sql/src/datafusion/
+-- json_tvf.rs              # TVF implementations (5 functions)
+-- json_path.rs             # SQL/JSON path compiler and evaluator
+-- json_path_parser.rs      # Path expression tokenizer and parser
+-- mod.rs                   # Updated: register_json_table_functions() call added
```

---

## Completion Checklist

- [ ] Code implemented in `laminar-sql/src/datafusion/json_tvf.rs`
- [ ] Code implemented in `laminar-sql/src/datafusion/json_path.rs`
- [ ] Registered in `create_streaming_context()` and `create_streaming_context_with_watermark()`
- [ ] LATERAL join integration in streaming planner
- [ ] Watermark propagation verified
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

- [PostgreSQL JSON Functions - Set Returning](https://www.postgresql.org/docs/16/functions-json.html#FUNCTIONS-JSON-PROCESSING-TABLE)
- [PostgreSQL SQL/JSON Path Language](https://www.postgresql.org/docs/16/functions-json.html#FUNCTIONS-SQLJSON-PATH)
- [DataFusion TableFunctionImpl](https://docs.rs/datafusion/latest/datafusion/datasource/function/trait.TableFunctionImpl.html)
- [F-SCHEMA-011: JSON Scalar Functions](F-SCHEMA-011-json-functions.md)
- [F-SCHEMA-004: JSONB Binary Format](F-SCHEMA-004-jsonb-binary.md)
- [Research: Schema Inference Design, Section 6.1](../../../research/schema-inference-design.md)
- [F-STREAM-004: Source](../streaming/F-STREAM-004-source.md) -- watermark propagation patterns
