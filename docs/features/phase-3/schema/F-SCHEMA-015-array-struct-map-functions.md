# F-SCHEMA-015: Array, Struct & Map Functions

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-015 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-SCHEMA-001 (Core Schema Traits) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-21 |
| **Crate** | `laminar-sql` |
| **Module** | `laminar-sql/src/datafusion/complex_type_udf.rs` |

## Summary

Add SQL functions for working with Arrow's complex types: List (arrays),
Struct, and Map. This includes element access, construction, transformation,
and -- critically -- lambda-based higher-order functions (`array_transform`,
`array_filter`, `array_reduce`, `map_filter`, `map_transform_values`). Many
array and struct functions already exist in DataFusion and only need
registration; others require custom UDF implementations.

## Motivation

Complex types are fundamental to streaming data. IoT payloads carry arrays of
sensor readings. Clickstream events contain maps of properties. Financial
messages nest structs within structs. Users need to manipulate these types
directly in SQL without flattening to multiple rows or resorting to application
code.

```sql
-- Filter high-value readings from sensor array
SELECT device_id,
       array_filter(readings, x -> x > threshold) AS alerts
FROM sensor_events;

-- Merge two config maps, overriding with user preferences
SELECT struct_merge(default_config, user_config) AS effective_config
FROM config_events;

-- Extract keys from a property map for indexing
SELECT event_id, map_keys(properties) AS prop_names
FROM events;
```

Without these functions, users must either unnest arrays to rows (expensive and
schema-distorting), handle complex types in application code (breaking the SQL
pipeline), or store everything as JSON strings (losing type safety and
performance).

## Goals

- Register DataFusion built-in array functions that already exist
- Implement custom struct functions: `struct_extract`, `struct_set`,
  `struct_drop`, `struct_rename`, `struct_merge`
- Implement custom map functions: `map_keys`, `map_values`,
  `map_contains_key`, `map_from_arrays`, `map_from_entries`, `element_at`
- Implement lambda-based higher-order functions: `array_transform`,
  `array_filter`, `array_reduce`, `map_filter`, `map_transform_values`
- All functions produce proper Arrow-typed outputs (not JSON strings)
- Zero-allocation where possible for simple accessors

## Non-Goals

- JSON-specific functions (covered by F-SCHEMA-013)
- UNNEST / table-valued functions (covered by DataFusion core)
- User-defined lambda syntax in the SQL parser (use the `x -> expr` sugar that
  maps to DataFusion's `LambdaFunction` expression)
- Nested lambda composition (e.g., `array_transform(arr, x -> array_filter(x.inner, ...))`)

## Technical Design

### Architecture

The implementation is split into three tiers based on existing DataFusion
support:

```
Tier 1: Already in DataFusion (register only)
  array_length, array_contains, array_position, array_distinct,
  array_sort, array_slice, array_flatten, array_agg, unnest,
  element_at, map_from_entries

Tier 2: Custom scalar UDFs (straightforward)
  struct_extract, struct_set, struct_drop, struct_rename, struct_merge,
  map_keys, map_values, map_contains_key, map_from_arrays

Tier 3: Lambda UDFs (complex, needs expression evaluation)
  array_transform, array_filter, array_reduce,
  map_filter, map_transform_values
```

### Ring Integration

| Operation | Ring | Latency Budget | Notes |
|-----------|------|----------------|-------|
| UDF registration | Ring 2 | One-time at startup | `register_complex_type_functions()` |
| Lambda compilation | Ring 2 | Milliseconds | Parse `x -> expr` once per query plan |
| `struct_extract` | Ring 0 | < 10ns/row | Direct Arrow column index lookup |
| `map_keys` / `map_values` | Ring 0 | < 50ns/row | Arrow Map column access |
| `array_filter` | Ring 1 | < 1us/row | Per-element predicate evaluation |
| `array_reduce` | Ring 1 | < 1us/row | Sequential fold over elements |
| `struct_merge` | Ring 1 | < 500ns/row | Field concatenation with dedup |

### API Design

#### Registration

```rust
use datafusion::prelude::SessionContext;
use datafusion_expr::ScalarUDF;

/// Register all complex-type functions with a DataFusion session context.
///
/// Includes array, struct, and map functions. DataFusion built-in
/// functions (Tier 1) are verified as already registered; custom
/// functions (Tier 2 and Tier 3) are explicitly registered.
pub fn register_complex_type_functions(ctx: &SessionContext) {
    // Tier 2: Struct functions
    ctx.register_udf(ScalarUDF::new_from_impl(StructExtractUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(StructSetUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(StructDropUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(StructRenameUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(StructMergeUdf::new()));

    // Tier 2: Map functions
    ctx.register_udf(ScalarUDF::new_from_impl(MapKeysUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(MapValuesUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(MapContainsKeyUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(MapFromArraysUdf::new()));

    // Tier 3: Lambda functions
    ctx.register_udf(ScalarUDF::new_from_impl(ArrayTransformUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ArrayFilterUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ArrayReduceUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(MapFilterUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(MapTransformValuesUdf::new()));
}
```

#### Tier 1: DataFusion Built-in Verification

DataFusion already registers many array functions. We verify their availability
at startup rather than re-registering:

```rust
/// Verify DataFusion built-in array functions are available.
///
/// Panics at startup if expected functions are missing, which would
/// indicate a DataFusion version mismatch.
pub fn verify_builtin_array_functions(ctx: &SessionContext) {
    let expected = [
        "array_length", "array_contains", "array_position",
        "array_distinct", "array_sort", "array_slice",
        "array_flatten", "array_agg",
    ];
    for name in expected {
        assert!(
            ctx.udf(name).is_ok(),
            "Expected DataFusion built-in function '{name}' not found. \
             Check DataFusion version compatibility."
        );
    }
}
```

#### Tier 2: Custom Struct Functions

```rust
/// `struct_extract(record, field)` -- Extract a field from a struct column.
///
/// Returns the value of the named field. Equivalent to `record.field_name`
/// dot notation but usable in dynamic contexts.
///
/// # Arguments
/// - `record`: Struct column
/// - `field`: Utf8 literal field name
///
/// # Example
/// ```sql
/// SELECT struct_extract(address, 'city') FROM customers;
/// ```
#[derive(Debug)]
pub struct StructExtractUdf {
    signature: Signature,
}

impl ScalarUDFImpl for StructExtractUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "struct_extract" }
    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        // Return type is the type of the named field within the struct.
        // Requires the field name to be a literal at planning time.
        match &arg_types[0] {
            DataType::Struct(_fields) => {
                // Field name resolved at planning time via return_type_from_exprs
                Ok(DataType::Utf8) // Placeholder
            }
            other => Err(datafusion_common::DataFusionError::Plan(
                format!("struct_extract requires Struct input, got {other}")
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // Extract the named field column from the struct array
        todo!()
    }
}

/// `struct_set(record, field, value)` -- Set or add a field in a struct.
///
/// Returns a new struct with the field set to the given value. If the
/// field already exists, its value is replaced. If it does not exist,
/// it is appended.
///
/// # Example
/// ```sql
/// SELECT struct_set(config, 'timeout', 30) FROM settings;
/// ```
#[derive(Debug)]
pub struct StructSetUdf {
    signature: Signature,
}

/// `struct_drop(record, field)` -- Remove a field from a struct.
///
/// Returns a new struct with the named field removed. Returns an error
/// if the field does not exist.
///
/// # Example
/// ```sql
/// SELECT struct_drop(event, 'internal_id') FROM events;
/// ```
#[derive(Debug)]
pub struct StructDropUdf {
    signature: Signature,
}

/// `struct_rename(record, old_name, new_name)` -- Rename a struct field.
///
/// # Example
/// ```sql
/// SELECT struct_rename(record, 'ts', 'event_time') FROM events;
/// ```
#[derive(Debug)]
pub struct StructRenameUdf {
    signature: Signature,
}

/// `struct_merge(record1, record2)` -- Merge two structs.
///
/// Fields from `record2` take precedence on name conflicts.
/// The result contains all fields from both structs.
///
/// # Example
/// ```sql
/// SELECT struct_merge(defaults, overrides) FROM config_events;
/// ```
#[derive(Debug)]
pub struct StructMergeUdf {
    signature: Signature,
}
```

#### Tier 2: Custom Map Functions

```rust
/// `map_keys(m)` -- Return all keys from a map as an array.
///
/// # Example
/// ```sql
/// SELECT map_keys(properties) FROM events;
/// -- Returns: ['color', 'size', 'weight']
/// ```
#[derive(Debug)]
pub struct MapKeysUdf {
    signature: Signature,
}

impl ScalarUDFImpl for MapKeysUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "map_keys" }
    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        match &arg_types[0] {
            DataType::Map(entries_field, _) => {
                match entries_field.data_type() {
                    DataType::Struct(fields) => {
                        let key_type = fields[0].data_type().clone();
                        Ok(DataType::List(Arc::new(Field::new("item", key_type, false))))
                    }
                    _ => Err(datafusion_common::DataFusionError::Plan(
                        "Map entries field is not a Struct".into()
                    )),
                }
            }
            other => Err(datafusion_common::DataFusionError::Plan(
                format!("map_keys requires Map input, got {other}")
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // Extract the keys array from the Map column
        todo!()
    }
}

/// `map_values(m)` -- Return all values from a map as an array.
#[derive(Debug)]
pub struct MapValuesUdf {
    signature: Signature,
}

/// `map_contains_key(m, key)` -- Check if a map contains a key.
///
/// # Example
/// ```sql
/// SELECT * FROM events WHERE map_contains_key(properties, 'priority');
/// ```
#[derive(Debug)]
pub struct MapContainsKeyUdf {
    signature: Signature,
}

/// `map_from_arrays(keys, values)` -- Construct a map from two arrays.
///
/// # Example
/// ```sql
/// SELECT map_from_arrays(
///     ARRAY['a', 'b', 'c'],
///     ARRAY[1, 2, 3]
/// ) AS m;
/// ```
#[derive(Debug)]
pub struct MapFromArraysUdf {
    signature: Signature,
}
```

#### Tier 3: Lambda Functions

Lambda functions are the most complex part of this feature. They require
evaluating a user-provided expression for each element in an array or map.

```rust
use datafusion_expr::Expr;

/// Compiled lambda expression for per-element evaluation.
///
/// Constructed at query planning time from the `x -> expr` syntax.
/// The lambda variable is bound to each element during execution.
#[derive(Debug)]
pub struct CompiledLambda {
    /// The variable name(s) bound in the lambda (e.g., "x" or "(k, v)").
    pub params: Vec<String>,
    /// The expression to evaluate for each element.
    pub body: Arc<dyn PhysicalExpr>,
    /// Output type of the lambda body.
    pub output_type: DataType,
}

/// `array_transform(arr, lambda)` -- Apply a function to each array element.
///
/// Returns a new array where each element is the result of applying
/// the lambda to the corresponding input element.
///
/// # Example
/// ```sql
/// SELECT array_transform(prices, x -> x * 1.1) AS adjusted FROM trades;
/// SELECT array_transform(names, x -> upper(x)) AS upper_names FROM events;
/// ```
#[derive(Debug)]
pub struct ArrayTransformUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ArrayTransformUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "array_transform" }
    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        // Return List with the lambda's output type.
        // Determined at planning time by type-checking the lambda body.
        match &arg_types[0] {
            DataType::List(_) => {
                // Lambda output type resolved via return_type_from_exprs
                Ok(arg_types[0].clone()) // Placeholder: same list type
            }
            other => Err(datafusion_common::DataFusionError::Plan(
                format!("array_transform requires List input, got {other}")
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // For each row:
        //   1. Get the List array for this row
        //   2. For each element, bind the lambda variable and evaluate
        //   3. Collect results into a new List array
        // Optimization: batch evaluation using Arrow compute kernels
        // where the lambda is a simple arithmetic or string expression.
        todo!()
    }
}

/// `array_filter(arr, lambda)` -- Filter array elements by predicate.
///
/// # Example
/// ```sql
/// SELECT array_filter(scores, x -> x >= 90) AS high_scores FROM students;
/// ```
#[derive(Debug)]
pub struct ArrayFilterUdf {
    signature: Signature,
}

/// `array_reduce(arr, init, lambda)` -- Fold/reduce an array to a scalar.
///
/// # Arguments
/// - `arr`: List column
/// - `init`: Initial accumulator value (scalar)
/// - `lambda`: `(acc, x) -> expr` -- binary lambda
///
/// # Example
/// ```sql
/// SELECT array_reduce(amounts, 0, (acc, x) -> acc + x) AS total
/// FROM transactions;
/// ```
#[derive(Debug)]
pub struct ArrayReduceUdf {
    signature: Signature,
}

/// `map_filter(m, lambda)` -- Filter map entries by predicate.
///
/// # Arguments
/// - `m`: Map column
/// - `lambda`: `(k, v) -> boolean_expr` -- binary lambda
///
/// # Example
/// ```sql
/// SELECT map_filter(metrics, (k, v) -> v > 0) AS positive_metrics
/// FROM telemetry;
/// ```
#[derive(Debug)]
pub struct MapFilterUdf {
    signature: Signature,
}

/// `map_transform_values(m, lambda)` -- Transform map values.
///
/// # Arguments
/// - `m`: Map column
/// - `lambda`: `(k, v) -> new_v` -- binary lambda producing new values
///
/// # Example
/// ```sql
/// SELECT map_transform_values(tags, (k, v) -> upper(v)) AS normalized_tags
/// FROM events;
/// ```
#[derive(Debug)]
pub struct MapTransformValuesUdf {
    signature: Signature,
}
```

### SQL Interface

#### Array Functions

```sql
-- DataFusion built-in (Tier 1)
SELECT array_length(readings) FROM sensors;
SELECT array_contains(tags, 'urgent') FROM events;
SELECT array_position(scores, 100) FROM students;
SELECT array_distinct(categories) FROM products;
SELECT array_sort(timestamps) FROM logs;
SELECT array_slice(readings, 1, 5) FROM sensors;
SELECT array_flatten(nested_arrays) FROM complex_data;

-- Lambda functions (Tier 3)
SELECT array_transform(prices, x -> x * exchange_rate) FROM trades;
SELECT array_filter(readings, x -> x > threshold) FROM sensors;
SELECT array_reduce(quantities, 0, (acc, x) -> acc + x) AS total FROM orders;
```

#### Struct Functions

```sql
-- Field access
SELECT struct_extract(address, 'city') FROM customers;

-- Field modification
SELECT struct_set(config, 'timeout', 30) FROM settings;
SELECT struct_drop(event, 'debug_info') FROM events;
SELECT struct_rename(record, 'ts', 'event_time') FROM raw_data;

-- Struct merging (record2 wins on conflict)
SELECT struct_merge(
    STRUCT('default' AS mode, 100 AS timeout),
    STRUCT('fast' AS mode)
) AS merged;
-- Result: {mode: 'fast', timeout: 100}
```

#### Map Functions

```sql
-- Key/value access
SELECT map_keys(properties) FROM events;
SELECT map_values(properties) FROM events;
SELECT element_at(properties, 'color') FROM events;
SELECT map_contains_key(headers, 'Authorization') FROM requests;

-- Construction
SELECT map_from_arrays(
    ARRAY['host', 'port', 'protocol'],
    ARRAY['localhost', '8080', 'https']
) AS config;

SELECT map_from_entries(
    ARRAY[STRUCT('a' AS key, 1 AS value), STRUCT('b' AS key, 2 AS value)]
) AS m;

-- Lambda functions
SELECT map_filter(metrics, (k, v) -> v > 0.0) FROM telemetry;
SELECT map_transform_values(tags, (k, v) -> lower(v)) FROM events;
```

### Data Structures

```rust
/// Metadata about a complex-type function for introspection.
#[derive(Debug, Clone)]
pub struct ComplexTypeFunctionMeta {
    /// Function name as used in SQL.
    pub name: &'static str,
    /// Implementation tier.
    pub tier: FunctionTier,
    /// Input types accepted.
    pub input_types: &'static str,
    /// Description for SHOW FUNCTIONS output.
    pub description: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub enum FunctionTier {
    /// Already in DataFusion -- register-only.
    BuiltIn,
    /// Custom scalar UDF -- straightforward implementation.
    Custom,
    /// Lambda UDF -- requires expression compilation.
    Lambda,
}

/// Catalog of all complex-type functions for SHOW FUNCTIONS support.
pub static COMPLEX_TYPE_FUNCTIONS: &[ComplexTypeFunctionMeta] = &[
    // Array -- built-in
    ComplexTypeFunctionMeta {
        name: "array_length",
        tier: FunctionTier::BuiltIn,
        input_types: "List<T> -> Int64",
        description: "Returns the number of elements in the array",
    },
    ComplexTypeFunctionMeta {
        name: "array_contains",
        tier: FunctionTier::BuiltIn,
        input_types: "List<T>, T -> Boolean",
        description: "Returns true if the array contains the value",
    },
    // ... additional entries for each function ...

    // Struct -- custom
    ComplexTypeFunctionMeta {
        name: "struct_extract",
        tier: FunctionTier::Custom,
        input_types: "Struct, Utf8 -> T",
        description: "Extract a field from a struct by name",
    },
    ComplexTypeFunctionMeta {
        name: "struct_merge",
        tier: FunctionTier::Custom,
        input_types: "Struct, Struct -> Struct",
        description: "Merge two structs (second wins on conflict)",
    },

    // Lambda
    ComplexTypeFunctionMeta {
        name: "array_transform",
        tier: FunctionTier::Lambda,
        input_types: "List<T>, (T -> U) -> List<U>",
        description: "Apply a lambda to each array element",
    },
    ComplexTypeFunctionMeta {
        name: "array_filter",
        tier: FunctionTier::Lambda,
        input_types: "List<T>, (T -> Boolean) -> List<T>",
        description: "Filter array elements by predicate lambda",
    },
    ComplexTypeFunctionMeta {
        name: "array_reduce",
        tier: FunctionTier::Lambda,
        input_types: "List<T>, U, ((U, T) -> U) -> U",
        description: "Fold array to scalar using binary lambda",
    },
];
```

### Error Handling

```rust
pub enum SqlError {
    // ... existing variants ...

    /// Struct field not found in struct_extract / struct_drop / struct_rename.
    #[error("struct field '{field}' not found in {struct_type}")]
    StructFieldNotFound {
        field: String,
        struct_type: String,
    },

    /// Invalid lambda expression in array_transform / array_filter / etc.
    #[error("invalid lambda expression in {function}(): {reason}")]
    InvalidLambda {
        function: String,
        reason: String,
    },

    /// Type mismatch in lambda result.
    #[error("lambda in {function}() returned {actual}, expected {expected}")]
    LambdaTypeMismatch {
        function: String,
        expected: String,
        actual: String,
    },

    /// Map key type mismatch in map_contains_key / element_at.
    #[error("map key type mismatch: map has {map_key_type} keys, got {provided_type}")]
    MapKeyTypeMismatch {
        map_key_type: String,
        provided_type: String,
    },

    /// Array length mismatch in map_from_arrays.
    #[error("map_from_arrays: keys array length ({keys}) != values array length ({values})")]
    MapFromArraysLengthMismatch {
        keys: usize,
        values: usize,
    },
}
```

## Implementation Plan

### Step 1: Tier 1 Verification (0.5 day)

- Audit which array functions DataFusion already registers
- Implement `verify_builtin_array_functions()` and call from startup
- Document any version-specific gaps in a compatibility matrix
- Write verification tests

### Step 2: Struct Functions (1 day)

- Implement `StructExtractUdf` -- direct Arrow StructArray column access
- Implement `StructSetUdf` -- rebuild StructArray with modified field
- Implement `StructDropUdf` -- rebuild StructArray without field
- Implement `StructRenameUdf` -- rebuild with renamed field
- Implement `StructMergeUdf` -- concatenate fields, dedup on conflict

### Step 3: Map Functions (1 day)

- Implement `MapKeysUdf` -- extract keys from Arrow MapArray
- Implement `MapValuesUdf` -- extract values from Arrow MapArray
- Implement `MapContainsKeyUdf` -- iterate entries for key match
- Implement `MapFromArraysUdf` -- construct MapArray from two ListArrays

### Step 4: Lambda Infrastructure (1 day)

- Implement `CompiledLambda` -- parse `x -> expr` and compile to PhysicalExpr
- Implement per-element evaluation loop with batched optimization
- Handle variable binding and scope resolution
- Type inference for lambda body output

### Step 5: Lambda Functions (1 day)

- Implement `ArrayTransformUdf` using `CompiledLambda`
- Implement `ArrayFilterUdf` using `CompiledLambda`
- Implement `ArrayReduceUdf` with sequential fold semantics
- Implement `MapFilterUdf` with `(k, v) -> bool` lambda
- Implement `MapTransformValuesUdf` with `(k, v) -> new_v` lambda

### Step 6: Integration (0.5 day)

- Wire `register_complex_type_functions()` into `create_streaming_context()`
- Add function catalog entries for `SHOW FUNCTIONS`
- End-to-end SQL test with nested array/struct/map combinations

## Testing Strategy

| Module | Tests | What |
|--------|-------|------|
| `builtin_verify` | 3 | Verify all Tier 1 functions exist, version compatibility |
| `struct_extract` | 6 | Simple field, nested struct, missing field error, null struct, type preservation |
| `struct_set` | 5 | New field, replace existing, null value, nested struct modification |
| `struct_drop` | 4 | Drop existing, drop missing (error), drop last field, nested |
| `struct_rename` | 4 | Basic rename, missing field error, rename to existing name |
| `struct_merge` | 5 | No conflict, conflict (second wins), nested, empty struct, null handling |
| `map_keys` | 4 | Basic extraction, empty map, null map, key type preservation |
| `map_values` | 4 | Basic extraction, empty map, null map, mixed value types |
| `map_contains_key` | 4 | Present key, absent key, null key, type mismatch error |
| `map_from_arrays` | 5 | Basic construction, length mismatch error, null handling, empty arrays |
| `array_transform` | 6 | Arithmetic lambda, string lambda, nested access, null handling, empty array, type change |
| `array_filter` | 5 | Basic predicate, filter all, filter none, null elements, complex predicate |
| `array_reduce` | 5 | Sum, string concat, custom init, empty array, single element |
| `map_filter` | 4 | Basic predicate, filter all, key-based filter, value-based filter |
| `map_transform_values` | 4 | Arithmetic, string function, key-dependent transform, null values |
| `registration` | 2 | All UDFs registered, callable via SQL |
| `integration` | 5 | Combined queries: struct_merge + map_keys, array_filter + array_reduce, nested lambda |

## Success Criteria

| Metric | Target | Validation |
|--------|--------|------------|
| `struct_extract` latency | < 10ns/row | Benchmark: direct column index |
| `map_keys` latency | < 50ns/row | Benchmark: MapArray key extraction |
| `array_transform` throughput | > 200K rows/sec | Benchmark: 100-element array, simple lambda |
| `array_filter` throughput | > 200K rows/sec | Benchmark: 100-element array, comparison predicate |
| `struct_merge` latency | < 500ns/row | Benchmark: two 10-field structs |
| All 76+ tests passing | 100% | `cargo test` |
| `cargo clippy` clean | Zero warnings | CI gate |
| All public APIs documented | 100% | `#![deny(missing_docs)]` |

## Competitive Comparison

| Feature | LaminarDB | Flink | RisingWave | Materialize | ClickHouse | DuckDB |
|---------|-----------|-------|------------|-------------|------------|--------|
| `array_length` | DataFusion | `CARDINALITY` | `array_length` | `array_length` | `length` | `len` |
| `array_contains` | DataFusion | `ARRAY_CONTAINS` | `array_contains` | `@>` | `has` | `list_contains` |
| `array_transform` | Planned | No | No | No | `arrayMap` | `list_transform` |
| `array_filter` | Planned | No | No | No | `arrayFilter` | `list_filter` |
| `array_reduce` | Planned | No | No | No | `arrayReduce` | `list_reduce` |
| `struct_extract` | Planned | Dot notation | Dot notation | Dot notation | `tupleElement` | `struct_extract` |
| `struct_set` | Planned | No | No | No | No | No |
| `struct_drop` | Planned | No | No | No | No | No |
| `struct_merge` | Planned | No | No | No | No | No |
| `map_keys` | Planned | `MAP_KEYS` | `map_keys` | `map_keys` | `mapKeys` | `map_keys` |
| `map_values` | Planned | `MAP_VALUES` | No | `map_values` | `mapValues` | `map_values` |
| `map_filter` | Planned | No | No | No | `mapFilter` | No |
| `map_transform_values` | Planned | No | No | No | `mapApply` | No |
| Lambda syntax | `x -> expr` | No | No | No | `x -> expr` | `x -> expr` |

LaminarDB's struct modification functions (`struct_set`, `struct_drop`,
`struct_rename`, `struct_merge`) are unique among streaming databases. Combined
with lambda support matching ClickHouse/DuckDB capabilities, this provides the
most complete complex-type function set in the streaming SQL space.

## Files

- `crates/laminar-sql/src/datafusion/complex_type_udf.rs` -- NEW: All Tier 2 and Tier 3 UDF implementations
- `crates/laminar-sql/src/datafusion/lambda.rs` -- NEW: CompiledLambda, lambda parsing and evaluation
- `crates/laminar-sql/src/datafusion/mod.rs` -- Wire into registration functions
- `crates/laminar-sql/src/error/mod.rs` -- New error variants
