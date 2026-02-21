# F-SCHEMA-016: Schema Hints with Wildcard Inference

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-016 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-SCHEMA-002 (Schema Inference Engine), F-SCHEMA-003 (Schema Registry Integration) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-21 |
| **Crate** | `laminar-connectors` (SDK), `laminar-sql` (parser) |
| **Module** | `laminar-connectors/src/sdk/schema.rs`, `laminar-sql/src/parser/source_parser.rs` |

## Summary

Add wildcard schema hints to `CREATE SOURCE` DDL, allowing users to declare a
partial schema for critical columns while letting LaminarDB auto-infer the
remaining fields from sample data. The `*` wildcard in a column list signals
"infer everything I did not explicitly declare." An optional `wildcard_prefix`
avoids field name collisions between declared and inferred columns. The merge
logic lives in `SchemaResolver::merge_with_declared()` and tracks each field's
origin (`UserDeclared` vs `WildcardInferred`) for introspection via
`DESCRIBE SOURCE`.

## Motivation

Streaming data sources often have evolving schemas with dozens or hundreds of
fields, yet the user only cares about a handful of critical columns with
specific types. Today, users must either:

1. **Declare every column** -- tedious and brittle when schemas change
2. **Use pure inference** -- lose control over critical column types and
   nullability
3. **Skip inference entirely** -- lose access to undeclared fields

Schema hints with wildcard solve this by combining the best of explicit and
inferred schemas:

```sql
-- Declare only the fields you care about; infer the rest
CREATE SOURCE sensor_data (
    device_id   VARCHAR NOT NULL,     -- must be non-nullable string
    temperature DOUBLE,               -- must be DOUBLE (not inferred as INT)
    *                                 -- infer remaining fields from data
) FROM KAFKA (
    brokers = 'localhost:9092',
    topic   = 'sensors'
)
FORMAT JSON
INFER SCHEMA (
    sample_size = 500,
    wildcard_prefix = 'inf_'          -- prefix inferred fields to avoid collisions
);
```

The result is a frozen Arrow schema where `device_id` and `temperature` have
the exact types the user specified, and all other fields discovered in the data
(e.g., `inf_humidity`, `inf_battery_pct`, `inf_firmware_version`) are included
with inferred types. If the source data later adds a new field, re-running
`CREATE OR REPLACE SOURCE` picks it up automatically.

## Goals

- Parse `*` wildcard in `CREATE SOURCE` column lists
- Parse `wildcard_prefix` option in `INFER SCHEMA (...)` clause
- Implement `DeclaredSchema` struct with `has_wildcard` and `wildcard_prefix`
- Implement `SchemaResolver::merge_with_declared()` with field origin tracking
- Track field origins: `UserDeclared`, `WildcardInferred`, `AutoResolved`
- Display field origins in `DESCRIBE SOURCE` output
- Support wildcard with all inference backends (JSON, CSV, schema registry)
- Validate that declared columns do not conflict with inferred columns

## Non-Goals

- Wildcard in `CREATE SINK` (sinks always have explicit schemas)
- Wildcard in `CREATE TABLE` (reference tables require full schemas)
- Runtime schema evolution after source creation (covered by F-SCHEMA-005)
- Wildcard with `SELECT *` semantics (this is DDL-only, not query-time)
- Negative wildcards / field exclusion patterns (e.g., `* EXCEPT (field)`)

## Technical Design

### Architecture

Schema hint resolution happens entirely in Ring 2 at `CREATE SOURCE` time.
The flow is:

```
SQL Parser (Ring 2)
  CREATE SOURCE s (device_id VARCHAR NOT NULL, temperature DOUBLE, *)
  FROM KAFKA (...) FORMAT JSON INFER SCHEMA (sample_size=500, wildcard_prefix='inf_')
          │                              │                     │
          ▼                              ▼                     ▼
    DeclaredSchema {               SourceConnector       InferenceConfig {
      columns: [device_id, temp],  .as_schema_inferable()  sample_size: 500,
      has_wildcard: true,                │                  ...
      wildcard_prefix: Some("inf_"),     │                }
    }                                    │
          │                              │
          └──────────────┬───────────────┘
                         ▼
               SchemaResolver::resolve()
                         │
          ┌──────────────┼──────────────┐
          │              │              │
          ▼              ▼              ▼
     Fully declared?  Registry?    Inferable?
     No (has *)       Maybe        Yes → sample_records()
                                        infer_from_samples()
                                              │
                                              ▼
                                        InferredSchema {
                                          schema: [device_id, temperature,
                                                   humidity, battery_pct, ...]
                                        }
                                              │
                         ┌────────────────────┘
                         ▼
          merge_with_declared()
            1. Add device_id (UserDeclared, VARCHAR NOT NULL)
            2. Add temperature (UserDeclared, DOUBLE)
            3. Skip device_id from inferred (conflict with declared)
            4. Skip temperature from inferred (conflict with declared)
            5. Add inf_humidity (WildcardInferred, DOUBLE)
            6. Add inf_battery_pct (WildcardInferred, DOUBLE)
            7. Add inf_firmware_version (WildcardInferred, VARCHAR)
                         │
                         ▼
               ResolvedSchema {
                 schema: Arc<Schema>,
                 field_origins: {
                   device_id: UserDeclared,
                   temperature: UserDeclared,
                   inf_humidity: WildcardInferred,
                   inf_battery_pct: WildcardInferred,
                   inf_firmware_version: WildcardInferred,
                 }
               }
                         │
                         ▼
               Frozen Arrow schema → Ring 1 decoder
```

### Ring Integration

| Operation | Ring | Latency Budget | Notes |
|-----------|------|----------------|-------|
| DDL parsing (wildcard detection) | Ring 2 | Milliseconds | One-time at CREATE SOURCE |
| Sample collection | Ring 2 | Seconds | Kafka consume N messages |
| Schema inference | Ring 2 | Seconds | JSON/CSV type analysis |
| Merge with declared | Ring 2 | Microseconds | Simple field iteration |
| DESCRIBE SOURCE | Ring 2 | Microseconds | Read frozen metadata |
| Hot-path decoding | Ring 0/1 | < 500ns | Uses frozen schema, no merge |

### API Design

#### Parser Changes (`source_parser.rs`)

```rust
use super::statements::CreateSourceStatement;

/// Detect the `*` wildcard token in a column definition list.
///
/// When parsing the column list in `CREATE SOURCE name (col1 TYPE, *, ...)`,
/// a standalone `*` (Token::Mul) is interpreted as the wildcard marker.
///
/// Rules:
/// - At most one `*` per column list
/// - `*` cannot have a type annotation or constraints
/// - `*` can appear at any position in the column list
fn parse_source_columns_with_wildcard(
    parser: &mut Parser,
) -> Result<(Vec<ColumnDef>, bool), ParseError> {
    let mut columns = Vec::new();
    let mut has_wildcard = false;

    parser.expect_token(&Token::LParen).map_err(ParseError::SqlParseError)?;

    loop {
        // Check for wildcard
        if parser.peek_token().token == Token::Mul {
            if has_wildcard {
                return Err(ParseError::SemanticError(
                    "Duplicate wildcard '*' in column list. \
                     Only one wildcard is allowed per CREATE SOURCE.".into()
                ));
            }
            parser.next_token(); // consume *
            has_wildcard = true;
        } else if parser.peek_token().token == Token::RParen {
            break;
        } else {
            // Regular column definition
            let col = parser.parse_column_def()
                .map_err(ParseError::SqlParseError)?;
            columns.push(col);
        }

        // Comma or end
        if !parser.consume_token(&Token::Comma) {
            break;
        }
    }

    parser.expect_token(&Token::RParen).map_err(ParseError::SqlParseError)?;
    Ok((columns, has_wildcard))
}

/// Parse the `INFER SCHEMA (...)` clause, including `wildcard_prefix`.
///
/// Supported options:
/// - `sample_size = N`
/// - `sample_timeout = 'duration'`
/// - `null_as = 'TYPE'`
/// - `number_as = 'TYPE'`
/// - `timestamp_formats = ('fmt1', 'fmt2', ...)`
/// - `max_depth = N`
/// - `array_strategy = 'first_element' | 'union' | 'jsonb'`
/// - `wildcard_prefix = 'prefix_'`  <-- NEW
fn parse_infer_schema_clause(
    parser: &mut Parser,
) -> Result<InferSchemaOptions, ParseError> {
    // ... existing option parsing ...
    // NEW: parse wildcard_prefix option
    todo!()
}
```

#### Core Data Structures (`laminar-connectors/src/sdk/schema.rs`)

```rust
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

/// User-provided schema declaration from DDL.
///
/// Constructed by the SQL parser from a `CREATE SOURCE` statement.
/// Passed to `SchemaResolver::resolve()` to produce the final frozen schema.
#[derive(Debug, Clone)]
pub struct DeclaredSchema {
    /// Explicitly declared columns with types.
    pub columns: Vec<DeclaredColumn>,
    /// Whether a wildcard `*` was present in the column list.
    pub has_wildcard: bool,
    /// Prefix to prepend to wildcard-inferred field names.
    /// `None` means no prefix (inferred fields keep their original names).
    pub wildcard_prefix: Option<String>,
}

/// A single explicitly declared column.
#[derive(Debug, Clone)]
pub struct DeclaredColumn {
    /// Column name.
    pub name: String,
    /// Arrow data type.
    pub data_type: DataType,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// Optional default value expression (SQL string).
    pub default_expr: Option<String>,
}

/// Origin of a field in the resolved schema.
///
/// Used for introspection (DESCRIBE SOURCE) and diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldOrigin {
    /// Column was explicitly declared in the DDL.
    UserDeclared,
    /// Column was inferred via wildcard from sample data.
    WildcardInferred,
    /// Column was auto-resolved (no user declaration at all).
    AutoResolved,
    /// Column was added with a default value during schema evolution.
    DefaultAdded,
}

impl std::fmt::Display for FieldOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UserDeclared => write!(f, "declared"),
            Self::WildcardInferred => write!(f, "inferred (wildcard)"),
            Self::AutoResolved => write!(f, "inferred"),
            Self::DefaultAdded => write!(f, "default"),
        }
    }
}

/// Fully resolved schema with field origin tracking.
///
/// Produced by `SchemaResolver::resolve()`. The `field_origins` map
/// enables `DESCRIBE SOURCE` to show where each column came from.
#[derive(Debug, Clone)]
pub struct ResolvedSchema {
    /// The frozen Arrow schema.
    pub schema: SchemaRef,
    /// How the schema was resolved.
    pub resolution: ResolutionKind,
    /// Per-field origin tracking (field name -> origin).
    pub field_origins: HashMap<String, FieldOrigin>,
    /// Warnings generated during resolution.
    pub warnings: Vec<InferenceWarning>,
}

/// How the schema was resolved.
#[derive(Debug, Clone)]
pub enum ResolutionKind {
    /// Fully declared by user (no inference).
    Declared,
    /// Provided by the source itself (Parquet metadata, Iceberg catalog).
    SourceProvided,
    /// Fetched from an external schema registry.
    Registry { schema_id: u32 },
    /// Inferred from sample data.
    Inferred {
        sample_count: usize,
        warnings: Vec<InferenceWarning>,
    },
    /// Hybrid: user declared some columns, rest inferred/provided.
    Hybrid {
        declared_count: usize,
        inferred_count: usize,
        sample_count: usize,
    },
}

/// Warning generated during schema inference.
#[derive(Debug, Clone)]
pub struct InferenceWarning {
    pub field_name: String,
    pub message: String,
    pub severity: WarningSeverity,
}

#[derive(Debug, Clone, Copy)]
pub enum WarningSeverity {
    Info,
    Warning,
    Error,
}
```

#### Schema Resolver (`merge_with_declared`)

```rust
use std::collections::HashSet;

impl SchemaResolver {
    /// Merge user-declared columns with auto-resolved schema.
    ///
    /// User declarations take priority. If the declared schema has a
    /// wildcard, non-conflicting resolved columns are added with the
    /// optional prefix.
    ///
    /// # Merge Algorithm
    ///
    /// 1. Add all explicitly declared columns first (exact type/nullability
    ///    from user DDL).
    /// 2. If `has_wildcard` is true:
    ///    a. Collect declared column names into a HashSet for O(1) lookup.
    ///    b. For each field in the resolved (inferred/registry) schema:
    ///       - If the field name matches a declared column, skip it
    ///         (user declaration wins).
    ///       - Otherwise, add it with the wildcard prefix and mark as
    ///         `WildcardInferred`.
    /// 3. All wildcard-inferred fields are forced nullable (we cannot
    ///    guarantee non-null for auto-discovered fields).
    /// 4. Generate warnings for type mismatches between declared and
    ///    inferred types for the same field name.
    ///
    /// # Arguments
    ///
    /// * `declared` - User-provided schema declaration (may be None)
    /// * `resolved` - Schema from inference, registry, or source provider
    /// * `resolution_kind` - How the resolved schema was obtained
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::Incompatible` if a declared column's type
    /// fundamentally conflicts with the inferred type (e.g., user declares
    /// `col INT` but inference finds it is always a JSON object).
    pub fn merge_with_declared(
        &self,
        declared: Option<&DeclaredSchema>,
        resolved: SchemaRef,
        resolution_kind: ResolutionKind,
    ) -> SchemaResult<ResolvedSchema> {
        let Some(decl) = declared else {
            // No user declarations -- use resolved as-is
            return Ok(ResolvedSchema {
                field_origins: resolved.fields().iter()
                    .map(|f| (f.name().clone(), FieldOrigin::AutoResolved))
                    .collect(),
                schema: resolved,
                resolution: resolution_kind,
                warnings: vec![],
            });
        };

        let mut fields = Vec::new();
        let mut origins = HashMap::new();
        let mut warnings = Vec::new();

        // Step 1: Add all explicitly declared columns
        for col in &decl.columns {
            fields.push(Field::new(&col.name, col.data_type.clone(), col.nullable));
            origins.insert(col.name.clone(), FieldOrigin::UserDeclared);
        }

        // Step 2: If wildcard, add non-conflicting resolved columns
        if decl.has_wildcard {
            let prefix = decl.wildcard_prefix.as_deref().unwrap_or("");
            let declared_names: HashSet<&str> = decl.columns.iter()
                .map(|c| c.name.as_str())
                .collect();

            for field in resolved.fields() {
                if declared_names.contains(field.name().as_str()) {
                    // Field conflicts with declared column -- skip, but
                    // warn if types differ
                    if let Some(declared_col) = decl.columns.iter()
                        .find(|c| c.name == *field.name())
                    {
                        if declared_col.data_type != *field.data_type() {
                            warnings.push(InferenceWarning {
                                field_name: field.name().clone(),
                                message: format!(
                                    "Declared type {} differs from inferred type {}; \
                                     using declared type",
                                    declared_col.data_type, field.data_type()
                                ),
                                severity: WarningSeverity::Info,
                            });
                        }
                    }
                    continue;
                }

                // Add with prefix, forced nullable
                let prefixed_name = format!("{prefix}{}", field.name());

                // Check for prefix collision with declared names
                if declared_names.contains(prefixed_name.as_str()) {
                    warnings.push(InferenceWarning {
                        field_name: prefixed_name.clone(),
                        message: format!(
                            "Inferred field '{}' with prefix '{}' collides with \
                             declared column '{}'; skipping inferred field",
                            field.name(), prefix, prefixed_name,
                        ),
                        severity: WarningSeverity::Warning,
                    });
                    continue;
                }

                fields.push(Field::new(
                    &prefixed_name,
                    field.data_type().clone(),
                    true, // wildcard-inferred fields are always nullable
                ));
                origins.insert(prefixed_name, FieldOrigin::WildcardInferred);
            }
        }

        let inferred_count = origins.values()
            .filter(|o| matches!(o, FieldOrigin::WildcardInferred))
            .count();

        Ok(ResolvedSchema {
            schema: Arc::new(Schema::new(fields)),
            resolution: ResolutionKind::Hybrid {
                declared_count: decl.columns.len(),
                inferred_count,
                sample_count: match &resolution_kind {
                    ResolutionKind::Inferred { sample_count, .. } => *sample_count,
                    _ => 0,
                },
            },
            field_origins: origins,
            warnings,
        })
    }
}
```

### SQL Interface

#### CREATE SOURCE with Wildcard

```sql
-- Minimal: wildcard only (infer everything, no prefix)
CREATE SOURCE raw_events (
    *
) FROM KAFKA (
    brokers = 'localhost:9092',
    topic   = 'events'
)
FORMAT JSON
INFER SCHEMA (sample_size = 1000);

-- Typical: declare critical columns, infer the rest
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
    sample_size = 500,
    wildcard_prefix = 'inf_'
);

-- With schema registry: declare critical columns, merge with registry schema
CREATE SOURCE trades (
    symbol VARCHAR NOT NULL,
    price  DOUBLE NOT NULL,
    *
) FROM KAFKA (
    brokers = 'broker1:9092',
    topic   = 'trades'
)
FORMAT AVRO
USING SCHEMA REGISTRY (
    url = 'http://schema-registry:8081'
);
-- Registry provides: symbol, price, volume, exchange, ts
-- Result: symbol (declared, NOT NULL), price (declared, NOT NULL),
--         volume (wildcard-inferred), exchange (wildcard-inferred),
--         ts (wildcard-inferred)

-- CSV with wildcard: headers inferred from first row
CREATE SOURCE csv_data (
    id   BIGINT NOT NULL,
    *
) FROM FILE (
    path = '/data/events.csv'
)
FORMAT CSV (has_header = true)
INFER SCHEMA (
    sample_size = 100,
    wildcard_prefix = 'col_'
);

-- Wildcard at beginning of column list (order does not matter)
CREATE SOURCE events (
    *,
    event_id VARCHAR NOT NULL,
    timestamp TIMESTAMP NOT NULL
) FROM KAFKA (
    brokers = 'localhost:9092',
    topic   = 'events'
)
FORMAT JSON
INFER SCHEMA (sample_size = 1000);
```

#### DESCRIBE SOURCE with Field Origins

```sql
DESCRIBE SOURCE sensor_data;
```

Output:

```
+-------------------------+------------+----------+----------------------+
| Column                  | Type       | Nullable | Origin               |
+-------------------------+------------+----------+----------------------+
| device_id               | VARCHAR    | NO       | declared             |
| temperature             | DOUBLE     | YES      | declared             |
| inf_humidity            | DOUBLE     | YES      | inferred (wildcard)  |
| inf_battery_pct         | DOUBLE     | YES      | inferred (wildcard)  |
| inf_firmware_version    | VARCHAR    | YES      | inferred (wildcard)  |
| inf_location_lat        | DOUBLE     | YES      | inferred (wildcard)  |
| inf_location_lon        | DOUBLE     | YES      | inferred (wildcard)  |
+-------------------------+------------+----------+----------------------+
Resolution: hybrid (2 declared, 5 inferred from 500 samples)
```

### Data Structures

See the API Design section above for `DeclaredSchema`, `DeclaredColumn`,
`FieldOrigin`, `ResolvedSchema`, `ResolutionKind`, and `InferenceWarning`.

Additional structures for the parser:

```rust
/// Parsed INFER SCHEMA options from DDL.
#[derive(Debug, Clone)]
pub struct InferSchemaOptions {
    /// Number of records to sample.
    pub sample_size: Option<usize>,
    /// Maximum time to wait for samples.
    pub sample_timeout: Option<String>,
    /// Default type for null-only fields.
    pub null_as: Option<String>,
    /// Default numeric type.
    pub number_as: Option<String>,
    /// Timestamp format patterns.
    pub timestamp_formats: Option<Vec<String>>,
    /// Max nesting depth.
    pub max_depth: Option<usize>,
    /// Array inference strategy.
    pub array_strategy: Option<String>,
    /// Prefix for wildcard-inferred fields. NEW.
    pub wildcard_prefix: Option<String>,
}

impl InferSchemaOptions {
    /// Convert to the core InferenceConfig used by SchemaInferable.
    pub fn to_inference_config(&self) -> InferenceConfig {
        let mut config = InferenceConfig::default();
        if let Some(n) = self.sample_size {
            config.sample_size = n;
        }
        if let Some(ref d) = self.sample_timeout {
            config.sample_timeout = parse_duration(d)
                .unwrap_or(config.sample_timeout);
        }
        if let Some(n) = self.max_depth {
            config.max_depth = n;
        }
        // ... map remaining options ...
        config
    }
}
```

### Error Handling

```rust
pub enum SqlError {
    // ... existing variants ...

    /// Duplicate wildcard in column list.
    #[error("Duplicate wildcard '*' in column list. Only one wildcard is allowed per CREATE SOURCE.")]
    DuplicateWildcard,

    /// Wildcard used without INFER SCHEMA or schema registry.
    #[error(
        "Wildcard '*' in column list requires either INFER SCHEMA (...) or \
         USING SCHEMA REGISTRY (...) to resolve remaining fields."
    )]
    WildcardWithoutInference,

    /// Wildcard prefix collision with declared column.
    #[error(
        "Wildcard prefix '{prefix}' causes collision: inferred field \
         '{inferred}' would become '{prefixed}', which conflicts with \
         declared column '{prefixed}'."
    )]
    WildcardPrefixCollision {
        prefix: String,
        inferred: String,
        prefixed: String,
    },

    /// No fields could be inferred for wildcard expansion.
    #[error(
        "Wildcard '*' found no additional fields to infer from {sample_count} \
         samples. All fields in the data match declared columns."
    )]
    WildcardNoNewFields {
        sample_count: usize,
    },
}
```

## Implementation Plan

### Step 1: Parser Changes (1 day)

- Modify `parse_source_columns_with_wildcard()` in `source_parser.rs` to
  detect `Token::Mul` as wildcard marker
- Add `has_wildcard` flag to `CreateSourceStatement`
- Parse `wildcard_prefix` option in `parse_infer_schema_clause()`
- Add `InferSchemaOptions` struct with all supported options
- Parser error for duplicate wildcards

### Step 2: Core Data Structures (0.5 day)

- Add `DeclaredSchema`, `DeclaredColumn`, `FieldOrigin` to
  `laminar-connectors/src/sdk/schema.rs`
- Add `ResolvedSchema`, `ResolutionKind`, `InferenceWarning` structs
- Implement `FieldOrigin::Display` for DESCRIBE output

### Step 3: Schema Resolver (1 day)

- Implement `SchemaResolver::resolve()` with the priority chain:
  fully-declared > registry > self-describing > inferable
- Implement `SchemaResolver::merge_with_declared()` with wildcard expansion
- Field origin tracking for all resolution paths
- Warning generation for type mismatches and prefix collisions
- Validation: wildcard requires either INFER SCHEMA or registry

### Step 4: DESCRIBE Integration (0.5 day)

- Extend `DESCRIBE SOURCE` output to include the `Origin` column
- Display resolution summary line (e.g., "hybrid: 2 declared, 5 inferred")
- Store `ResolvedSchema` metadata in the source catalog entry

### Step 5: End-to-End Wiring (1 day)

- Wire parser output into `SchemaResolver` in the DDL execution path
- Integrate with existing `infer_schema_from_samples()` in
  `laminar-connectors/src/sdk/schema.rs`
- Wire with `SchemaRegistryAware` sources (Avro + registry + wildcard)
- Handle `CREATE OR REPLACE SOURCE` with schema re-inference
- Test with Kafka JSON source + wildcard

## Testing Strategy

| Module | Tests | What |
|--------|-------|------|
| `source_parser` | 10 | Wildcard detection, wildcard position (start/middle/end), duplicate wildcard error, wildcard with no other columns, wildcard_prefix parsing, INFER SCHEMA full options |
| `declared_schema` | 5 | Construction, field lookup, has_wildcard flag, empty columns + wildcard, wildcard_prefix propagation |
| `merge_with_declared` | 12 | No wildcard (declared only), wildcard adds all inferred, prefix application, name collision skipping, type mismatch warning, empty inference result, declared-only fields when no wildcard, prefix collision detection, forced nullable on inferred, registry + wildcard merge, multiple declared + wildcard |
| `schema_resolver` | 8 | Full resolution chain, wildcard with JSON inference, wildcard with registry, wildcard without inference source (error), wildcard with CSV, hybrid resolution kind, sample_count propagation |
| `describe_source` | 4 | Origin column display, resolution summary, declared + inferred ordering, pure declared source (no wildcard) |
| `error_handling` | 5 | Duplicate wildcard, wildcard without inference, prefix collision, no new fields, invalid wildcard_prefix characters |
| `integration` | 4 | End-to-end Kafka JSON + wildcard, CREATE OR REPLACE re-inference, wildcard + schema registry, DESCRIBE after wildcard creation |

## Success Criteria

| Metric | Target | Validation |
|--------|--------|------------|
| Schema resolution latency (wildcard) | < 10s for 1000 samples | Integration test with timer |
| Merge latency (no I/O) | < 100us for 100-field schema | Unit benchmark |
| DESCRIBE SOURCE response | < 1ms | Unit test |
| Parser handles wildcard at any position | 100% | Parser unit tests |
| All 48+ tests passing | 100% | `cargo test` |
| `cargo clippy` clean | Zero warnings | CI gate |
| All public APIs documented | 100% | `#![deny(missing_docs)]` |

## Competitive Comparison

| Feature | LaminarDB | Flink | RisingWave | Materialize | ClickHouse | DuckDB |
|---------|-----------|-------|------------|-------------|------------|--------|
| Explicit schema in DDL | Yes | Yes | Yes | Yes | Yes | Yes |
| Schema registry | Yes | Yes | Yes | Yes | Yes (Avro) | No |
| Auto-inference (JSON) | Yes | No | No | No | Yes (file) | Yes (file) |
| Auto-inference (CSV) | Yes | No | No | No | Yes (file) | Yes (file) |
| Wildcard `*` in DDL | **Planned** | **No** | **No** | **No** | **No** | **No** |
| Wildcard prefix | **Planned** | **No** | **No** | **No** | **No** | **No** |
| Hybrid declared + inferred | **Planned** | **No** | **No** | **No** | **No** | **No** |
| Field origin tracking | **Planned** | **No** | **No** | **No** | **No** | **No** |
| DESCRIBE shows origin | **Planned** | **No** | **No** | **No** | **No** | **No** |

Wildcard schema hints are a **unique differentiator** for LaminarDB. No other
streaming database (Flink, RisingWave, Materialize) or OLAP engine
(ClickHouse, DuckDB) offers the ability to partially declare a schema and
auto-infer the rest within a single DDL statement. This bridges the gap between
production-grade explicit schemas and developer-friendly auto-inference,
providing the best of both worlds.

## Files

- `crates/laminar-sql/src/parser/source_parser.rs` -- Wildcard detection in column list, INFER SCHEMA parsing
- `crates/laminar-sql/src/parser/statements.rs` -- Add `has_wildcard`, `wildcard_prefix` to `CreateSourceStatement`
- `crates/laminar-connectors/src/sdk/schema.rs` -- `DeclaredSchema`, `DeclaredColumn`, `FieldOrigin`, `ResolvedSchema`, `SchemaResolver`
- `crates/laminar-sql/src/translator/streaming_ddl.rs` -- Wire parser output into resolver
- `crates/laminar-sql/src/error/mod.rs` -- New error variants
- `crates/laminar-sql/src/datafusion/execute.rs` -- DESCRIBE SOURCE origin column
