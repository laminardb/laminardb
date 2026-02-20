# F-LSQL-001: CREATE LOOKUP TABLE DDL

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-LSQL-001 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | L (5-10 days) |
| **Dependencies** | F006B (Production SQL Parser), F-LOOKUP-001 (Lookup Table Core) |
| **Blocks** | F-LSQL-002 (Lookup Join Plan Node) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-sql` |
| **Module** | `laminar-sql/src/parser/lookup_ddl.rs` |

## Summary

Implements a new SQL DDL statement, `CREATE LOOKUP TABLE`, for declaring lookup tables with their connector, caching strategy, memory limits, and predicate pushdown configuration. Lookup tables are the reference data side of temporal lookup joins -- they represent slowly-changing dimension data (e.g., customer records, instrument metadata, configuration) that streaming queries join against. This DDL extends the existing `CREATE TABLE` parser to support a `LOOKUP` keyword and a `WITH` clause containing connector-specific properties. The parsed AST node feeds into the query planner (F-LSQL-002) and the lookup table runtime (F-LOOKUP-001).

## Goals

- Define a new `CREATE LOOKUP TABLE` DDL syntax with column definitions and WITH clause
- Parse the WITH clause into a validated property map with supported keys
- Support connector types: `postgres-cdc`, `mysql-cdc`, `redis`, `s3-parquet`, `static`
- Support strategy values: `replicated` (full local copy), `partitioned` (shard by key), `on-demand` (fetch on miss)
- Support cache configuration: `cache.memory`, `cache.disk`, `cache.ttl`
- Support pushdown modes: `auto`, `enabled`, `disabled`
- Generate an AST node (`LookupTableDef`) for downstream consumption by the planner
- Validate property types and ranges at parse time with clear error messages
- Support optional `snapshot` property for initial bulk load from object storage

## Non-Goals

- Runtime lookup table implementation (covered by F-LOOKUP-001)
- Lookup join execution (covered by F-LSQL-002, F-LSQL-003)
- ALTER LOOKUP TABLE or DROP LOOKUP TABLE (deferred to a later feature)
- Schema evolution detection for lookup tables
- Automatic connector discovery (user must specify connector type explicitly)

## Technical Design

### Architecture

**Ring**: N/A (DDL parsing is a control plane operation, not on the data path)

**Crate**: `laminar-sql`

**Module**: `laminar-sql/src/parser/lookup_ddl.rs`

The parser extends LaminarDB's existing SQL parser (built on `sqlparser-rs`) to recognize the `CREATE LOOKUP TABLE` syntax. When encountered, it produces a `LookupTableDef` AST node containing the table name, column definitions, primary key, and a validated `LookupTableProperties` map. This AST node is then consumed by the catalog (to register the table) and the planner (to generate lookup join plan nodes).

```
┌─────────────────────────────────────────────────────────────────────┐
│  SQL Input                                                           │
│                                                                      │
│  CREATE LOOKUP TABLE customers (                                     │
│    id INT PRIMARY KEY,                                               │
│    name VARCHAR,                                                     │
│    region VARCHAR                                                    │
│  ) WITH (                                                            │
│    'connector' = 'postgres-cdc',                                     │
│    'connection' = 'postgresql://host/db',                            │
│    'strategy' = 'replicated'                                         │
│  );                                                                  │
│                                                                      │
├──────────────────────┬──────────────────────────────────────────────┤
│  Parser              │  LookupTableDef AST Node                     │
│  (lookup_ddl.rs)     │  {                                           │
│                      │    name: "customers",                        │
│                      │    columns: [...],                            │
│                      │    primary_key: ["id"],                       │
│                      │    properties: {                              │
│                      │      connector: PostgresCdc,                  │
│                      │      strategy: Replicated,                    │
│                      │      cache: { memory: 512MB },               │
│                      │      pushdown: Auto,                          │
│                      │    }                                          │
│                      │  }                                           │
├──────────────────────┴──────────────────────────────────────────────┤
│  Catalog Registration       │  Planner                              │
│  (register lookup table)    │  (generate LookupJoinNode)            │
└─────────────────────────────┴───────────────────────────────────────┘
```

### API/Interface

```rust
use sqlparser::ast::{ColumnDef, ObjectName, DataType};
use std::collections::HashMap;

/// Parse a CREATE LOOKUP TABLE statement from a SQL string.
///
/// Returns a `LookupTableDef` AST node if the input is a valid
/// CREATE LOOKUP TABLE statement, or an error with position information.
///
/// # Example
///
/// ```sql
/// CREATE LOOKUP TABLE customers (
///     id INT PRIMARY KEY,
///     name VARCHAR,
///     region VARCHAR,
///     credit_limit DOUBLE
/// ) WITH (
///     'connector' = 'postgres-cdc',
///     'connection' = 'postgresql://host/db',
///     'strategy' = 'replicated',
///     'cache.memory' = '512mb',
///     'cache.disk' = '10gb',
///     'pushdown' = 'auto',
///     'snapshot' = 's3://bucket/customers.parquet'
/// );
/// ```
pub fn parse_create_lookup_table(sql: &str) -> Result<LookupTableDef, ParseError> {
    let mut parser = LookupDdlParser::new(sql)?;
    parser.parse_create_lookup_table()
}

/// AST node for a CREATE LOOKUP TABLE statement.
///
/// Contains all information needed to register the lookup table
/// in the catalog and configure its runtime behavior.
#[derive(Debug, Clone)]
pub struct LookupTableDef {
    /// Table name (may be schema-qualified: schema.table).
    pub name: ObjectName,
    /// Column definitions with types and constraints.
    pub columns: Vec<LookupColumnDef>,
    /// Primary key column names (extracted from column constraints or table constraint).
    pub primary_key: Vec<String>,
    /// Validated properties from the WITH clause.
    pub properties: LookupTableProperties,
    /// Whether IF NOT EXISTS was specified.
    pub if_not_exists: bool,
}

/// Column definition for a lookup table.
#[derive(Debug, Clone)]
pub struct LookupColumnDef {
    /// Column name.
    pub name: String,
    /// SQL data type.
    pub data_type: DataType,
    /// Whether this column is part of the primary key.
    pub is_primary_key: bool,
    /// Whether this column is nullable (default: true).
    pub nullable: bool,
}

/// Validated properties parsed from the WITH clause.
///
/// All property values are validated at parse time. Invalid values
/// produce clear error messages with the property name and expected format.
#[derive(Debug, Clone)]
pub struct LookupTableProperties {
    /// Connector type (required).
    pub connector: ConnectorType,
    /// Connection string (required for database connectors).
    pub connection: Option<String>,
    /// Lookup strategy (default: Replicated).
    pub strategy: LookupStrategy,
    /// Cache configuration.
    pub cache: CacheConfig,
    /// Predicate pushdown mode (default: Auto).
    pub pushdown: PushdownMode,
    /// Optional initial snapshot location (S3, local file).
    pub snapshot: Option<String>,
    /// Additional connector-specific properties.
    pub extra: HashMap<String, String>,
}

/// Supported connector types for lookup tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectorType {
    /// PostgreSQL CDC (logical replication).
    PostgresCdc,
    /// MySQL CDC (binlog replication).
    MysqlCdc,
    /// Redis key-value store.
    Redis,
    /// S3 Parquet files (periodic reload).
    S3Parquet,
    /// Static in-memory data (loaded once from inline VALUES or file).
    Static,
    /// Custom connector (user-provided).
    Custom(String),
}

impl ConnectorType {
    /// Parse a connector type from a string value.
    pub fn from_str(s: &str) -> Result<Self, ParseError> {
        match s.to_lowercase().as_str() {
            "postgres-cdc" | "postgres_cdc" | "pg-cdc" => Ok(ConnectorType::PostgresCdc),
            "mysql-cdc" | "mysql_cdc" => Ok(ConnectorType::MysqlCdc),
            "redis" => Ok(ConnectorType::Redis),
            "s3-parquet" | "s3_parquet" | "parquet" => Ok(ConnectorType::S3Parquet),
            "static" | "inline" => Ok(ConnectorType::Static),
            other => Ok(ConnectorType::Custom(other.to_string())),
        }
    }
}

/// Lookup table data strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupStrategy {
    /// Full local replica of the entire lookup table.
    /// Best for small-to-medium tables (< 10GB).
    Replicated,
    /// Partition the lookup table by key across nodes.
    /// Best for large tables where each node only needs a subset.
    Partitioned,
    /// Fetch records on demand (cache misses trigger remote lookup).
    /// Best for very large tables with low hit rate.
    OnDemand,
}

impl LookupStrategy {
    /// Parse a strategy from a string value.
    pub fn from_str(s: &str) -> Result<Self, ParseError> {
        match s.to_lowercase().as_str() {
            "replicated" | "full" | "local" => Ok(LookupStrategy::Replicated),
            "partitioned" | "sharded" => Ok(LookupStrategy::Partitioned),
            "on-demand" | "on_demand" | "lazy" | "fetch" => Ok(LookupStrategy::OnDemand),
            other => Err(ParseError::InvalidProperty {
                property: "strategy".to_string(),
                value: other.to_string(),
                expected: "replicated | partitioned | on-demand".to_string(),
            }),
        }
    }
}

/// Cache configuration for lookup tables.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum memory for the in-memory cache (e.g., "512mb", "2gb").
    pub memory: Option<ByteSize>,
    /// Maximum disk space for the on-disk cache tier (e.g., "10gb").
    pub disk: Option<ByteSize>,
    /// Time-to-live for cached entries (e.g., "5m", "1h").
    pub ttl: Option<std::time::Duration>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            memory: None,
            disk: None,
            ttl: None,
        }
    }
}

/// Predicate pushdown mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushdownMode {
    /// Probe source capabilities at pipeline startup.
    /// If source supports predicate pushdown, enable it.
    /// If not, fall back to local filtering.
    Auto,
    /// Always push predicates to the source.
    /// Fails if source does not support pushdown.
    Enabled,
    /// Never push predicates. All filtering done locally.
    Disabled,
}

impl PushdownMode {
    /// Parse a pushdown mode from a string value.
    pub fn from_str(s: &str) -> Result<Self, ParseError> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(PushdownMode::Auto),
            "enabled" | "true" | "yes" => Ok(PushdownMode::Enabled),
            "disabled" | "false" | "no" => Ok(PushdownMode::Disabled),
            other => Err(ParseError::InvalidProperty {
                property: "pushdown".to_string(),
                value: other.to_string(),
                expected: "auto | enabled | disabled".to_string(),
            }),
        }
    }
}

/// Byte size parsed from strings like "512mb", "10gb", "1tb".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(pub u64);

impl ByteSize {
    /// Parse a byte size from a human-readable string.
    pub fn from_str(s: &str) -> Result<Self, ParseError> {
        let s = s.trim().to_lowercase();
        let (num_str, multiplier) = if s.ends_with("tb") {
            (&s[..s.len() - 2], 1_u64 << 40)
        } else if s.ends_with("gb") {
            (&s[..s.len() - 2], 1_u64 << 30)
        } else if s.ends_with("mb") {
            (&s[..s.len() - 2], 1_u64 << 20)
        } else if s.ends_with("kb") {
            (&s[..s.len() - 2], 1_u64 << 10)
        } else if s.ends_with("b") {
            (&s[..s.len() - 1], 1_u64)
        } else {
            (s.as_str(), 1_u64)
        };

        let num: u64 = num_str
            .trim()
            .parse()
            .map_err(|_| ParseError::InvalidProperty {
                property: "cache size".to_string(),
                value: s.to_string(),
                expected: "numeric value with unit (e.g., 512mb, 10gb)".to_string(),
            })?;

        Ok(ByteSize(num * multiplier))
    }
}
```

### Data Structures

```rust
/// Parser for CREATE LOOKUP TABLE statements.
///
/// Extends the base SQL parser to handle the LOOKUP keyword
/// and WITH clause with property validation.
pub struct LookupDdlParser {
    /// The underlying sqlparser-rs parser for base SQL syntax.
    base_parser: sqlparser::parser::Parser,
    /// Raw SQL input for error reporting.
    sql: String,
}

impl LookupDdlParser {
    /// Create a new parser from a SQL string.
    pub fn new(sql: &str) -> Result<Self, ParseError> {
        let dialect = sqlparser::dialect::GenericDialect {};
        let base_parser = sqlparser::parser::Parser::new(&dialect)
            .try_with_sql(sql)
            .map_err(|e| ParseError::SyntaxError(e.to_string()))?;
        Ok(Self {
            base_parser,
            sql: sql.to_string(),
        })
    }

    /// Parse a CREATE LOOKUP TABLE statement.
    pub fn parse_create_lookup_table(&mut self) -> Result<LookupTableDef, ParseError> {
        // Expect: CREATE [IF NOT EXISTS] LOOKUP TABLE <name> (columns) WITH (properties)
        self.expect_keyword("CREATE")?;
        let if_not_exists = self.parse_if_not_exists()?;
        self.expect_keyword("LOOKUP")?;
        self.expect_keyword("TABLE")?;

        let name = self.parse_object_name()?;
        let (columns, primary_key) = self.parse_column_defs()?;

        self.expect_keyword("WITH")?;
        let raw_properties = self.parse_with_clause()?;
        let properties = Self::validate_properties(raw_properties)?;

        Ok(LookupTableDef {
            name,
            columns,
            primary_key,
            properties,
            if_not_exists,
        })
    }

    /// Parse the WITH clause into a raw key-value map.
    fn parse_with_clause(&mut self) -> Result<HashMap<String, String>, ParseError> {
        self.expect_token("(")?;
        let mut props = HashMap::new();

        loop {
            let key = self.parse_string_literal()?;
            self.expect_token("=")?;
            let value = self.parse_string_literal()?;
            props.insert(key, value);

            if !self.try_consume_token(",") {
                break;
            }
        }

        self.expect_token(")")?;
        Ok(props)
    }

    /// Validate raw properties into a typed LookupTableProperties struct.
    fn validate_properties(
        raw: HashMap<String, String>,
    ) -> Result<LookupTableProperties, ParseError> {
        // Required: connector
        let connector_str = raw
            .get("connector")
            .ok_or_else(|| ParseError::MissingProperty("connector".to_string()))?;
        let connector = ConnectorType::from_str(connector_str)?;

        // Optional: connection (required for database connectors)
        let connection = raw.get("connection").cloned();
        if matches!(
            connector,
            ConnectorType::PostgresCdc | ConnectorType::MysqlCdc
        ) && connection.is_none()
        {
            return Err(ParseError::MissingProperty(format!(
                "connection (required for {:?} connector)",
                connector
            )));
        }

        // Optional: strategy (default: Replicated)
        let strategy = match raw.get("strategy") {
            Some(s) => LookupStrategy::from_str(s)?,
            None => LookupStrategy::Replicated,
        };

        // Optional: cache config
        let cache = CacheConfig {
            memory: raw
                .get("cache.memory")
                .map(|s| ByteSize::from_str(s))
                .transpose()?,
            disk: raw
                .get("cache.disk")
                .map(|s| ByteSize::from_str(s))
                .transpose()?,
            ttl: raw
                .get("cache.ttl")
                .map(|s| parse_duration(s))
                .transpose()?,
        };

        // Optional: pushdown (default: Auto)
        let pushdown = match raw.get("pushdown") {
            Some(s) => PushdownMode::from_str(s)?,
            None => PushdownMode::Auto,
        };

        // Optional: snapshot
        let snapshot = raw.get("snapshot").cloned();

        // Collect remaining properties as extras
        let known_keys = [
            "connector", "connection", "strategy", "cache.memory",
            "cache.disk", "cache.ttl", "pushdown", "snapshot",
        ];
        let extra: HashMap<String, String> = raw
            .into_iter()
            .filter(|(k, _)| !known_keys.contains(&k.as_str()))
            .collect();

        Ok(LookupTableProperties {
            connector,
            connection,
            strategy,
            cache,
            pushdown,
            snapshot,
            extra,
        })
    }
}

/// Parse errors from the lookup DDL parser.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// SQL syntax error from the base parser.
    #[error("Syntax error: {0}")]
    SyntaxError(String),

    /// Required property missing from WITH clause.
    #[error("Missing required property: {0}")]
    MissingProperty(String),

    /// Property value is invalid.
    #[error("Invalid value for property '{property}': '{value}' (expected: {expected})")]
    InvalidProperty {
        property: String,
        value: String,
        expected: String,
    },

    /// Unexpected token during parsing.
    #[error("Unexpected token: expected '{expected}', found '{found}'")]
    UnexpectedToken { expected: String, found: String },

    /// Column definition error.
    #[error("Column error: {0}")]
    ColumnError(String),

    /// No primary key defined.
    #[error("Lookup table must have a PRIMARY KEY")]
    NoPrimaryKey,
}
```

### Algorithm/Flow

#### Parse Flow

```
Input: "CREATE LOOKUP TABLE customers (...) WITH (...)"

1. Tokenize SQL string using sqlparser-rs
2. Match CREATE keyword
3. Check for optional IF NOT EXISTS
4. Match LOOKUP keyword (distinguishes from regular CREATE TABLE)
5. Match TABLE keyword
6. Parse table name (optional schema qualifier)
7. Parse column definitions:
   a. Parse each column: name, type, constraints
   b. Extract PRIMARY KEY from column constraints or table-level constraint
   c. Validate at least one PRIMARY KEY column exists
8. Match WITH keyword
9. Parse WITH clause:
   a. Match opening parenthesis
   b. Parse key-value pairs: 'key' = 'value'
   c. Match closing parenthesis
10. Validate properties:
    a. Check required properties (connector)
    b. Parse and validate each property value
    c. Check conditional requirements (connection for db connectors)
    d. Apply defaults (strategy=Replicated, pushdown=Auto)
    e. Collect unknown properties as extras
11. Construct and return LookupTableDef AST node
```

#### Catalog Registration Flow

```
After parsing:

1. LookupTableDef is passed to the catalog manager
2. Catalog creates a table entry with:
   a. Table name and schema
   b. Column types (mapped to Arrow DataTypes)
   c. Primary key columns
   d. Lookup strategy
   e. Connector configuration
3. Catalog registers the table as available for lookup joins
4. When a query references this table with temporal join syntax,
   the planner recognizes it as a lookup table and generates
   a LookupJoinNode (F-LSQL-002)
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `ParseError::SyntaxError` | Malformed SQL (missing parentheses, typos) | Return error with position; user fixes SQL |
| `ParseError::MissingProperty` | Required WITH property not specified | Return error naming the missing property |
| `ParseError::InvalidProperty` | Property value not in allowed set | Return error with expected values |
| `ParseError::NoPrimaryKey` | No column marked as PRIMARY KEY | Return error; lookup tables require PK for join |
| `ParseError::ColumnError` | Duplicate column name or unsupported type | Return error with column details |
| `ParseError::UnexpectedToken` | Unexpected token during parsing | Return error with expected vs found |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Parse time for typical DDL | < 1ms | `bench_parse_lookup_ddl` |
| Property validation time | < 100us | `bench_validate_properties` |
| Memory for LookupTableDef | < 2KB | Static analysis |
| ByteSize parsing | < 100ns | `bench_parse_byte_size` |

## Test Plan

### Unit Tests

- [ ] `test_parse_basic_lookup_table` - Minimal valid DDL with required properties
- [ ] `test_parse_full_lookup_table_all_properties` - DDL with all optional properties
- [ ] `test_parse_if_not_exists` - IF NOT EXISTS clause
- [ ] `test_parse_schema_qualified_name` - `schema.table` name
- [ ] `test_parse_multiple_primary_key_columns` - Composite primary key
- [ ] `test_parse_connector_postgres_cdc` - Connector type parsing
- [ ] `test_parse_connector_mysql_cdc` - MySQL CDC connector
- [ ] `test_parse_connector_redis` - Redis connector
- [ ] `test_parse_connector_s3_parquet` - S3 Parquet connector
- [ ] `test_parse_connector_custom` - Unknown connector stored as Custom
- [ ] `test_parse_strategy_replicated` - Strategy parsing
- [ ] `test_parse_strategy_partitioned` - Partitioned strategy
- [ ] `test_parse_strategy_on_demand` - On-demand strategy
- [ ] `test_parse_strategy_default_is_replicated` - Missing strategy defaults
- [ ] `test_parse_cache_memory_mb` - ByteSize parsing for MB
- [ ] `test_parse_cache_memory_gb` - ByteSize parsing for GB
- [ ] `test_parse_cache_disk` - Disk cache size
- [ ] `test_parse_cache_ttl` - Duration parsing for TTL
- [ ] `test_parse_pushdown_auto` - Pushdown mode parsing
- [ ] `test_parse_pushdown_enabled` - Pushdown enabled
- [ ] `test_parse_pushdown_disabled` - Pushdown disabled
- [ ] `test_parse_pushdown_default_is_auto` - Missing pushdown defaults
- [ ] `test_parse_snapshot_url` - Snapshot S3 URL
- [ ] `test_parse_extra_properties_preserved` - Unknown properties in extra map
- [ ] `test_parse_missing_connector_returns_error` - Required property missing
- [ ] `test_parse_missing_connection_for_postgres_returns_error` - Conditional requirement
- [ ] `test_parse_invalid_strategy_returns_error` - Bad strategy value
- [ ] `test_parse_invalid_cache_size_returns_error` - Non-numeric cache size
- [ ] `test_parse_no_primary_key_returns_error` - Missing PK
- [ ] `test_parse_empty_with_clause_returns_error` - No properties at all

### Integration Tests

- [ ] `test_parse_and_register_in_catalog` - Parse DDL, register table, query catalog
- [ ] `test_lookup_table_available_for_join` - Registered lookup table recognized in JOIN
- [ ] `test_parse_real_world_customers_table` - Full customers example from docs
- [ ] `test_parse_real_world_instruments_table` - Financial instruments lookup table
- [ ] `test_roundtrip_parse_and_serialize` - Parse, serialize to SQL, re-parse, compare

### Benchmarks

- [ ] `bench_parse_lookup_ddl` - Target: < 1ms for typical DDL
- [ ] `bench_validate_properties` - Target: < 100us
- [ ] `bench_parse_byte_size` - Target: < 100ns

## Rollout Plan

1. **Phase 1**: Define AST node types (`LookupTableDef`, `LookupTableProperties`, enums)
2. **Phase 2**: Implement `LookupDdlParser` extending sqlparser-rs
3. **Phase 3**: Implement property validation with defaults
4. **Phase 4**: Implement `ByteSize` and duration parsing utilities
5. **Phase 5**: Wire parser into LaminarDB's main SQL parser dispatch
6. **Phase 6**: Implement catalog registration for lookup tables
7. **Phase 7**: Unit tests for all parse paths and error cases
8. **Phase 8**: Integration tests with catalog and planner
9. **Phase 9**: Documentation and code review

## Open Questions

- [ ] Should the WITH clause use single-quoted strings for keys (SQL standard) or unquoted identifiers? Current design uses single-quoted for both keys and values for consistency.
- [ ] Should we support `CREATE OR REPLACE LOOKUP TABLE` for updating connector configuration without dropping the table?
- [ ] Should the `connection` property support environment variable references (e.g., `'${PG_URL}'`) for secret management?
- [ ] Should there be a `REFRESH INTERVAL` property for periodic full snapshot reload (applicable to S3 Parquet and static sources)?
- [ ] How to handle the case where the source schema changes after table creation? Should we support `ALTER LOOKUP TABLE ADD COLUMN`?

## Completion Checklist

- [ ] `LookupTableDef` AST node defined
- [ ] `LookupTableProperties` with all property types
- [ ] `ConnectorType`, `LookupStrategy`, `PushdownMode`, `CacheConfig` enums
- [ ] `LookupDdlParser` implementation
- [ ] `ByteSize` parsing utility
- [ ] Property validation with defaults and error messages
- [ ] `ParseError` enum with all variants
- [ ] Integration with main SQL parser dispatch
- [ ] Catalog registration for lookup tables
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated (`#![deny(missing_docs)]` clean)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [sqlparser-rs](https://docs.rs/sqlparser) -- SQL parser library
- [Apache Flink CREATE TABLE](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/create/#create-table) -- Flink's WITH clause design
- [F006B: Production SQL Parser](../../phase-1/F006B-production-sql-parser.md) -- Base SQL parser
- [F-LOOKUP-001: Lookup Table Core](../lookup/F-LOOKUP-001-lookup-table-core.md) -- Runtime lookup table
- [F-LSQL-002: Lookup Join Plan Node](./F-LSQL-002-lookup-join-plan-node.md) -- Plan node consuming this DDL
- [ARCHITECTURE.md](../../../ARCHITECTURE.md) -- System design
