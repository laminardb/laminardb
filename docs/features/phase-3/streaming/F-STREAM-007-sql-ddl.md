# F-STREAM-007: SQL DDL

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-STREAM-007 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F006B (Production SQL Parser) |
| **Owner** | TBD |
| **Created** | 2026-01-28 |
| **Updated** | 2026-01-28 |

## Summary

SQL DDL statements for defining streaming sources and sinks. This extends the existing SQL parser (F006B) with `CREATE SOURCE` and `CREATE SINK` statements that integrate with the streaming API.

**Key Design Principle**: NO channel option in DDL - channel type is always automatically derived.

## Goals

- `CREATE SOURCE` with column definitions and configuration
- `CREATE SINK` to connect materialized views to output
- Watermark clause for event-time processing
- WITH clause for configuration options
- Integration with existing F006B parser infrastructure

## Non-Goals

- User-specified channel types (always automatic)
- External connector DDL (Phase 4: Kafka, CDC)
- ALTER SOURCE/SINK (use DROP + CREATE)
- Partitioning clauses (derived from queries)

## Technical Design

### Architecture

**Crate**: `laminar-sql`
**Module**: `laminar-sql/src/parser/streaming_ddl.rs`

```
┌─────────────────────────────────────────────────────────────────┐
│                      SQL DDL Architecture                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  User SQL:                                                       │
│  ┌────────────────────────────────────────────────────────┐     │
│  │ CREATE SOURCE trades (                                  │     │
│  │     symbol VARCHAR NOT NULL,                            │     │
│  │     price DOUBLE NOT NULL,                              │     │
│  │     qty BIGINT NOT NULL,                                │     │
│  │     ts TIMESTAMP NOT NULL,                              │     │
│  │     WATERMARK FOR ts AS ts - INTERVAL '100ms'           │     │
│  │ ) WITH (                                                │     │
│  │     buffer_size = 131072,                               │     │
│  │     backpressure = 'block'                              │     │
│  │ );                                                      │     │
│  └────────────────────────────────────────────────────────┘     │
│                              │                                   │
│                              ▼                                   │
│  ┌────────────────────────────────────────────────────────┐     │
│  │                  SQL Parser (F006B)                     │     │
│  │                                                         │     │
│  │  parse_create_source()                                  │     │
│  │  parse_create_sink()                                    │     │
│  │                                                         │     │
│  └────────────────────────────────────────────────────────┘     │
│                              │                                   │
│                              ▼                                   │
│  ┌────────────────────────────────────────────────────────┐     │
│  │               SourceDefinition / SinkDefinition         │     │
│  │                                                         │     │
│  │  name: "trades"                                         │     │
│  │  columns: [symbol, price, qty, ts]                      │     │
│  │  watermark: Some(ts - 100ms)                            │     │
│  │  config: { buffer_size: 131072, ... }                   │     │
│  │                                                         │     │
│  └────────────────────────────────────────────────────────┘     │
│                              │                                   │
│                              ▼                                   │
│  ┌────────────────────────────────────────────────────────┐     │
│  │                   Catalog Registration                   │     │
│  │                                                         │     │
│  │  Sources: { "trades" → SourceDefinition }               │     │
│  │  Sinks: { "output" → SinkDefinition }                   │     │
│  │                                                         │     │
│  └────────────────────────────────────────────────────────┘     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::collections::HashMap;
use std::time::Duration;

/// Parsed CREATE SOURCE statement.
#[derive(Debug, Clone)]
pub struct SourceDefinition {
    /// Source name
    pub name: String,

    /// Column definitions
    pub columns: Vec<ColumnDefinition>,

    /// Arrow schema (derived from columns)
    pub schema: SchemaRef,

    /// Watermark specification
    pub watermark: Option<WatermarkSpec>,

    /// Configuration options from WITH clause
    pub config: SourceConfigOptions,
}

/// Column definition.
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    /// Column name
    pub name: String,

    /// SQL data type
    pub data_type: SqlDataType,

    /// Arrow data type (converted)
    pub arrow_type: DataType,

    /// NOT NULL constraint
    pub not_null: bool,

    /// Optional default value
    pub default: Option<String>,
}

/// SQL data types supported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlDataType {
    /// Variable-length string
    Varchar,
    /// Fixed-length string
    Char(usize),
    /// 32-bit integer
    Int,
    /// 64-bit integer
    BigInt,
    /// 16-bit integer
    SmallInt,
    /// 32-bit float
    Float,
    /// 64-bit float
    Double,
    /// Boolean
    Boolean,
    /// Timestamp (microseconds since epoch)
    Timestamp,
    /// Timestamp with timezone
    TimestampTz,
    /// Date (days since epoch)
    Date,
    /// Time
    Time,
    /// Interval
    Interval,
    /// Decimal with precision and scale
    Decimal(u8, i8),
    /// Binary data
    Binary,
    /// JSON
    Json,
}

impl SqlDataType {
    /// Convert to Arrow DataType.
    pub fn to_arrow(&self) -> DataType {
        match self {
            SqlDataType::Varchar => DataType::Utf8,
            SqlDataType::Char(_) => DataType::Utf8,
            SqlDataType::Int => DataType::Int32,
            SqlDataType::BigInt => DataType::Int64,
            SqlDataType::SmallInt => DataType::Int16,
            SqlDataType::Float => DataType::Float32,
            SqlDataType::Double => DataType::Float64,
            SqlDataType::Boolean => DataType::Boolean,
            SqlDataType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            SqlDataType::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            SqlDataType::Date => DataType::Date32,
            SqlDataType::Time => DataType::Time64(TimeUnit::Microsecond),
            SqlDataType::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
            SqlDataType::Decimal(p, s) => DataType::Decimal128(*p, *s),
            SqlDataType::Binary => DataType::Binary,
            SqlDataType::Json => DataType::Utf8, // JSON stored as string
        }
    }
}

/// Watermark specification.
#[derive(Debug, Clone)]
pub struct WatermarkSpec {
    /// Column name for event timestamp
    pub column: String,

    /// Watermark expression (e.g., "ts - INTERVAL '100ms'")
    pub expression: String,

    /// Parsed delay duration
    pub delay: Duration,
}

/// Configuration options from WITH clause.
///
/// **NOTE**: No `channel` field - channel type is NEVER configurable.
#[derive(Debug, Clone, Default)]
pub struct SourceConfigOptions {
    /// Buffer size (power of 2)
    pub buffer_size: Option<usize>,

    /// Backpressure strategy: 'block', 'drop_oldest', 'reject'
    pub backpressure: Option<String>,

    /// Wait strategy: 'spin', 'spin_yield', 'park'
    pub wait_strategy: Option<String>,

    /// Checkpoint interval (e.g., '10 seconds')
    pub checkpoint_interval: Option<Duration>,

    /// WAL mode: 'async', 'sync'
    pub wal_mode: Option<String>,
}

impl SourceConfigOptions {
    /// Convert to runtime SourceConfig.
    pub fn to_source_config(&self) -> SourceConfig {
        let mut config = SourceConfig::default();

        if let Some(size) = self.buffer_size {
            config.buffer_size = size;
        }

        if let Some(ref bp) = self.backpressure {
            config.backpressure = match bp.as_str() {
                "block" => Backpressure::Block,
                "drop_oldest" => Backpressure::DropOldest,
                "reject" => Backpressure::Reject,
                _ => Backpressure::Block,
            };
        }

        if let Some(ref ws) = self.wait_strategy {
            config.wait_strategy = match ws.as_str() {
                "spin" => WaitStrategy::Spin,
                "spin_yield" => WaitStrategy::SpinYield(100),
                "park" => WaitStrategy::Park,
                _ => WaitStrategy::SpinYield(100),
            };
        }

        config.checkpoint_interval = self.checkpoint_interval;

        if let Some(ref wm) = self.wal_mode {
            config.wal_mode = match wm.as_str() {
                "async" => Some(WalMode::Async),
                "sync" => Some(WalMode::Sync),
                _ => None,
            };
        }

        config
    }
}

/// Parsed CREATE SINK statement.
#[derive(Debug, Clone)]
pub struct SinkDefinition {
    /// Sink name
    pub name: String,

    /// Source materialized view name
    pub from_view: String,

    /// Configuration options
    pub config: SinkConfigOptions,
}

/// Sink configuration options.
///
/// **NOTE**: No `channel` field - always auto-derived from topology.
#[derive(Debug, Clone, Default)]
pub struct SinkConfigOptions {
    /// Buffer size per subscriber
    pub buffer_size: Option<usize>,

    /// Wait strategy
    pub wait_strategy: Option<String>,
}
```

### Grammar

```
-- CREATE SOURCE
create_source_stmt ::=
    'CREATE' 'SOURCE' identifier '(' column_defs ')' [with_clause]

column_defs ::=
    column_def (',' column_def)* [',' watermark_clause]

column_def ::=
    identifier data_type ['NOT' 'NULL'] ['DEFAULT' literal]

watermark_clause ::=
    'WATERMARK' 'FOR' identifier 'AS' watermark_expr

watermark_expr ::=
    identifier '-' 'INTERVAL' string_literal

with_clause ::=
    'WITH' '(' option (',' option)* ')'

option ::=
    identifier '=' (number | string_literal)

-- CREATE SINK
create_sink_stmt ::=
    'CREATE' 'SINK' identifier 'FROM' identifier [with_clause]

-- Data types
data_type ::=
    'VARCHAR' | 'CHAR' '(' number ')'
    | 'INT' | 'INTEGER'
    | 'BIGINT'
    | 'SMALLINT'
    | 'FLOAT' | 'REAL'
    | 'DOUBLE' ['PRECISION']
    | 'BOOLEAN' | 'BOOL'
    | 'TIMESTAMP' ['WITH' 'TIME' 'ZONE']
    | 'DATE'
    | 'TIME'
    | 'INTERVAL'
    | 'DECIMAL' '(' number [',' number] ')'
    | 'BINARY' | 'BYTEA'
    | 'JSON'
```

### Parser Implementation

```rust
impl StreamingDdlParser {
    /// Parse CREATE SOURCE statement.
    pub fn parse_create_source(&mut self) -> Result<SourceDefinition, ParseError> {
        self.expect_keyword("CREATE")?;
        self.expect_keyword("SOURCE")?;

        let name = self.parse_identifier()?;
        self.expect_token(Token::LeftParen)?;

        let (columns, watermark) = self.parse_column_definitions()?;
        self.expect_token(Token::RightParen)?;

        let config = if self.peek_keyword("WITH") {
            self.parse_with_clause()?
        } else {
            SourceConfigOptions::default()
        };

        // Build Arrow schema from columns
        let fields: Vec<Field> = columns.iter()
            .map(|c| Field::new(&c.name, c.arrow_type.clone(), !c.not_null))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        Ok(SourceDefinition {
            name,
            columns,
            schema,
            watermark,
            config,
        })
    }

    /// Parse column definitions including optional watermark.
    fn parse_column_definitions(&mut self) -> Result<(Vec<ColumnDefinition>, Option<WatermarkSpec>), ParseError> {
        let mut columns = Vec::new();
        let mut watermark = None;

        loop {
            // Check for WATERMARK clause
            if self.peek_keyword("WATERMARK") {
                watermark = Some(self.parse_watermark_clause()?);
            } else {
                // Regular column
                columns.push(self.parse_column_definition()?);
            }

            // Check for comma or end
            if !self.try_consume(Token::Comma) {
                break;
            }
        }

        Ok((columns, watermark))
    }

    /// Parse a single column definition.
    fn parse_column_definition(&mut self) -> Result<ColumnDefinition, ParseError> {
        let name = self.parse_identifier()?;
        let data_type = self.parse_data_type()?;

        let not_null = if self.peek_keyword("NOT") {
            self.expect_keyword("NOT")?;
            self.expect_keyword("NULL")?;
            true
        } else {
            false
        };

        let default = if self.peek_keyword("DEFAULT") {
            self.expect_keyword("DEFAULT")?;
            Some(self.parse_literal()?)
        } else {
            None
        };

        Ok(ColumnDefinition {
            name,
            arrow_type: data_type.to_arrow(),
            data_type,
            not_null,
            default,
        })
    }

    /// Parse WATERMARK clause.
    fn parse_watermark_clause(&mut self) -> Result<WatermarkSpec, ParseError> {
        self.expect_keyword("WATERMARK")?;
        self.expect_keyword("FOR")?;

        let column = self.parse_identifier()?;

        self.expect_keyword("AS")?;

        // Parse: column - INTERVAL 'delay'
        let expr_col = self.parse_identifier()?;
        if expr_col != column {
            return Err(ParseError::InvalidWatermark(
                "Watermark expression must reference the specified column".into()
            ));
        }

        self.expect_token(Token::Minus)?;
        self.expect_keyword("INTERVAL")?;

        let delay_str = self.parse_string_literal()?;
        let delay = parse_interval(&delay_str)?;

        Ok(WatermarkSpec {
            column,
            expression: format!("{} - INTERVAL '{}'", expr_col, delay_str),
            delay,
        })
    }

    /// Parse WITH clause options.
    fn parse_with_clause(&mut self) -> Result<SourceConfigOptions, ParseError> {
        self.expect_keyword("WITH")?;
        self.expect_token(Token::LeftParen)?;

        let mut config = SourceConfigOptions::default();

        loop {
            let key = self.parse_identifier()?;
            self.expect_token(Token::Equals)?;
            let value = self.parse_option_value()?;

            match key.to_lowercase().as_str() {
                "buffer_size" => {
                    config.buffer_size = Some(value.parse::<usize>()
                        .map_err(|_| ParseError::InvalidOption("buffer_size must be integer".into()))?);
                }
                "backpressure" => {
                    config.backpressure = Some(value.trim_matches('\'').to_string());
                }
                "wait_strategy" => {
                    config.wait_strategy = Some(value.trim_matches('\'').to_string());
                }
                "checkpoint_interval" => {
                    config.checkpoint_interval = Some(parse_interval(&value)?);
                }
                "wal_mode" => {
                    config.wal_mode = Some(value.trim_matches('\'').to_string());
                }
                // Explicitly reject 'channel' option
                "channel" => {
                    return Err(ParseError::InvalidOption(
                        "channel type cannot be specified; it is automatically derived".into()
                    ));
                }
                _ => {
                    return Err(ParseError::UnknownOption(key));
                }
            }

            if !self.try_consume(Token::Comma) {
                break;
            }
        }

        self.expect_token(Token::RightParen)?;
        Ok(config)
    }

    /// Parse CREATE SINK statement.
    pub fn parse_create_sink(&mut self) -> Result<SinkDefinition, ParseError> {
        self.expect_keyword("CREATE")?;
        self.expect_keyword("SINK")?;

        let name = self.parse_identifier()?;

        self.expect_keyword("FROM")?;
        let from_view = self.parse_identifier()?;

        let config = if self.peek_keyword("WITH") {
            self.parse_sink_with_clause()?
        } else {
            SinkConfigOptions::default()
        };

        Ok(SinkDefinition {
            name,
            from_view,
            config,
        })
    }
}

/// Parse interval string to Duration.
fn parse_interval(s: &str) -> Result<Duration, ParseError> {
    let s = s.trim().trim_matches('\'');

    // Parse formats: "100ms", "100 milliseconds", "10 seconds", "1 minute"
    let (num_str, unit) = if let Some(idx) = s.find(char::is_alphabetic) {
        (&s[..idx].trim(), &s[idx..].trim())
    } else {
        return Err(ParseError::InvalidInterval(s.to_string()));
    };

    let num: u64 = num_str.parse()
        .map_err(|_| ParseError::InvalidInterval(s.to_string()))?;

    let duration = match unit.to_lowercase().as_str() {
        "ms" | "millisecond" | "milliseconds" => Duration::from_millis(num),
        "s" | "sec" | "second" | "seconds" => Duration::from_secs(num),
        "m" | "min" | "minute" | "minutes" => Duration::from_secs(num * 60),
        "h" | "hour" | "hours" => Duration::from_secs(num * 3600),
        "d" | "day" | "days" => Duration::from_secs(num * 86400),
        _ => return Err(ParseError::InvalidInterval(s.to_string())),
    };

    Ok(duration)
}
```

### SQL Examples

```sql
-- Minimal source (all defaults)
CREATE SOURCE simple_events (
    id BIGINT,
    data VARCHAR
);

-- Full-featured source
CREATE SOURCE trades (
    symbol VARCHAR NOT NULL,
    price DOUBLE NOT NULL,
    qty BIGINT NOT NULL,
    exchange VARCHAR NOT NULL,
    ts TIMESTAMP NOT NULL,
    WATERMARK FOR ts AS ts - INTERVAL '100 milliseconds'
) WITH (
    buffer_size = 131072,
    backpressure = 'block',
    wait_strategy = 'spin_yield',
    checkpoint_interval = '10 seconds',
    wal_mode = 'async'
);

-- Source with just watermark
CREATE SOURCE sensor_data (
    sensor_id VARCHAR NOT NULL,
    temperature DOUBLE,
    humidity DOUBLE,
    event_time TIMESTAMP NOT NULL,
    WATERMARK FOR event_time AS event_time - INTERVAL '5 seconds'
);

-- Simple sink
CREATE SINK ohlc_output FROM ohlc_1min;

-- Sink with custom buffer
CREATE SINK alerts_output FROM high_value_alerts WITH (
    buffer_size = 8192
);

-- ERROR: channel cannot be specified
CREATE SOURCE bad_example (...) WITH (
    channel = 'mpsc'  -- ERROR: channel type cannot be specified
);
```

### Validation Rules

```rust
impl SourceDefinition {
    /// Validate the source definition.
    pub fn validate(&self) -> Result<(), ValidationError> {
        // 1. Name must be valid identifier
        if !is_valid_identifier(&self.name) {
            return Err(ValidationError::InvalidName(self.name.clone()));
        }

        // 2. Must have at least one column
        if self.columns.is_empty() {
            return Err(ValidationError::NoColumns);
        }

        // 3. Column names must be unique
        let names: HashSet<_> = self.columns.iter().map(|c| &c.name).collect();
        if names.len() != self.columns.len() {
            return Err(ValidationError::DuplicateColumn);
        }

        // 4. Watermark column must exist and be TIMESTAMP
        if let Some(ref wm) = self.watermark {
            let col = self.columns.iter()
                .find(|c| c.name == wm.column)
                .ok_or_else(|| ValidationError::WatermarkColumnNotFound(wm.column.clone()))?;

            if col.data_type != SqlDataType::Timestamp && col.data_type != SqlDataType::TimestampTz {
                return Err(ValidationError::WatermarkNotTimestamp(wm.column.clone()));
            }
        }

        // 5. Buffer size must be power of 2
        if let Some(size) = self.config.buffer_size {
            if !size.is_power_of_two() || size == 0 {
                return Err(ValidationError::BufferSizeNotPowerOfTwo(size));
            }
        }

        Ok(())
    }
}
```

### Error Handling

| Error | Cause | Message |
|-------|-------|---------|
| `ParseError::InvalidOption` | Unsupported option in WITH | "channel type cannot be specified; it is automatically derived" |
| `ParseError::UnknownOption` | Unknown option name | "Unknown option: {name}" |
| `ParseError::InvalidInterval` | Bad interval format | "Invalid interval: {value}" |
| `ValidationError::WatermarkNotTimestamp` | Watermark on non-timestamp column | "Watermark column must be TIMESTAMP" |
| `ValidationError::BufferSizeNotPowerOfTwo` | Bad buffer size | "buffer_size must be power of 2" |

## Test Plan

### Unit Tests

- [ ] `test_parse_create_source_minimal`
- [ ] `test_parse_create_source_with_watermark`
- [ ] `test_parse_create_source_with_options`
- [ ] `test_parse_create_source_all_data_types`
- [ ] `test_parse_create_sink`
- [ ] `test_parse_create_sink_with_options`
- [ ] `test_parse_interval_milliseconds`
- [ ] `test_parse_interval_seconds`
- [ ] `test_parse_interval_minutes`
- [ ] `test_reject_channel_option`
- [ ] `test_validation_watermark_column_exists`
- [ ] `test_validation_watermark_is_timestamp`
- [ ] `test_validation_buffer_size_power_of_two`

### Integration Tests

- [ ] Execute CREATE SOURCE and verify catalog
- [ ] Execute CREATE SINK and verify catalog
- [ ] End-to-end: CREATE SOURCE → INSERT → SELECT → CREATE SINK

### Property Tests

- [ ] `prop_valid_source_roundtrips`
- [ ] `prop_arrow_schema_matches_columns`

### Error Tests

- [ ] `test_error_channel_option_rejected`
- [ ] `test_error_invalid_buffer_size`
- [ ] `test_error_invalid_interval`
- [ ] `test_error_unknown_option`

## Rollout Plan

1. **Phase 1**: CREATE SOURCE parsing
2. **Phase 2**: CREATE SINK parsing
3. **Phase 3**: Validation and catalog integration
4. **Phase 4**: Integration with F-STREAM-004/005
5. **Phase 5**: Error handling and tests

## Open Questions

- [x] **Interval syntax**: PostgreSQL-style or custom?
  - Decision: Support both ('100ms' and '100 milliseconds')
- [ ] **ALTER statements**: Support ALTER SOURCE/SINK?
  - Leaning: No, use DROP + CREATE for simplicity

## Completion Checklist

- [ ] Code implemented in `laminar-sql/src/parser/streaming_ddl.rs`
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Catalog integration complete
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## Notes

**No Channel Option**: The parser explicitly rejects any attempt to specify `channel = ...` in the WITH clause. This enforces the zero-config philosophy where channel type is always derived.

**Integration with F006B**: This extends the existing production SQL parser infrastructure. The `CREATE SOURCE` and `CREATE SINK` statements are new streaming-specific DDL.

## References

- [F006B: Production SQL Parser](../../phase-1/F006B-production-sql-parser.md)
- [F-STREAM-004: Source](F-STREAM-004-source.md)
- [F-STREAM-005: Sink](F-STREAM-005-sink.md)
- [docs/research/laminardb-streaming-api-research.md](../../../research/laminardb-streaming-api-research.md)
