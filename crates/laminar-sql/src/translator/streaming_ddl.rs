//! SQL DDL to Streaming API translation.
//!
//! This module translates parsed SQL CREATE SOURCE/SINK statements into
//! typed streaming definitions that can be used to configure the runtime.
//!
//! ## Supported Syntax
//!
//! ```sql
//! -- In-memory streaming source
//! CREATE SOURCE trades (
//!     symbol VARCHAR NOT NULL,
//!     price DOUBLE NOT NULL,
//!     quantity BIGINT NOT NULL,
//!     ts TIMESTAMP NOT NULL,
//!     WATERMARK FOR ts AS ts - INTERVAL '100' MILLISECONDS
//! ) WITH (
//!     buffer_size = 131072,
//!     backpressure = 'block'
//! );
//!
//! -- In-memory streaming sink
//! CREATE SINK trade_aggregates AS
//!     SELECT * FROM trade_stats
//! WITH (
//!     buffer_size = 65536
//! );
//! ```
//!
//! ## Validation
//!
//! - Rejects `channel = ...` option (channel type is auto-derived)
//! - Validates buffer_size is within bounds
//! - Validates backpressure strategy names
//! - Validates wait_strategy names

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use sqlparser::ast::{ColumnDef, DataType as SqlDataType};

use crate::parser::{CreateSinkStatement, CreateSourceStatement, SinkFrom, WatermarkDef};
use crate::parser::ParseError;

/// Minimum buffer size for streaming channels.
pub const MIN_BUFFER_SIZE: usize = 4;

/// Maximum buffer size for streaming channels.
pub const MAX_BUFFER_SIZE: usize = 1 << 20; // 1M entries

/// Default buffer size for streaming channels.
pub const DEFAULT_BUFFER_SIZE: usize = 2048;

/// Backpressure strategy for streaming channels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressureStrategy {
    /// Block until space is available.
    #[default]
    Block,
    /// Drop oldest item to make room.
    DropOldest,
    /// Reject and return error immediately.
    Reject,
}

impl std::str::FromStr for BackpressureStrategy {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "block" | "blocking" => Ok(Self::Block),
            "drop" | "drop_oldest" | "dropoldest" => Ok(Self::DropOldest),
            "reject" | "error" => Ok(Self::Reject),
            _ => Err(ParseError::ValidationError(format!(
                "invalid backpressure strategy: '{}'. Valid values: block, drop_oldest, reject",
                s
            ))),
        }
    }
}

/// Wait strategy for streaming consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WaitStrategy {
    /// Spin-loop without yielding (lowest latency, highest CPU).
    Spin,
    /// Spin with occasional yields (balanced).
    #[default]
    SpinYield,
    /// Park the thread (lowest CPU, higher latency).
    Park,
}

impl std::str::FromStr for WaitStrategy {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "spin" => Ok(Self::Spin),
            "spin_yield" | "spinyield" | "yield" => Ok(Self::SpinYield),
            "park" | "parking" => Ok(Self::Park),
            _ => Err(ParseError::ValidationError(format!(
                "invalid wait strategy: '{}'. Valid values: spin, spin_yield, park",
                s
            ))),
        }
    }
}

/// Watermark specification for a source.
#[derive(Debug, Clone)]
pub struct WatermarkSpec {
    /// Column name for event time.
    pub column: String,
    /// Bounded out-of-orderness duration.
    pub max_out_of_orderness: Duration,
}

/// Configuration options for a streaming source.
#[derive(Debug, Clone)]
pub struct SourceConfigOptions {
    /// Buffer size for the channel.
    pub buffer_size: usize,
    /// Backpressure strategy.
    pub backpressure: BackpressureStrategy,
    /// Wait strategy for consumers.
    pub wait_strategy: WaitStrategy,
    /// Whether to track statistics.
    pub track_stats: bool,
}

impl Default for SourceConfigOptions {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_BUFFER_SIZE,
            backpressure: BackpressureStrategy::Block,
            wait_strategy: WaitStrategy::SpinYield,
            track_stats: false,
        }
    }
}

/// Column definition for a streaming source.
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    /// Column name.
    pub name: String,
    /// Arrow data type.
    pub data_type: DataType,
    /// Whether the column is nullable.
    pub nullable: bool,
}

/// A validated streaming source definition.
///
/// This is the output of translating a `CreateSourceStatement` to a typed
/// configuration that can be used to create runtime sources.
#[derive(Debug, Clone)]
pub struct SourceDefinition {
    /// Source name.
    pub name: String,
    /// Column definitions.
    pub columns: Vec<ColumnDefinition>,
    /// Arrow schema.
    pub schema: SchemaRef,
    /// Watermark specification, if defined.
    pub watermark: Option<WatermarkSpec>,
    /// Configuration options.
    pub config: SourceConfigOptions,
}

impl TryFrom<CreateSourceStatement> for SourceDefinition {
    type Error = ParseError;

    fn try_from(stmt: CreateSourceStatement) -> Result<Self, Self::Error> {
        translate_create_source(stmt)
    }
}

/// A validated streaming sink definition.
#[derive(Debug, Clone)]
pub struct SinkDefinition {
    /// Sink name.
    pub name: String,
    /// Input source or query.
    pub input: String,
    /// Configuration options.
    pub config: SourceConfigOptions,
}

impl TryFrom<CreateSinkStatement> for SinkDefinition {
    type Error = ParseError;

    fn try_from(stmt: CreateSinkStatement) -> Result<Self, Self::Error> {
        translate_create_sink(stmt)
    }
}

/// Translates a CREATE SOURCE statement to a typed SourceDefinition.
///
/// # Errors
///
/// Returns `ParseError::ValidationError` if:
/// - The `channel` option is specified (not user-configurable)
/// - An invalid option value is provided
/// - Column types cannot be converted to Arrow types
pub fn translate_create_source(stmt: CreateSourceStatement) -> Result<SourceDefinition, ParseError> {
    // Validate options first - reject 'channel' option
    validate_source_options(&stmt.with_options)?;

    // Parse configuration options
    let config = parse_source_options(&stmt.with_options)?;

    // Convert columns to Arrow types
    let columns = convert_columns(&stmt.columns)?;

    // Build Arrow schema
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| Field::new(&col.name, col.data_type.clone(), col.nullable))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // Parse watermark if present
    let watermark = if let Some(wm) = stmt.watermark {
        Some(parse_watermark(&wm, &columns)?)
    } else {
        None
    };

    Ok(SourceDefinition {
        name: stmt.name.to_string(),
        columns,
        schema,
        watermark,
        config,
    })
}

/// Translates a CREATE SINK statement to a typed SinkDefinition.
///
/// # Errors
///
/// Returns `ParseError::ValidationError` if:
/// - The `channel` option is specified (not user-configurable)
/// - An invalid option value is provided
pub fn translate_create_sink(stmt: CreateSinkStatement) -> Result<SinkDefinition, ParseError> {
    // Validate options first
    validate_source_options(&stmt.with_options)?;

    // Parse configuration options
    let config = parse_source_options(&stmt.with_options)?;

    // Get input name
    let input = match stmt.from {
        SinkFrom::Table(name) => name.to_string(),
        SinkFrom::Query(_) => {
            // For now, we don't support inline queries - need to create a view first
            return Err(ParseError::ValidationError(
                "inline queries not yet supported in CREATE SINK - use a view".to_string(),
            ));
        }
    };

    Ok(SinkDefinition {
        name: stmt.name.to_string(),
        input,
        config,
    })
}

/// Validates that source options don't include disallowed keys.
fn validate_source_options(options: &HashMap<String, String>) -> Result<(), ParseError> {
    // Reject 'channel' option - channel type is auto-derived
    if options.contains_key("channel") {
        return Err(ParseError::ValidationError(
            "the 'channel' option is not user-configurable - channel type is automatically derived from usage patterns".to_string(),
        ));
    }

    // Reject 'type' option for same reason
    if options.contains_key("type") {
        return Err(ParseError::ValidationError(
            "the 'type' option is not user-configurable for in-memory streaming sources".to_string(),
        ));
    }

    Ok(())
}

/// Parses source options from WITH clause.
fn parse_source_options(options: &HashMap<String, String>) -> Result<SourceConfigOptions, ParseError> {
    let mut config = SourceConfigOptions::default();

    for (key, value) in options {
        match key.to_lowercase().as_str() {
            "buffer_size" | "buffersize" => {
                config.buffer_size = parse_buffer_size(value)?;
            }
            "backpressure" => {
                config.backpressure = BackpressureStrategy::from_str(value)?;
            }
            "wait_strategy" | "waitstrategy" => {
                config.wait_strategy = WaitStrategy::from_str(value)?;
            }
            "track_stats" | "trackstats" | "stats" => {
                config.track_stats = parse_bool(value)?;
            }
            // Ignore connector-specific options for external sources
            // These are handled by connector implementations
            "connector" | "topic" | "bootstrap.servers" | "format" | "group.id" => {}
            _ => {
                // Unknown option - log warning but don't fail
                // This allows forward compatibility with new options
            }
        }
    }

    Ok(config)
}

/// Parses buffer_size option.
fn parse_buffer_size(value: &str) -> Result<usize, ParseError> {
    let size: usize = value.parse().map_err(|_| {
        ParseError::ValidationError(format!("invalid buffer_size: '{}' - must be a number", value))
    })?;

    if size < MIN_BUFFER_SIZE {
        return Err(ParseError::ValidationError(format!(
            "buffer_size {} is too small - minimum is {}",
            size, MIN_BUFFER_SIZE
        )));
    }

    if size > MAX_BUFFER_SIZE {
        return Err(ParseError::ValidationError(format!(
            "buffer_size {} is too large - maximum is {}",
            size, MAX_BUFFER_SIZE
        )));
    }

    Ok(size)
}

/// Parses a boolean option.
fn parse_bool(value: &str) -> Result<bool, ParseError> {
    match value.to_lowercase().as_str() {
        "true" | "yes" | "on" | "1" => Ok(true),
        "false" | "no" | "off" | "0" => Ok(false),
        _ => Err(ParseError::ValidationError(format!(
            "invalid boolean value: '{}' - expected true/false",
            value
        ))),
    }
}

/// Converts SQL column definitions to Arrow types.
fn convert_columns(columns: &[ColumnDef]) -> Result<Vec<ColumnDefinition>, ParseError> {
    columns.iter().map(convert_column).collect()
}

/// Converts a single SQL column definition to Arrow type.
fn convert_column(col: &ColumnDef) -> Result<ColumnDefinition, ParseError> {
    let data_type = sql_type_to_arrow(&col.data_type)?;

    // Check for NOT NULL constraint
    let nullable = !col.options.iter().any(|opt| {
        matches!(
            opt.option,
            sqlparser::ast::ColumnOption::NotNull
        )
    });

    Ok(ColumnDefinition {
        name: col.name.to_string(),
        data_type,
        nullable,
    })
}

/// Converts SQL data type to Arrow data type.
fn sql_type_to_arrow(sql_type: &SqlDataType) -> Result<DataType, ParseError> {
    match sql_type {
        // Integer types
        SqlDataType::TinyInt(_) => Ok(DataType::Int8),
        SqlDataType::SmallInt(_) => Ok(DataType::Int16),
        SqlDataType::Int(_) | SqlDataType::Integer(_) => Ok(DataType::Int32),
        SqlDataType::BigInt(_) => Ok(DataType::Int64),

        // Unsigned integer types - wrapped in Unsigned variant
        // Note: sqlparser wraps unsigned types differently in different versions

        // Floating point types
        SqlDataType::Float(_) | SqlDataType::Real => Ok(DataType::Float32),
        SqlDataType::Double(_) | SqlDataType::DoublePrecision => Ok(DataType::Float64),

        // Decimal types
        SqlDataType::Decimal(info) | SqlDataType::Numeric(info) => {
            #[allow(clippy::cast_possible_truncation)] // Precision/scale are typically small values
            let (precision, scale) = match info {
                sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as i8),
                sqlparser::ast::ExactNumberInfo::Precision(p) => (*p as u8, 0),
                sqlparser::ast::ExactNumberInfo::None => (38, 9), // Default precision/scale
            };
            Ok(DataType::Decimal128(precision, scale))
        }

        // String types
        SqlDataType::Char(_) | SqlDataType::Character(_) => Ok(DataType::Utf8),
        SqlDataType::Varchar(_) | SqlDataType::CharacterVarying(_) => Ok(DataType::Utf8),
        SqlDataType::Text | SqlDataType::String(_) => Ok(DataType::Utf8),

        // Binary types
        SqlDataType::Binary(_) | SqlDataType::Varbinary(_) | SqlDataType::Blob(_) => {
            Ok(DataType::Binary)
        }
        SqlDataType::Bytea => Ok(DataType::Binary),

        // Boolean type
        SqlDataType::Boolean | SqlDataType::Bool => Ok(DataType::Boolean),

        // Date/time types
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::Time(_, _) => Ok(DataType::Time64(TimeUnit::Microsecond)),
        SqlDataType::Timestamp(_, _) => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),

        // Interval type
        SqlDataType::Interval { .. } => Ok(DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)),

        // JSON type
        SqlDataType::JSON | SqlDataType::JSONB => Ok(DataType::Utf8), // Store as string for now

        // UUID type
        SqlDataType::Uuid => Ok(DataType::Utf8), // Store as string

        // Unsupported types
        _ => Err(ParseError::ValidationError(format!(
            "unsupported data type: {:?}",
            sql_type
        ))),
    }
}

/// Parses watermark definition.
fn parse_watermark(wm: &WatermarkDef, columns: &[ColumnDefinition]) -> Result<WatermarkSpec, ParseError> {
    let column_name = wm.column.to_string();

    // Verify column exists and is a timestamp type
    let col = columns
        .iter()
        .find(|c| c.name == column_name)
        .ok_or_else(|| {
            ParseError::ValidationError(format!(
                "watermark column '{}' not found in column list",
                column_name
            ))
        })?;

    // Check column is a timestamp type
    if !matches!(
        col.data_type,
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
    ) {
        return Err(ParseError::ValidationError(format!(
            "watermark column '{}' must be a timestamp type, found {:?}",
            column_name, col.data_type
        )));
    }

    // Parse the watermark expression to extract out-of-orderness
    // Expression should be: column - INTERVAL 'N' UNIT
    let max_out_of_orderness = parse_watermark_expression(&wm.expression)?;

    Ok(WatermarkSpec {
        column: column_name,
        max_out_of_orderness,
    })
}

/// Parses watermark expression to extract the bounded out-of-orderness.
fn parse_watermark_expression(expr: &sqlparser::ast::Expr) -> Result<Duration, ParseError> {
    use sqlparser::ast::Expr;

    match expr {
        Expr::BinaryOp { op, right, .. } => {
            match op {
                sqlparser::ast::BinaryOperator::Minus => {
                    // Extract interval from right side
                    parse_interval_expr(right)
                }
                _ => {
                    // For other operators, default to zero lateness
                    Ok(Duration::ZERO)
                }
            }
        }
        // If just the column name, assume zero lateness
        Expr::Identifier(_) => Ok(Duration::ZERO),
        _ => {
            // Default to 1 second for complex expressions
            Ok(Duration::from_secs(1))
        }
    }
}

/// Parses an interval expression to a Duration.
fn parse_interval_expr(expr: &sqlparser::ast::Expr) -> Result<Duration, ParseError> {
    use sqlparser::ast::Expr;

    match expr {
        Expr::Interval(interval) => {
            // Extract value and unit from interval
            let value_str = match interval.value.as_ref() {
                Expr::Value(v) => {
                    // v is ValueWithSpan, access the inner value
                    match &v.value {
                        sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                        sqlparser::ast::Value::Number(n, _) => n.clone(),
                        _ => return Ok(Duration::from_secs(1)),
                    }
                }
                _ => return Ok(Duration::from_secs(1)),
            };

            let value: u64 = value_str.parse().unwrap_or(1);

            // Determine unit
            let unit = interval
                .leading_field
                .as_ref()
                .map_or("second", |u| match u {
                    sqlparser::ast::DateTimeField::Microsecond => "microsecond",
                    sqlparser::ast::DateTimeField::Millisecond => "millisecond",
                    sqlparser::ast::DateTimeField::Second => "second",
                    sqlparser::ast::DateTimeField::Minute => "minute",
                    sqlparser::ast::DateTimeField::Hour => "hour",
                    sqlparser::ast::DateTimeField::Day => "day",
                    _ => "second",
                });

            match unit {
                "microsecond" | "microseconds" => Ok(Duration::from_micros(value)),
                "millisecond" | "milliseconds" => Ok(Duration::from_millis(value)),
                "second" | "seconds" => Ok(Duration::from_secs(value)),
                "minute" | "minutes" => Ok(Duration::from_secs(value * 60)),
                "hour" | "hours" => Ok(Duration::from_secs(value * 3600)),
                "day" | "days" => Ok(Duration::from_secs(value * 86400)),
                _ => Ok(Duration::from_secs(value)),
            }
        }
        _ => Ok(Duration::from_secs(1)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{parse_streaming_sql, StreamingStatement};

    fn parse_and_translate(sql: &str) -> Result<SourceDefinition, ParseError> {
        let statements = parse_streaming_sql(sql)?;
        let stmt = statements.into_iter().next().ok_or_else(|| {
            ParseError::StreamingError("No statement found".to_string())
        })?;
        match stmt {
            StreamingStatement::CreateSource(source) => translate_create_source(*source),
            _ => Err(ParseError::StreamingError("Expected CREATE SOURCE".to_string())),
        }
    }

    #[test]
    fn test_basic_source() {
        let def = parse_and_translate(
            "CREATE SOURCE events (id BIGINT NOT NULL, name VARCHAR)",
        )
        .unwrap();

        assert_eq!(def.name, "events");
        assert_eq!(def.columns.len(), 2);
        assert_eq!(def.columns[0].name, "id");
        assert_eq!(def.columns[0].data_type, DataType::Int64);
        assert!(!def.columns[0].nullable);
        assert_eq!(def.columns[1].name, "name");
        assert!(def.columns[1].nullable);
    }

    #[test]
    fn test_source_with_options() {
        let def = parse_and_translate(
            "CREATE SOURCE events (id BIGINT) WITH (
                'buffer_size' = '4096',
                'backpressure' = 'reject'
            )",
        )
        .unwrap();

        assert_eq!(def.config.buffer_size, 4096);
        assert_eq!(def.config.backpressure, BackpressureStrategy::Reject);
    }

    #[test]
    fn test_source_with_watermark() {
        let def = parse_and_translate(
            "CREATE SOURCE events (
                id BIGINT,
                ts TIMESTAMP,
                WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
            )",
        )
        .unwrap();

        assert!(def.watermark.is_some());
        let wm = def.watermark.unwrap();
        assert_eq!(wm.column, "ts");
        assert_eq!(wm.max_out_of_orderness, Duration::from_secs(5));
    }

    #[test]
    fn test_reject_channel_option() {
        let result = parse_and_translate(
            "CREATE SOURCE events (id BIGINT) WITH ('channel' = 'mpsc')",
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("channel"));
    }

    #[test]
    fn test_reject_type_option() {
        let result = parse_and_translate(
            "CREATE SOURCE events (id BIGINT) WITH ('type' = 'spsc')",
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_buffer_size_bounds() {
        // Too small
        let result = parse_and_translate(
            "CREATE SOURCE events (id BIGINT) WITH ('buffer_size' = '1')",
        );
        assert!(result.is_err());

        // Too large
        let result = parse_and_translate(
            "CREATE SOURCE events (id BIGINT) WITH ('buffer_size' = '999999999')",
        );
        assert!(result.is_err());

        // Valid
        let result = parse_and_translate(
            "CREATE SOURCE events (id BIGINT) WITH ('buffer_size' = '1024')",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_backpressure_strategies() {
        assert_eq!(
            BackpressureStrategy::from_str("block").unwrap(),
            BackpressureStrategy::Block
        );
        assert_eq!(
            BackpressureStrategy::from_str("drop_oldest").unwrap(),
            BackpressureStrategy::DropOldest
        );
        assert_eq!(
            BackpressureStrategy::from_str("reject").unwrap(),
            BackpressureStrategy::Reject
        );
        assert!(BackpressureStrategy::from_str("invalid").is_err());
    }

    #[test]
    fn test_wait_strategies() {
        assert_eq!(WaitStrategy::from_str("spin").unwrap(), WaitStrategy::Spin);
        assert_eq!(
            WaitStrategy::from_str("spin_yield").unwrap(),
            WaitStrategy::SpinYield
        );
        assert_eq!(WaitStrategy::from_str("park").unwrap(), WaitStrategy::Park);
        assert!(WaitStrategy::from_str("invalid").is_err());
    }

    #[test]
    fn test_sql_type_conversions() {
        let def = parse_and_translate(
            "CREATE SOURCE types (
                a TINYINT,
                b SMALLINT,
                c INT,
                d BIGINT,
                e FLOAT,
                f DOUBLE,
                g DECIMAL(10,2),
                h VARCHAR(255),
                i TEXT,
                j BOOLEAN,
                k TIMESTAMP,
                l DATE
            )",
        )
        .unwrap();

        assert_eq!(def.columns.len(), 12);
        assert_eq!(def.columns[0].data_type, DataType::Int8);
        assert_eq!(def.columns[1].data_type, DataType::Int16);
        assert_eq!(def.columns[2].data_type, DataType::Int32);
        assert_eq!(def.columns[3].data_type, DataType::Int64);
        assert_eq!(def.columns[4].data_type, DataType::Float32);
        assert_eq!(def.columns[5].data_type, DataType::Float64);
        assert_eq!(def.columns[6].data_type, DataType::Decimal128(10, 2));
        assert_eq!(def.columns[7].data_type, DataType::Utf8);
        assert_eq!(def.columns[8].data_type, DataType::Utf8);
        assert_eq!(def.columns[9].data_type, DataType::Boolean);
        assert!(matches!(def.columns[10].data_type, DataType::Timestamp(_, _)));
        assert_eq!(def.columns[11].data_type, DataType::Date32);
    }

    #[test]
    fn test_schema_generation() {
        let def = parse_and_translate(
            "CREATE SOURCE events (id BIGINT NOT NULL, name VARCHAR NOT NULL, value DOUBLE)",
        )
        .unwrap();

        let schema = def.schema;
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "name");
        assert!(!schema.field(1).is_nullable());
        assert_eq!(schema.field(2).name(), "value");
        assert!(schema.field(2).is_nullable());
    }

    #[test]
    fn test_watermark_column_not_found() {
        let result = parse_and_translate(
            "CREATE SOURCE events (
                id BIGINT,
                WATERMARK FOR nonexistent AS nonexistent - INTERVAL '1' SECOND
            )",
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_watermark_wrong_type() {
        let result = parse_and_translate(
            "CREATE SOURCE events (
                id BIGINT,
                WATERMARK FOR id AS id - INTERVAL '1' SECOND
            )",
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("timestamp type"));
    }

    #[test]
    fn test_watermark_milliseconds() {
        let def = parse_and_translate(
            "CREATE SOURCE events (
                ts TIMESTAMP,
                WATERMARK FOR ts AS ts - INTERVAL '100' MILLISECOND
            )",
        )
        .unwrap();

        let wm = def.watermark.unwrap();
        assert_eq!(wm.max_out_of_orderness, Duration::from_millis(100));
    }

    #[test]
    fn test_watermark_minutes() {
        let def = parse_and_translate(
            "CREATE SOURCE events (
                ts TIMESTAMP,
                WATERMARK FOR ts AS ts - INTERVAL '5' MINUTE
            )",
        )
        .unwrap();

        let wm = def.watermark.unwrap();
        assert_eq!(wm.max_out_of_orderness, Duration::from_secs(300));
    }

    #[test]
    fn test_track_stats_option() {
        let def = parse_and_translate(
            "CREATE SOURCE events (id BIGINT) WITH ('track_stats' = 'true')",
        )
        .unwrap();

        assert!(def.config.track_stats);
    }

    #[test]
    fn test_wait_strategy_option() {
        let def = parse_and_translate(
            "CREATE SOURCE events (id BIGINT) WITH ('wait_strategy' = 'park')",
        )
        .unwrap();

        assert_eq!(def.config.wait_strategy, WaitStrategy::Park);
    }

    #[test]
    fn test_default_config() {
        let def = parse_and_translate("CREATE SOURCE events (id BIGINT)").unwrap();

        assert_eq!(def.config.buffer_size, DEFAULT_BUFFER_SIZE);
        assert_eq!(def.config.backpressure, BackpressureStrategy::Block);
        assert_eq!(def.config.wait_strategy, WaitStrategy::SpinYield);
        assert!(!def.config.track_stats);
    }

    #[test]
    fn test_external_connector_options_ignored() {
        // External connector options should be accepted but not affect config
        let def = parse_and_translate(
            "CREATE SOURCE events (id BIGINT) WITH (
                'connector' = 'kafka',
                'topic' = 'events',
                'bootstrap.servers' = 'localhost:9092',
                'buffer_size' = '8192'
            )",
        )
        .unwrap();

        // Only buffer_size should affect config
        assert_eq!(def.config.buffer_size, 8192);
    }
}
