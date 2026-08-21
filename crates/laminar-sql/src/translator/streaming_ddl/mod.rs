//! SQL DDL to streaming API translation.

#[allow(clippy::disallowed_types)] // cold path: SQL translation
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use sqlparser::ast::{ColumnDef, DataType as SqlDataType};

use laminar_core::streaming::config::{DEFAULT_BUFFER_SIZE, MAX_BUFFER_SIZE, MIN_BUFFER_SIZE};

use crate::parser::ParseError;
use crate::parser::{CreateSourceStatement, WatermarkDef};

/// Watermark specification for a source.
#[derive(Debug, Clone)]
pub struct WatermarkSpec {
    /// Column name for event time.
    pub column: String,
    /// Bounded out-of-orderness duration.
    pub max_out_of_orderness: Duration,
    /// Whether this is a processing-time watermark (`PROCTIME()`).
    ///
    /// When `true`, the runtime should use `ProcessingTimeGenerator`
    /// instead of `BoundedOutOfOrdernessGenerator`.
    pub is_processing_time: bool,
}

/// Configuration options for a streaming source.
#[derive(Debug, Clone)]
pub struct SourceConfigOptions {
    /// Buffer size for the channel.
    pub buffer_size: usize,
}

impl Default for SourceConfigOptions {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_BUFFER_SIZE,
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
    /// Primary-key columns in declaration order.
    pub primary_key: Vec<String>,
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

/// Translates a CREATE SOURCE statement to a typed SourceDefinition.
///
/// # Errors
///
/// Returns `ParseError::ValidationError` if:
/// - An invalid `buffer_size` is provided
/// - Column types cannot be converted to Arrow types
pub fn translate_create_source(
    stmt: CreateSourceStatement,
) -> Result<SourceDefinition, ParseError> {
    let columns = convert_columns(&stmt.columns)?;
    translate_create_source_with_columns(stmt, columns)
}

/// Translate a CREATE SOURCE statement using an already-resolved column
/// list. Used by the DDL layer after `discover_schema` so `WATERMARK FOR`
/// validates against the discovered columns rather than the SQL text.
///
/// # Errors
///
/// Returns `ParseError` from option validation or watermark parsing.
pub fn translate_create_source_with_columns(
    stmt: CreateSourceStatement,
    mut columns: Vec<ColumnDefinition>,
) -> Result<SourceDefinition, ParseError> {
    let config = parse_source_options(&stmt.with_options)?;
    if let Some(column) = columns
        .iter()
        .find(|column| is_reserved_operation_column(&column.name))
    {
        return Err(ParseError::ValidationError(format!(
            "CREATE SOURCE column '{}' is reserved mutation metadata",
            column.name
        )));
    }
    validate_full_changelog_weight(&columns)?;
    let (primary_key, primary_key_indices) = resolve_source_primary_key(&stmt, &columns)?;
    for index in primary_key_indices {
        if stmt.columns.is_empty() && columns[index].nullable {
            return Err(ParseError::ValidationError(format!(
                "CREATE SOURCE discovered PRIMARY KEY column '{}' must be non-nullable",
                columns[index].name
            )));
        }
        columns[index].nullable = false;
    }

    let fields: Vec<Field> = columns
        .iter()
        .map(|col| Field::new(&col.name, col.data_type.clone(), col.nullable))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let watermark = if let Some(wm) = stmt.watermark {
        Some(parse_watermark(&wm, &columns)?)
    } else {
        None
    };

    Ok(SourceDefinition {
        name: stmt.name.to_string(),
        columns,
        primary_key,
        schema,
        watermark,
        config,
    })
}

fn resolve_source_primary_key(
    stmt: &CreateSourceStatement,
    columns: &[ColumnDefinition],
) -> Result<(Vec<String>, Vec<usize>), ParseError> {
    let mut names = Vec::with_capacity(stmt.primary_key.len());
    let mut indices = Vec::with_capacity(stmt.primary_key.len());

    for key in &stmt.primary_key {
        let matches: Vec<_> = if stmt.columns.is_empty() {
            columns
                .iter()
                .enumerate()
                .filter(|(_, column)| discovered_identifier_matches(key, &column.name))
                .map(|(index, _)| index)
                .collect()
        } else {
            stmt.columns
                .iter()
                .enumerate()
                .filter(|(_, column)| declared_identifier_matches(key, &column.name))
                .map(|(index, _)| index)
                .collect()
        };
        let [index] = matches.as_slice() else {
            let reason = if matches.is_empty() {
                "does not exist"
            } else {
                "is ambiguous"
            };
            return Err(ParseError::ValidationError(format!(
                "CREATE SOURCE PRIMARY KEY column '{key}' {reason}"
            )));
        };
        let column = columns.get(*index).ok_or_else(|| {
            ParseError::ValidationError(
                "CREATE SOURCE resolved column metadata does not match its declaration".into(),
            )
        })?;
        if indices.contains(index) {
            return Err(ParseError::ValidationError(format!(
                "CREATE SOURCE PRIMARY KEY repeats column '{}'",
                column.name
            )));
        }
        if stmt.columns.get(*index).is_some_and(|declared| {
            declared
                .options
                .iter()
                .any(|option| matches!(&option.option, sqlparser::ast::ColumnOption::Null))
        }) {
            return Err(ParseError::ValidationError(format!(
                "CREATE SOURCE PRIMARY KEY column '{}' cannot be declared NULL",
                column.name
            )));
        }
        names.push(column.name.clone());
        indices.push(*index);
    }

    Ok((names, indices))
}

fn discovered_identifier_matches(identifier: &sqlparser::ast::Ident, column: &str) -> bool {
    if identifier.quote_style.is_some() {
        identifier.value == column
    } else {
        identifier.value.eq_ignore_ascii_case(column)
    }
}

fn declared_identifier_matches(
    reference: &sqlparser::ast::Ident,
    declaration: &sqlparser::ast::Ident,
) -> bool {
    match (reference.quote_style, declaration.quote_style) {
        (None, None) => reference.value.eq_ignore_ascii_case(&declaration.value),
        (Some(_), Some(_)) => reference.value == declaration.value,
        (None, Some(_)) => reference.value.to_ascii_lowercase() == declaration.value,
        (Some(_), None) => reference.value == declaration.value.to_ascii_lowercase(),
    }
}

fn is_reserved_operation_column(column: &str) -> bool {
    ["_op", "__op"]
        .iter()
        .any(|reserved| column.eq_ignore_ascii_case(reserved))
}

fn validate_full_changelog_weight(columns: &[ColumnDefinition]) -> Result<(), ParseError> {
    let weights = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.name.eq_ignore_ascii_case("__weight"))
        .collect::<Vec<_>>();
    let [] = weights.as_slice() else {
        let [(index, column)] = weights.as_slice() else {
            return Err(ParseError::ValidationError(
                "CREATE SOURCE may declare at most one case-insensitive __weight column".into(),
            ));
        };
        if column.name != "__weight"
            || *index + 1 != columns.len()
            || column.data_type != DataType::Int64
            || column.nullable
        {
            return Err(ParseError::ValidationError(
                "CREATE SOURCE full-changelog metadata must be one exact trailing non-null BIGINT __weight column"
                    .into(),
            ));
        }
        return Ok(());
    };
    Ok(())
}

/// Parses source options from WITH clause.
fn parse_source_options(
    options: &HashMap<String, String>,
) -> Result<SourceConfigOptions, ParseError> {
    let mut config = SourceConfigOptions::default();

    for (key, value) in options {
        match key.to_lowercase().as_str() {
            "buffer_size" => {
                config.buffer_size = parse_buffer_size(value)?;
            }
            _ => {
                return Err(ParseError::ValidationError(format!(
                    "unsupported CREATE SOURCE runtime option '{key}'; only 'buffer_size' is supported"
                )));
            }
        }
    }

    Ok(config)
}

/// Parses buffer_size option.
fn parse_buffer_size(value: &str) -> Result<usize, ParseError> {
    let size: usize = value.parse().map_err(|_| {
        ParseError::ValidationError(format!(
            "invalid buffer_size: '{}' - must be a number",
            value
        ))
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

/// Converts SQL column definitions to Arrow types.
fn convert_columns(columns: &[ColumnDef]) -> Result<Vec<ColumnDefinition>, ParseError> {
    columns.iter().map(convert_column).collect()
}

/// Converts a single SQL column definition to Arrow type.
fn convert_column(col: &ColumnDef) -> Result<ColumnDefinition, ParseError> {
    let data_type = sql_type_to_arrow(&col.data_type)?;

    // Check for NOT NULL constraint
    let nullable = !col
        .options
        .iter()
        .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));

    Ok(ColumnDefinition {
        name: col.name.value.clone(),
        data_type,
        nullable,
    })
}

/// Converts SQL data type to Arrow data type.
///
/// # Errors
///
/// Returns `ParseError::ValidationError` for unsupported SQL data types.
pub fn sql_type_to_arrow(sql_type: &SqlDataType) -> Result<DataType, ParseError> {
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

        // String types (including JSON/UUID stored as strings)
        SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::Varchar(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::Text
        | SqlDataType::String(_)
        | SqlDataType::JSON
        | SqlDataType::JSONB
        | SqlDataType::Uuid => Ok(DataType::Utf8),

        // Binary types
        SqlDataType::Binary(_)
        | SqlDataType::Varbinary(_)
        | SqlDataType::Blob(_)
        | SqlDataType::Bytea => Ok(DataType::Binary),

        // Boolean type
        SqlDataType::Boolean | SqlDataType::Bool => Ok(DataType::Boolean),

        // Date/time types
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::Time(_, _) => Ok(DataType::Time64(TimeUnit::Microsecond)),
        SqlDataType::Timestamp(_, _) => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),

        // Interval type
        SqlDataType::Interval { .. } => Ok(DataType::Interval(
            arrow::datatypes::IntervalUnit::MonthDayNano,
        )),

        // Array type: ARRAY<T>, T[], Array(T)
        SqlDataType::Array(elem_def) => {
            let item_type = match elem_def {
                sqlparser::ast::ArrayElemTypeDef::AngleBracket(t)
                | sqlparser::ast::ArrayElemTypeDef::SquareBracket(t, _)
                | sqlparser::ast::ArrayElemTypeDef::Parenthesis(t) => sql_type_to_arrow(t)?,
                sqlparser::ast::ArrayElemTypeDef::None => {
                    return Err(ParseError::ValidationError(
                        "ARRAY type requires element type, e.g. ARRAY<INT>".into(),
                    ));
                }
            };
            Ok(DataType::List(Arc::new(Field::new(
                "item", item_type, true,
            ))))
        }

        // Complex types (MAP, STRUCT, nested records) — use auto-discovery instead.
        _ => Err(ParseError::ValidationError(format!(
            "unsupported data type in hand-declared column: {sql_type:?} \
             — use auto-discovery with an Avro source for complex types"
        ))),
    }
}

/// Checks if an expression is a `PROCTIME()` function call.
fn is_proctime_call(expr: &sqlparser::ast::Expr) -> bool {
    if let sqlparser::ast::Expr::Function(func) = expr {
        if let Some(name) = func.name.0.last() {
            return name.to_string().eq_ignore_ascii_case("proctime");
        }
    }
    false
}

/// Parses watermark definition.
fn parse_watermark(
    wm: &WatermarkDef,
    columns: &[ColumnDefinition],
) -> Result<WatermarkSpec, ParseError> {
    let column_name = wm.column.value.clone();

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

    if !matches!(col.data_type, DataType::Timestamp(_, _)) {
        return Err(ParseError::ValidationError(format!(
            "watermark column '{}' must be a TIMESTAMP, found {:?}",
            column_name, col.data_type
        )));
    }

    // Check for PROCTIME() watermark expression
    if let Some(expr) = &wm.expression {
        if is_proctime_call(expr) {
            return Ok(WatermarkSpec {
                column: column_name,
                max_out_of_orderness: Duration::ZERO,
                is_processing_time: true,
            });
        }
    }

    // Parse the watermark expression to extract out-of-orderness.
    // When expression is None (WATERMARK FOR col without AS), use zero delay.
    let max_out_of_orderness = match &wm.expression {
        Some(expr) => parse_watermark_expression(expr),
        None => Duration::ZERO,
    };

    Ok(WatermarkSpec {
        column: column_name,
        max_out_of_orderness,
        is_processing_time: false,
    })
}

/// Parses watermark expression to extract the bounded out-of-orderness.
fn parse_watermark_expression(expr: &sqlparser::ast::Expr) -> Duration {
    use sqlparser::ast::Expr;

    match expr {
        Expr::BinaryOp { op, right, .. } => match op {
            sqlparser::ast::BinaryOperator::Minus => parse_interval_expr(right),
            _ => Duration::ZERO,
        },
        // If just the column name, assume zero lateness
        Expr::Identifier(_) => Duration::ZERO,
        // Default to 1 second for complex expressions
        _ => Duration::from_secs(1),
    }
}

/// Parses an interval expression to a Duration.
fn parse_interval_expr(expr: &sqlparser::ast::Expr) -> Duration {
    use sqlparser::ast::Expr;

    let Expr::Interval(interval) = expr else {
        return Duration::from_secs(1);
    };

    // Extract value and unit from interval
    let value_str = match interval.value.as_ref() {
        Expr::Value(v) => {
            // v is ValueWithSpan, access the inner value
            match &v.value {
                sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                sqlparser::ast::Value::Number(n, _) => n.clone(),
                _ => return Duration::from_secs(1),
            }
        }
        _ => return Duration::from_secs(1),
    };

    let value: u64 = value_str.parse().unwrap_or(1);

    // Determine unit
    let unit = interval
        .leading_field
        .as_ref()
        .map_or("second", |u| match u {
            sqlparser::ast::DateTimeField::Microsecond => "microsecond",
            sqlparser::ast::DateTimeField::Millisecond => "millisecond",
            sqlparser::ast::DateTimeField::Minute => "minute",
            sqlparser::ast::DateTimeField::Hour => "hour",
            sqlparser::ast::DateTimeField::Day => "day",
            _ => "second",
        });

    match unit {
        "microsecond" | "microseconds" => Duration::from_micros(value),
        "millisecond" | "milliseconds" => Duration::from_millis(value),
        "minute" | "minutes" => Duration::from_secs(value * 60),
        "hour" | "hours" => Duration::from_secs(value * 3600),
        "day" | "days" => Duration::from_secs(value * 86400),
        _ => Duration::from_secs(value),
    }
}

#[cfg(test)]
mod tests;
