//! CREATE SOURCE parser using sqlparser primitives.
//!
//! Replaces manual string parsing with sqlparser's `Parser` API for
//! object names, column definitions, data types, and expressions.
//!
//! Supported syntax:
//! ```sql
//! CREATE [OR REPLACE] SOURCE [IF NOT EXISTS] name (
//!     column1 TYPE [NOT NULL] [DEFAULT expr] [PRIMARY KEY],
//!     column2 TYPE,
//!     PRIMARY KEY (column1, column2),
//!     WATERMARK FOR time_col AS time_col - INTERVAL 'n' UNIT
//! ) [FROM connector (...)] [FORMAT format [WITH (...)]]
//! [WITH ('buffer_size' = value)];
//! ```

#[allow(clippy::disallowed_types)] // cold path: SQL parsing
use std::collections::HashMap;

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::statements::{CreateSourceStatement, FormatSpec, WatermarkDef};
use super::tokenizer::{
    expect_custom_keyword, expect_statement_end, insert_unique_option, parse_with_options,
    try_parse_custom_keyword,
};
use super::ParseError;

/// Parse a CREATE SOURCE statement from a sqlparser `Parser`.
///
/// The parser should be positioned at the start of the SQL (at the CREATE token).
/// Uses sqlparser's built-in methods for parsing identifiers, data types,
/// column definitions, and expressions.
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
pub fn parse_create_source(parser: &mut Parser) -> Result<CreateSourceStatement, ParseError> {
    // CREATE
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(ParseError::SqlParseError)?;

    // OR REPLACE (optional)
    let or_replace = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);

    // SOURCE
    expect_custom_keyword(parser, "SOURCE")?;

    // IF NOT EXISTS (optional)
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

    // Object name (handles quoted identifiers, schema-qualified names)
    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;

    // Check for FROM <connector> (...) syntax (connector-first ordering)
    let (mut connector_type, mut connector_options) = parse_from_connector(parser)?;

    // Check for FORMAT <type> syntax (after connector, before columns)
    let mut format = parse_format_clause(parser)?;

    // SCHEMA (...) or (...) for column definitions with optional WATERMARK
    // If we have a connector, columns come after FORMAT/SCHEMA; otherwise right after name
    let has_schema_keyword = try_parse_custom_keyword(parser, "SCHEMA");
    let body = if has_schema_keyword || connector_type.is_none() {
        parse_source_body(parser)?
    } else {
        // If connector_type is set but no SCHEMA keyword and no paren, allow empty columns
        if let Token::LParen = parser.peek_token().token {
            parse_source_body(parser)?
        } else {
            SourceBody {
                columns: vec![],
                primary_key: vec![],
                watermark: None,
            }
        }
    };

    // Check for FROM <connector> (...) syntax AFTER columns (columns-first ordering).
    // Supports: CREATE SOURCE name (columns) FROM KAFKA (options)
    if connector_type.is_none() {
        let (ct, co) = parse_from_connector(parser)?;
        if ct.is_some() {
            connector_type = ct;
            connector_options = co;
        }
        if format.is_none() {
            format = parse_format_clause(parser)?;
        }
    }

    if format.is_some() && connector_type.is_none() {
        return Err(ParseError::StreamingError(
            "CREATE SOURCE FORMAT requires an explicit FROM connector".into(),
        ));
    }
    if connector_options
        .keys()
        .any(|key| key.eq_ignore_ascii_case("format"))
        || format.as_ref().is_some_and(|format| {
            format
                .options
                .keys()
                .any(|key| key.eq_ignore_ascii_case("format"))
        })
    {
        return Err(ParseError::StreamingError(
            "CREATE SOURCE option 'format' is unsupported; declare the format with the FORMAT clause"
                .into(),
        ));
    }

    // WITH options (optional) — source runtime configuration only. Connector
    // and format configuration has exactly one syntax: FROM / FORMAT.
    let with_options = parse_with_options(parser)?;
    if let Some(option) = with_options
        .keys()
        .filter(|option| !option.eq_ignore_ascii_case("buffer_size"))
        .min_by(|left, right| {
            left.to_ascii_lowercase()
                .cmp(&right.to_ascii_lowercase())
                .then_with(|| left.cmp(right))
        })
    {
        return Err(ParseError::StreamingError(format!(
            "CREATE SOURCE trailing WITH supports only 'buffer_size'; put connector options in FROM (...) and format options in FORMAT ... WITH (...); unsupported option '{option}'"
        )));
    }
    expect_statement_end(parser)?;

    Ok(CreateSourceStatement {
        name,
        columns: body.columns,
        primary_key: body.primary_key,
        watermark: body.watermark,
        with_options,
        or_replace,
        if_not_exists,
        connector_type,
        connector_options,
        format,
    })
}

/// Result of parsing the source body.
struct SourceBody {
    columns: Vec<sqlparser::ast::ColumnDef>,
    primary_key: Vec<sqlparser::ast::Ident>,
    watermark: Option<WatermarkDef>,
}

/// Parse the column list and optional WATERMARK clause inside parentheses.
///
/// Uses `parser.parse_column_def()` for each column, which supports all
/// SQL data types (including parameterized types like `DECIMAL(10,2)`,
/// `VARCHAR(255)`, `ARRAY<INT>`, etc.) and column constraints (`NOT NULL`,
/// `DEFAULT`, `PRIMARY KEY`, etc.).
fn parse_source_body(parser: &mut Parser) -> Result<SourceBody, ParseError> {
    // If no opening paren, no columns defined
    if !parser.consume_token(&Token::LParen) {
        return Ok(SourceBody {
            columns: vec![],
            primary_key: vec![],
            watermark: None,
        });
    }

    let mut columns = Vec::new();
    let mut primary_key = None;
    let mut watermark = None;

    loop {
        // Check for closing paren (empty list)
        if parser.consume_token(&Token::RParen) {
            break;
        }

        if parser.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
            parser
                .expect_token(&Token::LParen)
                .map_err(ParseError::SqlParseError)?;
            let mut key_columns = Vec::new();
            loop {
                key_columns.push(
                    parser
                        .parse_identifier()
                        .map_err(ParseError::SqlParseError)?,
                );
                if !parser.consume_token(&Token::Comma) {
                    break;
                }
            }
            parser
                .expect_token(&Token::RParen)
                .map_err(ParseError::SqlParseError)?;
            set_primary_key(&mut primary_key, key_columns)?;
        // Schema discovery is all-or-nothing. Mixing declared and inferred
        // columns otherwise produces different schemas in local and cluster mode.
        } else if parser.consume_token(&Token::Mul) {
            return Err(ParseError::StreamingError(
                "CREATE SOURCE does not support wildcard schema merging; omit the column list for connector discovery or declare the complete schema"
                    .into(),
            ));
        // Peek to check for WATERMARK keyword
        } else if try_parse_custom_keyword(parser, "WATERMARK") {
            watermark = Some(parse_watermark_def(parser)?);
        } else {
            // Parse as regular column definition using sqlparser
            let col = parser
                .parse_column_def()
                .map_err(ParseError::SqlParseError)?;
            let inline_primary_keys: Vec<_> = col
                .options
                .iter()
                .filter(|option| {
                    matches!(
                        &option.option,
                        sqlparser::ast::ColumnOption::Unique {
                            is_primary: true,
                            ..
                        }
                    )
                })
                .collect();
            match inline_primary_keys.as_slice() {
                [] => {}
                [option] => {
                    if option.name.is_some() {
                        return Err(ParseError::StreamingError(
                            "CREATE SOURCE does not support named PRIMARY KEY constraints".into(),
                        ));
                    }
                    let sqlparser::ast::ColumnOption::Unique {
                        characteristics, ..
                    } = &option.option
                    else {
                        unreachable!("filtered to inline primary-key options")
                    };
                    if characteristics.is_some() {
                        return Err(ParseError::StreamingError(
                            "CREATE SOURCE does not support PRIMARY KEY constraint characteristics"
                                .into(),
                        ));
                    }
                    set_primary_key(&mut primary_key, vec![col.name.clone()])?;
                }
                _ => {
                    return Err(ParseError::StreamingError(format!(
                        "CREATE SOURCE column '{}' repeats PRIMARY KEY",
                        col.name
                    )));
                }
            }
            columns.push(col);
        }

        // Expect comma or closing paren
        if !parser.consume_token(&Token::Comma) {
            parser
                .expect_token(&Token::RParen)
                .map_err(ParseError::SqlParseError)?;
            break;
        }
    }

    Ok(SourceBody {
        columns,
        primary_key: primary_key.unwrap_or_default(),
        watermark,
    })
}

fn set_primary_key(
    primary_key: &mut Option<Vec<sqlparser::ast::Ident>>,
    columns: Vec<sqlparser::ast::Ident>,
) -> Result<(), ParseError> {
    if primary_key.is_some() {
        return Err(ParseError::StreamingError(
            "CREATE SOURCE accepts at most one PRIMARY KEY declaration".into(),
        ));
    }
    *primary_key = Some(columns);
    Ok(())
}

/// Parse WATERMARK FOR column [AS expression].
///
/// Assumes the WATERMARK keyword has already been consumed.
/// Uses sqlparser's `parse_identifier()` for the column name and
/// `parse_expr()` for the watermark expression.
///
/// When `AS expr` is omitted, the watermark uses `source.watermark()`
/// directly with zero delay.
fn parse_watermark_def(parser: &mut Parser) -> Result<WatermarkDef, ParseError> {
    // FOR
    parser
        .expect_keyword(Keyword::FOR)
        .map_err(ParseError::SqlParseError)?;

    // Column name
    let column = parser
        .parse_identifier()
        .map_err(ParseError::SqlParseError)?;

    // AS is optional — if missing, watermark uses source.watermark() directly
    let expression = if parser.parse_keyword(Keyword::AS) {
        Some(parser.parse_expr().map_err(ParseError::SqlParseError)?)
    } else {
        None
    };

    Ok(WatermarkDef { column, expression })
}

/// Parse optional `FROM <connector_type> (key = 'value', ...)` clause.
///
/// Returns `(Some(connector_type), options)` if present, or `(None, empty_map)`.
fn parse_from_connector(
    parser: &mut Parser,
) -> Result<(Option<String>, HashMap<String, String>), ParseError> {
    if !parser.parse_keyword(Keyword::FROM) {
        return Ok((None, HashMap::new()));
    }

    // Connector type name (e.g., KAFKA, POSTGRES, FILE)
    let token = parser.next_token();
    let connector_type = match &token.token {
        Token::Word(w) => w.value.to_uppercase(),
        other => {
            return Err(ParseError::StreamingError(format!(
                "Expected connector type after FROM, found {other}"
            )));
        }
    };

    // Optional parenthesized options
    let options = if parser.consume_token(&Token::LParen) {
        let mut opts = HashMap::new();
        loop {
            if parser.consume_token(&Token::RParen) {
                break;
            }
            let key = parse_connector_option_key(parser)?;
            parser
                .expect_token(&Token::Eq)
                .map_err(ParseError::SqlParseError)?;
            let value = parse_connector_option_string(parser)?;
            insert_unique_option(&mut opts, key, value)?;
            if !parser.consume_token(&Token::Comma) {
                parser
                    .expect_token(&Token::RParen)
                    .map_err(ParseError::SqlParseError)?;
                break;
            }
        }
        opts
    } else {
        HashMap::new()
    };

    Ok((Some(connector_type), options))
}

/// Parse optional `FORMAT <type> [WITH (key = 'value', ...)]` clause.
fn parse_format_clause(parser: &mut Parser) -> Result<Option<FormatSpec>, ParseError> {
    if !try_parse_custom_keyword(parser, "FORMAT") {
        return Ok(None);
    }

    // Format type name (e.g., JSON, AVRO, PROTOBUF)
    let token = parser.next_token();
    let format_type = match &token.token {
        Token::Word(w) => w.value.to_uppercase(),
        other => {
            return Err(ParseError::StreamingError(format!(
                "Expected format type after FORMAT, found {other}"
            )));
        }
    };

    // Optional WITH (key = 'value', ...) for format-specific options
    let options = parse_with_options(parser)?;

    Ok(Some(FormatSpec {
        format_type,
        options,
    }))
}

/// Parse a connector option key, which may be a dotted identifier
/// (e.g., `json.path`, `json.column.stream_name`, `auth.type`).
fn parse_connector_option_key(parser: &mut Parser) -> Result<String, ParseError> {
    let first = parse_connector_option_string(parser)?;
    let mut key = first;
    while parser.consume_token(&Token::Period) {
        let next = parse_connector_option_string(parser)?;
        key.push('.');
        key.push_str(&next);
    }
    Ok(key)
}

/// Parse a single option key or value string in connector options.
fn parse_connector_option_string(parser: &mut Parser) -> Result<String, ParseError> {
    let token = parser.next_token();
    match token.token {
        Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => Ok(s),
        Token::Word(w) => Ok(w.value),
        Token::Number(n, _) => Ok(n),
        other => Err(ParseError::StreamingError(format!(
            "Expected string or identifier in connector options, found {other}"
        ))),
    }
}

#[cfg(test)]
mod tests;
