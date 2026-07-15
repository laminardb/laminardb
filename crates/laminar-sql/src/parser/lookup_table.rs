//! Parser for CREATE/DROP LOOKUP TABLE DDL statements.
//!
//! Lookup tables are dimension/reference tables used in enrichment joins.
//! They can be backed by snapshot-capable or on-demand connectors
//! (`PostgreSQL`, Redis, etc.)
//! with configurable caching and predicate pushdown strategies.

#[allow(clippy::disallowed_types)] // cold path: SQL parsing
use std::collections::HashMap;

use sqlparser::ast::{ColumnDef, ObjectName};
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::tokenizer::{expect_custom_keyword, expect_statement_end, parse_with_options};
use super::ParseError;

/// CREATE LOOKUP TABLE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateLookupTableStatement {
    /// Table name.
    pub name: ObjectName,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
    /// Primary key column names.
    pub primary_key: Vec<String>,
    /// WITH clause options.
    pub with_options: HashMap<String, String>,
    /// Whether OR REPLACE was specified.
    pub or_replace: bool,
    /// Whether IF NOT EXISTS was specified.
    pub if_not_exists: bool,
}

/// Validated lookup table properties from the WITH clause.
#[derive(Debug, Clone, PartialEq)]
pub struct LookupTableProperties {
    /// Connector type.
    pub connector: ConnectorType,
    /// Connection string.
    pub connection: Option<String>,
    /// Lookup strategy.
    pub strategy: LookupStrategy,
    /// In-memory cache size.
    pub cache_memory: Option<ByteSize>,
    /// Cache TTL in seconds.
    pub cache_ttl: Option<u64>,
    /// Predicate pushdown mode.
    pub pushdown_mode: PushdownMode,
}

/// Connector type for lookup tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectorType {
    /// PostgreSQL standalone connector (poll-based snapshot, no CDC).
    Postgres,
    /// Redis connector.
    Redis,
    /// S3 Parquet connector.
    S3Parquet,
    /// Delta Lake connector.
    DeltaLake,
    /// Static in-memory data.
    Static,
    /// Custom connector type.
    Custom(String),
}

impl ConnectorType {
    /// Parse a connector type from a string.
    ///
    /// # Errors
    ///
    /// Returns `ParseError` if the connector type is empty.
    pub fn parse(s: &str) -> Result<Self, ParseError> {
        let connector = s.trim().to_lowercase();
        if connector.is_empty() {
            return Err(ParseError::ValidationError(
                "connector type cannot be empty".to_string(),
            ));
        }
        Ok(match connector.as_str() {
            "postgres" | "postgresql" => Self::Postgres,
            "postgres-cdc" | "postgres_cdc" => {
                return Err(ParseError::ValidationError(
                    "connector 'postgres-cdc' is a streaming CDC source and cannot back a \
                     LOOKUP TABLE; use connector = 'postgres' for snapshot or on-demand lookups"
                        .to_string(),
                ));
            }
            "redis" => Self::Redis,
            "s3-parquet" | "s3_parquet" | "s3" => Self::S3Parquet,
            "delta-lake" | "delta_lake" | "delta" | "deltalake" => Self::DeltaLake,
            "static" | "memory" => Self::Static,
            other => Self::Custom(other.to_string()),
        })
    }
}

/// Lookup strategy for how table data is distributed/cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LookupStrategy {
    /// Full table replicated on every node.
    #[default]
    Replicated,
    /// Rows fetched on demand (no pre-loading).
    OnDemand,
}

impl LookupStrategy {
    /// Parse a lookup strategy from a string.
    ///
    /// # Errors
    ///
    /// Returns `ParseError` if the strategy is unknown.
    pub fn parse(s: &str) -> Result<Self, ParseError> {
        match s.as_bytes() {
            b"replicated" => Ok(Self::Replicated),
            b"on-demand" => Ok(Self::OnDemand),
            other => Err(ParseError::ValidationError(format!(
                "unknown lookup strategy: '{}' (expected: replicated or on-demand)",
                String::from_utf8_lossy(other)
            ))),
        }
    }
}

/// Predicate pushdown mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PushdownMode {
    /// Automatically decide based on connector capabilities.
    #[default]
    Auto,
    /// Always push predicates to the source.
    Enabled,
    /// Never push predicates to the source.
    Disabled,
}

impl PushdownMode {
    /// Parse a pushdown mode from a string.
    ///
    /// # Errors
    ///
    /// Returns `ParseError` if the mode is unknown.
    pub fn parse(s: &str) -> Result<Self, ParseError> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "enabled" | "true" | "on" => Ok(Self::Enabled),
            "disabled" | "false" | "off" => Ok(Self::Disabled),
            other => Err(ParseError::ValidationError(format!(
                "unknown pushdown mode: '{other}' (expected: auto, enabled, disabled)"
            ))),
        }
    }
}

/// A parsed byte size (e.g., "512mb", "1gb", "10kb").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(pub u64);

impl ByteSize {
    /// Parse a human-readable byte size string.
    ///
    /// Supports suffixes: b, kb, mb, gb, tb (case-insensitive).
    ///
    /// # Errors
    ///
    /// Returns `ParseError` if the string cannot be parsed.
    pub fn parse(s: &str) -> Result<Self, ParseError> {
        let s = s.trim().to_lowercase();
        let (num_str, multiplier) = if let Some(n) = s.strip_suffix("tb") {
            (n, 1024 * 1024 * 1024 * 1024)
        } else if let Some(n) = s.strip_suffix("gb") {
            (n, 1024 * 1024 * 1024)
        } else if let Some(n) = s.strip_suffix("mb") {
            (n, 1024 * 1024)
        } else if let Some(n) = s.strip_suffix("kb") {
            (n, 1024)
        } else if let Some(n) = s.strip_suffix('b') {
            (n, 1)
        } else {
            // Assume bytes if no suffix
            (s.as_str(), 1)
        };

        let num: u64 = num_str
            .trim()
            .parse()
            .map_err(|_| ParseError::ValidationError(format!("invalid byte size: '{s}'")))?;

        Ok(Self(num * multiplier))
    }

    /// Returns the size in bytes.
    #[must_use]
    pub fn as_bytes(&self) -> u64 {
        self.0
    }
}

/// Parse a CREATE LOOKUP TABLE statement.
///
/// Syntax:
/// ```sql
/// CREATE [OR REPLACE] LOOKUP TABLE [IF NOT EXISTS] <name> (
///   <col> <type> [NOT NULL],
///   ...
///   PRIMARY KEY (<col>, ...)
/// ) WITH (
///   'connector' = 'postgres',
///   'connection' = 'postgresql://...',
///   ...
/// );
/// ```
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
pub fn parse_create_lookup_table(
    parser: &mut Parser,
) -> Result<CreateLookupTableStatement, ParseError> {
    // CREATE already consumed by the router; consume it here for standalone parsing
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(ParseError::SqlParseError)?;

    let or_replace = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);

    expect_custom_keyword(parser, "LOOKUP")?;

    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(ParseError::SqlParseError)?;

    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;

    // Parse column definitions: ( col1 TYPE, col2 TYPE, ..., PRIMARY KEY (col1, ...) )
    parser
        .expect_token(&Token::LParen)
        .map_err(ParseError::SqlParseError)?;

    let mut columns = Vec::new();
    let mut primary_key = Vec::new();

    loop {
        // Check for PRIMARY KEY clause
        if parser.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
            parser
                .expect_token(&Token::LParen)
                .map_err(ParseError::SqlParseError)?;

            loop {
                let ident = parser
                    .parse_identifier()
                    .map_err(ParseError::SqlParseError)?;
                primary_key.push(ident.value);

                if !parser.consume_token(&Token::Comma) {
                    break;
                }
            }

            parser
                .expect_token(&Token::RParen)
                .map_err(ParseError::SqlParseError)?;

            // Consume optional trailing comma after PRIMARY KEY clause
            let _ = parser.consume_token(&Token::Comma);
        } else if parser.consume_token(&Token::RParen) {
            // End of column definitions
            break;
        } else {
            // Parse column definition
            let col = parser
                .parse_column_def()
                .map_err(ParseError::SqlParseError)?;
            columns.push(col);

            // Comma or closing paren
            if !parser.consume_token(&Token::Comma) {
                parser
                    .expect_token(&Token::RParen)
                    .map_err(ParseError::SqlParseError)?;
                break;
            }
        }
    }

    if columns.is_empty() {
        return Err(ParseError::StreamingError(
            "LOOKUP TABLE must have at least one column".to_string(),
        ));
    }

    // Parse WITH clause
    let with_options = parse_with_options(parser)?;
    if with_options.is_empty() {
        return Err(ParseError::StreamingError(
            "LOOKUP TABLE requires a WITH clause".to_string(),
        ));
    }
    expect_statement_end(parser)?;

    Ok(CreateLookupTableStatement {
        name,
        columns,
        primary_key,
        with_options,
        or_replace,
        if_not_exists,
    })
}

/// Parse a DROP LOOKUP TABLE statement.
///
/// Syntax: `DROP LOOKUP TABLE [IF EXISTS] <name>`
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
pub fn parse_drop_lookup_table(parser: &mut Parser) -> Result<(ObjectName, bool), ParseError> {
    parser
        .expect_keyword(Keyword::DROP)
        .map_err(ParseError::SqlParseError)?;

    expect_custom_keyword(parser, "LOOKUP")?;

    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(ParseError::SqlParseError)?;

    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);

    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;

    Ok((name, if_exists))
}

/// Validate and extract typed properties from raw WITH options.
///
/// # Errors
///
/// Returns `ParseError` if required properties are missing or invalid.
pub fn validate_properties<S: ::std::hash::BuildHasher>(
    options: &HashMap<String, String, S>,
) -> Result<LookupTableProperties, ParseError> {
    let connector_str = options.get("connector").ok_or_else(|| {
        ParseError::ValidationError("missing required property: 'connector'".to_string())
    })?;
    let connector = ConnectorType::parse(connector_str)?;

    let connection = options.get("connection").cloned();

    let strategy = match options.get("strategy") {
        Some(s) => LookupStrategy::parse(s)?,
        None => LookupStrategy::default(),
    };

    let cache_memory = options
        .get("cache.memory")
        .map(|s| ByteSize::parse(s))
        .transpose()?;

    if options.contains_key("cache.disk") {
        return Err(ParseError::ValidationError(
            "cache.disk is unsupported; on-demand lookup caching is memory-only".into(),
        ));
    }

    let cache_ttl = options
        .get("cache.ttl")
        .map(|s| {
            s.parse::<u64>()
                .map_err(|_| ParseError::ValidationError(format!("invalid cache.ttl: '{s}'")))
        })
        .transpose()?;

    let pushdown_mode = match options.get("pushdown") {
        Some(s) => PushdownMode::parse(s)?,
        None => PushdownMode::default(),
    };

    if strategy == LookupStrategy::Replicated && (cache_memory.is_some() || cache_ttl.is_some()) {
        return Err(ParseError::ValidationError(
            "cache.memory and cache.ttl require strategy = 'on-demand'".into(),
        ));
    }

    Ok(LookupTableProperties {
        connector,
        connection,
        strategy,
        cache_memory,
        cache_ttl,
        pushdown_mode,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::StreamingParser;
    use crate::parser::StreamingStatement;

    /// Helper to parse SQL and return the first statement.
    fn parse_one(sql: &str) -> StreamingStatement {
        let stmts = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 1, "Expected exactly 1 statement");
        stmts.into_iter().next().unwrap()
    }

    #[test]
    fn test_parse_basic_create_lookup_table() {
        let stmt = parse_one(
            "CREATE LOOKUP TABLE instruments (
                symbol VARCHAR NOT NULL,
                name VARCHAR,
                PRIMARY KEY (symbol)
            ) WITH (
                'connector' = 'postgres',
                'connection' = 'postgresql://localhost/db'
            )",
        );
        match stmt {
            StreamingStatement::CreateLookupTable(lt) => {
                assert_eq!(lt.name.to_string(), "instruments");
                assert_eq!(lt.columns.len(), 2);
                assert_eq!(lt.primary_key, vec!["symbol"]);
                assert!(!lt.or_replace);
                assert!(!lt.if_not_exists);
                assert_eq!(
                    lt.with_options.get("connector"),
                    Some(&"postgres".to_string())
                );
            }
            _ => panic!("Expected CreateLookupTable, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_or_replace_and_if_not_exists() {
        let stmt = parse_one(
            "CREATE OR REPLACE LOOKUP TABLE IF NOT EXISTS dims (
                id INT,
                PRIMARY KEY (id)
            ) WITH (
                'connector' = 'static'
            )",
        );
        match stmt {
            StreamingStatement::CreateLookupTable(lt) => {
                assert!(lt.or_replace);
                assert!(lt.if_not_exists);
            }
            _ => panic!("Expected CreateLookupTable, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_with_primary_key() {
        let stmt = parse_one(
            "CREATE LOOKUP TABLE t (
                a INT,
                b VARCHAR,
                c FLOAT,
                PRIMARY KEY (a, b)
            ) WITH ('connector' = 'static')",
        );
        match stmt {
            StreamingStatement::CreateLookupTable(lt) => {
                assert_eq!(lt.primary_key, vec!["a", "b"]);
                assert_eq!(lt.columns.len(), 3);
            }
            _ => panic!("Expected CreateLookupTable, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_with_clause_properties() {
        let stmt = parse_one(
            "CREATE LOOKUP TABLE t (
                id INT,
                PRIMARY KEY (id)
            ) WITH (
                'connector' = 'postgresql',
                'connection' = 'postgresql://localhost/db',
                'strategy' = 'on-demand',
                'cache.memory' = '512mb',
                'pushdown' = 'auto'
            )",
        );
        match stmt {
            StreamingStatement::CreateLookupTable(lt) => {
                let props = validate_properties(&lt.with_options).unwrap();
                assert_eq!(props.connector, ConnectorType::Postgres);
                assert_eq!(
                    props.connection.as_deref(),
                    Some("postgresql://localhost/db")
                );
                assert_eq!(props.strategy, LookupStrategy::OnDemand);
                assert_eq!(props.cache_memory, Some(ByteSize(512 * 1024 * 1024)));
                assert_eq!(props.pushdown_mode, PushdownMode::Auto);
            }
            _ => panic!("Expected CreateLookupTable, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_drop_lookup_table() {
        let stmt = parse_one("DROP LOOKUP TABLE instruments");
        match stmt {
            StreamingStatement::DropLookupTable { name, if_exists } => {
                assert_eq!(name.to_string(), "instruments");
                assert!(!if_exists);
            }
            _ => panic!("Expected DropLookupTable, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_drop_lookup_table_if_exists() {
        let stmt = parse_one("DROP LOOKUP TABLE IF EXISTS instruments");
        match stmt {
            StreamingStatement::DropLookupTable { name, if_exists } => {
                assert_eq!(name.to_string(), "instruments");
                assert!(if_exists);
            }
            _ => panic!("Expected DropLookupTable, got {stmt:?}"),
        }
    }

    #[test]
    fn test_byte_size_parsing() {
        assert_eq!(
            ByteSize::parse("512mb").unwrap(),
            ByteSize(512 * 1024 * 1024)
        );
        assert_eq!(
            ByteSize::parse("1gb").unwrap(),
            ByteSize(1024 * 1024 * 1024)
        );
        assert_eq!(ByteSize::parse("10kb").unwrap(), ByteSize(10 * 1024));
        assert_eq!(ByteSize::parse("100b").unwrap(), ByteSize(100));
        assert_eq!(ByteSize::parse("1024").unwrap(), ByteSize(1024));
        assert_eq!(
            ByteSize::parse("2tb").unwrap(),
            ByteSize(2 * 1024 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn test_connector_type_parsing() {
        assert_eq!(
            ConnectorType::parse("postgresql").unwrap(),
            ConnectorType::Postgres
        );
        for rejected in ["postgres-cdc", "postgres_cdc", " POSTGRES-CDC "] {
            let error = ConnectorType::parse(rejected).unwrap_err().to_string();
            assert!(error.contains("cannot back a LOOKUP TABLE"), "{error}");
            assert!(error.contains("connector = 'postgres'"), "{error}");
        }
        assert_eq!(ConnectorType::parse("redis").unwrap(), ConnectorType::Redis);
        assert_eq!(
            ConnectorType::parse("s3-parquet").unwrap(),
            ConnectorType::S3Parquet
        );
        assert_eq!(
            ConnectorType::parse("static").unwrap(),
            ConnectorType::Static
        );
        assert_eq!(
            ConnectorType::parse("delta-lake").unwrap(),
            ConnectorType::DeltaLake
        );
        assert_eq!(
            ConnectorType::parse("delta").unwrap(),
            ConnectorType::DeltaLake
        );
        assert_eq!(
            ConnectorType::parse("custom-src").unwrap(),
            ConnectorType::Custom("custom-src".to_string())
        );
    }

    #[test]
    fn test_error_missing_columns() {
        let result =
            StreamingParser::parse_sql("CREATE LOOKUP TABLE t () WITH ('connector' = 'static')");
        assert!(result.is_err());
    }

    #[test]
    fn test_error_missing_with_clause() {
        let result = StreamingParser::parse_sql("CREATE LOOKUP TABLE t (id INT, PRIMARY KEY (id))");
        assert!(result.is_err());
    }

    #[test]
    fn test_error_invalid_property() {
        let mut options = HashMap::new();
        options.insert("connector".to_string(), "postgres".to_string());
        options.insert("strategy".to_string(), "invalid-strategy".to_string());
        let result = validate_properties(&options);
        assert!(result.is_err());
    }

    #[test]
    fn lookup_strategy_and_cache_surface_is_strict() {
        assert_eq!(
            LookupStrategy::parse("replicated").unwrap(),
            LookupStrategy::Replicated
        );
        assert_eq!(
            LookupStrategy::parse("on-demand").unwrap(),
            LookupStrategy::OnDemand
        );
        for unsupported in [
            "partitioned",
            "sharded",
            "full",
            "poll",
            "snapshot",
            "cdc",
            "on_demand",
            "lazy",
            "manual",
        ] {
            assert!(LookupStrategy::parse(unsupported).is_err(), "{unsupported}");
        }

        let mut replicated_cache = HashMap::from([
            ("connector".to_string(), "postgres".to_string()),
            ("strategy".to_string(), "replicated".to_string()),
            ("cache.memory".to_string(), "1mb".to_string()),
        ]);
        assert!(validate_properties(&replicated_cache).is_err());
        replicated_cache.insert("strategy".into(), "on-demand".into());
        assert!(validate_properties(&replicated_cache).is_ok());
        replicated_cache.insert("cache.disk".into(), "1gb".into());
        assert!(validate_properties(&replicated_cache).is_err());
    }
}
