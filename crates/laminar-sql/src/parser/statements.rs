//! Streaming SQL statement types
//!
//! This module defines AST types for streaming SQL extensions and provides
//! conversion methods to translate them to runtime operator configurations.

use std::collections::HashMap;
use std::time::Duration;

use sqlparser::ast::{ColumnDef, Expr, Ident, ObjectName};

use super::window_rewriter::WindowRewriter;
use super::ParseError;

/// SHOW command variants for listing streaming objects.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowCommand {
    /// SHOW SOURCES - list all registered sources
    Sources,
    /// SHOW SINKS - list all registered sinks
    Sinks,
    /// SHOW QUERIES - list all running continuous queries
    Queries,
    /// SHOW MATERIALIZED VIEWS - list all materialized views
    MaterializedViews,
    /// SHOW STREAMS - list all named streams
    Streams,
    /// SHOW TABLES - list all reference/dimension tables
    Tables,
}

/// Streaming-specific SQL statements
#[derive(Debug, Clone, PartialEq)]
pub enum StreamingStatement {
    /// Standard SQL statement
    Standard(Box<sqlparser::ast::Statement>),

    /// CREATE SOURCE statement
    CreateSource(Box<CreateSourceStatement>),

    /// CREATE SINK statement
    CreateSink(Box<CreateSinkStatement>),

    /// CREATE CONTINUOUS QUERY
    CreateContinuousQuery {
        /// Query name
        name: ObjectName,
        /// SQL query with streaming extensions
        query: Box<StreamingStatement>,
        /// EMIT clause if present
        emit_clause: Option<EmitClause>,
    },

    /// DROP SOURCE statement
    DropSource {
        /// Source name to drop
        name: ObjectName,
        /// Whether IF EXISTS was specified
        if_exists: bool,
    },

    /// DROP SINK statement
    DropSink {
        /// Sink name to drop
        name: ObjectName,
        /// Whether IF EXISTS was specified
        if_exists: bool,
    },

    /// DROP MATERIALIZED VIEW statement
    DropMaterializedView {
        /// View name to drop
        name: ObjectName,
        /// Whether IF EXISTS was specified
        if_exists: bool,
        /// Whether CASCADE was specified
        cascade: bool,
    },

    /// SHOW SOURCES/SINKS/QUERIES/MATERIALIZED VIEWS
    Show(ShowCommand),

    /// DESCRIBE source, sink, or other streaming object
    Describe {
        /// Object name to describe
        name: ObjectName,
        /// Whether EXTENDED was specified for additional detail
        extended: bool,
    },

    /// EXPLAIN a streaming query plan
    Explain {
        /// The statement to explain
        statement: Box<StreamingStatement>,
    },

    /// CREATE MATERIALIZED VIEW
    CreateMaterializedView {
        /// View name
        name: ObjectName,
        /// The backing query
        query: Box<StreamingStatement>,
        /// Optional EMIT clause
        emit_clause: Option<EmitClause>,
        /// Whether OR REPLACE was specified
        or_replace: bool,
        /// Whether IF NOT EXISTS was specified
        if_not_exists: bool,
    },

    /// CREATE STREAM — named streaming pipeline
    CreateStream {
        /// Stream name
        name: ObjectName,
        /// Backing query (AS SELECT ...)
        query: Box<StreamingStatement>,
        /// Optional EMIT clause
        emit_clause: Option<EmitClause>,
        /// Whether OR REPLACE was specified
        or_replace: bool,
        /// Whether IF NOT EXISTS was specified
        if_not_exists: bool,
    },

    /// DROP STREAM statement
    DropStream {
        /// Stream name to drop
        name: ObjectName,
        /// Whether IF EXISTS was specified
        if_exists: bool,
    },

    /// INSERT INTO a streaming source or table
    InsertInto {
        /// Target table or source name
        table_name: ObjectName,
        /// Column names (empty if not specified)
        columns: Vec<Ident>,
        /// Row values
        values: Vec<Vec<Expr>>,
    },
}

/// Format specification for serialization (e.g., FORMAT JSON, FORMAT AVRO).
#[derive(Debug, Clone, PartialEq)]
pub struct FormatSpec {
    /// Format type (e.g., "JSON", "AVRO", "PROTOBUF").
    pub format_type: String,
    /// Additional format options (from WITH clause after FORMAT).
    pub options: HashMap<String, String>,
}

/// CREATE SOURCE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSourceStatement {
    /// Source name
    pub name: ObjectName,
    /// Column definitions
    pub columns: Vec<ColumnDef>,
    /// Watermark definition
    pub watermark: Option<WatermarkDef>,
    /// Source connector options (from WITH clause)
    pub with_options: HashMap<String, String>,
    /// Whether to replace existing source
    pub or_replace: bool,
    /// Whether to skip if exists
    pub if_not_exists: bool,
    /// Connector type (e.g., "KAFKA") from `FROM KAFKA (...)` syntax
    pub connector_type: Option<String>,
    /// Connector-specific options (from `FROM KAFKA (...)`)
    pub connector_options: HashMap<String, String>,
    /// Format specification (e.g., `FORMAT JSON`)
    pub format: Option<FormatSpec>,
}

/// CREATE SINK statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSinkStatement {
    /// Sink name
    pub name: ObjectName,
    /// Input query or table
    pub from: SinkFrom,
    /// Sink connector options (from WITH clause)
    pub with_options: HashMap<String, String>,
    /// Whether to replace existing sink
    pub or_replace: bool,
    /// Whether to skip if exists
    pub if_not_exists: bool,
    /// Optional WHERE filter expression
    pub filter: Option<Expr>,
    /// Connector type (e.g., "KAFKA") from `INTO KAFKA (...)` syntax
    pub connector_type: Option<String>,
    /// Connector-specific options (from `INTO KAFKA (...)`)
    pub connector_options: HashMap<String, String>,
    /// Format specification (e.g., `FORMAT JSON`)
    pub format: Option<FormatSpec>,
    /// Output options (from `WITH (key = ...)` after FORMAT)
    pub output_options: HashMap<String, String>,
}

/// Source for a sink
#[derive(Debug, Clone, PartialEq)]
pub enum SinkFrom {
    /// From a table or source
    Table(ObjectName),
    /// From a SELECT query
    Query(Box<StreamingStatement>),
}

/// Watermark definition
#[derive(Debug, Clone, PartialEq)]
pub struct WatermarkDef {
    /// Column to use for watermark
    pub column: Ident,
    /// Watermark expression (e.g., column - INTERVAL '5' SECOND).
    /// `None` when `WATERMARK FOR col` is used without `AS expr`,
    /// meaning watermark advances via `source.watermark()` with zero delay.
    pub expression: Option<Expr>,
}

/// Late data handling clause.
///
/// Controls what happens to events that arrive after their window has closed.
/// This is the SQL AST representation of late data configuration.
/// See `laminar_core::operator::window::LateDataConfig` for the runtime representation.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LateDataClause {
    /// Allowed lateness duration (e.g., `INTERVAL '1' HOUR`)
    pub allowed_lateness: Option<Box<Expr>>,
    /// Side output name for late events (e.g., `late_events`)
    pub side_output: Option<String>,
}

impl LateDataClause {
    /// Creates a clause with allowed lateness only.
    #[must_use]
    pub fn with_allowed_lateness(lateness: Expr) -> Self {
        Self {
            allowed_lateness: Some(Box::new(lateness)),
            side_output: None,
        }
    }

    /// Creates a clause with both allowed lateness and side output.
    #[must_use]
    pub fn with_side_output(lateness: Expr, side_output: String) -> Self {
        Self {
            allowed_lateness: Some(Box::new(lateness)),
            side_output: Some(side_output),
        }
    }

    /// Creates a clause with side output only (uses default lateness).
    #[must_use]
    pub fn side_output_only(side_output: String) -> Self {
        Self {
            allowed_lateness: None,
            side_output: Some(side_output),
        }
    }

    /// Convert to allowed lateness Duration.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::WindowError` if the interval cannot be parsed.
    pub fn to_allowed_lateness(&self) -> Result<Duration, ParseError> {
        match &self.allowed_lateness {
            Some(expr) => WindowRewriter::parse_interval_to_duration(expr),
            None => Ok(Duration::ZERO),
        }
    }

    /// Check if this clause has a side output configured.
    #[must_use]
    pub fn has_side_output(&self) -> bool {
        self.side_output.is_some()
    }

    /// Get the side output name, if configured.
    #[must_use]
    pub fn get_side_output(&self) -> Option<&str> {
        self.side_output.as_deref()
    }
}

/// Emit strategy for runtime operator configuration.
///
/// This is the runtime representation that operators use.
#[derive(Debug, Clone, PartialEq)]
pub enum EmitStrategy {
    /// Emit when watermark passes window end
    OnWatermark,
    /// Emit only when window closes (no intermediate results)
    OnWindowClose,
    /// Emit at fixed intervals
    Periodic(Duration),
    /// Emit on every state change
    OnUpdate,
    /// Emit changelog records with Z-set weights
    Changelog,
    /// Emit only final results, suppress all intermediate
    FinalOnly,
}

/// EMIT clause for controlling output timing.
///
/// This is the SQL AST representation of emit strategies.
/// See `laminar_core::operator::window::EmitStrategy` for the runtime representation.
#[derive(Debug, Clone, PartialEq)]
pub enum EmitClause {
    // === Existing (F011) ===
    /// EMIT AFTER WATERMARK (or EMIT ON WATERMARK)
    ///
    /// Emit results when the watermark passes the window end.
    /// This is the most efficient strategy.
    AfterWatermark,

    /// EMIT ON WINDOW CLOSE
    ///
    /// For append-only sinks (Kafka, S3, Delta Lake, Iceberg).
    /// Only emits when window closes, no intermediate results.
    /// Unlike `AfterWatermark`, this is NOT a synonym - it has distinct behavior.
    OnWindowClose,

    /// EMIT EVERY INTERVAL 'N' unit (or EMIT PERIODICALLY)
    ///
    /// Emit intermediate results at fixed intervals.
    /// Final results are still emitted on watermark.
    Periodically {
        /// The interval expression (e.g., INTERVAL '5' SECOND)
        interval: Box<Expr>,
    },

    /// EMIT ON UPDATE
    ///
    /// Emit updated results after every state change.
    /// This provides lowest latency but highest overhead.
    OnUpdate,

    // === New (F011B) ===
    /// EMIT CHANGES
    ///
    /// Emit changelog records with Z-set weights for CDC pipelines.
    /// Every emission includes operation type and weight:
    /// - Insert (+1 weight)
    /// - Delete (-1 weight)
    /// - Update (retraction pair: -1 old, +1 new)
    ///
    /// Required for:
    /// - CDC pipelines
    /// - Cascading materialized views
    /// - Downstream consumers that need to track changes
    Changes,

    /// EMIT FINAL
    ///
    /// Suppress ALL intermediate results, emit only finalized.
    /// Also drops late data entirely after window close.
    /// Use for BI reporting where only final, exact results matter.
    Final,
}

impl std::fmt::Display for EmitClause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmitClause::AfterWatermark => write!(f, "EMIT AFTER WATERMARK"),
            EmitClause::OnWindowClose => write!(f, "EMIT ON WINDOW CLOSE"),
            EmitClause::Periodically { interval } => write!(f, "EMIT EVERY {interval}"),
            EmitClause::OnUpdate => write!(f, "EMIT ON UPDATE"),
            EmitClause::Changes => write!(f, "EMIT CHANGES"),
            EmitClause::Final => write!(f, "EMIT FINAL"),
        }
    }
}

impl EmitClause {
    /// Convert to runtime EmitStrategy.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::WindowError` if the periodic interval cannot be parsed.
    pub fn to_emit_strategy(&self) -> Result<EmitStrategy, ParseError> {
        match self {
            EmitClause::AfterWatermark => Ok(EmitStrategy::OnWatermark),
            EmitClause::OnWindowClose => Ok(EmitStrategy::OnWindowClose),
            EmitClause::Periodically { interval } => {
                let duration = WindowRewriter::parse_interval_to_duration(interval)?;
                Ok(EmitStrategy::Periodic(duration))
            }
            EmitClause::OnUpdate => Ok(EmitStrategy::OnUpdate),
            EmitClause::Changes => Ok(EmitStrategy::Changelog),
            EmitClause::Final => Ok(EmitStrategy::FinalOnly),
        }
    }

    /// Check if this emit strategy requires changelog/retraction support.
    #[must_use]
    pub fn requires_changelog(&self) -> bool {
        matches!(self, EmitClause::Changes | EmitClause::OnUpdate)
    }

    /// Check if this emit strategy is append-only (no retractions).
    #[must_use]
    pub fn is_append_only(&self) -> bool {
        matches!(
            self,
            EmitClause::OnWindowClose | EmitClause::Final | EmitClause::AfterWatermark
        )
    }

    /// Returns true if this emit strategy requires a watermark on the source.
    ///
    /// `OnWindowClose`, `Final`, and `AfterWatermark` all depend on watermark
    /// advancement to trigger window closure. Without a watermark, timers will
    /// never fire and windows will never close.
    #[must_use]
    pub fn requires_watermark(&self) -> bool {
        matches!(
            self,
            EmitClause::OnWindowClose | EmitClause::Final | EmitClause::AfterWatermark
        )
    }
}

/// Window function types
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFunction {
    /// TUMBLE(column, interval)
    Tumble {
        /// The time column to window on
        time_column: Box<Expr>,
        /// The window interval
        interval: Box<Expr>,
    },
    /// HOP(column, slide, size)
    Hop {
        /// The time column to window on
        time_column: Box<Expr>,
        /// The slide interval (how often to create a new window)
        slide_interval: Box<Expr>,
        /// The window size interval
        window_interval: Box<Expr>,
    },
    /// SESSION(column, gap)
    Session {
        /// The time column to window on
        time_column: Box<Expr>,
        /// The gap interval (max gap between events in same session)
        gap_interval: Box<Expr>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{DataType, Expr, ObjectNamePart};

    #[test]
    fn test_create_source_statement() {
        let stmt = CreateSourceStatement {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("events"))]),
            columns: vec![
                ColumnDef {
                    name: Ident::new("id"),
                    data_type: DataType::BigInt(None),
                    options: vec![],
                },
                ColumnDef {
                    name: Ident::new("timestamp"),
                    data_type: DataType::Timestamp(None, sqlparser::ast::TimezoneInfo::None),
                    options: vec![],
                },
            ],
            watermark: Some(WatermarkDef {
                column: Ident::new("timestamp"),
                expression: Some(Expr::Identifier(Ident::new("timestamp"))),
            }),
            with_options: HashMap::from([
                ("connector".to_string(), "kafka".to_string()),
                ("topic".to_string(), "events".to_string()),
            ]),
            or_replace: false,
            if_not_exists: true,
            connector_type: None,
            connector_options: HashMap::new(),
            format: None,
        };

        // Check the statement fields
        assert_eq!(stmt.columns.len(), 2);
        assert!(stmt.watermark.is_some());
        assert_eq!(
            stmt.with_options.get("connector"),
            Some(&"kafka".to_string())
        );
    }

    #[test]
    fn test_emit_clause_variants() {
        let emit1 = EmitClause::AfterWatermark;
        let emit2 = EmitClause::OnWindowClose;
        let emit3 = EmitClause::Periodically {
            interval: Box::new(Expr::Identifier(Ident::new("5_SECONDS"))),
        };
        let emit4 = EmitClause::OnUpdate;

        match emit1 {
            EmitClause::AfterWatermark => (),
            _ => panic!("Expected AfterWatermark"),
        }

        match emit2 {
            EmitClause::OnWindowClose => (),
            _ => panic!("Expected OnWindowClose"),
        }

        match emit3 {
            EmitClause::Periodically { .. } => (),
            _ => panic!("Expected Periodically"),
        }

        match emit4 {
            EmitClause::OnUpdate => (),
            _ => panic!("Expected OnUpdate"),
        }
    }

    #[test]
    fn test_window_functions() {
        let tumble = WindowFunction::Tumble {
            time_column: Box::new(Expr::Identifier(Ident::new("event_time"))),
            interval: Box::new(Expr::Identifier(Ident::new("5_MINUTES"))),
        };

        let hop = WindowFunction::Hop {
            time_column: Box::new(Expr::Identifier(Ident::new("event_time"))),
            slide_interval: Box::new(Expr::Identifier(Ident::new("1_MINUTE"))),
            window_interval: Box::new(Expr::Identifier(Ident::new("5_MINUTES"))),
        };

        match tumble {
            WindowFunction::Tumble { .. } => (),
            _ => panic!("Expected Tumble"),
        }

        match hop {
            WindowFunction::Hop { .. } => (),
            _ => panic!("Expected Hop"),
        }
    }

    #[test]
    fn test_late_data_clause_default() {
        let clause = LateDataClause::default();
        assert!(clause.allowed_lateness.is_none());
        assert!(clause.side_output.is_none());
    }

    #[test]
    fn test_late_data_clause_with_allowed_lateness() {
        let lateness_expr = Expr::Identifier(Ident::new("INTERVAL '1' HOUR"));
        let clause = LateDataClause::with_allowed_lateness(lateness_expr);
        assert!(clause.allowed_lateness.is_some());
        assert!(clause.side_output.is_none());
    }

    #[test]
    fn test_late_data_clause_with_side_output() {
        let lateness_expr = Expr::Identifier(Ident::new("INTERVAL '1' HOUR"));
        let clause = LateDataClause::with_side_output(lateness_expr, "late_events".to_string());
        assert!(clause.allowed_lateness.is_some());
        assert_eq!(clause.side_output, Some("late_events".to_string()));
    }

    #[test]
    fn test_late_data_clause_side_output_only() {
        let clause = LateDataClause::side_output_only("late_events".to_string());
        assert!(clause.allowed_lateness.is_none());
        assert_eq!(clause.side_output, Some("late_events".to_string()));
    }

    #[test]
    fn test_show_command_variants() {
        let sources = ShowCommand::Sources;
        let sinks = ShowCommand::Sinks;
        let queries = ShowCommand::Queries;
        let mvs = ShowCommand::MaterializedViews;

        assert_eq!(sources, ShowCommand::Sources);
        assert_eq!(sinks, ShowCommand::Sinks);
        assert_eq!(queries, ShowCommand::Queries);
        assert_eq!(mvs, ShowCommand::MaterializedViews);
    }

    #[test]
    fn test_show_command_clone() {
        let cmd = ShowCommand::Sources;
        let cloned = cmd.clone();
        assert_eq!(cmd, cloned);
    }

    #[test]
    fn test_drop_source_statement() {
        let stmt = StreamingStatement::DropSource {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("events"))]),
            if_exists: true,
        };
        match stmt {
            StreamingStatement::DropSource { name, if_exists } => {
                assert_eq!(name.to_string(), "events");
                assert!(if_exists);
            }
            _ => panic!("Expected DropSource"),
        }
    }

    #[test]
    fn test_drop_sink_statement() {
        let stmt = StreamingStatement::DropSink {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("output"))]),
            if_exists: false,
        };
        match stmt {
            StreamingStatement::DropSink { name, if_exists } => {
                assert_eq!(name.to_string(), "output");
                assert!(!if_exists);
            }
            _ => panic!("Expected DropSink"),
        }
    }

    #[test]
    fn test_drop_materialized_view_statement() {
        let stmt = StreamingStatement::DropMaterializedView {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("live_stats"))]),
            if_exists: true,
            cascade: true,
        };
        match stmt {
            StreamingStatement::DropMaterializedView {
                name,
                if_exists,
                cascade,
            } => {
                assert_eq!(name.to_string(), "live_stats");
                assert!(if_exists);
                assert!(cascade);
            }
            _ => panic!("Expected DropMaterializedView"),
        }
    }

    #[test]
    fn test_show_statement() {
        let stmt = StreamingStatement::Show(ShowCommand::Sources);
        match stmt {
            StreamingStatement::Show(ShowCommand::Sources) => (),
            _ => panic!("Expected Show(Sources)"),
        }
    }

    #[test]
    fn test_describe_statement() {
        let stmt = StreamingStatement::Describe {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("events"))]),
            extended: true,
        };
        match stmt {
            StreamingStatement::Describe { name, extended } => {
                assert_eq!(name.to_string(), "events");
                assert!(extended);
            }
            _ => panic!("Expected Describe"),
        }
    }

    #[test]
    fn test_explain_statement() {
        // Build an inner Standard statement using sqlparser
        let dialect = sqlparser::dialect::GenericDialect {};
        let stmts = sqlparser::parser::Parser::parse_sql(&dialect, "SELECT 1").unwrap();
        let inner = StreamingStatement::Standard(Box::new(stmts.into_iter().next().unwrap()));

        let stmt = StreamingStatement::Explain {
            statement: Box::new(inner),
        };
        match stmt {
            StreamingStatement::Explain { statement } => {
                assert!(matches!(*statement, StreamingStatement::Standard(_)));
            }
            _ => panic!("Expected Explain"),
        }
    }

    #[test]
    fn test_create_materialized_view_statement() {
        // Build a query statement using sqlparser
        let dialect = sqlparser::dialect::GenericDialect {};
        let stmts =
            sqlparser::parser::Parser::parse_sql(&dialect, "SELECT COUNT(*) FROM events").unwrap();
        let query = StreamingStatement::Standard(Box::new(stmts.into_iter().next().unwrap()));

        let stmt = StreamingStatement::CreateMaterializedView {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("live_stats"))]),
            query: Box::new(query),
            emit_clause: Some(EmitClause::OnWindowClose),
            or_replace: false,
            if_not_exists: true,
        };
        match stmt {
            StreamingStatement::CreateMaterializedView {
                name,
                emit_clause,
                or_replace,
                if_not_exists,
                ..
            } => {
                assert_eq!(name.to_string(), "live_stats");
                assert_eq!(emit_clause, Some(EmitClause::OnWindowClose));
                assert!(!or_replace);
                assert!(if_not_exists);
            }
            _ => panic!("Expected CreateMaterializedView"),
        }
    }

    #[test]
    fn test_insert_into_statement() {
        let stmt = StreamingStatement::InsertInto {
            table_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("events"))]),
            columns: vec![Ident::new("id"), Ident::new("name")],
            values: vec![vec![
                Expr::Value(sqlparser::ast::Value::Number("1".to_string(), false).into()),
                Expr::Value(sqlparser::ast::Value::SingleQuotedString("test".to_string()).into()),
            ]],
        };
        match stmt {
            StreamingStatement::InsertInto {
                table_name,
                columns,
                values,
            } => {
                assert_eq!(table_name.to_string(), "events");
                assert_eq!(columns.len(), 2);
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].len(), 2);
            }
            _ => panic!("Expected InsertInto"),
        }
    }

    #[test]
    fn test_eowc_requires_watermark_helper() {
        // Watermark-dependent strategies
        assert!(EmitClause::OnWindowClose.requires_watermark());
        assert!(EmitClause::Final.requires_watermark());
        assert!(EmitClause::AfterWatermark.requires_watermark());

        // Non-watermark strategies
        assert!(!EmitClause::OnUpdate.requires_watermark());
        assert!(!EmitClause::Changes.requires_watermark());
        let periodic = EmitClause::Periodically {
            interval: Box::new(Expr::Identifier(Ident::new("5_SECONDS"))),
        };
        assert!(!periodic.requires_watermark());
    }
}
