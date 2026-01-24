//! Streaming SQL statement types

use std::collections::HashMap;
use sqlparser::ast::{ColumnDef, Expr, Ident, ObjectName};

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
    /// Source connector options
    pub with_options: HashMap<String, String>,
    /// Whether to replace existing source
    pub or_replace: bool,
    /// Whether to skip if exists
    pub if_not_exists: bool,
}

/// CREATE SINK statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSinkStatement {
    /// Sink name
    pub name: ObjectName,
    /// Input query or table
    pub from: SinkFrom,
    /// Sink connector options
    pub with_options: HashMap<String, String>,
    /// Whether to replace existing sink
    pub or_replace: bool,
    /// Whether to skip if exists
    pub if_not_exists: bool,
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
    /// Watermark expression (e.g., column - INTERVAL '5' SECOND)
    pub expression: Expr,
}

/// EMIT clause for controlling output timing.
///
/// This is the SQL AST representation of emit strategies.
/// See `laminar_core::operator::window::EmitStrategy` for the runtime representation.
#[derive(Debug, Clone, PartialEq)]
pub enum EmitClause {
    /// EMIT AFTER WATERMARK (or EMIT ON WATERMARK)
    ///
    /// Emit results when the watermark passes the window end.
    /// This is the most efficient strategy.
    AfterWatermark,

    /// EMIT ON WINDOW CLOSE
    ///
    /// Synonym for `AfterWatermark` - emit when window closes.
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
                expression: Expr::Identifier(Ident::new("timestamp")),
            }),
            with_options: HashMap::from([
                ("connector".to_string(), "kafka".to_string()),
                ("topic".to_string(), "events".to_string()),
            ]),
            or_replace: false,
            if_not_exists: true,
        };

        // Check the statement fields
        assert_eq!(stmt.columns.len(), 2);
        assert!(stmt.watermark.is_some());
        assert_eq!(stmt.with_options.get("connector"), Some(&"kafka".to_string()));
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
}