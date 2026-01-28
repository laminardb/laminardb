//! Streaming SQL statement types
//!
//! This module defines AST types for streaming SQL extensions and provides
//! conversion methods to translate them to runtime operator configurations.

use std::collections::HashMap;
use std::time::Duration;

use sqlparser::ast::{ColumnDef, Expr, Ident, ObjectName};

use super::window_rewriter::WindowRewriter;
use super::ParseError;

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

    // ==================== Late Data Clause Tests ====================

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
}