//! AST types for streaming SQL extensions and their runtime conversions.

#[allow(clippy::disallowed_types)] // cold path: SQL parsing
use std::collections::HashMap;
use std::time::Duration;

use sqlparser::ast::{ColumnDef, Expr, Ident, ObjectName, Statement};

use super::join_parser::JoinAnalysis;
use super::window_rewriter::WindowRewriter;
use super::ParseError;

/// Supported `SHOW` commands.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowCommand {
    /// List registered sources.
    Sources,
    /// List registered sinks.
    Sinks,
    /// List continuous queries.
    Queries,
    /// List materialized views.
    MaterializedViews,
    /// List named streams.
    Streams,
    /// List reference tables.
    Tables,
    /// Show checkpoint state.
    CheckpointStatus,
    /// Reconstruct source DDL.
    CreateSource {
        /// Source to reconstruct.
        name: ObjectName,
    },
    /// Reconstruct sink DDL.
    CreateSink {
        /// Sink to reconstruct.
        name: ObjectName,
    },
}

/// SQL statements understood by the streaming parser.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamingStatement {
    /// A standard SQL statement without streaming syntax.
    Standard(Box<sqlparser::ast::Statement>),

    /// A multi-horizon temporal probe normalized to the canonical AS-OF query shape.
    TemporalProbeQuery {
        /// AST used for projection and non-join analysis.
        statement: Box<Statement>,
        /// Parsed temporal-join contract.
        analysis: Box<JoinAnalysis>,
    },

    /// `CREATE SOURCE`.
    CreateSource(Box<CreateSourceStatement>),
    /// `CREATE SINK`.
    CreateSink(Box<CreateSinkStatement>),
    /// `CREATE CONTINUOUS QUERY`.
    CreateContinuousQuery {
        /// Query name.
        name: ObjectName,
        /// Streaming query body.
        query: Box<StreamingStatement>,
        /// Optional output strategy.
        emit_clause: Option<EmitClause>,
    },

    /// `DROP SOURCE`.
    DropSource {
        /// Source to drop.
        name: ObjectName,
        /// Ignore a missing source.
        if_exists: bool,
        /// Also drop dependent streams and materialized views.
        cascade: bool,
    },

    /// `DROP SINK`.
    DropSink {
        /// Sink to drop.
        name: ObjectName,
        /// Ignore a missing sink.
        if_exists: bool,
        /// Also drop dependent objects.
        cascade: bool,
    },

    /// `DROP MATERIALIZED VIEW`.
    DropMaterializedView {
        /// View to drop.
        name: ObjectName,
        /// Ignore a missing view.
        if_exists: bool,
        /// Also drop dependent objects.
        cascade: bool,
    },

    /// A `SHOW` command.
    Show(ShowCommand),

    /// `DESCRIBE` a streaming object.
    Describe {
        /// Object to describe.
        name: ObjectName,
        /// Include extended details.
        extended: bool,
    },

    /// `EXPLAIN [ANALYZE]` a streaming query plan.
    Explain {
        /// Statement to explain.
        statement: Box<StreamingStatement>,
        /// Execute the query and collect metrics.
        analyze: bool,
    },

    /// `CREATE MATERIALIZED VIEW`.
    CreateMaterializedView {
        /// View name.
        name: ObjectName,
        /// Backing query.
        query: Box<StreamingStatement>,
        /// Optional output strategy.
        emit_clause: Option<EmitClause>,
        /// Replace an existing view.
        or_replace: bool,
        /// Ignore an existing view.
        if_not_exists: bool,
        /// Raw query text between `AS` and `EMIT`.
        query_sql: String,
    },

    /// `CREATE STREAM` for a named streaming pipeline.
    CreateStream {
        /// Stream name.
        name: ObjectName,
        /// Backing query.
        query: Box<StreamingStatement>,
        /// Optional output strategy.
        emit_clause: Option<EmitClause>,
        /// Replace an existing stream.
        or_replace: bool,
        /// Ignore an existing stream.
        if_not_exists: bool,
        /// Raw query text between `AS` and `EMIT`.
        query_sql: String,
        /// `RETAIN_HISTORY` cap in bytes from the trailing `WITH (...)`.
        retention_bytes: Option<u64>,
    },

    /// `DROP STREAM`.
    DropStream {
        /// Stream to drop.
        name: ObjectName,
        /// Ignore a missing stream.
        if_exists: bool,
        /// Also drop dependent objects.
        cascade: bool,
    },

    /// `ALTER SOURCE`.
    AlterSource {
        /// Source to alter.
        name: ObjectName,
        /// Requested alteration.
        operation: AlterSourceOperation,
    },

    /// `INSERT INTO` a streaming source or table.
    InsertInto {
        /// Target source or table.
        table_name: ObjectName,
        /// Empty when the statement does not name columns.
        columns: Vec<Ident>,
        /// Rows to insert.
        values: Vec<Vec<Expr>>,
    },

    /// `CREATE LOOKUP TABLE`.
    CreateLookupTable(Box<super::lookup_table::CreateLookupTableStatement>),

    /// `DROP LOOKUP TABLE`.
    DropLookupTable {
        /// Lookup table to drop.
        name: ObjectName,
        /// Ignore a missing table.
        if_exists: bool,
    },

    /// Trigger an immediate checkpoint.
    Checkpoint,

    /// `RESTORE FROM CHECKPOINT <id>`.
    RestoreCheckpoint {
        /// Durable checkpoint identifier.
        checkpoint_id: u64,
    },

    /// `SUBSCRIBE <name> [WHERE ...] [WITH (...)]`.
    Subscribe(Box<SubscribeStatement>),

    /// `DECLARE <name> [NO SCROLL] CURSOR [WITHOUT HOLD] FOR SUBSCRIBE …`
    ///
    /// Forward-only cursor over a SUBSCRIBE, scoped to the current SimpleQuery
    /// connection. SCROLL, BINARY, WITH HOLD, INSENSITIVE, and ASENSITIVE are
    /// rejected at parse time.
    DeclareCursorForSubscribe {
        /// Cursor identifier.
        name: Ident,
        /// `NO SCROLL` was explicit in the source. We never emit SCROLL.
        no_scroll: bool,
        /// The SUBSCRIBE body the cursor wraps.
        subscribe: Box<SubscribeStatement>,
    },
}

/// `SUBSCRIBE <name> [AS OF EPOCH n] [WHERE <fragment>] [WITH (...)]`.
#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeStatement {
    /// Target stream or materialized view.
    pub name: ObjectName,
    /// Raw WHERE fragment, compiled by the engine against the target schema.
    pub filter_sql: Option<String>,
    /// `AS OF EPOCH n`: replay everything emitted strictly after barrier `n`.
    pub as_of_epoch: Option<u64>,
    /// Reserved `WITH` options.
    pub options: HashMap<String, String>,
}

/// Operations for ALTER SOURCE statements.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterSourceOperation {
    /// Add a new column: `ALTER SOURCE name ADD COLUMN col_name data_type`
    AddColumn {
        /// Column to add.
        column_def: ColumnDef,
    },
    /// Set source properties: `ALTER SOURCE name SET ('key' = 'value', ...)`
    SetProperties {
        /// Properties to replace.
        properties: HashMap<String, String>,
    },
}

/// Format specification for serialization (e.g., FORMAT JSON, FORMAT AVRO).
#[derive(Debug, Clone, PartialEq)]
pub struct FormatSpec {
    /// For example, `JSON`, `AVRO`, or `PROTOBUF`.
    pub format_type: String,
    /// Additional format options (from WITH clause after FORMAT).
    pub options: HashMap<String, String>,
}

/// Parsed `CREATE SOURCE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSourceStatement {
    /// Source name.
    pub name: ObjectName,
    /// Declared columns.
    pub columns: Vec<ColumnDef>,
    /// Primary-key columns, empty when no key is declared.
    pub primary_key: Vec<Ident>,
    /// Optional event-time watermark.
    pub watermark: Option<WatermarkDef>,
    /// Source runtime options from the trailing `WITH` clause.
    pub with_options: HashMap<String, String>,
    /// Replace an existing source.
    pub or_replace: bool,
    /// Ignore an existing source.
    pub if_not_exists: bool,
    /// Connector type (e.g., "KAFKA") from `FROM KAFKA (...)` syntax
    pub connector_type: Option<String>,
    /// Connector-specific options (from `FROM KAFKA (...)`)
    pub connector_options: HashMap<String, String>,
    /// Format specification (e.g., `FORMAT JSON`)
    pub format: Option<FormatSpec>,
}

/// Parsed `CREATE SINK` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSinkStatement {
    /// Sink name.
    pub name: ObjectName,
    /// Sink input.
    pub from: SinkFrom,
    /// Replace an existing sink.
    pub or_replace: bool,
    /// Ignore an existing sink.
    pub if_not_exists: bool,
    /// Optional row filter.
    pub filter: Option<Expr>,
    /// Connector type (e.g., "KAFKA") from `INTO KAFKA (...)` syntax
    pub connector_type: Option<String>,
    /// Connector-specific options (from `INTO KAFKA (...)`)
    pub connector_options: HashMap<String, String>,
    /// Format specification (e.g., `FORMAT JSON`)
    pub format: Option<FormatSpec>,
}

/// Input selected by a sink.
#[derive(Debug, Clone, PartialEq)]
pub enum SinkFrom {
    /// A named table or source.
    Table(ObjectName),
    /// A query result.
    Query(Box<StreamingStatement>),
}

/// Parsed watermark definition.
#[derive(Debug, Clone, PartialEq)]
pub struct WatermarkDef {
    /// Event-time column.
    pub column: Ident,
    /// Watermark expression (e.g., column - INTERVAL '5' SECOND).
    /// `None` when `WATERMARK FOR col` is used without `AS expr`,
    /// meaning watermark advances via `source.watermark()` with zero delay.
    pub expression: Option<Expr>,
}

/// SQL configuration for events that arrive after their window closes.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LateDataClause {
    /// For example, `INTERVAL '1' HOUR`.
    pub allowed_lateness: Option<Box<Expr>>,
    /// Destination for late events.
    pub side_output: Option<String>,
}

impl LateDataClause {
    /// Uses the given lateness without a side output.
    #[must_use]
    pub fn with_allowed_lateness(lateness: Expr) -> Self {
        Self {
            allowed_lateness: Some(Box::new(lateness)),
            side_output: None,
        }
    }

    /// Uses the given lateness and side output.
    #[must_use]
    pub fn with_side_output(lateness: Expr, side_output: String) -> Self {
        Self {
            allowed_lateness: Some(Box::new(lateness)),
            side_output: Some(side_output),
        }
    }

    /// Uses the default lateness with a side output.
    #[must_use]
    pub fn side_output_only(side_output: String) -> Self {
        Self {
            allowed_lateness: None,
            side_output: Some(side_output),
        }
    }

    /// # Errors
    ///
    /// Returns `ParseError::WindowError` if the interval cannot be parsed.
    pub fn to_allowed_lateness(&self) -> Result<Duration, ParseError> {
        match &self.allowed_lateness {
            Some(expr) => WindowRewriter::parse_interval_to_duration(expr),
            None => Ok(Duration::ZERO),
        }
    }

    #[must_use]
    /// Whether late events have a side-output destination.
    pub fn has_side_output(&self) -> bool {
        self.side_output.is_some()
    }

    #[must_use]
    /// The configured side-output destination.
    pub fn get_side_output(&self) -> Option<&str> {
        self.side_output.as_deref()
    }
}

/// Runtime output strategy for a streaming operator.
#[derive(Debug, Clone, PartialEq)]
pub enum EmitStrategy {
    /// Emit when the watermark passes a window end.
    OnWatermark,
    /// Emit only when a window closes.
    OnWindowClose,
    /// Emit at a fixed interval.
    Periodic(Duration),
    /// Emit after every state change.
    OnUpdate,
    /// Emit Z-set changelog records.
    Changelog,
    /// Suppress intermediate results.
    FinalOnly,
}

/// Parsed `EMIT` strategy.
#[derive(Debug, Clone, PartialEq)]
pub enum EmitClause {
    /// Emit when the watermark passes the window end.
    AfterWatermark,

    /// Emit only when the window closes; this is distinct from `AfterWatermark`.
    OnWindowClose,

    /// Emit intermediate results periodically and final results on watermark.
    Periodically {
        /// Parsed interval expression.
        interval: Box<Expr>,
    },

    /// Emit after every state change.
    OnUpdate,

    /// Emit Z-set changelog weights, including retraction pairs for updates.
    Changes,

    /// Emit only finalized results and drop data arriving after window close.
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

    /// Whether the strategy can emit retractions.
    #[must_use]
    pub fn requires_changelog(&self) -> bool {
        matches!(self, EmitClause::Changes | EmitClause::OnUpdate)
    }

    /// Whether the strategy produces no retractions.
    #[must_use]
    pub fn is_append_only(&self) -> bool {
        matches!(
            self,
            EmitClause::OnWindowClose | EmitClause::Final | EmitClause::AfterWatermark
        )
    }

    /// Whether output depends on source watermark advancement.
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

/// Parsed streaming window function.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFunction {
    /// `TUMBLE(column, interval [, offset])`.
    Tumble {
        /// Event-time expression.
        time_column: Box<Expr>,
        /// Window size.
        interval: Box<Expr>,
        /// Optional timezone-alignment offset.
        offset: Option<Box<Expr>>,
    },
    /// `HOP(column, slide, size [, offset])`.
    Hop {
        /// Event-time expression.
        time_column: Box<Expr>,
        /// How often a new window begins.
        slide_interval: Box<Expr>,
        /// Window size.
        window_interval: Box<Expr>,
        /// Optional timezone-alignment offset.
        offset: Option<Box<Expr>>,
    },
    /// `SESSION(column, gap)`.
    Session {
        /// Event-time expression.
        time_column: Box<Expr>,
        /// Maximum gap between events in one session.
        gap_interval: Box<Expr>,
    },
    /// `CUMULATE(column, step, size)`.
    Cumulate {
        /// Event-time expression.
        time_column: Box<Expr>,
        /// Window growth increment.
        step_interval: Box<Expr>,
        /// Maximum window size.
        max_size_interval: Box<Expr>,
    },
}

#[cfg(test)]
mod tests;
