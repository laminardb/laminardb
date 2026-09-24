//! Error types for the `LaminarDB` facade.

use laminar_core::error_codes;

/// Errors from database operations.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// SQL parse error
    Sql(#[from] laminar_sql::Error),

    /// Core engine error
    Engine(#[from] laminar_core::Error),

    /// Streaming API error
    Streaming(#[from] laminar_core::streaming::StreamingError),

    /// `DataFusion` error (translated to user-friendly messages on display)
    DataFusion(#[from] datafusion_common::DataFusionError),

    /// Source not found
    SourceNotFound(String),

    /// Sink not found
    SinkNotFound(String),

    /// Query not found
    QueryNotFound(String),

    /// Source already exists
    SourceAlreadyExists(String),

    /// Sink already exists
    SinkAlreadyExists(String),

    /// Stream not found
    StreamNotFound(String),

    /// Stream already exists
    StreamAlreadyExists(String),

    /// Table not found
    TableNotFound(String),

    /// Table already exists
    TableAlreadyExists(String),

    /// A local reference-table update or replacement exceeded its live quota before mutation.
    ReferenceTableQuotaExceeded {
        /// Table whose final state was rejected.
        table: String,
        /// Projected live row count.
        rows: usize,
        /// Projected retained-memory charge.
        bytes: usize,
        /// Configured per-table row limit.
        max_rows: usize,
        /// Configured per-table retained-memory limit.
        max_bytes: usize,
    },

    /// Insert error
    InsertError(String),

    /// Schema mismatch between Rust type and SQL definition
    SchemaMismatch(String),

    /// Invalid SQL statement for the operation
    InvalidOperation(String),

    /// Requested subscription epoch has committed but its replay suffix was pruned.
    SubscriptionReplayPruned {
        /// Subscription object name.
        name: String,
        /// Checkpoint epoch requested by the subscriber.
        requested: u64,
        /// Earliest checkpoint epoch whose cut remains replayable.
        earliest_retained: u64,
    },

    /// Requested subscription epoch has not committed.
    SubscriptionEpochNotCommitted {
        /// Subscription object name.
        name: String,
        /// Checkpoint epoch requested by the subscriber.
        requested: u64,
        /// Newest durably committed checkpoint epoch, if one exists.
        latest_committed: Option<u64>,
    },

    /// Requested subscription shared-log sequence is no longer retained for replay.
    SubscriptionSequencePruned {
        /// Subscription object name.
        name: String,
        /// Shared-log sequence requested by the subscriber; replay begins strictly after it.
        requested_sequence: u64,
        /// Earliest shared-log sequence still retained for replay.
        earliest_retained_sequence: u64,
    },

    /// Structured committed cluster-subscription failure.
    Subscription(#[from] crate::subscription::ClusterSubscriptionError),

    /// SQL parse error (from streaming parser)
    SqlParse(#[from] laminar_sql::parser::ParseError),

    /// Database is shut down
    Shutdown,

    /// Checkpoint error
    Checkpoint(String),

    /// Checkpoint store error (preserves structured source error).
    CheckpointStore(#[from] laminar_core::checkpoint::checkpoint_store::CheckpointStoreError),

    /// Unresolved config variable
    UnresolvedConfigVar(String),

    /// Connector error
    Connector(String),

    /// Connector operation error (preserves structured source error).
    ConnectorOp(#[from] laminar_connectors::error::ConnectorError),

    /// Pipeline error (start/shutdown lifecycle)
    Pipeline(String),

    /// A deterministic record-path failure that retry or checkpoint recovery cannot repair.
    PipelineTerminal(String),

    /// `BackpressurePolicy::Fail` tripped; coordinator halts the pipeline.
    BackpressureFail(String),

    /// Prospective graph input exceeds a port's retained-byte or batch limit. Operator state
    /// may already have changed, so the generation halts before output publication or retry.
    GraphBufferBudgetExceeded {
        /// Destination operator or source name.
        node: String,
        /// Destination input port.
        port: u8,
        /// Projected retained batch count, including the rejected input.
        batches: usize,
        /// Projected retained Arrow charge, including the rejected input.
        bytes: usize,
        /// Configured batch limit; zero disables the count limit.
        max_batches: usize,
        /// Configured byte limit; `None` disables the byte limit.
        max_bytes: Option<usize>,
    },

    /// A cross-node shuffle target isn't reachable yet (cluster formation).
    /// Recoverable — `OperatorGraph::execute_single_operator` defers on it.
    ShuffleNotReady(String),

    /// A permanent structural shuffle-routing failure. Retrying or restoring the same input cannot
    /// repair it, so the pipeline must halt instead of entering a recovery loop.
    ShuffleTerminal(String),

    /// A cross-node shuffle send failed after an earlier frame was admitted to the transport and
    /// may reach its peer. Unlike [`Self::ShuffleNotReady`], this must not be replayed locally: a
    /// retry could double-fold the admitted rows. Recovery rewinds the complete failure domain to a
    /// durable cut before replay.
    ShufflePartialSend(String),

    /// A stateful operator may have changed local state before the attempt failed. This denotes an
    /// indeterminate apply outcome, not proof that exactly a prefix was applied. The graph must not
    /// replay that input against the possibly changed state; recovery rewinds the failure domain to
    /// its last durable cut.
    StatefulOperatorPartialApply(String),

    /// Managed working state crossed the configured pipeline-wide charged-byte envelope. The
    /// current graph generation must halt: replaying the same input against the same limit would
    /// loop, and a record-path detection may follow state mutation but always precedes output.
    ManagedStateBudgetExceeded {
        /// Boundary at which the excess was detected.
        context: String,
        /// Combined live, prepared, and retired charged bytes.
        accounted_bytes: usize,
        /// Configured pipeline-wide maximum.
        limit_bytes: usize,
    },

    /// Query pipeline error — wraps a `DataFusion` error with stream context.
    /// Unlike `Pipeline`, this variant is translated to user-friendly messages.
    QueryPipeline {
        /// The stream or query name where the error occurred.
        context: String,
        /// The translated error message (already processed through
        /// `translate_datafusion_error`).
        translated: String,
    },

    /// Materialized view error
    MaterializedView(String),

    /// A local materialized-view update or restore exceeded its live or staging quota.
    MaterializedViewQuotaExceeded {
        /// View whose projected state was rejected.
        view: String,
        /// Projected live or staged row count (distinct rows for multisets).
        rows: usize,
        /// Projected retained-memory charge.
        bytes: usize,
        /// Effective per-view row limit for the rejected phase.
        max_rows: usize,
        /// Effective per-view byte limit for the rejected phase.
        max_bytes: usize,
    },

    /// Storage backend error.
    Storage(String),

    /// Configuration / profile validation error
    Config(String),

    /// Operation is not yet implemented.
    Unsupported(String),
}

impl DbError {
    /// Create a `QueryPipeline` error from a `DataFusion` error with stream context.
    ///
    /// The `DataFusion` error is translated to a user-friendly message with
    /// structured error codes. The raw `DataFusion` internals are never exposed.
    pub fn query_pipeline(
        context: impl Into<String>,
        df_error: &datafusion_common::DataFusionError,
    ) -> Self {
        let translated = laminar_sql::error::translate_datafusion_error(&df_error.to_string());
        Self::QueryPipeline {
            context: context.into(),
            translated: translated.to_string(),
        }
    }

    /// Create a `QueryPipeline` error from a `DataFusion` error with stream
    /// context and available column names for typo suggestions.
    pub fn query_pipeline_with_columns(
        context: impl Into<String>,
        df_error: &datafusion_common::DataFusionError,
        available_columns: &[&str],
    ) -> Self {
        let translated = laminar_sql::error::translate_datafusion_error_with_context(
            &df_error.to_string(),
            Some(available_columns),
        );
        Self::QueryPipeline {
            context: context.into(),
            translated: translated.to_string(),
        }
    }

    /// Create a `QueryPipeline` error from an Arrow error with stream context.
    pub fn query_pipeline_arrow(
        context: impl Into<String>,
        arrow_error: &arrow::error::ArrowError,
    ) -> Self {
        let translated = laminar_sql::error::translate_datafusion_error(&arrow_error.to_string());
        Self::QueryPipeline {
            context: context.into(),
            translated: translated.to_string(),
        }
    }

    /// Returns the structured `LDB-NNNN` error code for this error.
    ///
    /// Every `DbError` variant maps to a stable error code that can be used
    /// for programmatic handling, log searching, and metrics.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Sql(_) | Self::SqlParse(_) => error_codes::SQL_UNSUPPORTED,
            Self::Engine(_) | Self::Streaming(_) => error_codes::INTERNAL,
            Self::DataFusion(_) => error_codes::QUERY_EXECUTION_FAILED,
            Self::SourceNotFound(_) => error_codes::SOURCE_NOT_FOUND,
            Self::SinkNotFound(_) => error_codes::SINK_NOT_FOUND,
            Self::QueryNotFound(_) | Self::StreamNotFound(_) | Self::TableNotFound(_) => {
                error_codes::SQL_TABLE_NOT_FOUND
            }
            Self::SourceAlreadyExists(_)
            | Self::StreamAlreadyExists(_)
            | Self::TableAlreadyExists(_) => error_codes::SOURCE_ALREADY_EXISTS,
            Self::SinkAlreadyExists(_) => error_codes::SINK_ALREADY_EXISTS,
            Self::InsertError(_) => error_codes::CONNECTOR_WRITE_ERROR,
            Self::SchemaMismatch(_) => error_codes::SCHEMA_MISMATCH,
            Self::InvalidOperation(_)
            | Self::ReferenceTableQuotaExceeded { .. }
            | Self::SubscriptionReplayPruned { .. }
            | Self::SubscriptionEpochNotCommitted { .. }
            | Self::SubscriptionSequencePruned { .. }
            | Self::Unsupported(_) => error_codes::INVALID_OPERATION,
            Self::Subscription(error) => error.code(),
            Self::Shutdown => error_codes::SHUTDOWN,
            Self::Checkpoint(_) | Self::CheckpointStore(_) => error_codes::CHECKPOINT_FAILED,
            Self::UnresolvedConfigVar(_) => error_codes::UNRESOLVED_CONFIG_VAR,
            Self::Connector(_) | Self::ConnectorOp(_) => error_codes::CONNECTOR_CONNECTION_FAILED,
            Self::Pipeline(_)
            | Self::PipelineTerminal(_)
            | Self::BackpressureFail(_)
            | Self::GraphBufferBudgetExceeded { .. }
            | Self::ShuffleNotReady(_)
            | Self::ShuffleTerminal(_)
            | Self::ShufflePartialSend(_)
            | Self::StatefulOperatorPartialApply(_) => error_codes::PIPELINE_ERROR,
            Self::ManagedStateBudgetExceeded { .. } => error_codes::MANAGED_STATE_BUDGET_EXCEEDED,
            Self::QueryPipeline { .. } => error_codes::QUERY_PIPELINE_ERROR,
            Self::MaterializedView(_) | Self::MaterializedViewQuotaExceeded { .. } => {
                error_codes::MATERIALIZED_VIEW_ERROR
            }
            Self::Storage(_) => error_codes::WAL_ERROR,
            Self::Config(_) => error_codes::INVALID_CONFIG,
        }
    }

    /// `true` for [`Self::ShuffleNotReady`] — the operator defers instead of failing the cycle.
    #[must_use]
    pub fn is_shuffle_not_ready(&self) -> bool {
        matches!(self, Self::ShuffleNotReady(_))
    }

    /// `true` when retry or recovery cannot repair the current input and the pipeline must stop.
    #[must_use]
    pub fn requires_pipeline_halt(&self) -> bool {
        matches!(
            self,
            Self::PipelineTerminal(_)
                | Self::BackpressureFail(_)
                | Self::GraphBufferBudgetExceeded { .. }
                | Self::ShuffleTerminal(_)
                | Self::ManagedStateBudgetExceeded { .. }
        )
    }

    /// `true` when failure-domain isolation cannot safely keep the current pipeline alive.
    #[must_use]
    pub fn requires_pipeline_recovery(&self) -> bool {
        matches!(
            self,
            Self::Checkpoint(_)
                | Self::ShufflePartialSend(_)
                | Self::StatefulOperatorPartialApply(_)
        )
    }

    /// Whether this error is transient (retryable).
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Streaming(_)
            | Self::Connector(_)
            | Self::Checkpoint(_)
            | Self::CheckpointStore(_) => true,
            Self::ConnectorOp(e) => e.is_transient(),
            _ => false,
        }
    }
}

mod display;
