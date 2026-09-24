//! `Display` implementation for [`DbError`].

use super::DbError;

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sql(e) => write!(f, "SQL error: {e}"),
            Self::Engine(e) => write!(f, "Engine error: {e}"),
            Self::Streaming(e) => write!(f, "Streaming error: {e}"),
            Self::DataFusion(e) => {
                let translated = laminar_sql::error::translate_datafusion_error(&e.to_string());
                write!(f, "{translated}")
            }
            Self::SourceNotFound(name) => {
                write!(f, "[{}] Source '{name}' not found", self.code())
            }
            Self::SinkNotFound(name) => {
                write!(f, "[{}] Sink '{name}' not found", self.code())
            }
            Self::QueryNotFound(name) => {
                write!(f, "[{}] Query '{name}' not found", self.code())
            }
            Self::SourceAlreadyExists(name) => {
                write!(f, "[{}] Source '{name}' already exists", self.code())
            }
            Self::SinkAlreadyExists(name) => {
                write!(f, "[{}] Sink '{name}' already exists", self.code())
            }
            Self::StreamNotFound(name) => {
                write!(f, "[{}] Stream '{name}' not found", self.code())
            }
            Self::StreamAlreadyExists(name) => {
                write!(f, "[{}] Stream '{name}' already exists", self.code())
            }
            Self::TableNotFound(name) => {
                write!(f, "[{}] Table '{name}' not found", self.code())
            }
            Self::TableAlreadyExists(name) => {
                write!(f, "[{}] Table '{name}' already exists", self.code())
            }
            Self::InsertError(msg) => {
                write!(f, "[{}] Insert error: {msg}", self.code())
            }
            Self::SchemaMismatch(msg) => {
                write!(f, "[{}] Schema mismatch: {msg}", self.code())
            }
            Self::InvalidOperation(msg) => {
                write!(f, "[{}] Invalid operation: {msg}", self.code())
            }
            Self::SubscriptionReplayPruned {
                name,
                requested,
                earliest_retained,
            } => write!(
                f,
                "[{}] Epoch {requested} for stream '{name}' is no longer retained (earliest retained is {earliest_retained})",
                self.code()
            ),
            Self::SubscriptionSequencePruned {
                name,
                requested_sequence,
                earliest_retained_sequence,
            } => write!(
                f,
                "[{}] Sequence {requested_sequence} for stream '{name}' is no longer retained (earliest retained is {earliest_retained_sequence})",
                self.code()
            ),
            Self::SubscriptionEpochNotCommitted {
                name,
                requested,
                latest_committed,
            } => match latest_committed {
                Some(latest) => write!(
                    f,
                    "[{}] Epoch {requested} for stream '{name}' is not committed (latest committed is {latest})",
                    self.code()
                ),
                None => write!(
                    f,
                    "[{}] Epoch {requested} for stream '{name}' is not committed (no committed epoch is available)",
                    self.code()
                ),
            },
            Self::Subscription(error) => write!(f, "[{}] {error}", self.code()),
            Self::SqlParse(e) => write!(f, "SQL parse error: {e}"),
            Self::Shutdown => write!(f, "[{}] Database is shut down", self.code()),
            Self::Checkpoint(msg) => {
                write!(f, "[{}] Checkpoint error: {msg}", self.code())
            }
            Self::CheckpointStore(e) => {
                write!(f, "[{}] Checkpoint store error: {e}", self.code())
            }
            Self::UnresolvedConfigVar(msg) => {
                write!(f, "[{}] Unresolved config variable: {msg}", self.code())
            }
            Self::Connector(msg) => {
                write!(f, "[{}] Connector error: {msg}", self.code())
            }
            Self::ConnectorOp(e) => {
                write!(f, "[{}] Connector error: {e}", self.code())
            }
            Self::Pipeline(_)
            | Self::PipelineTerminal(_)
            | Self::BackpressureFail(_)
            | Self::GraphBufferBudgetExceeded { .. }
            | Self::ShuffleNotReady(_)
            | Self::ShuffleTerminal(_)
            | Self::ShufflePartialSend(_)
            | Self::StatefulOperatorPartialApply(_)
            | Self::ReferenceTableQuotaExceeded { .. }
            | Self::MaterializedViewQuotaExceeded { .. }
            | Self::ManagedStateBudgetExceeded { .. } => self.fmt_execution_error(f),
            Self::QueryPipeline {
                context,
                translated,
            } => write!(f, "Stream '{context}': {translated}"),
            Self::MaterializedView(msg) => {
                write!(f, "[{}] Materialized view error: {msg}", self.code())
            }
            Self::Storage(msg) => {
                write!(f, "[{}] Storage error: {msg}", self.code())
            }
            Self::Config(msg) => {
                write!(f, "[{}] Config error: {msg}", self.code())
            }
            Self::Unsupported(msg) => {
                write!(f, "[{}] Unsupported: {msg}", self.code())
            }
        }
    }
}

impl DbError {
    fn fmt_execution_error(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pipeline(msg) => {
                write!(f, "[{}] Pipeline error: {msg}", self.code())
            }
            Self::PipelineTerminal(msg) => {
                write!(f, "[{}] Terminal pipeline error: {msg}", self.code())
            }
            Self::BackpressureFail(msg) => {
                write!(f, "[{}] Backpressure fail: {msg}", self.code())
            }
            Self::GraphBufferBudgetExceeded {
                node, port, batches, bytes, max_batches, max_bytes,
            } => write!(
                f,
                "[{}] Graph input budget exceeded at '{node}' port {port}: projected={batches} batches/{bytes} bytes, limits={max_batches} batches/{max_bytes:?} bytes; reduce batch size or increase graph input limits; terminal fault resolution is required before restarting",
                self.code()
            ),
            Self::ShuffleNotReady(msg) => {
                write!(f, "[{}] Shuffle target not ready: {msg}", self.code())
            }
            Self::ShuffleTerminal(msg) => {
                write!(f, "[{}] Terminal shuffle routing error: {msg}", self.code())
            }
            Self::ShufflePartialSend(msg) => {
                write!(f, "[{}] Shuffle partial send: {msg}", self.code())
            }
            Self::StatefulOperatorPartialApply(msg) => {
                write!(
                    f,
                    "[{}] Stateful operator partial apply: {msg}",
                    self.code()
                )
            }
            Self::ManagedStateBudgetExceeded {
                context,
                accounted_bytes,
                limit_bytes,
            } => write!(
                f,
                "[{}] Managed state budget exceeded during {context}: accounted={accounted_bytes} bytes, limit={limit_bytes} bytes",
                self.code()
            ),
            Self::MaterializedViewQuotaExceeded { view, rows, bytes, max_rows, max_bytes } => write!(
                f, "[{}] Materialized-view '{view}' quota exceeded: projected={rows} rows/{bytes} bytes, limits={max_rows} rows/{max_bytes} bytes", self.code()
            ),
            Self::ReferenceTableQuotaExceeded { table, rows, bytes, max_rows, max_bytes } => write!(
                f, "[{}] Reference-table '{table}' quota exceeded: projected={rows} rows/{bytes} bytes, limits={max_rows} rows/{max_bytes} bytes", self.code()
            ),
            _ => unreachable!("execution formatting is dispatched only for execution errors"),
        }
    }
}
