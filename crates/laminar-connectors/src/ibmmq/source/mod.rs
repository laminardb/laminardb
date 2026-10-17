//! IBM MQ queue source over MQI with syncpoint-controlled gets.
//!
//! Every message a poll gets joins the connection's single unit of work. When the engine
//! captures a checkpoint, intake pauses so that unit of work holds exactly the messages that
//! checkpoint covers; when the checkpoint is durable, `MQCMIT` releases them and intake resumes.
//! A crash or failed generation backs the unit of work out (explicitly, or by the queue manager
//! when the connection breaks), so every uncommitted message is redelivered with its
//! `BackoutCount` incremented (at-least-once; replay may duplicate).
//!
//! The pause lasts from barrier capture to the durable-commit notification, once per checkpoint.

mod fetch;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::SchemaRef;
use async_trait::async_trait;

use super::config::IbmMqSourceConfig;
use super::library;
use super::metrics::SourceMetrics;
use super::session::Session;
use super::worker::Worker;
use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{
    ConnectorCancellationPolicy, ConnectorTaskOwner, ConnectorTaskTracker, SourceBatch,
    SourceConnector, SourceConsistency, SourceContract, SourceInputMode, SourcePosition,
    SourceStart, SourceTopology,
};
use crate::error::ConnectorError;
use crate::serde;
use fetch::{BackoutRoute, Limits, SourceSession};

const GENERATION: &str = "generation";
const GOTTEN: &str = "gotten";
const CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

/// IBM MQ queue source.
pub struct IbmMqSource {
    schema: SchemaRef,
    registry: Option<prometheus::Registry>,
    running: Option<Running>,
    failure: Option<String>,
    /// Messages covered by the latest barrier cursor; written by `checkpoint(&self)`.
    captured: AtomicU64,
    task_owner: ConnectorTaskOwner,
    task_tracker: ConnectorTaskTracker,
}

struct Running {
    worker: Worker<SourceSession>,
    config: Arc<IbmMqSourceConfig>,
    generation: u64,
    /// Messages this generation's units of work have taken from the queue.
    gotten: u64,
    /// Messages already committed.
    committed: u64,
    uow_messages: usize,
    uow_bytes: usize,
    uow_full: bool,
    metrics: SourceMetrics,
}

/// What to do with a committed cursor.
#[derive(Debug, PartialEq, Eq)]
enum CommitPlan {
    /// Nothing of this generation's open unit of work is covered.
    Nothing,
    /// The open unit of work is exactly the committed cut.
    Commit,
}

impl Running {
    fn cursor(&self, gotten: u64) -> SourceCheckpoint {
        let mut cursor = SourceCheckpoint::new();
        cursor.set_offset(GENERATION, format!("{:016x}", self.generation));
        cursor.set_offset(GOTTEN, gotten.to_string());
        cursor
    }

    fn plan(&self, cursor: &SourceCheckpoint) -> Result<CommitPlan, ConnectorError> {
        plan_for(self.generation, self.gotten, self.committed, cursor)
    }

    fn limits(&self, max_records: usize) -> Option<Limits> {
        limits_for(
            &self.config,
            self.uow_messages,
            self.uow_bytes,
            self.uow_full,
            max_records,
        )
    }
}

/// A unit of work commits as a whole, so a committed cursor must name all of it.
fn plan_for(
    generation: u64,
    gotten: u64,
    committed: u64,
    cursor: &SourceCheckpoint,
) -> Result<CommitPlan, ConnectorError> {
    let (Some(cursor_generation), Some(cursor_gotten)) =
        (cursor.get_offset(GENERATION), cursor.get_offset(GOTTEN))
    else {
        return Ok(CommitPlan::Nothing);
    };
    let corrupt = || ConnectorError::InvalidState {
        expected: "an IBM MQ source cursor".into(),
        actual: "an unparseable generation or message count".into(),
    };
    let cursor_generation = u64::from_str_radix(cursor_generation, 16).map_err(|_| corrupt())?;
    let cursor_gotten: u64 = cursor_gotten.parse().map_err(|_| corrupt())?;
    if cursor_generation != generation || cursor_gotten <= committed {
        return Ok(CommitPlan::Nothing);
    }
    if cursor_gotten != gotten {
        return Err(ConnectorError::InvalidState {
            expected: format!(
                "a committed cut covering the whole unit of work ({gotten} messages)"
            ),
            actual: format!("a committed cut covering {cursor_gotten} messages"),
        });
    }
    Ok(CommitPlan::Commit)
}

/// Poll limits within the unit of work's remaining capacity; `None` waits for a checkpoint.
fn limits_for(
    config: &IbmMqSourceConfig,
    uow_messages: usize,
    uow_bytes: usize,
    uow_full: bool,
    max_records: usize,
) -> Option<Limits> {
    let messages = config.max_uow_messages.saturating_sub(uow_messages);
    let bytes = config.max_uow_bytes.saturating_sub(uow_bytes);
    if uow_full || messages == 0 || bytes == 0 {
        return None;
    }
    Some(Limits {
        messages: messages.min(max_records.max(1)),
        bytes: bytes.min(config.max_batch_bytes),
        wait_ms: i32::try_from(config.wait_interval.as_millis()).unwrap_or(i32::MAX),
    })
}

impl IbmMqSource {
    /// Create an unstarted source; metrics register on `registry` at start.
    #[must_use]
    pub fn new(schema: SchemaRef, registry: Option<&prometheus::Registry>) -> Self {
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        Self {
            schema,
            registry: registry.cloned(),
            running: None,
            failure: None,
            captured: AtomicU64::new(0),
            task_owner,
            task_tracker,
        }
    }

    /// End this generation; dropping the worker backs out the open unit of work.
    fn fail(&mut self, error: &ConnectorError) -> ConnectorError {
        tracing::error!(%error, "IBM MQ source generation ended");
        self.running = None;
        let cause = error.to_string();
        self.failure.get_or_insert(cause.clone());
        terminal(cause)
    }

    fn resolve_output(
        &self,
        config: &ConnectorConfig,
        parsed: &IbmMqSourceConfig,
    ) -> Result<(SchemaRef, SchemaRef), ConnectorError> {
        let declared = config
            .arrow_schema()
            .or_else(|| (!self.schema.fields().is_empty()).then(|| Arc::clone(&self.schema)));
        let payload = declared
            .map(|output| parsed.metadata.payload_schema(&output))
            .transpose()?;
        let binding = serde::schema_contract::reader_binding(config, parsed.format, payload)?;
        let payload = Arc::new(binding.logical);
        parsed.metadata.validate_payload(&payload)?;
        let output = parsed.metadata.output_schema(&payload);
        if config
            .schema_binding()
            .is_some_and(|expected| expected.logical != *output)
        {
            return Err(ConnectorError::SchemaMismatch(
                "IBM MQ reader differs from its committed contract".into(),
            ));
        }
        Ok((payload, output))
    }
}

/// Open the input queue and validate the queue's backout settings before any get.
fn open_session(
    config: &Arc<IbmMqSourceConfig>,
    library: library::Library,
    deserializer: Box<dyn serde::RecordDeserializer>,
    payload: SchemaRef,
    output: SchemaRef,
) -> Result<SourceSession, ConnectorError> {
    use mqi::constants;
    let session = Session::open(
        &config.connection,
        library,
        &config.queue,
        None,
        constants::MQOO_INPUT_AS_Q_DEF | constants::MQOO_INQUIRE,
    )?;
    let (threshold, queue) = session.backout_settings()?;
    let route = match (threshold, queue.is_empty()) {
        (0, _) => None,
        (_, true) => {
            return Err(ConnectorError::ConfigurationError(format!(
                "queue '{}' sets BOTHRESH({threshold}) without BOQNAME; set BOQNAME to a local \
                 queue or BOTHRESH(0). The connector never falls back to the dead-letter queue",
                config.queue
            )))
        }
        (_, false) => {
            let resolved = session.resolve(&queue)?;
            if !resolved.is_empty() && resolved != session.queue_manager {
                return Err(ConnectorError::ConfigurationError(format!(
                    "BOQNAME '{queue}' resolves to queue manager '{resolved}', not '{}'; a poison \
                     message moves atomically only to a local queue",
                    session.queue_manager
                )));
            }
            Some(BackoutRoute { threshold, queue })
        }
    };
    if route.is_none() && config.on_decode_error == super::config::DecodeErrorPolicy::BackoutQueue {
        return Err(ConnectorError::ConfigurationError(format!(
            "on.decode.error = 'backout-queue' needs queue '{}' to set BOTHRESH and a local BOQNAME",
            config.queue
        )));
    }
    Ok(SourceSession::new(
        session,
        route,
        Arc::clone(config),
        deserializer,
        payload,
        output,
    ))
}

fn terminal(cause: String) -> ConnectorError {
    ConnectorError::InvalidState {
        expected: "a live IBM MQ connection".into(),
        actual: cause,
    }
}

#[async_trait]
impl SourceConnector for IbmMqSource {
    /// Dropping a poll or commit leaves the source valid: a poll whose result is lost after it
    /// got messages taints the unit of work, which then never commits and is backed out at close.
    /// The ledger only advances after an awaited result arrives.
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::CancelSafe
    }

    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    async fn resolve_schema(
        &mut self,
        config: &ConnectorConfig,
        explicit: Option<SchemaRef>,
    ) -> Result<crate::schema::resolution::SchemaBinding, ConnectorError> {
        use crate::schema::resolution::{logical_binding, SchemaDirection};
        let parsed = IbmMqSourceConfig::from_config(config)?;
        let reader = serde::schema_contract::reader_binding(config, parsed.format, explicit)?;
        let payload = Arc::new(reader.logical);
        parsed.metadata.validate_payload(&payload)?;
        logical_binding(
            config,
            SchemaDirection::Source,
            reader.origin,
            &parsed.metadata.output_schema(&payload),
        )
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        IbmMqSourceConfig::from_config(config)?;
        // MQCMIT runs only for a durable checkpoint. Exactly-once is not certified: the queue
        // commit and the sink commit are separate.
        Ok(SourceContract::new(
            SourceConsistency::CommitCoupled,
            SourceTopology::Singleton,
            SourceInputMode::AppendOnly,
        ))
    }

    fn recovery_identity_options(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Option<std::collections::BTreeMap<String, String>>, ConnectorError> {
        Ok(Some(IbmMqSourceConfig::from_config(config)?.identity()))
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (config, position, _) = request.into_parts();
        if matches!(position, SourcePosition::Initialized { .. }) {
            return Err(ConnectorError::ConfigurationError(
                "IBM MQ has no sealed topology startup contract".into(),
            ));
        }
        // A resume needs no positioning: the queue manager backed out every message the
        // committed cut did not cover when the previous connection ended.
        let parsed = Arc::new(IbmMqSourceConfig::from_config(&config)?);
        let (payload, output) = self.resolve_output(&config, &parsed)?;
        let deserializer = serde::create_deserializer(parsed.format)?;
        let library = library::load(parsed.connection.client_library.as_deref())?;
        let name = config
            .get("laminar.source.name")
            .unwrap_or("ibmmq")
            .to_string();
        let metrics = SourceMetrics::new(&name, self.registry.as_ref())?;
        let guard = self.task_owner.track().ok_or_else(|| {
            ConnectorError::Internal("IBM MQ source task generation is retired".into())
        })?;
        let session_config = Arc::clone(&parsed);
        let session_output = Arc::clone(&output);
        let worker = Worker::start(
            &format!("ibmmq-src-{name}"),
            guard,
            parsed.connection.operation_timeout,
            move || {
                open_session(
                    &session_config,
                    library,
                    deserializer,
                    payload,
                    session_output,
                )
            },
        )
        .await?;
        tracing::info!(
            queue = %parsed.queue,
            connection = ?parsed.connection,
            "IBM MQ source consuming under syncpoint"
        );
        self.schema = output;
        self.failure = None;
        self.captured.store(0, Ordering::Relaxed);
        self.running = Some(Running {
            worker,
            config: parsed,
            generation: rand::random(),
            gotten: 0,
            committed: 0,
            uow_messages: 0,
            uow_bytes: 0,
            uow_full: false,
            metrics,
        });
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if let Some(cause) = &self.failure {
            return Err(terminal(cause.clone()));
        }
        let Some(running) = self.running.as_mut() else {
            return Ok(None);
        };
        // A captured cut not yet committed seals the unit of work: getting more now would
        // commit messages that checkpoint does not contain.
        if self.captured.load(Ordering::Relaxed) > running.committed {
            return Ok(None);
        }
        let Some(limits) = running.limits(max_records) else {
            return Ok(None);
        };
        let timeout = running.config.connection.operation_timeout + running.config.wait_interval;
        let polled = match running
            .worker
            .call_or(
                "MQGET",
                timeout,
                move |session| session.poll(&limits),
                |session, polled| session.lost(&polled),
            )
            .await
        {
            Ok(polled) => polled,
            Err(error) => return Err(self.fail(&error)),
        };
        running.gotten += polled.gotten as u64;
        running.uow_messages += polled.gotten;
        running.uow_bytes += polled.bytes;
        running.uow_full |= polled.uow_full;
        let metrics = &running.metrics;
        metrics.messages.inc_by(polled.gotten as u64);
        metrics.bytes.inc_by(polled.bytes as u64);
        metrics.moved.inc_by(polled.moved as u64);
        metrics.redelivered.inc_by(polled.redelivered as u64);
        metrics
            .uncommitted
            .set(i64::try_from(running.uow_messages).unwrap_or(i64::MAX));
        let records = polled.records.filter(|records| records.num_rows() > 0);
        if let Some(records) = &records {
            metrics.rows.inc_by(records.num_rows() as u64);
        }
        // A bound cursor keeps the runtime from capturing through `checkpoint`, which seals
        // the unit of work; only a barrier capture should.
        let cursor = running.cursor(running.gotten);
        Ok(records.map(|records| SourceBatch::new(records).with_checkpoint(cursor)))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let Some(running) = self.running.as_ref() else {
            return SourceCheckpoint::new();
        };
        self.captured.store(running.gotten, Ordering::Relaxed);
        running.cursor(running.gotten)
    }

    async fn notify_epoch_committed(
        &mut self,
        epoch: u64,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        let Some(running) = self.running.as_mut() else {
            return Ok(());
        };
        let plan = match running.plan(checkpoint) {
            Ok(plan) => plan,
            Err(error) => return Err(self.fail(&error)),
        };
        if plan == CommitPlan::Nothing {
            return Ok(());
        }
        let timeout = running.config.connection.operation_timeout;
        if let Err(error) = running
            .worker
            .call("MQCMIT", timeout, |session| session.commit())
            .await
        {
            return Err(self.fail(&ConnectorError::outcome_unknown(
                format!(
                    "MQCMIT for committed epoch {epoch}: {error}; its messages may be redelivered"
                ),
                false,
            )));
        }
        running
            .metrics
            .committed
            .inc_by(running.gotten - running.committed);
        running.committed = running.gotten;
        running.uow_messages = 0;
        running.uow_bytes = 0;
        running.uow_full = false;
        running.metrics.uncommitted.set(0);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        let Some(running) = self.running.take() else {
            return Ok(());
        };
        running.worker.stop(CLOSE_TIMEOUT).await
    }
}

#[cfg(test)]
mod tests;
