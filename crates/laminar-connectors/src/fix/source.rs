//! `SourceConnector` implementation: one FIX initiator session per source.

use std::sync::Arc;
use std::time::Duration;

use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use tokio::sync::Notify;

use super::capture::{Capture, SessionStatus};
use super::config::{FixSourceConfig, SequenceResetPolicy, Transport};
use super::engine::{Application, Initiator, Settings};
use super::journal::{Journal, JournalReader, Record};
use super::metrics::FixMetrics;
use super::projection::{compile, Projection};
use super::tls::TlsBridge;
use super::{dictionary, NATIVE_FORMAT};
use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{
    ConnectorTaskGuard, ConnectorTaskOwner, ConnectorTaskTracker, SourceBatch, SourceConnector,
    SourceConsistency, SourceContract, SourceInputMode, SourcePosition, SourceReplayOrder,
    SourceRowPositionCapability, SourceStart, SourceTopology,
};
use crate::error::ConnectorError;
use crate::schema::resolution::{
    bind_external, logical_binding, NativeSchema, SchemaBinding, SchemaDirection, SchemaOrigin,
};

const CHECKPOINT_CONNECTOR: &str = "fix";
const CHECKPOINT_VERSION: &str = "1";
const OFFSET_NEXT: &str = "journal.next";
/// Upper bound on journal bytes projected by one poll, independent of `max_records`.
const MAX_POLL_BYTES: usize = 8 * 1024 * 1024;
/// Bound on waiting for the engine's graceful Logout (the engine itself waits at most ten
/// seconds for the counterparty's response).
const GRACEFUL_STOP: Duration = Duration::from_secs(12);

/// FIX initiator source: captures selected application messages into a durable journal and
/// emits them as Arrow rows.
pub struct FixSource {
    registry: Option<Arc<prometheus::Registry>>,
    schema: SchemaRef,
    data_ready: Arc<Notify>,
    task_owner: ConnectorTaskOwner,
    task_tracker: ConnectorTaskTracker,
    running: Option<Running>,
}

struct Engine {
    initiator: Option<Initiator>,
    tls: Option<TlsBridge>,
    guard: Option<ConnectorTaskGuard>,
}

impl Engine {
    /// Stop and delete the initiator on a dedicated thread that owns the task guard, so joining
    /// engine threads never blocks an async worker and the journal lock is released only after
    /// the last callback returned.
    fn spawn_stop(&mut self, force: bool) -> Option<tokio::sync::oneshot::Receiver<()>> {
        let initiator = self.initiator.take()?;
        let guard = self.guard.take();
        let (done, finished) = tokio::sync::oneshot::channel();
        let stop: Box<dyn FnOnce() + Send> = Box::new(move || {
            if let Err(error) = initiator.stop(force) {
                tracing::warn!(error = %error, "FIX engine stop failed");
            }
            drop(initiator);
            drop(guard);
            let _ = done.send(());
        });
        let stop = Arc::new(parking_lot::Mutex::new(Some(stop)));
        let threaded = stop.clone();
        let spawned = std::thread::Builder::new()
            .name("laminardb-fix-engine-stop".into())
            .spawn(move || {
                let stop = threaded.lock().take();
                if let Some(stop) = stop {
                    stop();
                }
            });
        if let Err(error) = spawned {
            tracing::error!(error = %error, "cannot spawn the FIX engine stop thread; stopping inline");
            let stop = stop.lock().take();
            if let Some(stop) = stop {
                stop();
            }
        }
        Some(finished)
    }

    /// Log out gracefully within `GRACEFUL_STOP`, then close the TLS bridge.
    async fn stop(mut self) {
        if let Some(finished) = self.spawn_stop(false) {
            if tokio::time::timeout(GRACEFUL_STOP, finished).await.is_err() {
                tracing::warn!(
                    "FIX engine logout did not finish in time; its stop thread continues"
                );
            }
        }
        if let Some(tls) = self.tls.take() {
            tls.stop().await;
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.spawn_stop(true);
    }
}

struct Running {
    journal: Arc<Journal>,
    reader: JournalReader,
    projection: Projection,
    status: Arc<SessionStatus>,
    metrics: Arc<FixMetrics>,
    on_sequence_reset: SequenceResetPolicy,
    engine: Option<Engine>,
}

impl FixSource {
    /// Create an unstarted source. Construction performs no I/O.
    #[must_use]
    pub fn new(registry: Option<Arc<prometheus::Registry>>) -> Self {
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        Self {
            registry,
            schema: Arc::new(Schema::empty()),
            data_ready: Arc::new(Notify::new()),
            task_owner,
            task_tracker,
            running: None,
        }
    }

    fn guard(&self) -> Result<ConnectorTaskGuard, ConnectorError> {
        self.task_owner
            .track()
            .ok_or_else(|| ConnectorError::Internal("FIX source generation is retired".into()))
    }

    fn checkpoint_for(journal: &Journal, next: u64) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset(OFFSET_NEXT, next.to_string());
        checkpoint.set_metadata("connector", CHECKPOINT_CONNECTOR);
        checkpoint.set_metadata("checkpoint.version", CHECKPOINT_VERSION);
        checkpoint.set_metadata("journal.id", journal.meta().journal_id.clone());
        checkpoint.set_metadata("capture.contract", journal.meta().capture_contract.clone());
        checkpoint
    }

    fn checkpoint_index(
        journal: &Journal,
        checkpoint: &SourceCheckpoint,
    ) -> Result<u64, ConnectorError> {
        let meta = journal.meta();
        let matches = checkpoint.get_metadata("connector") == Some(CHECKPOINT_CONNECTOR)
            && checkpoint.get_metadata("checkpoint.version") == Some(CHECKPOINT_VERSION)
            && checkpoint.get_metadata("journal.id") == Some(meta.journal_id.as_str())
            && checkpoint.get_metadata("capture.contract") == Some(meta.capture_contract.as_str());
        if !matches {
            return Err(ConnectorError::ConfigurationError(
                "FIX source checkpoint belongs to a different journal or capture contract; the \
                 journal was replaced or the profile changed incompatibly"
                    .into(),
            ));
        }
        checkpoint
            .get_offset(OFFSET_NEXT)
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "FIX source checkpoint has no journal position".into(),
                )
            })
    }

    fn running(&mut self) -> Result<&mut Running, ConnectorError> {
        self.running
            .as_mut()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "started".into(),
                actual: "not started".into(),
            })
    }
}

fn committed_dictionary(binding: Option<&SchemaBinding>) -> Option<String> {
    let native = binding?.value.as_ref()?;
    if native.format != NATIVE_FORMAT {
        return None;
    }
    native.identity.get("dictionary_sha256").cloned()
}

fn terminal(reason: String) -> ConnectorError {
    ConnectorError::InvalidState {
        expected: "FIX capture running".into(),
        actual: reason,
    }
}

#[async_trait]
impl SourceConnector for FixSource {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        FixSourceConfig::parse(config)?;
        // Journal retention advances only on committed checkpoints. One session is one ordered
        // channel; cluster placement is rejected because the journal is node-local.
        Ok(SourceContract::new(
            SourceConsistency::CommitCoupled,
            SourceTopology::Singleton,
            SourceInputMode::AppendOnly,
        )
        .with_row_positions(SourceRowPositionCapability::OrderedDeterministic)
        .with_replay_order(SourceReplayOrder::SingleChannel))
    }

    fn recovery_identity_options(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Option<std::collections::BTreeMap<String, String>>, ConnectorError> {
        Ok(Some(FixSourceConfig::parse(config)?.recovery_identity()))
    }

    async fn resolve_schema(
        &mut self,
        config: &ConnectorConfig,
        explicit: Option<SchemaRef>,
    ) -> Result<SchemaBinding, ConnectorError> {
        let schema = explicit.ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "FIX sources need declared columns mapped through 'column.<name>' options".into(),
            )
        })?;
        let parsed = FixSourceConfig::parse(config)?;
        let (_, sha256) = dictionary::read(&parsed.dictionary_path)?;
        let path = parsed.dictionary_path.display().to_string();
        let dictionary = super::engine::DataDictionary::from_path(&path)?;
        let plan = compile(config, &schema, &dictionary, &parsed.messages)?;
        let mut binding = logical_binding(
            config,
            SchemaDirection::Source,
            SchemaOrigin::Explicit,
            &schema,
        )?;
        bind_external(&mut binding, &schema)?;
        binding.value = Some(NativeSchema {
            format: NATIVE_FORMAT.into(),
            identity: std::collections::BTreeMap::from([
                ("dictionary_sha256".into(), sha256),
                ("session".into(), parsed.session.display()),
            ]),
            definition: plan.canonical(),
            references: Vec::new(),
        });
        Ok(binding)
    }

    #[allow(clippy::too_many_lines)] // INVARIANT: one ordered activation transaction: pin, open, position, start.
    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (config, position, _delivery) = request.into_parts();
        let parsed = FixSourceConfig::parse(&config)?;
        let schema = config.arrow_schema().ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "FIX source started without its declared schema".into(),
            )
        })?;
        let sha256 = match committed_dictionary(config.schema_binding()) {
            Some(sha256) => sha256,
            None => dictionary::read(&parsed.dictionary_path)?.1,
        };
        let pinned = dictionary::pin(&parsed.journal_path, &sha256, &parsed.dictionary_path)?;
        let pinned_text = pinned.display().to_string();
        let projection_dictionary =
            Arc::new(super::engine::DataDictionary::from_path(&pinned_text)?);
        let plan = compile(&config, &schema, &projection_dictionary, &parsed.messages)?;
        let session = parsed.session.display();
        let journal = Arc::new(Journal::open(
            &parsed.journal_path,
            &session,
            &parsed.capture_contract(&sha256),
            parsed.journal,
            self.data_ready.clone(),
        )?);
        let start_index = match &position {
            SourcePosition::Initial => journal.first_retained_index(),
            SourcePosition::Resume { checkpoint, .. } => {
                Self::checkpoint_index(&journal, checkpoint)?
            }
            SourcePosition::Initialized { .. } => {
                return Err(ConnectorError::FeatureUnsupported(
                    "FIX sources have no sealed topology initialization contract".into(),
                ))
            }
        };
        let reader = journal.reader(start_index)?;
        let source_name = config.get("laminar.source.name").unwrap_or("fix");
        let metrics = Arc::new(match &self.registry {
            Some(registry) => FixMetrics::registered(source_name, registry)?,
            None => FixMetrics::local(source_name)?,
        });
        metrics
            .journal_retained_bytes
            .set(i64::try_from(journal.retained_bytes()).unwrap_or(i64::MAX));
        let status = Arc::new(SessionStatus::default());

        let tls = match &parsed.transport {
            Transport::Tcp => None,
            Transport::Tls(settings) => {
                Some(TlsBridge::start(settings, &parsed.host, parsed.port, self.guard()?).await?)
            }
        };
        let connect = match &tls {
            Some(bridge) => ("127.0.0.1".to_string(), bridge.local_port),
            None => (parsed.host.clone(), parsed.port),
        };
        let store_dir = parsed.journal_path.join("engine");
        laminar_core::durable_fs::ensure_durable_directory(&store_dir)?;
        let defaults = parsed.engine_defaults(&pinned, &store_dir, (&connect.0, connect.1));
        let settings = Settings::single_session(&defaults, &parsed.session, &[])?;
        let capture = Arc::new(Capture::new(
            journal.clone(),
            parsed.session.clone(),
            parsed.messages.iter().cloned().collect(),
            parsed.credentials.clone(),
            parsed.max_message_bytes,
            status.clone(),
            metrics.clone(),
        ));
        let initiator = Initiator::new(settings, Application::new(capture)?)?;
        initiator.start()?;
        let engine = Engine {
            initiator: Some(initiator),
            tls,
            guard: Some(self.guard()?),
        };
        tracing::info!(
            session = %session,
            start_index,
            durability = ?journal.sync_policy(),
            "FIX source started"
        );
        self.schema = schema;
        self.running = Some(Running {
            projection: Projection::new(
                plan,
                projection_dictionary,
                Arc::from(session),
                parsed.max_rows_per_message,
            ),
            journal,
            reader,
            status,
            metrics,
            on_sequence_reset: parsed.on_sequence_reset,
            engine: Some(engine),
        });
        self.data_ready.notify_one();
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let data_ready = self.data_ready.clone();
        let running = self.running()?;
        if let Some(reason) = running.status.fatal() {
            return Err(terminal(reason));
        }
        let end = running.journal.durable_end();
        let mut bytes = 0_usize;
        while running.projection.rows() < max_records.max(1) && bytes < MAX_POLL_BYTES {
            let Some((index, record)) = running.reader.next(&end)? else {
                break;
            };
            match record {
                Record::Application(application) => {
                    bytes += application.message.len();
                    if let Err(error) = running.projection.push(index, &application) {
                        running.projection.reset();
                        running.metrics.projection_errors.inc();
                        return Err(error.into());
                    }
                }
                Record::SequenceReset {
                    from_seq, new_seq, ..
                } => {
                    running.metrics.feed_incomplete.set(1);
                    match running.on_sequence_reset {
                        SequenceResetPolicy::Fail => {
                            running.projection.reset();
                            return Err(terminal(format!(
                                "counterparty skipped inbound sequence numbers {from_seq}..{new_seq} with \
                                 SequenceReset-Reset; that history is unrecoverable. Reconcile it, then set \
                                 'on.sequence.reset' = 'continue' to proceed past journal record {index}"
                            )));
                        }
                        SequenceResetPolicy::Continue => running.projection.mark_gap(),
                    }
                }
                Record::SessionSnapshot(_)
                | Record::SeriesStarted { .. }
                | Record::GapFill { .. }
                | Record::Rejected { .. } => {}
            }
        }
        let unread = end.next_index.saturating_sub(running.reader.next_index());
        running
            .metrics
            .journal_unread_records
            .set(i64::try_from(unread).unwrap_or(i64::MAX));
        if unread > 0 {
            data_ready.notify_one();
        }
        if running.projection.rows() == 0 {
            return Ok(None);
        }
        let (records, positions) = running.projection.finish()?;
        running
            .metrics
            .rows_emitted
            .inc_by(records.num_rows() as u64);
        let checkpoint = Self::checkpoint_for(&running.journal, running.reader.next_index());
        Ok(Some(
            SourceBatch::positioned(records, positions)?.with_checkpoint(checkpoint),
        ))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        match &self.running {
            Some(running) => Self::checkpoint_for(&running.journal, running.reader.next_index()),
            None => SourceCheckpoint::new(),
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(mut running) = self.running.take() {
            if let Some(engine) = running.engine.take() {
                engine.stop().await;
            }
            tracing::info!(
                session = %running.journal.meta().session,
                next = running.reader.next_index(),
                "FIX source closed"
            );
        }
        Ok(())
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(self.data_ready.clone())
    }

    async fn notify_epoch_committed(
        &mut self,
        _epoch: u64,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        if checkpoint.is_empty() {
            return Ok(());
        }
        let running = self.running()?;
        let committed = Self::checkpoint_index(&running.journal, checkpoint)?;
        running.journal.release_before(committed)?;
        running
            .metrics
            .journal_retained_bytes
            .set(i64::try_from(running.journal.retained_bytes()).unwrap_or(i64::MAX));
        Ok(())
    }
}
