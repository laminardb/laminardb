//! `SourceConnector` implementation: one FIX initiator session per source.

use std::sync::Arc;
use std::time::Duration;

use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use tokio::sync::Notify;

use super::capture::{Capture, SessionStatus};
use super::config::{FixSourceConfig, SequenceResetPolicy, Transport};
use super::dictionary::DictionaryDigests;
use super::engine::{Application, Initiator, Settings, StoreFactory};
use super::journal::{GroupCommit, Journal, JournalReader, Record};
use super::metrics::{saturating, FixMetrics};
use super::projection::{compile, Projection};
use super::session_store::SessionStore;
use super::tls::TlsBridge;
use super::venue::Logon;
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
    store: Arc<SessionStore>,
    _group_commit: Option<GroupCommit>,
    /// Cursor metadata and input-channel inventory shared by every checkpoint of this run.
    cursor: SourceCheckpoint,
    reader: JournalReader,
    projection: Projection,
    status: Arc<SessionStatus>,
    metrics: Arc<FixMetrics>,
    on_sequence_reset: SequenceResetPolicy,
    /// Whether the counterparty retransmits what it sent while the session was down.
    retransmits: bool,
    /// Journal index of the latest committed checkpoint.
    committed: u64,
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

    /// The session is the source's one input channel, matching the row-position partition.
    fn cursor_template(journal: &Journal) -> Result<SourceCheckpoint, ConnectorError> {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_metadata("connector", CHECKPOINT_CONNECTOR);
        checkpoint.set_metadata("checkpoint.version", CHECKPOINT_VERSION);
        checkpoint.set_metadata("journal.id", journal.meta().journal_id.clone());
        checkpoint.set_metadata("capture.contract", journal.meta().capture_contract.clone());
        checkpoint.set_input_channels(vec![journal.meta().session.as_bytes().to_vec()])?;
        Ok(checkpoint)
    }

    fn checkpoint_for(template: &SourceCheckpoint, next: u64) -> SourceCheckpoint {
        let mut checkpoint = template.clone();
        checkpoint.set_offset(OFFSET_NEXT, next.to_string());
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

fn committed_digests(binding: Option<&SchemaBinding>) -> Option<DictionaryDigests> {
    let native = binding?.value.as_ref()?;
    if native.format != NATIVE_FORMAT {
        return None;
    }
    Some(DictionaryDigests {
        application: native.identity.get("dictionary_sha256")?.clone(),
        transport: native.identity.get("transport_dictionary_sha256").cloned(),
    })
}

/// Open the journal and the session store whose persisted inbound progress it bounds.
fn open_durable_state(
    parsed: &FixSourceConfig,
    digests: &DictionaryDigests,
    data_ready: Arc<Notify>,
) -> Result<(Arc<Journal>, Arc<SessionStore>), ConnectorError> {
    let journal = Arc::new(Journal::open(
        &parsed.journal_path,
        &parsed.session.display(),
        &parsed.capture_contract(digests),
        parsed.journal,
        data_ready,
    )?);
    let barrier_journal = Arc::downgrade(&journal);
    let store = Arc::new(SessionStore::open(
        &parsed.journal_path,
        parsed.journal.sync,
        Box::new(move || match barrier_journal.upgrade() {
            Some(journal) => journal.sync(),
            None => Err(terminal("FIX journal closed".into())),
        }),
    )?);
    journal.attach_floor(store.clone())?;
    Ok((journal, store))
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
        // channel; cluster placement is rejected because the journal is node-local. The barrier
        // cut is exact: a cursor is a journal index, every emitted record is durable, and one
        // FIX message is one atomic record with no upstream transaction spanning the cut.
        Ok(SourceContract::new(
            SourceConsistency::CommitCoupled,
            SourceTopology::Singleton,
            SourceInputMode::AppendOnly,
        )
        .with_row_positions(SourceRowPositionCapability::OrderedDeterministic)
        .with_replay_order(SourceReplayOrder::SingleChannel)
        .with_exact_delivery_certification())
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
        let files = parsed.dictionary_files();
        let digests = dictionary::digests(&files)?;
        let dictionaries = dictionary::load(&parsed, &files)?;
        let plan = compile(config, &schema, &dictionaries, &parsed.messages)?;
        let mut binding = logical_binding(
            config,
            SchemaDirection::Source,
            SchemaOrigin::Explicit,
            &schema,
        )?;
        bind_external(&mut binding, &schema)?;
        let mut identity = std::collections::BTreeMap::from([
            ("dictionary_sha256".into(), digests.application),
            ("session".into(), parsed.session.display()),
        ]);
        if let Some(transport) = digests.transport {
            identity.insert("transport_dictionary_sha256".into(), transport);
        }
        binding.value = Some(NativeSchema {
            format: NATIVE_FORMAT.into(),
            identity,
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
        let files = parsed.dictionary_files();
        let digests = match committed_digests(config.schema_binding()) {
            Some(digests) => digests,
            None => dictionary::digests(&files)?,
        };
        let pinned = dictionary::pin_all(&parsed.journal_path, &digests, &files)?;
        let dictionaries = Arc::new(dictionary::load(&parsed, &pinned)?);
        let plan = compile(&config, &schema, &dictionaries, &parsed.messages)?;
        let logon = Logon::load(&parsed)?;
        let session = parsed.session.display();
        let (journal, store) = open_durable_state(&parsed, &digests, self.data_ready.clone())?;
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
        let replay = journal.durable_end().next_index.saturating_sub(start_index);
        let cursor = Self::cursor_template(&journal)?;
        let source_name = config.get("laminar.source.name").unwrap_or("fix");
        let metrics = Arc::new(match &self.registry {
            Some(registry) => FixMetrics::registered(source_name, registry)?,
            None => FixMetrics::local(source_name)?,
        });
        metrics
            .journal_retained_bytes
            .set(saturating(journal.retained_bytes()));
        metrics.startup_replay_records.set(saturating(replay));
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
        let defaults = parsed.engine_defaults(&pinned, (&connect.0, connect.1));
        let settings = Settings::single_session(&defaults, &parsed.session, &[])?;
        let capture = Arc::new(Capture::new(
            journal.clone(),
            store.clone(),
            &parsed,
            logon,
            status.clone(),
            metrics.clone(),
        ));
        let guard = self.guard()?;
        let group_commit = journal.start_group_commit()?;
        let initiator = Initiator::new(
            settings,
            Application::new(capture)?,
            StoreFactory::owned(store.clone())?,
        )?;
        initiator.start()?;
        let engine = Engine {
            initiator: Some(initiator),
            tls,
            guard: Some(guard),
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
                dictionaries,
                Arc::from(session),
                parsed.max_rows_per_message,
            ),
            journal,
            store,
            _group_commit: group_commit,
            cursor,
            reader,
            status,
            metrics,
            on_sequence_reset: parsed.on_sequence_reset,
            retransmits: parsed.venue.retransmits(),
            committed: start_index,
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
        let stopped = running
            .status
            .fatal()
            .or_else(|| running.journal.poisoned())
            .or_else(|| running.store.poisoned());
        if let Some(reason) = stopped {
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
                Record::SeriesStarted { series, .. } if !running.retransmits && series > 1 => {
                    running.metrics.feed_incomplete.set(1);
                    running.projection.mark_gap();
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
            .checkpoint_lag_records
            .set(saturating(end.next_index.saturating_sub(running.committed)));
        running
            .metrics
            .journal_unread_records
            .set(saturating(unread));
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
        let checkpoint = Self::checkpoint_for(&running.cursor, running.reader.next_index());
        Ok(Some(
            SourceBatch::positioned(records, positions)?.with_checkpoint(checkpoint),
        ))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        match &self.running {
            Some(running) => Self::checkpoint_for(&running.cursor, running.reader.next_index()),
            None => SourceCheckpoint::new(),
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(mut running) = self.running.take() {
            if let Some(engine) = running.engine.take() {
                engine.stop().await;
            }
            // Lets the session store persist inbound progress up to the last record.
            if let Err(error) = running.journal.sync() {
                tracing::warn!(error = %error, "FIX journal sync on close failed");
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
        running.committed = running.committed.max(committed);
        running
            .metrics
            .journal_retained_bytes
            .set(saturating(running.journal.retained_bytes()));
        Ok(())
    }
}
