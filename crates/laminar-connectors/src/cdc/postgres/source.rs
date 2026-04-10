//! `PostgreSQL` CDC source connector implementation.
//!
//! Implements [`SourceConnector`] for streaming logical replication changes
//! from `PostgreSQL` into `LaminarDB` as Arrow `RecordBatch`es.
//!
//! # Architecture
//!
//! - **Ring 0**: No CDC code — just SPSC channel pop (~5ns)
//! - **Ring 1**: WAL consumption, pgoutput parsing, Arrow conversion
//! - **Ring 2**: Slot management, schema discovery, health checks

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Notify;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{PartitionInfo, SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::changelog::{events_to_record_batch, tuple_to_json, CdcOperation, ChangeEvent};
use super::config::PostgresCdcConfig;
use super::decoder::{decode_message, WalMessage};
use super::lsn::Lsn;
use super::metrics::PostgresCdcMetrics;
use super::schema::{cdc_envelope_schema, RelationCache, RelationInfo};
/// `PostgreSQL` CDC source connector.
///
/// Streams row-level changes from `PostgreSQL` using logical replication
/// (`pgoutput` plugin). Changes are emitted as Arrow `RecordBatch`es
/// in the CDC envelope format.
///
/// # Envelope Schema
///
/// | Column   | Type   | Nullable | Description                    |
/// |----------|--------|----------|--------------------------------|
/// | `_table` | Utf8   | no       | Schema-qualified table name    |
/// | `_op`    | Utf8   | no       | Operation: I, U, D             |
/// | `_lsn`   | UInt64 | no       | WAL position                   |
/// | `_ts_ms` | Int64  | no       | Commit timestamp (Unix ms)     |
/// | `_before`| Utf8   | yes      | Old row JSON (for U, D)        |
/// | `_after` | Utf8   | yes      | New row JSON (for I, U)        |
pub struct PostgresCdcSource {
    /// Connector configuration.
    config: PostgresCdcConfig,

    /// Current lifecycle state.
    state: ConnectorState,

    /// Output schema (CDC envelope).
    schema: SchemaRef,

    /// Lock-free metrics.
    metrics: Arc<PostgresCdcMetrics>,

    /// Cached relation (table) schemas from Relation messages.
    relation_cache: RelationCache,

    /// Buffered change events awaiting `poll_batch()`.
    event_buffer: VecDeque<ChangeEvent>,

    /// Current transaction state.
    current_txn: Option<TransactionState>,

    /// Confirmed flush LSN (last acknowledged position).
    confirmed_flush_lsn: Lsn,

    /// Write LSN (latest position received from server).
    write_lsn: Lsn,

    /// Polled LSN — tracks the latest position drained into a batch.
    /// Decoupled from `confirmed_flush_lsn` so the PG replication slot
    /// is only advanced when the pipeline actually checkpoints.
    polled_lsn: Lsn,

    /// Pending WAL messages to process (for testing and batch processing).
    pending_messages: VecDeque<Vec<u8>>,

    /// Notification handle signalled when WAL data arrives from the reader task.
    data_ready: Arc<Notify>,

    /// Background connection task handle (feature-gated).
    #[cfg(feature = "postgres-cdc")]
    connection_handle: Option<tokio::task::JoinHandle<()>>,

    /// Channel receiver for WAL events from the background reader task.
    #[cfg(feature = "postgres-cdc")]
    wal_rx: Option<WalPayloadRx>,

    /// Background WAL reader task handle.
    #[cfg(feature = "postgres-cdc")]
    reader_handle: Option<tokio::task::JoinHandle<()>>,

    /// Shutdown signal for the background reader task.
    #[cfg(feature = "postgres-cdc")]
    reader_shutdown: Option<tokio::sync::watch::Sender<bool>>,

    /// Sender for feeding confirmed flush LSN back to the reader task.
    /// The reader uses this to call `update_applied_lsn` only for
    /// durably-checkpointed positions (prevents at-least-once violation).
    #[cfg(feature = "postgres-cdc")]
    confirmed_lsn_tx: Option<tokio::sync::watch::Sender<u64>>,
}

/// In-progress transaction state.
#[derive(Debug, Clone)]
struct TransactionState {
    /// Final LSN of the transaction.
    final_lsn: Lsn,
    /// Commit timestamp in milliseconds.
    commit_ts_ms: i64,
    /// Change events accumulated in this transaction.
    events: Vec<ChangeEvent>,
}

/// Single-consumer async receiver for the WAL reader → `poll_batch` queue.
#[cfg(feature = "postgres-cdc")]
type WalPayloadRx = crossfire::AsyncRx<crossfire::mpsc::Array<WalPayload>>;

/// WAL event payload sent from the background reader task to [`PostgresCdcSource::poll_batch`].
#[allow(dead_code)] // constructed + consumed only with feature = "postgres-cdc"
enum WalPayload {
    Begin {
        final_lsn: u64,
        commit_ts_us: i64,
        xid: u32,
    },
    Commit {
        end_lsn: u64,
        commit_ts_us: i64,
        lsn: u64,
    },
    XLogData {
        wal_end: u64,
        data: Vec<u8>,
    },
    KeepAlive {
        wal_end: u64,
    },
    /// Fatal error from the reader task (e.g. reconnect exhaustion).
    Error(String),
}

impl PostgresCdcSource {
    /// Creates a new `PostgreSQL` CDC source with the given configuration.
    #[must_use]
    pub fn new(config: PostgresCdcConfig) -> Self {
        Self {
            config,
            state: ConnectorState::Created,
            schema: cdc_envelope_schema(),
            metrics: Arc::new(PostgresCdcMetrics::new()),
            relation_cache: RelationCache::new(),
            event_buffer: VecDeque::new(),
            current_txn: None,
            confirmed_flush_lsn: Lsn::ZERO,
            write_lsn: Lsn::ZERO,
            polled_lsn: Lsn::ZERO,
            pending_messages: VecDeque::new(),
            data_ready: Arc::new(Notify::new()),
            #[cfg(feature = "postgres-cdc")]
            connection_handle: None,
            #[cfg(feature = "postgres-cdc")]
            wal_rx: None,
            #[cfg(feature = "postgres-cdc")]
            reader_handle: None,
            #[cfg(feature = "postgres-cdc")]
            reader_shutdown: None,
            #[cfg(feature = "postgres-cdc")]
            confirmed_lsn_tx: None,
        }
    }

    /// Creates a new source from a generic [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the configuration is invalid.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let pg_config = PostgresCdcConfig::from_config(config)?;
        Ok(Self::new(pg_config))
    }

    /// Returns a reference to the CDC configuration.
    #[must_use]
    pub fn config(&self) -> &PostgresCdcConfig {
        &self.config
    }

    /// Returns the current confirmed flush LSN.
    #[must_use]
    pub fn confirmed_flush_lsn(&self) -> Lsn {
        self.confirmed_flush_lsn
    }

    /// Returns the current write LSN.
    #[must_use]
    pub fn write_lsn(&self) -> Lsn {
        self.write_lsn
    }

    /// Returns the current replication lag in bytes.
    #[must_use]
    pub fn replication_lag_bytes(&self) -> u64 {
        self.write_lsn.diff(self.confirmed_flush_lsn)
    }

    /// Returns a reference to the relation cache.
    #[must_use]
    pub fn relation_cache(&self) -> &RelationCache {
        &self.relation_cache
    }

    /// Returns the number of buffered events.
    #[must_use]
    pub fn buffered_events(&self) -> usize {
        self.event_buffer.len()
    }

    /// Enqueues raw WAL message bytes for processing.
    ///
    /// Used by the replication stream handler to feed data into the source,
    /// and by tests to inject synthetic messages.
    pub fn enqueue_wal_data(&mut self, data: Vec<u8>) {
        self.pending_messages.push_back(data);
    }

    /// Processes all pending WAL messages, converting them to change events.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ReadError` if decoding fails.
    pub fn process_pending_messages(&mut self) -> Result<(), ConnectorError> {
        while let Some(data) = self.pending_messages.pop_front() {
            self.metrics.record_bytes(data.len() as u64);
            let msg = decode_message(&data)
                .map_err(|e| ConnectorError::ReadError(format!("pgoutput decode: {e}")))?;
            self.process_wal_message(msg)?;
        }
        Ok(())
    }

    /// Processes a single decoded WAL message.
    fn process_wal_message(&mut self, msg: WalMessage) -> Result<(), ConnectorError> {
        match msg {
            WalMessage::Begin(begin) => {
                self.current_txn = Some(TransactionState {
                    final_lsn: begin.final_lsn,
                    commit_ts_ms: begin.commit_ts_ms,
                    events: Vec::new(),
                });
            }
            WalMessage::Commit(commit) => {
                if let Some(txn) = self.current_txn.take() {
                    self.event_buffer.extend(txn.events);
                    self.write_lsn = commit.end_lsn;
                    self.metrics.record_transaction();
                    self.metrics
                        .set_replication_lag_bytes(self.replication_lag_bytes());
                }
            }
            WalMessage::Relation(rel) => {
                let info = RelationInfo {
                    relation_id: rel.relation_id,
                    namespace: rel.namespace,
                    name: rel.name,
                    replica_identity: rel.replica_identity as char,
                    columns: rel.columns,
                };
                self.relation_cache.insert(info);
            }
            WalMessage::Insert(ins) => {
                self.process_insert(ins.relation_id, &ins.new_tuple)?;
            }
            WalMessage::Update(upd) => {
                self.process_update(upd.relation_id, upd.old_tuple.as_ref(), &upd.new_tuple)?;
            }
            WalMessage::Delete(del) => {
                self.process_delete(del.relation_id, &del.old_tuple)?;
            }
            WalMessage::Truncate(trunc) => {
                let table_names: Vec<String> = trunc
                    .relation_ids
                    .iter()
                    .map(|id| {
                        self.relation_cache
                            .get(*id)
                            .map_or_else(|| format!("oid:{id}"), RelationInfo::full_name)
                    })
                    .collect();
                return Err(ConnectorError::ReadError(format!(
                    "TRUNCATE received on table(s): {}. \
                     Cannot produce retraction events — restart the pipeline with a fresh snapshot.",
                    table_names.join(", ")
                )));
            }
            WalMessage::Origin(_) | WalMessage::Type(_) => {
                // Origin and Type messages are noted but don't
                // produce change events in the current implementation.
            }
        }
        Ok(())
    }

    fn process_insert(
        &mut self,
        relation_id: u32,
        new_tuple: &super::decoder::TupleData,
    ) -> Result<(), ConnectorError> {
        let relation = self.require_relation(relation_id)?;
        let table = relation.full_name();

        if !self.config.should_include_table(&table) {
            return Ok(());
        }

        let after_json = tuple_to_json(new_tuple, relation);
        let (lsn, ts_ms) = self.current_txn_context();

        let event = ChangeEvent {
            table,
            op: CdcOperation::Insert,
            lsn,
            ts_ms,
            before: None,
            after: Some(after_json),
        };

        self.push_event(event);
        self.metrics.record_insert();
        Ok(())
    }

    fn process_update(
        &mut self,
        relation_id: u32,
        old_tuple: Option<&super::decoder::TupleData>,
        new_tuple: &super::decoder::TupleData,
    ) -> Result<(), ConnectorError> {
        let relation = self.require_relation(relation_id)?;
        let table = relation.full_name();

        if !self.config.should_include_table(&table) {
            return Ok(());
        }

        let before_json = old_tuple.map(|t| tuple_to_json(t, relation));
        let after_json = tuple_to_json(new_tuple, relation);
        let (lsn, ts_ms) = self.current_txn_context();

        let event = ChangeEvent {
            table,
            op: CdcOperation::Update,
            lsn,
            ts_ms,
            before: before_json,
            after: Some(after_json),
        };

        self.push_event(event);
        self.metrics.record_update();
        Ok(())
    }

    fn process_delete(
        &mut self,
        relation_id: u32,
        old_tuple: &super::decoder::TupleData,
    ) -> Result<(), ConnectorError> {
        let relation = self.require_relation(relation_id)?;
        let table = relation.full_name();

        if !self.config.should_include_table(&table) {
            return Ok(());
        }

        let before_json = tuple_to_json(old_tuple, relation);
        let (lsn, ts_ms) = self.current_txn_context();

        let event = ChangeEvent {
            table,
            op: CdcOperation::Delete,
            lsn,
            ts_ms,
            before: Some(before_json),
            after: None,
        };

        self.push_event(event);
        self.metrics.record_delete();
        Ok(())
    }

    /// Looks up a relation by ID, returning a reference (no clone).
    ///
    /// The caller must extract all needed data (table name, JSON) from
    /// the reference before calling `push_event` or other `&mut self`
    /// methods (Rust's borrow rules require disjoint access).
    fn require_relation(&self, relation_id: u32) -> Result<&RelationInfo, ConnectorError> {
        self.relation_cache.get(relation_id).ok_or_else(|| {
            ConnectorError::ReadError(format!(
                "unknown relation ID {relation_id} (no Relation message received yet)"
            ))
        })
    }

    fn current_txn_context(&self) -> (Lsn, i64) {
        match &self.current_txn {
            Some(txn) => (txn.final_lsn, txn.commit_ts_ms),
            None => (self.write_lsn, 0),
        }
    }

    fn push_event(&mut self, event: ChangeEvent) {
        if let Some(txn) = &mut self.current_txn {
            txn.events.push(event);
        } else {
            self.event_buffer.push_back(event);
        }
    }

    /// Processes a [`WalPayload`] received from the background reader task.
    #[cfg(feature = "postgres-cdc")]
    fn process_wal_payload(&mut self, payload: WalPayload) -> Result<(), ConnectorError> {
        use super::decoder::pg_timestamp_to_unix_ms;

        match payload {
            WalPayload::Begin {
                final_lsn,
                commit_ts_us,
                xid,
            } => {
                let begin = super::decoder::BeginMessage {
                    final_lsn: Lsn::new(final_lsn),
                    commit_ts_ms: pg_timestamp_to_unix_ms(commit_ts_us),
                    xid,
                };
                self.process_wal_message(WalMessage::Begin(begin))
            }
            WalPayload::Commit {
                end_lsn,
                commit_ts_us,
                lsn,
            } => {
                let commit = super::decoder::CommitMessage {
                    flags: 0,
                    commit_lsn: Lsn::new(lsn),
                    end_lsn: Lsn::new(end_lsn),
                    commit_ts_ms: pg_timestamp_to_unix_ms(commit_ts_us),
                };
                self.process_wal_message(WalMessage::Commit(commit))
            }
            WalPayload::XLogData { wal_end, data } => {
                // Skip raw Begin/Commit bytes to avoid double-processing
                // (pgwire-replication delivers these as separate events).
                if !data.is_empty() && (data[0] == b'B' || data[0] == b'C') {
                    self.write_lsn = Lsn::new(wal_end);
                    return Ok(());
                }
                let msg = decode_message(&data)
                    .map_err(|e| ConnectorError::ReadError(format!("pgoutput decode: {e}")))?;
                self.process_wal_message(msg)?;
                self.write_lsn = Lsn::new(wal_end);
                Ok(())
            }
            WalPayload::KeepAlive { wal_end } => {
                self.write_lsn = Lsn::new(wal_end);
                Ok(())
            }
            WalPayload::Error(msg) => Err(ConnectorError::ReadError(msg)),
        }
    }

    /// Drains up to `max` events from the buffer and converts to a `RecordBatch`.
    fn drain_events(&mut self, max: usize) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.event_buffer.is_empty() {
            return Ok(None);
        }

        let count = max.min(self.event_buffer.len());
        let events: Vec<ChangeEvent> = self.event_buffer.drain(..count).collect();

        let batch = events_to_record_batch(&events)
            .map_err(|e| ConnectorError::Internal(format!("Arrow batch build: {e}")))?;

        self.metrics.record_batch();
        Ok(Some(batch))
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)]
impl SourceConnector for PostgresCdcSource {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        // If config has properties, re-parse (supports runtime config via SQL WITH).
        if !config.properties().is_empty() {
            self.config = PostgresCdcConfig::from_config(config)?;
        }

        // Set start LSN if configured
        if let Some(lsn) = self.config.start_lsn {
            self.confirmed_flush_lsn = lsn;
            self.write_lsn = lsn;
            self.polled_lsn = lsn;
        }

        // Without postgres-cdc feature, open() must fail loudly to prevent
        // silent data loss (poll_batch would return Ok(None) forever).
        #[cfg(not(feature = "postgres-cdc"))]
        {
            return Err(ConnectorError::ConfigurationError(
                "PostgreSQL CDC source requires the `postgres-cdc` feature flag. \
                 Rebuild with `--features postgres-cdc` to enable."
                    .to_string(),
            ));
        }

        #[cfg(all(feature = "postgres-cdc", not(test)))]
        {
            use super::postgres_io;

            // 1. Connect control-plane for slot management
            let (client, handle) = postgres_io::connect(&self.config).await?;
            self.connection_handle = Some(handle);

            // 2. Ensure replication slot exists
            let slot_lsn = postgres_io::ensure_replication_slot(
                &client,
                &self.config.slot_name,
                &self.config.output_plugin,
            )
            .await?;

            // Use slot's confirmed_flush_lsn if no explicit start LSN
            if self.config.start_lsn.is_none() {
                if let Some(lsn) = slot_lsn {
                    self.confirmed_flush_lsn = lsn;
                    self.write_lsn = lsn;
                    self.polled_lsn = lsn;
                }
            }

            // 3. Build pgwire-replication config and start WAL streaming
            let mut repl_config = postgres_io::build_replication_config(&self.config);
            // If we resolved a slot LSN, override start_lsn so we resume correctly
            if self.confirmed_flush_lsn != Lsn::ZERO {
                repl_config.start_lsn =
                    pgwire_replication::Lsn::from_u64(self.confirmed_flush_lsn.as_u64());
            }

            let repl_client = pgwire_replication::ReplicationClient::connect(repl_config)
                .await
                .map_err(|e| {
                    ConnectorError::ConnectionFailed(format!("pgwire-replication connect: {e}"))
                })?;

            // Spawn background reader task for event-driven wake-up.
            let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<WalPayload>(4096);
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
            let (confirmed_lsn_tx, mut confirmed_lsn_rx) =
                tokio::sync::watch::channel(self.confirmed_flush_lsn.as_u64());
            let data_ready = Arc::clone(&self.data_ready);
            let reader_config = self.config.clone();

            let reader_handle = tokio::spawn(async move {
                const MAX_FAILURES: u32 = 10;
                let mut repl_client = repl_client;
                let mut consecutive_failures: u32 = 0;

                'reconnect: loop {
                    // Inner recv loop — processes events from the current connection.
                    'recv: loop {
                        // Feed back confirmed LSN to PG (non-blocking check).
                        // Only positions that the main thread has checkpointed
                        // are reported as "applied", preserving at-least-once.
                        if confirmed_lsn_rx.has_changed().unwrap_or(false) {
                            let confirmed = *confirmed_lsn_rx.borrow_and_update();
                            if confirmed > 0 {
                                repl_client.update_applied_lsn(pgwire_replication::Lsn::from_u64(
                                    confirmed,
                                ));
                            }
                        }

                        tokio::select! {
                            biased;
                            _ = shutdown_rx.changed() => break 'reconnect,
                            event = repl_client.recv() => {
                                match event {
                                    Ok(Some(event)) => {
                                        consecutive_failures = 0;
                                        let payload = match &event {
                                            pgwire_replication::ReplicationEvent::Begin {
                                                final_lsn,
                                                xid,
                                                commit_time_micros,
                                            } => Some(WalPayload::Begin {
                                                final_lsn: final_lsn.as_u64(),
                                                commit_ts_us: *commit_time_micros,
                                                xid: *xid,
                                            }),
                                            pgwire_replication::ReplicationEvent::Commit {
                                                end_lsn,
                                                commit_time_micros,
                                                lsn,
                                            } => Some(WalPayload::Commit {
                                                end_lsn: end_lsn.as_u64(),
                                                commit_ts_us: *commit_time_micros,
                                                lsn: lsn.as_u64(),
                                            }),
                                            pgwire_replication::ReplicationEvent::XLogData {
                                                wal_end,
                                                data,
                                                ..
                                            } => Some(WalPayload::XLogData {
                                                wal_end: wal_end.as_u64(),
                                                data: data.to_vec(),
                                            }),
                                            pgwire_replication::ReplicationEvent::KeepAlive {
                                                wal_end,
                                                ..
                                            } => Some(WalPayload::KeepAlive {
                                                wal_end: wal_end.as_u64(),
                                            }),
                                            _ => None,
                                        };
                                        if let Some(p) = payload {
                                            if wal_tx.send(p).await.is_err() {
                                                break 'reconnect;
                                            }
                                            data_ready.notify_one();
                                        }
                                    }
                                    Ok(None) => break 'recv,
                                    Err(e) => {
                                        tracing::error!(error = %e, "WAL reader error");
                                        break 'recv;
                                    }
                                }
                            }
                        }
                    }

                    // Shut down old client before reconnecting.
                    let _ = repl_client.shutdown().await;
                    consecutive_failures += 1;

                    if consecutive_failures >= MAX_FAILURES {
                        tracing::error!(
                            failures = consecutive_failures,
                            "WAL reader exhausted reconnect attempts"
                        );
                        let _ = wal_tx.send(WalPayload::Error(format!(
                            "WAL reader failed after {consecutive_failures} consecutive reconnect attempts"
                        ))).await;
                        data_ready.notify_one();
                        break 'reconnect;
                    }

                    // Exponential backoff: 2^n seconds, capped at 30s.
                    let backoff_secs = (1u64 << consecutive_failures).min(30);
                    tracing::warn!(
                        attempt = consecutive_failures,
                        backoff_secs,
                        "WAL reader reconnecting"
                    );
                    tokio::select! {
                        biased;
                        _ = shutdown_rx.changed() => break 'reconnect,
                        () = tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)) => {}
                    }

                    // Rebuild replication config with resume LSN from checkpoint.
                    let resume_lsn = *confirmed_lsn_rx.borrow();
                    let mut new_config = postgres_io::build_replication_config(&reader_config);
                    if resume_lsn > 0 {
                        new_config.start_lsn = pgwire_replication::Lsn::from_u64(resume_lsn);
                    }

                    match pgwire_replication::ReplicationClient::connect(new_config).await {
                        Ok(new_client) => {
                            tracing::info!("WAL reader reconnected");
                            repl_client = new_client;
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "WAL reader reconnect failed");
                        }
                    }
                }
            });

            self.wal_rx = Some(wal_rx);
            self.reader_handle = Some(reader_handle);
            self.reader_shutdown = Some(shutdown_tx);
            self.confirmed_lsn_tx = Some(confirmed_lsn_tx);
        }

        self.state = ConnectorState::Running;
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "Running".to_string(),
                actual: self.state.to_string(),
            });
        }

        // Drain WAL events from background reader task.
        // Collect into a temp vec first to avoid holding a mutable
        // borrow on self.wal_rx while calling self.process_wal_payload().
        //
        // Backpressure: when the event buffer exceeds the high watermark,
        // stop draining the reader channel. The bounded mpsc channel (4096)
        // propagates backpressure to the WAL reader task, which in turn
        // applies TCP backpressure to the replication slot. This prevents
        // data loss from dropping events.
        #[cfg(feature = "postgres-cdc")]
        {
            let high_watermark = self.config.backpressure_high_watermark();
            let mut payloads = Vec::new();
            let mut reader_closed = false;

            if self.event_buffer.len() < high_watermark {
                if let Some(ref mut rx) = self.wal_rx {
                    while payloads.len() + self.event_buffer.len() < max_records
                        && self.event_buffer.len() + payloads.len() < high_watermark
                    {
                        match rx.try_recv() {
                            Ok(payload) => payloads.push(payload),
                            Err(crossfire::TryRecvError::Empty) => break,
                            Err(crossfire::TryRecvError::Disconnected) => {
                                reader_closed = true;
                                break;
                            }
                        }
                    }
                }
            } else {
                tracing::debug!(
                    buffered = self.event_buffer.len(),
                    high_watermark,
                    "CDC backpressure active — pausing WAL reader drain"
                );
            }

            for payload in payloads {
                self.process_wal_payload(payload)?;
            }
            if reader_closed && self.event_buffer.is_empty() {
                self.state = ConnectorState::Failed;
                return Err(ConnectorError::ReadError(
                    "WAL reader task terminated unexpectedly — replication stream lost".to_string(),
                ));
            }
        }

        // Process any pending WAL messages (test injection path)
        self.process_pending_messages()?;

        // Drain buffered events into a RecordBatch.
        // Watermark advancement: the batch contains `_ts_ms` (commit timestamp)
        // which downstream pipeline watermark extractors should use. The LSN
        // in PartitionInfo tracks replication progress for offset management.
        // TODO: extract max(_ts_ms) and expose as source-level watermark for
        // windowed aggregations that depend on CDC event time.
        match self.drain_events(max_records)? {
            Some(batch) => {
                // Advance polled_lsn (NOT confirmed_flush_lsn). PG slot
                // feedback happens in checkpoint(), not here.
                if self.event_buffer.is_empty() {
                    self.polled_lsn = self.write_lsn;
                }
                self.metrics
                    .set_confirmed_flush_lsn(self.confirmed_flush_lsn.as_u64());
                self.metrics
                    .set_replication_lag_bytes(self.replication_lag_bytes());

                let lsn_str = self.write_lsn.to_string();
                let partition = PartitionInfo::new(&self.config.slot_name, lsn_str);
                Ok(Some(SourceBatch::with_partition(batch, partition)))
            }
            None => Ok(None),
        }
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new(0);
        // polled_lsn = latest position drained into a batch. The PG
        // slot is only advanced here (not in poll_batch) to prevent
        // data loss on crash.
        cp.set_offset("lsn", self.polled_lsn.to_string());
        cp.set_offset("write_lsn", self.write_lsn.to_string());
        cp.set_metadata("slot_name", &self.config.slot_name);
        cp.set_metadata("publication", &self.config.publication);

        // Feed polled LSN to reader task so PG can reclaim WAL.
        // watch::Sender::send is &self, so this works from checkpoint().
        #[cfg(feature = "postgres-cdc")]
        if let Some(ref tx) = self.confirmed_lsn_tx {
            let _ = tx.send(self.polled_lsn.as_u64());
        }

        cp
    }

    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        if let Some(lsn_str) = checkpoint.get_offset("lsn") {
            let lsn: Lsn = lsn_str.parse().map_err(|e| {
                ConnectorError::CheckpointError(format!("invalid LSN in checkpoint: {e}"))
            })?;
            self.confirmed_flush_lsn = lsn;
            self.polled_lsn = lsn;
            self.metrics.set_confirmed_flush_lsn(lsn.as_u64());
        }
        if let Some(write_lsn_str) = checkpoint.get_offset("write_lsn") {
            if let Ok(lsn) = write_lsn_str.parse::<Lsn>() {
                self.write_lsn = lsn;
            }
        }
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        match self.state {
            ConnectorState::Running => {
                let lag = self.replication_lag_bytes();
                if lag > 100_000_000 {
                    // > 100MB lag
                    HealthStatus::Degraded(format!("replication lag: {lag} bytes"))
                } else {
                    HealthStatus::Healthy
                }
            }
            ConnectorState::Failed => HealthStatus::Unhealthy("connector failed".to_string()),
            _ => HealthStatus::Unknown,
        }
    }

    fn metrics(&self) -> ConnectorMetrics {
        self.metrics.to_connector_metrics()
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        // Signal reader task to shut down (it calls repl_client.shutdown() internally).
        #[cfg(feature = "postgres-cdc")]
        {
            if let Some(tx) = self.reader_shutdown.take() {
                let _ = tx.send(true);
            }
            if let Some(handle) = self.reader_handle.take() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
            }
            self.wal_rx = None;
            self.confirmed_lsn_tx = None;
        }

        // Abort the background control-plane connection task.
        #[cfg(feature = "postgres-cdc")]
        if let Some(handle) = self.connection_handle.take() {
            handle.abort();
        }

        self.state = ConnectorState::Closed;
        self.event_buffer.clear();
        self.pending_messages.clear();
        Ok(())
    }
}

// ── Test helpers ──

#[cfg(test)]
impl PostgresCdcSource {
    /// Injects a pre-built change event directly into the event buffer.
    fn inject_event(&mut self, event: ChangeEvent) {
        self.event_buffer.push_back(event);
    }

    /// Builds a binary pgoutput Relation message for testing.
    fn build_relation_message(
        relation_id: u32,
        namespace: &str,
        name: &str,
        columns: &[(u8, &str, u32, i32)], // (flags, name, oid, modifier)
    ) -> Vec<u8> {
        let mut buf = vec![b'R'];
        buf.extend_from_slice(&relation_id.to_be_bytes());
        buf.extend_from_slice(namespace.as_bytes());
        buf.push(0);
        buf.extend_from_slice(name.as_bytes());
        buf.push(0);
        buf.push(b'd'); // replica identity = default
        buf.extend_from_slice(&(columns.len() as i16).to_be_bytes());
        for (flags, col_name, oid, modifier) in columns {
            buf.push(*flags);
            buf.extend_from_slice(col_name.as_bytes());
            buf.push(0);
            buf.extend_from_slice(&oid.to_be_bytes());
            buf.extend_from_slice(&modifier.to_be_bytes());
        }
        buf
    }

    /// Builds a binary pgoutput Begin message for testing.
    fn build_begin_message(final_lsn: u64, commit_ts_us: i64, xid: u32) -> Vec<u8> {
        let mut buf = vec![b'B'];
        buf.extend_from_slice(&final_lsn.to_be_bytes());
        buf.extend_from_slice(&commit_ts_us.to_be_bytes());
        buf.extend_from_slice(&xid.to_be_bytes());
        buf
    }

    /// Builds a binary pgoutput Commit message for testing.
    fn build_commit_message(commit_lsn: u64, end_lsn: u64, commit_ts_us: i64) -> Vec<u8> {
        let mut buf = vec![b'C'];
        buf.push(0); // flags
        buf.extend_from_slice(&commit_lsn.to_be_bytes());
        buf.extend_from_slice(&end_lsn.to_be_bytes());
        buf.extend_from_slice(&commit_ts_us.to_be_bytes());
        buf
    }

    /// Builds a binary pgoutput Insert message for testing.
    fn build_insert_message(relation_id: u32, values: &[Option<&str>]) -> Vec<u8> {
        let mut buf = vec![b'I'];
        buf.extend_from_slice(&relation_id.to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for val in values {
            match val {
                Some(s) => {
                    buf.push(b't');
                    buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
                None => buf.push(b'n'),
            }
        }
        buf
    }

    /// Builds a binary pgoutput Delete message for testing.
    fn build_delete_message(relation_id: u32, values: &[Option<&str>]) -> Vec<u8> {
        let mut buf = vec![b'D'];
        buf.extend_from_slice(&relation_id.to_be_bytes());
        buf.push(b'K'); // key identity
        buf.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for val in values {
            match val {
                Some(s) => {
                    buf.push(b't');
                    buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
                None => buf.push(b'n'),
            }
        }
        buf
    }

    /// Builds a binary pgoutput Truncate message for testing.
    fn build_truncate_message(relation_ids: &[u32], options: u8) -> Vec<u8> {
        let mut buf = vec![b'T'];
        buf.extend_from_slice(&(relation_ids.len() as i32).to_be_bytes());
        buf.push(options);
        for id in relation_ids {
            buf.extend_from_slice(&id.to_be_bytes());
        }
        buf
    }

    /// Builds a binary pgoutput Update message (with old tuple) for testing.
    fn build_update_message(
        relation_id: u32,
        old_values: &[Option<&str>],
        new_values: &[Option<&str>],
    ) -> Vec<u8> {
        let mut buf = vec![b'U'];
        buf.extend_from_slice(&relation_id.to_be_bytes());
        // Old tuple with 'O' tag (REPLICA IDENTITY FULL)
        buf.push(b'O');
        buf.extend_from_slice(&(old_values.len() as i16).to_be_bytes());
        for val in old_values {
            match val {
                Some(s) => {
                    buf.push(b't');
                    buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
                None => buf.push(b'n'),
            }
        }
        // New tuple
        buf.push(b'N');
        buf.extend_from_slice(&(new_values.len() as i16).to_be_bytes());
        for val in new_values {
            match val {
                Some(s) => {
                    buf.push(b't');
                    buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
                None => buf.push(b'n'),
            }
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::postgres::types::{INT4_OID, INT8_OID, TEXT_OID};
    use arrow_array::cast::AsArray;
    use std::sync::atomic::Ordering;

    fn default_source() -> PostgresCdcSource {
        PostgresCdcSource::new(PostgresCdcConfig::default())
    }

    fn running_source() -> PostgresCdcSource {
        let mut src = default_source();
        src.state = ConnectorState::Running;
        src
    }

    // ── Construction ──

    #[test]
    fn test_new_source() {
        let src = default_source();
        assert_eq!(src.state, ConnectorState::Created);
        assert!(src.confirmed_flush_lsn.is_zero());
        assert_eq!(src.event_buffer.len(), 0);
        assert_eq!(src.schema().fields().len(), 6);
    }

    #[test]
    fn test_from_config() {
        let mut config = ConnectorConfig::new("postgres-cdc");
        config.set("host", "pg.local");
        config.set("database", "testdb");
        config.set("slot.name", "my_slot");
        config.set("publication", "my_pub");

        let src = PostgresCdcSource::from_config(&config).unwrap();
        assert_eq!(src.config().host, "pg.local");
        assert_eq!(src.config().database, "testdb");
    }

    #[test]
    fn test_from_config_invalid() {
        let config = ConnectorConfig::new("postgres-cdc");
        assert!(PostgresCdcSource::from_config(&config).is_err());
    }

    // ── Lifecycle ──

    // Without postgres-cdc feature, open() must return an error.
    #[cfg(not(feature = "postgres-cdc"))]
    #[tokio::test]
    async fn test_open_fails_without_feature() {
        let mut src = default_source();
        let config = ConnectorConfig::new("postgres-cdc");
        let result = src.open(&config).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("postgres-cdc"),
            "error should mention feature flag: {err}"
        );
    }

    #[tokio::test]
    async fn test_close() {
        let mut src = running_source();
        src.inject_event(ChangeEvent {
            table: "t".to_string(),
            op: CdcOperation::Insert,
            lsn: Lsn::ZERO,
            ts_ms: 0,
            before: None,
            after: Some("{}".to_string()),
        });

        src.close().await.unwrap();
        assert_eq!(src.state, ConnectorState::Closed);
        assert_eq!(src.event_buffer.len(), 0);
    }

    // ── Checkpoint / Restore ──

    #[test]
    fn test_checkpoint() {
        let mut src = default_source();
        src.confirmed_flush_lsn = "1/ABCD".parse().unwrap();
        src.polled_lsn = "1/ABCD".parse().unwrap();
        src.write_lsn = "1/ABCE".parse().unwrap();

        let cp = src.checkpoint();
        assert_eq!(cp.get_offset("lsn"), Some("1/ABCD"));
        assert_eq!(cp.get_offset("write_lsn"), Some("1/ABCE"));
        assert_eq!(cp.get_metadata("slot_name"), Some("laminar_slot"));
    }

    #[tokio::test]
    async fn test_restore() {
        let mut src = default_source();
        let mut cp = SourceCheckpoint::new(1);
        cp.set_offset("lsn", "2/FF00");
        cp.set_offset("write_lsn", "2/FF10");

        src.restore(&cp).await.unwrap();
        assert_eq!(src.confirmed_flush_lsn.as_u64(), 0x2_0000_FF00);
        assert_eq!(src.write_lsn.as_u64(), 0x2_0000_FF10);
    }

    #[tokio::test]
    async fn test_restore_invalid_lsn() {
        let mut src = default_source();
        let mut cp = SourceCheckpoint::new(1);
        cp.set_offset("lsn", "not_an_lsn");

        assert!(src.restore(&cp).await.is_err());
    }

    // ── Poll (empty) ──

    #[tokio::test]
    async fn test_poll_empty() {
        let mut src = running_source();
        let result = src.poll_batch(100).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_poll_not_running() {
        let mut src = default_source();
        assert!(src.poll_batch(100).await.is_err());
    }

    // ── WAL message processing: full transaction ──

    #[tokio::test]
    async fn test_process_insert_transaction() {
        let mut src = running_source();

        let rel_msg = PostgresCdcSource::build_relation_message(
            16384,
            "public",
            "users",
            &[(1, "id", INT8_OID, -1), (0, "name", TEXT_OID, -1)],
        );
        let begin_msg = PostgresCdcSource::build_begin_message(0x100, 0, 1);
        let insert_msg =
            PostgresCdcSource::build_insert_message(16384, &[Some("42"), Some("Alice")]);
        let commit_msg = PostgresCdcSource::build_commit_message(0x100, 0x200, 0);

        src.enqueue_wal_data(rel_msg);
        src.enqueue_wal_data(begin_msg);
        src.enqueue_wal_data(insert_msg);
        src.enqueue_wal_data(commit_msg);

        let batch = src.poll_batch(100).await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let records = &batch.records;
        let table_col = records.column(0).as_string::<i32>();
        assert_eq!(table_col.value(0), "users");

        let op_col = records.column(1).as_string::<i32>();
        assert_eq!(op_col.value(0), "I");

        let after_col = records.column(5).as_string::<i32>();
        let after_json: serde_json::Value = serde_json::from_str(after_col.value(0)).unwrap();
        assert_eq!(after_json["id"], "42");
        assert_eq!(after_json["name"], "Alice");

        // before should be null for INSERT
        assert!(records.column(4).is_null(0));
    }

    // ── Multiple events in one transaction ──

    #[tokio::test]
    async fn test_multi_event_transaction() {
        let mut src = running_source();

        // Register relation
        let rel_msg = PostgresCdcSource::build_relation_message(
            16384,
            "public",
            "users",
            &[(1, "id", INT8_OID, -1), (0, "name", TEXT_OID, -1)],
        );
        src.enqueue_wal_data(rel_msg);

        // Transaction with 3 events
        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x300, 0, 2));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
            16384,
            &[Some("1"), Some("Alice")],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
            16384,
            &[Some("2"), Some("Bob")],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
            16384,
            &[Some("3"), Some("Charlie")],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x300, 0x400, 0));

        let batch = src.poll_batch(100).await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    // ── Events buffered until commit ──

    #[tokio::test]
    async fn test_events_buffered_until_commit() {
        let mut src = running_source();

        let rel_msg = PostgresCdcSource::build_relation_message(
            16384,
            "public",
            "users",
            &[(1, "id", INT8_OID, -1)],
        );
        src.enqueue_wal_data(rel_msg);

        // Begin + Insert but NO commit
        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(16384, &[Some("1")]));

        // Poll should return nothing (events in txn buffer)
        let result = src.poll_batch(100).await.unwrap();
        assert!(result.is_none());

        // Now commit
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

        let batch = src.poll_batch(100).await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    // ── Update with old tuple ──

    #[tokio::test]
    async fn test_process_update() {
        let mut src = running_source();

        let rel_msg = PostgresCdcSource::build_relation_message(
            16384,
            "public",
            "users",
            &[(1, "id", INT8_OID, -1), (0, "name", TEXT_OID, -1)],
        );
        src.enqueue_wal_data(rel_msg);

        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
        src.enqueue_wal_data(PostgresCdcSource::build_update_message(
            16384,
            &[Some("42"), Some("Alice")],
            &[Some("42"), Some("Bob")],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

        let batch = src.poll_batch(100).await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let op_col = batch.records.column(1).as_string::<i32>();
        assert_eq!(op_col.value(0), "U");

        // Both before and after should be present
        assert!(!batch.records.column(4).is_null(0)); // before
        assert!(!batch.records.column(5).is_null(0)); // after
    }

    // ── Delete ──

    #[tokio::test]
    async fn test_process_delete() {
        let mut src = running_source();

        let rel_msg = PostgresCdcSource::build_relation_message(
            16384,
            "public",
            "users",
            &[(1, "id", INT8_OID, -1), (0, "name", TEXT_OID, -1)],
        );
        src.enqueue_wal_data(rel_msg);

        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
        src.enqueue_wal_data(PostgresCdcSource::build_delete_message(
            16384,
            &[Some("42")],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

        let batch = src.poll_batch(100).await.unwrap().unwrap();
        let op_col = batch.records.column(1).as_string::<i32>();
        assert_eq!(op_col.value(0), "D");

        // before present, after null
        assert!(!batch.records.column(4).is_null(0));
        assert!(batch.records.column(5).is_null(0));
    }

    // ── Table filtering ──

    #[tokio::test]
    async fn test_table_exclude_filter() {
        let mut config = PostgresCdcConfig::default();
        config.table_exclude = vec!["users".to_string()];
        let mut src = PostgresCdcSource::new(config);
        src.state = ConnectorState::Running;

        let rel_msg = PostgresCdcSource::build_relation_message(
            16384,
            "public",
            "users",
            &[(1, "id", INT8_OID, -1)],
        );
        src.enqueue_wal_data(rel_msg);

        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(16384, &[Some("1")]));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

        let result = src.poll_batch(100).await.unwrap();
        assert!(result.is_none()); // filtered out
    }

    // ── Max poll records batching ──

    #[tokio::test]
    async fn test_max_poll_records() {
        let mut src = running_source();

        // Inject 5 events directly
        for i in 0..5 {
            src.inject_event(ChangeEvent {
                table: "t".to_string(),
                op: CdcOperation::Insert,
                lsn: Lsn::new(i as u64),
                ts_ms: 0,
                before: None,
                after: Some(format!("{{\"id\":\"{i}\"}}")),
            });
        }

        // Poll only 2
        let batch = src.poll_batch(2).await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(src.buffered_events(), 3);

        // Poll remaining
        let batch = src.poll_batch(100).await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(src.buffered_events(), 0);
    }

    // ── Partition info ──

    #[tokio::test]
    async fn test_partition_info() {
        let mut src = running_source();
        src.write_lsn = "1/ABCD".parse().unwrap();

        src.inject_event(ChangeEvent {
            table: "t".to_string(),
            op: CdcOperation::Insert,
            lsn: Lsn::ZERO,
            ts_ms: 0,
            before: None,
            after: Some("{}".to_string()),
        });

        let batch = src.poll_batch(100).await.unwrap().unwrap();
        let partition = batch.partition.unwrap();
        assert_eq!(partition.id, "laminar_slot");
        assert_eq!(partition.offset, "1/ABCD");
    }

    // ── Health check ──

    #[test]
    fn test_health_check_healthy() {
        let src = running_source();
        assert!(src.health_check().is_healthy());
    }

    #[test]
    fn test_health_check_degraded() {
        let mut src = running_source();
        src.write_lsn = Lsn::new(200_000_000);
        src.confirmed_flush_lsn = Lsn::ZERO;
        assert!(matches!(src.health_check(), HealthStatus::Degraded(_)));
    }

    #[test]
    fn test_health_check_unknown_when_created() {
        let src = default_source();
        assert!(matches!(src.health_check(), HealthStatus::Unknown));
    }

    // ── Metrics ──

    #[tokio::test]
    async fn test_metrics_after_processing() {
        let mut src = running_source();

        let rel_msg = PostgresCdcSource::build_relation_message(
            16384,
            "public",
            "users",
            &[(1, "id", INT8_OID, -1)],
        );
        src.enqueue_wal_data(rel_msg);

        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(16384, &[Some("1")]));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(16384, &[Some("2")]));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

        let _ = src.poll_batch(100).await.unwrap();

        let metrics = src.metrics();
        assert_eq!(metrics.records_total, 2); // 2 inserts
    }

    // ── Replication lag ──

    #[test]
    fn test_replication_lag() {
        let mut src = default_source();
        src.write_lsn = Lsn::new(1000);
        src.confirmed_flush_lsn = Lsn::new(500);
        assert_eq!(src.replication_lag_bytes(), 500);
    }

    // ── Unknown relation ID ──

    #[tokio::test]
    async fn test_unknown_relation_error() {
        let mut src = running_source();

        // Insert without prior Relation message
        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(99999, &[Some("1")]));

        let result = src.poll_batch(100).await;
        assert!(result.is_err());
    }

    // ── Multi-table in one transaction ──

    #[tokio::test]
    async fn test_multi_table_transaction() {
        let mut src = running_source();

        // Two relations
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            100,
            "public",
            "users",
            &[(1, "id", INT4_OID, -1)],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            200,
            "public",
            "orders",
            &[(1, "order_id", INT4_OID, -1)],
        ));

        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x500, 0, 5));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("1")]));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
            200,
            &[Some("1001")],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x500, 0x600, 0));

        let batch = src.poll_batch(100).await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let table_col = batch.records.column(0).as_string::<i32>();
        assert_eq!(table_col.value(0), "users");
        assert_eq!(table_col.value(1), "orders");
    }

    // ── Relation cache update (schema change) ──

    #[tokio::test]
    async fn test_schema_change_mid_stream() {
        let mut src = running_source();

        // Initial schema: 1 column
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            100,
            "public",
            "users",
            &[(1, "id", INT4_OID, -1)],
        ));

        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("1")]));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

        let batch1 = src.poll_batch(100).await.unwrap().unwrap();
        assert_eq!(batch1.num_rows(), 1);

        // Schema changes: add a column
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            100,
            "public",
            "users",
            &[(1, "id", INT4_OID, -1), (0, "email", TEXT_OID, -1)],
        ));

        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x200, 0, 2));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
            100,
            &[Some("2"), Some("alice@example.com")],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x200, 0x300, 0));

        let batch2 = src.poll_batch(100).await.unwrap().unwrap();
        assert_eq!(batch2.num_rows(), 1);

        // Verify the new column appears in JSON
        let after_col = batch2.records.column(5).as_string::<i32>();
        let json: serde_json::Value = serde_json::from_str(after_col.value(0)).unwrap();
        assert_eq!(json["email"], "alice@example.com");
    }

    // ── Write LSN advances on commit ──

    #[tokio::test]
    async fn test_write_lsn_advances() {
        let mut src = running_source();

        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            100,
            "public",
            "t",
            &[(1, "id", INT4_OID, -1)],
        ));

        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("1")]));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x500, 0));

        let _ = src.poll_batch(100).await;
        assert_eq!(src.write_lsn().as_u64(), 0x500);
    }

    // ── TRUNCATE returns error ──

    #[tokio::test]
    async fn test_truncate_returns_error() {
        let mut src = running_source();

        // Register relation so the error message includes the table name.
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            16384,
            "public",
            "users",
            &[(1, "id", INT8_OID, -1)],
        ));

        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
        src.enqueue_wal_data(PostgresCdcSource::build_truncate_message(&[16384], 0));

        let result = src.poll_batch(100).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("TRUNCATE"),
            "error should mention TRUNCATE: {err}"
        );
        assert!(
            err.contains("users"),
            "error should mention table name: {err}"
        );
    }

    #[tokio::test]
    async fn test_truncate_unknown_relation_uses_oid() {
        let mut src = running_source();

        // No relation registered for ID 99999
        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
        src.enqueue_wal_data(PostgresCdcSource::build_truncate_message(&[99999], 0));

        let result = src.poll_batch(100).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("oid:99999"), "error should mention oid: {err}");
    }

    // ── confirmed_flush_lsn not advanced until checkpoint ──

    #[tokio::test]
    async fn test_confirmed_lsn_not_advanced_until_checkpoint() {
        let mut src = running_source();

        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            100,
            "public",
            "t",
            &[(1, "id", INT4_OID, -1)],
        ));

        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("1")]));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x500, 0));

        // Before poll: confirmed_flush_lsn is ZERO.
        assert!(src.confirmed_flush_lsn().is_zero());

        // After poll: confirmed_flush_lsn must NOT have advanced.
        let _ = src.poll_batch(100).await.unwrap().unwrap();
        assert!(
            src.confirmed_flush_lsn().is_zero(),
            "confirmed_flush_lsn should not advance on poll, got {}",
            src.confirmed_flush_lsn()
        );

        // polled_lsn should have advanced.
        assert_eq!(src.polled_lsn.as_u64(), 0x500);

        // After checkpoint: the checkpoint offset should use polled_lsn.
        let cp = src.checkpoint();
        assert_eq!(cp.get_offset("lsn"), Some("0/500"));
    }

    // ── Restore sets polled_lsn ──

    #[tokio::test]
    async fn test_restore_sets_polled_lsn() {
        let mut src = default_source();
        let mut cp = SourceCheckpoint::new(1);
        cp.set_offset("lsn", "2/FF00");
        cp.set_offset("write_lsn", "2/FF10");

        src.restore(&cp).await.unwrap();
        assert_eq!(src.confirmed_flush_lsn.as_u64(), 0x2_0000_FF00);
        assert_eq!(src.polled_lsn.as_u64(), 0x2_0000_FF00);
        assert_eq!(src.write_lsn.as_u64(), 0x2_0000_FF10);
    }

    // ── Backpressure (no event dropping) ──

    #[tokio::test]
    async fn test_backpressure_does_not_drop_buffered_events() {
        let mut src = running_source();
        src.config.max_buffered_events = 100;

        // Inject 200 events directly into the event buffer.
        // With backpressure, existing buffered events are never dropped —
        // only channel draining is paused when the buffer exceeds the
        // high watermark. Direct-injected events are already in the buffer.
        for i in 0..200u64 {
            src.inject_event(ChangeEvent {
                table: "public.t".to_string(),
                op: CdcOperation::Insert,
                before: None,
                after: Some(format!("{{\"id\": {i}}}")),
                ts_ms: i as i64,
                lsn: Lsn::new(i),
            });
        }
        assert_eq!(src.event_buffer.len(), 200);

        // poll_batch drains events from the buffer — no dropping.
        let batch = src.poll_batch(50).await.unwrap().unwrap();
        assert_eq!(batch.records.num_rows(), 50);
        // 200 - 50 drained = 150 remaining. No events dropped.
        assert_eq!(src.event_buffer.len(), 150);
        assert_eq!(src.metrics.events_dropped.load(Ordering::Relaxed), 0);
    }
}
