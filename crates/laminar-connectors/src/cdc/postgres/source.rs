//! `PostgreSQL` CDC source connector implementation.
//!
//! Implements [`SourceConnector`] for streaming logical replication changes
//! from `PostgreSQL` into `LaminarDB` as Arrow `RecordBatch`es.
//!
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    PartitionInfo, SourceBatch, SourceConnector, SourceContract, SourcePosition, SourceStart,
};
use crate::connector::{SourceConsistency, SourceTopology};
use crate::error::ConnectorError;

use super::changelog::{
    events_to_record_batch, old_tuple_json_encoded_len, old_tuple_to_json, plan_record_batch,
    tuple_json_encoded_len, tuple_to_json, CdcOperation, ChangeEvent,
};
use super::config::PostgresCdcConfig;
use super::decoder::{decode_message, OldTuple, WalMessage};
use super::lsn::Lsn;
use super::metrics::PostgresCdcMetrics;
use super::postgres_io::{source_config_digest, PostgresCheckpointBinding};
use super::schema::{cdc_envelope_schema, RelationCache, RelationInfo};

#[cfg(not(test))]
const PGWIRE_IN_FLIGHT_EVENTS: usize = 1;
#[cfg(not(test))]
const RAW_WAL_QUEUE_CAPACITY: usize = 4_096;
const INITIAL_BOOTSTRAP_NOT_ADMITTED: &str = "[LDB-5060] PostgreSQL CDC initial startup is not admitted until a complete, certified snapshot-to-WAL handoff is implemented";
const CHECKPOINT_CONNECTOR: &str = "postgres-cdc";
const CHECKPOINT_VERSION: &str = "3";
const SYSTEM_IDENTIFIER_METADATA: &str = "system_identifier";
const TIMELINE_ID_METADATA: &str = "timeline_id";
const DATABASE_OID_METADATA: &str = "database_oid";
const PUBLICATION_OID_METADATA: &str = "publication_oid";
const PUBLICATION_DEFINITION_METADATA: &str = "publication_definition_sha256";
const SOURCE_CONFIG_METADATA: &str = "source_config_sha256";
const SLOT_PLUGIN_METADATA: &str = "slot_plugin";
const SLOT_TWO_PHASE_METADATA: &str = "slot_two_phase";
const SLOT_FAILOVER_METADATA: &str = "slot_failover";

/// `PostgreSQL` CDC source connector.
///
/// Streams row-level changes from `PostgreSQL` using logical replication
/// (`pgoutput` plugin). Changes are emitted as Arrow `RecordBatch`es
/// in the CDC envelope format.
///
/// # Envelope Schema
///
/// | Column   | Type          | Nullable | Description                              |
/// |----------|---------------|----------|------------------------------------------|
/// | `_table` | Utf8          | no       | Schema-qualified table name              |
/// | `_op`    | Utf8          | no       | Operation: I, U, D                       |
/// | `_lsn`   | UInt64        | no       | WAL position                             |
/// | `_ts_ms` | Timestamp(ms) | no       | Commit timestamp                         |
/// | `_before`| Utf8          | yes      | Available old identity/full-row JSON     |
/// | `_after` | Utf8          | yes      | New row JSON (for I, U)                  |
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

    /// Committed transactions awaiting `poll_batch()` in WAL order.
    committed_transactions: VecDeque<CommittedTransaction>,

    /// Number of decoded events retained across the current and committed transactions.
    buffered_event_count: usize,

    /// Variable-width bytes retained by decoded events across current and committed transactions.
    /// Event container capacities are measured separately when enforcing the decoded-stage limit.
    buffered_event_bytes: usize,

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

    /// Exact database, publication, slot, and filter identity bound to checkpoints.
    checkpoint_binding: Option<PostgresCheckpointBinding>,

    /// Pending WAL messages used only by deterministic decoder tests.
    #[cfg(test)]
    pending_messages: VecDeque<Vec<u8>>,

    /// Notification handle signalled when WAL data arrives from the reader task.
    data_ready: Arc<Notify>,

    /// Channel receiver for WAL events from the background reader task.
    wal_rx: Option<WalPayloadRx>,

    /// Background WAL reader task handle.
    reader_handle: Option<tokio::task::JoinHandle<()>>,

    /// Shutdown signal for the background reader task.
    reader_shutdown: Option<tokio::sync::watch::Sender<bool>>,

    /// Sender for feeding confirmed flush LSN back to the reader task.
    /// The reader uses this to call `update_applied_lsn` only for
    /// durably-checkpointed positions (prevents at-least-once violation).
    confirmed_lsn_tx: Option<tokio::sync::watch::Sender<u64>>,

    /// One-item readiness lookahead retained across bounded polls.
    pending_payloads: VecDeque<OwnedWalPayload>,

    /// Weighted budget shared by the reader queue and payloads deferred between polls.
    wal_byte_budget: Option<Arc<Semaphore>>,

    /// Fatal reader error delivered out of band so a full WAL queue cannot hide it.
    wal_terminal_error: Option<WalTerminalError>,
}

/// In-progress transaction state.
#[derive(Debug)]
struct TransactionState {
    /// Final LSN of the transaction.
    final_lsn: Lsn,
    /// Commit timestamp in milliseconds.
    commit_ts_ms: i64,
    /// Change events accumulated in this transaction.
    events: VecDeque<ChangeEvent>,
}

/// A transaction is resumable only after every one of its events has been emitted.
#[derive(Debug)]
struct CommittedTransaction {
    end_lsn: Lsn,
    events: VecDeque<ChangeEvent>,
}

/// Single-consumer async receiver for the WAL reader → `poll_batch` queue.
type WalPayloadRx = crossfire::AsyncRx<crossfire::mpsc::Array<OwnedWalPayload>>;

type WalPayloadTx = crossfire::MAsyncTx<crossfire::mpsc::Array<OwnedWalPayload>>;

type WalTerminalError = Arc<std::sync::Mutex<Option<String>>>;

/// WAL event payload sent from the background reader task to [`PostgresCdcSource::poll_batch`].
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
        data: Bytes,
    },
    KeepAlive {
        wal_end: u64,
    },
}

struct OwnedWalPayload {
    payload: WalPayload,
    _byte_permit: OwnedSemaphorePermit,
    wire_bytes: Option<pgwire_replication::WireBytesGuard>,
}

fn retained_wal_payload_bytes(payload: &WalPayload) -> usize {
    let dynamic_bytes = match payload {
        WalPayload::XLogData { data, .. } => data.len(),
        WalPayload::Begin { .. } | WalPayload::Commit { .. } | WalPayload::KeepAlive { .. } => 0,
    };
    std::mem::size_of::<OwnedWalPayload>()
        .saturating_add(dynamic_bytes)
        .max(1)
}

fn logical_wal_payload_bytes(payload: &WalPayload) -> usize {
    match payload {
        WalPayload::Begin { .. } => 1 + 8 + 8 + 4,
        WalPayload::Commit { .. } => 1 + 1 + 8 + 8 + 8,
        WalPayload::XLogData { data, .. } => data.len(),
        WalPayload::KeepAlive { .. } => 0,
    }
}

#[cfg(test)]
async fn send_wal_or_shutdown(
    tx: &WalPayloadTx,
    payload: WalPayload,
    byte_budget: &Arc<Semaphore>,
    max_payload_bytes: usize,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<bool, String> {
    send_wal_with_wire_guard(
        tx,
        payload,
        None,
        byte_budget,
        max_payload_bytes,
        shutdown_rx,
    )
    .await
}

async fn send_wal_with_wire_guard(
    tx: &WalPayloadTx,
    payload: WalPayload,
    wire_bytes: Option<pgwire_replication::WireBytesGuard>,
    byte_budget: &Arc<Semaphore>,
    max_payload_bytes: usize,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<bool, String> {
    if *shutdown_rx.borrow() {
        return Ok(false);
    }

    let retained_bytes = retained_wal_payload_bytes(&payload);
    if retained_bytes > max_payload_bytes {
        return Err(format!(
            "PostgreSQL CDC WAL payload exceeds the hard raw buffer limit \
             (retained bytes: {retained_bytes}/{max_payload_bytes})"
        ));
    }
    let permits = u32::try_from(retained_bytes)
        .map_err(|_| "PostgreSQL CDC raw WAL byte budget exceeds semaphore capacity".to_string())?;
    let permit = tokio::select! {
        biased;
        _ = shutdown_rx.changed() => return Ok(false),
        result = Arc::clone(byte_budget).acquire_many_owned(permits) => result.map_err(|_| {
            "PostgreSQL CDC raw WAL byte budget closed unexpectedly".to_string()
        })?,
    };
    let owned = OwnedWalPayload {
        payload,
        _byte_permit: permit,
        wire_bytes,
    };
    tokio::select! {
        biased;
        _ = shutdown_rx.changed() => Ok(false),
        result = tx.send(owned) => Ok(result.is_ok()),
    }
}

fn publish_terminal_wal_error(error: &WalTerminalError, message: String, data_ready: &Notify) {
    let mut slot = error
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if slot.is_none() {
        *slot = Some(message);
    }
    drop(slot);
    data_ready.notify_one();
}

fn take_confirmed_lsn(
    receiver: &mut tokio::sync::watch::Receiver<u64>,
) -> Option<pgwire_replication::Lsn> {
    let confirmed = *receiver.borrow_and_update();
    (confirmed > 0).then(|| pgwire_replication::Lsn::from_u64(confirmed))
}

fn retained_event_bytes(event: &ChangeEvent) -> Result<usize, ConnectorError> {
    planned_event_bytes(
        event.table.capacity(),
        event.before.as_ref().map(String::capacity),
        event.after.as_ref().map(String::capacity),
    )
}

fn planned_event_bytes(
    table_bytes: usize,
    before_bytes: Option<usize>,
    after_bytes: Option<usize>,
) -> Result<usize, ConnectorError> {
    [
        table_bytes,
        before_bytes.unwrap_or(0),
        after_bytes.unwrap_or(0),
    ]
    .into_iter()
    .try_fold(0_usize, |total, bytes| {
        total.checked_add(bytes).ok_or_else(|| {
            ConnectorError::ReadError(
                "PostgreSQL CDC decoded-event retained-byte size overflow".into(),
            )
        })
    })
}

fn conservative_deque_growth_bytes(
    len: usize,
    capacity: usize,
    element_size: usize,
) -> Result<usize, ConnectorError> {
    if len < capacity {
        return Ok(0);
    }
    capacity.max(4).checked_mul(element_size).ok_or_else(|| {
        ConnectorError::ReadError("PostgreSQL CDC container growth size overflow".into())
    })
}

fn required_checkpoint_metadata<'a>(
    checkpoint: &'a SourceCheckpoint,
    key: &str,
    context: &str,
) -> Result<&'a str, ConnectorError> {
    checkpoint.get_metadata(key).ok_or_else(|| {
        ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} is missing required '{key}' metadata"
        ))
    })
}

fn parse_checkpoint_decimal<T>(
    checkpoint: &SourceCheckpoint,
    key: &str,
    context: &str,
) -> Result<T, ConnectorError>
where
    T: std::str::FromStr + ToString,
    T::Err: std::fmt::Display,
{
    let value = required_checkpoint_metadata(checkpoint, key, context)?;
    let parsed = value.parse::<T>().map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} has invalid '{key}' metadata '{value}': {error}"
        ))
    })?;
    if parsed.to_string() != value {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} has non-canonical '{key}' metadata '{value}'"
        )));
    }
    Ok(parsed)
}

fn parse_checkpoint_bool(
    checkpoint: &SourceCheckpoint,
    key: &str,
    context: &str,
) -> Result<bool, ConnectorError> {
    match required_checkpoint_metadata(checkpoint, key, context)? {
        "true" => Ok(true),
        "false" => Ok(false),
        value => Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} has invalid '{key}' metadata '{value}'"
        ))),
    }
}

fn parse_checkpoint_sha256(
    checkpoint: &SourceCheckpoint,
    key: &str,
    context: &str,
) -> Result<String, ConnectorError> {
    let value = required_checkpoint_metadata(checkpoint, key, context)?;
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} has invalid '{key}' SHA-256 metadata"
        )));
    }
    Ok(value.to_string())
}

fn checkpoint_binding(
    checkpoint: &SourceCheckpoint,
    context: &str,
) -> Result<PostgresCheckpointBinding, ConnectorError> {
    Ok(PostgresCheckpointBinding {
        system_identifier: parse_checkpoint_decimal(
            checkpoint,
            SYSTEM_IDENTIFIER_METADATA,
            context,
        )?,
        timeline_id: parse_checkpoint_decimal(checkpoint, TIMELINE_ID_METADATA, context)?,
        database_oid: parse_checkpoint_decimal(checkpoint, DATABASE_OID_METADATA, context)?,
        publication_oid: parse_checkpoint_decimal(checkpoint, PUBLICATION_OID_METADATA, context)?,
        publication_definition_sha256: parse_checkpoint_sha256(
            checkpoint,
            PUBLICATION_DEFINITION_METADATA,
            context,
        )?,
        source_config_sha256: parse_checkpoint_sha256(checkpoint, SOURCE_CONFIG_METADATA, context)?,
        slot_plugin: required_checkpoint_metadata(checkpoint, SLOT_PLUGIN_METADATA, context)?
            .to_string(),
        slot_two_phase: parse_checkpoint_bool(checkpoint, SLOT_TWO_PHASE_METADATA, context)?,
        slot_failover: parse_checkpoint_bool(checkpoint, SLOT_FAILOVER_METADATA, context)?,
    })
}

fn validate_checkpoint_identity(
    checkpoint: &SourceCheckpoint,
    config: &PostgresCdcConfig,
    context: &str,
) -> Result<PostgresCheckpointBinding, ConnectorError> {
    for (key, expected) in [
        ("checkpoint_version", CHECKPOINT_VERSION),
        ("connector", CHECKPOINT_CONNECTOR),
        ("slot_name", config.slot_name.as_str()),
        ("publication", config.publication.as_str()),
        ("database", config.database.as_str()),
    ] {
        let actual = required_checkpoint_metadata(checkpoint, key, context)?;
        if actual != expected {
            return Err(ConnectorError::ConfigurationError(format!(
                "PostgreSQL CDC {context} has '{key}' identity '{actual}', expected '{expected}'"
            )));
        }
    }

    let binding = checkpoint_binding(checkpoint, context)?;
    let configured_digest = source_config_digest(config);
    if binding.source_config_sha256 != configured_digest {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} source filter/configuration identity drifted from its checkpoint"
        )));
    }
    Ok(binding)
}

fn write_checkpoint_binding(
    checkpoint: &mut SourceCheckpoint,
    binding: &PostgresCheckpointBinding,
) {
    checkpoint.set_metadata("connector", CHECKPOINT_CONNECTOR);
    checkpoint.set_metadata("checkpoint_version", CHECKPOINT_VERSION);
    checkpoint.set_metadata(
        SYSTEM_IDENTIFIER_METADATA,
        binding.system_identifier.to_string(),
    );
    checkpoint.set_metadata(TIMELINE_ID_METADATA, binding.timeline_id.to_string());
    checkpoint.set_metadata(DATABASE_OID_METADATA, binding.database_oid.to_string());
    checkpoint.set_metadata(
        PUBLICATION_OID_METADATA,
        binding.publication_oid.to_string(),
    );
    checkpoint.set_metadata(
        PUBLICATION_DEFINITION_METADATA,
        &binding.publication_definition_sha256,
    );
    checkpoint.set_metadata(SOURCE_CONFIG_METADATA, &binding.source_config_sha256);
    checkpoint.set_metadata(SLOT_PLUGIN_METADATA, &binding.slot_plugin);
    checkpoint.set_metadata(SLOT_TWO_PHASE_METADATA, binding.slot_two_phase.to_string());
    checkpoint.set_metadata(SLOT_FAILOVER_METADATA, binding.slot_failover.to_string());
}

fn validate_live_binding(
    checkpoint: &PostgresCheckpointBinding,
    live: &PostgresCheckpointBinding,
    context: &str,
) -> Result<(), ConnectorError> {
    if checkpoint != live {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} identity drifted from the live database, publication, or replication slot (checkpoint: {checkpoint:?}; live: {live:?})"
        )));
    }
    Ok(())
}

impl PostgresCdcSource {
    /// Creates a new `PostgreSQL` CDC source with the given configuration.
    #[must_use]
    pub fn new(mut config: PostgresCdcConfig, registry: Option<&prometheus::Registry>) -> Self {
        config.normalize_table_filters();
        Self {
            config,
            state: ConnectorState::Created,
            schema: cdc_envelope_schema(),
            metrics: Arc::new(PostgresCdcMetrics::new(registry)),
            relation_cache: RelationCache::new(),
            committed_transactions: VecDeque::new(),
            buffered_event_count: 0,
            buffered_event_bytes: 0,
            current_txn: None,
            confirmed_flush_lsn: Lsn::ZERO,
            write_lsn: Lsn::ZERO,
            polled_lsn: Lsn::ZERO,
            checkpoint_binding: None,
            #[cfg(test)]
            pending_messages: VecDeque::new(),
            data_ready: Arc::new(Notify::new()),
            wal_rx: None,
            reader_handle: None,
            reader_shutdown: None,
            confirmed_lsn_tx: None,
            pending_payloads: VecDeque::new(),
            wal_byte_budget: None,
            wal_terminal_error: None,
        }
    }

    /// Creates a new source from a generic [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the configuration is invalid.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let pg_config = PostgresCdcConfig::from_config(config)?;
        Ok(Self::new(pg_config, None))
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
        self.buffered_event_count
    }

    #[cfg(test)]
    fn enqueue_wal_data(&mut self, data: Vec<u8>) {
        self.pending_messages.push_back(data);
    }

    #[cfg(test)]
    fn process_pending_messages(&mut self) -> Result<(), ConnectorError> {
        while let Some(data) = self.pending_messages.pop_front() {
            let result = decode_message(Bytes::from(data))
                .map_err(|error| ConnectorError::ReadError(format!("pgoutput decode: {error}")))
                .and_then(|message| self.process_wal_message(message));
            if let Err(error) = result {
                self.state = ConnectorState::Failed;
                return Err(error);
            }
        }
        Ok(())
    }

    /// Processes a single decoded WAL message.
    fn process_wal_message(&mut self, msg: WalMessage) -> Result<(), ConnectorError> {
        match msg {
            WalMessage::Begin(begin) => {
                if self.current_txn.is_some() {
                    return Err(ConnectorError::ReadError(
                        "PostgreSQL CDC received BEGIN before the current transaction committed"
                            .into(),
                    ));
                }
                self.current_txn = Some(TransactionState {
                    final_lsn: begin.final_lsn,
                    commit_ts_ms: begin.commit_ts_ms,
                    events: VecDeque::new(),
                });
                self.reserve_committed_transaction_slot()?;
            }
            WalMessage::Commit(commit) => {
                if let Err(error) = self.validate_commit_boundary(&commit) {
                    self.state = ConnectorState::Failed;
                    return Err(error);
                }
                self.reserve_committed_transaction_slot()?;
                let txn = self.current_txn.take().ok_or_else(|| {
                    ConnectorError::ReadError(
                        "PostgreSQL CDC received COMMIT without an open transaction".into(),
                    )
                })?;
                self.committed_transactions.push_back(CommittedTransaction {
                    end_lsn: commit.end_lsn,
                    events: txn.events,
                });
                self.write_lsn = self.write_lsn.max(commit.end_lsn);
                self.metrics.record_transaction();
                self.metrics
                    .set_replication_lag_bytes(self.replication_lag_bytes());
            }
            WalMessage::Relation(rel) => {
                let info = RelationInfo {
                    relation_id: rel.relation_id,
                    namespace: rel.namespace,
                    name: rel.name,
                    replica_identity: rel.replica_identity as char,
                    columns: rel.columns,
                };
                self.admit_relation(info)?;
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
                            .map_or_else(|| Ok(format!("oid:{id}")), RelationInfo::full_name)
                    })
                    .collect::<Result<_, ConnectorError>>()?;
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
        let (lsn, ts_ms) = self.require_current_txn_context()?;
        let (table, after_len) = {
            let relation = self.require_relation(relation_id)?;
            let table = relation.full_name()?;

            if !self.config.should_include_table(&table) {
                return Ok(());
            }

            let after_len = tuple_json_encoded_len(new_tuple, relation)?;
            (table, after_len)
        };
        let event_bytes = planned_event_bytes(table.capacity(), None, Some(after_len))?;
        self.reserve_current_event_slot()?;
        self.ensure_event_capacity(event_bytes)?;
        let after_json = tuple_to_json(new_tuple, self.require_relation(relation_id)?, after_len)?;

        let event = ChangeEvent {
            table,
            op: CdcOperation::Insert,
            lsn,
            ts_ms,
            before: None,
            after: Some(after_json),
        };

        self.push_event(event, event_bytes)?;
        self.metrics.record_insert();
        Ok(())
    }

    fn process_update(
        &mut self,
        relation_id: u32,
        old_tuple: Option<&OldTuple>,
        new_tuple: &super::decoder::TupleData,
    ) -> Result<(), ConnectorError> {
        let (lsn, ts_ms) = self.require_current_txn_context()?;
        let (table, before_len, after_len) = {
            let relation = self.require_relation(relation_id)?;
            let table = relation.full_name()?;

            if !self.config.should_include_table(&table) {
                return Ok(());
            }

            let before_len = old_tuple
                .map(|tuple| old_tuple_json_encoded_len(tuple, relation))
                .transpose()?;
            let after_len = tuple_json_encoded_len(new_tuple, relation)?;
            (table, before_len, after_len)
        };
        let event_bytes = planned_event_bytes(table.capacity(), before_len, Some(after_len))?;
        self.reserve_current_event_slot()?;
        self.ensure_event_capacity(event_bytes)?;
        let relation = self.require_relation(relation_id)?;
        let before_json = old_tuple
            .zip(before_len)
            .map(|(tuple, length)| old_tuple_to_json(tuple, relation, length))
            .transpose()?;
        let after_json = tuple_to_json(new_tuple, relation, after_len)?;

        let event = ChangeEvent {
            table,
            op: CdcOperation::Update,
            lsn,
            ts_ms,
            before: before_json,
            after: Some(after_json),
        };

        self.push_event(event, event_bytes)?;
        self.metrics.record_update();
        Ok(())
    }

    fn process_delete(
        &mut self,
        relation_id: u32,
        old_tuple: &OldTuple,
    ) -> Result<(), ConnectorError> {
        let (lsn, ts_ms) = self.require_current_txn_context()?;
        let (table, before_len) = {
            let relation = self.require_relation(relation_id)?;
            let table = relation.full_name()?;

            if !self.config.should_include_table(&table) {
                return Ok(());
            }

            let before_len = old_tuple_json_encoded_len(old_tuple, relation)?;
            (table, before_len)
        };
        let event_bytes = planned_event_bytes(table.capacity(), Some(before_len), None)?;
        self.reserve_current_event_slot()?;
        self.ensure_event_capacity(event_bytes)?;
        let before_json =
            old_tuple_to_json(old_tuple, self.require_relation(relation_id)?, before_len)?;

        let event = ChangeEvent {
            table,
            op: CdcOperation::Delete,
            lsn,
            ts_ms,
            before: Some(before_json),
            after: None,
        };

        self.push_event(event, event_bytes)?;
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

    fn require_current_txn_context(&mut self) -> Result<(Lsn, i64), ConnectorError> {
        if let Some(txn) = &self.current_txn {
            return Ok((txn.final_lsn, txn.commit_ts_ms));
        }
        self.state = ConnectorState::Failed;
        Err(ConnectorError::ReadError(
            "PostgreSQL CDC received a row change outside a transaction".into(),
        ))
    }

    fn validate_commit_boundary(
        &self,
        commit: &super::decoder::CommitMessage,
    ) -> Result<(), ConnectorError> {
        let transaction = self.current_txn.as_ref().ok_or_else(|| {
            ConnectorError::ReadError(
                "PostgreSQL CDC received COMMIT without an open transaction".into(),
            )
        })?;
        if commit.commit_lsn != transaction.final_lsn {
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC COMMIT LSN {} does not match BEGIN final LSN {}",
                commit.commit_lsn, transaction.final_lsn
            )));
        }
        if commit.commit_ts_ms != transaction.commit_ts_ms {
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC COMMIT timestamp {} does not match BEGIN timestamp {}",
                commit.commit_ts_ms, transaction.commit_ts_ms
            )));
        }
        if commit.end_lsn < commit.commit_lsn {
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC COMMIT end LSN {} is before commit LSN {}",
                commit.end_lsn, commit.commit_lsn
            )));
        }
        let last_resumable_lsn = self
            .committed_transactions
            .back()
            .map_or(self.polled_lsn, |transaction| {
                transaction.end_lsn.max(self.polled_lsn)
            });
        if commit.end_lsn < last_resumable_lsn {
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC COMMIT end LSN {} is behind the last emitted or queued LSN {last_resumable_lsn}",
                commit.end_lsn
            )));
        }
        Ok(())
    }

    fn event_container_retained_bytes(&self) -> Result<usize, ConnectorError> {
        let event_size = std::mem::size_of::<ChangeEvent>();
        let mut retained = self
            .committed_transactions
            .capacity()
            .checked_mul(std::mem::size_of::<CommittedTransaction>())
            .ok_or_else(|| {
                ConnectorError::ReadError(
                    "PostgreSQL CDC committed-transaction container size overflow".into(),
                )
            })?;
        if let Some(transaction) = &self.current_txn {
            retained = retained
                .checked_add(
                    transaction
                        .events
                        .capacity()
                        .checked_mul(event_size)
                        .ok_or_else(|| {
                            ConnectorError::ReadError(
                                "PostgreSQL CDC open-transaction container size overflow".into(),
                            )
                        })?,
                )
                .ok_or_else(|| {
                    ConnectorError::ReadError(
                        "PostgreSQL CDC event-container retained-byte overflow".into(),
                    )
                })?;
        }
        for transaction in &self.committed_transactions {
            retained = retained
                .checked_add(
                    transaction
                        .events
                        .capacity()
                        .checked_mul(event_size)
                        .ok_or_else(|| {
                            ConnectorError::ReadError(
                                "PostgreSQL CDC committed-event container size overflow".into(),
                            )
                        })?,
                )
                .ok_or_else(|| {
                    ConnectorError::ReadError(
                        "PostgreSQL CDC event-container retained-byte overflow".into(),
                    )
                })?;
        }
        Ok(retained)
    }

    fn decoded_retained_bytes(&self) -> Result<usize, ConnectorError> {
        let container_bytes = self.event_container_retained_bytes()?;
        let relation_bytes = self.relation_cache.retained_bytes()?;
        self.buffered_event_bytes
            .checked_add(container_bytes)
            .and_then(|bytes| bytes.checked_add(relation_bytes))
            .ok_or_else(|| {
                ConnectorError::ReadError(
                    "PostgreSQL CDC decoded-stage retained-byte accounting overflow".into(),
                )
            })
    }

    fn ensure_decoded_byte_limit(
        &mut self,
        additional_bytes: usize,
        context: &str,
    ) -> Result<usize, ConnectorError> {
        let retained_bytes = self
            .decoded_retained_bytes()
            .and_then(|bytes| {
                bytes.checked_add(additional_bytes).ok_or_else(|| {
                    ConnectorError::ReadError(
                        "PostgreSQL CDC decoded-stage retained-byte accounting overflow".into(),
                    )
                })
            })
            .map_err(|error| {
                self.state = ConnectorState::Failed;
                error
            })?;
        let max_bytes = self.config.decoded_event_bytes();
        if retained_bytes > max_bytes {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC {context} exceeds the hard decoded-stage buffer limit (retained bytes: {retained_bytes}/{max_bytes})"
            )));
        }
        Ok(retained_bytes)
    }

    fn reserve_current_event_slot(&mut self) -> Result<(), ConnectorError> {
        let Some(transaction) = self.current_txn.as_ref() else {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(
                "PostgreSQL CDC received a row change outside a transaction".into(),
            ));
        };
        let old_capacity = transaction.events.capacity();
        let growth_bytes = conservative_deque_growth_bytes(
            transaction.events.len(),
            old_capacity,
            std::mem::size_of::<ChangeEvent>(),
        )?;
        self.ensure_decoded_byte_limit(growth_bytes, "event-container growth")?;
        let Some(transaction) = self.current_txn.as_mut() else {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC open transaction disappeared during container preflight".into(),
            ));
        };
        if let Err(error) = transaction.events.try_reserve_exact(1) {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC could not reserve decoded-event storage: {error}"
            )));
        }
        let Some(actual_growth) = transaction
            .events
            .capacity()
            .checked_sub(old_capacity)
            .and_then(|capacity| capacity.checked_mul(std::mem::size_of::<ChangeEvent>()))
        else {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC event-container growth accounting failed".into(),
            ));
        };
        if actual_growth > growth_bytes {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(format!(
                "PostgreSQL CDC event-container growth exceeded its conservative preflight: actual={actual_growth}, planned={growth_bytes}"
            )));
        }
        self.ensure_decoded_byte_limit(0, "event-container growth")?;
        Ok(())
    }

    fn reserve_committed_transaction_slot(&mut self) -> Result<(), ConnectorError> {
        if self.current_txn.is_none() {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(
                "PostgreSQL CDC received COMMIT without an open transaction".into(),
            ));
        }
        self.committed_transactions
            .len()
            .checked_add(1)
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::ReadError(
                    "PostgreSQL CDC committed-transaction count overflow".into(),
                )
            })?;
        let old_capacity = self.committed_transactions.capacity();
        let growth_bytes = conservative_deque_growth_bytes(
            self.committed_transactions.len(),
            old_capacity,
            std::mem::size_of::<CommittedTransaction>(),
        )?;
        self.ensure_decoded_byte_limit(growth_bytes, "committed-transaction container growth")?;
        if let Err(error) = self.committed_transactions.try_reserve_exact(1) {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC could not reserve committed-transaction storage: {error}"
            )));
        }
        let actual_growth = self
            .committed_transactions
            .capacity()
            .checked_sub(old_capacity)
            .and_then(|capacity| capacity.checked_mul(std::mem::size_of::<CommittedTransaction>()))
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::Internal(
                    "PostgreSQL CDC committed-transaction growth accounting failed".into(),
                )
            })?;
        if actual_growth > growth_bytes {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(format!(
                "PostgreSQL CDC committed-transaction growth exceeded its conservative preflight: actual={actual_growth}, planned={growth_bytes}"
            )));
        }
        self.ensure_decoded_byte_limit(0, "committed-transaction container growth")?;
        Ok(())
    }

    fn admit_relation(&mut self, info: RelationInfo) -> Result<(), ConnectorError> {
        let existing_bytes = self
            .relation_cache
            .get(info.relation_id)
            .map(RelationInfo::variable_retained_bytes)
            .transpose()
            .map_err(|error| {
                self.state = ConnectorState::Failed;
                error
            })?;
        let new_relation = usize::from(existing_bytes.is_none());
        let existing_bytes = existing_bytes.unwrap_or(0);
        self.relation_cache
            .len()
            .checked_add(new_relation)
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::ReadError("PostgreSQL CDC relation-cache count overflow".into())
            })?;
        let incoming_bytes = info.variable_retained_bytes().map_err(|error| {
            self.state = ConnectorState::Failed;
            error
        })?;
        let retained_growth = incoming_bytes.saturating_sub(existing_bytes);
        let growth_bytes = self
            .relation_cache
            .reservation_growth_bytes(info.relation_id)
            .map_err(|error| {
                self.state = ConnectorState::Failed;
                error
            })?;
        let admission_bytes = retained_growth.checked_add(growth_bytes).ok_or_else(|| {
            self.state = ConnectorState::Failed;
            ConnectorError::ReadError(
                "PostgreSQL CDC relation-cache admission size overflow".into(),
            )
        })?;
        self.ensure_decoded_byte_limit(admission_bytes, "relation-cache admission")?;
        let old_cache_bytes = self.relation_cache.retained_bytes()?;
        self.relation_cache
            .try_reserve_for(info.relation_id)
            .map_err(|error| {
                self.state = ConnectorState::Failed;
                error
            })?;
        let actual_growth = self
            .relation_cache
            .retained_bytes()?
            .checked_sub(old_cache_bytes)
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::Internal(
                    "PostgreSQL CDC relation-cache growth accounting failed".into(),
                )
            })?;
        if actual_growth > growth_bytes {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(format!(
                "PostgreSQL CDC relation-cache growth exceeded its conservative preflight: actual={actual_growth}, planned={growth_bytes}"
            )));
        }
        self.ensure_decoded_byte_limit(retained_growth, "relation-cache admission")?;
        self.relation_cache.insert(info).map_err(|error| {
            self.state = ConnectorState::Failed;
            error
        })?;
        self.ensure_decoded_byte_limit(0, "relation-cache retention")?;
        Ok(())
    }

    fn ensure_event_capacity(&mut self, event_bytes: usize) -> Result<(), ConnectorError> {
        if self.current_txn.is_none() {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(
                "PostgreSQL CDC received a row change outside a transaction".into(),
            ));
        }

        self.buffered_event_count.checked_add(1).ok_or_else(|| {
            self.state = ConnectorState::Failed;
            ConnectorError::ReadError(
                "PostgreSQL CDC decoded-event count accounting overflow".into(),
            )
        })?;
        self.ensure_decoded_byte_limit(event_bytes, "transaction")?;
        Ok(())
    }

    fn push_event(
        &mut self,
        event: ChangeEvent,
        preflight_bytes: usize,
    ) -> Result<(), ConnectorError> {
        let event_bytes = retained_event_bytes(&event)?;
        if event_bytes > preflight_bytes {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(format!(
                "PostgreSQL CDC decoded event exceeded its retained-byte preflight: actual={event_bytes}, planned={preflight_bytes}"
            )));
        }
        self.ensure_event_capacity(event_bytes)?;
        let next_event_count = self.buffered_event_count.checked_add(1).ok_or_else(|| {
            self.state = ConnectorState::Failed;
            ConnectorError::Internal(
                "PostgreSQL CDC preflighted event-count accounting overflow".into(),
            )
        })?;
        let next_event_bytes = self
            .buffered_event_bytes
            .checked_add(event_bytes)
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::Internal(
                    "PostgreSQL CDC preflighted retained-byte accounting overflow".into(),
                )
            })?;

        let Some(txn) = self.current_txn.as_mut() else {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC open transaction disappeared after event preflight".into(),
            ));
        };
        txn.events.push_back(event);
        self.buffered_event_count = next_event_count;
        self.buffered_event_bytes = next_event_bytes;
        Ok(())
    }

    /// Processes a [`WalPayload`] received from the background reader task.
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
                    commit_ts_ms: pg_timestamp_to_unix_ms(commit_ts_us).map_err(|error| {
                        ConnectorError::ReadError(format!(
                            "pgoutput BEGIN timestamp decode: {error}"
                        ))
                    })?,
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
                    commit_ts_ms: pg_timestamp_to_unix_ms(commit_ts_us).map_err(|error| {
                        ConnectorError::ReadError(format!(
                            "pgoutput COMMIT timestamp decode: {error}"
                        ))
                    })?,
                };
                self.process_wal_message(WalMessage::Commit(commit))
            }
            WalPayload::XLogData { wal_end, data } => {
                let msg = decode_message(data)
                    .map_err(|e| ConnectorError::ReadError(format!("pgoutput decode: {e}")))?;
                self.process_wal_message(msg)?;
                self.write_lsn = self.write_lsn.max(Lsn::new(wal_end));
                Ok(())
            }
            WalPayload::KeepAlive { wal_end } => {
                self.write_lsn = self.write_lsn.max(Lsn::new(wal_end));
                Ok(())
            }
        }
    }

    fn process_owned_wal_payload(
        &mut self,
        payload: OwnedWalPayload,
    ) -> Result<(), ConnectorError> {
        let received_bytes = u64::try_from(logical_wal_payload_bytes(&payload.payload))
            .map_err(|_| ConnectorError::Internal("PostgreSQL CDC byte metric overflow".into()))?;
        self.metrics.record_bytes(received_bytes);
        let OwnedWalPayload {
            payload,
            _byte_permit,
            wire_bytes,
        } = payload;
        let result = self.process_wal_payload(payload);
        drop(wire_bytes);
        result
    }

    fn fail_on_terminal_wal_error(&mut self) -> Result<(), ConnectorError> {
        let message = self.wal_terminal_error.as_ref().and_then(|error| {
            error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        });
        if let Some(message) = message {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(message));
        }
        Ok(())
    }

    /// Drains committed transactions without exposing a cursor inside a transaction.
    ///
    /// `max` is a batching target, not permission to split a PostgreSQL transaction. Logical
    /// replication can resume only at a WAL position, so a checkpoint between two fragments of
    /// one transaction would restore before rows already included in the checkpoint. When the
    /// first queued transaction is larger than `max`, emit it whole; the configured hard event
    /// and byte limits remain the memory bound.
    fn drain_events(&mut self, max: usize) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.committed_transactions.is_empty() || max == 0 {
            return Ok(None);
        }

        let mut selected_transactions = 0_usize;
        let mut selected_events = 0_usize;
        let mut resumable_lsn = self.polled_lsn;
        for transaction in &self.committed_transactions {
            let candidate_events = selected_events
                .checked_add(transaction.events.len())
                .ok_or_else(|| {
                    self.state = ConnectorState::Failed;
                    ConnectorError::Internal(
                        "PostgreSQL CDC drain event-count accounting overflow".into(),
                    )
                })?;
            // Once the row target is full, still absorb immediately-following
            // filtered transactions. They emit no rows, so advancing across
            // them preserves WAL order without splitting visible output or
            // forcing a separate empty poll just to move the durable cursor.
            if selected_events != 0 && !transaction.events.is_empty() && candidate_events > max {
                break;
            }
            selected_events = candidate_events;
            selected_transactions = selected_transactions.checked_add(1).ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::Internal(
                    "PostgreSQL CDC drain transaction-count accounting overflow".into(),
                )
            })?;
            resumable_lsn = transaction.end_lsn;
        }

        if selected_events == 0 {
            // All leading transactions were filtered to zero output rows.
            for _ in 0..selected_transactions {
                self.committed_transactions.pop_front();
            }
            if self.committed_transactions.is_empty() {
                self.committed_transactions = VecDeque::new();
            }
            self.polled_lsn = resumable_lsn;
            return Ok(None);
        }

        let drained_count = selected_events;
        let selected = self
            .committed_transactions
            .iter()
            .take(selected_transactions);
        let drained_bytes = match selected
            .clone()
            .flat_map(|transaction| transaction.events.iter())
            .try_fold(0_usize, |bytes, event| {
                bytes
                    .checked_add(retained_event_bytes(event)?)
                    .ok_or_else(|| {
                        ConnectorError::Internal(
                            "PostgreSQL CDC drained-event retained-byte accounting overflow".into(),
                        )
                    })
            }) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.state = ConnectorState::Failed;
                return Err(error);
            }
        };
        if drained_count > self.buffered_event_count || drained_bytes > self.buffered_event_bytes {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC retained-buffer accounting invariant failed".into(),
            ));
        }

        let plan =
            match plan_record_batch(selected.flat_map(|transaction| transaction.events.iter())) {
                Ok(plan) => plan,
                Err(error) => {
                    self.state = ConnectorState::Failed;
                    return Err(error);
                }
            };
        let arrow_byte_limit = self.config.arrow_build_bytes();
        let minimum_extraction_bytes = selected_transactions
            .checked_mul(std::mem::size_of::<VecDeque<ChangeEvent>>())
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::ReadError(
                    "PostgreSQL CDC Arrow extraction-container size overflow".into(),
                )
            })?;
        let minimum_arrow_bytes = plan
            .retained_bytes
            .checked_add(minimum_extraction_bytes)
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::ReadError(
                    "PostgreSQL CDC Arrow build retained-byte accounting overflow".into(),
                )
            })?;
        if minimum_arrow_bytes > arrow_byte_limit {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC Arrow batch exceeds the hard build-buffer limit (retained bytes: {}/{arrow_byte_limit})",
                minimum_arrow_bytes
            )));
        }

        // Move each transaction's existing event deque as a unit. This avoids allocating a second
        // `ChangeEvent` container while the decoded-stage container is still resident.
        let mut event_groups = Vec::new();
        if let Err(error) = event_groups.try_reserve_exact(selected_transactions) {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC could not reserve Arrow extraction storage: {error}"
            )));
        }
        let extraction_capacity = event_groups.capacity();
        let extraction_bytes = extraction_capacity
            .checked_mul(std::mem::size_of::<VecDeque<ChangeEvent>>())
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::ReadError(
                    "PostgreSQL CDC Arrow extraction-container size overflow".into(),
                )
            })?;
        let planned_arrow_bytes = plan
            .retained_bytes
            .checked_add(extraction_bytes)
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::ReadError(
                    "PostgreSQL CDC Arrow build retained-byte accounting overflow".into(),
                )
            })?;
        if planned_arrow_bytes > arrow_byte_limit {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC Arrow batch exceeds the hard build-buffer limit (retained bytes: {planned_arrow_bytes}/{arrow_byte_limit})"
            )));
        }

        for _ in 0..selected_transactions {
            let Some(mut transaction) = self.committed_transactions.pop_front() else {
                self.state = ConnectorState::Failed;
                return Err(ConnectorError::Internal(
                    "PostgreSQL CDC committed transaction disappeared after drain preflight".into(),
                ));
            };
            event_groups.push(std::mem::take(&mut transaction.events));
        }
        let extracted_events = match event_groups.iter().try_fold(0_usize, |count, events| {
            count.checked_add(events.len()).ok_or_else(|| {
                ConnectorError::Internal(
                    "PostgreSQL CDC Arrow extraction row-count overflow".into(),
                )
            })
        }) {
            Ok(count) => count,
            Err(error) => {
                self.state = ConnectorState::Failed;
                return Err(error);
            }
        };
        if extracted_events != selected_events || event_groups.capacity() != extraction_capacity {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC Arrow extraction changed after capacity preflight".into(),
            ));
        }
        if self.committed_transactions.is_empty() {
            self.committed_transactions = VecDeque::new();
        }

        let batch = match events_to_record_batch(event_groups.into_iter().flatten(), plan) {
            Ok(batch) => batch,
            Err(error) => {
                self.buffered_event_count -= drained_count;
                self.buffered_event_bytes -= drained_bytes;
                self.state = ConnectorState::Failed;
                return Err(error);
            }
        };

        self.buffered_event_count -= drained_count;
        self.buffered_event_bytes -= drained_bytes;
        self.polled_lsn = resumable_lsn;
        self.metrics.record_batch();
        Ok(Some(batch))
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)]
impl SourceConnector for PostgresCdcSource {
    fn recovery_identity_options(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Option<BTreeMap<String, String>>, ConnectorError> {
        let mut parsed = if config.properties().is_empty() {
            self.config.clone()
        } else {
            PostgresCdcConfig::from_config(config)?
        };
        parsed.normalize_table_filters();
        parsed.validate()?;

        Ok(Some(BTreeMap::from([
            ("database".into(), parsed.database),
            ("publication".into(), parsed.publication),
            ("slot.name".into(), parsed.slot_name),
            ("table.exclude".into(), parsed.table_exclude.join(",")),
            ("table.include".into(), parsed.table_include.join(",")),
            ("wire.protocol".into(), "pgoutput-v1".into()),
        ])))
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        if self.state != ConnectorState::Created {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Created.to_string(),
                actual: self.state.to_string(),
            });
        }
        let SourceStart {
            config, position, ..
        } = request;

        // Parse all configuration and validate the exact engine cursor before
        // changing lifecycle state or opening either the control or replication
        // connection. This keeps startup atomic from the connector's perspective.
        let mut parsed_config = if config.properties().is_empty() {
            self.config.clone()
        } else {
            PostgresCdcConfig::from_config(&config)?
        };
        parsed_config.normalize_table_filters();
        parsed_config.validate()?;

        let (start_lsn, expected_binding) = match position {
            SourcePosition::Initial => {
                return Err(ConnectorError::ConfigurationError(
                    INITIAL_BOOTSTRAP_NOT_ADMITTED.into(),
                ));
            }
            SourcePosition::Resume {
                attempt,
                checkpoint,
            } => {
                let context = format!("checkpoint {attempt:?}");
                let binding = validate_checkpoint_identity(&checkpoint, &parsed_config, &context)?;
                let lsn_str = checkpoint.get_offset("lsn").ok_or_else(|| {
                    ConnectorError::ConfigurationError(format!(
                        "PostgreSQL CDC checkpoint {attempt:?} is missing required 'lsn' offset"
                    ))
                })?;
                let lsn = lsn_str.parse::<Lsn>().map_err(|e| {
                    ConnectorError::ConfigurationError(format!(
                        "invalid LSN '{lsn_str}' in PostgreSQL CDC checkpoint {attempt:?}: {e}"
                    ))
                })?;
                (lsn, binding)
            }
        };

        #[cfg(not(test))]
        {
            use super::postgres_io;

            // 1. Connect control-plane for slot management
            let control = postgres_io::connect(&parsed_config).await?;

            // 2. Inspect the exact durable slot. Resume is deliberately
            // read-only: a replacement slot starts at a different history and
            // cannot satisfy the engine checkpoint.
            let slot_inspection = postgres_io::inspect_replication_slot(
                control.client(),
                &parsed_config.slot_name,
                "pgoutput",
                &parsed_config.database,
                &parsed_config.publication,
                expected_binding.source_config_sha256.clone(),
            )
            .await;
            control.close().await;
            let slot_lsn = slot_inspection?;

            let Some(slot) = slot_lsn.as_ref() else {
                return Err(ConnectorError::ConfigurationError(format!(
                    "cannot resume PostgreSQL CDC slot '{}': the exact durable slot is missing",
                    parsed_config.slot_name
                )));
            };
            validate_live_binding(&expected_binding, &slot.binding, "resume checkpoint")?;
            let Some(resume_slot_lsn) = slot.confirmed_flush_lsn.as_ref() else {
                return Err(ConnectorError::ConfigurationError(format!(
                    "cannot resume PostgreSQL CDC slot '{}': the slot has no retained durable position",
                    parsed_config.slot_name
                )));
            };
            if resume_slot_lsn.as_u64() > start_lsn.as_u64() {
                return Err(ConnectorError::ConfigurationError(format!(
                    "cannot resume PostgreSQL CDC checkpoint at {}: slot '{}' has already advanced to {}; required WAL may have been reclaimed",
                    start_lsn, parsed_config.slot_name, resume_slot_lsn
                )));
            }

            // 3. Build pgwire-replication config and start WAL streaming
            let mut repl_config = postgres_io::build_replication_config(&parsed_config);
            repl_config.buffer_events = PGWIRE_IN_FLIGHT_EVENTS;
            // If we resolved a slot LSN, override start_lsn so we resume correctly
            if start_lsn != Lsn::ZERO {
                repl_config.start_lsn = pgwire_replication::Lsn::from_u64(start_lsn.as_u64());
            }
            repl_config.expected_recovery_identity =
                Some(pgwire_replication::ExpectedRecoveryIdentity {
                    system_identifier: expected_binding.system_identifier,
                    timeline_id: expected_binding.timeline_id,
                });

            let repl_client = match tokio::time::timeout(
                postgres_io::CONNECT_TIMEOUT,
                pgwire_replication::ReplicationClient::connect(repl_config),
            )
            .await
            {
                Ok(Ok(client)) => client,
                Ok(Err(error)) => {
                    return Err(ConnectorError::ConnectionFailed(format!(
                        "pgwire-replication connect: {error}"
                    )));
                }
                Err(_) => {
                    return Err(ConnectorError::ConnectionFailed(
                        "pgwire-replication connect timed out after 10 seconds".into(),
                    ));
                }
            };

            // Spawn background reader task for event-driven wake-up.
            let raw_wal_byte_limit = parsed_config.raw_wal_bytes();
            let (wal_tx, wal_rx) =
                crossfire::mpsc::bounded_async::<OwnedWalPayload>(RAW_WAL_QUEUE_CAPACITY);
            // This is Laminar's raw-WAL queue ceiling. pgwire independently applies the same
            // aggregate ceiling while reading and handing off backend frames; its reservation is
            // retained until this queue's permit is acquired, leaving no unaccounted ownership gap.
            let wal_byte_budget = Arc::new(Semaphore::new(raw_wal_byte_limit));
            let reader_byte_budget = Arc::clone(&wal_byte_budget);
            let terminal_error: WalTerminalError = Arc::new(std::sync::Mutex::new(None));
            let reader_terminal_error = Arc::clone(&terminal_error);
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
            let (confirmed_lsn_tx, mut confirmed_lsn_rx) =
                tokio::sync::watch::channel(start_lsn.as_u64());
            let data_ready = Arc::clone(&self.data_ready);

            let reader_handle = tokio::spawn(async move {
                let mut repl_client = repl_client;
                'read: loop {
                    tokio::select! {
                        biased;
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break 'read;
                            }
                        }
                        changed = confirmed_lsn_rx.changed() => {
                            if changed.is_err() {
                                break 'read;
                            }
                            if let Some(confirmed) = take_confirmed_lsn(&mut confirmed_lsn_rx) {
                                repl_client.update_applied_lsn(confirmed);
                            }
                        }
                        event = repl_client.recv() => {
                            match event {
                                Ok(Some(event)) => {
                                    let payload = match event {
                                        pgwire_replication::ReplicationEvent::Begin {
                                            final_lsn,
                                            xid,
                                            commit_time_micros,
                                        } => Some((
                                            WalPayload::Begin {
                                                final_lsn: final_lsn.as_u64(),
                                                commit_ts_us: commit_time_micros,
                                                xid,
                                            },
                                            None,
                                        )),
                                        pgwire_replication::ReplicationEvent::Commit {
                                            end_lsn,
                                            commit_time_micros,
                                            lsn,
                                        } => Some((
                                            WalPayload::Commit {
                                                end_lsn: end_lsn.as_u64(),
                                                commit_ts_us: commit_time_micros,
                                                lsn: lsn.as_u64(),
                                            },
                                            None,
                                        )),
                                        pgwire_replication::ReplicationEvent::XLogData {
                                            wal_end,
                                            data,
                                            wire_bytes,
                                            ..
                                        } => Some((
                                            WalPayload::XLogData {
                                                wal_end: wal_end.as_u64(),
                                                data,
                                            },
                                            Some(wire_bytes),
                                        )),
                                        pgwire_replication::ReplicationEvent::KeepAlive {
                                            wal_end,
                                            ..
                                        } => Some((
                                            WalPayload::KeepAlive {
                                                wal_end: wal_end.as_u64(),
                                            },
                                            None,
                                        )),
                                        pgwire_replication::ReplicationEvent::Message { .. } => {
                                            publish_terminal_wal_error(
                                                &reader_terminal_error,
                                                "PostgreSQL emitted a logical decoding message even though replication was started with messages=false"
                                                    .to_string(),
                                                &data_ready,
                                            );
                                            break 'read;
                                        }
                                        pgwire_replication::ReplicationEvent::StoppedAt {
                                            reached,
                                        } => {
                                            publish_terminal_wal_error(
                                                &reader_terminal_error,
                                                format!(
                                                    "PostgreSQL replication stopped unexpectedly at {reached}; no stop LSN was configured"
                                                ),
                                                &data_ready,
                                            );
                                            break 'read;
                                        }
                                    };
                                    if let Some((payload, wire_bytes)) = payload {
                                        match send_wal_with_wire_guard(
                                            &wal_tx,
                                            payload,
                                            wire_bytes,
                                            &reader_byte_budget,
                                            raw_wal_byte_limit,
                                            &mut shutdown_rx,
                                        ).await {
                                            Ok(true) => data_ready.notify_one(),
                                            Ok(false) => break 'read,
                                            Err(message) => {
                                                publish_terminal_wal_error(
                                                    &reader_terminal_error,
                                                    message,
                                                    &data_ready,
                                                );
                                                break 'read;
                                            }
                                        }
                                    }
                                }
                                Ok(None) => {
                                    publish_terminal_wal_error(
                                        &reader_terminal_error,
                                        "PostgreSQL replication stream ended unexpectedly".into(),
                                        &data_ready,
                                    );
                                    break 'read;
                                }
                                Err(e) => {
                                    publish_terminal_wal_error(
                                        &reader_terminal_error,
                                        format!("PostgreSQL replication stream failed: {e}"),
                                        &data_ready,
                                    );
                                    break 'read;
                                }
                            }
                        }
                    }
                }
                let _ = repl_client.shutdown().await;
            });

            self.wal_rx = Some(wal_rx);
            self.wal_byte_budget = Some(wal_byte_budget);
            self.wal_terminal_error = Some(terminal_error);
            self.reader_handle = Some(reader_handle);
            self.reader_shutdown = Some(shutdown_tx);
            self.confirmed_lsn_tx = Some(confirmed_lsn_tx);
        }

        // Publish the new runtime only after all fallible startup work has
        // succeeded. A failed start remains a clean Created connector.
        self.config = parsed_config;
        self.confirmed_flush_lsn = start_lsn;
        self.write_lsn = start_lsn;
        self.polled_lsn = start_lsn;
        self.checkpoint_binding = Some(expected_binding);
        self.metrics.set_confirmed_flush_lsn(start_lsn.as_u64());
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

        // Backpressure: stop draining raw WAL before the decoded-stage hard limit. The raw byte
        // budget then propagates pressure to the replication reader and PostgreSQL.
        {
            self.fail_on_terminal_wal_error()?;
            let high_watermark = self.config.decoded_high_watermark_bytes();
            let decoded_retained_bytes = self.decoded_retained_bytes().map_err(|error| {
                self.state = ConnectorState::Failed;
                error
            })?;
            let mut reader_closed = false;
            let must_finish_transaction = self.current_txn.is_some();
            let payload_budget = max_records.max(1);
            let drain_reader = must_finish_transaction || decoded_retained_bytes < high_watermark;
            if !drain_reader && self.pending_payloads.is_empty() {
                tracing::debug!(
                    retained_bytes = decoded_retained_bytes,
                    high_watermark,
                    "CDC backpressure active — pausing WAL reader drain"
                );
            }

            let mut processed_payloads = 0_usize;
            while processed_payloads < payload_budget {
                let payload = if let Some(payload) = self.pending_payloads.pop_front() {
                    Some(payload)
                } else if drain_reader {
                    match self.wal_rx.as_mut().map(|receiver| receiver.try_recv()) {
                        Some(Ok(payload)) => Some(payload),
                        Some(Err(crossfire::TryRecvError::Empty)) | None => None,
                        Some(Err(crossfire::TryRecvError::Disconnected)) => {
                            reader_closed = true;
                            None
                        }
                    }
                } else {
                    None
                };
                let Some(payload) = payload else {
                    break;
                };
                if let Err(e) = self.process_owned_wal_payload(payload) {
                    self.state = ConnectorState::Failed;
                    return Err(e);
                }
                processed_payloads = processed_payloads.checked_add(1).ok_or_else(|| {
                    self.state = ConnectorState::Failed;
                    ConnectorError::Internal(
                        "PostgreSQL CDC poll payload-count accounting overflow".into(),
                    )
                })?;
            }

            // Notify ourselves only when a bounded drain demonstrably left work queued.
            // Retaining one item avoids both a lost coalesced notification and an
            // open-transaction busy loop while the server is genuinely idle.
            let reached_payload_budget = processed_payloads == payload_budget;
            let may_drain_more = if reached_payload_budget && self.current_txn.is_none() {
                self.decoded_retained_bytes().map_err(|error| {
                    self.state = ConnectorState::Failed;
                    error
                })? < high_watermark
            } else {
                reached_payload_budget
            };
            if may_drain_more && !reader_closed {
                if let Some(ref mut rx) = self.wal_rx {
                    match rx.try_recv() {
                        Ok(payload) => self.pending_payloads.push_back(payload),
                        Err(crossfire::TryRecvError::Empty) => {}
                        Err(crossfire::TryRecvError::Disconnected) => reader_closed = true,
                    }
                }
            }
            if !self.pending_payloads.is_empty() {
                self.data_ready.notify_one();
            }
            self.fail_on_terminal_wal_error()?;
            if reader_closed && self.committed_transactions.is_empty() {
                self.state = ConnectorState::Failed;
                return Err(ConnectorError::ReadError(
                    "WAL reader task terminated unexpectedly — replication stream lost".to_string(),
                ));
            }
        }

        #[cfg(test)]
        self.process_pending_messages()?;

        // Drain buffered events into a RecordBatch.
        // Configured Arrow-column extractors derive event-time watermarks from `_ts_ms`.
        // The LSN in PartitionInfo tracks replication progress for offset management.
        let result = match self.drain_events(max_records)? {
            Some(batch) => {
                self.metrics
                    .set_confirmed_flush_lsn(self.confirmed_flush_lsn.as_u64());
                self.metrics
                    .set_replication_lag_bytes(self.replication_lag_bytes());

                let lsn_str = self.polled_lsn.to_string();
                let partition = PartitionInfo::new(&self.config.slot_name, lsn_str);
                Ok(Some(SourceBatch::with_partition(batch, partition)))
            }
            None => Ok(None),
        };
        let emitted_batch = matches!(&result, Ok(Some(_)));
        if max_records > 0 && (emitted_batch || !self.committed_transactions.is_empty()) {
            self.data_ready.notify_one();
        }
        result
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new();
        // polled_lsn = latest position drained into a batch — the resumable point recorded in the
        // manifest. The PG slot is NOT advanced here: doing so per poll lets PG reclaim WAL for
        // data that is only in-pipeline, so a crash loses an LSN range recovery still needs.
        // Slot feedback is deferred to notify_epoch_committed (durable-commit only).
        cp.set_offset("lsn", self.polled_lsn.to_string());
        cp.set_metadata("slot_name", &self.config.slot_name);
        cp.set_metadata("publication", &self.config.publication);
        cp.set_metadata("database", &self.config.database);
        if let Some(binding) = &self.checkpoint_binding {
            write_checkpoint_binding(&mut cp, binding);
        }
        cp
    }

    async fn notify_epoch_committed(
        &mut self,
        epoch: u64,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        // Advance the PG replication slot only after the epoch is durably committed (manifest
        // persisted + sinks committed), so PG never reclaims WAL for data still in-pipeline.
        // The checkpoint carries the exact LSN persisted for this epoch; a timer-driven empty
        // checkpoint has no "lsn" offset and is a no-op.
        let Some(lsn_str) = checkpoint.get_offset("lsn") else {
            return Ok(());
        };
        let lsn = lsn_str.parse::<Lsn>().map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "committed PostgreSQL CDC epoch {epoch} contains invalid LSN '{lsn_str}': {error}"
            ))
        })?;
        let context = format!("committed epoch {epoch} checkpoint");
        let committed_binding = validate_checkpoint_identity(checkpoint, &self.config, &context)?;
        let active_binding =
            self.checkpoint_binding
                .as_ref()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "running PostgreSQL CDC checkpoint binding".into(),
                    actual: "checkpoint binding is missing".into(),
                })?;
        validate_live_binding(&committed_binding, active_binding, &context)?;
        if lsn.as_u64() > self.polled_lsn.as_u64() {
            return Err(ConnectorError::ConfigurationError(format!(
                "committed PostgreSQL CDC epoch {epoch} LSN {lsn} is ahead of the source's polled LSN {}; refusing irreversible slot feedback",
                self.polled_lsn
            )));
        }
        // A strictly stale notification is already satisfied and must never regress either cursor.
        // An equal notification is handed off again: that is idempotent and repairs feedback after
        // a reader restart whose local cursor was restored before its channel was created.
        if lsn.as_u64() < self.confirmed_flush_lsn.as_u64() {
            return Ok(());
        }

        let tx = self
            .confirmed_lsn_tx
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "running PostgreSQL CDC confirmed-LSN feedback channel".into(),
                actual: "feedback channel is missing".into(),
            })?;
        tx.send(lsn.as_u64()).map_err(|_| {
            ConnectorError::ConnectionFailed(
                "PostgreSQL CDC confirmed-LSN feedback channel is closed".into(),
            )
        })?;
        // The local cursor is authoritative only after the reader accepted the handoff.
        self.confirmed_flush_lsn = lsn;
        self.metrics.set_confirmed_flush_lsn(lsn.as_u64());
        Ok(())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        // The replication slot's WAL is reclaimed only as the confirmed-flush LSN advances, which
        // happens on durable commit. Without checkpointing the slot never advances and the source
        // database's WAL fills without bound, so this source is commit-coupled.
        if config.properties().is_empty() {
            self.config.validate()?;
        } else {
            PostgresCdcConfig::from_config(config)?.validate()?;
        }
        Ok(SourceContract::new(
            SourceConsistency::CommitCoupled,
            SourceTopology::Singleton,
        ))
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        // Signal the reader, then abort and join it if cooperative shutdown stalls.
        if let Some(tx) = self.reader_shutdown.take() {
            let _ = tx.send(true);
        }
        if let Some(mut handle) = self.reader_handle.take() {
            if tokio::time::timeout(std::time::Duration::from_secs(5), &mut handle)
                .await
                .is_err()
            {
                tracing::warn!("PostgreSQL CDC reader did not stop before the deadline");
                handle.abort();
                let _ = handle.await;
            }
        }
        self.wal_rx = None;
        self.confirmed_lsn_tx = None;
        self.pending_payloads.clear();
        self.wal_byte_budget = None;
        self.wal_terminal_error = None;

        self.state = ConnectorState::Closed;
        self.committed_transactions.clear();
        self.current_txn = None;
        self.relation_cache.clear();
        self.buffered_event_count = 0;
        self.buffered_event_bytes = 0;
        #[cfg(test)]
        self.pending_messages.clear();
        Ok(())
    }
}

// ── Test helpers ──

#[cfg(test)]
impl PostgresCdcSource {
    /// Injects a pre-built change event directly into the event buffer.
    fn inject_event(&mut self, event: ChangeEvent) {
        let end_lsn = event.lsn;
        self.buffered_event_bytes = self
            .buffered_event_bytes
            .checked_add(retained_event_bytes(&event).expect("test event size must be valid"))
            .expect("test buffered-event bytes must be valid");
        self.committed_transactions.push_back(CommittedTransaction {
            end_lsn,
            events: VecDeque::from([event]),
        });
        self.buffered_event_count += 1;
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
        old_tuple_tag: u8,
        old_values: &[Option<&str>],
        new_values: &[Option<&str>],
    ) -> Vec<u8> {
        assert!(matches!(old_tuple_tag, b'K' | b'O'));
        let mut buf = vec![b'U'];
        buf.extend_from_slice(&relation_id.to_be_bytes());
        buf.push(old_tuple_tag);
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

    fn default_source() -> PostgresCdcSource {
        let mut config = PostgresCdcConfig::default();
        config.ssl_mode = crate::postgres::SslMode::Disable;
        PostgresCdcSource::new(config, None)
    }

    fn test_binding(config: &PostgresCdcConfig) -> PostgresCheckpointBinding {
        PostgresCheckpointBinding {
            system_identifier: 7,
            timeline_id: 1,
            database_oid: 5,
            publication_oid: 16_384,
            publication_definition_sha256: "11".repeat(32),
            source_config_sha256: source_config_digest(config),
            slot_plugin: "pgoutput".into(),
            slot_two_phase: false,
            slot_failover: true,
        }
    }

    fn running_source() -> PostgresCdcSource {
        let mut src = default_source();
        src.state = ConnectorState::Running;
        src.checkpoint_binding = Some(test_binding(&src.config));
        src
    }

    fn recovery_identity_config() -> ConnectorConfig {
        let mut config = ConnectorConfig::new("postgres-cdc");
        config.set("host", "db-a.internal");
        config.set("database", "orders");
        config.set("username", "replicator");
        config.set("password", "secret-a");
        config.set("slot.name", "orders_slot");
        config.set("publication", "orders_pub");
        config.set("table.include", "public.z, public.a");
        config
    }

    // ── Construction ──

    #[test]
    fn test_new_source() {
        let src = default_source();
        assert_eq!(src.state, ConnectorState::Created);
        assert!(src.confirmed_flush_lsn.is_zero());
        assert_eq!(src.buffered_events(), 0);
        assert_eq!(src.schema().fields().len(), 6);
    }

    #[test]
    fn test_source_contract_is_commit_coupled_singleton() {
        // The replication slot's WAL only advances on durable commit, so the pipeline must reject a
        // CDC source without checkpointing or its WAL grows without bound.
        let contract = default_source()
            .contract(&ConnectorConfig::new("postgres-cdc"))
            .unwrap();
        assert_eq!(contract.consistency, SourceConsistency::CommitCoupled);
        assert_eq!(contract.topology, SourceTopology::Singleton);
    }

    #[test]
    fn recovery_identity_ignores_operational_connection_tuning() {
        let left = recovery_identity_config();
        let source = PostgresCdcSource::from_config(&left).unwrap();
        let mut right = recovery_identity_config();
        right.set("host", "db-b.internal");
        right.set("port", "6432");
        right.set("username", "rotated-user");
        right.set("password", "rotated-secret");
        right.set("ssl.mode", "disable");
        right.set("max.buffered.bytes", "134217728");

        let stored = source.recovery_identity_options(&left).unwrap();
        assert_eq!(
            stored,
            source.recovery_identity_options(&right).unwrap(),
            "connection and memory tuning must not fence durable recovery"
        );
        assert_eq!(
            stored,
            source
                .recovery_identity_options(&ConnectorConfig::new("postgres-cdc"))
                .unwrap(),
            "an empty runtime config must use the validated provider config"
        );
    }

    #[test]
    fn recovery_identity_normalizes_filters_and_fences_slot_semantics() {
        let left = recovery_identity_config();
        let source = PostgresCdcSource::from_config(&left).unwrap();
        let mut reordered = recovery_identity_config();
        reordered.set("table.include", "public.a,public.z,public.a");
        assert_eq!(
            source.recovery_identity_options(&left).unwrap(),
            source.recovery_identity_options(&reordered).unwrap(),
            "equivalent filters must have one canonical identity"
        );

        let mut different_slot = recovery_identity_config();
        different_slot.set("slot.name", "other_slot");
        assert_ne!(
            source.recovery_identity_options(&left).unwrap(),
            source.recovery_identity_options(&different_slot).unwrap(),
            "a different replication history must fence recovery"
        );
    }

    #[test]
    fn test_from_config() {
        let mut config = ConnectorConfig::new("postgres-cdc");
        config.set("host", "pg.local");
        config.set("database", "testdb");
        config.set("slot.name", "my_slot");
        config.set("publication", "my_pub");
        config.set("ssl.mode", "disable");

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

    #[tokio::test]
    async fn initial_start_fails_closed_before_external_io() {
        let mut src = default_source();
        let error = src
            .start(SourceStart {
                config: ConnectorConfig::new("postgres-cdc"),
                position: SourcePosition::Initial,
                delivery: crate::connector::DeliveryGuarantee::AtLeastOnce,
            })
            .await
            .expect_err("initial startup must wait for certified snapshot/WAL bootstrap");
        assert!(error.to_string().contains("[LDB-5060]"), "{error}");
        assert_eq!(src.state, ConnectorState::Created);
        assert!(src.reader_handle.is_none());
        assert!(src.wal_rx.is_none());
    }

    #[tokio::test]
    async fn start_normalizes_a_programmatic_filter_before_checkpoint_identity() {
        let mut src = default_source();
        src.config.table_include = vec![
            " public.users ".into(),
            String::new(),
            "public.orders".into(),
            "public.users".into(),
        ];
        let mut expected_config = src.config.clone();
        expected_config.normalize_table_filters();
        let mut checkpoint = src.checkpoint();
        checkpoint.set_offset("lsn", "1/10");
        write_checkpoint_binding(&mut checkpoint, &test_binding(&expected_config));

        src.start(SourceStart {
            config: ConnectorConfig::new("postgres-cdc"),
            position: SourcePosition::Resume {
                attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                checkpoint,
            },
            delivery: crate::connector::DeliveryGuarantee::AtLeastOnce,
        })
        .await
        .unwrap();

        assert_eq!(
            src.config.table_include,
            vec!["public.orders", "public.users"]
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
        assert_eq!(src.buffered_events(), 0);
    }

    #[tokio::test]
    async fn close_interrupts_reader_blocked_on_full_wal_queue() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let mut src = running_source();
        let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(1);
        let payload_bytes = retained_wal_payload_bytes(&WalPayload::KeepAlive { wal_end: 1 });
        let byte_budget = Arc::new(Semaphore::new(payload_bytes * 2));
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        assert!(send_wal_or_shutdown(
            &wal_tx,
            WalPayload::KeepAlive { wal_end: 1 },
            &byte_budget,
            payload_bytes * 2,
            &mut shutdown_rx,
        )
        .await
        .unwrap());
        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_in_task = Arc::clone(&stopped);
        let task_byte_budget = Arc::clone(&byte_budget);
        let reader_handle = tokio::spawn(async move {
            let sent = send_wal_or_shutdown(
                &wal_tx,
                WalPayload::KeepAlive { wal_end: 2 },
                &task_byte_budget,
                payload_bytes * 2,
                &mut shutdown_rx,
            )
            .await;
            stopped_in_task.store(matches!(sent, Ok(false)), Ordering::Release);
        });

        src.wal_rx = Some(wal_rx);
        src.wal_byte_budget = Some(byte_budget);
        src.reader_shutdown = Some(shutdown_tx);
        src.reader_handle = Some(reader_handle);

        tokio::time::timeout(std::time::Duration::from_millis(250), src.close())
            .await
            .expect("close must not wait for WAL queue capacity")
            .unwrap();
        assert!(stopped.load(Ordering::Acquire));
        assert_eq!(src.state, ConnectorState::Closed);
    }

    #[tokio::test]
    async fn oversized_raw_wal_payload_reports_terminal_error_without_waiting_for_capacity() {
        let max_payload_bytes = 128;
        let byte_budget = Arc::new(Semaphore::new(max_payload_bytes));
        let _all_capacity = Arc::clone(&byte_budget)
            .acquire_many_owned(u32::try_from(max_payload_bytes).unwrap())
            .await
            .unwrap();
        let (wal_tx, _wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(1);
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let oversized = WalPayload::XLogData {
            wal_end: 1,
            data: Bytes::from(vec![0; max_payload_bytes]),
        };

        let message = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            send_wal_or_shutdown(
                &wal_tx,
                oversized,
                &byte_budget,
                max_payload_bytes,
                &mut shutdown_rx,
            ),
        )
        .await
        .expect("oversized payload must fail before waiting for byte permits")
        .expect_err("payload and envelope exceed the byte budget");
        assert!(message.contains("hard raw buffer limit"), "{message}");

        let terminal_error: WalTerminalError = Arc::new(std::sync::Mutex::new(None));
        let data_ready = Notify::new();
        publish_terminal_wal_error(&terminal_error, message.clone(), &data_ready);
        let mut source = running_source();
        source.wal_terminal_error = Some(terminal_error);
        let error = source
            .fail_on_terminal_wal_error()
            .expect_err("reader terminal error must fail the source");
        assert!(error.to_string().contains(message.as_str()));
        assert_eq!(source.state, ConnectorState::Failed);
    }

    #[tokio::test]
    async fn raw_wal_budget_backpressures_aggregate_payload_bytes() {
        let first = WalPayload::XLogData {
            wal_end: 1,
            data: Bytes::from_static(&[1; 32]),
        };
        let payload_bytes = retained_wal_payload_bytes(&first);
        let byte_limit = payload_bytes * 2 - 1;
        let byte_budget = Arc::new(Semaphore::new(byte_limit));
        let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(2);
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        assert!(
            send_wal_or_shutdown(&wal_tx, first, &byte_budget, byte_limit, &mut shutdown_rx,)
                .await
                .unwrap()
        );

        let task_budget = Arc::clone(&byte_budget);
        let mut handle = tokio::spawn(async move {
            send_wal_or_shutdown(
                &wal_tx,
                WalPayload::XLogData {
                    wal_end: 2,
                    data: Bytes::from_static(&[2; 32]),
                },
                &task_budget,
                byte_limit,
                &mut shutdown_rx,
            )
            .await
        });

        let first_owned = wal_rx.recv().await.unwrap();
        assert_eq!(byte_budget.available_permits(), payload_bytes - 1);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), &mut handle)
                .await
                .is_err(),
            "receiving must not release byte ownership before processing"
        );
        drop(first_owned);
        assert!(handle.await.unwrap().unwrap());
        let second_owned = wal_rx.recv().await.unwrap();
        drop(second_owned);
        assert_eq!(byte_budget.available_permits(), byte_limit);
    }

    #[tokio::test]
    async fn pending_wal_payload_keeps_its_byte_reservation() {
        let payload = WalPayload::KeepAlive { wal_end: 7 };
        let payload_bytes = retained_wal_payload_bytes(&payload);
        let byte_budget = Arc::new(Semaphore::new(payload_bytes));
        let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(1);
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        assert!(send_wal_or_shutdown(
            &wal_tx,
            payload,
            &byte_budget,
            payload_bytes,
            &mut shutdown_rx,
        )
        .await
        .unwrap());

        let mut source = running_source();
        source
            .pending_payloads
            .push_back(wal_rx.recv().await.unwrap());
        assert_eq!(byte_budget.available_permits(), 0);
        let pending = source.pending_payloads.pop_front().unwrap();
        source.process_owned_wal_payload(pending).unwrap();
        assert_eq!(source.write_lsn, Lsn::new(7));
        assert_eq!(byte_budget.available_permits(), payload_bytes);
    }

    #[tokio::test]
    async fn owned_wal_path_records_boundary_bytes_once() {
        let begin = WalPayload::Begin {
            final_lsn: 0x100,
            commit_ts_us: 0,
            xid: 1,
        };
        let commit = WalPayload::Commit {
            end_lsn: 0x200,
            commit_ts_us: 0,
            lsn: 0x100,
        };
        let expected_bytes = logical_wal_payload_bytes(&begin)
            .checked_add(logical_wal_payload_bytes(&commit))
            .unwrap();
        let byte_limit = retained_wal_payload_bytes(&begin)
            .checked_add(retained_wal_payload_bytes(&commit))
            .unwrap();
        let byte_budget = Arc::new(Semaphore::new(byte_limit));
        let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(2);
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        assert!(
            send_wal_or_shutdown(&wal_tx, begin, &byte_budget, byte_limit, &mut shutdown_rx,)
                .await
                .unwrap()
        );
        assert!(
            send_wal_or_shutdown(&wal_tx, commit, &byte_budget, byte_limit, &mut shutdown_rx,)
                .await
                .unwrap()
        );

        let mut source = running_source();
        source
            .process_owned_wal_payload(wal_rx.recv().await.unwrap())
            .unwrap();
        source
            .process_owned_wal_payload(wal_rx.recv().await.unwrap())
            .unwrap();

        assert_eq!(
            source.metrics.bytes_received.get(),
            u64::try_from(expected_bytes).unwrap()
        );
        assert_eq!(byte_budget.available_permits(), byte_limit);
    }

    #[tokio::test]
    async fn decoded_byte_high_watermark_stops_raw_lookahead() {
        let mut source = running_source();
        source.config.max_buffered_bytes = 1024 * 1024;
        let relation_name = "x".repeat(source.config.decoded_high_watermark_bytes());
        let relation = WalPayload::XLogData {
            wal_end: 1,
            data: Bytes::from(PostgresCdcSource::build_relation_message(
                1,
                "public",
                &relation_name,
                &[],
            )),
        };
        let keepalive = WalPayload::KeepAlive { wal_end: 2 };
        let byte_limit = source.config.raw_wal_bytes();
        let byte_budget = Arc::new(Semaphore::new(byte_limit));
        let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(2);
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        assert!(send_wal_or_shutdown(
            &wal_tx,
            relation,
            &byte_budget,
            byte_limit,
            &mut shutdown_rx,
        )
        .await
        .unwrap());
        assert!(send_wal_or_shutdown(
            &wal_tx,
            keepalive,
            &byte_budget,
            byte_limit,
            &mut shutdown_rx,
        )
        .await
        .unwrap());
        source.wal_rx = Some(wal_rx);
        source.wal_byte_budget = Some(byte_budget);

        assert!(source.poll_batch(1).await.unwrap().is_none());
        assert_eq!(source.relation_cache.len(), 1);
        assert!(
            source.decoded_retained_bytes().unwrap()
                >= source.config.decoded_high_watermark_bytes()
        );
        assert!(source.pending_payloads.is_empty());
        assert_eq!(source.write_lsn, Lsn::new(1));
    }

    #[tokio::test]
    async fn bounded_poll_self_notifies_when_an_open_transaction_has_queued_work() {
        let begin = WalPayload::Begin {
            final_lsn: 0x100,
            commit_ts_us: 0,
            xid: 1,
        };
        let commit = WalPayload::Commit {
            end_lsn: 0x200,
            commit_ts_us: 0,
            lsn: 0x100,
        };
        let byte_limit =
            retained_wal_payload_bytes(&begin).saturating_add(retained_wal_payload_bytes(&commit));
        let byte_budget = Arc::new(Semaphore::new(byte_limit));
        let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(2);
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        assert!(
            send_wal_or_shutdown(&wal_tx, begin, &byte_budget, byte_limit, &mut shutdown_rx,)
                .await
                .unwrap()
        );
        assert!(
            send_wal_or_shutdown(&wal_tx, commit, &byte_budget, byte_limit, &mut shutdown_rx,)
                .await
                .unwrap()
        );

        let mut source = running_source();
        source.wal_rx = Some(wal_rx);
        source.wal_byte_budget = Some(byte_budget);
        assert!(source.poll_batch(1).await.unwrap().is_none());
        assert!(source.current_txn.is_some());
        assert_eq!(source.pending_payloads.len(), 1);
        tokio::time::timeout(
            std::time::Duration::from_millis(25),
            source.data_ready.notified(),
        )
        .await
        .expect("queued protocol work must leave a readiness permit");
        assert!(source.poll_batch(1).await.unwrap().is_none());
        assert!(source.current_txn.is_none());
        assert!(source.pending_payloads.is_empty());
    }

    #[tokio::test]
    async fn close_interrupts_reader_waiting_for_raw_wal_byte_budget() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let mut source = running_source();
        let payload = WalPayload::KeepAlive { wal_end: 9 };
        let payload_bytes = retained_wal_payload_bytes(&payload);
        let byte_budget = Arc::new(Semaphore::new(payload_bytes));
        let held_capacity = Arc::clone(&byte_budget)
            .acquire_many_owned(u32::try_from(payload_bytes).unwrap())
            .await
            .unwrap();
        let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(1);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let task_budget = Arc::clone(&byte_budget);
        let stopped = Arc::new(AtomicBool::new(false));
        let task_stopped = Arc::clone(&stopped);
        let reader_handle = tokio::spawn(async move {
            let sent = send_wal_or_shutdown(
                &wal_tx,
                payload,
                &task_budget,
                payload_bytes,
                &mut shutdown_rx,
            )
            .await;
            task_stopped.store(matches!(sent, Ok(false)), Ordering::Release);
        });

        source.wal_rx = Some(wal_rx);
        source.wal_byte_budget = Some(byte_budget);
        source.reader_shutdown = Some(shutdown_tx);
        source.reader_handle = Some(reader_handle);
        tokio::time::timeout(std::time::Duration::from_millis(250), source.close())
            .await
            .expect("close must interrupt a WAL byte-permit wait")
            .unwrap();
        drop(held_capacity);
        assert!(stopped.load(Ordering::Acquire));
        assert_eq!(source.state, ConnectorState::Closed);
    }

    // ── Checkpoint / Restore ──

    #[test]
    fn test_checkpoint() {
        let mut src = running_source();
        src.confirmed_flush_lsn = "1/ABCD".parse().unwrap();
        src.polled_lsn = "1/ABCD".parse().unwrap();
        src.write_lsn = "1/ABCE".parse().unwrap();

        let cp = src.checkpoint();
        assert_eq!(cp.get_offset("lsn"), Some("1/ABCD"));
        assert_eq!(cp.get_offset("write_lsn"), None);
        assert_eq!(cp.get_metadata("slot_name"), Some("laminar_slot"));
        assert_eq!(cp.get_metadata("checkpoint_version"), Some("3"));
        assert_eq!(cp.get_metadata(SYSTEM_IDENTIFIER_METADATA), Some("7"));
        assert_eq!(cp.get_metadata(TIMELINE_ID_METADATA), Some("1"));
        assert_eq!(cp.get_metadata(SLOT_PLUGIN_METADATA), Some("pgoutput"));
    }

    fn committed_lsn_checkpoint(lsn: &str) -> SourceCheckpoint {
        let source = default_source();
        let mut checkpoint = source.checkpoint();
        checkpoint.set_offset("lsn", lsn);
        write_checkpoint_binding(&mut checkpoint, &test_binding(&source.config));
        checkpoint
    }

    #[tokio::test]
    async fn committed_epoch_rejects_malformed_durable_lsn() {
        let mut source = default_source();
        let error = source
            .notify_epoch_committed(7, &committed_lsn_checkpoint("not-an-lsn"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("invalid LSN"), "{error}");
        assert!(source.confirmed_flush_lsn.is_zero());
    }

    #[tokio::test]
    async fn committed_epoch_rejects_missing_feedback_channel() {
        let mut source = running_source();
        source.polled_lsn = "1/10".parse().unwrap();
        let error = source
            .notify_epoch_committed(7, &committed_lsn_checkpoint("1/10"))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("feedback channel is missing"),
            "{error}"
        );
        assert!(source.confirmed_flush_lsn.is_zero());
    }

    #[tokio::test]
    async fn committed_epoch_rejects_closed_feedback_without_advancing_local_lsn() {
        let mut source = running_source();
        source.polled_lsn = "1/10".parse().unwrap();
        let (feedback_tx, feedback_rx) = tokio::sync::watch::channel(0);
        drop(feedback_rx);
        source.confirmed_lsn_tx = Some(feedback_tx);

        let error = source
            .notify_epoch_committed(7, &committed_lsn_checkpoint("1/10"))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("feedback channel is closed"),
            "{error}"
        );
        assert!(source.confirmed_flush_lsn.is_zero());
    }

    #[tokio::test]
    async fn committed_epoch_advances_local_lsn_only_after_feedback_handoff() {
        let mut source = running_source();
        source.polled_lsn = "1/10".parse().unwrap();
        let (feedback_tx, mut feedback_rx) = tokio::sync::watch::channel(0);
        source.confirmed_lsn_tx = Some(feedback_tx);

        source
            .notify_epoch_committed(7, &committed_lsn_checkpoint("1/10"))
            .await
            .unwrap();
        let expected = "1/10".parse::<Lsn>().unwrap();
        assert_eq!(source.confirmed_flush_lsn, expected);
        assert_eq!(*feedback_rx.borrow_and_update(), expected.as_u64());
    }

    #[tokio::test]
    async fn committed_epoch_ahead_of_polled_lsn_leaves_feedback_and_cursor_unchanged() {
        let mut source = running_source();
        source.confirmed_flush_lsn = "1/8".parse().unwrap();
        source.polled_lsn = "1/10".parse().unwrap();
        let (feedback_tx, mut feedback_rx) = tokio::sync::watch::channel(0x1008);
        source.confirmed_lsn_tx = Some(feedback_tx);

        let error = source
            .notify_epoch_committed(7, &committed_lsn_checkpoint("1/11"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("ahead of"), "{error}");
        assert_eq!(source.confirmed_flush_lsn, "1/8".parse().unwrap());
        assert_eq!(*feedback_rx.borrow_and_update(), 0x1008);
    }

    #[tokio::test]
    async fn committed_epoch_rejects_binding_drift_before_feedback() {
        let mut source = running_source();
        source.polled_lsn = "1/10".parse().unwrap();
        let (feedback_tx, mut feedback_rx) = tokio::sync::watch::channel(0);
        source.confirmed_lsn_tx = Some(feedback_tx);
        let mut checkpoint = committed_lsn_checkpoint("1/10");
        checkpoint.set_metadata(PUBLICATION_OID_METADATA, "16385");

        let error = source
            .notify_epoch_committed(7, &checkpoint)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("identity drifted"), "{error}");
        assert!(source.confirmed_flush_lsn.is_zero());
        assert_eq!(*feedback_rx.borrow_and_update(), 0);
    }

    #[tokio::test]
    async fn confirmed_lsn_watch_wakes_without_a_replication_event() {
        let (feedback_tx, mut feedback_rx) = tokio::sync::watch::channel(0);
        feedback_tx.send(0x1234).unwrap();

        tokio::time::timeout(std::time::Duration::from_millis(25), feedback_rx.changed())
            .await
            .expect("confirmed LSN notification must wake the reader select")
            .unwrap();
        assert_eq!(
            take_confirmed_lsn(&mut feedback_rx).unwrap().as_u64(),
            0x1234
        );
    }

    #[tokio::test]
    async fn committed_epoch_never_regresses_confirmed_lsn() {
        let mut source = running_source();
        source.confirmed_flush_lsn = "2/20".parse().unwrap();
        source.polled_lsn = "2/20".parse().unwrap();

        source
            .notify_epoch_committed(6, &committed_lsn_checkpoint("1/10"))
            .await
            .unwrap();
        assert_eq!(source.confirmed_flush_lsn, "2/20".parse().unwrap());
    }

    #[tokio::test]
    async fn test_resume_installs_exact_engine_lsn() {
        let mut src = default_source();
        let cp = committed_lsn_checkpoint("2/FF00");

        let result = src
            .start(SourceStart {
                config: ConnectorConfig::new("postgres-cdc"),
                position: SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                    checkpoint: cp,
                },
                delivery: crate::connector::DeliveryGuarantee::AtLeastOnce,
            })
            .await;
        result.unwrap();
        assert_eq!(src.confirmed_flush_lsn.as_u64(), 0x2_0000_FF00);
        assert_eq!(src.polled_lsn.as_u64(), 0x2_0000_FF00);
        assert_eq!(
            src.write_lsn.as_u64(),
            0x2_0000_FF00,
            "diagnostic write_lsn starts at the durable recovery cursor"
        );
    }

    #[tokio::test]
    async fn test_resume_invalid_lsn_fails_before_replication() {
        let mut src = default_source();
        let cp = committed_lsn_checkpoint("not_an_lsn");

        let error = src
            .start(SourceStart {
                config: ConnectorConfig::new("postgres-cdc"),
                position: SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                    checkpoint: cp,
                },
                delivery: crate::connector::DeliveryGuarantee::AtLeastOnce,
            })
            .await
            .expect_err("invalid durable LSN must fail closed");
        assert!(error.to_string().contains("invalid LSN"));
        assert_eq!(src.state, ConnectorState::Created);
    }

    #[tokio::test]
    async fn old_checkpoint_version_fails_without_installing_runtime_state() {
        let mut src = default_source();
        let mut checkpoint = committed_lsn_checkpoint("1/10");
        checkpoint.set_metadata("checkpoint_version", "2");

        let error = src
            .start(SourceStart {
                config: ConnectorConfig::new("postgres-cdc"),
                position: SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                    checkpoint,
                },
                delivery: crate::connector::DeliveryGuarantee::AtLeastOnce,
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("expected '3'"), "{error}");
        assert_eq!(src.state, ConnectorState::Created);
        assert!(src.checkpoint_binding.is_none());
        assert!(src.reader_handle.is_none());
        assert!(src.wal_rx.is_none());
        assert!(src.confirmed_lsn_tx.is_none());
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
        assert_eq!(table_col.value(0), "public.users");

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

    #[tokio::test]
    async fn decoded_container_growth_is_charged_before_the_rejected_event() {
        let mut src = running_source();
        src.config.max_buffered_bytes = 1024 * 1024;
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            100,
            "public",
            "orders",
            &[(1, "id", INT4_OID, -1)],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x400, 0, 1));
        for _ in 0..10_000 {
            src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("1")]));
        }

        let error = src.poll_batch(100).await.unwrap_err();
        assert!(error.to_string().contains("decoded-stage buffer limit"));
        assert_eq!(src.state, ConnectorState::Failed);
        assert!(src.buffered_event_count > 0);
        assert_eq!(
            src.current_txn.as_ref().unwrap().events.len(),
            src.buffered_event_count
        );
        assert!(src.decoded_retained_bytes().unwrap() <= src.config.decoded_event_bytes());
        assert!(src.committed_transactions.is_empty());
        assert!(src.write_lsn.is_zero());
        assert!(src.polled_lsn.is_zero());
        assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/0"));
    }

    #[tokio::test]
    async fn relation_cache_growth_is_bounded_by_the_decoded_stage() {
        let mut src = running_source();
        src.config.max_buffered_bytes = 1024 * 1024;
        for relation_id in 1..=5_000 {
            let name = format!("t{relation_id}");
            src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
                relation_id,
                "public",
                &name,
                &[(1, "id", INT4_OID, -1)],
            ));
        }

        let error = src.poll_batch(100).await.unwrap_err();
        assert!(error.to_string().contains("relation-cache"), "{error}");
        assert_eq!(src.state, ConnectorState::Failed);
        assert!(!src.relation_cache.is_empty());
        assert!(src.decoded_retained_bytes().unwrap() <= src.config.decoded_event_bytes());
    }

    #[test]
    fn relation_replacement_charges_only_retained_growth() {
        let mut src = running_source();
        let relation = RelationInfo {
            relation_id: 1,
            namespace: "public".to_string(),
            name: "orders".to_string(),
            replica_identity: 'd',
            columns: Vec::new(),
        };
        src.admit_relation(relation.clone()).unwrap();
        let retained = src.decoded_retained_bytes().unwrap();
        src.config.max_buffered_bytes = retained.checked_mul(3).unwrap();

        src.admit_relation(relation).unwrap();

        assert_eq!(src.relation_cache.len(), 1);
        assert_eq!(src.decoded_retained_bytes().unwrap(), retained);
    }

    #[tokio::test]
    async fn json_escape_expansion_is_rejected_by_the_total_byte_limit() {
        let mut src = running_source();
        src.config.max_buffered_bytes = 1024 * 1024;
        let oversized_value = "\n".repeat(src.config.decoded_event_bytes() / 2 + 128);
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            100,
            "public",
            "orders",
            &[(1, "payload", TEXT_OID, -1)],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x400, 0, 1));
        let insert = PostgresCdcSource::build_insert_message(100, &[Some(&oversized_value)]);
        assert!(insert.len() <= src.config.raw_wal_bytes());
        src.enqueue_wal_data(insert);

        let error = src.poll_batch(100).await.unwrap_err();
        assert!(error.to_string().contains("retained bytes"));
        assert_eq!(src.state, ConnectorState::Failed);
        assert_eq!(src.buffered_event_count, 0);
        assert_eq!(src.buffered_event_bytes, 0);
        assert!(src.current_txn.as_ref().unwrap().events.is_empty());
        assert!(src.write_lsn.is_zero());
        assert!(src.polled_lsn.is_zero());
    }

    #[tokio::test]
    async fn row_change_outside_transaction_fails_closed() {
        let mut src = running_source();
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            100,
            "public",
            "orders",
            &[(1, "id", INT4_OID, -1)],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("1")]));

        let error = src.poll_batch(100).await.unwrap_err();
        assert!(error.to_string().contains("outside a transaction"));
        assert_eq!(src.buffered_events(), 0);
    }

    #[test]
    fn corrupt_commit_boundaries_fail_closed() {
        let cases = [
            super::super::decoder::CommitMessage {
                flags: 0,
                commit_lsn: Lsn::new(0x101),
                end_lsn: Lsn::new(0x200),
                commit_ts_ms: 7,
            },
            super::super::decoder::CommitMessage {
                flags: 0,
                commit_lsn: Lsn::new(0x100),
                end_lsn: Lsn::new(0x0ff),
                commit_ts_ms: 7,
            },
            super::super::decoder::CommitMessage {
                flags: 0,
                commit_lsn: Lsn::new(0x100),
                end_lsn: Lsn::new(0x200),
                commit_ts_ms: 8,
            },
        ];

        for commit in cases {
            let mut src = running_source();
            src.process_wal_message(WalMessage::Begin(super::super::decoder::BeginMessage {
                final_lsn: Lsn::new(0x100),
                commit_ts_ms: 7,
                xid: 1,
            }))
            .unwrap();
            let error = src
                .process_wal_message(WalMessage::Commit(commit))
                .unwrap_err();
            assert_eq!(src.state, ConnectorState::Failed, "{error}");
            assert!(src.committed_transactions.is_empty());
            assert!(src.current_txn.is_some());
        }
    }

    #[test]
    fn commit_end_lsn_cannot_move_behind_a_queued_transaction() {
        let mut src = running_source();
        for (final_lsn, end_lsn) in [(0x100, 0x300), (0x200, 0x250)] {
            src.process_wal_message(WalMessage::Begin(super::super::decoder::BeginMessage {
                final_lsn: Lsn::new(final_lsn),
                commit_ts_ms: 0,
                xid: 1,
            }))
            .unwrap();
            let result =
                src.process_wal_message(WalMessage::Commit(super::super::decoder::CommitMessage {
                    flags: 0,
                    commit_lsn: Lsn::new(final_lsn),
                    end_lsn: Lsn::new(end_lsn),
                    commit_ts_ms: 0,
                }));
            if end_lsn == 0x250 {
                let error = result.unwrap_err();
                assert!(error.to_string().contains("behind"), "{error}");
            } else {
                result.unwrap();
            }
        }
        assert_eq!(src.state, ConnectorState::Failed);
        assert_eq!(src.committed_transactions.len(), 1);
        assert!(src.current_txn.is_some());
    }

    #[test]
    fn vendor_timestamp_overflow_is_rejected() {
        let mut src = running_source();
        let error = src
            .process_wal_payload(WalPayload::Begin {
                final_lsn: 0x100,
                commit_ts_us: i64::MAX,
                xid: 1,
            })
            .unwrap_err();
        assert!(error.to_string().contains("timestamp"), "{error}");
        assert!(src.current_txn.is_none());
    }

    #[test]
    fn malformed_raw_boundary_is_not_silently_skipped() {
        let mut src = running_source();
        let mut data = PostgresCdcSource::build_begin_message(0x100, 0, 1);
        data.push(0xff);
        let error = src
            .process_wal_payload(WalPayload::XLogData {
                wal_end: 0x100,
                data: Bytes::from(data),
            })
            .unwrap_err();
        assert!(error.to_string().contains("trailing bytes"), "{error}");
        assert!(src.current_txn.is_none());
        assert!(src.write_lsn.is_zero());
    }

    #[tokio::test]
    async fn checkpoint_stays_before_open_transaction_when_write_lsn_advances() {
        let mut src = running_source();
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            100,
            "public",
            "orders",
            &[(1, "id", INT4_OID, -1)],
        ));

        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("1")]));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));
        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x300, 0, 2));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("2")]));

        let batch = src.poll_batch(100).await.unwrap().unwrap();
        assert_eq!(batch.partition.unwrap().offset, "0/200");
        assert!(src.current_txn.is_some());

        src.process_wal_payload(WalPayload::KeepAlive { wal_end: 0x500 })
            .unwrap();
        assert_eq!(src.write_lsn.as_u64(), 0x500);
        assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/200"));
    }

    #[tokio::test]
    async fn batch_target_never_splits_a_committed_transaction() {
        let mut src = running_source();
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            100,
            "public",
            "orders",
            &[(1, "id", INT4_OID, -1)],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x400, 0, 1));
        for id in ["1", "2", "3"] {
            src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some(id)]));
        }
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x400, 0x500, 0));

        let first = src.poll_batch(2).await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 3);
        assert_eq!(first.partition.unwrap().offset, "0/500");
        assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/500"));
        assert!(src.poll_batch(2).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn batch_target_stops_before_the_next_whole_transaction() {
        let mut src = running_source();
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            100,
            "public",
            "orders",
            &[(1, "id", INT4_OID, -1)],
        ));
        for (xid, ids, final_lsn, end_lsn) in
            [(1, ["1", "2"], 0x100, 0x200), (2, ["3", "4"], 0x300, 0x400)]
        {
            src.enqueue_wal_data(PostgresCdcSource::build_begin_message(final_lsn, 0, xid));
            for id in ids {
                src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some(id)]));
            }
            src.enqueue_wal_data(PostgresCdcSource::build_commit_message(
                final_lsn, end_lsn, 0,
            ));
        }

        let first = src.poll_batch(3).await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 2);
        assert_eq!(first.partition.unwrap().offset, "0/200");
        let second = src.poll_batch(3).await.unwrap().unwrap();
        assert_eq!(second.num_rows(), 2);
        assert_eq!(second.partition.unwrap().offset, "0/400");
    }

    #[tokio::test]
    async fn buffered_whole_transaction_wakes_an_event_driven_next_poll() {
        let mut src = running_source();
        src.inject_event(ChangeEvent {
            table: "public.orders".into(),
            op: CdcOperation::Insert,
            lsn: Lsn::new(0x100),
            ts_ms: 0,
            before: None,
            after: Some("{\"id\":\"1\"}".into()),
        });
        src.inject_event(ChangeEvent {
            table: "public.orders".into(),
            op: CdcOperation::Insert,
            lsn: Lsn::new(0x200),
            ts_ms: 0,
            before: None,
            after: Some("{\"id\":\"2\"}".into()),
        });
        let ready = src.data_ready_notify().unwrap();

        let first = src.poll_batch(1).await.unwrap().unwrap();
        assert_eq!(first.partition.unwrap().offset, "0/100");
        tokio::time::timeout(std::time::Duration::from_millis(25), ready.notified())
            .await
            .expect("a buffered committed transaction must retain a readiness permit");
        let second = src.poll_batch(1).await.unwrap().unwrap();
        assert_eq!(second.partition.unwrap().offset, "0/200");
    }

    #[tokio::test]
    async fn zero_capacity_poll_does_not_self_wake() {
        let mut src = running_source();
        src.inject_event(ChangeEvent {
            table: "public.orders".into(),
            op: CdcOperation::Insert,
            lsn: Lsn::new(0x100),
            ts_ms: 0,
            before: None,
            after: Some("{\"id\":\"1\"}".into()),
        });
        let ready = src.data_ready_notify().unwrap();

        assert!(src.poll_batch(0).await.unwrap().is_none());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), ready.notified())
                .await
                .is_err(),
            "a zero-capacity poll must not create a readiness busy loop"
        );
    }

    #[tokio::test]
    async fn final_batch_rearms_raw_polling_once() {
        let mut src = running_source();
        src.inject_event(ChangeEvent {
            table: "public.orders".into(),
            op: CdcOperation::Insert,
            lsn: Lsn::new(0x100),
            ts_ms: 0,
            before: None,
            after: Some("{\"id\":\"1\"}".into()),
        });
        let ready = src.data_ready_notify().unwrap();

        src.poll_batch(1).await.unwrap().unwrap();
        tokio::time::timeout(std::time::Duration::from_millis(25), ready.notified())
            .await
            .expect("the final batch must re-arm raw WAL polling");
        assert!(src.poll_batch(1).await.unwrap().is_none());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), ready.notified())
                .await
                .is_err(),
            "an empty follow-up poll must quiesce"
        );
    }

    #[tokio::test]
    async fn empty_filtered_transaction_advances_only_in_wal_order() {
        let mut config = PostgresCdcConfig::default();
        config.ssl_mode = crate::postgres::SslMode::Disable;
        config.table_exclude = vec!["public.users".to_string()];
        let mut src = PostgresCdcSource::new(config, None);
        src.state = ConnectorState::Running;
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            100,
            "public",
            "orders",
            &[(1, "id", INT4_OID, -1)],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            101,
            "public",
            "users",
            &[(1, "id", INT4_OID, -1)],
        ));

        for (xid, relation, id, commit_lsn) in [
            (1, 100, "1", 0x100),
            (2, 101, "2", 0x200),
            (3, 100, "3", 0x300),
        ] {
            src.enqueue_wal_data(PostgresCdcSource::build_begin_message(
                commit_lsn - 1,
                0,
                xid,
            ));
            src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
                relation,
                &[Some(id)],
            ));
            if xid == 1 {
                src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
                    relation,
                    &[Some("11")],
                ));
            }
            src.enqueue_wal_data(PostgresCdcSource::build_commit_message(
                commit_lsn - 1,
                commit_lsn,
                0,
            ));
        }

        let first = src.poll_batch(1).await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 2);
        assert_eq!(first.partition.unwrap().offset, "0/200");
        let second = src.poll_batch(1).await.unwrap().unwrap();
        assert_eq!(second.partition.unwrap().offset, "0/300");
        assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/300"));
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
            b'O',
            &[Some("42"), Some("Alice")],
            &[Some("42"), Some("Bob")],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

        let batch = src.poll_batch(100).await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let op_col = batch.records.column(1).as_string::<i32>();
        assert_eq!(op_col.value(0), "U");

        let before = batch.records.column(4).as_string::<i32>();
        let before: serde_json::Value = serde_json::from_str(before.value(0)).unwrap();
        assert_eq!(before["id"], "42");
        assert_eq!(before["name"], "Alice");

        let after = batch.records.column(5).as_string::<i32>();
        let after: serde_json::Value = serde_json::from_str(after.value(0)).unwrap();
        assert_eq!(after["id"], "42");
        assert_eq!(after["name"], "Bob");
    }

    #[tokio::test]
    async fn key_update_before_image_omits_non_identity_fields() {
        let mut src = running_source();
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            16384,
            "public",
            "users",
            &[(1, "id", INT8_OID, -1), (0, "name", TEXT_OID, -1)],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
        src.enqueue_wal_data(PostgresCdcSource::build_update_message(
            16384,
            b'K',
            &[Some("41"), None],
            &[Some("42"), Some("Alice")],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

        let batch = src.poll_batch(100).await.unwrap().unwrap();
        let before = batch.records.column(4).as_string::<i32>();
        let before: serde_json::Value = serde_json::from_str(before.value(0)).unwrap();
        assert_eq!(before["id"], "41");
        assert!(before.get("name").is_none());
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
            &[Some("42"), None],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

        let batch = src.poll_batch(100).await.unwrap().unwrap();
        let op_col = batch.records.column(1).as_string::<i32>();
        assert_eq!(op_col.value(0), "D");

        let before = batch.records.column(4).as_string::<i32>();
        let before: serde_json::Value = serde_json::from_str(before.value(0)).unwrap();
        assert_eq!(before["id"], "42");
        assert!(before.get("name").is_none());
        assert!(batch.records.column(5).is_null(0));
    }

    // ── Table filtering ──

    #[tokio::test]
    async fn test_table_exclude_filter() {
        let mut config = PostgresCdcConfig::default();
        config.table_exclude = vec!["public.users".to_string()];
        let mut src = PostgresCdcSource::new(config, None);
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
        assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/200"));
    }

    #[tokio::test]
    async fn public_qualified_include_matches_runtime_table_name() {
        let mut config = PostgresCdcConfig::default();
        config.table_include = vec!["public.users".to_string()];
        let mut src = PostgresCdcSource::new(config, None);
        src.state = ConnectorState::Running;
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            16_384,
            "public",
            "users",
            &[(1, "id", INT8_OID, -1)],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
            16_384,
            &[Some("1")],
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

        let batch = src.poll_batch(100).await.unwrap().unwrap();
        let table = batch.records.column(0).as_string::<i32>();
        assert_eq!(table.value(0), "public.users");
    }

    // ── Max poll records batching ──

    #[tokio::test]
    async fn test_poll_batch_honors_engine_limit() {
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
        let committed_lsn = "1/ABCD".parse().unwrap();
        src.write_lsn = committed_lsn;

        src.inject_event(ChangeEvent {
            table: "t".to_string(),
            op: CdcOperation::Insert,
            lsn: committed_lsn,
            ts_ms: 0,
            before: None,
            after: Some("{}".to_string()),
        });

        let batch = src.poll_batch(100).await.unwrap().unwrap();
        let partition = batch.partition.unwrap();
        assert_eq!(partition.id, "laminar_slot");
        assert_eq!(partition.offset, "1/ABCD");
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
        assert_eq!(table_col.value(0), "public.users");
        assert_eq!(table_col.value(1), "public.orders");
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

    // ── Resume identity validation ──

    #[tokio::test]
    async fn test_resume_rejects_slot_identity_mismatch() {
        let mut src = default_source();
        let mut cp = committed_lsn_checkpoint("2/FF00");
        cp.set_metadata("slot_name", "different_slot");

        let error = src
            .start(SourceStart {
                config: ConnectorConfig::new("postgres-cdc"),
                position: SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                    checkpoint: cp,
                },
                delivery: crate::connector::DeliveryGuarantee::AtLeastOnce,
            })
            .await
            .expect_err("checkpoint for another slot must fail closed");
        assert!(error.to_string().contains("different_slot"));
        assert_eq!(src.state, ConnectorState::Created);
    }

    // ── Backpressure (no event dropping) ──

    #[tokio::test]
    async fn test_backpressure_does_not_drop_buffered_events() {
        let mut src = running_source();

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
        assert_eq!(src.buffered_events(), 200);

        // poll_batch drains events from the buffer — no dropping.
        let batch = src.poll_batch(50).await.unwrap().unwrap();
        assert_eq!(batch.records.num_rows(), 50);
        // 200 - 50 drained = 150 remaining. No events dropped.
        assert_eq!(src.buffered_events(), 150);
    }
}
