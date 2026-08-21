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
    ConnectorTaskOwner, ConnectorTaskTracker, SourceBatch, SourceConnector, SourceContract,
    SourcePosition, SourceStart,
};
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

    /// Admission authority and terminal observer for this connector generation.
    task_owner: ConnectorTaskOwner,
    task_tracker: ConnectorTaskTracker,
}

impl Drop for PostgresCdcSource {
    fn drop(&mut self) {
        if let Some(shutdown) = self.reader_shutdown.take() {
            shutdown.send_replace(true);
        }
        if let Some(handle) = self.reader_handle.take() {
            reap_postgres_reader(handle, &self.task_owner);
        }
    }
}

fn reap_postgres_reader(handle: tokio::task::JoinHandle<()>, task_owner: &ConnectorTaskOwner) {
    let Some(reaper_guard) = task_owner.track() else {
        tracing::warn!("PostgreSQL CDC task generation was sealed before reader reaping");
        return;
    };
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        // The reader's own guard remains authoritative. Dropping its runtime
        // destroys the future and therefore resolves the generation tracker.
        drop(reaper_guard);
        return;
    };
    drop(runtime.spawn(async move {
        let _reaper_guard = reaper_guard;
        if let Err(error) = handle.await {
            tracing::debug!(%error, "PostgreSQL CDC retired reader task reaped");
        }
    }));
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

mod accounting;
mod checkpoint;
mod decoding;
mod drain;
mod lifecycle;
mod reader;
#[cfg(not(test))]
mod startup;

use accounting::{conservative_deque_growth_bytes, planned_event_bytes, retained_event_bytes};
use checkpoint::{validate_checkpoint_identity, validate_live_binding, write_checkpoint_binding};
#[cfg(not(test))]
use reader::run_wal_reader;
use reader::{
    logical_wal_payload_bytes, OwnedWalPayload, WalPayload, WalPayloadRx, WalTerminalError,
};
#[cfg(test)]
use reader::{
    publish_terminal_wal_error, retained_wal_payload_bytes, send_wal_or_shutdown,
    take_confirmed_lsn,
};

impl PostgresCdcSource {
    /// Creates a new `PostgreSQL` CDC source with the given configuration.
    #[must_use]
    pub fn new(mut config: PostgresCdcConfig, registry: Option<&prometheus::Registry>) -> Self {
        config.normalize_table_filters();
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
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
            task_owner,
            task_tracker,
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
mod tests;
