//! Network startup: contract validation, the restart matrix for a committed cursor, and the
//! replication reader launch.

use arrow_schema::SchemaRef;

use super::super::config::{PostgresCdcConfig, SnapshotMode};
use super::super::postgres_io::{
    self, inspect_capture_table, inspect_source, unreadable_columns, CaptureTable,
    ControlConnection, PostgresCheckpointBinding,
};
use super::super::schema::RelationInfo;
use super::super::schema_resolution::{bind_layout, validate_relation};
use super::super::typed_rows::RowLayout;
use super::checkpoint::{validate_live_binding, Cursor, CursorPhase};
use super::claim::{busy, report_orphans, settle, warn_orphaning, ResumeAction, SlotClaim};
use super::reader::{run_wal_reader, OwnedWalPayload, WalPayloadRx, WalTerminalError};
use super::{
    Arc, ConnectorError, ConnectorTaskOwner, Lsn, Notify, Semaphore, PGWIRE_IN_FLIGHT_EVENTS,
    RAW_WAL_QUEUE_CAPACITY,
};

/// Where a prepared source begins.
pub(super) enum StartPhase {
    /// Hold intake under `claim`; create its slot once a checkpoint carrying the claim commits,
    /// or at once when `committed`.
    Claim { claim: SlotClaim, committed: bool },
    /// The claim's existing slot was adopted at `consistent_point`; streaming waits for a
    /// committed cursor naming it.
    AwaitStream {
        claim: SlotClaim,
        consistent_point: Lsn,
    },
    /// Stream the claim's existing slot from `lsn`.
    Stream {
        claim: SlotClaim,
        consistent_point: Lsn,
        lsn: Lsn,
        runtime: ReaderRuntime,
    },
}

/// Everything a successful startup installs at once.
pub(super) struct PreparedStart {
    pub(super) layout: RowLayout,
    pub(super) relation: RelationInfo,
    pub(super) binding: PostgresCheckpointBinding,
    pub(super) phase: StartPhase,
    /// Orphaned slots under the prefix, when they could be listed.
    pub(super) orphans: Option<usize>,
}

/// Declared inputs of one start request.
pub(super) struct StartInputs<'a> {
    pub(super) data_ready: &'a Arc<Notify>,
    pub(super) config: &'a PostgresCdcConfig,
    pub(super) declared: &'a SchemaRef,
    pub(super) primary_key: &'a [String],
    pub(super) committed_relation: Option<&'a RelationInfo>,
    pub(super) incarnation: &'a str,
}

pub(super) struct ReaderRuntime {
    pub(super) wal_rx: WalPayloadRx,
    pub(super) wal_byte_budget: Arc<Semaphore>,
    pub(super) terminal_error: WalTerminalError,
    pub(super) reader_handle: tokio::task::JoinHandle<()>,
    pub(super) shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub(super) applied_lsn: pgwire_replication::AppliedLsnHandle,
}

fn guard(
    owner: &ConnectorTaskOwner,
) -> Result<crate::connector::ConnectorTaskGuard, ConnectorError> {
    owner.track().ok_or_else(|| {
        ConnectorError::Internal("PostgreSQL CDC connector generation is already retired".into())
    })
}

/// Validate the live contract and position the source: a fresh start claims a new slot name,
/// and a committed cursor goes through the restart matrix.
///
/// # Errors
/// Returns an actionable error for contract drift, a cursor that cannot be resumed, a slot held
/// by another consumer, or I/O failure.
pub(super) async fn prepare(
    owner: &ConnectorTaskOwner,
    inputs: StartInputs<'_>,
    cursor: Option<Cursor>,
) -> Result<PreparedStart, ConnectorError> {
    let config = inputs.config;
    let claim = cursor.as_ref().map_or_else(
        || SlotClaim::generate(&config.slot_name),
        |cursor| cursor.claim.clone(),
    );
    let control = postgres_io::connect(
        config,
        &claim.application_name(inputs.incarnation),
        guard(owner)?,
    )
    .await?;
    let prepared = prepare_on(&control, owner, &inputs, claim, cursor).await;
    control.close().await;
    prepared
}

async fn prepare_on(
    control: &ControlConnection,
    owner: &ConnectorTaskOwner,
    inputs: &StartInputs<'_>,
    claim: SlotClaim,
    cursor: Option<Cursor>,
) -> Result<PreparedStart, ConnectorError> {
    let config = inputs.config;
    let live = inspect_source(control.client(), config, Some(claim.slot())).await?;
    let table = inspect_capture_table(control.client(), config).await?;
    let layout = bind_layout(config, inputs.declared, inputs.primary_key, &table)?;
    if let Some(committed) = inputs.committed_relation {
        validate_relation(committed, &table.relation)?;
    }
    let metadata = table.relation.retained_bytes()?;
    if metadata > config.relation_metadata_bytes() {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL table {} layout needs {metadata} bytes of metadata, more than the {} bytes \
             max.buffered.bytes leaves beside transaction data; raise max.buffered.bytes",
            config.table,
            config.relation_metadata_bytes()
        )));
    }
    let copies = config.snapshot_mode == SnapshotMode::Initial
        && cursor
            .as_ref()
            .is_none_or(|cursor| cursor.phase == CursorPhase::Claimed);
    if copies {
        admit_copy(control, config, &table, &layout).await?;
    }
    let (binding, phase) = match cursor {
        None => (
            live.binding(config),
            StartPhase::Claim {
                claim,
                committed: false,
            },
        ),
        Some(cursor) => {
            let action = settle(
                control,
                config,
                &claim,
                inputs.incarnation,
                cursor.phase,
                &cursor.binding,
            )
            .await?;
            let phase = resumed_phase(owner, inputs, claim, &cursor, action).await?;
            (cursor.binding, phase)
        }
    };
    let current = match &phase {
        StartPhase::Claim { claim, .. }
        | StartPhase::AwaitStream { claim, .. }
        | StartPhase::Stream { claim, .. } => claim,
    };
    let orphans = report_orphans(control.client(), &config.slot_name, current).await;
    Ok(PreparedStart {
        layout,
        relation: table.relation,
        binding,
        phase,
        orphans,
    })
}

/// Require the initial copy to read every row and declared column that logical replication
/// streams, before any slot exists: a refused copy would otherwise fail after creating one.
async fn admit_copy(
    control: &ControlConnection,
    config: &PostgresCdcConfig,
    table: &CaptureTable,
    layout: &RowLayout,
) -> Result<(), ConnectorError> {
    let (user, name) = (&config.username, &config.table);
    if table.row_security {
        return Err(ConnectorError::ConfigurationError(format!(
            "row-level security applies to role '{user}' on PostgreSQL table {name}, so the \
             initial snapshot would silently miss rows that logical replication still streams; \
             grant the role BYPASSRLS (ALTER ROLE {user} BYPASSRLS), connect as the table owner \
             without FORCE ROW LEVEL SECURITY, or disable row-level security on the table"
        )));
    }
    let columns: Vec<String> = layout
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let unreadable =
        unreadable_columns(control.client(), table.relation.relation_id, &columns).await?;
    if !unreadable.is_empty() {
        return Err(ConnectorError::ConfigurationError(format!(
            "role '{user}' cannot SELECT column(s) {} of PostgreSQL table {name}, which the \
             initial snapshot copies; run GRANT SELECT ON {name} TO {user}",
            unreadable.join(", ")
        )));
    }
    Ok(())
}

/// Carry out the restart matrix's decision for a committed cursor.
async fn resumed_phase(
    owner: &ConnectorTaskOwner,
    inputs: &StartInputs<'_>,
    claim: SlotClaim,
    cursor: &Cursor,
    action: ResumeAction,
) -> Result<StartPhase, ConnectorError> {
    let config = inputs.config;
    match action {
        ResumeAction::Create => Ok(StartPhase::Claim {
            claim,
            committed: true,
        }),
        ResumeAction::Adopt(lsn) => {
            let CursorPhase::Streaming {
                consistent_point, ..
            } = cursor.phase
            else {
                tracing::info!(slot = claim.slot(), %lsn, "adopted the claimed PostgreSQL replication slot");
                return Ok(StartPhase::AwaitStream {
                    claim,
                    consistent_point: lsn,
                });
            };
            let runtime = launch_reader(
                owner,
                Arc::clone(inputs.data_ready),
                config,
                &claim,
                inputs.incarnation,
                &cursor.binding,
                lsn,
            )
            .await?;
            tracing::info!(slot = claim.slot(), %lsn, "resuming PostgreSQL replication slot");
            Ok(StartPhase::Stream {
                claim,
                consistent_point,
                lsn,
                runtime,
            })
        }
        ResumeAction::NewClaim(reason) => {
            warn_orphaning(claim.slot(), &reason);
            Ok(StartPhase::Claim {
                claim: SlotClaim::generate(&config.slot_name),
                committed: false,
            })
        }
        ResumeAction::Busy(holder) | ResumeAction::TerminateStale(holder) => {
            Err(busy(claim.slot(), &holder))
        }
        ResumeAction::FailClosed(reason) => Err(ConnectorError::ConfigurationError(reason)),
    }
}

/// Connect the replication stream of `claim`'s slot at `start_lsn` and spawn the bounded reader
/// task.
///
/// # Errors
/// Returns an error when the replication socket rejects the identity or the cursor.
pub(super) async fn launch_reader(
    owner: &ConnectorTaskOwner,
    data_ready: Arc<Notify>,
    config: &PostgresCdcConfig,
    claim: &SlotClaim,
    incarnation: &str,
    binding: &PostgresCheckpointBinding,
    start_lsn: Lsn,
) -> Result<ReaderRuntime, ConnectorError> {
    let mut replication = postgres_io::build_replication_config(
        config,
        claim.slot(),
        &claim.application_name(incarnation),
    );
    replication.buffer_events = PGWIRE_IN_FLIGHT_EVENTS;
    replication.start_lsn = pgwire_replication::Lsn::from_u64(start_lsn.as_u64());
    replication.expected_recovery_identity = Some(pgwire_replication::ExpectedRecoveryIdentity {
        system_identifier: binding.system_identifier,
        timeline_id: binding.timeline_id,
    });
    let client = match tokio::time::timeout(
        postgres_io::CONNECT_TIMEOUT,
        pgwire_replication::ReplicationClient::connect_with_worker_lifetime(
            replication,
            guard(owner)?,
        ),
    )
    .await
    {
        Ok(Ok(client)) => client,
        Ok(Err(error)) => {
            return Err(ConnectorError::ConnectionFailed(format!(
                "pgwire-replication connect to slot '{}': {error}",
                claim.slot()
            )));
        }
        Err(_) => {
            return Err(ConnectorError::ConnectionFailed(format!(
                "pgwire-replication connect to slot '{}' timed out after 10 seconds",
                claim.slot()
            )));
        }
    };
    let applied_lsn = client.applied_lsn_handle();
    let raw_wal_byte_limit = config.raw_wal_bytes();
    let (wal_tx, wal_rx) =
        crossfire::mpsc::bounded_async::<OwnedWalPayload>(RAW_WAL_QUEUE_CAPACITY);
    // pgwire holds the same aggregate ceiling until this queue acquires its
    // permit, so ownership never crosses an unaccounted gap.
    let wal_byte_budget = Arc::new(Semaphore::new(raw_wal_byte_limit));
    let terminal_error: WalTerminalError = Arc::new(std::sync::Mutex::new(None));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let reader_handle = tokio::spawn(run_wal_reader(
        client,
        wal_tx,
        Arc::clone(&wal_byte_budget),
        raw_wal_byte_limit,
        shutdown_rx,
        Arc::clone(&terminal_error),
        data_ready,
        guard(owner)?,
    ));
    Ok(ReaderRuntime {
        wal_rx,
        wal_byte_budget,
        terminal_error,
        reader_handle,
        shutdown_tx,
        applied_lsn,
    })
}

/// Re-read the live publication, slot, and table, require them to match the binding, and
/// report orphaned slots under the prefix.
///
/// # Errors
/// Returns an error when the contract drifted, the slot can no longer be adopted, or the
/// catalog cannot be read.
pub(super) async fn revalidate(
    owner: &ConnectorTaskOwner,
    config: &PostgresCdcConfig,
    claim: &SlotClaim,
    incarnation: &str,
    binding: &PostgresCheckpointBinding,
    relation: &RelationInfo,
) -> Result<Option<usize>, ConnectorError> {
    let control =
        postgres_io::connect(config, &claim.application_name(incarnation), guard(owner)?).await?;
    let checked = async {
        let live = inspect_source(control.client(), config, Some(claim.slot())).await?;
        validate_live_binding(binding, &live.binding(config), "running source")?;
        let slot = claim.slot();
        match live.slot.as_ref().map(|facts| facts.unusable.as_deref()) {
            None => {
                return Err(ConnectorError::ReadError(format!(
                    "PostgreSQL replication slot '{slot}' disappeared"
                )));
            }
            Some(Some(problem)) => {
                return Err(ConnectorError::ReadError(format!(
                    "PostgreSQL replication slot '{slot}' {problem}"
                )));
            }
            Some(None) => {}
        }
        let table = inspect_capture_table(control.client(), config).await?;
        validate_relation(relation, &table.relation)?;
        Ok(report_orphans(control.client(), &config.slot_name, claim).await)
    }
    .await;
    control.close().await;
    checked
}
