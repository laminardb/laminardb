//! Network startup: contract validation, owned slot creation or resume validation, and the
//! replication reader launch.

use arrow_schema::SchemaRef;

use super::super::config::{PostgresCdcConfig, SnapshotMode};
use super::super::postgres_io::{
    self, inspect_capture_table, inspect_source, ControlConnection, InspectedSource,
    PostgresCheckpointBinding,
};
use super::super::schema::RelationInfo;
use super::super::schema_resolution::{bind_layout, validate_relation};
use super::super::typed_rows::RowLayout;
use super::checkpoint::validate_live_binding;
use super::reader::{run_wal_reader, OwnedWalPayload, WalPayloadRx, WalTerminalError};
use super::snapshot::SnapshotReader;
use super::{
    Arc, ConnectorError, ConnectorTaskOwner, Lsn, Notify, Semaphore, PGWIRE_IN_FLIGHT_EVENTS,
    RAW_WAL_QUEUE_CAPACITY,
};

/// How a start request positions the source.
pub(super) enum StartPlan {
    /// No checkpoint: create the slot this start owns.
    Fresh,
    /// Resume from a committed streaming cursor.
    Resume {
        lsn: Lsn,
        binding: PostgresCheckpointBinding,
    },
}

/// Where a prepared source begins reading.
pub(super) enum StartPhase {
    Snapshot(SnapshotReader),
    Stream(Lsn, ReaderRuntime),
}

/// Everything a successful startup installs at once.
pub(super) struct PreparedStart {
    pub(super) layout: RowLayout,
    pub(super) relation: RelationInfo,
    pub(super) binding: PostgresCheckpointBinding,
    pub(super) phase: StartPhase,
}

/// Declared inputs of one start request.
pub(super) struct StartInputs<'a> {
    pub(super) data_ready: &'a Arc<Notify>,
    pub(super) config: &'a PostgresCdcConfig,
    pub(super) declared: &'a SchemaRef,
    pub(super) primary_key: &'a [String],
    pub(super) committed_relation: Option<&'a RelationInfo>,
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

/// Validate the live contract and position the source.
///
/// # Errors
/// Returns an actionable error for contract drift, an unowned existing slot, or I/O failure.
/// A slot created by this call is dropped again if a later step fails.
pub(super) async fn prepare(
    owner: &ConnectorTaskOwner,
    inputs: StartInputs<'_>,
    plan: StartPlan,
) -> Result<PreparedStart, ConnectorError> {
    let control = postgres_io::connect(inputs.config, guard(owner)?).await?;
    let prepared = prepare_on(&control, owner, &inputs, plan).await;
    control.close().await;
    prepared
}

async fn prepare_on(
    control: &ControlConnection,
    owner: &ConnectorTaskOwner,
    inputs: &StartInputs<'_>,
    plan: StartPlan,
) -> Result<PreparedStart, ConnectorError> {
    let config = inputs.config;
    let inspected = inspect_source(control.client(), config).await?;
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
    match plan {
        StartPlan::Resume { lsn, binding } => {
            validate_resume(config, &inspected, &binding, lsn)?;
            let reader =
                launch_reader(owner, Arc::clone(inputs.data_ready), config, &binding, lsn).await?;
            Ok(PreparedStart {
                layout,
                relation: table.relation,
                binding,
                phase: StartPhase::Stream(lsn, reader),
            })
        }
        StartPlan::Fresh => {
            if inspected.slot.is_some() {
                return Err(ConnectorError::ConfigurationError(format!(
                    "PostgreSQL replication slot '{}' already exists, but no LaminarDB checkpoint \
                     references it (an interrupted initial snapshot, a crash before the first \
                     checkpoint, or another consumer's slot). LaminarDB never adopts or drops an \
                     existing slot: drop it with SELECT pg_drop_replication_slot('{}'), clear \
                     downstream targets of this source, and start again",
                    config.slot_name, config.slot_name
                )));
            }
            let created = create_slot(config).await?;
            match finish_fresh(control, owner, inputs, &layout, &inspected, created).await {
                Ok((binding, phase)) => Ok(PreparedStart {
                    layout,
                    relation: table.relation,
                    binding,
                    phase,
                }),
                Err(error) => {
                    if let Err(cleanup) =
                        postgres_io::drop_created_slot(control.client(), &config.slot_name).await
                    {
                        tracing::warn!(
                            slot = %config.slot_name,
                            %cleanup,
                            "could not drop the PostgreSQL CDC slot this failed start created"
                        );
                    }
                    Err(error)
                }
            }
        }
    }
}

fn validate_resume(
    config: &PostgresCdcConfig,
    inspected: &InspectedSource,
    binding: &PostgresCheckpointBinding,
    lsn: Lsn,
) -> Result<(), ConnectorError> {
    let Some(slot) = inspected.slot.as_ref() else {
        return Err(ConnectorError::ConfigurationError(format!(
            "cannot resume PostgreSQL CDC slot '{}': the slot is missing; LaminarDB never \
             recreates a recovery slot because the WAL it retained is gone",
            config.slot_name
        )));
    };
    validate_live_binding(binding, &inspected.binding(config)?, "resume checkpoint")?;
    let confirmed = slot.confirmed_flush_lsn.ok_or_else(|| {
        ConnectorError::ConfigurationError(format!(
            "cannot resume PostgreSQL CDC slot '{}': the slot has no retained durable position",
            config.slot_name
        ))
    })?;
    if confirmed > lsn {
        return Err(ConnectorError::ConfigurationError(format!(
            "cannot resume PostgreSQL CDC checkpoint at {lsn}: slot '{}' has already advanced to \
             {confirmed}; required WAL may have been reclaimed",
            config.slot_name
        )));
    }
    Ok(())
}

async fn create_slot(
    config: &PostgresCdcConfig,
) -> Result<pgwire_replication::CreatedSlot, ConnectorError> {
    let snapshot = match config.snapshot_mode {
        SnapshotMode::Initial => pgwire_replication::SlotSnapshot::Export,
        SnapshotMode::Never => pgwire_replication::SlotSnapshot::Nothing,
    };
    let replication = postgres_io::build_replication_config(config);
    tokio::time::timeout(
        postgres_io::CONNECT_TIMEOUT,
        pgwire_replication::create_logical_slot(&replication, snapshot),
    )
    .await
    .map_err(|_| {
        ConnectorError::ConnectionFailed(format!(
            "creating PostgreSQL replication slot '{}' timed out; check whether it exists before \
             retrying",
            config.slot_name
        ))
    })?
    .map_err(|error| {
        ConnectorError::ConnectionFailed(format!(
            "create PostgreSQL replication slot '{}': {error}",
            config.slot_name
        ))
    })
}

async fn finish_fresh(
    control: &ControlConnection,
    owner: &ConnectorTaskOwner,
    inputs: &StartInputs<'_>,
    layout: &RowLayout,
    inspected: &InspectedSource,
    created: pgwire_replication::CreatedSlot,
) -> Result<(PostgresCheckpointBinding, StartPhase), ConnectorError> {
    if (created.system_identifier, created.timeline_id)
        != (inspected.system_identifier, inspected.timeline_id)
    {
        return Err(ConnectorError::ConfigurationError(
            "the replication and control connections reached different PostgreSQL clusters or \
             timelines"
                .into(),
        ));
    }
    let config = inputs.config;
    let consistent_point = Lsn::new(created.consistent_point.as_u64());
    let snapshot = match config.snapshot_mode {
        SnapshotMode::Initial => {
            let snapshot_name = created.snapshot_name.clone().ok_or_else(|| {
                ConnectorError::ReadError("PostgreSQL exported no snapshot".into())
            })?;
            let connection = postgres_io::connect(config, guard(owner)?).await?;
            let reader =
                SnapshotReader::open(connection, config, layout, &snapshot_name, consistent_point)
                    .await?;
            Some(reader)
        }
        SnapshotMode::Never => None,
    };
    // The importer holds its own copy of the snapshot, so the exporting session can end now.
    if let Err(error) = created.release().await {
        tracing::debug!(%error, "PostgreSQL slot-creating session closed with an error");
    }
    let binding = inspect_source(control.client(), config)
        .await?
        .binding(config)?;
    if let Some(reader) = snapshot {
        return Ok((binding, StartPhase::Snapshot(reader)));
    }
    let reader = launch_reader(
        owner,
        Arc::clone(inputs.data_ready),
        config,
        &binding,
        consistent_point,
    )
    .await?;
    Ok((binding, StartPhase::Stream(consistent_point, reader)))
}

/// Connect the replication stream at `start_lsn` and spawn the bounded reader task.
///
/// # Errors
/// Returns an error when the replication socket rejects the identity or the cursor.
pub(super) async fn launch_reader(
    owner: &ConnectorTaskOwner,
    data_ready: Arc<Notify>,
    config: &PostgresCdcConfig,
    binding: &PostgresCheckpointBinding,
    start_lsn: Lsn,
) -> Result<ReaderRuntime, ConnectorError> {
    let mut replication = postgres_io::build_replication_config(config);
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
                "pgwire-replication connect: {error}"
            )));
        }
        Err(_) => {
            return Err(ConnectorError::ConnectionFailed(
                "pgwire-replication connect timed out after 10 seconds".into(),
            ));
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

/// Re-read the live publication, slot, and table and require them to match the binding.
///
/// # Errors
/// Returns an error when the contract drifted or cannot be read.
pub(super) async fn revalidate(
    owner: &ConnectorTaskOwner,
    config: &PostgresCdcConfig,
    binding: &PostgresCheckpointBinding,
    relation: &RelationInfo,
) -> Result<(), ConnectorError> {
    let control = postgres_io::connect(config, guard(owner)?).await?;
    let checked = async {
        let live = inspect_source(control.client(), config)
            .await?
            .binding(config)?;
        validate_live_binding(binding, &live, "running source")?;
        let table = inspect_capture_table(control.client(), config).await?;
        validate_relation(relation, &table.relation)
    }
    .await;
    control.close().await;
    checked
}
