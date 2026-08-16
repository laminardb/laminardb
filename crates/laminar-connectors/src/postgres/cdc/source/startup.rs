//! Prepared ownership for replication-slot validation and reader startup.

use super::super::postgres_io;
use super::{
    run_wal_reader, validate_live_binding, Arc, ConnectorError, Lsn, OwnedWalPayload,
    PostgresCdcConfig, PostgresCdcSource, PostgresCheckpointBinding, Semaphore, WalPayloadRx,
    WalTerminalError, PGWIRE_IN_FLIGHT_EVENTS, RAW_WAL_QUEUE_CAPACITY,
};

pub(super) struct PreparedReaderRuntime {
    pub(super) wal_rx: WalPayloadRx,
    pub(super) wal_byte_budget: Arc<Semaphore>,
    pub(super) terminal_error: WalTerminalError,
    pub(super) reader_handle: tokio::task::JoinHandle<()>,
    pub(super) shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub(super) confirmed_lsn_tx: tokio::sync::watch::Sender<u64>,
}

async fn validate_resume_slot(
    source: &mut PostgresCdcSource,
    config: &PostgresCdcConfig,
    expected_binding: &PostgresCheckpointBinding,
    start_lsn: Lsn,
) -> Result<(), ConnectorError> {
    let control_driver_guard = source.task_owner.track().ok_or_else(|| {
        ConnectorError::Internal("PostgreSQL CDC connector generation is already retired".into())
    })?;
    let control = postgres_io::connect(config, control_driver_guard).await?;
    let slot_inspection = postgres_io::inspect_replication_slot(
        control.client(),
        &config.slot_name,
        "pgoutput",
        &config.database,
        &config.publication,
        expected_binding.source_config_sha256.clone(),
    )
    .await;
    control.close().await;
    let slot_lsn = slot_inspection?;

    let Some(slot) = slot_lsn.as_ref() else {
        return Err(ConnectorError::ConfigurationError(format!(
            "cannot resume PostgreSQL CDC slot '{}': the exact durable slot is missing",
            config.slot_name
        )));
    };
    validate_live_binding(expected_binding, &slot.binding, "resume checkpoint")?;
    let Some(resume_slot_lsn) = slot.confirmed_flush_lsn.as_ref() else {
        return Err(ConnectorError::ConfigurationError(format!(
            "cannot resume PostgreSQL CDC slot '{}': the slot has no retained durable position",
            config.slot_name
        )));
    };
    if resume_slot_lsn.as_u64() > start_lsn.as_u64() {
        return Err(ConnectorError::ConfigurationError(format!(
            "cannot resume PostgreSQL CDC checkpoint at {}: slot '{}' has already advanced to {}; required WAL may have been reclaimed",
            start_lsn, config.slot_name, resume_slot_lsn
        )));
    }
    Ok(())
}

async fn connect_replication_client(
    source: &mut PostgresCdcSource,
    config: &PostgresCdcConfig,
    expected_binding: &PostgresCheckpointBinding,
    start_lsn: Lsn,
) -> Result<pgwire_replication::ReplicationClient, ConnectorError> {
    let mut repl_config = postgres_io::build_replication_config(config);
    repl_config.buffer_events = PGWIRE_IN_FLIGHT_EVENTS;
    if start_lsn != Lsn::ZERO {
        repl_config.start_lsn = pgwire_replication::Lsn::from_u64(start_lsn.as_u64());
    }
    repl_config.expected_recovery_identity = Some(pgwire_replication::ExpectedRecoveryIdentity {
        system_identifier: expected_binding.system_identifier,
        timeline_id: expected_binding.timeline_id,
    });

    let replication_worker_guard = source.task_owner.track().ok_or_else(|| {
        ConnectorError::Internal("PostgreSQL CDC connector generation is already retired".into())
    })?;
    match tokio::time::timeout(
        postgres_io::CONNECT_TIMEOUT,
        pgwire_replication::ReplicationClient::connect_with_worker_lifetime(
            repl_config,
            replication_worker_guard,
        ),
    )
    .await
    {
        Ok(Ok(client)) => Ok(client),
        Ok(Err(error)) => Err(ConnectorError::ConnectionFailed(format!(
            "pgwire-replication connect: {error}"
        ))),
        Err(_) => Err(ConnectorError::ConnectionFailed(
            "pgwire-replication connect timed out after 10 seconds".into(),
        )),
    }
}

fn spawn_reader(
    source: &mut PostgresCdcSource,
    config: &PostgresCdcConfig,
    start_lsn: Lsn,
    repl_client: pgwire_replication::ReplicationClient,
) -> Result<PreparedReaderRuntime, ConnectorError> {
    let raw_wal_byte_limit = config.raw_wal_bytes();
    let (wal_tx, wal_rx) =
        crossfire::mpsc::bounded_async::<OwnedWalPayload>(RAW_WAL_QUEUE_CAPACITY);
    // pgwire holds the same aggregate ceiling until this queue acquires its
    // permit, so ownership never crosses an unaccounted gap.
    let wal_byte_budget = Arc::new(Semaphore::new(raw_wal_byte_limit));
    let reader_byte_budget = Arc::clone(&wal_byte_budget);
    let terminal_error: WalTerminalError = Arc::new(std::sync::Mutex::new(None));
    let reader_terminal_error = Arc::clone(&terminal_error);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (confirmed_lsn_tx, confirmed_lsn_rx) = tokio::sync::watch::channel(start_lsn.as_u64());
    let data_ready = Arc::clone(&source.data_ready);
    let reader_guard = source.task_owner.track().ok_or_else(|| {
        ConnectorError::Internal("PostgreSQL CDC connector generation is already retired".into())
    })?;
    let reader_handle = tokio::spawn(run_wal_reader(
        repl_client,
        wal_tx,
        reader_byte_budget,
        raw_wal_byte_limit,
        shutdown_rx,
        confirmed_lsn_rx,
        reader_terminal_error,
        data_ready,
        reader_guard,
    ));
    Ok(PreparedReaderRuntime {
        wal_rx,
        wal_byte_budget,
        terminal_error,
        reader_handle,
        shutdown_tx,
        confirmed_lsn_tx,
    })
}

pub(super) async fn prepare_reader_runtime(
    source: &mut PostgresCdcSource,
    config: &PostgresCdcConfig,
    expected_binding: &PostgresCheckpointBinding,
    start_lsn: Lsn,
) -> Result<PreparedReaderRuntime, ConnectorError> {
    validate_resume_slot(source, config, expected_binding, start_lsn).await?;
    let repl_client =
        connect_replication_client(source, config, expected_binding, start_lsn).await?;
    spawn_reader(source, config, start_lsn, repl_client)
}
