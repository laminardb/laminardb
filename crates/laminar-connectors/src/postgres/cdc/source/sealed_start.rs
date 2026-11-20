//! Sealed start: the slot claim a source added to a running cluster starts from. It is resolved
//! once when the topology change is sealed, which commits the claim, and validated again before
//! the source is installed. Nothing is created: the owner creates the claimed slot when it opens.

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::ConnectorTaskGuard;

use super::super::config::{PostgresCdcConfig, SnapshotMode};
use super::super::postgres_io::PostgresCheckpointBinding;
use super::super::schema_resolution::{
    declared_primary_key, declared_schema, inspect_declared_table,
};
use super::checkpoint::{parse_cursor, validate_live_binding, write_cursor, Cursor, CursorPhase};
use super::claim::SlotClaim;
use super::ConnectorError;

/// Only a stream without a copy starts sealed: a copy reads from the slot's creation.
pub(super) fn require(config: &PostgresCdcConfig) -> Result<(), ConnectorError> {
    if config.snapshot_mode == SnapshotMode::Never {
        return Ok(());
    }
    Err(ConnectorError::ConfigurationError(
        "a PostgreSQL CDC source starts sealed only with snapshot.mode=never".into(),
    ))
}

/// The claimed cursor of a sealed start in `sealed`, or the error a different phase is.
pub(super) fn claimed_cursor(
    sealed: &SourceCheckpoint,
    config: &PostgresCdcConfig,
) -> Result<Cursor, ConnectorError> {
    let cursor = parse_cursor(sealed, config, "sealed start")?;
    if cursor.phase != CursorPhase::Claimed {
        return Err(ConnectorError::ConfigurationError(
            "a PostgreSQL CDC sealed start must be a claim on a slot not yet created".into(),
        ));
    }
    Ok(cursor)
}

/// A new claim bound to the live server, database and publication.
pub(super) async fn resolve(
    config: &PostgresCdcConfig,
    connector: &ConnectorConfig,
    guard: ConnectorTaskGuard,
) -> Result<SourceCheckpoint, ConnectorError> {
    require(config)?;
    let claim = SlotClaim::generate(&config.slot_name);
    let binding = live_binding(config, connector, &claim, guard).await?;
    Ok(write_cursor(
        config,
        Some((&claim, &binding, CursorPhase::Claimed)),
    ))
}

/// Require the live server, database, publication and declared table layout to be the ones the
/// sealed claim is bound to.
pub(super) async fn validate(
    config: &PostgresCdcConfig,
    connector: &ConnectorConfig,
    sealed: &SourceCheckpoint,
    guard: ConnectorTaskGuard,
) -> Result<(), ConnectorError> {
    require(config)?;
    let cursor = claimed_cursor(sealed, config)?;
    let binding = live_binding(config, connector, &cursor.claim, guard).await?;
    validate_live_binding(&cursor.binding, &binding, "sealed start")
}

async fn live_binding(
    config: &PostgresCdcConfig,
    connector: &ConnectorConfig,
    claim: &SlotClaim,
    guard: ConnectorTaskGuard,
) -> Result<PostgresCheckpointBinding, ConnectorError> {
    let (live, _) = inspect_declared_table(
        config,
        &declared_schema(connector, None)?,
        &declared_primary_key(connector),
        Some(claim.slot()),
        guard,
    )
    .await?;
    Ok(live.binding(config))
}
