//! Source cursor encoding, parsing, and recovery-identity validation.

use crate::checkpoint::SourceCheckpoint;
use crate::error::ConnectorError;

use super::super::config::PostgresCdcConfig;
use super::super::postgres_io::{source_config_digest, PostgresCheckpointBinding};
use super::Lsn;

const CHECKPOINT_CONNECTOR: &str = "postgres-cdc";
const CHECKPOINT_VERSION: &str = "4";
const PHASE_METADATA: &str = "phase";
const PHASE_SNAPSHOT: &str = "snapshot";
const PHASE_STREAMING: &str = "streaming";
const LSN_OFFSET: &str = "lsn";
const SYSTEM_IDENTIFIER_METADATA: &str = "system_identifier";
const TIMELINE_ID_METADATA: &str = "timeline_id";
const DATABASE_OID_METADATA: &str = "database_oid";
const PUBLICATION_OID_METADATA: &str = "publication_oid";
const PUBLICATION_DEFINITION_METADATA: &str = "publication_definition_sha256";
const SOURCE_CONFIG_METADATA: &str = "source_config_sha256";
const SLOT_PLUGIN_METADATA: &str = "slot_plugin";
const SLOT_TWO_PHASE_METADATA: &str = "slot_two_phase";
const SLOT_FAILOVER_METADATA: &str = "slot_failover";

/// The source position a cursor describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CursorPhase {
    /// Inside the initial snapshot; not resumable.
    Snapshot,
    /// After every transaction ending at or before this LSN.
    Streaming(Lsn),
}

/// Encode a cursor bound to the exact slot, publication, and cluster identity.
pub(super) fn write_cursor(
    config: &PostgresCdcConfig,
    binding: Option<&PostgresCheckpointBinding>,
    phase: CursorPhase,
) -> SourceCheckpoint {
    let mut checkpoint = SourceCheckpoint::new();
    match phase {
        CursorPhase::Snapshot => checkpoint.set_metadata(PHASE_METADATA, PHASE_SNAPSHOT),
        CursorPhase::Streaming(lsn) => {
            checkpoint.set_offset(LSN_OFFSET, lsn.to_string());
            checkpoint.set_metadata(PHASE_METADATA, PHASE_STREAMING);
        }
    }
    checkpoint.set_metadata("connector", CHECKPOINT_CONNECTOR);
    checkpoint.set_metadata("checkpoint_version", CHECKPOINT_VERSION);
    checkpoint.set_metadata("slot_name", &config.slot_name);
    checkpoint.set_metadata("publication", &config.publication);
    checkpoint.set_metadata("database", &config.database);
    checkpoint.set_metadata("table", config.table.to_string());
    let Some(binding) = binding else {
        return checkpoint;
    };
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
    checkpoint
}

/// Parse a committed cursor into its resumable LSN and recovery identity.
///
/// # Errors
/// Rejects a cursor from another source, an older format, drifted configuration, or a cursor
/// captured inside an initial snapshot, which cannot be resumed.
pub(super) fn parse_resumable(
    checkpoint: &SourceCheckpoint,
    config: &PostgresCdcConfig,
    context: &str,
) -> Result<(Lsn, PostgresCheckpointBinding), ConnectorError> {
    for (key, expected) in [
        ("checkpoint_version", CHECKPOINT_VERSION),
        ("connector", CHECKPOINT_CONNECTOR),
        ("slot_name", config.slot_name.as_str()),
        ("publication", config.publication.as_str()),
        ("database", config.database.as_str()),
        ("table", config.table.to_string().as_str()),
    ] {
        let actual = required(checkpoint, key, context)?;
        if actual != expected {
            return Err(ConnectorError::ConfigurationError(format!(
                "PostgreSQL CDC {context} has '{key}' identity '{actual}', expected '{expected}'"
            )));
        }
    }
    if required(checkpoint, PHASE_METADATA, context)? != PHASE_STREAMING {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} was captured inside the initial snapshot, which cannot be \
             resumed: drop slot '{}', clear downstream targets and this pipeline's checkpoints, \
             and start the source again",
            config.slot_name
        )));
    }
    let binding = binding(checkpoint, context)?;
    if binding.source_config_sha256 != source_config_digest(config) {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} table or output.mode drifted from its checkpoint"
        )));
    }
    let lsn_text = checkpoint.get_offset(LSN_OFFSET).ok_or_else(|| {
        ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} is missing required '{LSN_OFFSET}' offset"
        ))
    })?;
    let lsn = lsn_text.parse::<Lsn>().map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "invalid LSN '{lsn_text}' in PostgreSQL CDC {context}: {error}"
        ))
    })?;
    Ok((lsn, binding))
}

/// Require a live binding to equal the one a checkpoint was captured under.
pub(super) fn validate_live_binding(
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

fn required<'a>(
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

fn decimal<T>(checkpoint: &SourceCheckpoint, key: &str, context: &str) -> Result<T, ConnectorError>
where
    T: std::str::FromStr + ToString,
    T::Err: std::fmt::Display,
{
    let value = required(checkpoint, key, context)?;
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

fn boolean(
    checkpoint: &SourceCheckpoint,
    key: &str,
    context: &str,
) -> Result<bool, ConnectorError> {
    match required(checkpoint, key, context)? {
        "true" => Ok(true),
        "false" => Ok(false),
        value => Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} has invalid '{key}' metadata '{value}'"
        ))),
    }
}

fn sha256(
    checkpoint: &SourceCheckpoint,
    key: &str,
    context: &str,
) -> Result<String, ConnectorError> {
    let value = required(checkpoint, key, context)?;
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

fn binding(
    checkpoint: &SourceCheckpoint,
    context: &str,
) -> Result<PostgresCheckpointBinding, ConnectorError> {
    Ok(PostgresCheckpointBinding {
        system_identifier: decimal(checkpoint, SYSTEM_IDENTIFIER_METADATA, context)?,
        timeline_id: decimal(checkpoint, TIMELINE_ID_METADATA, context)?,
        database_oid: decimal(checkpoint, DATABASE_OID_METADATA, context)?,
        publication_oid: decimal(checkpoint, PUBLICATION_OID_METADATA, context)?,
        publication_definition_sha256: sha256(
            checkpoint,
            PUBLICATION_DEFINITION_METADATA,
            context,
        )?,
        source_config_sha256: sha256(checkpoint, SOURCE_CONFIG_METADATA, context)?,
        slot_plugin: required(checkpoint, SLOT_PLUGIN_METADATA, context)?.to_string(),
        slot_two_phase: boolean(checkpoint, SLOT_TWO_PHASE_METADATA, context)?,
        slot_failover: boolean(checkpoint, SLOT_FAILOVER_METADATA, context)?,
    })
}
