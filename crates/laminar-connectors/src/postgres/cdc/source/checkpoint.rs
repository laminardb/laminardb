//! Source cursor encoding, parsing, and recovery-identity validation.

use crate::checkpoint::SourceCheckpoint;
use crate::error::ConnectorError;

use super::super::config::PostgresCdcConfig;
use super::super::postgres_io::{source_config_digest, PostgresCheckpointBinding};
use super::claim::{reset_instructions, SlotClaim};
use super::Lsn;

const CHECKPOINT_CONNECTOR: &str = "postgres-cdc";
const CHECKPOINT_VERSION: &str = "5";
const PHASE_METADATA: &str = "phase";
const PHASE_CLAIMED: &str = "claimed";
const PHASE_SNAPSHOT: &str = "snapshot";
const PHASE_STREAMING: &str = "streaming";
const CLAIM_METADATA: &str = "claim";
const SLOT_METADATA: &str = "slot";
/// The slot key of version 4 cursors, read only to name the slot in the reset instructions.
const V4_SLOT_METADATA: &str = "slot_name";
const CONSISTENT_POINT_METADATA: &str = "consistent_point";
const LSN_OFFSET: &str = "lsn";
const SYSTEM_IDENTIFIER_METADATA: &str = "system_identifier";
const TIMELINE_ID_METADATA: &str = "timeline_id";
const DATABASE_OID_METADATA: &str = "database_oid";
const PUBLICATION_OID_METADATA: &str = "publication_oid";
const PUBLICATION_DEFINITION_METADATA: &str = "publication_definition_sha256";
const SOURCE_CONFIG_METADATA: &str = "source_config_sha256";

/// The source position a cursor describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CursorPhase {
    /// The slot name is claimed; the slot is created once this cursor commits. Nothing was
    /// emitted.
    Claimed,
    /// Inside the initial snapshot of the slot created at `consistent_point`; not resumable.
    Snapshot { consistent_point: Lsn },
    /// After every transaction ending at or before `lsn` on the slot created at
    /// `consistent_point`.
    Streaming { consistent_point: Lsn, lsn: Lsn },
}

/// A committed cursor: the claim, the identity it was taken under, and the position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Cursor {
    pub(super) claim: SlotClaim,
    pub(super) binding: PostgresCheckpointBinding,
    pub(super) phase: CursorPhase,
}

/// Encode a cursor bound to the exact claim, publication, and cluster identity. Without a
/// position, only the source identity is written and the cursor cannot be resumed.
pub(super) fn write_cursor(
    config: &PostgresCdcConfig,
    position: Option<(&SlotClaim, &PostgresCheckpointBinding, CursorPhase)>,
) -> SourceCheckpoint {
    let mut checkpoint = SourceCheckpoint::new();
    checkpoint.set_metadata("connector", CHECKPOINT_CONNECTOR);
    checkpoint.set_metadata("checkpoint_version", CHECKPOINT_VERSION);
    checkpoint.set_metadata("slot_prefix", &config.slot_name);
    checkpoint.set_metadata("publication", &config.publication);
    checkpoint.set_metadata("database", &config.database);
    checkpoint.set_metadata("table", config.table.to_string());
    let Some((claim, binding, phase)) = position else {
        return checkpoint;
    };
    checkpoint.set_metadata(CLAIM_METADATA, claim.id());
    checkpoint.set_metadata(SLOT_METADATA, claim.slot());
    match phase {
        CursorPhase::Claimed => checkpoint.set_metadata(PHASE_METADATA, PHASE_CLAIMED),
        CursorPhase::Snapshot { consistent_point } => {
            checkpoint.set_metadata(PHASE_METADATA, PHASE_SNAPSHOT);
            checkpoint.set_metadata(CONSISTENT_POINT_METADATA, consistent_point.to_string());
        }
        CursorPhase::Streaming {
            consistent_point,
            lsn,
        } => {
            checkpoint.set_metadata(PHASE_METADATA, PHASE_STREAMING);
            checkpoint.set_metadata(CONSISTENT_POINT_METADATA, consistent_point.to_string());
            checkpoint.set_offset(LSN_OFFSET, lsn.to_string());
        }
    }
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
    checkpoint
}

/// Parse a committed cursor.
///
/// # Errors
/// Rejects a cursor from another source or an older format, a slot that is not the cursor's
/// claim, and drifted configuration.
pub(super) fn parse_cursor(
    checkpoint: &SourceCheckpoint,
    config: &PostgresCdcConfig,
    context: &str,
) -> Result<Cursor, ConnectorError> {
    let version = required(checkpoint, "checkpoint_version", context)?;
    if version != CHECKPOINT_VERSION {
        let slot = checkpoint
            .get_metadata(SLOT_METADATA)
            .or_else(|| checkpoint.get_metadata(V4_SLOT_METADATA))
            .unwrap_or(&config.slot_name);
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} has checkpoint format version '{version}'; this release \
             resumes only version {CHECKPOINT_VERSION}: {}",
            reset_instructions(slot)
        )));
    }
    for (key, expected) in [
        ("connector", CHECKPOINT_CONNECTOR),
        ("slot_prefix", config.slot_name.as_str()),
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
    let claim = parse_claim(checkpoint, config, context)?;
    let phase = match required(checkpoint, PHASE_METADATA, context)? {
        PHASE_CLAIMED => CursorPhase::Claimed,
        PHASE_SNAPSHOT => CursorPhase::Snapshot {
            consistent_point: lsn(checkpoint.get_metadata(CONSISTENT_POINT_METADATA), context)?,
        },
        PHASE_STREAMING => CursorPhase::Streaming {
            consistent_point: lsn(checkpoint.get_metadata(CONSISTENT_POINT_METADATA), context)?,
            lsn: lsn(checkpoint.get_offset(LSN_OFFSET), context)?,
        },
        other => {
            return Err(ConnectorError::ConfigurationError(format!(
                "PostgreSQL CDC {context} has unknown phase '{other}'"
            )));
        }
    };
    let binding = binding(checkpoint, context)?;
    if binding.source_config_sha256 != source_config_digest(config) {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} table or output.mode drifted from its checkpoint"
        )));
    }
    Ok(Cursor {
        claim,
        binding,
        phase,
    })
}

fn parse_claim(
    checkpoint: &SourceCheckpoint,
    config: &PostgresCdcConfig,
    context: &str,
) -> Result<SlotClaim, ConnectorError> {
    let id = required(checkpoint, CLAIM_METADATA, context)?;
    let slot = required(checkpoint, SLOT_METADATA, context)?;
    match SlotClaim::parse(&config.slot_name, id) {
        Some(claim) if claim.slot() == slot => Ok(claim),
        _ => Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} names slot '{slot}' with claim '{id}', which is not a \
             slot this source creates under slot.name '{}': {}",
            config.slot_name,
            reset_instructions(slot)
        ))),
    }
}

fn lsn(value: Option<&str>, context: &str) -> Result<Lsn, ConnectorError> {
    let text = value.ok_or_else(|| {
        ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} is missing its slot position"
        ))
    })?;
    text.parse::<Lsn>().map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "invalid LSN '{text}' in PostgreSQL CDC {context}: {error}"
        ))
    })
}

/// Require a live binding to equal the one a checkpoint was captured under.
pub(super) fn validate_live_binding(
    checkpoint: &PostgresCheckpointBinding,
    live: &PostgresCheckpointBinding,
    context: &str,
) -> Result<(), ConnectorError> {
    if checkpoint != live {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC {context} identity drifted from the live database or publication (checkpoint: {checkpoint:?}; live: {live:?})"
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
    })
}
