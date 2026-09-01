//! Persisted `PostgreSQL` recovery identity parsing and live-binding validation.

use super::{
    source_config_digest, ConnectorError, PostgresCdcConfig, PostgresCheckpointBinding,
    SourceCheckpoint, CHECKPOINT_CONNECTOR, CHECKPOINT_VERSION, DATABASE_OID_METADATA,
    PUBLICATION_DEFINITION_METADATA, PUBLICATION_OID_METADATA, SLOT_FAILOVER_METADATA,
    SLOT_PLUGIN_METADATA, SLOT_TWO_PHASE_METADATA, SOURCE_CONFIG_METADATA,
    SYSTEM_IDENTIFIER_METADATA, TIMELINE_ID_METADATA,
};

pub(super) fn required_checkpoint_metadata<'a>(
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

pub(super) fn parse_checkpoint_decimal<T>(
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

pub(super) fn parse_checkpoint_bool(
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

pub(super) fn parse_checkpoint_sha256(
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

pub(super) fn checkpoint_binding(
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

pub(super) fn validate_checkpoint_identity(
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

pub(super) fn write_checkpoint_binding(
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
