//! Versioned recovery evidence persisted before a coordinated epoch can write.

use serde::{Deserialize, Serialize};

use crate::connector::{CoordinatedAbortEntry, CoordinatedCommitNamespace};
use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::{stable_catalog_identity, IcebergSinkConfig};
use crate::lakehouse::iceberg_io::{
    effective_data_location, effective_data_location_from_metadata,
    validate_credential_free_location,
};

use super::descriptor::{validate_table_binding_shape, IcebergTableBindingV1};
use super::descriptor_batch::validate_table_incarnation;
use super::epoch_writer::EpochIdentity;
use super::file_finalizer::replay_safe_prefix;

const EPOCH_INTENT_VERSION: u8 = 1;
const ATTEMPT_DIRECTORY: &str = "laminardb";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IcebergEpochIntentV1 {
    version: u8,
    table: IcebergTableBindingV1,
    deployment_id: String,
    sink_id: String,
    participant_id: u64,
    epoch_id: u64,
    data_location: String,
    attempt_root: String,
}

impl std::fmt::Debug for IcebergEpochIntentV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergEpochIntentV1")
            .field("version", &self.version)
            .field("table", &self.table)
            .field("participant_id", &self.participant_id)
            .field("epoch_id", &self.epoch_id)
            .finish_non_exhaustive()
    }
}

impl IcebergEpochIntentV1 {
    pub(super) fn capture(
        table: &iceberg::table::Table,
        config: &IcebergSinkConfig,
        identity: &EpochIdentity,
    ) -> Result<Self, ConnectorError> {
        let data_location = normalize_data_location(&effective_data_location(table))?;
        let attempt_root = coordinated_attempt_root(&data_location, identity);
        let intent = Self {
            version: EPOCH_INTENT_VERSION,
            table: IcebergTableBindingV1::from_table(table, config)?,
            deployment_id: identity.deployment_id.clone(),
            sink_id: identity.sink_id.clone(),
            participant_id: identity.participant_id,
            epoch_id: identity.epoch,
            data_location,
            attempt_root,
        };
        intent.validate_shape()?;
        Ok(intent)
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, ConnectorError> {
        laminar_core::checkpoint::canonical_json_bytes(self).map_err(|error| {
            ConnectorError::Internal(format!("encode Iceberg epoch artifact intent: {error}"))
        })
    }

    pub(super) fn decode(payload: &[u8]) -> Result<Self, ConnectorError> {
        let intent: Self = serde_json::from_slice(payload).map_err(|_| {
            ConnectorError::TransactionError(
                "[LDB-ICEBERG-EPOCH-INTENT-DECODE] invalid Iceberg epoch artifact intent".into(),
            )
        })?;
        intent.validate_shape()?;
        Ok(intent)
    }

    pub(super) fn attempt_root(&self) -> &str {
        &self.attempt_root
    }

    pub(super) fn namespace_prefix(&self) -> &str {
        self.attempt_root
            .rsplit('/')
            .next()
            .unwrap_or(self.attempt_root.as_str())
    }

    pub(super) fn validate_writer(
        &self,
        table: &iceberg::table::Table,
        config: &IcebergSinkConfig,
        identity: &EpochIdentity,
    ) -> Result<(), ConnectorError> {
        self.validate_runtime(identity)?;
        let current = IcebergTableBindingV1::from_table(table, config)?;
        if !self.table.has_same_append_target(&current)
            || self.data_location != normalize_data_location(&effective_data_location(table))?
        {
            return Err(ConnectorError::TransactionError(
                "[LDB-ICEBERG-EPOCH-INTENT-CHANGED] Iceberg table layout changed after artifact admission"
                    .into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn validate_cleanup(
        &self,
        config: &IcebergSinkConfig,
        namespace: &CoordinatedCommitNamespace,
        entry: &CoordinatedAbortEntry,
        table: &iceberg::table::Table,
        deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        let identity = EpochIdentity {
            deployment_id: namespace.deployment_id.clone(),
            sink_id: namespace.sink_id.clone(),
            participant_id: entry.participant_id,
            epoch: entry.attempt.epoch,
        };
        self.validate_runtime(&identity)?;
        if self.table.catalog_identity != stable_catalog_identity(&config.catalog, &config.storage)
        {
            return Err(intent_binding_error());
        }
        validate_table_incarnation(config, &self.table, table)?;
        let metadata = tokio::time::timeout_at(
            deadline,
            iceberg::spec::TableMetadata::read_from(table.file_io(), &self.table.metadata_location),
        )
        .await
        .map_err(|_| {
            ConnectorError::WriteError(
                "[LDB-ICEBERG-EPOCH-INTENT-TIMEOUT] historical table metadata read timed out"
                    .into(),
            )
        })?
        .map_err(|error| {
            ConnectorError::WriteError(format!(
                "[LDB-ICEBERG-EPOCH-INTENT-METADATA] historical table metadata read failed ({})",
                crate::lakehouse::iceberg_io::external_error_summary(&error)
            ))
        })?;
        let historical_data =
            normalize_data_location(&effective_data_location_from_metadata(&metadata))?;
        if metadata.uuid().to_string() != self.table.table_uuid
            || metadata.location() != self.table.table_location
            || historical_data != self.data_location
        {
            return Err(intent_binding_error());
        }
        Ok(())
    }

    fn validate_runtime(&self, identity: &EpochIdentity) -> Result<(), ConnectorError> {
        if self.deployment_id != identity.deployment_id
            || self.sink_id != identity.sink_id
            || self.participant_id != identity.participant_id
            || self.epoch_id != identity.epoch
        {
            return Err(intent_binding_error());
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), ConnectorError> {
        validate_table_binding_shape(&self.table)?;
        validate_credential_free_location("epoch data location", &self.data_location)?;
        validate_credential_free_location("epoch attempt root", &self.attempt_root)?;
        let identity = EpochIdentity {
            deployment_id: self.deployment_id.clone(),
            sink_id: self.sink_id.clone(),
            participant_id: self.participant_id,
            epoch: self.epoch_id,
        };
        if self.version != EPOCH_INTENT_VERSION
            || self.participant_id == 0
            || self.epoch_id == 0
            || self.data_location.is_empty()
            || self.attempt_root != coordinated_attempt_root(&self.data_location, &identity)
        {
            return Err(intent_binding_error());
        }
        Ok(())
    }
}

pub(super) fn coordinated_attempt_root(data_location: &str, identity: &EpochIdentity) -> String {
    format!(
        "{data_location}/{ATTEMPT_DIRECTORY}/{}",
        replay_safe_prefix(
            &identity.deployment_id,
            &identity.sink_id,
            identity.participant_id,
            identity.epoch,
        )
    )
}

fn normalize_data_location(location: &str) -> Result<String, ConnectorError> {
    let normalized = location.trim_end_matches('/');
    if normalized.is_empty()
        || normalized
            .bytes()
            .any(|byte| byte.is_ascii_control() || matches!(byte, b'\\' | b'?' | b'#'))
    {
        return Err(intent_binding_error());
    }
    Ok(normalized.to_owned())
}

fn intent_binding_error() -> ConnectorError {
    ConnectorError::TransactionError(
        "[LDB-ICEBERG-EPOCH-INTENT-BINDING] Iceberg epoch artifact intent is not bound to the exact table and runtime"
            .into(),
    )
}
