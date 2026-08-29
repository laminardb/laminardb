use iceberg::spec::Snapshot;
use iceberg::table::Table;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::checkpoint::SourceCheckpoint;
use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::{stable_catalog_identity, IcebergSourceConfig};

const CURSOR_KEY: &str = "iceberg.cursor";
const CURSOR_VERSION: u8 = 1;
const MAX_CURSOR_BYTES: usize = 16 * 1024;

/// Replay position for an Iceberg snapshot lineage.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergSourceCursorV1 {
    /// Serialized cursor version.
    pub version: u8,
    /// Hash of non-secret catalog and storage identity.
    pub catalog_identity: String,
    /// Iceberg table UUID.
    pub table_uuid: String,
    /// Fully qualified namespace and table identifier.
    pub table_identifier: String,
    /// Iceberg ref followed by this source.
    pub table_ref: String,
    /// Last completely emitted snapshot.
    pub snapshot_id: i64,
    /// Sequence number of that snapshot.
    pub sequence_number: i64,
    /// Schema that binds the declared output columns to Iceberg field IDs.
    pub read_schema_id: i32,
    /// Metadata file observed when the cursor was created.
    pub metadata_location: String,
}

impl fmt::Debug for IcebergSourceCursorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table_identifier =
            crate::security::sanitize_identity_value("table_identifier", &self.table_identifier);
        let table_ref = crate::security::sanitize_identity_value("table_ref", &self.table_ref);
        let metadata_location =
            crate::security::sanitize_identity_value("metadata_location", &self.metadata_location);
        formatter
            .debug_struct("IcebergSourceCursorV1")
            .field("version", &self.version)
            .field("catalog_identity", &self.catalog_identity)
            .field("table_uuid", &self.table_uuid)
            .field("table_identifier", &table_identifier)
            .field("table_ref", &table_ref)
            .field("snapshot_id", &self.snapshot_id)
            .field("sequence_number", &self.sequence_number)
            .field("read_schema_id", &self.read_schema_id)
            .field("metadata_location", &metadata_location)
            .finish()
    }
}

impl IcebergSourceCursorV1 {
    pub(super) fn from_snapshot(
        config: &IcebergSourceConfig,
        table: &Table,
        snapshot: &Snapshot,
        read_schema_id: i32,
    ) -> Self {
        Self {
            version: CURSOR_VERSION,
            catalog_identity: stable_catalog_identity(&config.catalog, &config.storage),
            table_uuid: table.metadata().uuid().to_string(),
            table_identifier: table_identifier(config),
            table_ref: config.table_ref.clone(),
            snapshot_id: snapshot.snapshot_id(),
            sequence_number: snapshot.sequence_number(),
            read_schema_id,
            metadata_location: table.metadata_location().unwrap_or_default().to_string(),
        }
    }

    pub(super) fn validate_binding(
        &self,
        config: &IcebergSourceConfig,
        table: &Table,
    ) -> Result<(), ConnectorError> {
        if self.catalog_identity != stable_catalog_identity(&config.catalog, &config.storage) {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-CATALOG",
                "checkpoint catalog identity differs from configured catalog",
            ));
        }
        if self.table_uuid != table.metadata().uuid().to_string() {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-TABLE-UUID",
                "checkpoint table UUID differs from loaded table",
            ));
        }
        if self.table_identifier != table_identifier(config) {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-TABLE",
                "checkpoint table identifier differs from configured table",
            ));
        }
        if self.table_ref != config.table_ref {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-REF",
                "checkpoint table ref differs from configured table ref",
            ));
        }
        let snapshot = table
            .metadata()
            .snapshot_by_id(self.snapshot_id)
            .ok_or_else(|| {
                cursor_error(
                    "LDB-ICEBERG-CURSOR-EXPIRED",
                    "checkpoint snapshot is absent from retained table history",
                )
            })?;
        if snapshot.sequence_number() != self.sequence_number {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-SEQUENCE",
                "checkpoint snapshot sequence number differs from table metadata",
            ));
        }
        self.retained_schema(table)?;
        Ok(())
    }

    pub(super) fn retained_schema(
        &self,
        table: &Table,
    ) -> Result<iceberg::spec::SchemaRef, ConnectorError> {
        table
            .metadata()
            .schema_by_id(self.read_schema_id)
            .cloned()
            .ok_or_else(|| {
                cursor_error(
                    "LDB-ICEBERG-CURSOR-SCHEMA-EXPIRED",
                    &format!(
                        "checkpoint read schema {} is absent from retained table metadata",
                        self.read_schema_id
                    ),
                )
            })
    }

    /// Encodes this cursor as a connector checkpoint.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the cursor cannot be encoded.
    pub fn to_checkpoint(&self) -> Result<SourceCheckpoint, ConnectorError> {
        self.validate_payload()?;
        let encoded =
            serde_json::to_string(self).map_err(|error| ConnectorError::Serde(error.into()))?;
        if encoded.len() > MAX_CURSOR_BYTES {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-SIZE",
                "Iceberg cursor exceeds its fixed durability bound",
            ));
        }
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset(CURSOR_KEY, encoded);
        checkpoint.set_metadata("connector_type", "iceberg");
        Ok(checkpoint)
    }

    /// Decodes a versioned cursor from a connector checkpoint.
    ///
    /// # Errors
    ///
    /// Returns a stable configuration error for missing or unknown versions.
    pub fn from_checkpoint(checkpoint: &SourceCheckpoint) -> Result<Self, ConnectorError> {
        if checkpoint.get_metadata("connector_type") != Some("iceberg") {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-CONNECTOR",
                "resume checkpoint is not bound to the Iceberg connector",
            ));
        }
        let encoded = checkpoint.get_offset(CURSOR_KEY).ok_or_else(|| {
            cursor_error(
                "LDB-ICEBERG-CURSOR-MISSING",
                "resume checkpoint has no versioned Iceberg cursor",
            )
        })?;
        if encoded.len() > MAX_CURSOR_BYTES {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-SIZE",
                "Iceberg cursor exceeds its fixed durability bound",
            ));
        }
        let cursor: Self = serde_json::from_str(encoded).map_err(|_| {
            cursor_error(
                "LDB-ICEBERG-CURSOR-DECODE",
                "Iceberg cursor is not valid versioned JSON",
            )
        })?;
        cursor.validate_payload()?;
        Ok(cursor)
    }

    fn validate_payload(&self) -> Result<(), ConnectorError> {
        if self.version != CURSOR_VERSION {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-VERSION",
                &format!("unsupported cursor version {}", self.version),
            ));
        }
        if self.catalog_identity.len() != 64
            || !self
                .catalog_identity
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-CATALOG-IDENTITY",
                "cursor catalog identity is not a canonical SHA-256 digest",
            ));
        }
        let table_uuid = uuid::Uuid::parse_str(&self.table_uuid).map_err(|_| {
            cursor_error(
                "LDB-ICEBERG-CURSOR-TABLE-UUID-FORMAT",
                "cursor table UUID is not canonical",
            )
        })?;
        if table_uuid.is_nil() || table_uuid.to_string() != self.table_uuid {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-TABLE-UUID-FORMAT",
                "cursor table UUID is not canonical",
            ));
        }
        for (code, label, value) in [
            (
                "LDB-ICEBERG-CURSOR-TABLE-FORMAT",
                "table identifier",
                self.table_identifier.as_str(),
            ),
            (
                "LDB-ICEBERG-CURSOR-REF-FORMAT",
                "table ref",
                self.table_ref.as_str(),
            ),
        ] {
            if value.is_empty() || value.chars().any(char::is_control) {
                return Err(cursor_error(
                    code,
                    &format!("cursor {label} is empty or contains control characters"),
                ));
            }
        }
        if self.metadata_location.chars().any(char::is_control)
            || crate::security::value_contains_uri_secret(&self.metadata_location, false)
        {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-METADATA-LOCATION",
                "cursor metadata location is not safe for durable state",
            ));
        }
        Ok(())
    }
}

fn cursor_error(code: &str, detail: &str) -> ConnectorError {
    ConnectorError::ConfigurationError(format!("[{code}] {detail}"))
}

fn table_identifier(config: &IcebergSourceConfig) -> String {
    format!("{}.{}", config.catalog.namespace, config.catalog.table_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor() -> IcebergSourceCursorV1 {
        IcebergSourceCursorV1 {
            version: 1,
            catalog_identity: "0".repeat(64),
            table_uuid: "018f0f9d-7b2f-7a61-b72d-f4be1c7f43e1".into(),
            table_identifier: "ns.table".into(),
            table_ref: "main".into(),
            snapshot_id: 42,
            sequence_number: 7,
            read_schema_id: 3,
            metadata_location: "s3://bucket/metadata/v7.json".into(),
        }
    }

    fn unchecked_checkpoint(cursor: &IcebergSourceCursorV1) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset(CURSOR_KEY, serde_json::to_string(cursor).unwrap());
        checkpoint.set_metadata("connector_type", "iceberg");
        checkpoint
    }

    #[test]
    fn checkpoint_round_trip_is_versioned() {
        let cursor = cursor();
        let checkpoint = cursor.to_checkpoint().unwrap();
        assert_eq!(
            IcebergSourceCursorV1::from_checkpoint(&checkpoint).unwrap(),
            cursor
        );
    }

    #[test]
    fn unknown_cursor_version_is_rejected() {
        let mut cursor = cursor();
        cursor.version = 2;
        let error = IcebergSourceCursorV1::from_checkpoint(&unchecked_checkpoint(&cursor))
            .expect_err("future cursor version must fail closed");
        assert!(error.to_string().contains("LDB-ICEBERG-CURSOR-VERSION"));
    }

    #[test]
    fn cursor_shape_and_size_fail_closed_without_echoing_payloads() {
        let mut wrong_connector = cursor().to_checkpoint().unwrap();
        wrong_connector.set_metadata("connector_type", "kafka");
        assert!(IcebergSourceCursorV1::from_checkpoint(&wrong_connector)
            .unwrap_err()
            .to_string()
            .contains("LDB-ICEBERG-CURSOR-CONNECTOR"));

        let mut oversized = SourceCheckpoint::new();
        oversized.set_metadata("connector_type", "iceberg");
        oversized.set_offset(CURSOR_KEY, "x".repeat(MAX_CURSOR_BYTES + 1));
        assert!(IcebergSourceCursorV1::from_checkpoint(&oversized)
            .unwrap_err()
            .to_string()
            .contains("LDB-ICEBERG-CURSOR-SIZE"));

        let mut encoded = serde_json::to_value(cursor()).unwrap();
        encoded["oauth_client_secret=do-not-echo"] = serde_json::Value::Bool(true);
        let mut unknown = SourceCheckpoint::new();
        unknown.set_metadata("connector_type", "iceberg");
        unknown.set_offset(CURSOR_KEY, serde_json::to_string(&encoded).unwrap());
        let message = IcebergSourceCursorV1::from_checkpoint(&unknown)
            .unwrap_err()
            .to_string();
        assert!(message.contains("LDB-ICEBERG-CURSOR-DECODE"));
        assert!(!message.contains("do-not-echo"));
    }

    #[test]
    fn credential_bearing_metadata_location_is_never_durable_or_echoed() {
        let mut cursor = cursor();
        cursor.metadata_location = "s3://catalog-user:do-not-echo@bucket/metadata/v7.json".into();
        let debug = format!("{cursor:?}");
        assert!(!debug.contains("catalog-user"));
        assert!(!debug.contains("do-not-echo"));
        let message = cursor.to_checkpoint().unwrap_err().to_string();
        assert!(message.contains("LDB-ICEBERG-CURSOR-METADATA-LOCATION"));
        assert!(!message.contains("do-not-echo"));

        let message = IcebergSourceCursorV1::from_checkpoint(&unchecked_checkpoint(&cursor))
            .unwrap_err()
            .to_string();
        assert!(message.contains("LDB-ICEBERG-CURSOR-METADATA-LOCATION"));
        assert!(!message.contains("do-not-echo"));
    }

    #[test]
    fn noncanonical_cursor_identities_are_rejected() {
        let mut invalid_catalog = cursor();
        invalid_catalog.catalog_identity = "A".repeat(64);
        assert!(
            IcebergSourceCursorV1::from_checkpoint(&unchecked_checkpoint(&invalid_catalog))
                .unwrap_err()
                .to_string()
                .contains("LDB-ICEBERG-CURSOR-CATALOG-IDENTITY")
        );

        let mut invalid_uuid = cursor();
        invalid_uuid.table_uuid = uuid::Uuid::nil().to_string();
        assert!(
            IcebergSourceCursorV1::from_checkpoint(&unchecked_checkpoint(&invalid_uuid))
                .unwrap_err()
                .to_string()
                .contains("LDB-ICEBERG-CURSOR-TABLE-UUID-FORMAT")
        );
    }

    #[tokio::test]
    async fn table_uuid_and_ref_mismatches_are_rejected() {
        use crate::config::ConnectorConfig;
        use crate::lakehouse::iceberg::test_support::{append_rows, create_test_table};

        let fixture = create_test_table(false).await;
        let (table, _) = append_rows(&fixture, &fixture.table, 1, &[(1, Some("a"))]).await;
        let mut raw = ConnectorConfig::new("iceberg");
        raw.set("catalog.uri", fixture.config.catalog.catalog_uri.clone());
        raw.set(
            "catalog.warehouse",
            fixture.config.catalog.warehouse.clone(),
        );
        raw.set("namespace", "test");
        raw.set("table.name", "events");
        let config = IcebergSourceConfig::from_config(&raw).unwrap();
        let snapshot = table.metadata().current_snapshot().unwrap();
        let cursor = IcebergSourceCursorV1::from_snapshot(
            &config,
            &table,
            snapshot,
            table.metadata().current_schema_id(),
        );

        let mut wrong_uuid = cursor.clone();
        wrong_uuid.table_uuid = uuid::Uuid::now_v7().to_string();
        assert!(wrong_uuid
            .validate_binding(&config, &table)
            .unwrap_err()
            .to_string()
            .contains("LDB-ICEBERG-CURSOR-TABLE-UUID"));

        let mut wrong_ref = cursor;
        wrong_ref.table_ref = "audit".into();
        assert!(wrong_ref
            .validate_binding(&config, &table)
            .unwrap_err()
            .to_string()
            .contains("LDB-ICEBERG-CURSOR-REF"));

        let mut missing_schema = IcebergSourceCursorV1::from_snapshot(
            &config,
            &table,
            snapshot,
            table.metadata().current_schema_id(),
        );
        missing_schema.read_schema_id = i32::MAX;
        assert!(missing_schema
            .validate_binding(&config, &table)
            .unwrap_err()
            .to_string()
            .contains("LDB-ICEBERG-CURSOR-SCHEMA-EXPIRED"));
    }
}
