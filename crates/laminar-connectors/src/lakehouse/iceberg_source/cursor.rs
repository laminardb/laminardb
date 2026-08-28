use iceberg::spec::Snapshot;
use iceberg::table::Table;
use serde::{Deserialize, Serialize};

use crate::checkpoint::SourceCheckpoint;
use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::{stable_catalog_identity, IcebergSourceConfig};

const CURSOR_KEY: &str = "iceberg.cursor";
const CURSOR_VERSION: u8 = 1;

/// Replay position for an Iceberg snapshot lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Metadata file observed when the cursor was created.
    pub metadata_location: String,
}

impl IcebergSourceCursorV1 {
    pub(super) fn from_snapshot(
        config: &IcebergSourceConfig,
        table: &Table,
        snapshot: &Snapshot,
    ) -> Self {
        Self {
            version: CURSOR_VERSION,
            catalog_identity: stable_catalog_identity(&config.catalog, &config.storage),
            table_uuid: table.metadata().uuid().to_string(),
            table_identifier: table_identifier(config),
            table_ref: config.table_ref.clone(),
            snapshot_id: snapshot.snapshot_id(),
            sequence_number: snapshot.sequence_number(),
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
        Ok(())
    }

    /// Encodes this cursor as a connector checkpoint.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the cursor cannot be encoded.
    pub fn to_checkpoint(&self) -> Result<SourceCheckpoint, ConnectorError> {
        let encoded =
            serde_json::to_string(self).map_err(|error| ConnectorError::Serde(error.into()))?;
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
        let encoded = checkpoint.get_offset(CURSOR_KEY).ok_or_else(|| {
            cursor_error(
                "LDB-ICEBERG-CURSOR-MISSING",
                "resume checkpoint has no versioned Iceberg cursor",
            )
        })?;
        let cursor: Self = serde_json::from_str(encoded)
            .map_err(|error| cursor_error("LDB-ICEBERG-CURSOR-DECODE", &error.to_string()))?;
        if cursor.version != CURSOR_VERSION {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-VERSION",
                &format!("unsupported cursor version {}", cursor.version),
            ));
        }
        Ok(cursor)
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
            catalog_identity: "catalog".into(),
            table_uuid: "uuid".into(),
            table_identifier: "ns.table".into(),
            table_ref: "main".into(),
            snapshot_id: 42,
            sequence_number: 7,
            metadata_location: "s3://bucket/metadata/v7.json".into(),
        }
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
        let error = IcebergSourceCursorV1::from_checkpoint(&cursor.to_checkpoint().unwrap())
            .expect_err("future cursor version must fail closed");
        assert!(error.to_string().contains("LDB-ICEBERG-CURSOR-VERSION"));
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
        let cursor = IcebergSourceCursorV1::from_snapshot(&config, &table, snapshot);

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
    }
}
