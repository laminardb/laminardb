//! Connector-owned semantic options used to admit durable source recovery.

use std::collections::BTreeMap;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

/// Return the semantic source options that must remain stable across recovery.
///
/// `None` leaves custom and unadapted connectors on the conservative raw-property
/// identity. Adapted CDC sources omit endpoint, credentials, TLS, and memory tuning
/// because their checkpoints independently bind the live database object.
///
/// # Errors
///
/// Returns a configuration error when an adapted source cannot be parsed.
pub fn source_options(
    config: &ConnectorConfig,
) -> Result<Option<BTreeMap<String, String>>, ConnectorError> {
    match config.connector_type() {
        #[cfg(feature = "postgres-cdc")]
        "postgres-cdc" => postgres_options(config).map(Some),
        #[cfg(feature = "mongodb-cdc")]
        "mongodb-cdc" => mongodb_options(config).map(Some),
        _ => Ok(None),
    }
}

#[cfg(feature = "postgres-cdc")]
fn postgres_options(config: &ConnectorConfig) -> Result<BTreeMap<String, String>, ConnectorError> {
    let parsed = crate::cdc::postgres::PostgresCdcConfig::from_config(config)?;
    Ok(BTreeMap::from([
        ("database".into(), parsed.database),
        ("publication".into(), parsed.publication),
        ("slot.name".into(), parsed.slot_name),
        ("table.exclude".into(), parsed.table_exclude.join(",")),
        ("table.include".into(), parsed.table_include.join(",")),
        ("wire.protocol".into(), "pgoutput-v1".into()),
    ]))
}

#[cfg(feature = "mongodb-cdc")]
fn mongodb_options(config: &ConnectorConfig) -> Result<BTreeMap<String, String>, ConnectorError> {
    let parsed = crate::mongodb::MongoDbSourceConfig::from_config(config)?;
    let pipeline = crate::mongodb::config::canonical_pipeline_json(&parsed.pipeline);
    Ok(BTreeMap::from([
        ("collection".into(), parsed.collection),
        ("database".into(), parsed.database),
        (
            "full.document.mode".into(),
            parsed.full_document_mode.to_string(),
        ),
        ("pipeline".into(), pipeline),
        ("wire.protocol".into(), "change-stream-expanded-v1".into()),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "postgres-cdc")]
    fn postgres_config() -> ConnectorConfig {
        let mut config = ConnectorConfig::new("postgres-cdc");
        config.set("host", "db-a.internal");
        config.set("database", "orders");
        config.set("username", "replicator");
        config.set("password", "secret-a");
        config.set("slot.name", "orders_slot");
        config.set("publication", "orders_pub");
        config.set("table.include", "public.z, public.a");
        config
    }

    #[cfg(feature = "postgres-cdc")]
    #[test]
    fn postgres_identity_ignores_operational_connection_tuning() {
        let left = postgres_config();
        let mut right = postgres_config();
        right.set("host", "db-b.internal");
        right.set("port", "6432");
        right.set("username", "rotated-user");
        right.set("password", "rotated-secret");
        right.set("ssl.mode", "disable");
        right.set("max.buffered.bytes", "134217728");

        assert_eq!(
            source_options(&left).unwrap(),
            source_options(&right).unwrap()
        );
    }

    #[cfg(feature = "postgres-cdc")]
    #[test]
    fn postgres_identity_normalizes_filters_and_tracks_slot_semantics() {
        let left = postgres_config();
        let mut reordered = postgres_config();
        reordered.set("table.include", "public.a,public.z,public.a");
        assert_eq!(
            source_options(&left).unwrap(),
            source_options(&reordered).unwrap()
        );

        let mut different_slot = postgres_config();
        different_slot.set("slot.name", "other_slot");
        assert_ne!(
            source_options(&left).unwrap(),
            source_options(&different_slot).unwrap()
        );
    }

    #[cfg(feature = "mongodb-cdc")]
    fn mongodb_config() -> ConnectorConfig {
        let mut config = ConnectorConfig::new("mongodb-cdc");
        config.set("connection.uri", "mongodb://db-a.internal:27017");
        config.set("database", "orders");
        config.set("collection", "events");
        config
    }

    #[cfg(feature = "mongodb-cdc")]
    #[test]
    fn mongodb_identity_ignores_endpoint_and_memory_tuning() {
        let left = mongodb_config();
        let mut right = mongodb_config();
        right.set("connection.uri", "mongodb://db-b.internal:27017");
        right.set("max.buffered.bytes", "134217728");

        assert_eq!(
            source_options(&left).unwrap(),
            source_options(&right).unwrap()
        );
    }

    #[cfg(feature = "mongodb-cdc")]
    #[test]
    fn mongodb_identity_tracks_collection_and_delivery_shape() {
        let left = mongodb_config();
        let mut collection = mongodb_config();
        collection.set("collection", "other_events");
        assert_ne!(
            source_options(&left).unwrap(),
            source_options(&collection).unwrap()
        );

        let mut full_document = mongodb_config();
        full_document.set("full.document.mode", "required");
        assert_ne!(
            source_options(&left).unwrap(),
            source_options(&full_document).unwrap()
        );
    }
}
