//! Read-only native collection identity and validator metadata.

use std::collections::BTreeMap;
use std::time::Duration;

use mongodb::bson::{doc, Bson, Document};

use crate::error::ConnectorError;
use crate::schema::resolution::NativeSchema;

pub(super) struct CollectionMetadata {
    pub(super) native: NativeSchema,
    pub(super) validator: Document,
}

pub(super) async fn client(uri: &str) -> Result<mongodb::Client, ConnectorError> {
    let mut options = tokio::time::timeout(
        Duration::from_secs(10),
        mongodb::options::ClientOptions::parse(uri),
    )
    .await
    .map_err(|_| ConnectorError::Timeout(10_000))?
    .map_err(|_| {
        ConnectorError::ConfigurationError(
            "MongoDB metadata connection settings are invalid".into(),
        )
    })?;
    super::sink::harden_mongodb_tls(&mut options)?;
    options.server_selection_timeout = Some(Duration::from_secs(10));
    mongodb::Client::with_options(options)
        .map_err(|_| ConnectorError::ConnectionFailed("MongoDB metadata client unavailable".into()))
}

pub(super) async fn collection(
    database: &mongodb::Database,
    name: &str,
) -> Result<Option<CollectionMetadata>, ConnectorError> {
    let result = database
        .run_command(
            doc! {"listCollections": 1, "filter": {"name": name}, "cursor": {"batchSize": 1}},
        )
        .await
        .map_err(|_| {
            ConnectorError::ConnectionFailed(
                "MongoDB listCollections requires metadata authorization".into(),
            )
        })?;
    let entries = result
        .get_document("cursor")
        .and_then(|cursor| cursor.get_array("firstBatch"))
        .map_err(|_| {
            ConnectorError::SchemaMismatch("MongoDB collection metadata is malformed".into())
        })?;
    let Some(entry) = entries.first() else {
        return Ok(None);
    };
    let entry = entry.as_document().ok_or_else(|| {
        ConnectorError::SchemaMismatch("MongoDB collection metadata is not a document".into())
    })?;
    let uuid = entry
        .get_document("info")
        .ok()
        .and_then(|info| info.get("uuid"))
        .and_then(|value| match value {
            Bson::Binary(binary) => uuid::Uuid::from_slice(&binary.bytes).ok(),
            _ => None,
        })
        .ok_or_else(|| {
            ConnectorError::FeatureUnsupported(
                "MongoDB collection has no stable UUID; views are unsupported".into(),
            )
        })?;
    let options = entry.get_document("options").cloned().unwrap_or_default();
    let validator = options
        .get_document("validator")
        .cloned()
        .unwrap_or_default();
    let definition = serde_json::to_value(doc! {"validator": &validator,
    "timeseries": options.get("timeseries").cloned().unwrap_or(Bson::Null),
    "collation": options.get("collation").cloned().unwrap_or(Bson::Null)})
    .map_err(|_| {
        ConnectorError::SchemaMismatch("MongoDB native metadata cannot be retained".into())
    })?;
    let native = NativeSchema {
        format: "mongodb-bson".into(),
        identity: BTreeMap::from([
            ("collection_uuid".into(), uuid.hyphenated().to_string()),
            ("database".into(), database.name().into()),
            ("collection".into(), name.into()),
        ]),
        definition,
        references: Vec::new(),
    };
    Ok(Some(CollectionMetadata { native, validator }))
}
