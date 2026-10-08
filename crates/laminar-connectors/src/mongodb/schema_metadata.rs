//! Read-only native collection identity and validator metadata.

use std::collections::BTreeMap;
use std::time::Duration;

use mongodb::bson::{doc, Bson, Document};

use super::CollectionKind;
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
    kind: &CollectionKind,
) -> Result<Option<CollectionMetadata>, ConnectorError> {
    let Some(entry) = collection_entry(database, name).await? else {
        return Ok(None);
    };
    let (uuid, bucket_name) = match (kind, entry.get_str("type")) {
        (CollectionKind::Standard, Ok("collection")) => (collection_uuid(&entry)?, None),
        (CollectionKind::TimeSeries(_), Ok("timeseries")) => {
            // INVARIANT: the logical time-series view has no UUID; its bucket UUID
            // fences recreation while the logical options retain the writer contract.
            let bucket_name = format!("system.buckets.{name}");
            let bucket = collection_entry(database, &bucket_name)
                .await?
                .ok_or_else(|| {
                    ConnectorError::SchemaMismatch(
                        "MongoDB time-series bucket collection is missing".into(),
                    )
                })?;
            if bucket.get_str("type") != Ok("collection") {
                return Err(ConnectorError::SchemaMismatch(
                    "MongoDB time-series bucket is not a collection".into(),
                ));
            }
            (collection_uuid(&bucket)?, Some(bucket_name))
        }
        (CollectionKind::Standard, Ok("timeseries")) => {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB standard target '{name}' already exists as Timeseries"
            )));
        }
        (_, Ok("view")) => {
            return Err(ConnectorError::FeatureUnsupported(
                "MongoDB views are unsupported".into(),
            ));
        }
        (CollectionKind::TimeSeries(_), Ok("collection")) => {
            return Err(ConnectorError::ConfigurationError(format!(
                "existing MongoDB collection '{name}' is not a time series collection"
            )));
        }
        _ => {
            return Err(ConnectorError::SchemaMismatch(
                "MongoDB collection type is malformed or unsupported".into(),
            ));
        }
    };
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
    let mut identity = BTreeMap::from([
        ("collection_uuid".into(), uuid.hyphenated().to_string()),
        ("database".into(), database.name().into()),
        ("collection".into(), name.into()),
    ]);
    if let Some(bucket_name) = bucket_name {
        identity.insert("bucket_collection".into(), bucket_name);
    }
    let native = NativeSchema {
        format: "mongodb-bson".into(),
        identity,
        definition,
        references: Vec::new(),
    };
    Ok(Some(CollectionMetadata { native, validator }))
}

async fn collection_entry(
    database: &mongodb::Database,
    name: &str,
) -> Result<Option<Document>, ConnectorError> {
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
    entries
        .first()
        .map(|entry| {
            entry.as_document().cloned().ok_or_else(|| {
                ConnectorError::SchemaMismatch(
                    "MongoDB collection metadata is not a document".into(),
                )
            })
        })
        .transpose()
}

fn collection_uuid(entry: &Document) -> Result<uuid::Uuid, ConnectorError> {
    entry
        .get_document("info")
        .ok()
        .and_then(|info| info.get("uuid"))
        .and_then(|value| match value {
            Bson::Binary(binary) if binary.subtype == mongodb::bson::spec::BinarySubtype::Uuid => {
                uuid::Uuid::from_slice(&binary.bytes).ok()
            }
            _ => None,
        })
        .ok_or_else(|| {
            ConnectorError::FeatureUnsupported("MongoDB collection has no valid stable UUID".into())
        })
}
