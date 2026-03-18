//! MongoDB I/O layer — driver connection and change stream management.
//!
//! This module is only compiled with the `mongodb-cdc` feature and contains
//! the actual MongoDB driver calls.

use mongodb::bson::Document;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType, ResumeToken};
use mongodb::options::{
    ChangeStreamOptions, ClientOptions, FullDocumentBeforeChangeType, FullDocumentType,
};
use mongodb::{Client, Collection, Database};

use crate::error::ConnectorError;

use super::changelog::{CdcOperation, ChangeEvent};
use super::config::{FullDocumentBeforeChangeMode, FullDocumentMode, MongoCdcConfig};

/// Connects to MongoDB using the configuration.
///
/// # Errors
///
/// Returns `ConnectorError::ConnectionFailed` if the connection fails.
pub async fn connect(config: &MongoCdcConfig) -> Result<Client, ConnectorError> {
    let client_options = ClientOptions::parse(&config.connection_uri)
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

    let client = Client::with_options(client_options)
        .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

    // Verify connectivity by listing database names
    client
        .list_database_names()
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("ping failed: {e}")))?;

    Ok(client)
}

/// Opens a change stream on the specified database or collection.
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` if the change stream cannot be opened.
pub async fn open_change_stream(
    client: &Client,
    config: &MongoCdcConfig,
    resume_token: Option<&str>,
) -> Result<mongodb::change_stream::ChangeStream<ChangeStreamEvent<Document>>, ConnectorError> {
    let mut options = ChangeStreamOptions::default();

    // Set full document mode
    options.full_document = match config.full_document {
        FullDocumentMode::Off => None,
        FullDocumentMode::UpdateLookup => Some(FullDocumentType::UpdateLookup),
        FullDocumentMode::WhenAvailable => Some(FullDocumentType::WhenAvailable),
        FullDocumentMode::Required => Some(FullDocumentType::Required),
    };

    // Set full document before change mode
    options.full_document_before_change = match config.full_document_before_change {
        FullDocumentBeforeChangeMode::Off => None,
        FullDocumentBeforeChangeMode::WhenAvailable => {
            Some(FullDocumentBeforeChangeType::WhenAvailable)
        }
        FullDocumentBeforeChangeMode::Required => Some(FullDocumentBeforeChangeType::Required),
    };

    // Set resume token if provided (deserialize from JSON)
    if let Some(token_str) = resume_token {
        let token: ResumeToken = serde_json::from_str(token_str)
            .map_err(|e| ConnectorError::ReadError(format!("invalid resume token: {e}")))?;
        options.resume_after = Some(token);
    }

    // Parse pipeline stages
    let pipeline: Vec<Document> = config
        .pipeline
        .iter()
        .filter_map(|stage| serde_json::from_str::<serde_json::Value>(stage).ok())
        .filter_map(|v| mongodb::bson::to_document(&v).ok())
        .collect();

    let db: Database = client.database(&config.database);

    let stream = if let Some(ref collection_name) = config.collection {
        let collection: Collection<Document> = db.collection(collection_name);
        collection
            .watch()
            .pipeline(pipeline)
            .with_options(options)
            .await
            .map_err(|e| ConnectorError::ReadError(format!("change stream open failed: {e}")))?
    } else {
        db.watch()
            .pipeline(pipeline)
            .with_options(options)
            .await
            .map_err(|e| ConnectorError::ReadError(format!("change stream open failed: {e}")))?
    };

    Ok(stream)
}

/// Maps a MongoDB `OperationType` enum to our `CdcOperation`.
fn map_operation_type(op: &OperationType) -> Option<CdcOperation> {
    match op {
        OperationType::Insert => Some(CdcOperation::Insert),
        OperationType::Update => Some(CdcOperation::Update),
        OperationType::Replace => Some(CdcOperation::Replace),
        OperationType::Delete => Some(CdcOperation::Delete),
        // DDL and control events are not CDC row events
        _ => None,
    }
}

/// Decodes a MongoDB change stream event into a `ChangeEvent`.
///
/// Returns `None` for non-DML events (drop, invalidate, etc.) or if
/// the event's collection is filtered out.
pub fn decode_change_event(
    event: &ChangeStreamEvent<Document>,
    collection_include: &[String],
    collection_exclude: &[String],
) -> Option<ChangeEvent> {
    // Extract operation type
    let operation = map_operation_type(&event.operation_type)?;

    // Extract namespace (database.collection)
    let ns = event.ns.as_ref()?;
    let db_name = ns.db.as_str();
    let coll_name = ns.coll.as_ref()?;

    // Apply collection filter
    if collection_exclude.iter().any(|c| c == coll_name.as_str()) {
        return None;
    }
    if !collection_include.is_empty() && !collection_include.iter().any(|c| c == coll_name.as_str())
    {
        return None;
    }

    let collection = format!("{db_name}.{coll_name}");

    // Extract timestamp
    let timestamp_ms = event
        .cluster_time
        .map(|ts| i64::from(ts.time) * 1000)
        .unwrap_or(0);

    // Extract document key
    let document_key = event
        .document_key
        .as_ref()
        .map(|dk| serde_json::to_string(dk).unwrap_or_default())
        .unwrap_or_default();

    // Extract full document
    let full_document = event
        .full_document
        .as_ref()
        .map(|doc| serde_json::to_string(doc).unwrap_or_default());

    // Extract full document before change
    let full_document_before_change = event
        .full_document_before_change
        .as_ref()
        .map(|doc| serde_json::to_string(doc).unwrap_or_default());

    // Extract update description
    let update_description = event.update_description.as_ref().map(|ud| {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "updatedFields".to_string(),
            serde_json::to_value(&ud.updated_fields).unwrap_or_default(),
        );
        obj.insert(
            "removedFields".to_string(),
            serde_json::to_value(&ud.removed_fields).unwrap_or_default(),
        );
        serde_json::to_string(&obj).unwrap_or_default()
    });

    // Extract resume token as JSON for checkpointing
    let resume_token = serde_json::to_string(&event.id).unwrap_or_default();

    Some(ChangeEvent {
        collection,
        operation,
        timestamp_ms,
        document_key,
        full_document,
        full_document_before_change,
        update_description,
        resume_token,
    })
}
