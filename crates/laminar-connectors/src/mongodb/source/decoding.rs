//! `MongoDB` change-event decoding into the stable `LaminarDB` envelope.

use super::{ConnectorError, MongoDbChangeEvent, OperationType};

/// Parses a `ChangeStreamEvent<Document>` into a [`MongoDbChangeEvent`].
#[cfg(feature = "mongodb-cdc")]
pub(super) fn parse_change_stream_event(
    event: &mongodb::change_stream::event::ChangeStreamEvent<mongodb::bson::Document>,
) -> Result<MongoDbChangeEvent, ConnectorError> {
    let operation_type = parse_operation_type(&event.operation_type)?;
    let namespace = parse_namespace(event.ns.as_ref());
    let document_key = event.document_key.as_ref().map_or_else(
        || Ok(String::new()),
        |document| serialize_document(document, "document key"),
    )?;
    let full_document = event
        .full_document
        .as_ref()
        .map(|document| serialize_document(document, "full document"))
        .transpose()?;
    let update_description = event
        .update_description
        .as_ref()
        .map(parse_update_description)
        .transpose()?;
    let (cluster_time_secs, cluster_time_inc) = event
        .cluster_time
        .map_or((0, 0), |timestamp| (timestamp.time, timestamp.increment));
    let wall_time_ms = event
        .wall_time
        .map_or(0, mongodb::bson::DateTime::timestamp_millis);
    let resume_token = serde_json::to_string(&event.id)
        .map_err(|error| ConnectorError::ReadError(format!("resume token: {error}")))?;

    Ok(MongoDbChangeEvent {
        operation_type,
        namespace,
        document_key,
        full_document,
        update_description,
        cluster_time_secs,
        cluster_time_inc,
        resume_token,
        wall_time_ms,
    })
}

#[cfg(feature = "mongodb-cdc")]
fn parse_operation_type(
    operation: &mongodb::change_stream::event::OperationType,
) -> Result<OperationType, ConnectorError> {
    use mongodb::change_stream::event::OperationType as MongoOpType;

    let parsed = match operation {
        MongoOpType::Insert => OperationType::Insert,
        MongoOpType::Update => OperationType::Update,
        MongoOpType::Replace => OperationType::Replace,
        MongoOpType::Delete => OperationType::Delete,
        MongoOpType::Drop => OperationType::Drop,
        MongoOpType::Rename => OperationType::Rename,
        MongoOpType::Invalidate => OperationType::Invalidate,
        MongoOpType::DropDatabase => OperationType::DropDatabase,
        MongoOpType::Other(value) => OperationType::Other(value.clone()),
        other => {
            return Err(ConnectorError::ReadError(format!(
                "unsupported MongoDB operation type: {other:?}"
            )));
        }
    };
    Ok(parsed)
}

#[cfg(feature = "mongodb-cdc")]
fn parse_namespace(
    namespace: Option<&mongodb::change_stream::event::ChangeNamespace>,
) -> super::super::change_event::Namespace {
    namespace.map_or_else(
        || super::super::change_event::Namespace {
            db: String::new(),
            coll: String::new(),
        },
        |ns| super::super::change_event::Namespace {
            db: ns.db.clone(),
            coll: ns.coll.clone().unwrap_or_default(),
        },
    )
}

#[cfg(feature = "mongodb-cdc")]
fn serialize_document(
    document: &mongodb::bson::Document,
    context: &str,
) -> Result<String, ConnectorError> {
    serde_json::to_string(document)
        .map_err(|error| ConnectorError::ReadError(format!("{context}: {error}")))
}

#[cfg(feature = "mongodb-cdc")]
fn parse_update_description(
    update: &mongodb::change_stream::event::UpdateDescription,
) -> Result<super::super::change_event::UpdateDescription, ConnectorError> {
    use super::super::change_event::UpdateDescription;

    let updated_fields = update
        .updated_fields
        .iter()
        .map(|(key, value)| {
            serde_json::to_value(value)
                .map(|value| (key.clone(), value))
                .map_err(|error| {
                    ConnectorError::ReadError(format!("updated field '{key}': {error}"))
                })
        })
        .collect::<Result<_, _>>()?;
    let truncated_arrays = update
        .truncated_arrays
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|array| {
            let new_size = u32::try_from(array.new_size).map_err(|_| {
                ConnectorError::ReadError(format!(
                    "truncated array '{}' has negative newSize {}",
                    array.field, array.new_size
                ))
            })?;
            Ok(super::super::change_event::TruncatedArray {
                field: array.field.clone(),
                new_size,
            })
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    let disambiguated_paths = update
        .disambiguated_paths
        .as_ref()
        .map(|paths| {
            paths
                .iter()
                .map(|(key, value)| {
                    serde_json::to_value(value)
                        .map(|value| (key.clone(), value))
                        .map_err(|error| {
                            ConnectorError::ReadError(format!("updated field '{key}': {error}"))
                        })
                })
                .collect::<Result<_, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    Ok(UpdateDescription {
        updated_fields,
        removed_fields: update.removed_fields.clone(),
        truncated_arrays,
        disambiguated_paths,
    })
}
