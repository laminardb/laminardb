//! One-pass decoding of raw change-stream events into their stable fields.

use mongodb::bson::{spec::BinarySubtype, RawBsonRef, RawDocument, RawDocumentBuf, Timestamp};
use uuid::Uuid;

use super::super::change_event::{canonical_document_extjson, ChangeOperation};
use super::ConnectorError;

/// Fields read once from a raw change event. Borrowed values point into the event bytes.
pub(super) struct DecodedChange<'a> {
    pub(super) operation: &'a str,
    pub(super) database: Option<&'a str>,
    pub(super) collection: Option<&'a str>,
    pub(super) collection_uuid: Option<Uuid>,
    pub(super) document_key: Option<&'a RawDocument>,
    pub(super) full_document: Option<&'a RawDocument>,
    pub(super) update_description: Option<&'a RawDocument>,
    pub(super) cluster_time: Option<Timestamp>,
    pub(super) wall_time_us: Option<i64>,
    pub(super) txn_number: Option<i64>,
    pub(super) lsid: Option<&'a RawDocument>,
    /// Every other field (for example `to`, `nsType`, `operationDescription`).
    pub(super) details: Option<RawDocumentBuf>,
}

fn malformed(field: &str, error: impl std::fmt::Display) -> ConnectorError {
    ConnectorError::ReadError(format!(
        "malformed MongoDB change event field '{field}': {error}"
    ))
}

fn optional_document<'a>(
    field: &str,
    value: RawBsonRef<'a>,
) -> Result<Option<&'a RawDocument>, ConnectorError> {
    match value {
        RawBsonRef::Document(document) => Ok(Some(document)),
        RawBsonRef::Null => Ok(None),
        other => Err(malformed(
            field,
            format!("expected a document, got {:?}", other.element_type()),
        )),
    }
}

/// Read the operation type without decoding the rest of the event.
pub(super) fn event_operation(raw: &RawDocument) -> Result<ChangeOperation, ConnectorError> {
    let operation = raw
        .get_str("operationType")
        .map_err(|error| malformed("operationType", error))?;
    Ok(ChangeOperation::classify(operation))
}

/// Canonical JSON of the event's own `_id`, encoded exactly like a driver resume token.
pub(super) fn event_token(raw: &RawDocument) -> Result<String, ConnectorError> {
    #[derive(serde::Deserialize)]
    struct EventId {
        #[serde(rename = "_id")]
        id: mongodb::change_stream::event::ResumeToken,
    }
    let id: EventId =
        mongodb::bson::from_slice(raw.as_bytes()).map_err(|error| malformed("_id", error))?;
    serde_json::to_string(&id.id).map_err(|error| malformed("_id", error))
}

pub(super) fn decode_change(raw: &RawDocument) -> Result<DecodedChange<'_>, ConnectorError> {
    let mut decoded = DecodedChange {
        operation: "",
        database: None,
        collection: None,
        collection_uuid: None,
        document_key: None,
        full_document: None,
        update_description: None,
        cluster_time: None,
        wall_time_us: None,
        txn_number: None,
        lsid: None,
        details: None,
    };
    let mut details = RawDocumentBuf::new();
    for element in raw {
        let (key, value) = element.map_err(|error| malformed("event", error))?;
        match key {
            "_id" => {}
            "operationType" => {
                decoded.operation = value
                    .as_str()
                    .ok_or_else(|| malformed(key, "not a string"))?;
            }
            "ns" => {
                let ns = value
                    .as_document()
                    .ok_or_else(|| malformed(key, "not a document"))?;
                decoded.database = Some(
                    ns.get_str("db")
                        .map_err(|error| malformed("ns.db", error))?,
                );
                decoded.collection = match ns
                    .get("coll")
                    .map_err(|error| malformed("ns.coll", error))?
                {
                    Some(RawBsonRef::String(collection)) => Some(collection),
                    None => None,
                    Some(_) => return Err(malformed("ns.coll", "not a string")),
                };
            }
            "collectionUUID" => match value {
                RawBsonRef::Binary(binary) if binary.subtype == BinarySubtype::Uuid => {
                    decoded.collection_uuid = Some(
                        Uuid::from_slice(binary.bytes).map_err(|error| malformed(key, error))?,
                    );
                }
                _ => return Err(malformed(key, "not a UUID")),
            },
            "documentKey" => decoded.document_key = optional_document(key, value)?,
            "fullDocument" => decoded.full_document = optional_document(key, value)?,
            "updateDescription" => decoded.update_description = optional_document(key, value)?,
            "clusterTime" => {
                decoded.cluster_time = Some(
                    value
                        .as_timestamp()
                        .ok_or_else(|| malformed(key, "not a timestamp"))?,
                );
            }
            "wallTime" => {
                decoded.wall_time_us = Some(
                    value
                        .as_datetime()
                        .ok_or_else(|| malformed(key, "not a date"))?
                        .timestamp_millis()
                        .checked_mul(1000)
                        .ok_or_else(|| malformed(key, "outside the microsecond range"))?,
                );
            }
            "txnNumber" => {
                decoded.txn_number = Some(match value {
                    RawBsonRef::Int64(number) => number,
                    RawBsonRef::Int32(number) => i64::from(number),
                    _ => return Err(malformed(key, "not an integer")),
                });
            }
            "lsid" => decoded.lsid = optional_document(key, value)?,
            _ => details.append(key, value.to_raw_bson()),
        }
    }
    if decoded.operation.is_empty() {
        return Err(malformed("operationType", "missing"));
    }
    decoded.details = (!details.is_empty()).then_some(details);
    Ok(decoded)
}

/// Canonical Extended JSON for an optional raw document.
pub(super) fn optional_extjson(
    document: Option<&RawDocument>,
) -> Result<Option<String>, ConnectorError> {
    document.map(canonical_document_extjson).transpose()
}
