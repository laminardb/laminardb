//! Fail-closed CDC replay validation and ordered write preparation.

use super::{
    account_bson_document, cdc_bulk_models, json_to_bson, json_to_bson_document,
    validate_cdc_document_key, validate_cdc_replacement_key, CdcWrite, ConnectorError, MongoDbSink,
};
use crate::mongodb::change_event::ChangeOperation;
use crate::mongodb::SNAPSHOT_OPERATION;
use mongodb::bson::{doc, Bson, Document};

impl MongoDbSink {
    pub(super) fn prepare_cdc_writes(
        docs: &[serde_json::Value],
        converted_limit: usize,
        expected_namespace: &str,
    ) -> Result<(Vec<CdcWrite>, u64), ConnectorError> {
        let mut writes = Vec::with_capacity(docs.len());
        let mut bytes = 0;

        for value in docs {
            let operation = cdc_operation(value, expected_namespace)?;
            writes.push(prepare_cdc_write(
                value,
                operation,
                expected_namespace,
                converted_limit,
                &mut bytes,
            )?);
        }

        Ok((writes, bytes))
    }

    pub(super) async fn execute_cdc_writes(
        &self,
        writes: Vec<CdcWrite>,
    ) -> Result<(), ConnectorError> {
        let collection = self
            .collection
            .as_ref()
            .ok_or_else(|| ConnectorError::Internal("collection not initialized".to_string()))?;
        let namespace = collection.namespace();
        let (models, counts) = cdc_bulk_models(&namespace, writes);
        self.execute_bulk_models(models, counts, "MongoDB CDC bulk_write")
            .await
    }
}

fn cdc_operation<'a>(
    value: &'a serde_json::Value,
    expected_namespace: &str,
) -> Result<&'a str, ConnectorError> {
    let text = |field: &str| value.get(field).and_then(serde_json::Value::as_str);
    let database = text("database").ok_or_else(|| {
        ConnectorError::ConfigurationError(
            "MongoDB CDC replay record requires a non-null string 'database'".into(),
        )
    })?;
    let operation = text("operation").ok_or_else(|| {
        ConnectorError::ConfigurationError(
            "MongoDB CDC replay record requires a non-null string 'operation'".into(),
        )
    })?;
    let same_namespace = expected_namespace
        .split_once('.')
        .is_some_and(|(db, coll)| db == database && text("collection") == Some(coll));
    if !same_namespace {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB CDC replay record from '{database}.{}' does not match the mapped source \
             namespace '{expected_namespace}'; set replay.source.namespace to mirror into a \
             different collection",
            text("collection").unwrap_or("<none>")
        )));
    }
    Ok(operation)
}

fn prepare_cdc_write(
    value: &serde_json::Value,
    operation: &str,
    expected_namespace: &str,
    converted_limit: usize,
    bytes: &mut u64,
) -> Result<CdcWrite, ConnectorError> {
    if operation == SNAPSHOT_OPERATION {
        return prepare_replacement(value, false, converted_limit, bytes);
    }
    match ChangeOperation::classify(operation) {
        ChangeOperation::Insert => prepare_replacement(value, true, converted_limit, bytes),
        ChangeOperation::Replace => prepare_replacement(value, false, converted_limit, bytes),
        ChangeOperation::Update => prepare_update(value, converted_limit, bytes),
        ChangeOperation::Delete => prepare_delete(value, converted_limit, bytes),
        ChangeOperation::Metadata => Ok(CdcWrite::Noop),
        ChangeOperation::Drop
        | ChangeOperation::Rename
        | ChangeOperation::DropDatabase
        | ChangeOperation::Invalidate => Err(ConnectorError::ConfigurationError(format!(
            "MongoDB CDC replay cannot apply lifecycle operation '{operation}' to fixed \
             destination '{expected_namespace}'; the target is left unchanged"
        ))),
        ChangeOperation::Unknown => Err(ConnectorError::ConfigurationError(format!(
            "MongoDB CDC replay does not support operation '{operation}'"
        ))),
    }
}

fn prepare_replacement(
    value: &serde_json::Value,
    insert: bool,
    converted_limit: usize,
    bytes: &mut u64,
) -> Result<CdcWrite, ConnectorError> {
    let key = parse_cdc_field(value, "document_key")?;
    let full_document = parse_cdc_field(value, "full_document")?;
    let key = json_to_bson_document(key.as_ref())?;
    let filter = validate_cdc_document_key(&key)?;
    let replacement = json_to_bson_document(full_document.as_ref())?;
    validate_cdc_replacement_key(&key, &replacement)?;
    account_cdc_key_and_document(
        bytes,
        &filter,
        &replacement,
        converted_limit,
        "MongoDB CDC replacement document",
    )?;
    if insert {
        Ok(CdcWrite::Insert {
            filter,
            replacement,
        })
    } else {
        Ok(CdcWrite::Replace {
            filter,
            replacement,
        })
    }
}

fn prepare_update(
    value: &serde_json::Value,
    converted_limit: usize,
    bytes: &mut u64,
) -> Result<CdcWrite, ConnectorError> {
    let key = parse_cdc_field(value, "document_key")?;
    let key = json_to_bson_document(key.as_ref())?;
    let filter = validate_cdc_document_key(&key)?;

    if value
        .get("full_document")
        .is_some_and(|document| !document.is_null())
    {
        let full_document = parse_cdc_field(value, "full_document")?;
        let replacement = json_to_bson_document(full_document.as_ref())?;
        validate_cdc_replacement_key(&key, &replacement)?;
        account_cdc_key_and_document(
            bytes,
            &filter,
            &replacement,
            converted_limit,
            "MongoDB CDC replacement document",
        )?;
        return Ok(CdcWrite::Replace {
            filter,
            replacement,
        });
    }

    let description = parse_cdc_field(value, "update_description")?;
    let update = build_update_document(description.as_ref())?;
    account_bson_document(bytes, &filter, converted_limit, "MongoDB CDC document key")?;
    if update.is_empty() {
        return Ok(CdcWrite::Noop);
    }
    account_bson_document(
        bytes,
        &update,
        converted_limit,
        "MongoDB CDC update document",
    )?;
    Ok(CdcWrite::Update { filter, update })
}

fn prepare_delete(
    value: &serde_json::Value,
    converted_limit: usize,
    bytes: &mut u64,
) -> Result<CdcWrite, ConnectorError> {
    let key = parse_cdc_field(value, "document_key")?;
    let key = json_to_bson_document(key.as_ref())?;
    let filter = validate_cdc_document_key(&key)?;
    account_bson_document(bytes, &filter, converted_limit, "MongoDB CDC document key")?;
    Ok(CdcWrite::Delete { filter })
}

fn account_cdc_key_and_document(
    bytes: &mut u64,
    filter: &Document,
    document: &Document,
    converted_limit: usize,
    document_context: &str,
) -> Result<(), ConnectorError> {
    account_bson_document(bytes, filter, converted_limit, "MongoDB CDC document key")?;
    account_bson_document(bytes, document, converted_limit, document_context)
}

fn build_update_document(description: &serde_json::Value) -> Result<Document, ConnectorError> {
    reject_disambiguated_paths(description)?;
    let mut update = Document::new();
    append_updated_fields(description, &mut update)?;
    append_removed_fields(description, &mut update)?;
    append_truncated_arrays(description, &mut update)?;
    Ok(update)
}

fn reject_disambiguated_paths(description: &serde_json::Value) -> Result<(), ConnectorError> {
    let Some(disambiguated) = description.get("disambiguatedPaths") else {
        return Ok(());
    };
    let paths = disambiguated.as_object().ok_or_else(|| {
        ConnectorError::ConfigurationError(
            "MongoDB CDC disambiguated_paths must be an object".into(),
        )
    })?;
    if paths.is_empty() {
        return Ok(());
    }
    Err(ConnectorError::ConfigurationError(
        "MongoDB CDC replay cannot safely apply ambiguous field paths; use a full-document update \
         mode"
            .into(),
    ))
}

fn append_updated_fields(
    description: &serde_json::Value,
    update: &mut Document,
) -> Result<(), ConnectorError> {
    if let Some(updated) = description.get("updatedFields") {
        update.insert("$set", Bson::Document(json_to_bson_document(updated)?));
    }
    Ok(())
}

fn append_removed_fields(
    description: &serde_json::Value,
    update: &mut Document,
) -> Result<(), ConnectorError> {
    let Some(removed) = description.get("removedFields") else {
        return Ok(());
    };
    let fields = removed.as_array().ok_or_else(|| {
        ConnectorError::ConfigurationError("MongoDB CDC removed_fields must be an array".into())
    })?;
    if fields.is_empty() {
        return Ok(());
    }
    let mut unset = Document::new();
    for field in fields {
        let field = field.as_str().ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "MongoDB CDC removed_fields entries must be strings".into(),
            )
        })?;
        unset.insert(field, "");
    }
    update.insert("$unset", unset);
    Ok(())
}

fn append_truncated_arrays(
    description: &serde_json::Value,
    update: &mut Document,
) -> Result<(), ConnectorError> {
    let Some(truncated) = description.get("truncatedArrays") else {
        return Ok(());
    };
    let arrays = truncated.as_array().ok_or_else(|| {
        ConnectorError::ConfigurationError("MongoDB CDC truncated_arrays must be an array".into())
    })?;
    if arrays.is_empty() {
        return Ok(());
    }
    let mut push = Document::new();
    for array in arrays {
        let field = array
            .get("field")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "MongoDB CDC truncated array requires 'field'".into(),
                )
            })?;
        let new_size = array
            .get("newSize")
            .map(json_to_bson)
            .transpose()?
            .and_then(|size| match size {
                Bson::Int32(size) => Some(i64::from(size)),
                Bson::Int64(size) => Some(size),
                _ => None,
            })
            .filter(|size| *size >= 0)
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "MongoDB CDC truncated array requires a non-negative integer 'newSize'".into(),
                )
            })?;
        push.insert(
            field,
            doc! { "$each": Bson::Array(Vec::new()), "$slice": new_size },
        );
    }
    update.insert("$push", push);
    Ok(())
}

/// Accepts CDC envelope fields encoded either as objects or as JSON strings from Utf8 columns.
fn parse_cdc_field<'a>(
    value: &'a serde_json::Value,
    field: &str,
) -> Result<std::borrow::Cow<'a, serde_json::Value>, ConnectorError> {
    let value = value.get(field).ok_or_else(|| {
        ConnectorError::ConfigurationError(format!("CDC event missing {field} field"))
    })?;
    match value {
        serde_json::Value::Object(_) => Ok(std::borrow::Cow::Borrowed(value)),
        serde_json::Value::String(encoded) => {
            let parsed = serde_json::from_str(encoded).map_err(|error| {
                ConnectorError::ConfigurationError(format!("parse {field} JSON: {error}"))
            })?;
            Ok(std::borrow::Cow::Owned(parsed))
        }
        _ => Err(ConnectorError::ConfigurationError(format!(
            "{field} must be a JSON object or JSON string, got {value}"
        ))),
    }
}
