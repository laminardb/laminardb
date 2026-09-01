use crate::connector::CoordinatedCommitCursor;
use crate::error::ConnectorError;

#[derive(Debug, Clone)]
pub(super) struct CursorRecord {
    pub(super) cursor: CoordinatedCommitCursor,
    pub(super) batch_fingerprint: String,
    pub(super) file_set_fingerprint: String,
    pub(super) commit_uuid: String,
}

pub(super) struct CursorPropertyKeys {
    pub(super) checkpoint: String,
    pub(super) fence: String,
    pub(super) batch_fingerprint: String,
    pub(super) file_set: String,
    pub(super) commit_uuid: String,
}

pub(super) fn cursor_property_keys(external_key: &str) -> CursorPropertyKeys {
    let prefix = format!("laminardb.commit.{external_key}");
    CursorPropertyKeys {
        checkpoint: format!("{prefix}.checkpoint"),
        fence: format!("{prefix}.fence"),
        batch_fingerprint: format!("{prefix}.batch-fingerprint"),
        file_set: format!("{prefix}.file-set"),
        commit_uuid: format!("{prefix}.uuid"),
    }
}

pub(super) fn cursor_record(
    table: &iceberg::table::Table,
    external_key: &str,
) -> Result<Option<CursorRecord>, ConnectorError> {
    let keys = cursor_property_keys(external_key);
    let properties = table.metadata().properties();
    let values = [
        properties.get(&keys.checkpoint),
        properties.get(&keys.fence),
        properties.get(&keys.batch_fingerprint),
        properties.get(&keys.file_set),
        properties.get(&keys.commit_uuid),
    ];
    if values.iter().all(Option::is_none) {
        return Ok(None);
    }
    let [Some(checkpoint), Some(fence), Some(batch_fingerprint), Some(file_set), Some(commit_uuid)] =
        values
    else {
        return Err(ConnectorError::TransactionError(
            "Iceberg coordinated cursor table properties are incomplete".into(),
        ));
    };
    let checkpoint_id = checkpoint.parse().map_err(|_| {
        ConnectorError::TransactionError("Iceberg cursor checkpoint is not a u64".into())
    })?;
    let fencing_token = fence.parse().map_err(|_| {
        ConnectorError::TransactionError("Iceberg cursor fencing token is not a u64".into())
    })?;
    if checkpoint_id == 0 || fencing_token == 0 {
        return Err(ConnectorError::TransactionError(
            "Iceberg coordinated cursor contains zero checkpoint or fencing authority".into(),
        ));
    }
    validate_sha256(batch_fingerprint, "batch fingerprint")?;
    validate_sha256(file_set, "file-set fingerprint")?;
    let parsed_uuid = uuid::Uuid::parse_str(commit_uuid).map_err(|_| {
        ConnectorError::TransactionError("Iceberg cursor commit UUID is malformed".into())
    })?;
    if parsed_uuid.to_string() != *commit_uuid {
        return Err(ConnectorError::TransactionError(
            "Iceberg cursor commit UUID is not canonical".into(),
        ));
    }
    Ok(Some(CursorRecord {
        cursor: CoordinatedCommitCursor {
            checkpoint_id,
            fencing_token,
        },
        batch_fingerprint: batch_fingerprint.clone(),
        file_set_fingerprint: file_set.clone(),
        commit_uuid: commit_uuid.clone(),
    }))
}

fn validate_sha256(value: &str, label: &str) -> Result<(), ConnectorError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(ConnectorError::TransactionError(format!(
            "Iceberg cursor {label} is not a canonical SHA-256 digest"
        )))
    }
}
