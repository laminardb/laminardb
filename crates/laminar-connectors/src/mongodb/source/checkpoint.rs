//! Checkpoint identity, canonical encoding, and fail-closed restore validation.

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::checkpoint::SourceCheckpoint;

use super::{
    ConnectorError, MongoDbSourceConfig, COLLECTION_UUID_METADATA, DEPLOYMENT_IDENTITY_METADATA,
    MAX_RESUME_TOKEN_BYTES, MONGODB_CHECKPOINT_CONNECTOR, MONGODB_CHECKPOINT_VERSION,
    RESUME_TOKEN_OFFSET, START_AFTER_TOKEN_OFFSET, STREAM_IDENTITY_METADATA,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MongoCheckpointPosition {
    ResumeAfter(String),
    StartAfter(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MongoDeploymentIdentity {
    ReplicaSet(String),
    ShardedCluster(String),
}

impl MongoDeploymentIdentity {
    pub(super) fn encode(&self) -> String {
        match self {
            Self::ReplicaSet(id) => format!("replica-set:{id}"),
            Self::ShardedCluster(id) => format!("sharded-cluster:{id}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParsedMongoCheckpoint {
    pub(super) position: MongoCheckpointPosition,
    pub(super) collection_uuid: Uuid,
    pub(super) deployment_identity: MongoDeploymentIdentity,
}

pub(super) fn mongodb_stream_identity(config: &MongoDbSourceConfig) -> String {
    let mut digest = Sha256::new();
    digest.update(b"laminardb-mongodb-change-stream-v4\0");
    let full_document_mode = match config.full_document_mode {
        super::super::config::FullDocumentMode::Delta => 0_u8,
        super::super::config::FullDocumentMode::RequirePostImage => 1,
    };
    digest.update([full_document_mode]);
    digest.update([1]); // showExpandedEvents is always enabled.
    let pipeline = super::super::config::canonical_pipeline_json(&config.pipeline);
    digest.update(
        u64::try_from(pipeline.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    digest.update(pipeline.as_bytes());
    format!("{:x}", digest.finalize())
}

pub(super) fn canonical_resume_token(token: &str) -> Result<String, ConnectorError> {
    if token.is_empty() || token.len() > MAX_RESUME_TOKEN_BYTES {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB CDC resume token size must be 1..={MAX_RESUME_TOKEN_BYTES} bytes"
        )));
    }
    let value: serde_json::Value = serde_json::from_str(token).map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "MongoDB CDC resume token is not valid JSON: {error}"
        ))
    })?;
    let serde_json::Value::Object(document) = &value else {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC resume token must be a JSON document".into(),
        ));
    };
    if document.is_empty() {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC resume token document must not be empty".into(),
        ));
    }
    let canonical = serde_json::to_string(&value).map_err(|error| {
        ConnectorError::Internal(format!("serialize MongoDB CDC resume token: {error}"))
    })?;
    if canonical != token {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC resume token is not in canonical JSON form".into(),
        ));
    }
    Ok(canonical)
}

pub(super) fn parse_collection_uuid(encoded: &str) -> Result<Uuid, ConnectorError> {
    let uuid = Uuid::parse_str(encoded).map_err(|error| {
        ConnectorError::ConfigurationError(format!("invalid MongoDB CDC collection UUID: {error}"))
    })?;
    if uuid.hyphenated().to_string() != encoded {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC collection UUID is not in canonical lowercase hyphenated form".into(),
        ));
    }
    Ok(uuid)
}

pub(super) fn parse_deployment_identity(
    encoded: &str,
) -> Result<MongoDeploymentIdentity, ConnectorError> {
    let (kind, id) = encoded.split_once(':').ok_or_else(|| {
        ConnectorError::ConfigurationError(
            "MongoDB CDC deployment identity must include its deployment type".into(),
        )
    })?;
    if id.contains(':') {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC deployment identity has too many fields".into(),
        ));
    }
    let object_id = mongodb::bson::oid::ObjectId::parse_str(id).map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "invalid MongoDB CDC deployment ObjectId: {error}"
        ))
    })?;
    if object_id.to_hex() != id {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC deployment ObjectId is not canonical lowercase hex".into(),
        ));
    }
    match kind {
        "replica-set" => Ok(MongoDeploymentIdentity::ReplicaSet(id.to_string())),
        "sharded-cluster" => Ok(MongoDeploymentIdentity::ShardedCluster(id.to_string())),
        _ => Err(ConnectorError::ConfigurationError(format!(
            "unknown MongoDB CDC deployment identity type '{kind}'"
        ))),
    }
}

pub(super) fn parse_mongodb_checkpoint(
    checkpoint: &SourceCheckpoint,
    config: &MongoDbSourceConfig,
) -> Result<ParsedMongoCheckpoint, ConnectorError> {
    let expected_stream_identity = mongodb_stream_identity(config);
    if checkpoint.get_metadata("connector") != Some(MONGODB_CHECKPOINT_CONNECTOR)
        || checkpoint.get_metadata("version") != Some(MONGODB_CHECKPOINT_VERSION)
        || checkpoint.get_metadata("database") != Some(config.database.as_str())
        || checkpoint.get_metadata("collection") != Some(config.collection.as_str())
        || checkpoint.get_metadata(STREAM_IDENTITY_METADATA)
            != Some(expected_stream_identity.as_str())
    {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC checkpoint identity or format does not match the configured source".into(),
        ));
    }
    let collection_uuid = checkpoint
        .get_metadata(COLLECTION_UUID_METADATA)
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "MongoDB CDC checkpoint is missing its collection UUID".into(),
            )
        })
        .and_then(parse_collection_uuid)?;
    let deployment_identity = checkpoint
        .get_metadata(DEPLOYMENT_IDENTITY_METADATA)
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "MongoDB CDC checkpoint is missing its deployment identity".into(),
            )
        })
        .and_then(parse_deployment_identity)?;
    if checkpoint.metadata().len() != 7 {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC checkpoint contains unknown metadata fields".into(),
        ));
    }
    if checkpoint.offsets().len() != 1 {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC checkpoint must contain exactly one resume token".into(),
        ));
    }
    let position = if let Some(token) = checkpoint.get_offset(RESUME_TOKEN_OFFSET) {
        canonical_resume_token(token).map(MongoCheckpointPosition::ResumeAfter)?
    } else if let Some(token) = checkpoint.get_offset(START_AFTER_TOKEN_OFFSET) {
        canonical_resume_token(token).map(MongoCheckpointPosition::StartAfter)?
    } else {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC checkpoint contains an unknown position key".into(),
        ));
    };
    Ok(ParsedMongoCheckpoint {
        position,
        collection_uuid,
        deployment_identity,
    })
}
