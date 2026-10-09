//! Checkpoint identity, canonical encoding, and fail-closed restore validation.

use mongodb::bson::Timestamp;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::checkpoint::SourceCheckpoint;

use super::super::config::{FullDocumentMode, SnapshotMode, SourceOutputMode};
use super::{
    ConnectorError, MongoDbSourceConfig, COLLECTION_UUID_METADATA, DEPLOYMENT_IDENTITY_METADATA,
    MAX_RESUME_TOKEN_BYTES, MONGODB_CHECKPOINT_CONNECTOR, MONGODB_CHECKPOINT_VERSION,
    STREAM_IDENTITY_METADATA,
};

pub(super) const RESUME_TOKEN_OFFSET: &str = "resume_token";
pub(super) const START_AFTER_TOKEN_OFFSET: &str = "start_after_token";
pub(super) const START_AT_OFFSET: &str = "start_at_operation_time";
pub(super) const SNAPSHOT_AT_OFFSET: &str = "snapshot_at";
pub(super) const SNAPSHOT_AFTER_KEY_OFFSET: &str = "snapshot_after_key";
pub(super) const SEQUENCE_OFFSET: &str = "sequence";
const MAX_SNAPSHOT_KEY_BYTES: usize = 16 * 1024;

/// Change-stream position from which the next unemitted event is reproduced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StreamPosition {
    ResumeAfter(String),
    /// Invalidation tokens must be restored with `startAfter`, never `resumeAfter`.
    StartAfter(String),
    /// Inclusive cluster time; used after a snapshot until the stream reports a token.
    StartAt(Timestamp),
}

/// Durable initial-snapshot progress. Every event at or after `at` is streamed after the scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SnapshotCut {
    pub(super) at: Timestamp,
    /// Canonical Extended JSON of the last emitted `_id`, or `None` before the first row.
    pub(super) after_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MongoCheckpointPosition {
    Stream(StreamPosition),
    Snapshot(SnapshotCut),
}

/// Emitted progress: the position plus the number of rows emitted before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EmittedPosition {
    pub(super) position: MongoCheckpointPosition,
    pub(super) next_sequence: u64,
}

impl EmittedPosition {
    pub(super) fn in_snapshot(&self) -> bool {
        matches!(self.position, MongoCheckpointPosition::Snapshot(_))
    }
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
    pub(super) emitted: EmittedPosition,
    pub(super) collection_uuid: Uuid,
    pub(super) deployment_identity: MongoDeploymentIdentity,
}

pub(super) fn mongodb_stream_identity(config: &MongoDbSourceConfig) -> String {
    let mut digest = Sha256::new();
    digest.update(b"laminardb-mongodb-change-stream-v5\0");
    digest.update([
        match config.full_document_mode {
            FullDocumentMode::Delta => 0_u8,
            FullDocumentMode::RequirePostImage => 1,
        },
        match config.output_mode {
            SourceOutputMode::History => 0,
            SourceOutputMode::Document => 1,
        },
        match config.snapshot_mode {
            SnapshotMode::Never => 0,
            SnapshotMode::Initial => 1,
        },
    ]);
    digest.update([1]); // showExpandedEvents is always enabled.
    let mut update_framed = |value: &str| {
        digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(value.as_bytes());
    };
    update_framed(&super::super::config::canonical_pipeline_json(
        &config.pipeline,
    ));
    for column in &config.objectid_columns {
        update_framed(column);
    }
    update_framed("\0json");
    update_framed(config.document_json_column.as_deref().unwrap_or(""));
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

pub(super) fn encode_timestamp(timestamp: Timestamp) -> String {
    format!("{}.{}", timestamp.time, timestamp.increment)
}

pub(super) fn parse_timestamp(encoded: &str) -> Result<Timestamp, ConnectorError> {
    let invalid = || {
        ConnectorError::ConfigurationError(format!(
            "MongoDB CDC cluster time '{encoded}' is not canonical '<seconds>.<increment>'"
        ))
    };
    let (time, increment) = encoded.split_once('.').ok_or_else(invalid)?;
    let parse = |part: &str| {
        let value = part.parse::<u32>().map_err(|_| invalid())?;
        if value.to_string() == part {
            Ok(value)
        } else {
            Err(invalid())
        }
    };
    Ok(Timestamp {
        time: parse(time)?,
        increment: parse(increment)?,
    })
}

fn parse_snapshot_key(encoded: &str) -> Result<String, ConnectorError> {
    if encoded.is_empty() || encoded.len() > MAX_SNAPSHOT_KEY_BYTES {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB CDC snapshot key size must be 1..={MAX_SNAPSHOT_KEY_BYTES} bytes"
        )));
    }
    let value: serde_json::Value = serde_json::from_str(encoded).map_err(|error| {
        ConnectorError::ConfigurationError(format!("MongoDB CDC snapshot key is invalid: {error}"))
    })?;
    let bson = mongodb::bson::Bson::try_from(value).map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "MongoDB CDC snapshot key is not Extended JSON: {error}"
        ))
    })?;
    // Text, not value, equality: document keys compare in field order.
    let canonical = bson.into_canonical_extjson().to_string();
    if canonical != encoded {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC snapshot key is not canonical Extended JSON".into(),
        ));
    }
    Ok(encoded.to_string())
}

pub(super) fn set_emitted_offsets(checkpoint: &mut SourceCheckpoint, emitted: &EmittedPosition) {
    match &emitted.position {
        MongoCheckpointPosition::Stream(StreamPosition::ResumeAfter(token)) => {
            checkpoint.set_offset(RESUME_TOKEN_OFFSET, token);
        }
        MongoCheckpointPosition::Stream(StreamPosition::StartAfter(token)) => {
            checkpoint.set_offset(START_AFTER_TOKEN_OFFSET, token);
        }
        MongoCheckpointPosition::Stream(StreamPosition::StartAt(at)) => {
            checkpoint.set_offset(START_AT_OFFSET, encode_timestamp(*at));
        }
        MongoCheckpointPosition::Snapshot(cut) => {
            checkpoint.set_offset(SNAPSHOT_AT_OFFSET, encode_timestamp(cut.at));
            if let Some(key) = &cut.after_key {
                checkpoint.set_offset(SNAPSHOT_AFTER_KEY_OFFSET, key);
            }
        }
    }
    checkpoint.set_offset(SEQUENCE_OFFSET, emitted.next_sequence.to_string());
}

fn parse_position(
    checkpoint: &SourceCheckpoint,
) -> Result<MongoCheckpointPosition, ConnectorError> {
    let offset = |key| checkpoint.get_offset(key);
    let position = match (
        offset(RESUME_TOKEN_OFFSET),
        offset(START_AFTER_TOKEN_OFFSET),
        offset(START_AT_OFFSET),
        offset(SNAPSHOT_AT_OFFSET),
        offset(SNAPSHOT_AFTER_KEY_OFFSET),
    ) {
        (Some(token), None, None, None, None) => MongoCheckpointPosition::Stream(
            StreamPosition::ResumeAfter(canonical_resume_token(token)?),
        ),
        (None, Some(token), None, None, None) => MongoCheckpointPosition::Stream(
            StreamPosition::StartAfter(canonical_resume_token(token)?),
        ),
        (None, None, Some(at), None, None) => {
            MongoCheckpointPosition::Stream(StreamPosition::StartAt(parse_timestamp(at)?))
        }
        (None, None, None, Some(at), after_key) => MongoCheckpointPosition::Snapshot(SnapshotCut {
            at: parse_timestamp(at)?,
            after_key: after_key.map(parse_snapshot_key).transpose()?,
        }),
        _ => {
            return Err(ConnectorError::ConfigurationError(
                "MongoDB CDC checkpoint must contain exactly one stream or snapshot position"
                    .into(),
            ));
        }
    };
    Ok(position)
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
    let position = parse_position(checkpoint)?;
    let next_sequence = checkpoint
        .get_offset(SEQUENCE_OFFSET)
        .and_then(|encoded| {
            encoded
                .parse::<u64>()
                .ok()
                .filter(|value| value.to_string() == encoded)
        })
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "MongoDB CDC checkpoint is missing its canonical row sequence".into(),
            )
        })?;
    let expected_offsets = match &position {
        MongoCheckpointPosition::Snapshot(SnapshotCut {
            after_key: Some(_), ..
        }) => 3,
        _ => 2,
    };
    if checkpoint.offsets().len() != expected_offsets {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC checkpoint contains unknown position fields".into(),
        ));
    }
    if matches!(position, MongoCheckpointPosition::Snapshot(_))
        && config.snapshot_mode != SnapshotMode::Initial
    {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC checkpoint holds snapshot progress for a source without \
             snapshot.mode=initial"
                .into(),
        ));
    }
    Ok(ParsedMongoCheckpoint {
        emitted: EmittedPosition {
            position,
            next_sequence,
        },
        collection_uuid,
        deployment_identity,
    })
}
