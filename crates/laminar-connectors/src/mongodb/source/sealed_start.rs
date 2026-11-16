//! Sealed start: the cursor a source added to a running cluster starts from. It is resolved once
//! when the topology change is sealed and validated again before the source is installed.

use crate::checkpoint::SourceCheckpoint;

use super::super::config::{SnapshotMode, SourceOutputMode};
use super::admission::source_database;
use super::checkpoint::encode_checkpoint;
use super::lifecycle::restored_start;
use super::reader::{fresh_stream_anchor, open_bootstrap_cursor, verify_mongodb_admission};
use super::{
    observe_mongodb_admission, parse_mongodb_checkpoint, ConnectorError, EmittedPosition,
    MongoCheckpointPosition, MongoDbSourceConfig, ParsedMongoCheckpoint, ReaderStart,
    StreamPosition, READER_STARTUP_TIMEOUT,
};

/// Only the change stream starts sealed: an initial copy has no boundary to seal.
pub(super) fn require(config: &MongoDbSourceConfig) -> Result<(), ConnectorError> {
    if config.output_mode == SourceOutputMode::History
        && config.snapshot_mode == SnapshotMode::Never
    {
        return Ok(());
    }
    Err(ConnectorError::ConfigurationError(
        "a MongoDB CDC source starts sealed only with output.mode=history and snapshot.mode=never"
            .into(),
    ))
}

/// Resolve a new sealed start, or validate `sealed`. Either way the collection and deployment
/// identity are bound and an empty change stream is opened at the position, so a recreated
/// collection or a token the oplog no longer holds fails here. Nothing is read, and no cursor
/// stays open.
pub(super) async fn inspect(
    config: &MongoDbSourceConfig,
    sealed: Option<&SourceCheckpoint>,
) -> Result<SourceCheckpoint, ConnectorError> {
    require(config)?;
    let sealed = sealed
        .map(|checkpoint| parse_mongodb_checkpoint(checkpoint, config))
        .transpose()?;
    tokio::time::timeout(READER_STARTUP_TIMEOUT, probe(config, sealed))
        .await
        .map_err(|_| {
            ConnectorError::ConnectionFailed(format!(
                "MongoDB CDC sealed start inspection exceeded {READER_STARTUP_TIMEOUT:?}"
            ))
        })?
}

async fn probe(
    config: &MongoDbSourceConfig,
    sealed: Option<ParsedMongoCheckpoint>,
) -> Result<SourceCheckpoint, ConnectorError> {
    let db = source_database(&config.connection_uri, &config.database).await?;
    let observation = observe_mongodb_admission(&db, &config.database, &config.collection).await?;
    let (collection_uuid, deployment) = match &sealed {
        Some(sealed) => (sealed.collection_uuid, sealed.deployment_identity.clone()),
        None => (
            observation.collection.collection_uuid,
            observation.deployment_identity.clone(),
        ),
    };
    verify_mongodb_admission(config, &deployment, collection_uuid, &observation)?;
    let Some(sealed) = sealed else {
        let cursor = open_bootstrap_cursor(&db, config, None).await?;
        let (_, token) = fresh_stream_anchor(&cursor)?;
        let emitted = EmittedPosition {
            position: MongoCheckpointPosition::Stream(StreamPosition::ResumeAfter(token)),
            next_sequence: 0,
        };
        return Ok(encode_checkpoint(
            config,
            collection_uuid,
            &deployment,
            &emitted,
        ));
    };
    let ReaderStart::Stream(position) = restored_start(&sealed)? else {
        return Err(ConnectorError::Internal(
            "a sealed MongoDB CDC start holds snapshot progress".into(),
        ));
    };
    open_bootstrap_cursor(&db, config, Some(&position)).await?;
    Ok(encode_checkpoint(
        config,
        collection_uuid,
        &deployment,
        &sealed.emitted,
    ))
}
