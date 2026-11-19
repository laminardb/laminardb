//! Source contract, lifecycle, polling, and shutdown.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    ConnectorTaskTracker, SourceBatch, SourceConnector, SourceConsistency, SourceContract,
    SourceInputMode, SourcePosition, SourceRowPositionCapability, SourceStart, SourceTopology,
};
use crate::error::ConnectorError;

use super::super::config::{SnapshotMode, SourceOutputMode};
use super::admission::ReaderLaunch;
use super::checkpoint::{decode_resume_token, encode_checkpoint};
use super::{
    parse_mongodb_checkpoint, reap_mongo_reader, DocumentProjection, EmittedPosition,
    MongoCheckpointPosition, MongoDbCdcSource, MongoDbSourceConfig, MongoDeploymentIdentity,
    MongoResumePosition, ParsedMongoCheckpoint, ReaderStart, StreamPosition,
    READER_SHUTDOWN_TIMEOUT,
};

fn parsed_config(
    source: &MongoDbSourceConfig,
    config: &ConnectorConfig,
) -> Result<MongoDbSourceConfig, ConnectorError> {
    if config.properties().is_empty() {
        let mut config = source.clone();
        config.normalize_pipeline()?;
        config.validate()?;
        Ok(config)
    } else {
        MongoDbSourceConfig::from_config(config)
    }
}

pub(super) fn declared_primary_key(config: &ConnectorConfig) -> Vec<String> {
    config
        .get("_primary_key_columns")
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(str::to_string)
        .collect()
}

/// Document-mode projection from the engine-supplied declared schema and primary key.
pub(super) fn document_projection(
    parsed: &MongoDbSourceConfig,
    config: &ConnectorConfig,
    schema: Option<SchemaRef>,
) -> Result<Option<DocumentProjection>, ConnectorError> {
    if parsed.output_mode != SourceOutputMode::Document {
        return Ok(None);
    }
    let schema = schema.ok_or_else(|| {
        ConnectorError::ConfigurationError(
            "MongoDB CDC output.mode=document requires declared columns and a PRIMARY KEY".into(),
        )
    })?;
    DocumentProjection::try_new(
        &schema,
        &declared_primary_key(config),
        &parsed.objectid_columns,
        parsed.document_json_column.as_deref(),
        parsed.change_time_column.as_deref(),
    )
    .map(Some)
}

/// The reader start, emitted position and admitted identity a stored cursor restores.
type Restored = (
    ReaderStart,
    Option<EmittedPosition>,
    Option<Uuid>,
    Option<MongoDeploymentIdentity>,
);

fn restore(
    checkpoint: &SourceCheckpoint,
    config: &MongoDbSourceConfig,
    origin: &str,
) -> Result<Restored, ConnectorError> {
    let parsed = parse_mongodb_checkpoint(checkpoint, config).map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "invalid MongoDB CDC checkpoint {origin}: {error}"
        ))
    })?;
    Ok((
        restored_start(&parsed)?,
        Some(parsed.emitted),
        Some(parsed.collection_uuid),
        Some(parsed.deployment_identity),
    ))
}

pub(super) fn restored_start(
    checkpoint: &ParsedMongoCheckpoint,
) -> Result<ReaderStart, ConnectorError> {
    let token = decode_resume_token;
    Ok(match &checkpoint.emitted.position {
        MongoCheckpointPosition::Stream(StreamPosition::ResumeAfter(encoded)) => {
            ReaderStart::Stream(MongoResumePosition::ResumeAfter(token(encoded)?))
        }
        MongoCheckpointPosition::Stream(StreamPosition::StartAfter(encoded)) => {
            ReaderStart::Stream(MongoResumePosition::StartAfter(token(encoded)?))
        }
        MongoCheckpointPosition::Stream(StreamPosition::StartAt(at)) => {
            ReaderStart::Stream(MongoResumePosition::StartAt(*at))
        }
        MongoCheckpointPosition::Snapshot(cut) => ReaderStart::Snapshot(cut.clone()),
    })
}

#[async_trait]
impl SourceConnector for MongoDbCdcSource {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn recovery_identity_options(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Option<BTreeMap<String, String>>, ConnectorError> {
        let mut parsed = parsed_config(&self.config, config)?;
        parsed.normalize_pipeline()?;
        let pipeline = super::super::config::canonical_pipeline_json(&parsed.pipeline);
        let wire_protocol = match parsed.output_mode {
            SourceOutputMode::History => "change-stream-history-v1",
            SourceOutputMode::Document => "change-stream-document-v1",
        };
        let mut identity = BTreeMap::from([
            ("collection".into(), parsed.collection),
            ("database".into(), parsed.database),
            (
                "full.document.mode".into(),
                parsed.full_document_mode.to_string(),
            ),
            ("output.mode".into(), parsed.output_mode.to_string()),
            ("snapshot.mode".into(), parsed.snapshot_mode.to_string()),
            ("objectid.columns".into(), parsed.objectid_columns.join(",")),
            (
                "document.json.column".into(),
                parsed.document_json_column.unwrap_or_default(),
            ),
            ("pipeline".into(), pipeline),
            ("wire.protocol".into(), wire_protocol.into()),
        ]);
        // Present only when set, so sources without it keep their stored identity.
        if let Some(column) = parsed.change_time_column {
            identity.insert("change.time.column".into(), column);
        }
        Ok(Some(identity))
    }

    async fn resolve_schema(
        &mut self,
        config: &ConnectorConfig,
        explicit: Option<SchemaRef>,
    ) -> Result<crate::schema::resolution::SchemaBinding, ConnectorError> {
        super::schema_resolution::resolve(config, explicit).await
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        // A cluster placement closes the source when another node takes it over, and starts it
        // again if it comes back.
        if !matches!(self.state, ConnectorState::Created | ConnectorState::Closed) {
            return Err(ConnectorError::InvalidState {
                expected: "a created or closed source".into(),
                actual: self.state.to_string(),
            });
        }
        let (config, position, _) = request.into_parts();
        let parsed = parsed_config(&self.config, &config)?;
        if matches!(position, SourcePosition::Initialized { .. }) {
            super::sealed_start::require(&parsed)?;
        }
        let projection = document_projection(&parsed, &config, config.arrow_schema())?;
        let (start, restored, mut expected_collection_uuid, mut expected_deployment_identity) =
            match position {
                SourcePosition::Initial => (
                    match parsed.snapshot_mode {
                        SnapshotMode::Never => ReaderStart::FreshChanges,
                        SnapshotMode::Initial => ReaderStart::FreshSnapshot,
                    },
                    None,
                    None,
                    None,
                ),
                SourcePosition::Initialized { checkpoint } => {
                    restore(&checkpoint, &parsed, "of the sealed start")?
                }
                SourcePosition::Resume {
                    attempt,
                    checkpoint,
                } => restore(&checkpoint, &parsed, &format!("{attempt:?}"))?,
            };

        if let Some((collection, deployment)) =
            super::schema_resolution::committed_identity(config.schema_binding())?
        {
            if expected_collection_uuid.is_some_and(|expected| expected != collection)
                || expected_deployment_identity
                    .as_ref()
                    .is_some_and(|expected| expected != &deployment)
            {
                return Err(ConnectorError::SchemaMismatch(
                    "MongoDB checkpoint and committed collection identity disagree".into(),
                ));
            }
            expected_collection_uuid = Some(collection);
            expected_deployment_identity = Some(deployment);
        }
        self.start_change_stream_reader(ReaderLaunch {
            config: parsed,
            start,
            restored,
            expected_collection_uuid,
            expected_deployment_identity,
            projection,
        })
        .await?;

        self.state = ConnectorState::Running;
        tracing::info!(
            database = %self.config.database,
            collection = %self.config.collection,
            output_mode = %self.config.output_mode,
            snapshot_mode = %self.config.snapshot_mode,
            full_document_mode = ?self.config.full_document_mode,
            "MongoDB CDC source opened"
        );

        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        self.drain_channel(max_records.saturating_sub(self.event_buffer.len()));
        if let Some(batch) = self.drain_to_batch(max_records)? {
            return Ok(Some(batch));
        }
        self.check_reader_error()?;
        Ok(None)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        // A configured namespace is not a physical replay identity until admission has read the
        // server-assigned collection UUID and deployment identity.
        let (Some(collection_uuid), Some(deployment_identity), Some(emitted)) = (
            self.collection_uuid,
            self.deployment_identity.as_ref(),
            self.emitted.as_ref(),
        ) else {
            return SourceCheckpoint::new();
        };
        encode_checkpoint(&self.config, collection_uuid, deployment_identity, emitted)
    }

    async fn notify_epoch_committed(
        &mut self,
        _epoch: u64,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        let Some((at, _)) = self.snapshot_committed.as_ref() else {
            return Ok(());
        };
        if checkpoint.offsets().is_empty() {
            return Ok(());
        }
        let committed = parse_mongodb_checkpoint(checkpoint, &self.config)?;
        if matches!(&committed.emitted.position, MongoCheckpointPosition::Snapshot(cut) if cut.at == *at)
        {
            if let Some((_, committed_tx)) = self.snapshot_committed.take() {
                committed_tx.send_replace(true);
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        let mut reader_join_error = None;
        if let Some(tx) = self.reader_shutdown.as_ref() {
            tx.send_replace(true);
        }
        let mut detach_reader = false;
        if let Some(handle) = self.reader_handle.as_mut() {
            match tokio::time::timeout(READER_SHUTDOWN_TIMEOUT, &mut *handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Err(error)) => reader_join_error = Some(error.to_string()),
                Err(_) => {
                    tracing::warn!(
                        "MongoDB CDC reader exceeded its close deadline; its tracked reaper retains shutdown ownership"
                    );
                    detach_reader = true;
                }
            }
        }
        if detach_reader {
            if let Some(handle) = self.reader_handle.take() {
                reap_mongo_reader(handle, &self.task_owner);
            }
        } else {
            self.reader_handle = None;
        }
        self.reader_shutdown = None;
        self.event_rx = None;
        self.reader_error = None;
        self.snapshot_committed = None;

        self.event_buffer.clear();
        self.state = ConnectorState::Closed;
        tracing::info!("MongoDB CDC source closed");
        if let Some(error) = reader_join_error {
            return Err(ConnectorError::ReadError(format!(
                "MongoDB CDC reader task failed during close: {error}"
            )));
        }
        Ok(())
    }

    fn supports_initialized_start(&self) -> bool {
        true
    }

    async fn resolve_initial_position(
        &mut self,
        config: &ConnectorConfig,
    ) -> Result<SourceCheckpoint, ConnectorError> {
        super::sealed_start::inspect(&parsed_config(&self.config, config)?, None).await
    }

    async fn validate_initial_position(
        &mut self,
        config: &ConnectorConfig,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        super::sealed_start::inspect(&parsed_config(&self.config, config)?, Some(checkpoint))
            .await
            .map(|_| ())
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        let parsed = parsed_config(&self.config, config)?;
        // The initial copy starts only after its cut is durably committed.
        let consistency = match parsed.snapshot_mode {
            SnapshotMode::Never => SourceConsistency::Replayable,
            SnapshotMode::Initial => SourceConsistency::CommitCoupled,
        };
        let contract = match parsed.output_mode {
            SourceOutputMode::History => SourceContract::new(
                consistency,
                SourceTopology::Singleton,
                SourceInputMode::AppendOnly,
            ),
            SourceOutputMode::Document => SourceContract::new(
                consistency,
                SourceTopology::Singleton,
                SourceInputMode::KeyedUpsert,
            )
            .with_row_positions(SourceRowPositionCapability::OrderedDeterministic),
        };
        // Any one node may follow the change stream: it resumes after the committed token, the
        // whole cursor in either mode (document updates carry their post-images). The initial
        // copy is not qualified for cluster sources.
        Ok(match parsed.snapshot_mode {
            SnapshotMode::Never => contract.with_cluster_singleton_placement(),
            SnapshotMode::Initial => contract,
        })
    }
}
