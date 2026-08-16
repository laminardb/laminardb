//! Source contract, lifecycle, polling, and shutdown.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tokio::sync::Notify;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    ConnectorTaskTracker, SourceBatch, SourceConnector, SourceContract, SourcePosition, SourceStart,
};
use crate::error::ConnectorError;

use super::{
    mongodb_stream_identity, parse_mongodb_checkpoint, reap_mongo_reader, MongoCheckpointPosition,
    MongoDbCdcSource, MongoDbSourceConfig, MongoResumePosition, ParsedMongoCheckpoint,
    COLLECTION_UUID_METADATA, DEPLOYMENT_IDENTITY_METADATA, MONGODB_CHECKPOINT_CONNECTOR,
    MONGODB_CHECKPOINT_VERSION, READER_SHUTDOWN_TIMEOUT, RESUME_TOKEN_OFFSET,
    START_AFTER_TOKEN_OFFSET, STREAM_IDENTITY_METADATA,
};

#[async_trait]
impl SourceConnector for MongoDbCdcSource {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn recovery_identity_options(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Option<BTreeMap<String, String>>, ConnectorError> {
        let mut parsed = if config.properties().is_empty() {
            self.config.clone()
        } else {
            MongoDbSourceConfig::from_config(config)?
        };
        parsed.normalize_pipeline()?;
        parsed.validate()?;
        let pipeline = super::super::config::canonical_pipeline_json(&parsed.pipeline);

        Ok(Some(BTreeMap::from([
            ("collection".into(), parsed.collection),
            ("database".into(), parsed.database),
            (
                "full.document.mode".into(),
                parsed.full_document_mode.to_string(),
            ),
            ("pipeline".into(), pipeline),
            ("wire.protocol".into(), "change-stream-expanded-v1".into()),
        ])))
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        if self.state != ConnectorState::Created {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Created.to_string(),
                actual: self.state.to_string(),
            });
        }
        let (config, position, _) = request.into_parts();
        let parsed_config = if config.properties().is_empty() {
            let mut config = self.config.clone();
            config.normalize_pipeline()?;
            config.validate()?;
            config
        } else {
            MongoDbSourceConfig::from_config(&config)?
        };
        let (
            checkpoint_resume_token,
            checkpoint_requires_start_after,
            initial_resume_position,
            expected_collection_uuid,
            expected_deployment_identity,
        ) = match position {
            SourcePosition::Initial => (None, false, None, None, None),
            SourcePosition::Resume {
                attempt,
                checkpoint,
            } => {
                let ParsedMongoCheckpoint {
                    position,
                    collection_uuid,
                    deployment_identity,
                } = parse_mongodb_checkpoint(&checkpoint, &parsed_config).map_err(|error| {
                    ConnectorError::ConfigurationError(format!(
                        "invalid MongoDB CDC checkpoint {attempt:?}: {error}"
                    ))
                })?;
                match position {
                    MongoCheckpointPosition::ResumeAfter(token) => {
                        let driver_token = serde_json::from_str(&token).map_err(|error| {
                            ConnectorError::ConfigurationError(format!(
                                "invalid MongoDB CDC resume token in checkpoint {attempt:?}: \
                                 {error}"
                            ))
                        })?;
                        (
                            Some(token),
                            false,
                            Some(MongoResumePosition::ResumeAfter(driver_token)),
                            Some(collection_uuid),
                            Some(deployment_identity),
                        )
                    }
                    MongoCheckpointPosition::StartAfter(token) => {
                        let driver_token = serde_json::from_str(&token).map_err(|error| {
                            ConnectorError::ConfigurationError(format!(
                                "invalid MongoDB CDC start-after token in checkpoint {attempt:?}: \
                                 {error}"
                            ))
                        })?;
                        (
                            Some(token),
                            true,
                            Some(MongoResumePosition::StartAfter(driver_token)),
                            Some(collection_uuid),
                            Some(deployment_identity),
                        )
                    }
                }
            }
        };

        self.start_change_stream_reader(
            parsed_config,
            checkpoint_resume_token,
            checkpoint_requires_start_after,
            initial_resume_position,
            expected_collection_uuid,
            expected_deployment_identity,
        )
        .await?;

        self.state = ConnectorState::Running;
        tracing::info!(
            database = %self.config.database,
            collection = %self.config.collection,
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
        let mut checkpoint = SourceCheckpoint::new();
        let Some(collection_uuid) = self.collection_uuid else {
            // A configured namespace is not a physical replay identity until admission has read
            // the server-assigned collection UUID.
            return checkpoint;
        };
        let Some(deployment_identity) = self.deployment_identity.as_ref() else {
            return checkpoint;
        };
        if let Some(token) = self.checkpoint_resume_token.as_ref() {
            checkpoint.set_offset(
                if self.checkpoint_requires_start_after {
                    START_AFTER_TOKEN_OFFSET
                } else {
                    RESUME_TOKEN_OFFSET
                },
                token,
            );
        } else {
            // Before a fresh source has opened, it has no lossless replay position.
            return checkpoint;
        }
        checkpoint.set_metadata("connector", MONGODB_CHECKPOINT_CONNECTOR);
        checkpoint.set_metadata("version", MONGODB_CHECKPOINT_VERSION);
        checkpoint.set_metadata("database", &self.config.database);
        checkpoint.set_metadata("collection", &self.config.collection);
        checkpoint.set_metadata(
            COLLECTION_UUID_METADATA,
            collection_uuid.hyphenated().to_string(),
        );
        checkpoint.set_metadata(DEPLOYMENT_IDENTITY_METADATA, deployment_identity.encode());
        checkpoint.set_metadata(
            STREAM_IDENTITY_METADATA,
            mongodb_stream_identity(&self.config),
        );
        checkpoint
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        #[cfg(feature = "mongodb-cdc")]
        let mut reader_join_error = None;
        #[cfg(feature = "mongodb-cdc")]
        {
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
                let handle = self
                    .reader_handle
                    .take()
                    .expect("reader handle was present while awaiting it");
                reap_mongo_reader(handle, &self.task_owner);
            } else {
                self.reader_handle = None;
            }
            self.reader_shutdown = None;
            self.event_rx = None;
            self.reader_error = None;
        }

        self.event_buffer.clear();
        self.state = ConnectorState::Closed;
        tracing::info!("MongoDB CDC source closed");
        #[cfg(feature = "mongodb-cdc")]
        if let Some(error) = reader_join_error {
            return Err(ConnectorError::ReadError(format!(
                "MongoDB CDC reader task failed during close: {error}"
            )));
        }
        Ok(())
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        if config.properties().is_empty() {
            self.config.validate()?;
        } else {
            MongoDbSourceConfig::from_config(config)?;
        }
        Err(ConnectorError::ConfigurationError(
            "MongoDB CDC emits a raw JSON change envelope; canonical primary-keyed row/delete records are required"
                .into(),
        ))
    }
}
