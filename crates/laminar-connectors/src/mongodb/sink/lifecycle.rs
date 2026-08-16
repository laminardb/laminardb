//! Sink admission, buffering lifecycle, flushing, and shutdown.

use super::{
    async_trait, info, Arc, ConnectorConfig, ConnectorError, ConnectorState, ConnectorTaskTracker,
    Duration, MongoDbSink, RecordBatch, SchemaRef, SinkConnector, SinkConsistency, SinkContract,
    SinkInputMode, SinkTopology, WriteMode, WriteResult, MAX_BUFFERED_RETAINED_BYTES,
};

#[async_trait]
impl SinkConnector for MongoDbSink {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let (cfg, schema, _) = if config.properties().is_empty() {
            (
                self.config.clone(),
                Arc::clone(&self.schema),
                self.write_timeout,
            )
        } else {
            Self::decode_connector_config(config)?
        };
        cfg.validate()?;
        Self::validate_schema(&schema, &cfg)?;
        let (topology, input_mode) = match cfg.write_mode {
            WriteMode::Insert => (SinkTopology::MultiWriter, SinkInputMode::AppendOnly),
            WriteMode::Upsert { .. } | WriteMode::CdcReplay => {
                (SinkTopology::Singleton, SinkInputMode::FullChangelog)
            }
        };
        Ok(SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            topology,
            input_mode,
        ))
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        if self.state != ConnectorState::Created {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Created.to_string(),
                actual: self.state.to_string(),
            });
        }
        if config.properties().is_empty() {
            self.config.validate()?;
            Self::validate_schema(&self.schema, &self.config)?;
        } else {
            self.apply_connector_config(config)?;
        }
        self.connect().await?;

        self.state = ConnectorState::Running;
        info!(
            database = %self.config.database,
            collection = %self.config.collection,
            write_mode = ?self.config.write_mode,
            "MongoDB sink opened"
        );

        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        self.write_batch_with_retained_limit(batch, MAX_BUFFERED_RETAINED_BYTES)
            .await
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        self.write_timeout
    }

    fn flush_interval(&self) -> Duration {
        self.config.flush_interval()
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Running.to_string(),
                actual: self.state.to_string(),
            });
        }
        self.flush_inner().await.map(|_| ())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if self.state == ConnectorState::Closed {
            return Ok(());
        }

        let flush_result = if self.state == ConnectorState::Running && !self.buffer.is_empty() {
            self.flush_inner().await.map(|_| ())
        } else {
            drop(self.take_buffer());
            Ok(())
        };
        self.collection = None;
        self.client = None;
        self.state = ConnectorState::Closed;
        info!("MongoDB sink closed");
        flush_result
    }
}
