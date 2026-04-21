//! NATS source connector.
//!
//! This milestone validates and stores the configuration. The real fetch
//! loop (pull-consumer messages → Arrow batches) lands with the data path.

use arrow_schema::SchemaRef;
use async_trait::async_trait;

use super::config::NatsSourceConfig;
use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{SourceBatch, SourceConnector};
use crate::error::ConnectorError;

/// NATS source — core and `JetStream` modes behind a single type.
pub struct NatsSource {
    schema: SchemaRef,
    config: Option<NatsSourceConfig>,
}

impl NatsSource {
    /// Creates a new NATS source with the given schema.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            config: None,
        }
    }

    /// Parsed config — available after [`SourceConnector::open`].
    #[must_use]
    pub fn config(&self) -> Option<&NatsSourceConfig> {
        self.config.as_ref()
    }
}

#[async_trait]
impl SourceConnector for NatsSource {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.config = Some(NatsSourceConfig::from_config(config)?);
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        Ok(None)
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::default()
    }

    async fn restore(&mut self, _checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
