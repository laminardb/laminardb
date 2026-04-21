//! NATS sink connector.
//!
//! This milestone validates and stores the configuration. Publish, ack
//! tracking, and dedup handling land with the data path.

use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;

use super::config::NatsSinkConfig;
use crate::config::ConnectorConfig;
use crate::connector::{DeliveryGuarantee, SinkConnector, SinkConnectorCapabilities, WriteResult};
use crate::error::ConnectorError;

/// NATS sink — core and `JetStream` modes behind a single type.
pub struct NatsSink {
    schema: SchemaRef,
    config: Option<NatsSinkConfig>,
}

impl NatsSink {
    /// Creates a new NATS sink with the given input schema.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            config: None,
        }
    }

    /// Parsed config — available after [`SinkConnector::open`].
    #[must_use]
    pub fn config(&self) -> Option<&NatsSinkConfig> {
        self.config.as_ref()
    }
}

#[async_trait]
impl SinkConnector for NatsSink {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.config = Some(NatsSinkConfig::from_config(config)?);
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        // Data path lands in the follow-up.
        Ok(WriteResult::new(0, 0))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn capabilities(&self) -> SinkConnectorCapabilities {
        let mut caps = SinkConnectorCapabilities::new(Duration::from_secs(5))
            .with_idempotent()
            .with_partitioned();
        if matches!(
            self.config.as_ref().map(|c| c.delivery_guarantee),
            Some(DeliveryGuarantee::ExactlyOnce)
        ) {
            caps = caps.with_exactly_once().with_two_phase_commit();
        }
        caps
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
