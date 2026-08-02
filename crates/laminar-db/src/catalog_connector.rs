//! Bridge connector that routes `db.insert()` data through the pipeline.
//!
//! When a source has no external connector (e.g., created without a `FROM` clause),
//! this connector wraps the catalog's SPSC subscription so data inserted via
//! `db.insert()` flows through the standard source task → coordinator → execute
//! cycle. Without this bridge, connector-less sources are orphaned in pipeline mode.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use tokio::sync::Notify;

use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceInputMode,
    SourcePosition, SourceStart, SourceTopology,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::streaming;

use crate::catalog::ArrowRecord;

/// Bridges a catalog SPSC subscription into the pipeline alongside external connectors.
pub(crate) struct CatalogSourceConnector {
    subscription: streaming::Subscription<ArrowRecord>,
    schema: SchemaRef,
    data_notify: Arc<Notify>,
}

impl CatalogSourceConnector {
    pub fn new(
        subscription: streaming::Subscription<ArrowRecord>,
        schema: SchemaRef,
        data_notify: Arc<Notify>,
    ) -> Self {
        Self {
            subscription,
            schema,
            data_notify,
        }
    }
}

#[async_trait]
impl SourceConnector for CatalogSourceConnector {
    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        match request.into_parts().1 {
            SourcePosition::Initial => Ok(()),
            SourcePosition::Resume { attempt, .. } => {
                Err(ConnectorError::ConfigurationError(format!(
                    "catalog bridge is ephemeral and cannot resume checkpoint epoch={} id={}",
                    attempt.epoch, attempt.checkpoint_id
                )))
            }
        }
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows = 0;

        for _ in 0..max_records {
            match self.subscription.poll() {
                Some(batch) => {
                    total_rows += batch.num_rows();
                    batches.push(batch);
                    if total_rows >= max_records {
                        break;
                    }
                }
                None => break,
            }
        }

        if batches.is_empty() {
            return Ok(None);
        }

        // Skip concat when there's only one batch (common case; saves a memcpy).
        let records = if batches.len() == 1 {
            batches.into_iter().next().expect("len==1 checked above")
        } else {
            arrow::compute::concat_batches(&self.schema, &batches)
                .map_err(|e| ConnectorError::ReadError(format!("Failed to concat batches: {e}")))?
        };

        Ok(Some(SourceBatch::new(records)))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        // `db.insert()` is process-local ingress backed by an in-memory subscription. A row
        // count cannot reproduce accepted events after restart, so exposing it as a recovery
        // cursor would contradict the connector's Ephemeral contract. Empty checkpoints still
        // let this source participate in barrier alignment without entering durable handoff.
        SourceCheckpoint::new()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_notify))
    }

    fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Ephemeral,
            SourceTopology::NodeLocalIngress,
            SourceInputMode::AppendOnly,
        ))
    }
}
