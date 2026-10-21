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

use crate::catalog::{ArrowRecord, SourceEntry};

/// Bridges a catalog SPSC subscription into the pipeline alongside external connectors.
pub(crate) struct CatalogSourceConnector {
    subscription: streaming::Subscription<ArrowRecord>,
    schema: SchemaRef,
    data_notify: Arc<Notify>,
    /// Catalog entry backing this bridge. Used to bind the managed admission cut this
    /// bridge has already forwarded when a checkpoint barrier is established at this source.
    source: Arc<SourceEntry>,
}

impl CatalogSourceConnector {
    pub fn new(
        subscription: streaming::Subscription<ArrowRecord>,
        schema: SchemaRef,
        data_notify: Arc<Notify>,
        source: Arc<SourceEntry>,
    ) -> Self {
        Self {
            subscription,
            schema,
            data_notify,
            source,
        }
    }
}

#[async_trait]
impl SourceConnector for CatalogSourceConnector {
    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        match request.into_parts().1 {
            SourcePosition::Initial => Ok(()),
            SourcePosition::Initialized { .. } => Err(ConnectorError::ConfigurationError(
                "catalog bridge has no sealed topology startup contract".into(),
            )),
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

        // Record exactly the admitted batches this poll has forwarded. A managed source's
        // committed cut is therefore bound to what the pipeline has actually seen, never to a
        // batch still queued in the subscription behind the barrier.
        self.source.record_forwarded_input(batches.len() as u64);

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
        let mut checkpoint = SourceCheckpoint::new();
        // For a declared managed push source, bind the native instance and the cut of the batches
        // this bridge has polled and forwarded, captured here at the moment the source establishes
        // its barrier. A batch that is admitted but still queued in the subscription stays outside
        // the committed cut, so a committed checkpoint never reports coverage for a batch it
        // excludes. This metadata is not a recovery cursor.
        if let Some((source_instance, ordered_input_offset)) = self.source.managed_admission_cut() {
            checkpoint.set_metadata(
                crate::source_admission::SOURCE_INSTANCE_METADATA_KEY,
                source_instance.as_str(),
            );
            checkpoint.set_metadata(
                crate::source_admission::ORDERED_INPUT_OFFSET_METADATA_KEY,
                ordered_input_offset.value().to_string(),
            );
        }
        checkpoint
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SourceCatalog;
    use crate::source_admission::{
        ORDERED_INPUT_OFFSET_METADATA_KEY, SOURCE_INSTANCE_METADATA_KEY,
    };
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::streaming::BackpressureStrategy;

    fn payload_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )]))
    }

    fn payload_batch(value: &str, schema: &SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(StringArray::from(vec![value]))],
        )
        .unwrap()
    }

    async fn poll_until_some(
        bridge: &mut CatalogSourceConnector,
        max_records: usize,
    ) -> SourceBatch {
        for _ in 0..10_000 {
            if let Some(batch) = bridge.poll_batch(max_records).await.unwrap() {
                return batch;
            }
            tokio::task::yield_now().await;
        }
        panic!("subscription never delivered admitted batches");
    }

    #[tokio::test]
    async fn managed_cut_covers_only_batches_forwarded_before_the_barrier() {
        let schema = payload_schema();
        let catalog = SourceCatalog::new(16, BackpressureStrategy::Block);
        let entry = catalog
            .register_source("input", Arc::clone(&schema), vec![], None, None, None, None)
            .unwrap();
        entry.declare_managed_push();
        let mut bridge = CatalogSourceConnector::new(
            entry.sink.subscribe(),
            Arc::clone(&schema),
            entry.data_notify(),
            Arc::clone(&entry),
        );

        // Three managed batches are admitted, but the bridge has not polled any of them yet.
        for value in ["a", "b", "c"] {
            entry.admit_arrow(payload_batch(value, &schema)).unwrap();
        }
        let baseline = bridge.checkpoint();
        assert_eq!(
            baseline.get_metadata(ORDERED_INPUT_OFFSET_METADATA_KEY),
            Some("0"),
            "an admitted but not-yet-forwarded batch must stay outside the committed cut"
        );

        // Forward exactly one admitted batch: the cut advances by exactly that batch, not
        // by the admitted tail still queued behind the barrier.
        let forwarded = poll_until_some(&mut bridge, 1).await;
        assert_eq!(forwarded.num_rows(), 1);
        let after_one = bridge.checkpoint();
        assert_eq!(
            after_one.get_metadata(ORDERED_INPUT_OFFSET_METADATA_KEY),
            Some("1")
        );
        assert_eq!(
            after_one.get_metadata(SOURCE_INSTANCE_METADATA_KEY),
            Some(entry.source_instance().as_str())
        );
    }

    #[tokio::test]
    async fn unmanaged_bridge_keeps_checkpoint_metadata_empty() {
        let schema = payload_schema();
        let catalog = SourceCatalog::new(16, BackpressureStrategy::Block);
        let entry = catalog
            .register_source("input", Arc::clone(&schema), vec![], None, None, None, None)
            .unwrap();
        let mut bridge = CatalogSourceConnector::new(
            entry.sink.subscribe(),
            Arc::clone(&schema),
            entry.data_notify(),
            Arc::clone(&entry),
        );
        entry.admit_arrow(payload_batch("a", &schema)).unwrap();
        let _ = poll_until_some(&mut bridge, 1).await;
        let checkpoint = bridge.checkpoint();
        assert!(
            checkpoint.metadata().is_empty(),
            "an undeclared push source must keep its connector checkpoint byte-identical"
        );
    }
}
