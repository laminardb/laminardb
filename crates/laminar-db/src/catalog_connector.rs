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
use laminar_connectors::connector::{SourceBatch, SourceConnector};
use laminar_connectors::error::ConnectorError;
use laminar_core::streaming;

use crate::catalog::ArrowRecord;

/// Bridges a catalog SPSC subscription into the pipeline alongside external connectors.
pub(crate) struct CatalogSourceConnector {
    subscription: streaming::Subscription<ArrowRecord>,
    schema: SchemaRef,
    data_notify: Arc<Notify>,
    records_polled: u64,
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
            records_polled: 0,
        }
    }
}

#[async_trait]
impl SourceConnector for CatalogSourceConnector {
    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
        Ok(())
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

        self.records_polled += u64::try_from(records.num_rows()).unwrap_or(u64::MAX);

        Ok(Some(SourceBatch {
            records,
            partition: None,
        }))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new(0);
        cp.set_offset(
            "records_polled".to_string(),
            self.records_polled.to_string(),
        );
        cp
    }

    async fn restore(&mut self, _checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_notify))
    }

    fn supports_replay(&self) -> bool {
        false
    }
}
