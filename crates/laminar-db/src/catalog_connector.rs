//! Bridge connector that routes `db.insert()` data through the pipeline.
//!
//! When a source has no external connector (e.g., created without a `FROM` clause),
//! this connector leases the catalog source's bounded input queue so data inserted via
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

use crate::catalog::{IngressLease, SourceEntry};
use crate::DbError;

/// Bridges a catalog source's input queue into the pipeline alongside external connectors.
pub(crate) struct CatalogSourceConnector {
    ingress: IngressLease,
    schema: SchemaRef,
    data_notify: Arc<Notify>,
}

impl CatalogSourceConnector {
    pub fn new(entry: &Arc<SourceEntry>) -> Result<Self, DbError> {
        Ok(Self {
            ingress: entry.lease_ingress()?,
            schema: Arc::clone(&entry.schema),
            data_notify: entry.data_notify(),
        })
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
            match self.ingress.poll() {
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
        // `db.insert()` is process-local ingress backed by an in-memory queue. A row
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::streaming::StreamingError;

    use super::*;
    use crate::catalog::SourceCatalog;
    use crate::{LaminarConfig, LaminarDB};

    fn one_row(schema: &SchemaRef, id: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int64Array::from(vec![id]))],
        )
        .unwrap()
    }

    async fn drain_ids(bridge: &mut CatalogSourceConnector) -> Vec<i64> {
        let mut ids = Vec::new();
        while let Some(batch) = bridge.poll_batch(1024).await.unwrap() {
            let column = batch.records.column(0).as_any();
            ids.extend(column.downcast_ref::<Int64Array>().unwrap().values());
        }
        ids
    }

    async fn wait_for_events(db: &LaminarDB, stream: &str, events: u64) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while db.stream_metrics(stream).unwrap().total_events < events {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("stream '{stream}' never reached {events} events"));
    }

    #[tokio::test]
    async fn stalled_bridge_pushes_back_instead_of_dropping_inserts() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let entry = SourceCatalog::from_config(&LaminarConfig {
            default_buffer_size: 4096,
            ..Default::default()
        })
        .register_source("input", Arc::clone(&schema), vec![], None, None, None, None)
        .unwrap();
        let mut bridge = CatalogSourceConnector::new(&entry).unwrap();

        let (mut accepted, mut rejected) = (0, 0);
        for id in 0..8192 {
            match entry.push_and_buffer(one_row(&schema, id)) {
                Ok(()) => accepted += 1,
                Err(StreamingError::ChannelFull) => rejected += 1,
                Err(error) => panic!("unexpected insert error: {error}"),
            }
        }
        assert_eq!((accepted, rejected), (4096, 4096));
        assert_eq!(drain_ids(&mut bridge).await, (0..4096).collect::<Vec<_>>());
        entry.push_and_buffer(one_row(&schema, 8192)).unwrap();
    }

    #[tokio::test]
    async fn second_bridge_is_refused_until_the_first_returns_the_queue() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let entry = SourceCatalog::from_config(&LaminarConfig::default())
            .register_source("input", Arc::clone(&schema), vec![], None, None, None, None)
            .unwrap();
        let first = CatalogSourceConnector::new(&entry).unwrap();
        assert!(matches!(
            CatalogSourceConnector::new(&entry),
            Err(DbError::Pipeline(message)) if message.contains("already consumed")
        ));
        entry.push_and_buffer(one_row(&schema, 1)).unwrap();
        drop(first);
        let mut second = CatalogSourceConnector::new(&entry).unwrap();
        assert_eq!(drain_ids(&mut second).await, vec![1]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inserts_before_start_and_while_stopped_reach_streams() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE input (id BIGINT)").await.unwrap();
        db.execute("CREATE STREAM output AS SELECT id FROM input")
            .await
            .unwrap();
        let input = db.source_untyped("input").unwrap();
        let schema = Arc::clone(input.schema());
        let mut next_id = 0..;
        let mut insert = |rows: usize| {
            for id in next_id.by_ref().take(rows) {
                input.push_arrow(one_row(&schema, id)).unwrap();
            }
        };

        insert(10);
        db.start().await.unwrap();
        insert(1);
        wait_for_events(&db, "output", 11).await;

        db.stop_pipeline().await.unwrap();
        insert(5);
        db.start().await.unwrap();
        wait_for_events(&db, "output", 16).await;
        db.shutdown().await.unwrap();
        assert_eq!(db.stream_metrics("output").unwrap().total_events, 16);
    }
}
