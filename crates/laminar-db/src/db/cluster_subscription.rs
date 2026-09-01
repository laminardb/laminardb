//! Planner-certified cluster subscription admission and gateway construction.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::physical_expr::PhysicalExpr;
use laminar_core::checkpoint::{CheckpointStore, ObjectStoreCheckpointStore, OutputDistribution};

use super::LaminarDB;
use crate::error::DbError;
use crate::subscription::cluster::ClusterSubscriptionReader;
use crate::subscription::{ClusterSubscriptionError, SubscribeStart, SubscriptionPortal};

impl LaminarDB {
    pub(super) async fn open_cluster_subscription(
        &self,
        name: &str,
        schema: SchemaRef,
        filter: Option<Arc<dyn PhysicalExpr>>,
        start: SubscribeStart,
    ) -> Result<SubscriptionPortal, DbError> {
        let metrics = self.engine_metrics();
        if let Some(metrics) = metrics.as_ref() {
            metrics.cluster_subscription.open_total.inc();
        }
        let result = self
            .open_cluster_subscription_inner(name, schema, filter, start, metrics.clone())
            .await;
        if let Err(error) = &result {
            if let Some(metrics) = metrics.as_ref() {
                metrics.cluster_subscription.open_failures_total.inc();
                if matches!(
                    error,
                    DbError::Subscription(ClusterSubscriptionError::ReplayPruned { .. })
                ) {
                    metrics.cluster_subscription.replay_pruned_total.inc();
                }
            }
        }
        result
    }

    async fn open_cluster_subscription_inner(
        &self,
        name: &str,
        schema: SchemaRef,
        filter: Option<Arc<dyn PhysicalExpr>>,
        start: SubscribeStart,
        metrics: Option<Arc<crate::engine_metrics::EngineMetrics>>,
    ) -> Result<SubscriptionPortal, DbError> {
        let registration = self
            .connector_manager
            .lock()
            .streams()
            .get(name)
            .cloned()
            .ok_or_else(|| unsupported("the named object is not a managed stream"))?;
        let certificate = registration
            .subscription_certificate
            .map(Arc::new)
            .ok_or_else(|| {
                unsupported("the final operator has no output-distribution certificate")
            })?;
        if registration.window_config.is_some()
            || registration.join_config.is_some()
            || registration.has_analytic
            || registration.has_frame
            || !matches!(
                certificate.distribution,
                OutputDistribution::VnodePartitioned { .. }
            )
        {
            return Err(unsupported(
                "only non-windowed managed keyed aggregate streams are admitted",
            ));
        }
        certificate
            .validate(self.checkpoint_key_groups())
            .map_err(|error| unsupported(format!("invalid distribution certificate: {error}")))?;
        let schema_fingerprint =
            crate::pipeline_identity::subscription_schema_fingerprint(&schema)?;
        if schema_fingerprint != certificate.schema_fingerprint {
            return Err(ClusterSubscriptionError::SchemaMismatch.into());
        }

        // No authority or object-store operation occurs before plan and schema certification.
        let controller =
            self.cluster_controller.lock().clone().ok_or_else(|| {
                DbError::Subscription(ClusterSubscriptionError::BackendUnavailable)
            })?;
        let authority = controller
            .checkpoint_authority()
            .map_err(|_| ClusterSubscriptionError::BackendUnavailable)?;
        let object_store = self
            .cluster_checkpoint_object_store()
            .ok_or_else(|| DbError::Subscription(ClusterSubscriptionError::BackendUnavailable))?;
        let checkpoint_store: Arc<dyn CheckpointStore> = Arc::new(
            ObjectStoreCheckpointStore::new(object_store, "")
                .with_key_group_count(self.checkpoint_key_groups()),
        );
        let reader = ClusterSubscriptionReader::open(
            authority,
            checkpoint_store,
            Arc::clone(&certificate),
            start,
            metrics,
        )
        .await?;
        tracing::info!(
            stream_generation = %certificate.stream_generation,
            start = ?start,
            partitions = certificate.distribution.partition_count(),
            "opened committed cluster subscription"
        );
        Ok(SubscriptionPortal::open_cluster(
            name, schema, reader, filter,
        ))
    }
}

fn unsupported(reason: impl Into<String>) -> DbError {
    ClusterSubscriptionError::UnsupportedPlan {
        reason: reason.into(),
    }
    .into()
}
