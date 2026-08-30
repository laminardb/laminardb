use async_trait::async_trait;

use crate::connector::{
    CoordinatedAbortBatch, CoordinatedCommitBatch, CoordinatedCommitContext,
    CoordinatedCommitCursor, CoordinatedCommitNamespace, CoordinatedCommitter,
};
use crate::error::ConnectorError;

use super::{aborted_cleanup, publication, IcebergSink};

#[async_trait]
impl CoordinatedCommitter for IcebergSink {
    async fn commit_aggregated(
        &self,
        batch: CoordinatedCommitBatch,
        context: CoordinatedCommitContext,
    ) -> Result<(), ConnectorError> {
        let catalog = self
            .catalog
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open Iceberg sink".into(),
                actual: "catalog is not initialized".into(),
            })?;
        let pending = publication::unresolved_publication(&self.config, &batch)?;
        {
            let mut unresolved = self.unresolved_publication.lock();
            if unresolved
                .as_ref()
                .is_some_and(|existing| existing != &pending)
            {
                return Err(ConnectorError::TransactionError(
                    "Iceberg has a different unresolved publication; only that exact cut may be reconciled"
                        .into(),
                ));
            }
            *unresolved = Some(pending.clone());
        }
        let result = publication::publish_coordinated(
            catalog,
            &self.catalog_capabilities,
            &self.catalog_session,
            &self.config,
            &batch,
            context,
            &self.metrics,
        )
        .await;
        if result.is_ok()
            || result
                .as_ref()
                .is_err_and(|error| !error.is_outcome_unknown())
        {
            let mut unresolved = self.unresolved_publication.lock();
            if unresolved.as_ref() == Some(&pending) {
                *unresolved = None;
            }
        }
        result
    }

    async fn cleanup_aborted(
        &self,
        batch: CoordinatedAbortBatch,
        context: CoordinatedCommitContext,
    ) -> Result<(), ConnectorError> {
        self.ensure_no_unresolved_publication()?;
        let catalog = self
            .catalog
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open Iceberg sink".into(),
                actual: "catalog is not initialized".into(),
            })?;
        let result = aborted_cleanup::cleanup_aborted_files(
            catalog,
            &self.config,
            &batch,
            context,
            &self.metrics,
        )
        .await;
        if result
            .as_ref()
            .err()
            .is_some_and(ConnectorError::is_outcome_unknown)
        {
            self.metrics.unknown_outcomes.inc();
        }
        result
    }

    async fn committed_cursor(
        &self,
        namespace: &CoordinatedCommitNamespace,
    ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
        namespace.validate()?;
        let catalog = self
            .catalog
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open Iceberg sink".into(),
                actual: "catalog is not initialized".into(),
            })?;
        let deadline = tokio::time::Instant::now() + self.config.catalog.request_timeout;
        let pending = self.unresolved_publication.lock().clone();
        let cursor = publication::read_committed_cursor(
            catalog,
            &self.config,
            namespace,
            deadline,
            &self.metrics,
            pending.as_ref(),
        )
        .await?;
        let mut unresolved = self.unresolved_publication.lock();
        if unresolved.as_ref().is_some_and(|pending| {
            pending.external_key == namespace.external_key() && pending.reconciled_by(cursor)
        }) {
            *unresolved = None;
        }
        Ok(cursor)
    }
}
