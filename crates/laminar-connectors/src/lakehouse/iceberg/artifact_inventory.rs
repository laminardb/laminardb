//! Exact ownership of files created before an Iceberg checkpoint decision.

use std::fmt;
use std::sync::Arc;

use iceberg::io::FileIO;
use iceberg::spec::PartitionKey;
use iceberg::writer::file_writer::location_generator::{
    DefaultLocationGenerator, LocationGenerator,
};
use parking_lot::Mutex;

use crate::error::ConnectorError;

use super::metrics::IcebergMetrics;
use super::IcebergSink;

#[derive(Default)]
struct ArtifactPaths {
    generated: Mutex<Vec<String>>,
    created_finals: Mutex<Vec<String>>,
}

impl fmt::Debug for ArtifactPaths {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactPaths")
            .field("generated_count", &self.generated.lock().len())
            .field("created_final_count", &self.created_finals.lock().len())
            .finish()
    }
}

/// Shared, bounded inventory populated synchronously by Iceberg writer callbacks.
#[derive(Clone, Default, Debug)]
pub(super) struct EpochArtifactTracker {
    paths: Arc<ArtifactPaths>,
}

impl EpochArtifactTracker {
    pub(super) fn record_generated(&self, path: String) {
        self.paths.generated.lock().push(path);
    }

    pub(super) fn record_created_final(&self, path: String) {
        self.paths.created_finals.lock().push(path);
    }

    pub(super) fn snapshot(&self) -> EpochArtifacts {
        EpochArtifacts {
            generated: self.paths.generated.lock().clone(),
            created_finals: self.paths.created_finals.lock().clone(),
        }
    }
}

/// Location generator that records each exact object path before writer I/O starts.
#[derive(Clone)]
pub(super) struct InventoryLocationGenerator {
    inner: DefaultLocationGenerator,
    artifacts: EpochArtifactTracker,
}

impl InventoryLocationGenerator {
    pub(super) fn new(inner: DefaultLocationGenerator, artifacts: EpochArtifactTracker) -> Self {
        Self { inner, artifacts }
    }
}

impl fmt::Debug for InventoryLocationGenerator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InventoryLocationGenerator")
            .field("artifacts", &self.artifacts)
            .finish_non_exhaustive()
    }
}

impl LocationGenerator for InventoryLocationGenerator {
    fn generate_location(&self, partition_key: Option<&PartitionKey>, file_name: &str) -> String {
        let path = self.inner.generate_location(partition_key, file_name);
        self.artifacts.record_generated(path.clone());
        path
    }
}

/// Files whose ownership has not yet transferred to an Iceberg snapshot.
#[derive(Clone, Default, PartialEq, Eq)]
pub(super) struct EpochArtifacts {
    generated: Vec<String>,
    created_finals: Vec<String>,
}

impl fmt::Debug for EpochArtifacts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EpochArtifacts")
            .field("generated_count", &self.generated.len())
            .field("created_final_count", &self.created_finals.len())
            .finish()
    }
}

impl EpochArtifacts {
    pub(super) fn validate_completed(
        &self,
        completed_paths: &[&str],
        max_files: usize,
    ) -> Result<(), ConnectorError> {
        if self.generated.len() != completed_paths.len()
            || self.generated.len() > max_files
            || self.created_finals.len() > self.generated.len()
        {
            return Err(ConnectorError::Internal(
                "Iceberg epoch artifact inventory does not match completed files".into(),
            ));
        }
        let completed = completed_paths
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let generated = self
            .generated
            .iter()
            .collect::<std::collections::HashSet<_>>();
        let created_finals = self
            .created_finals
            .iter()
            .collect::<std::collections::HashSet<_>>();
        if completed.len() != completed_paths.len()
            || generated.len() != self.generated.len()
            || created_finals.len() != self.created_finals.len()
            || self
                .created_finals
                .iter()
                .any(|path| !completed.contains(path.as_str()) || generated.contains(path))
        {
            return Err(ConnectorError::Internal(
                "Iceberg epoch artifact inventory contains a duplicate or foreign final file"
                    .into(),
            ));
        }
        Ok(())
    }

    pub(super) fn path_count(&self) -> usize {
        self.generated
            .len()
            .saturating_add(self.created_finals.len())
    }

    pub(super) async fn cleanup_aborted(
        mut self,
        file_io: FileIO,
        metrics: IcebergMetrics,
    ) -> Result<(), ConnectorError> {
        self.generated.append(&mut self.created_finals);
        delete_exact_paths(file_io, self.generated, metrics).await
    }

    pub(super) async fn cleanup_committed_staging(
        self,
        file_io: FileIO,
        metrics: IcebergMetrics,
    ) -> Result<(), ConnectorError> {
        delete_exact_paths(file_io, self.generated, metrics).await
    }

    #[cfg(test)]
    pub(super) fn generated_paths(&self) -> &[String] {
        &self.generated
    }

    #[cfg(test)]
    pub(super) fn created_final_paths(&self) -> &[String] {
        &self.created_finals
    }
}

/// Checkpoint lifecycle proof that determines which exact files may be removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PreparedEpochCleanup {
    Successor { next_epoch: u64 },
    Abort { epoch: u64 },
    Close,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreparedPublicationState {
    Unpublished,
    DescriptorIssued,
}

/// Bounded participant-local ownership retained from pre-commit to a terminal transition.
pub(super) struct PreparedEpochArtifacts {
    epoch: u64,
    artifacts: EpochArtifactTracker,
    publication: PreparedPublicationState,
}

impl fmt::Debug for PreparedEpochArtifacts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedEpochArtifacts")
            .field("epoch", &self.epoch)
            .field("artifacts", &self.artifacts)
            .field("publication", &self.publication)
            .finish()
    }
}

impl PreparedEpochArtifacts {
    pub(super) fn new(
        epoch: u64,
        artifacts: EpochArtifactTracker,
        metrics: &IcebergMetrics,
    ) -> Self {
        let path_count = artifacts.snapshot().path_count();
        metrics
            .pending_artifact_paths
            .set(i64::try_from(path_count).unwrap_or(i64::MAX));
        Self {
            epoch,
            artifacts,
            publication: PreparedPublicationState::Unpublished,
        }
    }

    pub(super) fn seal(
        &self,
        completed: &EpochArtifacts,
        metrics: &IcebergMetrics,
    ) -> Result<(), ConnectorError> {
        let tracked = self.artifacts.snapshot();
        if tracked != *completed {
            return Err(ConnectorError::Internal(
                "Iceberg prepared artifact inventory changed during writer close".into(),
            ));
        }
        metrics
            .pending_artifact_paths
            .set(i64::try_from(tracked.path_count()).unwrap_or(i64::MAX));
        Ok(())
    }

    pub(super) fn mark_descriptor_issued(&mut self) {
        self.publication = PreparedPublicationState::DescriptorIssued;
    }

    fn validate_cleanup(&self, cleanup: PreparedEpochCleanup) -> Result<(), ConnectorError> {
        match cleanup {
            PreparedEpochCleanup::Successor { next_epoch } if self.epoch >= next_epoch => {
                Err(ConnectorError::InvalidState {
                    expected: format!("a successor to prepared Iceberg epoch {}", self.epoch),
                    actual: format!("begin epoch {next_epoch}"),
                })
            }
            PreparedEpochCleanup::Abort { epoch } if self.epoch != epoch => {
                Err(ConnectorError::InvalidState {
                    expected: format!("rollback prepared Iceberg epoch {}", self.epoch),
                    actual: format!("rollback epoch {epoch}"),
                })
            }
            PreparedEpochCleanup::Successor { .. }
            | PreparedEpochCleanup::Abort { .. }
            | PreparedEpochCleanup::Close => Ok(()),
        }
    }

    #[cfg(test)]
    pub(super) fn artifacts(&self) -> EpochArtifacts {
        self.artifacts.snapshot()
    }

    fn removes_final_files(&self, cleanup: PreparedEpochCleanup) -> bool {
        matches!(cleanup, PreparedEpochCleanup::Abort { .. })
            || matches!(
                (cleanup, self.publication),
                (
                    PreparedEpochCleanup::Close,
                    PreparedPublicationState::Unpublished
                )
            )
    }
}

pub(super) async fn cleanup_prepared_epoch(
    prepared: &mut Option<PreparedEpochArtifacts>,
    cleanup: PreparedEpochCleanup,
    file_io: FileIO,
    metrics: IcebergMetrics,
) -> Result<(), ConnectorError> {
    let Some(current) = prepared.as_ref() else {
        return Ok(());
    };
    current.validate_cleanup(cleanup)?;
    let artifacts = current.artifacts.snapshot();
    metrics
        .pending_artifact_paths
        .set(i64::try_from(artifacts.path_count()).unwrap_or(i64::MAX));
    let result = if current.removes_final_files(cleanup) {
        artifacts.cleanup_aborted(file_io, metrics.clone()).await
    } else {
        artifacts
            .cleanup_committed_staging(file_io, metrics.clone())
            .await
    };
    result?;
    *prepared = None;
    metrics.pending_artifact_paths.set(0);
    Ok(())
}

impl IcebergSink {
    pub(super) fn ensure_no_unresolved_publication(&self) -> Result<(), ConnectorError> {
        if self.unresolved_publication.lock().is_some() {
            Err(ConnectorError::InvalidState {
                expected: "reconciliation of the exact ambiguous Iceberg publication".into(),
                actual: "a prior coordinated publication remains unresolved".into(),
            })
        } else {
            Ok(())
        }
    }

    pub(super) async fn discard_active_epoch(&mut self) -> Result<(), ConnectorError> {
        let has_writer = self.active_epoch.get_mut().is_some();
        if has_writer && self.active_epoch_id.is_none() {
            return Err(ConnectorError::Internal(
                "Iceberg active writer is missing its epoch identity".into(),
            ));
        }
        if has_writer && self.prepared_epoch.is_some() {
            return Err(ConnectorError::InvalidState {
                expected: "one Iceberg epoch artifact owner".into(),
                actual: "active and prepared artifact ownership overlap".into(),
            });
        }
        let epoch = self.active_epoch_id.take();
        let writer = self.active_epoch.get_mut().take();
        self.metrics.set_buffer(0, 0);
        self.metrics.set_active_writers(0);
        if let Some(writer) = writer {
            let epoch = epoch.ok_or_else(|| {
                ConnectorError::Internal("Iceberg active writer lost its epoch identity".into())
            })?;
            self.prepared_epoch = Some(PreparedEpochArtifacts::new(
                epoch,
                writer.artifact_tracker(),
                &self.metrics,
            ));
            writer.abort().await?;
        }
        Ok(())
    }

    pub(super) async fn cleanup_prepared_epoch(
        &mut self,
        cleanup: PreparedEpochCleanup,
    ) -> Result<(), ConnectorError> {
        if matches!(cleanup, PreparedEpochCleanup::Abort { .. }) {
            self.ensure_no_unresolved_publication()?;
        }
        let file_io = self.table()?.file_io().clone();
        cleanup_prepared_epoch(
            &mut self.prepared_epoch,
            cleanup,
            file_io,
            self.metrics.clone(),
        )
        .await
    }
}

async fn delete_exact_paths(
    file_io: FileIO,
    paths: Vec<String>,
    metrics: IcebergMetrics,
) -> Result<(), ConnectorError> {
    let mut first_error = None;
    let mut failure_count = 0_u64;
    for path in paths {
        match file_io.delete(&path).await {
            Ok(()) => metrics.artifact_delete_successes.inc(),
            Err(error) => {
                failure_count = failure_count.saturating_add(1);
                first_error.get_or_insert_with(|| {
                    crate::lakehouse::iceberg_io::external_error_summary(&error)
                });
            }
        }
    }
    if failure_count == 0 {
        return Ok(());
    }
    metrics.artifact_cleanup_failures.inc_by(failure_count);
    Err(ConnectorError::WriteError(format!(
        "[LDB-ICEBERG-ARTIFACT-CLEANUP] failed to delete {failure_count} exact checkpoint-owned paths ({})",
        first_error.unwrap_or_else(|| "storage error".into())
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output_exposes_counts_but_not_object_paths() {
        let secret_path = "https://user:artifact-secret@objects.test/data/file.parquet";
        let artifacts = EpochArtifacts {
            generated: vec![secret_path.into()],
            created_finals: vec![secret_path.into()],
        };
        let tracker = EpochArtifactTracker::default();
        tracker.record_generated(secret_path.into());
        tracker.record_created_final(secret_path.into());

        for debug in [format!("{artifacts:?}"), format!("{tracker:?}")] {
            assert!(debug.contains("count"));
            assert!(!debug.contains("artifact-secret"));
        }
    }

    #[test]
    fn completed_inventory_rejects_duplicate_or_foreign_paths() {
        let duplicate = EpochArtifacts {
            generated: vec!["memory:///stage-a".into(), "memory:///stage-a".into()],
            created_finals: Vec::new(),
        };
        assert!(duplicate
            .validate_completed(&["memory:///final-a", "memory:///final-b"], 2)
            .is_err());

        let foreign = EpochArtifacts {
            generated: vec!["memory:///stage-a".into()],
            created_finals: vec!["memory:///foreign".into()],
        };
        assert!(foreign
            .validate_completed(&["memory:///final-a"], 1)
            .is_err());

        let overlapping = EpochArtifacts {
            generated: vec!["memory:///final-a".into()],
            created_finals: vec!["memory:///final-a".into()],
        };
        assert!(overlapping
            .validate_completed(&["memory:///final-a"], 1)
            .is_err());
    }
}
