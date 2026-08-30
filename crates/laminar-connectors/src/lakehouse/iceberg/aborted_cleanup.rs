//! Fenced cleanup of exact files retained in aborted checkpoint descriptors.

use std::collections::HashMap;
use std::sync::Arc;

use futures_util::{stream, StreamExt};
use iceberg::Catalog;
use parking_lot::Mutex;

use crate::connector::{
    CoordinatedAbortBatch, CoordinatedAbortCleaner, CoordinatedAbortDescriptor,
    CoordinatedCommitContext,
};
use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::IcebergSinkConfig;

use super::commit_cursor::cursor_record;
use super::descriptor_batch::{prepare_aborted_descriptor_files, validate_table_incarnation};
use super::epoch_intent::IcebergEpochIntentV1;
use super::file_finalizer::REPLAY_SAFE_PREFIX_BYTES;
use super::metrics::IcebergMetrics;
use super::publication::{
    load_table_until, UnresolvedIcebergPublication, SUMMARY_CHECKPOINT, SUMMARY_NAMESPACE,
};

const DELETE_CONCURRENCY: usize = 8;

pub(super) struct IcebergAbortCleaner {
    catalog: Arc<dyn Catalog>,
    config: IcebergSinkConfig,
    metrics: IcebergMetrics,
    unresolved_publication: Arc<Mutex<Option<UnresolvedIcebergPublication>>>,
}

impl IcebergAbortCleaner {
    pub(super) fn new(
        catalog: Arc<dyn Catalog>,
        config: IcebergSinkConfig,
        metrics: IcebergMetrics,
        unresolved_publication: Arc<Mutex<Option<UnresolvedIcebergPublication>>>,
    ) -> Self {
        Self {
            catalog,
            config,
            metrics,
            unresolved_publication,
        }
    }
}

#[async_trait::async_trait]
impl CoordinatedAbortCleaner for IcebergAbortCleaner {
    async fn cleanup_aborted(
        &self,
        batch: CoordinatedAbortBatch,
        context: CoordinatedCommitContext,
    ) -> Result<(), ConnectorError> {
        if self.unresolved_publication.lock().is_some() {
            self.metrics.unknown_outcomes.inc();
            return Err(ConnectorError::outcome_unknown(
                "[LDB-ICEBERG-ABORT-CLEANUP-OUTCOME-UNKNOWN] exact publication reconciliation is incomplete",
                true,
            ));
        }
        let result =
            cleanup_aborted_files(&self.catalog, &self.config, &batch, context, &self.metrics)
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
}

pub(super) fn ensure_no_unresolved_publication(
    unresolved: &Mutex<Option<UnresolvedIcebergPublication>>,
) -> Result<(), ConnectorError> {
    if unresolved.lock().is_some() {
        return Err(ConnectorError::InvalidState {
            expected: "reconciliation of the exact ambiguous Iceberg publication".into(),
            actual: "a prior coordinated publication remains unresolved".into(),
        });
    }
    Ok(())
}

pub(super) async fn cleanup_aborted_files(
    catalog: &Arc<dyn Catalog>,
    config: &IcebergSinkConfig,
    batch: &CoordinatedAbortBatch,
    context: CoordinatedCommitContext,
    metrics: &IcebergMetrics,
) -> Result<(), ConnectorError> {
    batch.validate_shape().map_err(|error| {
        ConnectorError::TransactionError(format!(
            "Iceberg coordinated abort validation failed: {error}"
        ))
    })?;
    require_cleanup_budget(context.deadline())?;
    let table = load_table_until(catalog, config, context.deadline(), false).await?;
    let prepared =
        prepare_aborted_descriptor_files(config, &batch.namespace, &batch.entries, &table)?;
    validate_table_incarnation(config, &prepared.binding, &table)?;
    reject_published_abort(&table, batch)?;
    let roots = validate_attempt_roots(config, batch, &table, context.deadline()).await?;
    let legacy_paths = prepared
        .data_files
        .into_iter()
        .filter_map(|file| {
            let path = file.file_path();
            (!path_belongs_to_attempt_root(path, &roots)).then(|| path.to_owned())
        })
        .collect::<Vec<_>>();
    delete_attempt_roots(
        table.file_io().clone(),
        roots.into_values().collect(),
        context.deadline(),
        metrics,
    )
    .await?;
    delete_exact_paths(
        table.file_io().clone(),
        legacy_paths,
        context.deadline(),
        metrics,
    )
    .await
}

async fn validate_attempt_roots(
    config: &IcebergSinkConfig,
    batch: &CoordinatedAbortBatch,
    table: &iceberg::table::Table,
    deadline: tokio::time::Instant,
) -> Result<HashMap<String, String>, ConnectorError> {
    let mut roots = HashMap::with_capacity(batch.entries.len());
    for entry in &batch.entries {
        let Some(payload) = entry.artifact_intent.as_deref() else {
            if matches!(entry.descriptor, CoordinatedAbortDescriptor::Open) {
                return Err(ConnectorError::TransactionError(
                    "[LDB-ICEBERG-EPOCH-INTENT-MISSING] aborted open Iceberg participant has no durable artifact intent"
                        .into(),
                ));
            }
            continue;
        };
        let intent = IcebergEpochIntentV1::decode(payload)?;
        intent
            .validate_cleanup(config, &batch.namespace, entry, table, deadline)
            .await?;
        if roots
            .insert(
                intent.namespace_prefix().to_owned(),
                intent.attempt_root().to_owned(),
            )
            .is_some()
        {
            return Err(ConnectorError::TransactionError(
                "Iceberg aborted participants repeat an artifact namespace".into(),
            ));
        }
    }
    Ok(roots)
}

fn path_belongs_to_attempt_root(path: &str, roots: &HashMap<String, String>) -> bool {
    let Some(name) = path.rsplit('/').next() else {
        return false;
    };
    let Some(prefix) = name.get(..REPLAY_SAFE_PREFIX_BYTES) else {
        return false;
    };
    roots.get(prefix).is_some_and(|root| {
        path.strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

async fn delete_attempt_roots(
    file_io: iceberg::io::FileIO,
    roots: Vec<String>,
    deadline: tokio::time::Instant,
    metrics: &IcebergMetrics,
) -> Result<(), ConnectorError> {
    let mut deletes = stream::iter(roots.into_iter().map(|root| {
        let file_io = file_io.clone();
        async move {
            require_cleanup_budget(deadline).map_err(|error| error.to_string())?;
            tokio::time::timeout_at(deadline, file_io.delete_prefix(root))
                .await
                .map_err(|_| "aborted artifact namespace delete timed out".to_owned())?
                .map_err(|error| storage_error("delete aborted artifact namespace", &error))
        }
    }))
    .buffer_unordered(DELETE_CONCURRENCY);
    let mut failures = 0_u64;
    let mut first_error = None;
    while let Some(result) = deletes.next().await {
        match result {
            Ok(()) => metrics.artifact_delete_successes.inc(),
            Err(error) => {
                failures = failures.saturating_add(1);
                first_error.get_or_insert(error);
            }
        }
    }
    if failures == 0 {
        return Ok(());
    }
    metrics.artifact_cleanup_failures.inc_by(failures);
    Err(ConnectorError::WriteError(format!(
        "[LDB-ICEBERG-DURABLE-ARTIFACT-CLEANUP] failed to delete {failures} exact aborted artifact namespaces ({})",
        first_error.unwrap_or_else(|| "storage error".into())
    )))
}

fn reject_published_abort(
    table: &iceberg::table::Table,
    batch: &CoordinatedAbortBatch,
) -> Result<(), ConnectorError> {
    let external_key = batch.namespace.external_key();
    if cursor_record(table, &external_key)?
        .is_some_and(|record| record.cursor.checkpoint_id >= batch.target.checkpoint_id)
    {
        return Err(published_abort_error());
    }
    let checkpoint = batch.target.checkpoint_id.to_string();
    let snapshot_exists = table.metadata().snapshots().any(|snapshot| {
        let properties = &snapshot.summary().additional_properties;
        properties.get(SUMMARY_NAMESPACE).map(String::as_str) == Some(external_key.as_str())
            && properties.get(SUMMARY_CHECKPOINT).map(String::as_str) == Some(checkpoint.as_str())
    });
    if snapshot_exists {
        return Err(published_abort_error());
    }
    Ok(())
}

fn published_abort_error() -> ConnectorError {
    ConnectorError::outcome_unknown(
        "[LDB-ICEBERG-ABORT-CLEANUP-PUBLISHED] an Iceberg snapshot or cursor already references the aborted checkpoint",
        true,
    )
}

async fn delete_exact_paths(
    file_io: iceberg::io::FileIO,
    paths: Vec<String>,
    deadline: tokio::time::Instant,
    metrics: &IcebergMetrics,
) -> Result<(), ConnectorError> {
    let mut deletes = stream::iter(paths.into_iter().map(|path| {
        let file_io = file_io.clone();
        async move { delete_exact_path(&file_io, &path, deadline).await }
    }))
    .buffer_unordered(DELETE_CONCURRENCY);
    let mut failures = 0_u64;
    let mut first_error = None;
    while let Some(result) = deletes.next().await {
        match result {
            Ok(true) => metrics.artifact_delete_successes.inc(),
            Ok(false) => {}
            Err(error) => {
                failures = failures.saturating_add(1);
                first_error.get_or_insert(error);
            }
        }
    }
    if failures == 0 {
        return Ok(());
    }
    metrics.artifact_cleanup_failures.inc_by(failures);
    Err(ConnectorError::WriteError(format!(
        "[LDB-ICEBERG-DURABLE-ARTIFACT-CLEANUP] failed to delete {failures} exact aborted data files ({})",
        first_error.unwrap_or_else(|| "storage error".into())
    )))
}

async fn delete_exact_path(
    file_io: &iceberg::io::FileIO,
    path: &str,
    deadline: tokio::time::Instant,
) -> Result<bool, String> {
    require_cleanup_budget(deadline).map_err(|error| error.to_string())?;
    let input = file_io
        .new_input(path)
        .map_err(|error| storage_error("open aborted data file", &error))?;
    let exists = tokio::time::timeout_at(deadline, input.exists())
        .await
        .map_err(|_| "aborted data-file existence check timed out".to_owned())?
        .map_err(|error| storage_error("inspect aborted data file", &error))?;
    if !exists {
        return Ok(false);
    }
    tokio::time::timeout_at(deadline, file_io.delete(path))
        .await
        .map_err(|_| "aborted data-file delete timed out".to_owned())?
        .map_err(|error| storage_error("delete aborted data file", &error))?;
    Ok(true)
}

fn require_cleanup_budget(deadline: tokio::time::Instant) -> Result<(), ConnectorError> {
    if tokio::time::Instant::now() >= deadline {
        Err(ConnectorError::WriteError(
            "[LDB-ICEBERG-DURABLE-ARTIFACT-CLEANUP-TIMEOUT] aborted data-file cleanup deadline elapsed"
                .into(),
        ))
    } else {
        Ok(())
    }
}

fn storage_error(operation: &str, error: &iceberg::Error) -> String {
    format!(
        "Iceberg {operation} ({})",
        crate::lakehouse::iceberg_io::external_error_summary(error)
    )
}
