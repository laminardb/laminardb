//! Fenced cleanup of exact files retained in aborted checkpoint descriptors.

use std::sync::Arc;

use futures_util::{stream, StreamExt};
use iceberg::Catalog;

use crate::connector::{CoordinatedAbortBatch, CoordinatedCommitContext};
use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::IcebergSinkConfig;

use super::commit_cursor::cursor_record;
use super::descriptor_batch::{prepare_descriptor_files, validate_table_incarnation};
use super::metrics::IcebergMetrics;
use super::publication::{load_table_until, SUMMARY_CHECKPOINT, SUMMARY_NAMESPACE};

const DELETE_CONCURRENCY: usize = 8;

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
    let prepared = prepare_descriptor_files(config, &batch.namespace, &batch.entries, &table)?;
    validate_table_incarnation(config, &prepared.binding, &table)?;
    reject_published_abort(&table, batch)?;
    // RECOVERY: the durable Abort and current process/leader fence exclude publication for this
    // attempt; deterministic names exclude every other attempt from owning these exact paths.
    let paths = prepared
        .data_files
        .into_iter()
        .map(|file| file.file_path().to_owned())
        .collect::<Vec<_>>();
    delete_exact_paths(table.file_io().clone(), paths, context.deadline(), metrics).await
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
