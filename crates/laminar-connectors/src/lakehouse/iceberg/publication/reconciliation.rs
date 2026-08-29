use std::collections::HashSet;
use std::sync::Arc;

use iceberg::spec::{DataContentType, DataFile, ManifestStatus};
use iceberg::Catalog;

use crate::connector::{
    CoordinatedCommitBatch, CoordinatedCommitCursor, CoordinatedCommitNamespace,
};
use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::IcebergSinkConfig;
use crate::lakehouse::iceberg_scan::{load_manifest, load_manifest_list, ManifestReadLimits};

use super::{
    data_file_set_fingerprint, ensure_deadline, file_set_fingerprint, hex, load_table_until,
    PreparedPublication, UnresolvedIcebergPublication, SUMMARY_BATCH_FINGERPRINT,
    SUMMARY_CHECKPOINT, SUMMARY_COMMIT_UUID, SUMMARY_FENCE, SUMMARY_FILE_SET, SUMMARY_NAMESPACE,
};
use crate::lakehouse::iceberg::commit_cursor::{cursor_record, CursorRecord};
use crate::lakehouse::iceberg::metrics::IcebergMetrics;
use crate::lakehouse::iceberg_config::ICEBERG_MAX_FILES_PER_CHECKPOINT;

pub(in crate::lakehouse::iceberg) async fn read_committed_cursor(
    catalog: &Arc<dyn Catalog>,
    config: &IcebergSinkConfig,
    namespace: &CoordinatedCommitNamespace,
    deadline: tokio::time::Instant,
    metrics: &IcebergMetrics,
    unresolved: Option<&UnresolvedIcebergPublication>,
) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
    let started = std::time::Instant::now();
    let table = load_table_until(catalog, config, deadline, false).await?;
    #[cfg(test)]
    crate::lakehouse::iceberg::fault_injection::fail_outcome_unknown_if(
        crate::lakehouse::iceberg::fault_injection::IcebergFaultPoint::DuringCommittedCursor,
    )?;
    let record = cursor_record(&table, &namespace.external_key())?;
    if let (Some(record), Some(unresolved)) = (&record, unresolved) {
        validate_unresolved_record(namespace, record, unresolved)?;
    }
    if let Some(record) = &record {
        verify_cursor_record(&table, &namespace.external_key(), record, deadline)
            .await
            .map_err(|error| {
                ConnectorError::outcome_unknown(
                    format!("Iceberg committed cursor lacks exact snapshot evidence: {error}"),
                    true,
                )
            })?;
    }
    let cursor = record.map(|record| record.cursor);
    metrics
        .reconciliation_duration
        .observe(started.elapsed().as_secs_f64());
    if let Some(cursor) = cursor {
        metrics
            .committed_checkpoint
            .set(i64::try_from(cursor.checkpoint_id).unwrap_or(i64::MAX));
    }
    Ok(cursor)
}

fn validate_unresolved_record(
    namespace: &CoordinatedCommitNamespace,
    record: &CursorRecord,
    unresolved: &UnresolvedIcebergPublication,
) -> Result<(), ConnectorError> {
    let exact_fingerprint = hex(&unresolved.exact_batch_fingerprint);
    if unresolved.external_key == namespace.external_key()
        && (record.cursor != unresolved.target
            || record.batch_fingerprint != exact_fingerprint
            || record.file_set_fingerprint != unresolved.expected_file_set_fingerprint)
    {
        return Err(ConnectorError::outcome_unknown(
            "Iceberg cursor does not prove the exact unresolved publication and file set",
            true,
        ));
    }
    Ok(())
}

pub(super) async fn reconcile_exact_publication(
    table: &iceberg::table::Table,
    batch: &CoordinatedCommitBatch,
    prepared: &PreparedPublication,
    exact_batch_fingerprint: &str,
    commit_uuid: uuid::Uuid,
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    let external_key = batch.namespace.external_key();
    let record = cursor_record(table, &external_key)?.ok_or_else(|| {
        ConnectorError::TransactionError("Iceberg coordinated cursor is absent".into())
    })?;
    let expected_cursor = CoordinatedCommitCursor {
        checkpoint_id: batch.target.checkpoint_id,
        fencing_token: batch.fencing_token,
    };
    validate_expected_record(
        &record,
        expected_cursor,
        exact_batch_fingerprint,
        &prepared.file_set_fingerprint,
        commit_uuid,
    )?;
    if prepared.data_files.is_empty() {
        if record.cursor != expected_cursor {
            return Err(ConnectorError::TransactionError(
                "superseded empty Iceberg checkpoint cannot be proven from snapshot history".into(),
            ));
        }
        return Ok(());
    }

    let snapshot = find_exact_snapshot(
        table,
        &external_key,
        expected_cursor,
        exact_batch_fingerprint,
        &prepared.file_set_fingerprint,
        commit_uuid,
        deadline,
    )?;
    let observed_files = added_data_files_for_snapshot(table, snapshot, deadline).await?;
    let observed_paths = observed_files
        .iter()
        .map(|file| file.file_path().to_string())
        .collect::<HashSet<_>>();
    if observed_paths != prepared.expected_paths {
        return Err(ConnectorError::TransactionError(
            "Iceberg snapshot summary matched but its exact added data-file set did not".into(),
        ));
    }
    if data_file_set_fingerprint(&observed_files)? != prepared.file_set_fingerprint {
        return Err(ConnectorError::TransactionError(
            "Iceberg snapshot contains data-file metadata different from its descriptor set".into(),
        ));
    }
    Ok(())
}

fn validate_expected_record(
    record: &CursorRecord,
    expected: CoordinatedCommitCursor,
    batch_fingerprint: &str,
    file_set_fingerprint: &str,
    commit_uuid: uuid::Uuid,
) -> Result<(), ConnectorError> {
    if record.cursor.checkpoint_id < expected.checkpoint_id {
        return Err(ConnectorError::TransactionError(format!(
            "Iceberg cursor is at checkpoint {}, expected {}",
            record.cursor.checkpoint_id, expected.checkpoint_id
        )));
    }
    if record.cursor == expected
        && (record.batch_fingerprint != batch_fingerprint
            || record.file_set_fingerprint != file_set_fingerprint
            || record.commit_uuid != commit_uuid.to_string())
    {
        return Err(ConnectorError::TransactionError(
            "Iceberg target checkpoint exists with a different fingerprint or commit UUID".into(),
        ));
    }
    Ok(())
}

async fn verify_cursor_record(
    table: &iceberg::table::Table,
    external_key: &str,
    record: &CursorRecord,
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    if record.file_set_fingerprint == file_set_fingerprint(&[])? {
        return Ok(());
    }
    let commit_uuid = uuid::Uuid::parse_str(&record.commit_uuid).map_err(|_| {
        ConnectorError::TransactionError("Iceberg cursor commit UUID is malformed".into())
    })?;
    let snapshot = find_exact_snapshot(
        table,
        external_key,
        record.cursor,
        &record.batch_fingerprint,
        &record.file_set_fingerprint,
        commit_uuid,
        deadline,
    )?;
    let observed = added_data_files_for_snapshot(table, snapshot, deadline).await?;
    if data_file_set_fingerprint(&observed)? != record.file_set_fingerprint {
        return Err(ConnectorError::TransactionError(
            "Iceberg cursor snapshot does not match its recorded data-file set".into(),
        ));
    }
    Ok(())
}

pub(super) fn find_exact_snapshot<'a>(
    table: &'a iceberg::table::Table,
    external_key: &str,
    cursor: CoordinatedCommitCursor,
    exact_batch_fingerprint: &str,
    file_set_fingerprint: &str,
    commit_uuid: uuid::Uuid,
    deadline: tokio::time::Instant,
) -> Result<&'a iceberg::spec::SnapshotRef, ConnectorError> {
    let metadata = table.metadata();
    let checkpoint_id = cursor.checkpoint_id.to_string();
    let fencing_token = cursor.fencing_token.to_string();
    let commit_uuid = commit_uuid.to_string();
    let mut snapshot = metadata.current_snapshot();
    for _ in 0..metadata.snapshots().count() {
        ensure_deadline(deadline, "snapshot-history reconciliation")?;
        let Some(current) = snapshot else {
            break;
        };
        let properties = &current.summary().additional_properties;
        let matches = properties.get(SUMMARY_NAMESPACE).map(String::as_str) == Some(external_key)
            && properties.get(SUMMARY_CHECKPOINT).map(String::as_str)
                == Some(checkpoint_id.as_str())
            && properties.get(SUMMARY_FENCE).map(String::as_str) == Some(fencing_token.as_str())
            && properties
                .get(SUMMARY_BATCH_FINGERPRINT)
                .map(String::as_str)
                == Some(exact_batch_fingerprint)
            && properties.get(SUMMARY_FILE_SET).map(String::as_str) == Some(file_set_fingerprint)
            && properties.get(SUMMARY_COMMIT_UUID).map(String::as_str)
                == Some(commit_uuid.as_str());
        if matches {
            return Ok(current);
        }
        snapshot = match current.parent_snapshot_id() {
            Some(parent_id) => Some(metadata.snapshot_by_id(parent_id).ok_or_else(|| {
                ConnectorError::TransactionError(
                    "Iceberg current snapshot lineage references expired history".into(),
                )
            })?),
            None => None,
        };
    }
    Err(ConnectorError::TransactionError(
        "exact Iceberg snapshot summary is absent from the current snapshot lineage".into(),
    ))
}

async fn added_data_files_for_snapshot(
    table: &iceberg::table::Table,
    snapshot: &iceberg::spec::SnapshotRef,
    deadline: tokio::time::Instant,
) -> Result<Vec<DataFile>, ConnectorError> {
    ensure_deadline(deadline, "publication manifest reconciliation")?;
    #[cfg(test)]
    crate::lakehouse::iceberg::fault_injection::fail_if(
        crate::lakehouse::iceberg::fault_injection::IcebergFaultPoint::DuringManifestReconciliation,
    )?;
    let limits = ManifestReadLimits::fixed();
    let manifest_list = load_manifest_list(table, snapshot, limits, deadline).await?;
    let mut paths = HashSet::new();
    let mut files = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.added_snapshot_id != snapshot.snapshot_id() {
            continue;
        }
        let manifest = load_manifest(table, manifest_file, limits, deadline).await?;
        collect_added_files(
            snapshot.snapshot_id(),
            manifest.entries(),
            &mut paths,
            &mut files,
        )?;
    }
    files.sort_by(|left, right| left.file_path().cmp(right.file_path()));
    Ok(files)
}

fn collect_added_files(
    snapshot_id: i64,
    entries: &[Arc<iceberg::spec::ManifestEntry>],
    paths: &mut HashSet<String>,
    files: &mut Vec<DataFile>,
) -> Result<(), ConnectorError> {
    for entry in entries {
        if entry.snapshot_id() != Some(snapshot_id) {
            continue;
        }
        if entry.status() == ManifestStatus::Deleted {
            return Err(ConnectorError::TransactionError(
                "Iceberg append snapshot contains a removed file".into(),
            ));
        }
        if entry.status() != ManifestStatus::Added {
            continue;
        }
        if entry.content_type() != DataContentType::Data {
            return Err(ConnectorError::TransactionError(
                "Iceberg append snapshot contains an added delete file".into(),
            ));
        }
        if files.len() == ICEBERG_MAX_FILES_PER_CHECKPOINT {
            return Err(ConnectorError::TransactionError(format!(
                "Iceberg publication snapshot exceeds the {ICEBERG_MAX_FILES_PER_CHECKPOINT}-file verification limit"
            )));
        }
        if !paths.insert(entry.file_path().to_string()) {
            return Err(ConnectorError::TransactionError(format!(
                "Iceberg publication snapshot repeats data file '{}'",
                entry.file_path()
            )));
        }
        files.push(entry.data_file().clone());
    }
    Ok(())
}
