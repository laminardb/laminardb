//! Atomic `FastAppend` publication and outcome reconciliation.

mod identity;
mod preflight;
mod reconciliation;

pub(super) use reconciliation::read_committed_cursor;
use reconciliation::reconcile_exact_publication;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use iceberg::spec::{DataContentType, DataFile};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, ErrorKind, TableIdent};

use crate::connector::{CoordinatedCommitBatch, CoordinatedCommitContext, CoordinatedCommitCursor};
use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::{
    stable_catalog_identity, IcebergSinkConfig, ICEBERG_MAX_FILES_PER_CHECKPOINT,
};
use crate::lakehouse::iceberg_io::{
    build_publication_catalog, external_error_summary, validate_loaded_table_locations,
    CatalogCapabilities, CatalogSession, SingleDispatchCatalog,
};

use super::commit_cursor::{cursor_property_keys, cursor_record, CursorRecord};
use super::descriptor::{IcebergCommitDescriptorV1, IcebergTableBindingV1};
use super::metrics::IcebergMetrics;
use identity::{
    data_file_set_fingerprint, deterministic_commit_uuid, deterministic_idempotency_key,
    file_set_fingerprint, hex, summary_properties, PublicationIdentity, SUMMARY_BATCH_FINGERPRINT,
    SUMMARY_CHECKPOINT, SUMMARY_COMMIT_UUID, SUMMARY_FENCE, SUMMARY_FILE_SET, SUMMARY_NAMESPACE,
};
use preflight::validate_data_file_objects;

const MAX_PUBLICATION_ATTEMPTS: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct UnresolvedIcebergPublication {
    pub(super) external_key: String,
    pub(super) target: CoordinatedCommitCursor,
    pub(super) exact_batch_fingerprint: [u8; 32],
    pub(super) expected_file_set_fingerprint: String,
}

impl UnresolvedIcebergPublication {
    pub(super) fn reconciled_by(&self, cursor: Option<CoordinatedCommitCursor>) -> bool {
        cursor == Some(self.target)
    }
}

pub(super) fn unresolved_publication(
    config: &IcebergSinkConfig,
    batch: &CoordinatedCommitBatch,
) -> Result<UnresolvedIcebergPublication, ConnectorError> {
    batch.validate_shape().map_err(|error| {
        ConnectorError::TransactionError(format!(
            "Iceberg coordinated batch validation failed: {error}"
        ))
    })?;
    let descriptor_limit = config
        .max_descriptor_bytes
        .min(crate::connector::MAX_COORDINATED_COMMIT_PAYLOAD_BYTES);
    let file_limit = config
        .max_files_per_checkpoint
        .min(ICEBERG_MAX_FILES_PER_CHECKPOINT);
    let mut files = Vec::new();
    let mut paths = HashSet::new();
    for entry in &batch.entries {
        let Some(payload) = &entry.payload else {
            continue;
        };
        if payload.len() > descriptor_limit {
            return Err(ConnectorError::TransactionError(format!(
                "Iceberg participant descriptor is {} bytes; configured limit is {descriptor_limit}",
                payload.len()
            )));
        }
        let descriptor = IcebergCommitDescriptorV1::decode(payload)?;
        if descriptor.deployment_id != batch.namespace.deployment_id
            || descriptor.sink_id != batch.namespace.sink_id
            || descriptor.participant_id != entry.participant_id
            || descriptor.epoch_id != entry.attempt.epoch
        {
            return Err(ConnectorError::TransactionError(
                "Iceberg descriptor runtime identity does not match its coordinated entry".into(),
            ));
        }
        let projected = files
            .len()
            .checked_add(descriptor.files.len())
            .ok_or_else(|| {
                ConnectorError::TransactionError("Iceberg aggregate file count overflow".into())
            })?;
        if projected > file_limit {
            return Err(ConnectorError::TransactionError(format!(
                "Iceberg coordinated publication exceeds the {file_limit}-file checkpoint limit"
            )));
        }
        for file in descriptor.files {
            if !paths.insert(file.path.clone()) {
                return Err(ConnectorError::TransactionError(
                    "Iceberg coordinated descriptors repeat a data file".into(),
                ));
            }
            files.push(file);
        }
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(UnresolvedIcebergPublication {
        external_key: batch.namespace.external_key(),
        target: CoordinatedCommitCursor {
            checkpoint_id: batch.target.checkpoint_id,
            fencing_token: batch.fencing_token,
        },
        exact_batch_fingerprint: batch.exact_fingerprint(),
        expected_file_set_fingerprint: file_set_fingerprint(&files),
    })
}

#[derive(Debug)]
struct PreparedPublication {
    binding: IcebergTableBindingV1,
    data_files: Vec<DataFile>,
    expected_paths: HashSet<String>,
    file_set_fingerprint: String,
}

pub(super) async fn publish_coordinated(
    catalog: &Arc<dyn Catalog>,
    catalog_capabilities: &CatalogCapabilities,
    catalog_session: &CatalogSession,
    config: &IcebergSinkConfig,
    batch: &CoordinatedCommitBatch,
    context: CoordinatedCommitContext,
    metrics: &IcebergMetrics,
) -> Result<(), ConnectorError> {
    batch.validate_shape().map_err(|error| {
        ConnectorError::TransactionError(format!(
            "Iceberg coordinated batch validation failed: {error}"
        ))
    })?;
    ensure_deadline(context.deadline(), "batch admission")?;
    let (table, prepared, identity, already_published) =
        admit_publication(catalog, config, batch, context.deadline()).await?;
    if already_published {
        return reconcile_exact_publication(
            &table,
            batch,
            &prepared,
            &identity.exact_batch_hex,
            identity.commit_uuid,
            context.deadline(),
        )
        .await
        .map_err(|error| {
            metrics.unknown_outcomes.inc();
            ConnectorError::outcome_unknown(
                format!("existing Iceberg cursor could not be proven exact: {error}"),
                true,
            )
        });
    }
    let started = std::time::Instant::now();
    let result = publish_with_retries(
        catalog,
        catalog_capabilities,
        catalog_session,
        config,
        batch,
        context,
        metrics,
        table,
        &prepared,
        &identity,
    )
    .await;
    if result.is_ok() {
        metrics
            .publication_duration
            .observe(started.elapsed().as_secs_f64());
        record_success(metrics, identity.target.checkpoint_id);
    }
    result
}

async fn admit_publication(
    catalog: &Arc<dyn Catalog>,
    config: &IcebergSinkConfig,
    batch: &CoordinatedCommitBatch,
    deadline: tokio::time::Instant,
) -> Result<
    (
        iceberg::table::Table,
        PreparedPublication,
        PublicationIdentity,
        bool,
    ),
    ConnectorError,
> {
    let table = load_table_until(catalog, config, deadline, false).await?;
    let prepared = prepare_publication(config, batch, &table)?;
    validate_table_binding(config, &prepared.binding, &table)?;
    let exact_batch_fingerprint = batch.exact_fingerprint();
    let identity = PublicationIdentity {
        exact_batch_hex: hex(&exact_batch_fingerprint),
        external_key: batch.namespace.external_key(),
        commit_uuid: deterministic_commit_uuid(batch, &exact_batch_fingerprint),
        target: CoordinatedCommitCursor {
            checkpoint_id: batch.target.checkpoint_id,
            fencing_token: batch.fencing_token,
        },
    };
    let existing = cursor_record(&table, &identity.external_key)?;
    let already_published = validate_publication_cursor(batch, existing.as_ref())?;
    Ok((table, prepared, identity, already_published))
}

fn validate_publication_cursor(
    batch: &CoordinatedCommitBatch,
    existing: Option<&CursorRecord>,
) -> Result<bool, ConnectorError> {
    batch
        .validate_observed_cursor(existing.map(|record| record.cursor))
        .map_err(|error| {
            ConnectorError::TransactionError(format!(
                "Iceberg coordinated cursor validation failed: {error}"
            ))
        })?;
    let already_published =
        existing.is_some_and(|record| record.cursor.checkpoint_id >= batch.target.checkpoint_id);
    let exact_predecessor = existing.map(|record| record.cursor)
        == Some(batch.expected_predecessor)
        || (existing.is_none() && batch.expected_predecessor.checkpoint_id == 0);
    if !already_published && !exact_predecessor {
        return Err(ConnectorError::TransactionError(
            "Iceberg external cursor is not the exact expected predecessor".into(),
        ));
    }
    Ok(already_published)
}

#[allow(clippy::too_many_arguments)]
async fn publish_with_retries(
    catalog: &Arc<dyn Catalog>,
    catalog_capabilities: &CatalogCapabilities,
    catalog_session: &CatalogSession,
    config: &IcebergSinkConfig,
    batch: &CoordinatedCommitBatch,
    context: CoordinatedCommitContext,
    metrics: &IcebergMetrics,
    mut table: iceberg::table::Table,
    prepared: &PreparedPublication,
    identity: &PublicationIdentity,
) -> Result<(), ConnectorError> {
    for attempt in 0..MAX_PUBLICATION_ATTEMPTS {
        ensure_deadline(context.deadline(), "catalog publication")?;
        if attempt > 0 {
            metrics.commit_retries.inc();
            match refresh_after_conflict(
                catalog,
                config,
                batch,
                context.deadline(),
                metrics,
                prepared,
                identity,
            )
            .await?
            {
                None => return Ok(()),
                Some(refreshed) => table = refreshed,
            }
        }

        let preflight_deadline = commit_deadline(context, config.catalog.commit_timeout);
        validate_data_file_objects(&table, &prepared.data_files, preflight_deadline).await?;

        let attempt_catalog = publication_catalog_for_attempt(
            catalog,
            catalog_capabilities,
            catalog_session,
            config,
            batch,
            identity.commit_uuid,
            attempt,
        )
        .await?;
        let commit_catalog = attempt_catalog.as_ref().unwrap_or(catalog);
        let operation = commit_once(
            commit_catalog.as_ref(),
            &table,
            batch,
            prepared,
            &identity.external_key,
            &identity.exact_batch_hex,
            identity.commit_uuid,
        );
        let commit_deadline = commit_deadline(context, config.catalog.commit_timeout);
        let result = tokio::time::timeout_at(commit_deadline, operation).await;
        match result {
            Ok(Ok(updated)) => {
                reconcile_successful_commit(
                    &updated,
                    batch,
                    prepared,
                    identity,
                    context.deadline(),
                    metrics,
                )
                .await?;
                return Ok(());
            }
            Ok(Err(error)) if error.kind() == ErrorKind::CatalogCommitConflicts => {
                metrics.commit_conflicts.inc();
                if attempt + 1 == MAX_PUBLICATION_ATTEMPTS {
                    return Err(ConnectorError::WriteError(format!(
                        "Iceberg catalog commit conflict after {MAX_PUBLICATION_ATTEMPTS} bounded attempts ({})",
                        external_error_summary(&error)
                    )));
                }
                jitter_before_retry(context.deadline(), attempt).await?;
            }
            Ok(Err(error)) if commit_outcome_may_be_unknown(&error) => {
                metrics.unknown_outcomes.inc();
                return reconcile_after_unknown(
                    catalog,
                    config,
                    batch,
                    prepared,
                    &identity.exact_batch_hex,
                    identity.commit_uuid,
                    context.deadline(),
                    metrics,
                    format!("catalog commit returned {}", external_error_summary(&error)),
                )
                .await;
            }
            Ok(Err(error)) => return Err(classify_rejected_commit(&error)),
            Err(_) => {
                metrics.unknown_outcomes.inc();
                return reconcile_after_unknown(
                    catalog,
                    config,
                    batch,
                    prepared,
                    &identity.exact_batch_hex,
                    identity.commit_uuid,
                    context.deadline(),
                    metrics,
                    "catalog commit exceeded its bounded dispatch deadline".into(),
                )
                .await;
            }
        }
    }
    Err(ConnectorError::Internal(
        "Iceberg publication retry loop exited unexpectedly".into(),
    ))
}

async fn reconcile_successful_commit(
    table: &iceberg::table::Table,
    batch: &CoordinatedCommitBatch,
    prepared: &PreparedPublication,
    identity: &PublicationIdentity,
    deadline: tokio::time::Instant,
    metrics: &IcebergMetrics,
) -> Result<(), ConnectorError> {
    reconcile_exact_publication(
        table,
        batch,
        prepared,
        &identity.exact_batch_hex,
        identity.commit_uuid,
        deadline,
    )
    .await
    .map_err(|error| {
        metrics.unknown_outcomes.inc();
        ConnectorError::outcome_unknown(
            format!(
                "Iceberg commit returned success but exact publication verification failed: {error}"
            ),
            true,
        )
    })
}

async fn refresh_after_conflict(
    catalog: &Arc<dyn Catalog>,
    config: &IcebergSinkConfig,
    batch: &CoordinatedCommitBatch,
    deadline: tokio::time::Instant,
    metrics: &IcebergMetrics,
    prepared: &PreparedPublication,
    identity: &PublicationIdentity,
) -> Result<Option<iceberg::table::Table>, ConnectorError> {
    let table = load_table_until(catalog, config, deadline, false).await?;
    validate_table_binding(config, &prepared.binding, &table)?;
    let existing = cursor_record(&table, &identity.external_key)?;
    if !validate_publication_cursor(batch, existing.as_ref())? {
        return Ok(Some(table));
    }
    reconcile_exact_publication(
        &table,
        batch,
        prepared,
        &identity.exact_batch_hex,
        identity.commit_uuid,
        deadline,
    )
    .await
    .map_err(|error| {
        metrics.unknown_outcomes.inc();
        ConnectorError::outcome_unknown(
            format!("conflict refresh found an unprovable Iceberg cursor: {error}"),
            true,
        )
    })?;
    Ok(None)
}

async fn publication_catalog_for_attempt(
    catalog: &Arc<dyn Catalog>,
    capabilities: &CatalogCapabilities,
    session: &CatalogSession,
    config: &IcebergSinkConfig,
    batch: &CoordinatedCommitBatch,
    logical_commit_uuid: uuid::Uuid,
    attempt: usize,
) -> Result<Option<Arc<dyn Catalog>>, ConnectorError> {
    if capabilities.idempotency_key_lifetime.is_none() {
        return Ok(None);
    }
    // RECOVERY: only a definite conflict changes the request and its key.
    let idempotency_key = deterministic_idempotency_key(batch, logical_commit_uuid, attempt)?;
    build_publication_catalog(
        Arc::clone(catalog),
        &config.catalog,
        &config.storage,
        capabilities,
        session,
        idempotency_key,
    )
    .await
}

fn prepare_publication(
    config: &IcebergSinkConfig,
    batch: &CoordinatedCommitBatch,
    table: &iceberg::table::Table,
) -> Result<PreparedPublication, ConnectorError> {
    let mut binding: Option<IcebergTableBindingV1> = None;
    let mut data_files = Vec::new();
    let mut expected_paths = HashSet::new();
    let descriptor_limit = config
        .max_descriptor_bytes
        .min(crate::connector::MAX_COORDINATED_COMMIT_PAYLOAD_BYTES);
    let file_limit = config
        .max_files_per_checkpoint
        .min(ICEBERG_MAX_FILES_PER_CHECKPOINT);
    for entry in &batch.entries {
        let Some(payload) = &entry.payload else {
            continue;
        };
        if payload.len() > descriptor_limit {
            return Err(ConnectorError::TransactionError(format!(
                "Iceberg participant descriptor is {} bytes; configured limit is {descriptor_limit}",
                payload.len()
            )));
        }
        let descriptor = IcebergCommitDescriptorV1::decode(payload)?;
        if descriptor.deployment_id != batch.namespace.deployment_id
            || descriptor.sink_id != batch.namespace.sink_id
            || descriptor.participant_id != entry.participant_id
            || descriptor.epoch_id != entry.attempt.epoch
        {
            return Err(ConnectorError::TransactionError(
                "Iceberg descriptor runtime identity does not match its coordinated entry".into(),
            ));
        }
        if descriptor.table.catalog_identity
            != stable_catalog_identity(&config.catalog, &config.storage)
        {
            return Err(ConnectorError::TransactionError(
                "Iceberg descriptor catalog identity differs from the configured catalog".into(),
            ));
        }
        match &binding {
            Some(expected) if !expected.has_same_append_target(&descriptor.table) => {
                return Err(ConnectorError::TransactionError(
                    "Iceberg descriptors bind different tables, refs, schemas, specs, or sort orders"
                        .into(),
                ));
            }
            None => binding = Some(descriptor.table.clone()),
            Some(_) => {}
        }
        let decoded = descriptor.decode_data_files(table)?;
        let projected = data_files.len().checked_add(decoded.len()).ok_or_else(|| {
            ConnectorError::TransactionError("Iceberg aggregate file count overflow".into())
        })?;
        if projected > file_limit {
            return Err(ConnectorError::TransactionError(format!(
                "Iceberg coordinated publication exceeds the {file_limit}-file checkpoint limit"
            )));
        }
        for file in decoded {
            if file.content_type() != DataContentType::Data {
                return Err(ConnectorError::TransactionError(
                    "Iceberg append descriptor contains a delete file".into(),
                ));
            }
            if !expected_paths.insert(file.file_path().to_string()) {
                return Err(ConnectorError::TransactionError(
                    "Iceberg coordinated descriptors repeat a data file".into(),
                ));
            }
            data_files.push(file);
        }
    }
    data_files.sort_by(|left, right| left.file_path().cmp(right.file_path()));
    let binding = binding.unwrap_or_else(|| IcebergTableBindingV1::from_table(table, config));
    let file_set_fingerprint = data_file_set_fingerprint(&data_files);
    Ok(PreparedPublication {
        binding,
        data_files,
        expected_paths,
        file_set_fingerprint,
    })
}

fn validate_table_binding(
    config: &IcebergSinkConfig,
    binding: &IcebergTableBindingV1,
    table: &iceberg::table::Table,
) -> Result<(), ConnectorError> {
    let metadata = table.metadata();
    let mismatch = binding.catalog_implementation != config.catalog.catalog_type.to_string()
        || binding.catalog_identity != stable_catalog_identity(&config.catalog, &config.storage)
        || binding.table_uuid != metadata.uuid().to_string()
        || binding.table_identifier != table.identifier().to_string()
        || binding.table_location != metadata.location()
        || binding.table_ref != config.table_ref
        || binding.schema_id != metadata.current_schema_id()
        || binding.partition_spec_id != metadata.default_partition_spec_id()
        || binding.sort_order_id != metadata.default_sort_order_id()
        || binding.format_version
            != super::descriptor::format_version_number(metadata.format_version());
    if mismatch {
        return Err(ConnectorError::TransactionError(
            "Iceberg table UUID, ref, schema, partition spec, sort order, or catalog binding changed before publication"
                .into(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn commit_once(
    catalog: &dyn Catalog,
    table: &iceberg::table::Table,
    batch: &CoordinatedCommitBatch,
    prepared: &PreparedPublication,
    external_key: &str,
    exact_batch_fingerprint: &str,
    logical_commit_uuid: uuid::Uuid,
) -> iceberg::Result<iceberg::table::Table> {
    let target = CoordinatedCommitCursor {
        checkpoint_id: batch.target.checkpoint_id,
        fencing_token: batch.fencing_token,
    };
    let keys = cursor_property_keys(external_key);
    let tx = Transaction::new(table);
    let properties = tx
        .update_table_properties()
        .set(keys.checkpoint, target.checkpoint_id.to_string())
        .set(keys.fence, target.fencing_token.to_string())
        .set(keys.batch_fingerprint, exact_batch_fingerprint.to_string())
        .set(keys.file_set, prepared.file_set_fingerprint.clone())
        .set(keys.commit_uuid, logical_commit_uuid.to_string());
    let tx = properties.apply(tx)?;
    let tx = if prepared.data_files.is_empty() {
        tx
    } else {
        // RECOVERY: Iceberg uses this UUID in manifest paths, so every dispatch needs a
        // distinct physical namespace. The replay-stable logical UUID stays in the commit state.
        let manifest_commit_uuid = uuid::Uuid::now_v7();
        tx.fast_append()
            .set_commit_uuid(manifest_commit_uuid)
            .set_snapshot_properties(summary_properties(
                batch,
                external_key,
                exact_batch_fingerprint,
                &prepared.file_set_fingerprint,
                logical_commit_uuid,
            ))
            .add_data_files(prepared.data_files.clone())
            .apply(tx)?
    };
    #[cfg(test)]
    if super::fault_injection::hit(super::fault_injection::IcebergFaultPoint::BeforeCatalogCommit) {
        return Err(iceberg::Error::new(
            ErrorKind::DataInvalid,
            "injected failure before catalog commit dispatch",
        ));
    }
    let single_dispatch = SingleDispatchCatalog::new(catalog, table);
    let updated = tx.commit(&single_dispatch).await?;
    #[cfg(test)]
    if super::fault_injection::hit(super::fault_injection::IcebergFaultPoint::AfterCatalogCommit) {
        return Err(iceberg::Error::new(
            ErrorKind::Unexpected,
            "injected response loss after applied catalog commit",
        )
        .with_retryable(true));
    }
    Ok(updated)
}

async fn reconcile_after_unknown(
    catalog: &Arc<dyn Catalog>,
    config: &IcebergSinkConfig,
    batch: &CoordinatedCommitBatch,
    prepared: &PreparedPublication,
    exact_batch_fingerprint: &str,
    commit_uuid: uuid::Uuid,
    deadline: tokio::time::Instant,
    metrics: &IcebergMetrics,
    cause: String,
) -> Result<(), ConnectorError> {
    let started = std::time::Instant::now();
    let result = load_table_until(catalog, config, deadline, true).await;
    metrics
        .reconciliation_duration
        .observe(started.elapsed().as_secs_f64());
    let table = match result {
        Ok(table) => table,
        Err(error) => {
            return Err(ConnectorError::outcome_unknown(
                format!("{cause}; metadata reconciliation failed: {error}"),
                true,
            ));
        }
    };
    match reconcile_exact_publication(
        &table,
        batch,
        prepared,
        exact_batch_fingerprint,
        commit_uuid,
        deadline,
    )
    .await
    {
        Ok(()) => {
            record_success(metrics, batch.target.checkpoint_id);
            Ok(())
        }
        Err(error) => Err(ConnectorError::outcome_unknown(
            format!("{cause}; exact Iceberg publication was not proven: {error}"),
            true,
        )),
    }
}

async fn load_table_until(
    catalog: &Arc<dyn Catalog>,
    config: &IcebergSinkConfig,
    deadline: tokio::time::Instant,
    unknown_context: bool,
) -> Result<iceberg::table::Table, ConnectorError> {
    ensure_deadline(deadline, "table metadata refresh")?;
    #[cfg(test)]
    if super::fault_injection::hit(super::fault_injection::IcebergFaultPoint::DuringMetadataRefresh)
    {
        return if unknown_context {
            Err(ConnectorError::outcome_unknown(
                "injected metadata refresh failure after ambiguous Iceberg publication",
                true,
            ))
        } else {
            Err(ConnectorError::ReadError(
                "injected Iceberg metadata refresh failure".into(),
            ))
        };
    }
    let ident = table_ident(config)?;
    match tokio::time::timeout_at(deadline, catalog.load_table(&ident)).await {
        Ok(Ok(table)) => match validate_loaded_table_locations(&table) {
            Ok(()) => Ok(table),
            Err(error) if unknown_context => Err(ConnectorError::outcome_unknown(
                format!("Iceberg metadata refresh after ambiguous publication was unsafe: {error}"),
                false,
            )),
            Err(error) => Err(error),
        },
        Ok(Err(error)) if unknown_context => Err(ConnectorError::outcome_unknown(
            format!(
                "Iceberg metadata refresh after ambiguous publication failed ({})",
                external_error_summary(&error)
            ),
            true,
        )),
        Ok(Err(error)) => Err(ConnectorError::ReadError(format!(
            "refresh Iceberg table metadata ({})",
            external_error_summary(&error)
        ))),
        Err(_) if unknown_context => Err(ConnectorError::outcome_unknown(
            "Iceberg metadata refresh after ambiguous publication exceeded its deadline",
            true,
        )),
        Err(_) => Err(ConnectorError::Timeout(
            u64::try_from(config.catalog.request_timeout.as_millis()).unwrap_or(u64::MAX),
        )),
    }
}

fn table_ident(config: &IcebergSinkConfig) -> Result<TableIdent, ConnectorError> {
    let namespace = iceberg::NamespaceIdent::from_strs(config.catalog.namespace.split('.'))
        .map_err(|error| {
            ConnectorError::ConfigurationError(format!("invalid Iceberg namespace: {error}"))
        })?;
    Ok(TableIdent::new(
        namespace,
        config.catalog.table_name.clone(),
    ))
}

fn commit_deadline(
    context: CoordinatedCommitContext,
    configured_timeout: Duration,
) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    let remaining = context.remaining();
    let reserve = (remaining / 4).min(Duration::from_secs(5));
    let outer_dispatch = context.deadline().checked_sub(reserve).unwrap_or(now);
    (now + configured_timeout).min(outer_dispatch.max(now))
}

async fn jitter_before_retry(
    deadline: tokio::time::Instant,
    attempt: usize,
) -> Result<(), ConnectorError> {
    let base_ms = 25u64.saturating_mul(1u64 << attempt.min(6));
    let jitter = rand::random_range(0..=base_ms);
    let delay = Duration::from_millis(base_ms.saturating_add(jitter));
    if tokio::time::Instant::now() + delay >= deadline {
        return Err(ConnectorError::WriteError(
            "Iceberg conflict retry deadline exhausted".into(),
        ));
    }
    tokio::time::sleep(delay).await;
    Ok(())
}

fn commit_outcome_may_be_unknown(error: &iceberg::Error) -> bool {
    error.kind() == ErrorKind::Unexpected || error.retryable()
}

fn classify_rejected_commit(error: &iceberg::Error) -> ConnectorError {
    ConnectorError::TransactionError(format!(
        "Iceberg catalog rejected coordinated commit ({})",
        external_error_summary(error)
    ))
}

fn ensure_deadline(deadline: tokio::time::Instant, operation: &str) -> Result<(), ConnectorError> {
    if deadline <= tokio::time::Instant::now() {
        Err(ConnectorError::TransactionError(format!(
            "Iceberg coordinated publication deadline elapsed during {operation}"
        )))
    } else {
        Ok(())
    }
}

fn record_success(metrics: &IcebergMetrics, checkpoint_id: u64) {
    metrics
        .committed_checkpoint
        .set(i64::try_from(checkpoint_id).unwrap_or(i64::MAX));
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    metrics
        .last_successful_commit_timestamp
        .set(i64::try_from(timestamp).unwrap_or(i64::MAX));
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    use laminar_core::checkpoint::CheckpointAttempt;

    use crate::connector::{CoordinatedCommitNamespace, CoordinatedCommitPayload};
    use crate::lakehouse::iceberg::descriptor::IcebergCommitDescriptorV1;
    use crate::lakehouse::iceberg::epoch_writer::{EpochIdentity, IcebergEpochWriter};
    use crate::lakehouse::iceberg::test_support::{
        batch as record_batch, create_test_table, table_ident,
    };

    use super::*;

    const NO_CATALOG_CAPABILITIES: CatalogCapabilities = CatalogCapabilities {
        idempotency_key_lifetime: None,
    };

    fn batch() -> CoordinatedCommitBatch {
        let target = CheckpointAttempt::canonical(9);
        CoordinatedCommitBatch {
            namespace: CoordinatedCommitNamespace::try_new(
                PipelineIdentity::empty(),
                "018f0000-0000-7000-8000-000000000001",
                "orders",
            )
            .unwrap(),
            expected_predecessor: CoordinatedCommitCursor {
                checkpoint_id: 8,
                fencing_token: 2,
            },
            fencing_token: 3,
            target,
            entries: vec![CoordinatedCommitPayload {
                attempt: target,
                participant_id: 1,
                payload: None,
            }],
        }
    }

    fn namespace() -> CoordinatedCommitNamespace {
        CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "018f0000-0000-7000-8000-000000000001",
            "orders",
        )
        .unwrap()
    }

    fn commit_batch(
        checkpoint_id: u64,
        predecessor: CoordinatedCommitCursor,
        payloads: Vec<(u64, Option<Vec<u8>>)>,
    ) -> CoordinatedCommitBatch {
        let target = CheckpointAttempt::canonical(checkpoint_id);
        CoordinatedCommitBatch {
            namespace: namespace(),
            expected_predecessor: predecessor,
            fencing_token: 7,
            target,
            entries: payloads
                .into_iter()
                .map(|(participant_id, payload)| CoordinatedCommitPayload {
                    attempt: target,
                    participant_id,
                    payload,
                })
                .collect(),
        }
    }

    async fn descriptor(
        table: &iceberg::table::Table,
        config: &IcebergSinkConfig,
        participant_id: u64,
        checkpoint_id: u64,
        rows: &[(i64, Option<&str>)],
    ) -> Vec<u8> {
        let identity = EpochIdentity {
            deployment_id: namespace().deployment_id,
            sink_id: "orders".into(),
            participant_id,
            epoch: checkpoint_id,
        };
        let mut coordinated_config = config.clone();
        coordinated_config.delivery_guarantee = crate::connector::DeliveryGuarantee::ExactlyOnce;
        let mut writer = IcebergEpochWriter::new(
            table,
            &coordinated_config,
            &identity,
            IcebergMetrics::new(None),
        )
        .unwrap();
        writer.write(record_batch(table, rows)).await.unwrap();
        IcebergCommitDescriptorV1::encode(
            table,
            &coordinated_config,
            &identity,
            writer.close().await.unwrap(),
        )
        .unwrap()
    }

    fn table_with_metadata(
        table: &iceberg::table::Table,
        metadata: iceberg::spec::TableMetadata,
    ) -> iceberg::table::Table {
        let mut builder = iceberg::table::Table::builder()
            .identifier(table.identifier().clone())
            .metadata(metadata)
            .file_io(table.file_io().clone())
            .runtime(iceberg::Runtime::try_current().unwrap());
        if let Some(location) = table.metadata_location() {
            builder = builder.metadata_location(location);
        }
        builder.build().unwrap()
    }

    #[test]
    fn logical_commit_uuid_is_replay_stable_and_version_eight() {
        let batch = batch();
        let fingerprint = batch.exact_fingerprint();
        let first = deterministic_commit_uuid(&batch, &fingerprint);
        assert_eq!(first, deterministic_commit_uuid(&batch, &fingerprint));
        assert_eq!(first.get_version_num(), 8);
    }

    #[test]
    fn logical_commit_uuid_changes_with_fence() {
        let first = batch();
        let mut second = first.clone();
        second.fencing_token += 1;
        assert_ne!(
            deterministic_commit_uuid(&first, &first.exact_fingerprint()),
            deterministic_commit_uuid(&second, &second.exact_fingerprint())
        );
    }

    #[test]
    fn rest_idempotency_key_is_stable_uuid_v7_per_attempt() {
        let batch = batch();
        let fingerprint = batch.exact_fingerprint();
        let logical = deterministic_commit_uuid(&batch, &fingerprint);
        let first = deterministic_idempotency_key(&batch, logical, 0).unwrap();
        assert_eq!(
            first,
            deterministic_idempotency_key(&batch, logical, 0).unwrap()
        );
        assert_eq!(first.get_version_num(), 7);
        let deployment = uuid::Uuid::parse_str(&batch.namespace.deployment_id).unwrap();
        assert_eq!(&first.as_bytes()[..6], &deployment.as_bytes()[..6]);
        assert_ne!(
            first,
            deterministic_idempotency_key(&batch, logical, 1).unwrap()
        );

        let mut invalid = batch;
        invalid.namespace.deployment_id = "d66e462a-6af7-4ad6-8c81-b1d053496502".into();
        assert!(deterministic_idempotency_key(&invalid, logical, 0).is_err());
    }

    #[tokio::test]
    async fn replay_publishes_at_most_one_snapshot() {
        use tokio_stream::StreamExt;

        let fixture = create_test_table(false).await;
        let payload = descriptor(
            &fixture.table,
            &fixture.config,
            1,
            1,
            &[(1, Some("a")), (2, Some("b"))],
        )
        .await;
        let batch = commit_batch(
            1,
            CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            },
            vec![(1, Some(payload))],
        );
        let metrics = IcebergMetrics::new(None);
        for _ in 0..2 {
            publish_coordinated(
                &fixture.catalog,
                &NO_CATALOG_CAPABILITIES,
                &CatalogSession::default(),
                &fixture.config,
                &batch,
                CoordinatedCommitContext::new(
                    tokio::time::Instant::now() + Duration::from_secs(10),
                ),
                &metrics,
            )
            .await
            .unwrap();
        }
        let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
        assert_eq!(table.metadata().snapshots().count(), 1);
        assert_eq!(
            cursor_record(&table, &batch.namespace.external_key())
                .unwrap()
                .unwrap()
                .cursor,
            CoordinatedCommitCursor {
                checkpoint_id: 1,
                fencing_token: 7,
            }
        );
        let stream = table
            .scan()
            .select_all()
            .build()
            .unwrap()
            .to_arrow()
            .await
            .unwrap();
        let mut stream = std::pin::pin!(stream);
        let mut rows = 0;
        while let Some(batch) = stream.next().await {
            rows += batch.unwrap().num_rows();
        }
        assert_eq!(
            rows, 2,
            "published Parquet files must be complete and readable"
        );
    }

    #[tokio::test]
    async fn response_loss_after_applied_commit_reconciles_as_success() {
        let fixture = create_test_table(false).await;
        let payload = descriptor(&fixture.table, &fixture.config, 1, 1, &[(1, Some("a"))]).await;
        let batch = commit_batch(
            1,
            CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            },
            vec![(1, Some(payload))],
        );
        let metrics = IcebergMetrics::new(None);
        super::super::fault_injection::scope(
            [super::super::fault_injection::IcebergFault::first(
                super::super::fault_injection::IcebergFaultPoint::AfterCatalogCommit,
            )],
            async {
                publish_coordinated(
                    &fixture.catalog,
                    &NO_CATALOG_CAPABILITIES,
                    &CatalogSession::default(),
                    &fixture.config,
                    &batch,
                    CoordinatedCommitContext::new(
                        tokio::time::Instant::now() + Duration::from_secs(10),
                    ),
                    &metrics,
                )
                .await
            },
        )
        .await
        .unwrap();
        let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
        assert_eq!(table.metadata().snapshots().count(), 1);
        assert_eq!(metrics.unknown_outcomes.get(), 1);
    }

    #[tokio::test]
    async fn missing_data_file_is_rejected_before_catalog_commit() {
        let fixture = create_test_table(false).await;
        let payload = descriptor(&fixture.table, &fixture.config, 1, 1, &[(1, Some("a"))]).await;
        let path = IcebergCommitDescriptorV1::decode(&payload).unwrap().files[0]
            .path
            .clone();
        fixture.table.file_io().delete(&path).await.unwrap();
        let batch = commit_batch(
            1,
            CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            },
            vec![(1, Some(payload))],
        );
        let error = publish_coordinated(
            &fixture.catalog,
            &NO_CATALOG_CAPABILITIES,
            &CatalogSession::default(),
            &fixture.config,
            &batch,
            CoordinatedCommitContext::new(tokio::time::Instant::now() + Duration::from_secs(10)),
            &IcebergMetrics::new(None),
        )
        .await
        .expect_err("a missing participant file must not be published");
        assert!(error
            .to_string()
            .contains("LDB-ICEBERG-DATA-FILE-PREFLIGHT"));
        assert!(!error.to_string().contains(&path));
        let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
        assert_eq!(table.metadata().snapshots().count(), 0);
    }

    #[tokio::test]
    async fn incomplete_data_file_is_rejected_before_catalog_commit() {
        let fixture = create_test_table(false).await;
        let payload = descriptor(&fixture.table, &fixture.config, 1, 1, &[(1, Some("a"))]).await;
        let path = IcebergCommitDescriptorV1::decode(&payload).unwrap().files[0]
            .path
            .clone();
        fixture
            .table
            .file_io()
            .new_output(&path)
            .unwrap()
            .write(bytes::Bytes::from_static(b"incomplete"))
            .await
            .unwrap();
        let batch = commit_batch(
            1,
            CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            },
            vec![(1, Some(payload))],
        );
        let error = publish_coordinated(
            &fixture.catalog,
            &NO_CATALOG_CAPABILITIES,
            &CatalogSession::default(),
            &fixture.config,
            &batch,
            CoordinatedCommitContext::new(tokio::time::Instant::now() + Duration::from_secs(10)),
            &IcebergMetrics::new(None),
        )
        .await
        .expect_err("an incomplete participant file must not be published");
        assert!(error.to_string().contains("LDB-ICEBERG-DATA-FILE-SIZE"));
        let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
        assert_eq!(table.metadata().snapshots().count(), 0);
    }

    #[tokio::test]
    async fn same_size_invalid_parquet_file_is_rejected_before_catalog_commit() {
        let fixture = create_test_table(false).await;
        let payload = descriptor(&fixture.table, &fixture.config, 1, 1, &[(1, Some("a"))]).await;
        let file = IcebergCommitDescriptorV1::decode(&payload).unwrap().files[0].clone();
        let mut corrupt = vec![b'x'; usize::try_from(file.bytes).unwrap()];
        let trailer_magic = corrupt.len() - 4;
        corrupt[..4].copy_from_slice(b"PAR1");
        corrupt[trailer_magic..].copy_from_slice(b"PAR1");
        fixture
            .table
            .file_io()
            .new_output(&file.path)
            .unwrap()
            .write(bytes::Bytes::from(corrupt))
            .await
            .unwrap();
        let batch = commit_batch(
            1,
            CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            },
            vec![(1, Some(payload))],
        );
        let error = publish_coordinated(
            &fixture.catalog,
            &NO_CATALOG_CAPABILITIES,
            &CatalogSession::default(),
            &fixture.config,
            &batch,
            CoordinatedCommitContext::new(tokio::time::Instant::now() + Duration::from_secs(10)),
            &IcebergMetrics::new(None),
        )
        .await
        .expect_err("a same-size invalid Parquet participant file must not be published");
        assert!(error.to_string().contains("LDB-ICEBERG-DATA-FILE-PARQUET"));
        let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
        assert_eq!(table.metadata().snapshots().count(), 0);
    }

    #[tokio::test]
    async fn reconciliation_rejects_a_matching_detached_snapshot() {
        use iceberg::spec::{Operation, Snapshot, Summary};

        let fixture = create_test_table(false).await;
        let payload = descriptor(&fixture.table, &fixture.config, 1, 1, &[(1, Some("a"))]).await;
        let batch = commit_batch(
            1,
            CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            },
            vec![(1, Some(payload))],
        );
        let exact_fingerprint = batch.exact_fingerprint();
        let exact_fingerprint_hex = hex(&exact_fingerprint);
        let logical_commit_uuid = deterministic_commit_uuid(&batch, &exact_fingerprint);
        publish_coordinated(
            &fixture.catalog,
            &NO_CATALOG_CAPABILITIES,
            &CatalogSession::default(),
            &fixture.config,
            &batch,
            CoordinatedCommitContext::new(tokio::time::Instant::now() + Duration::from_secs(10)),
            &IcebergMetrics::new(None),
        )
        .await
        .unwrap();
        let published = fixture.catalog.load_table(&table_ident()).await.unwrap();
        let prepared = prepare_publication(&fixture.config, &batch, &published).unwrap();
        let current = published.metadata().current_snapshot().unwrap();
        let detached_main = Snapshot::builder()
            .with_snapshot_id(current.snapshot_id().wrapping_add(1))
            .with_parent_snapshot_id(None)
            .with_sequence_number(current.sequence_number().saturating_add(1))
            .with_timestamp_ms(current.timestamp_ms().saturating_add(1))
            .with_manifest_list("memory:///unused-detached-manifest-list.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(published.metadata().current_schema_id())
            .build();
        let metadata = published
            .metadata()
            .clone()
            .into_builder(None)
            .set_branch_snapshot(detached_main, "main")
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let detached = table_with_metadata(&published, metadata);

        let error = reconciliation::find_exact_snapshot(
            &detached,
            &batch.namespace.external_key(),
            CoordinatedCommitCursor {
                checkpoint_id: 1,
                fencing_token: 7,
            },
            &exact_fingerprint_hex,
            &prepared.file_set_fingerprint,
            logical_commit_uuid,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .expect_err("detached snapshot evidence must not reconcile the main branch cursor");
        assert!(error.to_string().contains("current snapshot lineage"));
    }

    #[tokio::test]
    async fn empty_checkpoint_advances_table_metadata_without_snapshot() {
        let fixture = create_test_table(false).await;
        let batch = commit_batch(
            1,
            CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            },
            vec![(1, None)],
        );
        publish_coordinated(
            &fixture.catalog,
            &NO_CATALOG_CAPABILITIES,
            &CatalogSession::default(),
            &fixture.config,
            &batch,
            CoordinatedCommitContext::new(tokio::time::Instant::now() + Duration::from_secs(10)),
            &IcebergMetrics::new(None),
        )
        .await
        .unwrap();
        let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
        assert_eq!(table.metadata().snapshots().count(), 0);
        assert_eq!(
            cursor_record(&table, &batch.namespace.external_key())
                .unwrap()
                .unwrap()
                .cursor
                .checkpoint_id,
            1
        );
    }

    #[tokio::test]
    async fn mixed_table_descriptors_are_rejected() {
        let first = create_test_table(false).await;
        let second = create_test_table(false).await;
        let mut second_config = second.config.clone();
        second_config.catalog = first.config.catalog.clone();
        second_config.storage = first.config.storage.clone();
        let first_payload = descriptor(&first.table, &first.config, 1, 1, &[(1, Some("a"))]).await;
        let second_payload =
            descriptor(&second.table, &second_config, 2, 1, &[(2, Some("b"))]).await;
        let batch = commit_batch(
            1,
            CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            },
            vec![(1, Some(first_payload)), (2, Some(second_payload))],
        );
        let error = publish_coordinated(
            &first.catalog,
            &NO_CATALOG_CAPABILITIES,
            &CatalogSession::default(),
            &first.config,
            &batch,
            CoordinatedCommitContext::new(tokio::time::Instant::now() + Duration::from_secs(10)),
            &IcebergMetrics::new(None),
        )
        .await
        .expect_err("mixed table UUIDs must fail closed");
        assert!(error.to_string().contains("bind different tables"));
        let table = first.catalog.load_table(&table_ident()).await.unwrap();
        assert_eq!(table.metadata().snapshots().count(), 0);
    }

    #[tokio::test]
    async fn participants_loaded_across_compatible_append_metadata_are_accepted() {
        let fixture = create_test_table(false).await;
        let first_payload =
            descriptor(&fixture.table, &fixture.config, 1, 1, &[(1, Some("a"))]).await;

        let outside_identity = EpochIdentity {
            deployment_id: "018f0000-0000-7000-8000-000000000099".into(),
            sink_id: "outside-writer".into(),
            participant_id: 9,
            epoch: 1,
        };
        let mut outside = IcebergEpochWriter::new(
            &fixture.table,
            &fixture.config,
            &outside_identity,
            IcebergMetrics::new(None),
        )
        .unwrap();
        outside
            .write(record_batch(&fixture.table, &[(10, Some("outside"))]))
            .await
            .unwrap();
        let refreshed = crate::lakehouse::iceberg_io::commit_data_files_append(
            &fixture.table,
            fixture.catalog.as_ref(),
            outside.close().await.unwrap().data_files,
        )
        .await
        .unwrap();
        let second_payload = descriptor(&refreshed, &fixture.config, 2, 1, &[(2, Some("b"))]).await;
        let batch = commit_batch(
            1,
            CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            },
            vec![(1, Some(first_payload)), (2, Some(second_payload))],
        );

        publish_coordinated(
            &fixture.catalog,
            &NO_CATALOG_CAPABILITIES,
            &CatalogSession::default(),
            &fixture.config,
            &batch,
            CoordinatedCommitContext::new(tokio::time::Instant::now() + Duration::from_secs(10)),
            &IcebergMetrics::new(None),
        )
        .await
        .unwrap();
        let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
        assert_eq!(table.metadata().snapshots().count(), 2);
    }
}
