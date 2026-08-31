use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::{Catalog, ErrorKind};

use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::IcebergSinkConfig;
use crate::lakehouse::iceberg_io::{
    build_publication_catalog, checked_deadline, commit_generated_data_files_append,
    external_error_summary, iceberg_commit_error, AtomicTableRequirements, CatalogCapabilities,
    CatalogSession, GeneratedAppendError, SingleDispatchCatalog,
};

use super::publication::jitter_before_retry;
use super::validate_direct_publication_table;
use super::IcebergMetrics;

const MAX_DIRECT_APPEND_ATTEMPTS: usize = 3;

enum DirectAppendAttemptError {
    Setup(ConnectorError),
    Generated(GeneratedAppendError),
}

pub(super) async fn publish_direct_append(
    config: &IcebergSinkConfig,
    catalog: &Arc<dyn Catalog>,
    capabilities: &CatalogCapabilities,
    session: &CatalogSession,
    writer_table: &Table,
    data_files: Vec<DataFile>,
    metrics: &IcebergMetrics,
) -> Result<Table, ConnectorError> {
    let deadline = checked_deadline(config.catalog.commit_timeout, "catalog.commit_timeout")?;
    let mut current = load_current_table(config, catalog, deadline).await?;

    for attempt in 0..MAX_DIRECT_APPEND_ATTEMPTS {
        if attempt > 0 {
            metrics.commit_retries.inc();
        }
        validate_direct_publication_table(writer_table, &current)?;
        let update_started = AtomicBool::new(false);
        let operation = dispatch_attempt(
            config,
            catalog,
            capabilities,
            session,
            &current,
            data_files.clone(),
            deadline,
            &update_started,
        );
        let result = tokio::time::timeout_at(deadline, operation).await;
        let dispatched = update_started.load(Ordering::Relaxed);
        match result {
            Ok(Ok(updated)) => return Ok(updated),
            Ok(Err(
                DirectAppendAttemptError::Setup(error)
                | DirectAppendAttemptError::Generated(GeneratedAppendError::Preflight(error)),
            )) => return Err(error),
            Ok(Err(DirectAppendAttemptError::Generated(GeneratedAppendError::Commit(error))))
                if dispatched && error.kind() == ErrorKind::CatalogCommitConflicts =>
            {
                metrics.commit_conflicts.inc();
                if attempt + 1 == MAX_DIRECT_APPEND_ATTEMPTS {
                    return Err(ConnectorError::WriteError(format!(
                        "Iceberg direct append conflict after {MAX_DIRECT_APPEND_ATTEMPTS} bounded attempts ({})",
                        external_error_summary(&error)
                    )));
                }
                jitter_before_retry(deadline, attempt).await?;
                current = load_current_table(config, catalog, deadline).await?;
            }
            Ok(Err(DirectAppendAttemptError::Generated(GeneratedAppendError::Commit(error))))
                if dispatched && (error.kind() == ErrorKind::Unexpected || error.retryable()) =>
            {
                metrics.unknown_outcomes.inc();
                return Err(ConnectorError::outcome_unknown(
                    format!(
                        "Iceberg direct append may have applied ({})",
                        external_error_summary(&error)
                    ),
                    true,
                ));
            }
            Ok(Err(DirectAppendAttemptError::Generated(GeneratedAppendError::Commit(error)))) => {
                return Err(iceberg_commit_error(&error))
            }
            Err(_) if dispatched => {
                metrics.unknown_outcomes.inc();
                return Err(ConnectorError::outcome_unknown(
                    "Iceberg direct append exceeded its deadline after catalog dispatch",
                    true,
                ));
            }
            Err(_) => {
                return Err(ConnectorError::WriteError(
                    "Iceberg direct append exceeded its deadline before catalog dispatch".into(),
                ));
            }
        }
    }
    Err(ConnectorError::Internal(
        "Iceberg direct append retry loop exited unexpectedly".into(),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_attempt(
    config: &IcebergSinkConfig,
    catalog: &Arc<dyn Catalog>,
    capabilities: &CatalogCapabilities,
    session: &CatalogSession,
    table: &Table,
    data_files: Vec<DataFile>,
    deadline: tokio::time::Instant,
    update_started: &AtomicBool,
) -> Result<Table, DirectAppendAttemptError> {
    let publication_catalog = build_publication_catalog(
        Arc::clone(catalog),
        &config.catalog,
        &config.storage,
        capabilities,
        session,
        None,
        AtomicTableRequirements::from_table(table),
    )
    .await
    .map_err(DirectAppendAttemptError::Setup)?;
    let catalog = publication_catalog.as_deref().unwrap_or(catalog.as_ref());
    let single_dispatch = SingleDispatchCatalog::new(catalog, table, update_started);
    #[cfg(test)]
    if super::fault_injection::hit(super::fault_injection::IcebergFaultPoint::CatalogCommitConflict)
    {
        update_started.store(true, Ordering::Relaxed);
        return Err(DirectAppendAttemptError::Generated(
            GeneratedAppendError::Commit(
                iceberg::Error::new(
                    ErrorKind::CatalogCommitConflicts,
                    "injected direct append conflict",
                )
                .with_retryable(true),
            ),
        ));
    }
    let result = commit_generated_data_files_append(table, &single_dispatch, data_files, deadline)
        .await
        .map_err(DirectAppendAttemptError::Generated);
    #[cfg(test)]
    if result.is_ok()
        && super::fault_injection::hit(
            super::fault_injection::IcebergFaultPoint::AfterCatalogCommit,
        )
    {
        return Err(DirectAppendAttemptError::Generated(
            GeneratedAppendError::Commit(
                iceberg::Error::new(
                    ErrorKind::Unexpected,
                    "injected direct append response loss",
                )
                .with_retryable(true),
            ),
        ));
    }
    result
}

async fn load_current_table(
    config: &IcebergSinkConfig,
    catalog: &Arc<dyn Catalog>,
    deadline: tokio::time::Instant,
) -> Result<Table, ConnectorError> {
    tokio::time::timeout_at(
        deadline,
        crate::lakehouse::iceberg_io::load_table_with_timeout(
            catalog.as_ref(),
            &config.catalog.namespace,
            &config.catalog.table_name,
            config.catalog.request_timeout,
        ),
    )
    .await
    .map_err(|_| {
        ConnectorError::WriteError(
            "Iceberg direct append metadata refresh exceeded its bounded deadline".into(),
        )
    })?
}
