//! Coordinated object validation and terminal catalog publication.

use super::{
    coordinated_table_binding, coordinated_transaction_ids, decode_commit_descriptors_until,
    delta_error_has_retryable_transport, ensure_publication_deadline, get_coordinated_cursor,
    is_definite_coordinated_nonpublication, validate_coordinated_descriptors,
    validate_coordinated_log_store, Arc, AtomicUsize, CommitProperties, ConnectorError,
    CoordinatedCommitBatch, CoordinatedCommitCursor, CoordinatedObject, DeltaTable,
    DeltaTableBinding, Ordering, SaveMode, Transaction, COORDINATED_CLOCK_SKEW_MARGIN,
    COORDINATED_HEAD_CONCURRENCY, COORDINATED_TERMINAL_IO_HORIZON,
    MIN_COORDINATED_DELETED_FILE_RETENTION,
};
use std::time::Duration;

#[cfg(all(feature = "delta-lake", test))]
#[derive(Clone)]
pub(in crate::lakehouse) struct DelayedCoordinatedCatalogCommit {
    pub(in crate::lakehouse) started: Arc<tokio::sync::Notify>,
    pub(in crate::lakehouse) release: Arc<tokio::sync::Notify>,
}

#[cfg(all(feature = "delta-lake", test))]
tokio::task_local! {
    pub(in crate::lakehouse) static DELAY_COORDINATED_CATALOG_COMMIT: DelayedCoordinatedCatalogCommit;
}

#[cfg(feature = "delta-lake")]
pub(super) fn validate_coordinated_retention(retention: Duration) -> Result<(), ConnectorError> {
    if retention < MIN_COORDINATED_DELETED_FILE_RETENTION {
        return Err(ConnectorError::ConfigurationError(format!(
            "Delta exactly-once requires deleted-file retention of at least {MIN_COORDINATED_DELETED_FILE_RETENTION:?}; table config is {retention:?}",
        )));
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
async fn validate_coordinated_objects(
    table: &DeltaTable,
    objects: Vec<CoordinatedObject>,
    deleted_file_retention: Duration,
    required_alive_until: chrono::DateTime<chrono::Utc>,
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    if objects.is_empty() {
        return Ok(());
    }
    validate_coordinated_retention(deleted_file_retention)?;

    let retention = chrono::Duration::from_std(deleted_file_retention).map_err(|_| {
        ConnectorError::TransactionError(
            "Delta deleted-file retention duration exceeds the supported clock range".into(),
        )
    })?;
    let objects = Arc::new(objects);
    let next = Arc::new(AtomicUsize::new(0));
    let store = table.log_store().object_store(None);
    let mut workers = tokio::task::JoinSet::new();

    for _ in 0..COORDINATED_HEAD_CONCURRENCY.min(objects.len()) {
        let objects = Arc::clone(&objects);
        let next = Arc::clone(&next);
        let store = Arc::clone(&store);
        workers.spawn(async move {
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                let Some(object) = objects.get(index) else {
                    return Ok::<(), ConnectorError>(());
                };
                let metadata = tokio::time::timeout_at(deadline, store.head(&object.path))
                    .await
                    .map_err(|_| {
                        ConnectorError::TransactionError(format!(
                            "Delta coordinated object HEAD exceeded the publication deadline for '{}'",
                            object.path
                        ))
                    })?
                    .map_err(|error| {
                        ConnectorError::TransactionError(format!(
                            "HEAD Delta coordinated object '{}': {error}",
                            object.path
                        ))
                    })?;
                if metadata.size != object.expected_size {
                    return Err(ConnectorError::TransactionError(format!(
                        "Delta coordinated object '{}' size mismatch: descriptor {}, physical {}",
                        object.path, object.expected_size, metadata.size
                    )));
                }
                let vacuum_eligible_at = metadata
                    .last_modified
                    .checked_add_signed(retention)
                    .ok_or_else(|| {
                        ConnectorError::TransactionError(format!(
                            "Delta coordinated object '{}' recovery horizon exceeds the supported \
                             clock range",
                            object.path
                        ))
                    })?;
                if vacuum_eligible_at <= required_alive_until {
                    return Err(ConnectorError::TransactionError(format!(
                        "Delta coordinated object '{}' is at or inside the recovery-horizon safety \
                         margin: vacuum eligible at {vacuum_eligible_at}, publication must remain \
                         safe through {required_alive_until}",
                        object.path
                    )));
                }
            }
        });
    }

    while let Some(result) = workers.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                workers.abort_all();
                return Err(error);
            }
            Err(error) => {
                workers.abort_all();
                return Err(ConnectorError::Internal(format!(
                    "Delta coordinated object validation worker failed: {error}"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
enum PreparedCoordinatedPublication {
    AlreadyCommitted,
    Commit {
        adds: Vec<deltalake::kernel::Add>,
        binding: Option<DeltaTableBinding>,
        cursor: CoordinatedCommitCursor,
        descriptor_count: usize,
    },
}

#[cfg(feature = "delta-lake")]
struct CoordinatedPublicationOutcome {
    descriptor_count: usize,
}

#[cfg(feature = "delta-lake")]
fn coordinated_recovery_horizon(
    deadline: tokio::time::Instant,
) -> Result<chrono::DateTime<chrono::Utc>, ConnectorError> {
    let publication_budget = deadline.saturating_duration_since(tokio::time::Instant::now());
    let budget_on_clock = chrono::Duration::from_std(publication_budget).map_err(|_| {
        ConnectorError::TransactionError(
            "Delta coordinated publication budget exceeds the supported clock range".into(),
        )
    })?;
    let terminal_horizon =
        chrono::Duration::from_std(COORDINATED_TERMINAL_IO_HORIZON).map_err(|_| {
            ConnectorError::TransactionError(
                "Delta coordinated terminal I/O horizon exceeds the supported clock range".into(),
            )
        })?;
    let clock_skew_margin =
        chrono::Duration::from_std(COORDINATED_CLOCK_SKEW_MARGIN).map_err(|_| {
            ConnectorError::TransactionError(
                "Delta coordinated clock-skew margin exceeds the supported clock range".into(),
            )
        })?;
    chrono::Utc::now()
        .checked_add_signed(budget_on_clock)
        .and_then(|deadline| deadline.checked_add_signed(terminal_horizon))
        .and_then(|horizon| horizon.checked_add_signed(clock_skew_margin))
        .ok_or_else(|| {
            ConnectorError::TransactionError(
                "Delta coordinated recovery horizon exceeds the supported clock range".into(),
            )
        })
}

#[cfg(feature = "delta-lake")]
async fn refresh_publication_cursor(
    table: &DeltaTable,
    external_key: &str,
    deadline: tokio::time::Instant,
) -> Result<(DeltaTable, Option<CoordinatedCommitCursor>), ConnectorError> {
    // Cursor filtering and the commit base share this freshly updated snapshot.
    let mut current = table.clone();
    validate_coordinated_log_store(&current)?;
    tokio::time::timeout_at(deadline, current.update_state())
        .await
        .map_err(|_| {
            ConnectorError::WriteError(
                "Delta coordinated snapshot refresh exceeded the publication deadline without dispatching a commit"
                    .into(),
            )
        })?
        .map_err(|error| {
            ConnectorError::WriteError(format!(
                "refresh Delta coordinated publication snapshot before commit dispatch: {error}"
            ))
        })?;
    ensure_publication_deadline(deadline, "snapshot refresh")?;
    let observed = tokio::time::timeout_at(deadline, get_coordinated_cursor(&current, external_key))
        .await
        .map_err(|_| {
            ConnectorError::WriteError(
                "Delta coordinated cursor read exceeded the publication deadline without dispatching a commit"
                    .into(),
            )
        })??;
    ensure_publication_deadline(deadline, "cursor read")?;
    Ok((current, observed))
}

#[cfg(feature = "delta-lake")]
fn delta_transaction_versions(
    cursor: CoordinatedCommitCursor,
) -> Result<(i64, i64), ConnectorError> {
    if cursor.fencing_token == 0 {
        return Err(ConnectorError::TransactionError(
            "Delta coordinated fencing token must be non-zero".into(),
        ));
    }
    let checkpoint_id = i64::try_from(cursor.checkpoint_id).map_err(|_| {
        ConnectorError::TransactionError(
            "checkpoint id exceeds Delta transaction-version range".into(),
        )
    })?;
    let fencing_token = i64::try_from(cursor.fencing_token).map_err(|_| {
        ConnectorError::TransactionError(
            "fencing token exceeds Delta transaction-version range".into(),
        )
    })?;
    Ok((checkpoint_id, fencing_token))
}

#[cfg(feature = "delta-lake")]
fn validate_staged_table_binding(
    current: &DeltaTable,
    adds: &[deltalake::kernel::Add],
    binding: Option<&DeltaTableBinding>,
) -> Result<(), ConnectorError> {
    if adds.is_empty() {
        if binding.is_some() {
            return Err(ConnectorError::TransactionError(
                "empty Delta coordinated publication unexpectedly carries a table binding".into(),
            ));
        }
        return Ok(());
    }
    let binding = binding.ok_or_else(|| {
        ConnectorError::TransactionError(
            "non-empty Delta coordinated publication has no table binding".into(),
        )
    })?;
    let current_binding = coordinated_table_binding(current)?;
    if binding != &current_binding {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated descriptor table binding changed before publication (staged table '{}', live table '{}')",
            binding.table_id, current_binding.table_id
        )));
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
async fn reconcile_prepared_commit_failure(
    current: &DeltaTable,
    external_key: &str,
    cursor: CoordinatedCommitCursor,
    error: &deltalake::DeltaTableError,
) -> Result<(), ConnectorError> {
    let reconciliation = async {
        let mut reconciled = current.clone();
        reconciled.update_state().await.map_err(|refresh_error| {
            format!("refresh after prepared-commit failure: {refresh_error}")
        })?;
        get_coordinated_cursor(&reconciled, external_key)
            .await
            .map_err(|cursor_error| {
                format!("read cursor after prepared-commit failure: {cursor_error}")
            })
    }
    .await;
    if reconciliation.as_ref() == Ok(&Some(cursor)) {
        return Ok(());
    }
    if is_definite_coordinated_nonpublication(error) {
        return Err(ConnectorError::WriteError(format!(
            "Delta coordinated optimistic collision did not publish: {error}"
        )));
    }
    let retryable = delta_error_has_retryable_transport(error);
    let reconciliation = match reconciliation {
        Ok(observed) => format!("reconciliation observed cursor {observed:?}"),
        Err(reconciliation_error) => reconciliation_error,
    };
    Err(ConnectorError::outcome_unknown(
        format!(
            "Delta coordinated catalog write was dispatched but its outcome is not known: {error}; {reconciliation}"
        ),
        retryable,
    ))
}

#[cfg(feature = "delta-lake")]
async fn publish_coordinated<F>(
    table: &DeltaTable,
    external_key: &str,
    deadline: tokio::time::Instant,
    prepare: F,
) -> Result<CoordinatedPublicationOutcome, ConnectorError>
where
    F: Fn(
            Option<CoordinatedCommitCursor>,
        ) -> Result<PreparedCoordinatedPublication, ConnectorError>
        + Send,
{
    use deltalake::kernel::transaction::CommitBuilder;
    use deltalake::kernel::Action;
    use deltalake::protocol::DeltaOperation;
    use deltalake::table::config::TablePropertiesExt as _;

    ensure_publication_deadline(deadline, "admission")?;
    let required_alive_until = coordinated_recovery_horizon(deadline)?;
    let (current, observed) = refresh_publication_cursor(table, external_key, deadline).await?;
    let PreparedCoordinatedPublication::Commit {
        adds,
        binding,
        cursor,
        descriptor_count,
    } = prepare(observed)?
    else {
        current.version().ok_or_else(|| {
            ConnectorError::TransactionError(
                "Delta coordinated cursor exists on an unversioned table".into(),
            )
        })?;
        return Ok(CoordinatedPublicationOutcome {
            descriptor_count: 0,
        });
    };
    let (checkpoint_id, fencing_token) = delta_transaction_versions(cursor)?;

    let snapshot = current
        .snapshot()
        .map_err(|error| ConnectorError::TransactionError(format!("snapshot: {error}")))?;
    validate_staged_table_binding(&current, &adds, binding.as_ref())?;
    let partition_columns = snapshot.metadata().partition_columns().clone();
    let objects = validate_coordinated_descriptors(&adds, &partition_columns, deadline)?;
    validate_coordinated_objects(
        &current,
        objects,
        snapshot.table_config().deleted_file_retention_duration(),
        required_alive_until,
        deadline,
    )
    .await?;
    ensure_publication_deadline(deadline, "object validation")?;

    let partition_by = (!partition_columns.is_empty()).then_some(partition_columns);
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by,
        predicate: None,
    };
    let actions: Vec<Action> = adds.into_iter().map(Action::Add).collect();
    let (checkpoint_transaction_id, fence_transaction_id) =
        coordinated_transaction_ids(external_key);
    // Checkpoint recovery owns optimistic retries. delta-rs receives one
    // conditional catalog attempt so a timed-out terminal fence has a bounded
    // amount of provider work and cannot amplify descriptor HEAD traffic.
    let props = CommitProperties::default()
        .with_max_retries(0)
        .with_application_transactions(vec![
            Transaction::new(checkpoint_transaction_id, checkpoint_id),
            Transaction::new(fence_transaction_id, fencing_token),
        ]);
    ensure_publication_deadline(deadline, "catalog commit preparation")?;
    let pre_commit = CommitBuilder::from(props).with_actions(actions).build(
        Some(snapshot),
        current.log_store(),
        operation,
    );
    let prepared_commit = tokio::time::timeout_at(
        deadline,
        pre_commit.into_prepared_commit_future(),
    )
    .await
    .map_err(|_| {
        ConnectorError::WriteError(
            "Delta coordinated commit preparation exceeded the publication deadline without dispatching a catalog write"
                .into(),
        )
    })?
    .map_err(|error| {
        if delta_error_has_retryable_transport(&error) {
            ConnectorError::WriteError(format!(
                "prepare Delta coordinated commit before catalog dispatch: {error}"
            ))
        } else {
            ConnectorError::TransactionError(format!(
                "prepare Delta coordinated commit before catalog dispatch: {error}"
            ))
        }
    })?;
    ensure_publication_deadline(deadline, "catalog commit admission")?;

    #[cfg(test)]
    if let Ok(delay) = DELAY_COORDINATED_CATALOG_COMMIT.try_with(Clone::clone) {
        delay.started.notify_one();
        delay.release.notified().await;
    }

    // Only PreparedCommit may outlive the caller: it performs conflict checks
    // and the atomic log write. Dropping the returned PostCommit deliberately
    // keeps snapshot refresh, checkpoints, and log cleanup off this path.
    match prepared_commit.await {
        Ok(_post_commit) => Ok(CoordinatedPublicationOutcome { descriptor_count }),
        Err(error) => {
            reconcile_prepared_commit_failure(&current, external_key, cursor, &error).await?;
            Ok(CoordinatedPublicationOutcome { descriptor_count })
        }
    }
}

/// Filter a validated checkpoint batch against one freshly observed cursor,
/// then publish the remaining Adds and target cursor from that same snapshot.
///
/// # Errors
/// Returns `ConnectorError::TransactionError` on validation or commit failure.
#[cfg(feature = "delta-lake")]
pub(in crate::lakehouse) async fn commit_batch_coordinated(
    table: &DeltaTable,
    batch: &CoordinatedCommitBatch,
    deadline: tokio::time::Instant,
) -> Result<usize, ConnectorError> {
    let external_key = batch.namespace.external_key();
    let outcome = publish_coordinated(table, &external_key, deadline, |observed_cursor| {
        batch
            .validate_observed_cursor(observed_cursor)
            .map_err(|error| {
                ConnectorError::TransactionError(format!(
                    "Delta coordinated cursor continuity check failed: {error}"
                ))
            })?;
        if let Some(cursor) = observed_cursor {
            if cursor.checkpoint_id == batch.target.checkpoint_id
                && cursor.fencing_token == batch.fencing_token
            {
                return Ok(PreparedCoordinatedPublication::AlreadyCommitted);
            }
            if cursor.checkpoint_id > batch.target.checkpoint_id
                && cursor.fencing_token == batch.fencing_token
            {
                return Err(ConnectorError::TransactionError(format!(
                    "Delta coordinated cursor {} is already above exact target {}; the target cannot be inferred as the timed-out publication",
                    cursor.checkpoint_id, batch.target.checkpoint_id
                )));
            }
        }

        let observed_checkpoint_id = observed_cursor.map_or(0, |cursor| cursor.checkpoint_id);
        let descriptors: Vec<Vec<u8>> = batch
            .entries
            .iter()
            .filter(|entry| entry.attempt.checkpoint_id > observed_checkpoint_id)
            .filter_map(|entry| entry.payload.clone())
            .collect();
        let descriptor = decode_commit_descriptors_until(&descriptors, deadline)?;
        let (binding, adds) = descriptor.map_or((None, Vec::new()), |descriptor| {
            (Some(descriptor.binding), descriptor.adds)
        });
        Ok(PreparedCoordinatedPublication::Commit {
            adds: adds.clone(),
            binding: binding.clone(),
            cursor: CoordinatedCommitCursor {
                checkpoint_id: batch.target.checkpoint_id,
                fencing_token: batch.fencing_token,
            },
            descriptor_count: descriptors.len(),
        })
    })
    .await?;
    Ok(outcome.descriptor_count)
}

/// Low-level cursor primitive retained only for focused unit tests. Production
/// must use `commit_batch_coordinated` so overlap filtering occurs after refresh.
#[cfg(all(feature = "delta-lake", test))]
pub(in crate::lakehouse) async fn commit_adds_coordinated(
    table: &DeltaTable,
    adds: Vec<deltalake::kernel::Add>,
    external_key: &str,
    cursor: CoordinatedCommitCursor,
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    let binding = (!adds.is_empty())
        .then(|| coordinated_table_binding(table))
        .transpose()?;
    publish_coordinated(table, external_key, deadline, |observed_cursor| {
        if let Some(observed) = observed_cursor {
            if observed.fencing_token > cursor.fencing_token {
                return Err(ConnectorError::TransactionError(format!(
                    "Delta fencing token {} is stale; target already records {}",
                    cursor.fencing_token, observed.fencing_token
                )));
            }
            if observed.checkpoint_id > cursor.checkpoint_id {
                return Err(ConnectorError::TransactionError(format!(
                    "Delta checkpoint cursor would roll back from {} to {}",
                    observed.checkpoint_id, cursor.checkpoint_id
                )));
            }
            if observed.checkpoint_id == cursor.checkpoint_id {
                if observed.fencing_token != cursor.fencing_token {
                    return Err(ConnectorError::TransactionError(format!(
                        "Delta checkpoint {} already records fencing token {}; it cannot \
                             change to {}",
                        cursor.checkpoint_id, observed.fencing_token, cursor.fencing_token
                    )));
                }
                return Ok(PreparedCoordinatedPublication::AlreadyCommitted);
            }
        }
        Ok(PreparedCoordinatedPublication::Commit {
            adds: adds.clone(),
            binding: binding.clone(),
            cursor,
            descriptor_count: 1,
        })
    })
    .await?;
    Ok(())
}
