//! Coordinated publication and Delta write failure certainty.

#[cfg(feature = "delta-lake")]
use super::{
    info, Arc, Array, ConnectorError, ConnectorTaskGuard, DeltaTable, Duration, Future, RecordBatch,
};

#[cfg(feature = "delta-lake")]
const COORDINATED_TABLE_OPEN_MAX_ATTEMPTS: usize = 3;
#[cfg(feature = "delta-lake")]
const COORDINATED_TABLE_OPEN_RETRY_INITIAL: Duration = Duration::from_millis(50);
#[cfg(feature = "delta-lake")]
const COORDINATED_TABLE_OPEN_RETRY_MAX: Duration = Duration::from_millis(200);

#[cfg(feature = "delta-lake")]
pub(super) async fn run_tracked_delta_task<F>(guard: ConnectorTaskGuard, task: F) -> F::Output
where
    F: Future,
{
    let _guard = guard;
    task.await
}

#[cfg(feature = "delta-lake")]
pub(super) fn classify_delta_attempt_error(
    error: super::super::delta_io::DeltaWriteAttemptError,
) -> ConnectorError {
    use super::super::delta_io::DeltaWriteAttemptError;

    if error.is_definite_optimistic_conflict() {
        return ConnectorError::WriteError(format!(
            "Delta optimistic commit collision did not publish: {error}"
        ));
    }

    match error {
        DeltaWriteAttemptError::Local(error) => error,
        DeltaWriteAttemptError::Delta(error) => {
            // A storage failure may make progress in a fresh generation, but
            // it still cannot prove whether the catalog accepted the commit.
            // Structural/protocol errors are terminal and must not be turned
            // into retries merely because their message says "conflict".
            let retryable = super::super::delta_io::delta_error_has_retryable_transport(&error);
            ConnectorError::outcome_unknown(
                format!(
                    "Delta write was dispatched but its catalog commit outcome is not known: {error}"
                ),
                retryable,
            )
        }
    }
}

#[cfg(feature = "delta-lake")]
pub(super) struct DeltaWriteTaskSuccess {
    pub(super) table: DeltaTable,
    pub(super) merge_result: Option<super::super::delta_io::MergeResult>,
}

/// Counts `(upserts, deletes)` in a collapsed changelog batch's `_op` column.
/// A row is a delete iff `_op == "D"`; everything else (including a missing or
/// null op) counts as an upsert. Used only for collapse observability.
#[cfg(feature = "delta-lake")]
pub(super) fn count_collapsed_ops(batch: &RecordBatch) -> (u64, u64) {
    let Ok(idx) = batch.schema().index_of("_op") else {
        return (0, 0);
    };
    let Some(ops) = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
    else {
        return (0, 0);
    };
    let deletes = (0..ops.len())
        .filter(|&i| !ops.is_null(i) && ops.value(i) == "D")
        .count() as u64;
    let upserts = ops.len() as u64 - deletes;
    (upserts, deletes)
}

#[cfg(feature = "delta-lake")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct UnresolvedDeltaPublication {
    pub(super) external_key: String,
    pub(super) target: crate::connector::CoordinatedCommitCursor,
    pub(super) exact_batch_fingerprint: [u8; 32],
}

#[cfg(feature = "delta-lake")]
impl UnresolvedDeltaPublication {
    pub(super) fn reconciled_by(
        &self,
        observed: Option<crate::connector::CoordinatedCommitCursor>,
    ) -> bool {
        observed == Some(self.target)
    }
}

#[cfg(feature = "delta-lake")]
async fn retry_coordinated_table_open_until<F, Fut, T>(
    deadline: tokio::time::Instant,
    mut open: F,
) -> Result<T, ConnectorError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ConnectorError>>,
{
    let mut attempts = 0;
    let mut backoff = COORDINATED_TABLE_OPEN_RETRY_INITIAL;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(ConnectorError::TransactionError(
                "Delta coordinated table open exceeded the publication deadline".into(),
            ));
        }
        attempts += 1;
        let result = tokio::time::timeout_at(deadline, open())
            .await
            .map_err(|_| {
                ConnectorError::TransactionError(
                    "Delta coordinated table open exceeded the publication deadline".into(),
                )
            })?;
        match result {
            Ok(table) => return Ok(table),
            Err(error)
                if error.is_transient() && attempts < COORDINATED_TABLE_OPEN_MAX_ATTEMPTS =>
            {
                let now = tokio::time::Instant::now();
                tokio::time::sleep_until((now + backoff).min(deadline)).await;
                backoff = backoff
                    .saturating_mul(2)
                    .min(COORDINATED_TABLE_OPEN_RETRY_MAX);
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(feature = "delta-lake")]
pub(super) async fn publish_coordinated_delta_batch(
    table_path: String,
    storage_options: std::collections::HashMap<String, String>,
    unresolved: Arc<parking_lot::Mutex<Option<UnresolvedDeltaPublication>>>,
    pending: UnresolvedDeltaPublication,
    batch: crate::connector::CoordinatedCommitBatch,
    deadline: tokio::time::Instant,
    publication_budget: Duration,
) -> Result<(), ConnectorError> {
    super::super::delta_io::validate_coordinated_storage_preflight(&table_path, &storage_options)?;
    let storage_options =
        super::super::delta_io::bound_coordinated_storage_options(storage_options);
    let result = async {
        // RECOVERY: `schema = None` keeps retries metadata-only. The conditional commit below is
        // dispatched exactly once and retains its existing outcome-unknown reconciliation path.
        let table = retry_coordinated_table_open_until(deadline, || {
            super::super::delta_io::open_or_create_table(&table_path, storage_options.clone(), None)
        })
        .await?;
        let descriptor_count =
            super::super::delta_io::commit_batch_coordinated(&table, &batch, deadline).await?;
        info!(
            epoch = batch.target.epoch,
            checkpoint_id = batch.target.checkpoint_id,
            descriptors = descriptor_count,
            "delta coordinated commit"
        );
        Ok(())
    }
    .await;

    if result.is_ok() && tokio::time::Instant::now() < deadline {
        let mut unresolved = unresolved.lock();
        if unresolved.as_ref() == Some(&pending) {
            *unresolved = None;
        }
        return Ok(());
    }
    if result.is_ok() {
        return Err(ConnectorError::outcome_unknown(
            format!(
                "Delta coordinated publication exceeded its {publication_budget:?} remaining \
                 budget; reconcile the exact cursor before replay"
            ),
            true,
        ));
    }
    result
}

#[cfg(all(test, feature = "delta-lake"))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(start_paused = true)]
    async fn coordinated_table_open_retries_only_transient_failures() {
        let attempts = AtomicUsize::new(0);
        let opened = retry_coordinated_table_open_until(
            tokio::time::Instant::now() + Duration::from_secs(1),
            || async {
                if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err(ConnectorError::ConnectionFailed(
                        "temporary GET failure".into(),
                    ))
                } else {
                    Ok(7_u64)
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(opened, 7);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        let attempts = AtomicUsize::new(0);
        let error = retry_coordinated_table_open_until(
            tokio::time::Instant::now() + Duration::from_secs(1),
            || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(ConnectorError::ConfigurationError("bad credentials".into()))
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ConnectorError::ConfigurationError(_)));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
