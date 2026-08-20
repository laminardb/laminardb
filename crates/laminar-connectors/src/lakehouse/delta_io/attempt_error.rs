//! Typed Delta write failures and publication-certainty classification.

use super::ConnectorError;

/// Preserves the typed delta-rs commit failure until the sink can classify
/// publication certainty. Local planning failures have not dispatched a
/// catalog commit.
#[cfg(feature = "delta-lake")]
#[derive(Debug, thiserror::Error)]
pub(crate) enum DeltaWriteAttemptError {
    /// Local validation or query construction failed before commit dispatch.
    #[error(transparent)]
    Local(#[from] ConnectorError),
    /// delta-rs entered the write operation and returned its typed failure.
    #[error(transparent)]
    Delta(#[from] deltalake::DeltaTableError),
}

#[cfg(feature = "delta-lake")]
impl DeltaWriteAttemptError {
    /// True only when delta-rs proves this attempt lost an optimistic race and
    /// did not publish a commit.
    pub(crate) fn is_definite_optimistic_conflict(&self) -> bool {
        matches!(self, Self::Delta(error) if is_definite_prewrite_conflict(error))
    }
}

#[cfg(feature = "delta-lake")]
fn is_definite_prewrite_conflict(error: &deltalake::DeltaTableError) -> bool {
    use deltalake::kernel::transaction::{CommitConflictError, TransactionError};

    matches!(
        error,
        deltalake::DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(
                CommitConflictError::ConcurrentAppend
                    | CommitConflictError::ConcurrentDeleteRead
                    | CommitConflictError::ConcurrentDeleteDelete
                    | CommitConflictError::ConcurrentTransaction
            )
        }
    )
}

#[cfg(feature = "delta-lake")]
fn object_store_error_has_retryable_transport(error: &deltalake::ObjectStoreError) -> bool {
    use delta_object_store::client::{HttpError, HttpErrorKind};

    // Typed permanent/conditional variants fail closed. A Generic wrapper is
    // retryable only when its source chain contains a typed transport failure.
    let deltalake::ObjectStoreError::Generic { source, .. } = error else {
        return false;
    };
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(source.as_ref());
    while let Some(current) = cause {
        if let Some(http) = current.downcast_ref::<HttpError>() {
            return matches!(
                http.kind(),
                HttpErrorKind::Connect
                    | HttpErrorKind::Request
                    | HttpErrorKind::Timeout
                    | HttpErrorKind::Interrupted
            );
        }
        cause = current.source();
    }
    false
}

#[cfg(feature = "delta-lake")]
pub(crate) fn delta_error_has_retryable_transport(error: &deltalake::DeltaTableError) -> bool {
    match error {
        deltalake::DeltaTableError::ObjectStore { source }
        | deltalake::DeltaTableError::Transaction {
            source: deltalake::kernel::transaction::TransactionError::ObjectStore { source },
        } => object_store_error_has_retryable_transport(source),
        _ => false,
    }
}

#[cfg(feature = "delta-lake")]
pub(crate) fn classify_delta_metadata_error(
    context: &str,
    error: &deltalake::DeltaTableError,
) -> ConnectorError {
    if delta_error_has_retryable_transport(error) {
        ConnectorError::ReadError(format!("{context}: {error}"))
    } else {
        ConnectorError::TransactionError(format!("{context}: {error}"))
    }
}

#[cfg(feature = "delta-lake")]
pub(crate) fn is_definite_coordinated_nonpublication(error: &deltalake::DeltaTableError) -> bool {
    use deltalake::kernel::transaction::TransactionError;

    is_definite_prewrite_conflict(error)
        || matches!(
            error,
            deltalake::DeltaTableError::VersionAlreadyExists(_)
                | deltalake::DeltaTableError::Transaction {
                    source: TransactionError::VersionAlreadyExists(_)
                        | TransactionError::MaxCommitAttempts(_)
                }
        )
}
