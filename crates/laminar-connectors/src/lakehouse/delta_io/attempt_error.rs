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
#[derive(Clone, Copy)]
enum DeltaHttpRetryPolicy {
    ProvenTransport,
    IdempotentMetadataRead,
}

#[cfg(feature = "delta-lake")]
fn object_store_error_is_retryable(
    error: &deltalake::ObjectStoreError,
    policy: DeltaHttpRetryPolicy,
) -> bool {
    use delta_object_store::client::{HttpError, HttpErrorKind};

    // Typed permanent/conditional variants fail closed. Unknown HTTP failures are
    // retryable only for bounded idempotent reads; writes require a concrete I/O cause.
    let deltalake::ObjectStoreError::Generic { source, .. } = error else {
        return false;
    };
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(source.as_ref());
    let mut unknown_http = false;
    while let Some(current) = cause {
        if let Some(http) = current.downcast_ref::<HttpError>() {
            match http.kind() {
                HttpErrorKind::Connect
                | HttpErrorKind::Request
                | HttpErrorKind::Timeout
                | HttpErrorKind::Interrupted => return true,
                HttpErrorKind::Unknown
                    if matches!(policy, DeltaHttpRetryPolicy::IdempotentMetadataRead) =>
                {
                    return true;
                }
                HttpErrorKind::Unknown => unknown_http = true,
                _ => return false,
            }
        }
        // COMPAT: object_store can classify newer reqwest/hyper chains as Unknown. Only a
        // concrete transport I/O cause below that typed HTTP boundary is safe to retry.
        if unknown_http
            && current.downcast_ref::<std::io::Error>().is_some_and(|io| {
                matches!(
                    io.kind(),
                    std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::UnexpectedEof
                )
            })
        {
            return true;
        }
        cause = current.source();
    }
    false
}

#[cfg(feature = "delta-lake")]
fn kernel_error_is_retryable(
    error: &delta_kernel::error::Error,
    policy: DeltaHttpRetryPolicy,
) -> bool {
    match error {
        delta_kernel::error::Error::Backtraced { source, .. } => {
            kernel_error_is_retryable(source, policy)
        }
        delta_kernel::error::Error::ObjectStore(error) => {
            object_store_error_is_retryable(error, policy)
        }
        _ => false,
    }
}

#[cfg(feature = "delta-lake")]
fn delta_error_is_retryable(
    error: &deltalake::DeltaTableError,
    policy: DeltaHttpRetryPolicy,
) -> bool {
    match error {
        deltalake::DeltaTableError::KernelError(error) => kernel_error_is_retryable(error, policy),
        deltalake::DeltaTableError::ObjectStore { source }
        | deltalake::DeltaTableError::Transaction {
            source: deltalake::kernel::transaction::TransactionError::ObjectStore { source },
        }
        | deltalake::DeltaTableError::Kernel {
            source: deltalake::kernel::Error::ObjectStore(source),
        } => object_store_error_is_retryable(source, policy),
        _ => false,
    }
}

#[cfg(feature = "delta-lake")]
pub(crate) fn delta_error_has_retryable_transport(error: &deltalake::DeltaTableError) -> bool {
    delta_error_is_retryable(error, DeltaHttpRetryPolicy::ProvenTransport)
}

#[cfg(feature = "delta-lake")]
pub(crate) fn classify_delta_metadata_error(
    context: &str,
    error: &deltalake::DeltaTableError,
) -> ConnectorError {
    if delta_error_is_retryable(error, DeltaHttpRetryPolicy::IdempotentMetadataRead) {
        ConnectorError::ReadError(format!("{context}: {error}"))
    } else {
        ConnectorError::TransactionError(format!("{context}: {error}"))
    }
}

#[cfg(feature = "delta-lake")]
pub(crate) fn classify_delta_object_store_metadata_error(
    context: &str,
    error: &deltalake::ObjectStoreError,
) -> ConnectorError {
    if object_store_error_is_retryable(error, DeltaHttpRetryPolicy::IdempotentMetadataRead) {
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
