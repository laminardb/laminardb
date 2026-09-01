//! Delta Lake I/O integration module.
//!
//! This module provides the actual I/O operations for Delta Lake tables via the
//! `deltalake` crate. All functions are feature-gated behind `delta-lake`.
//!
//! # Architecture
//!
//! The I/O module is separate from the business logic in [`delta.rs`](super::delta)
//! to allow:
//! - Testing business logic without the `deltalake` dependency
//! - Clean separation of concerns (buffering/epoch management vs. actual writes)
//! - Easy mocking for unit tests
//!
//! Coordinated exactly-once publication uses runtime-owned, stable transaction
//! namespaces. Ordinary direct writes do not emit writer-local transaction
//! actions because a process-random identity cannot deduplicate recovery.

mod attempt_error;
mod catalog;
mod descriptor;
mod merge;
mod publication;
mod read;
mod storage_preflight;
mod table;

#[cfg(feature = "delta-lake")]
pub use catalog::resolve_catalog_options;
#[cfg(feature = "delta-lake")]
pub use merge::MergeResult;
#[cfg(feature = "delta-lake")]
pub use read::{
    get_latest_version, get_partition_columns, get_table_schema, map_cdf_to_changelog,
    read_cdf_batches,
};
#[cfg(feature = "delta-lake")]
pub use table::{get_coordinated_cursor, open_or_create_table};

#[cfg(feature = "delta-lake")]
pub(crate) use attempt_error::{
    classify_delta_metadata_error, classify_delta_object_store_metadata_error,
    delta_error_has_retryable_transport, is_definite_coordinated_nonpublication,
    DeltaWriteAttemptError,
};
#[cfg(feature = "delta-lake")]
pub(crate) use merge::merge_changelog;
#[cfg(feature = "delta-lake")]
pub(crate) use read::read_batches_at_version;
#[cfg(feature = "delta-lake")]
pub(crate) use table::{
    widen_batch_millisecond_timestamps, widen_millisecond_timestamps, write_batches,
};

#[cfg(all(feature = "delta-lake", test))]
pub(super) use descriptor::decode_commit_descriptors;
#[cfg(feature = "delta-lake")]
pub(super) use descriptor::{
    coordinated_table_binding, encode_commit_descriptor, encoded_add_array_len,
    MAX_COORDINATED_ADD_ACTIONS,
};
#[cfg(feature = "delta-lake")]
pub(super) use publication::commit_batch_coordinated;
#[cfg(all(feature = "delta-lake", test))]
pub(super) use publication::{
    commit_adds_coordinated, DelayedCoordinatedCatalogCommit, DELAY_COORDINATED_CATALOG_COMMIT,
};
#[cfg(feature = "delta-lake")]
pub(super) use storage_preflight::{
    bound_coordinated_storage_options, validate_coordinated_storage_preflight,
};

#[cfg(feature = "delta-lake")]
use descriptor::{
    decode_commit_descriptors_until, ensure_publication_deadline, validate_coordinated_descriptors,
    CoordinatedObject,
};
#[cfg(feature = "delta-lake")]
use storage_preflight::validate_coordinated_log_store;
#[cfg(feature = "delta-lake")]
use table::coordinated_transaction_ids;

#[cfg(all(feature = "delta-lake", test))]
use descriptor::{
    validate_descriptor_batch_lengths, MAX_COORDINATED_PARTITION_BYTES, MAX_COORDINATED_PATH_BYTES,
    MAX_COORDINATED_STATS_BYTES,
};
#[cfg(all(feature = "delta-lake", test))]
use publication::validate_coordinated_retention;
#[cfg(all(feature = "delta-lake", test))]
use read::{checked_cdf_commit_usage, map_cdf_scan_build_error};
#[cfg(all(feature = "delta-lake", test))]
use storage_preflight::{
    is_certified_coordinated_log_store, validate_coordinated_storage_preflight_with_env,
};
#[cfg(all(feature = "delta-lake", test))]
use table::{adapt_delta_location, apply_url_derived_options, path_to_url};

#[cfg(feature = "delta-lake")]
use std::collections::{BTreeMap, HashMap, HashSet};

#[cfg(feature = "delta-lake")]
use std::sync::Arc;

#[cfg(feature = "delta-lake")]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "delta-lake")]
use std::time::Duration;

#[cfg(feature = "delta-lake")]
use arrow_array::RecordBatch;

#[cfg(feature = "delta-lake")]
use arrow_schema::SchemaRef;

#[cfg(feature = "delta-lake")]
use deltalake::kernel::transaction::CommitProperties;

#[cfg(feature = "delta-lake")]
use deltalake::kernel::Transaction;

#[cfg(feature = "delta-lake")]
use deltalake::operations::write::SchemaMode;

#[cfg(feature = "delta-lake")]
use deltalake::protocol::SaveMode;

#[cfg(feature = "delta-lake")]
use deltalake::DeltaTable;

#[cfg(feature = "delta-lake")]
use tracing::{debug, info};

#[cfg(feature = "delta-lake")]
use url::Url;

#[cfg(feature = "delta-lake")]
use crate::error::ConnectorError;

#[cfg(feature = "delta-lake")]
use crate::storage::StorageProvider;

#[cfg(feature = "delta-lake")]
use crate::connector::{
    CoordinatedCommitBatch, CoordinatedCommitCursor, MAX_COORDINATED_COMMIT_BATCH_BYTES,
};

#[cfg(feature = "delta-lake")]
use super::commit_descriptor::{DeltaCommitDescriptor, DeltaTableBinding};

#[cfg(feature = "delta-lake")]
pub(super) fn to_delta_version(version: i64) -> Result<u64, ConnectorError> {
    u64::try_from(version).map_err(|_| {
        ConnectorError::ConfigurationError(format!(
            "Delta table version must be non-negative, got {version}"
        ))
    })
}

#[cfg(feature = "delta-lake")]
pub(super) fn from_delta_version(version: u64) -> Result<i64, ConnectorError> {
    i64::try_from(version).map_err(|_| {
        ConnectorError::ReadError(format!(
            "Delta table version {version} exceeds LaminarDB's supported range"
        ))
    })
}

#[cfg(feature = "delta-lake")]
const SET_TRANSACTION_RETENTION: &str = "delta.setTransactionRetentionDuration";

#[cfg(feature = "delta-lake")]
const COORDINATED_HEAD_CONCURRENCY: usize = 16;

// Coordinated publication uses a separate metadata-client budget. The caller
// deadline owns preparation; after preparation, one conditional catalog
// attempt is allowed to finish without cancellation. Bounding both the HTTP
// and optimistic retry layers makes that terminal fence finite.
#[cfg(feature = "delta-lake")]
const COORDINATED_REQUEST_TIMEOUT: &str = "30s";
#[cfg(feature = "delta-lake")]
const COORDINATED_CONNECT_TIMEOUT: &str = "10s";
#[cfg(feature = "delta-lake")]
const COORDINATED_RETRY_TIMEOUT: &str = "30s";
#[cfg(feature = "delta-lake")]
const COORDINATED_HTTP_MAX_RETRIES: &str = "0";
#[cfg(feature = "delta-lake")]
const COORDINATED_MAX_BACKOFF: &str = "1s";
#[cfg(feature = "delta-lake")]
const COORDINATED_TERMINAL_IO_HORIZON: Duration = Duration::from_secs(24 * 60 * 60);
#[cfg(feature = "delta-lake")]
const COORDINATED_CLOCK_SKEW_MARGIN: Duration = Duration::from_secs(5 * 60);
#[cfg(feature = "delta-lake")]
const MIN_COORDINATED_DELETED_FILE_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[cfg(all(test, feature = "delta-lake"))]
mod tests;
