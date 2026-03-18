//! Delta Lake crash recovery (F031B).
//!
//! On restart, the sink must handle two failure scenarios:
//!
//! 1. **Parquet written, commit succeeded**: Normal — the txn metadata records the
//!    epoch, and `get_last_committed_epoch()` handles idempotent replay.
//!
//! 2. **Parquet written, commit failed**: Orphan Parquet files exist in the table
//!    directory but are not referenced by any Delta log entry. These must be
//!    detected and cleaned up to avoid storage leaks.
//!
//! # Recovery Protocol
//!
//! ```text
//! open() →
//!   1. Open Delta table, load snapshot
//!   2. Enumerate all Parquet files in the table directory
//!   3. Diff against files referenced in the Delta log (snapshot)
//!   4. Delete orphans that are older than a safety threshold (default: 15 min)
//!   5. Resolve last committed epoch via txn metadata (existing)
//!   6. Resume normal operation
//! ```
//!
//! The age threshold prevents deleting files from an in-flight concurrent writer
//! that hasn't committed yet.

#[cfg(feature = "delta-lake")]
use std::collections::HashSet;

#[cfg(feature = "delta-lake")]
use std::time::Duration;

#[cfg(feature = "delta-lake")]
use deltalake::DeltaTable;

#[cfg(feature = "delta-lake")]
use tracing::{debug, info, warn};

#[cfg(feature = "delta-lake")]
use crate::error::ConnectorError;

/// Minimum age of orphan files before they are eligible for cleanup.
/// This prevents deleting files from concurrent writers that haven't
/// committed yet.
#[cfg(feature = "delta-lake")]
const ORPHAN_AGE_THRESHOLD: Duration = Duration::from_secs(15 * 60); // 15 minutes

/// Result of a recovery scan.
#[derive(Debug, Clone)]
pub struct RecoveryResult {
    /// Number of orphan Parquet files detected.
    pub orphans_detected: usize,
    /// Number of orphan files deleted.
    pub orphans_deleted: usize,
    /// Last committed epoch for this writer (from txn metadata).
    pub last_committed_epoch: u64,
    /// Whether incomplete transactions were found.
    pub had_incomplete_transactions: bool,
}

/// Performs crash recovery on a Delta Lake table.
///
/// Scans for orphan Parquet files (written but never committed to the Delta log)
/// and removes those older than [`ORPHAN_AGE_THRESHOLD`]. Also resolves the last
/// committed epoch for exactly-once recovery.
///
/// # Arguments
///
/// * `table` - The Delta Lake table handle (must have a loaded snapshot)
/// * `writer_id` - Writer ID for txn metadata lookup
///
/// # Errors
///
/// Returns `ConnectorError` if the scan or cleanup fails. Non-fatal errors
/// (e.g., failure to delete a single orphan) are logged as warnings.
#[cfg(feature = "delta-lake")]
pub async fn recover_delta_table(
    table: &DeltaTable,
    writer_id: &str,
) -> Result<RecoveryResult, ConnectorError> {
    use super::delta_io;

    info!(writer_id, "starting Delta Lake crash recovery scan");

    // Step 1: Resolve last committed epoch from txn metadata.
    let last_committed_epoch = delta_io::get_last_committed_epoch(table, writer_id).await;

    // Step 2: Collect all Parquet files referenced in the current snapshot.
    let referenced_files = collect_referenced_files(table)?;

    // Step 3: List all Parquet files in the table directory.
    let all_files = list_parquet_files(table).await?;

    // Step 4: Find orphans (files not in the snapshot).
    let orphan_paths: Vec<(deltalake::Path, Option<chrono::DateTime<chrono::Utc>>)> = all_files
        .into_iter()
        .filter(|(path, _)| {
            let path_str = path.as_ref();
            // Skip _delta_log files and non-Parquet files.
            !path_str.starts_with("_delta_log")
                && path_str.ends_with(".parquet")
                && !referenced_files.contains(path_str)
        })
        .collect();

    let orphans_detected = orphan_paths.len();
    let had_incomplete_transactions = orphans_detected > 0;

    if orphans_detected == 0 {
        info!(writer_id, "no orphan files detected, recovery complete");
        return Ok(RecoveryResult {
            orphans_detected: 0,
            orphans_deleted: 0,
            last_committed_epoch,
            had_incomplete_transactions: false,
        });
    }

    info!(
        writer_id,
        orphans_detected, "found orphan Parquet files, checking age threshold"
    );

    // Step 5: Delete orphans older than the safety threshold.
    let now = chrono::Utc::now();
    let store = table.log_store().object_store(None);
    let mut orphans_deleted = 0;

    for (path, modified) in &orphan_paths {
        // Only delete if the file is older than the threshold.
        let is_old_enough = match modified {
            Some(ts) => {
                let age = now.signed_duration_since(*ts);
                age > chrono::Duration::from_std(ORPHAN_AGE_THRESHOLD).unwrap_or_default()
            }
            // If we can't determine the age, be conservative and skip.
            None => false,
        };

        if !is_old_enough {
            debug!(
                path = path.as_ref(),
                "orphan file too recent, skipping (may be from concurrent writer)"
            );
            continue;
        }

        match store.delete(path).await {
            Ok(()) => {
                debug!(path = path.as_ref(), "deleted orphan Parquet file");
                orphans_deleted += 1;
            }
            Err(e) => {
                warn!(
                    path = path.as_ref(),
                    error = %e,
                    "failed to delete orphan file (non-fatal)"
                );
            }
        }
    }

    info!(
        writer_id,
        orphans_detected,
        orphans_deleted,
        last_committed_epoch,
        "Delta Lake crash recovery complete"
    );

    Ok(RecoveryResult {
        orphans_detected,
        orphans_deleted,
        last_committed_epoch,
        had_incomplete_transactions,
    })
}

/// Collects all file paths referenced by the current Delta log snapshot.
#[cfg(feature = "delta-lake")]
fn collect_referenced_files(table: &DeltaTable) -> Result<HashSet<String>, ConnectorError> {
    let snapshot = table
        .snapshot()
        .map_err(|e| ConnectorError::Internal(format!("no snapshot for recovery scan: {e}")))?;

    let mut referenced = HashSet::new();

    // Iterate over the log data to find all active file paths.
    let log_data = snapshot.log_data();
    for file_entry in log_data {
        let path = file_entry.path().as_ref().to_string();
        referenced.insert(path);
    }

    debug!(
        referenced_files = referenced.len(),
        "collected referenced files from Delta log"
    );

    Ok(referenced)
}

/// Lists all Parquet files in the table's storage directory.
#[cfg(feature = "delta-lake")]
async fn list_parquet_files(
    table: &DeltaTable,
) -> Result<Vec<(deltalake::Path, Option<chrono::DateTime<chrono::Utc>>)>, ConnectorError> {
    use tokio_stream::StreamExt;

    let store = table.log_store().object_store(None);

    let mut files = Vec::new();
    let mut list_stream = store.list(None);

    while let Some(result) = list_stream.next().await {
        match result {
            Ok(meta) => {
                if meta.location.as_ref().ends_with(".parquet") {
                    files.push((meta.location, Some(meta.last_modified)));
                }
            }
            Err(e) => {
                warn!(error = %e, "error listing files during recovery scan");
            }
        }
    }

    debug!(
        total_parquet_files = files.len(),
        "listed Parquet files in table directory"
    );

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recovery_result_default() {
        let result = RecoveryResult {
            orphans_detected: 0,
            orphans_deleted: 0,
            last_committed_epoch: 0,
            had_incomplete_transactions: false,
        };
        assert_eq!(result.orphans_detected, 0);
        assert!(!result.had_incomplete_transactions);
    }

    #[test]
    fn test_recovery_result_with_orphans() {
        let result = RecoveryResult {
            orphans_detected: 5,
            orphans_deleted: 3,
            last_committed_epoch: 42,
            had_incomplete_transactions: true,
        };
        assert_eq!(result.orphans_detected, 5);
        assert_eq!(result.orphans_deleted, 3);
        assert_eq!(result.last_committed_epoch, 42);
        assert!(result.had_incomplete_transactions);
    }
}
