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
//!
//! # Edge Case: Corrupted Delta Log
//!
//! If the Delta log itself is corrupted (missing/truncated checkpoint files,
//! invalid JSON entries in `_delta_log/`), `snapshot()` will fail and we cannot
//! determine which Parquet files are referenced. In this case, recovery skips
//! orphan cleanup entirely and returns a degraded result rather than crashing
//! the sink. The operator should repair the Delta log (e.g., via
//! `FSCK REPAIR TABLE` in Spark, or by restoring from a backup) and then
//! restart the sink to complete orphan cleanup.

use std::collections::HashSet;
use std::time::Duration;

use crate::error::ConnectorError;

#[cfg(feature = "delta-lake")]
use deltalake::DeltaTable;

#[cfg(feature = "delta-lake")]
use tracing::{debug, info, warn};

/// Minimum age of orphan files before they are eligible for cleanup.
/// This prevents deleting files from concurrent writers that haven't
/// committed yet.
const ORPHAN_AGE_THRESHOLD: Duration = Duration::from_secs(15 * 60); // 15 minutes

/// Maximum orphan files to delete per recovery run. If there are more
/// orphans than this (e.g., from a crash loop), we bail and let the
/// next restart continue the cleanup rather than blocking the sink.
#[cfg(feature = "delta-lake")]
const MAX_ORPHAN_DELETES: usize = 1000;

/// Concurrency limit for orphan file deletion (S3 round trips).
#[cfg(feature = "delta-lake")]
const DELETE_CONCURRENCY: usize = 16;

/// Result of a recovery scan.
#[derive(Debug, Clone)]
pub struct RecoveryResult {
    /// Number of orphan Parquet files detected.
    pub orphans_detected: usize,
    /// Number of orphan files deleted.
    pub orphans_deleted: usize,
    /// Last committed epoch for this writer (from txn metadata).
    pub last_committed_epoch: u64,
}

/// Performs crash recovery on a Delta Lake table.
///
/// Scans for orphan Parquet files (written but never committed to the Delta log)
/// and removes those older than [`ORPHAN_AGE_THRESHOLD`]. Also resolves the last
/// committed epoch for exactly-once recovery.
///
/// Orphan deletes run concurrently (up to [`DELETE_CONCURRENCY`]) to avoid
/// blocking startup on S3 round trips. If more than [`MAX_ORPHAN_DELETES`]
/// orphans are found, only the first batch is cleaned up — the next restart
/// continues where we left off.
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
///
/// # Panics
///
/// Panics if `ORPHAN_AGE_THRESHOLD` exceeds `chrono::Duration::max_value()`
/// (cannot happen with the 15-minute constant).
#[cfg(feature = "delta-lake")]
#[allow(clippy::too_many_lines)]
pub async fn recover_delta_table(
    table: &DeltaTable,
    writer_id: &str,
) -> Result<RecoveryResult, ConnectorError> {
    use super::delta_io;

    info!(writer_id, "starting Delta Lake crash recovery scan");

    // Step 1: Resolve last committed epoch from txn metadata.
    let last_committed_epoch = delta_io::get_last_committed_epoch(table, writer_id).await;

    // Step 2: Collect all Parquet files referenced in the current snapshot.
    // Guard: if the Delta log is corrupted and snapshot() fails, skip orphan
    // cleanup entirely rather than crashing the sink. The epoch metadata is
    // already resolved above (it uses a separate code path), so the sink can
    // still resume writes — it just won't clean up orphan files this time.
    let referenced_files = match collect_referenced_files(table) {
        Ok(files) => files,
        Err(e) => {
            warn!(
                writer_id,
                error = %e,
                "Delta log snapshot failed during recovery — skipping orphan cleanup. \
                 The Delta log may be corrupted. Repair it (e.g., FSCK REPAIR TABLE) \
                 and restart the sink to complete orphan cleanup."
            );
            return Ok(RecoveryResult {
                orphans_detected: 0,
                orphans_deleted: 0,
                last_committed_epoch,
            });
        }
    };

    // Step 3: List all Parquet files in the table directory.
    let all_files = list_parquet_files(table).await?;

    // Step 4: Find orphans (files not in the snapshot).
    let now = chrono::Utc::now();
    let threshold = chrono::Duration::from_std(ORPHAN_AGE_THRESHOLD)
        .expect("ORPHAN_AGE_THRESHOLD (15 min) fits in chrono::Duration");
    let orphan_paths: Vec<deltalake::Path> = all_files
        .into_iter()
        .filter(|(path, _)| {
            let path_str = path.as_ref();
            // Skip _delta_log files and non-Parquet files.
            !path_str.starts_with("_delta_log")
                && path_str.ends_with(".parquet")
                && !referenced_files.contains(path_str)
        })
        .filter(|(path, modified)| {
            // Only include orphans older than the safety threshold.
            match modified {
                Some(ts) => {
                    let age = now.signed_duration_since(*ts);
                    if age <= threshold {
                        debug!(
                            path = path.as_ref(),
                            "orphan file too recent, skipping (may be from concurrent writer)"
                        );
                        false
                    } else {
                        true
                    }
                }
                // If we can't determine the age, be conservative and skip.
                None => false,
            }
        })
        .map(|(path, _)| path)
        .collect();

    let orphans_detected = orphan_paths.len();

    if orphans_detected == 0 {
        info!(writer_id, "no orphan files detected, recovery complete");
        return Ok(RecoveryResult {
            orphans_detected: 0,
            orphans_deleted: 0,
            last_committed_epoch,
        });
    }

    if orphans_detected > MAX_ORPHAN_DELETES {
        warn!(
            writer_id,
            orphans_detected,
            max = MAX_ORPHAN_DELETES,
            "more orphans than cleanup cap — cleaning first batch, next restart continues"
        );
    }

    info!(
        writer_id,
        orphans_detected, "found orphan Parquet files eligible for deletion"
    );

    // Step 5: Delete orphans concurrently with capped parallelism.
    // Uses a semaphore to limit concurrent S3 round trips.
    let store = table.log_store().object_store(None);
    let delete_batch = &orphan_paths[..orphans_detected.min(MAX_ORPHAN_DELETES)];
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(DELETE_CONCURRENCY));

    let mut join_set = tokio::task::JoinSet::new();
    for path in delete_batch.iter().cloned() {
        let store = store.clone();
        let sem = std::sync::Arc::clone(&semaphore);
        join_set.spawn(async move {
            let _permit = sem.acquire().await;
            let result = store.delete(&path).await;
            (path, result)
        });
    }

    let mut orphans_deleted = 0;
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok((path, Ok(()))) => {
                debug!(path = path.as_ref(), "deleted orphan Parquet file");
                orphans_deleted += 1;
            }
            Ok((path, Err(e))) => {
                warn!(
                    path = path.as_ref(),
                    error = %e,
                    "failed to delete orphan file (non-fatal)"
                );
            }
            Err(e) => {
                warn!(error = ?e, "orphan delete task panicked");
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
///
/// Listing errors for individual entries are logged as warnings and skipped.
/// Partial listing is acceptable here because fewer orphans = fewer deletes,
/// which is the safe direction (we never delete a referenced file).
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

/// Computes the set of orphan file paths: files present on storage but not
/// referenced in the Delta log snapshot.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn find_orphan_paths(all_files: &[String], referenced_files: &HashSet<String>) -> Vec<String> {
    all_files
        .iter()
        .filter(|path| {
            !path.starts_with("_delta_log")
                && path.ends_with(".parquet")
                && !referenced_files.contains(path.as_str())
        })
        .cloned()
        .collect()
}

/// Filters orphan paths by age threshold, returning only those old enough
/// to be safely deleted.
///
/// # Panics
///
/// Panics if `threshold` exceeds `chrono::Duration::max_value()` (~292 billion years).
#[must_use]
pub fn filter_by_age(
    orphans_with_ts: &[(String, Option<chrono::DateTime<chrono::Utc>>)],
    now: chrono::DateTime<chrono::Utc>,
    threshold: Duration,
) -> Vec<String> {
    let chrono_threshold =
        chrono::Duration::from_std(threshold).expect("threshold fits in chrono::Duration");
    orphans_with_ts
        .iter()
        .filter(|(_, modified)| match modified {
            Some(ts) => now.signed_duration_since(*ts) > chrono_threshold,
            None => false,
        })
        .map(|(path, _)| path.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_recovery_result_fields() {
        let result = RecoveryResult {
            orphans_detected: 5,
            orphans_deleted: 3,
            last_committed_epoch: 42,
        };
        assert_eq!(result.orphans_detected, 5);
        assert_eq!(result.orphans_deleted, 3);
        assert_eq!(result.last_committed_epoch, 42);
    }

    #[test]
    fn test_find_orphan_paths_detects_unreferenced() {
        let referenced: HashSet<String> = ["part-0001.parquet", "part-0002.parquet"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        let all_files: Vec<String> = [
            "part-0001.parquet",
            "part-0002.parquet",
            "part-0003.parquet",
            "orphan-abc.parquet",
            "_delta_log/00000.json",
            "some_file.csv",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        let orphans = find_orphan_paths(&all_files, &referenced);
        assert_eq!(orphans.len(), 2);
        assert!(orphans.contains(&"part-0003.parquet".to_string()));
        assert!(orphans.contains(&"orphan-abc.parquet".to_string()));
    }

    #[test]
    fn test_find_orphan_paths_skips_delta_log_and_non_parquet() {
        let referenced = HashSet::new();
        let all_files = vec![
            "_delta_log/00000.json".to_string(),
            "data.csv".to_string(),
            "real-orphan.parquet".to_string(),
        ];

        let orphans = find_orphan_paths(&all_files, &referenced);
        assert_eq!(orphans, vec!["real-orphan.parquet"]);
    }

    #[test]
    fn test_find_orphan_paths_empty_when_all_referenced() {
        let all_files = vec!["part-0001.parquet".to_string()];
        let referenced: HashSet<String> = all_files.iter().cloned().collect();

        let orphans = find_orphan_paths(&all_files, &referenced);
        assert!(orphans.is_empty());
    }

    #[test]
    fn test_filter_by_age_excludes_recent() {
        let now = Utc::now();
        let old_ts = now - chrono::Duration::hours(1);
        let recent_ts = now - chrono::Duration::minutes(5);

        let orphans = vec![
            ("old-orphan.parquet".to_string(), Some(old_ts)),
            ("recent-orphan.parquet".to_string(), Some(recent_ts)),
            ("no-ts-orphan.parquet".to_string(), None),
        ];

        let eligible = filter_by_age(&orphans, now, ORPHAN_AGE_THRESHOLD);
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0], "old-orphan.parquet");
    }

    #[test]
    fn test_filter_by_age_all_too_recent() {
        let now = Utc::now();
        let recent = now - chrono::Duration::minutes(1);

        let orphans = vec![
            ("a.parquet".to_string(), Some(recent)),
            ("b.parquet".to_string(), Some(recent)),
        ];

        let eligible = filter_by_age(&orphans, now, ORPHAN_AGE_THRESHOLD);
        assert!(eligible.is_empty());
    }

    #[test]
    fn test_filter_by_age_boundary() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        // Exactly at threshold — should NOT be deleted (need strictly older).
        let at_threshold = now - chrono::Duration::minutes(15);
        // Just past threshold.
        let past_threshold = now - chrono::Duration::minutes(16);

        let orphans = vec![
            ("at-boundary.parquet".to_string(), Some(at_threshold)),
            ("past-boundary.parquet".to_string(), Some(past_threshold)),
        ];

        let eligible = filter_by_age(&orphans, now, ORPHAN_AGE_THRESHOLD);
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0], "past-boundary.parquet");
    }
}
