//! Replay-start resolution for the retained subscription log.
//!
//! Purpose: translate a requested [`SubscribeStart`](super::SubscribeStart)
//! coordinate into the first physical sequence a reader replays. Ownership
//! boundary: cursor math and the typed pruned outcomes only; it does not touch
//! reader registration, wake channels, or retention mutation. Both coordinates
//! are resolved under the caller's log lock so the attach cut stays atomic.
//!
//! Coordinate domains are deliberately distinct: `AsOfEpoch` resolves a
//! checkpoint-epoch barrier, while `AfterSequence` resolves a retained
//! shared-log sequence and never consults committed epochs.

use super::{MvUpdate, StreamLogInner, SubscriptionOpenError};

/// Resolve the first physical sequence an `AfterSequence(n)` reader replays.
///
/// Shared-log sequences are 1-based, so a requested `0` maps to the first
/// entry (the beginning of the log) and a cursor at or beyond the current head
/// attaches live with no replay and no future entry skipped. A cursor behind
/// the retention floor (or any cursor when retention is disabled) fails closed
/// with the earliest replay-eligible sequence so the caller can report a
/// truthful sequence-coordinate diagnostic. `0` is the reserved "no
/// replay-eligible entry" coordinate. Admission keys off the retention window
/// rather than the physical head, because a slow attached reader can pin
/// entries below `retention_floor`.
pub(super) fn cursor_after_sequence(
    inner: &StreamLogInner,
    requested: u64,
) -> Result<u64, SubscriptionOpenError> {
    let cursor = requested.saturating_add(1);
    if cursor >= inner.next_sequence {
        return Ok(inner.next_sequence);
    }
    if inner.retention_cap == 0 {
        // `0` is the sentinel for "no replay-eligible entry is retained",
        // matching the epoch resolver's disabled-retention outcome.
        return Err(SubscriptionOpenError::SequencePruned {
            requested,
            earliest_retained: 0,
        });
    }
    // Key off the retention floor, not the physical deque head: a slow reader
    // can keep expired entries resident below the head, and replaying those
    // would hand out updates outside the configured retention window. When the
    // log was fully evicted `retention_floor` is `next_sequence`, which is the
    // truthful earliest replay-eligible sequence in that case.
    if cursor < inner.retention_floor {
        return Err(SubscriptionOpenError::SequencePruned {
            requested,
            earliest_retained: inner.retention_floor,
        });
    }
    Ok(cursor)
}

/// Resolve the `(replay cursor, barrier sequence)` an `AsOfEpoch(n)` reader uses.
///
/// Returns [`SubscriptionOpenError::EpochNotCommitted`] when no earlier epoch
/// has committed, and [`SubscriptionOpenError::ReplayPruned`] when the epoch
/// committed but its checkpoint cut is no longer retained.
pub(super) fn cursor_after_retained_epoch(
    inner: &StreamLogInner,
    requested_epoch: u64,
) -> Result<(u64, u64), SubscriptionOpenError> {
    let Some(latest_committed) = inner.latest_committed_epoch else {
        return Err(SubscriptionOpenError::EpochNotCommitted {
            requested: requested_epoch,
            latest_committed: None,
        });
    };
    if requested_epoch > latest_committed {
        return Err(SubscriptionOpenError::EpochNotCommitted {
            requested: requested_epoch,
            latest_committed: Some(latest_committed),
        });
    }
    if inner.retention_cap == 0 {
        return Err(SubscriptionOpenError::ReplayPruned {
            earliest_retained: 0,
        });
    }

    let mut cursor = None;
    let mut earliest_retained = u64::MAX;
    for entry in inner
        .entries
        .iter()
        .filter(|entry| entry.sequence >= inner.retention_floor)
    {
        if let MvUpdate::Barrier {
            epoch,
            through_sequence,
            ..
        } = entry.update.as_ref()
        {
            if *through_sequence < inner.retention_floor {
                continue;
            }
            earliest_retained = earliest_retained.min(*epoch);
            if *epoch == requested_epoch {
                cursor = Some((*through_sequence, entry.sequence));
            }
        }
    }

    if let Some(cursor) = cursor {
        return Ok(cursor);
    }
    if earliest_retained == u64::MAX || requested_epoch < earliest_retained {
        return Err(SubscriptionOpenError::ReplayPruned {
            earliest_retained: if earliest_retained == u64::MAX {
                0
            } else {
                earliest_retained
            },
        });
    }
    Err(SubscriptionOpenError::EpochNotCommitted {
        requested: requested_epoch,
        latest_committed: Some(latest_committed),
    })
}
