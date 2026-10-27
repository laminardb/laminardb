//! Recent reads: the newest committed cut that holds every record the read's sources had
//! upstream when the read arrived.

use std::time::Instant;

use laminar_connectors::connector::RecencyTarget;
use laminar_core::checkpoint::CommittedCheckpointIndex;

use super::{CommittedReadError, CommittedViews, PinnedCut};
use crate::checkpoint_coordinator::committed_reads::PinAcquire;
use crate::checkpoint_coordinator::connector_to_source_checkpoint;
use crate::error::DbError;

/// Each source behind a read, with its upstream's end when the read arrived.
#[derive(Debug)]
pub(crate) struct RecencyTargets {
    targets: Vec<(String, Box<dyn RecencyTarget>)>,
}

impl RecencyTargets {
    pub(crate) fn new(targets: Vec<(String, Box<dyn RecencyTarget>)>) -> Self {
        Self { targets }
    }

    /// Mark each source `index` holds. A source the cut does not list is not held yet.
    async fn check(
        &self,
        index: &CommittedCheckpointIndex,
        covered: &mut [bool],
        deadline: Instant,
    ) -> Result<(), CommittedReadError> {
        for ((source, target), covered) in self.targets.iter().zip(covered) {
            let Some(offsets) = index.source_offsets.get(source).filter(|_| !*covered) else {
                continue;
            };
            let checkpoint = connector_to_source_checkpoint(offsets);
            *covered = target
                .covered_by(&checkpoint, deadline)
                .await
                .map_err(|error| CommittedReadError::RecencyProbe {
                    input: source.clone(),
                    reason: error.to_string(),
                })?;
        }
        Ok(())
    }

    fn uncovered(&self, covered: &[bool]) -> Vec<String> {
        self.targets
            .iter()
            .zip(covered)
            .filter(|(_, covered)| !**covered)
            .map(|((source, _), _)| source.clone())
            .collect()
    }
}

impl CommittedViews {
    /// Pin the newest committed cut at or after `at_least` that holds every target, waiting for
    /// one until `deadline`. Positions only grow, so a source one cut holds is held by every
    /// later cut and is not checked again.
    pub(crate) async fn pin_covering(
        &self,
        targets: &RecencyTargets,
        at_least: u64,
        deadline: Instant,
    ) -> Result<PinnedCut, DbError> {
        let mut covered = vec![false; targets.targets.len()];
        let mut next = at_least;
        // Each pass needs a newer cut than the last, so the deadline ends the loop.
        loop {
            let cut = match self.await_epoch(Some(next), deadline).await {
                Ok(cut) => cut,
                Err(floor_not_committed) if next == at_least && at_least > 0 => {
                    return Err(floor_not_committed)
                }
                Err(_) => {
                    return Err(CommittedReadError::NotRecent {
                        latest: self.latest().map(|cut| cut.index.epoch),
                        uncovered: targets.uncovered(&covered),
                    }
                    .into())
                }
            };
            next = cut.index.epoch + 1;
            targets.check(&cut.index, &mut covered, deadline).await?;
            if !covered.iter().all(|covered| *covered) {
                continue;
            }
            match self.pins.acquire(&cut.reference, deadline) {
                PinAcquire::Acquired(pin) => return self.pinned(&cut, pin, deadline).await,
                PinAcquire::Capacity => return Err(CommittedReadError::PinCapacity.into()),
                // A newer cut replaced it, or a recovery round holds pins until one commits.
                PinAcquire::Pruned => {}
            }
        }
    }
}
