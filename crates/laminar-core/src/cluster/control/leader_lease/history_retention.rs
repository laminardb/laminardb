//! Validate the immutable Commit boundary before deleting expired authority history.

use super::{BTreeSet, LeaderAuthorityRecord, LeaderLeaseStore, LeaseError, OutcomeLink};

impl LeaderLeaseStore {
    pub(super) fn validate_retained_commit_boundary(
        head: &LeaderAuthorityRecord,
        expected_commit_head: Option<OutcomeLink>,
        retained_commit_links: &BTreeSet<OutcomeLink>,
        terminal_commit_links: &BTreeSet<OutcomeLink>,
        artifact_floor: u64,
    ) -> Result<(), LeaseError> {
        let boundary_commit_head = head
            .outcome_floor
            .as_ref()
            .and_then(|floor| floor.terminal_anchor.as_ref())
            .and_then(|anchor| {
                retained_commit_links
                    .iter()
                    .rev()
                    .find(|link| link.epoch <= anchor.epoch)
                    .copied()
                    .or_else(|| {
                        head.outcome_floor
                            .as_ref()
                            .and_then(|floor| floor.committed_anchor_link)
                    })
            });
        if expected_commit_head != boundary_commit_head {
            return Err(LeaseError::Invalid(
                "retained terminal chain lost Commit continuity at its durable floor".into(),
            ));
        }
        if !terminal_commit_links.is_subset(retained_commit_links) {
            return Err(LeaseError::Invalid(
                "terminal Commit records are not linked from the retained Commit chain".into(),
            ));
        }
        if let Some((anchor, anchor_link)) = head.outcome_floor.as_ref().and_then(|floor| {
            floor
                .terminal_anchor
                .as_ref()
                .zip(floor.terminal_anchor_link)
        }) {
            if anchor.is_commit() {
                let linked = if anchor.epoch >= artifact_floor {
                    retained_commit_links.contains(&anchor_link)
                } else {
                    head.outcome_floor.as_ref().is_some_and(|floor| {
                        floor.committed_anchor.as_ref() == Some(anchor)
                            && floor.committed_anchor_link == Some(anchor_link)
                    })
                };
                if !linked {
                    return Err(LeaseError::Invalid(
                        "terminal Commit anchor is not linked from the retained Commit chain"
                            .into(),
                    ));
                }
            }
        }
        Ok(())
    }
}
