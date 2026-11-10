//! Committed event-time frontiers of append-only views in a cluster cut.

use super::{
    validate_name, BTreeMap, CheckpointManifest, CheckpointScope, CheckpointWatermark,
    CommittedCheckpointIndex, SOURCE_WATERMARK_INDEX_VERSION,
};

/// The committed frontier of each append-only view at a cluster cut: the minimum of the
/// participants' frontiers, never below the predecessor's. A participant whose view has no
/// frontier yet holds the predecessor's, so rows it may still replay stay retained; an idle
/// participant takes no part. A view no participant logs is dropped.
#[must_use]
pub fn fold_view_frontiers<'a>(
    predecessor: &BTreeMap<String, i64>,
    manifests: impl IntoIterator<Item = &'a CheckpointManifest>,
) -> BTreeMap<String, i64> {
    let mut reported = BTreeMap::<&str, CheckpointWatermark>::new();
    for log in manifests
        .into_iter()
        .flat_map(|manifest| &manifest.view_logs)
    {
        let participant = match (log.idle, log.frontier) {
            (true, _) => CheckpointWatermark::Idle,
            (false, None) => CheckpointWatermark::Uninitialized,
            (false, Some(frontier)) => CheckpointWatermark::Active(frontier),
        };
        reported
            .entry(log.view.as_str())
            .and_modify(|cut| *cut = cut.cluster_min(participant))
            .or_insert(participant);
    }
    reported
        .into_iter()
        .filter_map(|(view, cut)| {
            let held = predecessor.get(view).copied();
            let frontier = match cut.active_value() {
                Some(minimum) => Some(held.map_or(minimum, |held| held.max(minimum))),
                None => held,
            };
            frontier.map(|frontier| (view.to_owned(), frontier))
        })
        .collect()
}

impl CommittedCheckpointIndex {
    pub(super) fn validate_view_frontiers(&self) -> Result<(), String> {
        if self.view_frontiers.is_empty() {
            return Ok(());
        }
        if self.version <= SOURCE_WATERMARK_INDEX_VERSION || self.scope != CheckpointScope::Cluster
        {
            return Err("only a version 5 cluster checkpoint carries view frontiers".into());
        }
        for (view, frontier) in &self.view_frontiers {
            validate_name("view frontier", view)?;
            if *frontier == i64::MIN {
                return Err(format!(
                    "view frontier '{view}' uses the reserved uninitialized value"
                ));
            }
        }
        Ok(())
    }

    /// No view's committed frontier regresses across `predecessor`.
    pub(crate) fn validate_view_frontier_continuity(
        &self,
        predecessor: &CommittedCheckpointIndex,
    ) -> Result<(), String> {
        for (view, frontier) in &self.view_frontiers {
            if predecessor
                .view_frontiers
                .get(view)
                .is_some_and(|predecessor| predecessor > frontier)
            {
                return Err(format!(
                    "committed frontier of view '{view}' regresses across its predecessor"
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::ViewLog;

    fn manifest(logs: &[(&str, Option<i64>, bool)]) -> CheckpointManifest {
        let mut manifest = CheckpointManifest::new(1, 1);
        manifest.view_logs = logs
            .iter()
            .map(|&(view, frontier, idle)| ViewLog {
                view: view.into(),
                next_sequence: 0,
                frontier,
                idle,
                segments: Vec::new(),
            })
            .collect();
        manifest
    }

    fn frontiers(entries: &[(&str, i64)]) -> BTreeMap<String, i64> {
        entries
            .iter()
            .map(|&(view, frontier)| (view.to_owned(), frontier))
            .collect()
    }

    #[test]
    fn active_participants_set_the_minimum_and_idle_ones_take_no_part() {
        let cut = [
            manifest(&[("v", Some(300), false)]),
            manifest(&[("v", Some(200), false)]),
            manifest(&[("v", Some(50), true)]),
        ];
        assert_eq!(
            fold_view_frontiers(&frontiers(&[("v", 100)]), &cut),
            frontiers(&[("v", 200)])
        );
    }

    #[test]
    fn an_uninitialized_participant_holds_the_predecessor_frontier() {
        let cut = [
            manifest(&[("v", Some(300), false)]),
            manifest(&[("v", None, false)]),
        ];
        assert_eq!(
            fold_view_frontiers(&frontiers(&[("v", 100)]), &cut),
            frontiers(&[("v", 100)])
        );
        assert!(fold_view_frontiers(&BTreeMap::new(), &cut).is_empty());
    }

    #[test]
    fn the_frontier_never_regresses_and_an_all_idle_view_holds_it() {
        let lower = [manifest(&[("v", Some(80), false)])];
        assert_eq!(
            fold_view_frontiers(&frontiers(&[("v", 100)]), &lower),
            frontiers(&[("v", 100)])
        );
        let idle = [manifest(&[("v", Some(500), true)])];
        assert_eq!(
            fold_view_frontiers(&frontiers(&[("v", 100)]), &idle),
            frontiers(&[("v", 100)])
        );
    }

    #[test]
    fn views_no_participant_logs_are_dropped() {
        let cut = [manifest(&[("kept", Some(10), false)])];
        assert_eq!(
            fold_view_frontiers(&frontiers(&[("dropped", 5), ("kept", 1)]), &cut),
            frontiers(&[("kept", 10)])
        );
    }
}
