//! Cluster peer-frontier merge for the temporal join.

use super::{
    max_watermark, merge_input_frontier_iter, validate_frontier, BTreeMap, DbError, InputFrontier,
    ManagedTemporalJoinOperator,
};

impl ManagedTemporalJoinOperator {
    pub(super) fn effective_cluster_frontiers(
        &self,
        local: [InputFrontier; 2],
        applied: Option<&[BTreeMap<u64, InputFrontier>; 2]>,
        consumed: Option<&[BTreeMap<u64, usize>; 2]>,
    ) -> Result<[InputFrontier; 2], DbError> {
        let merge_side = |port: usize| -> Result<InputFrontier, DbError> {
            let peers = self.peer_channels[port].iter().map(|(peer, channel)| {
                let mut frontier = applied
                    .and_then(|frontiers| frontiers[port].get(peer).copied())
                    .unwrap_or(channel.applied);
                let consumed = consumed
                    .and_then(|counts| counts[port].get(peer).copied())
                    .unwrap_or(0);
                if channel.events.len() > consumed {
                    frontier.idle = false;
                    frontier.watermark =
                        max_watermark(frontier.watermark, self.frontiers[port].watermark);
                }
                frontier
            });
            // Temporal inputs are capped at the committed cut, so an idle local input is it.
            let merged = crate::operator::frontier::hold_all_idle_merge(
                merge_input_frontier_iter(std::iter::once(local[port]).chain(peers), i64::MIN),
                self.frontiers[port].watermark,
                crate::operator::frontier::idle_close_ceiling(local[port]),
            );
            let side = if port == 0 { "left" } else { "right" };
            validate_frontier(self.frontiers[port], merged, side, &self.name)?;
            if self.pending_cluster_input.is_some() {
                return Ok(InputFrontier {
                    watermark: self.frontiers[port].watermark,
                    idle: false,
                });
            }
            Ok(merged)
        };
        Ok([merge_side(0)?, merge_side(1)?])
    }
}
