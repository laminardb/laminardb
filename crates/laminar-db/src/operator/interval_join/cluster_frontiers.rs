//! Cluster peer-frontier merge for the interval join.

use super::{DbError, InputFrontier, IntervalJoinOperator, JoinInputSide};
use crate::operator::frontier::{hold_all_idle_merge, is_held_all_idle_frontier, max_watermark};
use crate::operator_graph::merge_input_frontier_iter;

impl IntervalJoinOperator {
    /// Whether a checkpointed applied cut matches `merged`, the peer merge recomputed at restore.
    /// The idle flag is compared only when `compare_idle` holds, and a held all-idle close may sit
    /// below the raw merge.
    pub(super) fn cut_matches(
        expected_watermark: i64,
        expected_idle: bool,
        merged: InputFrontier,
        compare_idle: bool,
    ) -> bool {
        let saved = InputFrontier {
            watermark: Self::watermark_option(expected_watermark),
            idle: expected_idle,
        };
        is_held_all_idle_frontier(saved, merged)
            || (merged.watermark.unwrap_or(i64::MIN) == expected_watermark
                && (!compare_idle || merged.idle == expected_idle))
    }

    pub(super) fn effective_cluster_frontiers(
        &self,
        local: [InputFrontier; 2],
        override_event: Option<(usize, u64, InputFrontier, usize)>,
    ) -> Result<[InputFrontier; 2], DbError> {
        let merge = |port: usize| -> Result<InputFrontier, DbError> {
            let peers = self.peer_channels[port].iter().map(|(&peer, channel)| {
                let mut applied = override_event
                    .filter(|(event_port, event_peer, _, _)| {
                        *event_port == port && *event_peer == peer
                    })
                    .map_or(channel.applied, |(_, _, frontier, _)| frontier);
                let consumed = override_event
                    .filter(|(event_port, event_peer, _, _)| {
                        *event_port == port && *event_peer == peer
                    })
                    .map_or(0, |(_, _, _, consumed)| consumed);
                if channel.events.len() > consumed {
                    applied.idle = false;
                    let floor = if port == 0 {
                        self.applied_left_watermark
                    } else {
                        self.applied_right_watermark
                    };
                    applied.watermark =
                        max_watermark(applied.watermark, Self::watermark_option(floor));
                }
                applied
            });
            let previous = self.applied_frontiers()[port];
            let merged = hold_all_idle_merge(
                merge_input_frontier_iter(std::iter::once(local[port]).chain(peers), i64::MIN),
                previous.watermark,
                self.idle_close_ceilings[port],
            );
            self.validate_frontier(
                previous,
                merged,
                if port == 0 {
                    JoinInputSide::Left
                } else {
                    JoinInputSide::Right
                },
            )?;
            if self.pending_cluster_input.is_some() {
                return Ok(InputFrontier {
                    watermark: previous.watermark,
                    idle: false,
                });
            }
            Ok(merged)
        };
        Ok([merge(0)?, merge(1)?])
    }
}
