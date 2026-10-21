//! Cluster peer-frontier merge for the interval join.

use super::{DbError, InputFrontier, IntervalJoinOperator, JoinInputSide};
use crate::operator::frontier::max_watermark;
use crate::operator_graph::merge_input_frontier_iter;

impl IntervalJoinOperator {
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
            let mut merged =
                merge_input_frontier_iter(std::iter::once(local[port]).chain(peers), i64::MIN);
            let previous = self.applied_frontiers()[port];
            // Idle channels never echo the applied cut, so an all-idle merge can trail it; hold
            // there instead of regressing. A reader idle at its own higher watermark must not
            // close past the committed cut, where a second idle reader revives.
            if merged.idle {
                let closable = match self.idle_close_ceilings[port] {
                    Some(ceiling) => merged.watermark.map(|watermark| watermark.min(ceiling)),
                    None => merged.watermark,
                };
                merged.watermark = max_watermark(closable, previous.watermark);
            }
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
