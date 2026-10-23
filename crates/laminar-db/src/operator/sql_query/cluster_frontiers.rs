//! Cluster peer-frontier merge for the managed aggregate.

use super::{merge_input_frontier_iter, DbError, InputFrontier, SqlQueryOperator};
use crate::operator::frontier::hold_all_idle_merge;

impl SqlQueryOperator {
    pub(super) fn effective_cluster_frontier(
        &self,
        local: InputFrontier,
        consumed: Option<(u64, InputFrontier, bool)>,
    ) -> Result<InputFrontier, DbError> {
        let remote = self.peer_channels.iter().map(|(&peer, channel)| {
            let (mut applied, pending) = if let Some((target, applied, pending)) = consumed {
                if target == peer {
                    (applied, pending)
                } else {
                    (channel.applied, !channel.events.is_empty())
                }
            } else {
                (channel.applied, !channel.events.is_empty())
            };
            if pending {
                applied.idle = false;
                applied.watermark =
                    Self::max_watermark(applied.watermark, self.effective_frontier.watermark);
            }
            applied
        });
        let merged = hold_all_idle_merge(
            merge_input_frontier_iter(std::iter::once(local).chain(remote), i64::MIN),
            self.effective_frontier.watermark,
            self.idle_close_ceiling,
        );
        self.validate_frontier(self.effective_frontier, merged, "effective")?;
        if self.pending_cluster_input.is_some() {
            return Ok(InputFrontier {
                watermark: self.effective_frontier.watermark,
                idle: false,
            });
        }
        Ok(merged)
    }
}
