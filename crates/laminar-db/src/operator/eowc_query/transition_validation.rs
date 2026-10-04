use super::{
    merge_input_frontier_iter, CoreWindowState, DbError, EowcQueryOperator, NodeId,
    VnodeAssignmentSnapshot, REMOTE_EVENT_CHARGE,
};

impl EowcQueryOperator {
    #[cfg(feature = "cluster")]
    pub(super) fn validate_drained_transition_cut(
        &self,
        assignment: &VnodeAssignmentSnapshot,
        window: &CoreWindowState,
        self_id: NodeId,
    ) -> Result<(), DbError> {
        let expected_peers = Self::remote_owner_peers(assignment, self_id);
        if self.cluster_peers.as_ref() != expected_peers.as_slice()
            || self.peer_channels.len() != expected_peers.len()
            || !self
                .peer_channels
                .keys()
                .copied()
                .eq(expected_peers.iter().copied())
            || self
                .remote_peer_cursor
                .is_some_and(|peer| expected_peers.binary_search(&peer).is_err())
            || self.pending_cluster_input.is_some()
            || self.last_broadcast != self.local_frontier
            || self.queued_payload_bytes != 0
            || self.queued_remote_events != 0
            || self.local_frontier.watermark == Some(i64::MIN)
            || self.effective_frontier.watermark == Some(i64::MIN)
            || self.cluster_assignment_digest != Some(self.owner_map_digest(assignment))
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' transition requires a drained frontier and channel cut",
                self.op_name
            )));
        }
        let mut event_capacity_bytes = 0usize;
        for channel in self.peer_channels.values() {
            if channel.applied.watermark == Some(i64::MIN)
                || channel.accepted != channel.applied
                || !channel.events.is_empty()
            {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' transition found retained ordered channel state",
                    self.op_name
                )));
            }
            event_capacity_bytes = event_capacity_bytes
                .checked_add(
                    channel
                        .events
                        .capacity()
                        .checked_mul(REMOTE_EVENT_CHARGE)
                        .ok_or_else(|| self.accounting_error())?,
                )
                .ok_or_else(|| self.accounting_error())?;
        }
        let merged = merge_input_frontier_iter(
            std::iter::once(self.local_frontier)
                .chain(self.peer_channels.values().map(|channel| channel.applied)),
            i64::MIN,
        );
        if event_capacity_bytes != self.queued_event_capacity_bytes
            || merged != self.effective_frontier
            || window.high_watermark_ms() != Self::frontier_watermark(self.effective_frontier)
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' transition found inconsistent channel accounting or frontier",
                self.op_name
            )));
        }
        Ok(())
    }
}
