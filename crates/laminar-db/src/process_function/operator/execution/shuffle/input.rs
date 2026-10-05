use arrow::array::{Array, TimestampMicrosecondArray};
use laminar_core::state::PartitionKeyCodecV1;

use super::{
    PeerEvent, PeerInput, ProcessExecution, ProcessFunctionOperator, ProcessShuffle, ShuffleState,
};
use crate::error::DbError;
use crate::operator::RetainedBatch;
use crate::operator_graph::InputFrontier;

pub(super) fn validate_frontier(
    previous: InputFrontier,
    next: InputFrontier,
) -> Result<(), DbError> {
    if next.watermark == Some(i64::MIN)
        || (previous.watermark.is_some() && next.watermark.is_none())
        || matches!((previous.watermark, next.watermark), (Some(before), Some(after)) if after < before)
    {
        return Err(DbError::ShuffleTerminal(format!(
            "process shuffle frontier regressed or became uninitialized: {previous:?} -> {next:?}"
        )));
    }
    Ok(())
}

impl ProcessShuffle {
    fn require_peer(&self, stage: &str, peer: u64) -> Result<InputFrontier, DbError> {
        if stage != self.stage {
            return Err(DbError::ShuffleTerminal(
                "process shuffle stage differs from its operator".into(),
            ));
        }
        self.peers
            .get(&peer)
            .map(|channel| channel.accepted)
            .ok_or_else(|| {
                DbError::ShuffleTerminal(
                    "process shuffle came from a peer outside its assignment".into(),
                )
            })
    }

    fn retain(
        &mut self,
        event: PeerEvent,
        max_rows: usize,
        max_bytes: usize,
        available: usize,
    ) -> Result<(), DbError> {
        let rows = self
            .queued_rows
            .checked_add(self.pending_rows())
            .and_then(|rows| rows.checked_add(event.rows))
            .ok_or_else(budget_error)?;
        let bytes = self
            .queued_bytes
            .checked_add(event.bytes)
            .ok_or_else(budget_error)?;
        let max_events = max_rows
            .saturating_mul(2)
            .saturating_add(self.peers.len().saturating_mul(2));
        if rows > max_rows || bytes > max_bytes || self.events.len() >= max_events {
            return Err(budget_error());
        }
        self.events.try_reserve(1).map_err(|_| budget_error())?;
        if self
            .retained_bytes()
            .checked_add(event.bytes)
            .is_none_or(|bytes| bytes > available)
        {
            self.events.shrink_to_fit();
            return Err(budget_error());
        }
        let channel = self.peers.get_mut(&event.peer).ok_or_else(|| {
            DbError::ShuffleTerminal("process shuffle retention lost its peer".into())
        })?;
        let next_queued = channel.queued.checked_add(1).ok_or_else(budget_error)?;
        if let PeerInput::Frontier(frontier) = event.input {
            channel.accepted = frontier;
        }
        channel.queued = next_queued;
        self.queued_bytes = bytes;
        self.queued_rows += event.rows;
        self.events.push_back(event);
        Ok(())
    }
}

impl ProcessFunctionOperator {
    pub(in super::super::super::super) fn retain_shuffle_data(
        &mut self,
        stage: &str,
        batch: RetainedBatch,
    ) -> Result<(), DbError> {
        self.require_execution_current()?;
        let ProcessExecution::Distributed(authority) = &self.execution else {
            return Err(DbError::ShuffleTerminal(
                "process operator has no cross-node shuffle authority".into(),
            ));
        };
        let peer = batch
            .peer()
            .ok_or_else(|| DbError::ShuffleTerminal("process shuffle data is unscoped".into()))?;
        if batch.assignment_version() != Some(authority.assignment.version())
            || batch.recovery_gen() != Some(authority.recovery_generation)
        {
            return Err(DbError::ShuffleTerminal(
                "process shuffle data belongs to a stale assignment or recovery".into(),
            ));
        }
        let ShuffleState::Active(shuffle) = &self.shuffle else {
            return Err(DbError::ShuffleTerminal(
                "process shuffle state is not bound".into(),
            ));
        };
        let accepted = shuffle.require_peer(stage, peer)?;
        let earliest_us = self.validate_peer_batch(&batch, accepted)?;
        let rows = batch.num_rows();
        let bytes = batch.heap_bytes().ok_or_else(budget_error)?;
        let available = self
            .graph_budget
            .checked_sub(self.live_bytes)
            .ok_or_else(budget_error)?;
        let ShuffleState::Active(shuffle) = &mut self.shuffle else {
            unreachable!("validated active process shuffle");
        };
        shuffle.retain(
            PeerEvent {
                peer,
                input: PeerInput::Data(batch),
                bytes,
                rows,
                hold_ms: Some(earliest_us.saturating_sub(1).div_euclid(1_000)),
            },
            self.descriptor.limits.max_input_rows,
            self.descriptor.limits.max_input_bytes,
            available,
        )?;
        self.require_execution_current()
    }

    fn validate_peer_batch(
        &self,
        batch: &RetainedBatch,
        frontier: InputFrontier,
    ) -> Result<i64, DbError> {
        if frontier.idle
            || batch.num_rows() == 0
            || batch.routed_vnodes().is_empty()
            || batch
                .routed_vnodes()
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(DbError::ShuffleTerminal(
                "process shuffle data has an idle channel or invalid route set".into(),
            ));
        }
        self.validate_routed_batches(std::iter::once(batch.batch()))?;
        let ProcessExecution::Distributed(authority) = &self.execution else {
            unreachable!("peer validation requires distributed authority");
        };
        if batch.routed_vnodes().iter().any(|&vnode| {
            authority.assignment.owners().get(vnode as usize) != Some(&authority.config.self_id)
        }) {
            return Err(DbError::ShuffleTerminal(
                "process shuffle names a vnode outside local ownership".into(),
            ));
        }
        let columns = self
            .key_indices
            .iter()
            .map(|&index| std::sync::Arc::clone(batch.column(index)))
            .collect::<Vec<_>>();
        let keys = self.key_codec.encode_columns(&columns).map_err(|error| {
            DbError::ShuffleTerminal(format!("process shuffle key encoding: {error}"))
        })?;
        let scratch = keys.size().saturating_add(batch.routed_vnodes().len());
        if self
            .live_bytes
            .saturating_add(self.shuffle.retained_bytes())
            .saturating_add(scratch)
            .saturating_add(batch.heap_bytes().ok_or_else(budget_error)?)
            > self.graph_budget
        {
            return Err(budget_error());
        }
        let time = batch
            .column(self.time_index)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| {
                DbError::ShuffleTerminal("process shuffle has an invalid event-time array".into())
            })?;
        let mut seen = vec![false; batch.routed_vnodes().len()];
        let mut earliest_us = i64::MAX;
        for (row, key) in keys.iter().enumerate() {
            let vnode = PartitionKeyCodecV1::vnode_for_encoded(key.data(), self.vnode_count);
            let slot = batch.routed_vnodes().binary_search(&vnode).map_err(|_| {
                DbError::ShuffleTerminal(
                    "process shuffle route set omits a row's canonical vnode".into(),
                )
            })?;
            seen[slot] = true;
            earliest_us = earliest_us.min(time.value(row));
            if time.is_null(row)
                || frontier
                    .watermark
                    .is_some_and(|ms| time.value(row) < ms.saturating_mul(1_000))
            {
                return Err(DbError::ShuffleTerminal(
                    "process shuffle data precedes its accepted frontier".into(),
                ));
            }
        }
        if seen.iter().any(|seen| !seen) {
            return Err(DbError::ShuffleTerminal(
                "process shuffle route set names an absent vnode".into(),
            ));
        }
        Ok(earliest_us)
    }

    pub(in super::super::super::super) fn retain_shuffle_frontier(
        &mut self,
        stage: &str,
        peer: u64,
        frontier: InputFrontier,
        assignment_version: u64,
        recovery_gen: u64,
    ) -> Result<(), DbError> {
        self.require_execution_current()?;
        let ProcessExecution::Distributed(authority) = &self.execution else {
            return Err(DbError::ShuffleTerminal(
                "process operator has no cross-node shuffle authority".into(),
            ));
        };
        if assignment_version != authority.assignment.version()
            || recovery_gen != authority.recovery_generation
        {
            return Err(DbError::ShuffleTerminal(
                "process shuffle frontier belongs to a stale assignment or recovery".into(),
            ));
        }
        let available = self
            .graph_budget
            .checked_sub(self.live_bytes)
            .ok_or_else(budget_error)?;
        let ShuffleState::Active(shuffle) = &mut self.shuffle else {
            return Err(DbError::ShuffleTerminal(
                "process shuffle state is not bound".into(),
            ));
        };
        let previous = shuffle.require_peer(stage, peer)?;
        if frontier.watermark == Some(i64::MIN)
            || (previous.watermark.is_some() && frontier.watermark.is_none())
        {
            validate_frontier(previous, frontier)?;
        }
        let watermark = match (frontier.watermark, shuffle.effective.watermark) {
            (Some(observed), Some(floor)) => Some(observed.max(floor)),
            (observed, None) => observed,
            (None, floor) => floor,
        };
        let frontier = InputFrontier {
            watermark,
            ..frontier
        };
        validate_frontier(previous, frontier)?;
        shuffle.retain(
            PeerEvent {
                peer,
                input: PeerInput::Frontier(frontier),
                bytes: 0,
                rows: 0,
                hold_ms: None,
            },
            self.descriptor.limits.max_input_rows,
            self.descriptor.limits.max_input_bytes,
            available,
        )?;
        self.require_execution_current()
    }
}

pub(super) fn budget_error() -> DbError {
    DbError::BackpressureFail("process shuffle input or retained-state budget exceeded".into())
}
