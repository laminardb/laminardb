//! `splits = n`: channel `c` emits sequence numbers `c, c + n, c + 2n, …`. In a cluster each
//! channel is read by the owner of the vnode the engine's channel route gives it.

use std::num::NonZeroU32;

use laminar_core::checkpoint::AssignmentDrainId;

use super::validate_generator_checkpoint;
use crate::checkpoint::SourceCheckpoint;
use crate::connector::split_ownership::{
    split_input_channels, OwnershipChange, SplitAssignment, SplitOwnership,
};
use crate::connector::SourceDrainSettlement;
use crate::error::ConnectorError;

pub(super) const SPLIT_CHECKPOINT_VERSION: &str = "3";
pub(super) const SPLITS_METADATA: &str = "splits";

pub(super) struct SplitInput {
    /// Next sequence number of each channel this node reads.
    ownership: SplitOwnership<u64>,
    /// Position in the owned channels the next poll tries first.
    next_slot: usize,
}

fn sequence_key(channel: u32) -> String {
    format!("seq.{channel}")
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::ConfigurationError(message.into())
}

impl SplitInput {
    /// Start every channel from `start` (`None` for a fresh source). Without a cluster assignment
    /// this node reads every channel.
    pub fn new(
        source_name: &str,
        count: NonZeroU32,
        start: Option<&SourceCheckpoint>,
        assignment: Option<SplitAssignment>,
    ) -> Result<Self, ConnectorError> {
        let start = match start {
            None => (0..count.get()).map(u64::from).collect(),
            Some(checkpoint) => complete_cursor(
                checkpoint,
                count,
                &split_input_channels(source_name, count)?,
            )?,
        };
        Ok(Self {
            ownership: SplitOwnership::new(source_name, start, assignment)?,
            next_slot: 0,
        })
    }

    pub fn is_cluster(&self) -> bool {
        self.ownership.is_cluster()
    }

    pub fn channel(&self, channel: u32) -> &[u8] {
        self.ownership.channel(channel)
    }

    /// Whether the owned channels follow the published assignment. A cluster source reads
    /// nothing and admits no barrier until they do.
    pub fn ready(&self) -> Result<bool, ConnectorError> {
        self.ownership.ready()
    }

    pub fn is_draining(&self) -> bool {
        self.ownership.is_draining()
    }

    /// The lowest next sequence number among owned channels.
    pub fn min_next(&self) -> Option<u64> {
        self.ownership.owned().values().copied().min()
    }

    /// Take up to `limit` rows below `allowed` from the next owned channel that has any.
    pub fn take(&mut self, allowed: u64, limit: usize) -> Option<(u32, Vec<u64>)> {
        let owned: Vec<(u32, u64)> = self
            .ownership
            .owned()
            .iter()
            .map(|(channel, next)| (*channel, *next))
            .collect();
        let step = u64::from(self.ownership.count().get());
        for offset in 0..owned.len() {
            let slot = (self.next_slot + offset) % owned.len();
            let (channel, next) = owned[slot];
            if next >= allowed {
                continue;
            }
            let sequences: Vec<u64> = (next..allowed)
                .step_by(usize::try_from(step).unwrap_or(usize::MAX))
                .take(limit)
                .collect();
            let last = *sequences.last()?;
            self.ownership
                .advance(channel, last.saturating_add(step))
                .expect("the channel was read from the owned set");
            self.next_slot = (slot + 1) % owned.len();
            return Some((channel, sequences));
        }
        None
    }

    pub fn checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        for (channel, next) in self.ownership.owned() {
            checkpoint.set_offset(sequence_key(*channel), next.to_string());
        }
        checkpoint.set_metadata("connector", super::GENERATOR_CHECKPOINT_CONNECTOR);
        checkpoint.set_metadata(super::CHECKPOINT_VERSION_METADATA, SPLIT_CHECKPOINT_VERSION);
        checkpoint.set_metadata(SPLITS_METADATA, self.ownership.count().to_string());
        self.ownership.bind_checkpoint(&mut checkpoint);
        checkpoint
    }

    pub fn drive(&mut self) {
        let count = self.ownership.count();
        let change = self
            .ownership
            .drive(|checkpoint, channels| complete_cursor(checkpoint, count, channels));
        if change == OwnershipChange::Moved {
            self.next_slot = 0;
        }
    }

    pub fn begin_drain(&mut self, round: AssignmentDrainId) -> Result<(), ConnectorError> {
        self.ownership.begin_drain(round)
    }

    // Each poll hands over whole rows, so the generator is ready as soon as it drains.
    pub fn drain_ready(&self, round: AssignmentDrainId) -> Result<bool, ConnectorError> {
        self.ownership.ensure_active_drain(round).map(|()| true)
    }

    /// Resolve the drain. A commit moves acquired channels to the committed handoff cursor once
    /// the target assignment is published.
    pub fn finish_drain(
        &mut self,
        settlement: &SourceDrainSettlement,
    ) -> Result<(), ConnectorError> {
        let count = self.ownership.count();
        let change = self
            .ownership
            .finish_drain(settlement, |checkpoint, channels| {
                complete_cursor(checkpoint, count, channels)
            })?;
        if change == OwnershipChange::Moved {
            self.next_slot = 0;
        }
        Ok(())
    }
}

/// The next sequence number of every channel in a cursor that covers all of them: a single-node
/// checkpoint or a cluster cut merged from every participant.
fn complete_cursor(
    checkpoint: &SourceCheckpoint,
    count: NonZeroU32,
    channels: &[Vec<u8>],
) -> Result<Vec<u64>, ConnectorError> {
    validate_generator_checkpoint(checkpoint, SPLIT_CHECKPOINT_VERSION)?;
    if checkpoint.get_metadata(SPLITS_METADATA) != Some(count.to_string().as_str()) {
        return Err(invalid(format!(
            "generator checkpoint has {} splits, the source is configured with {count}",
            checkpoint.get_metadata(SPLITS_METADATA).unwrap_or("no")
        )));
    }
    if checkpoint.input_channels() != Some(channels) || checkpoint.offsets().len() != channels.len()
    {
        return Err(invalid(
            "generator checkpoint does not cover every split channel",
        ));
    }
    let step = u64::from(count.get());
    (0..count.get())
        .map(|channel| {
            let key = sequence_key(channel);
            let next = checkpoint
                .get_offset(&key)
                .ok_or_else(|| invalid(format!("generator checkpoint is missing '{key}'")))?;
            let next: u64 = next.parse().map_err(|error| {
                invalid(format!("bad generator offset '{key}={next}': {error}"))
            })?;
            if next % step != u64::from(channel) {
                return Err(invalid(format!(
                    "generator offset '{key}={next}' is not a sequence number of channel {channel}"
                )));
            }
            Ok(next)
        })
        .collect()
}
