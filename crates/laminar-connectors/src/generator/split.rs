//! `splits = n`: channel `c` emits sequence numbers `c, c + n, c + 2n, …`. In a cluster each
//! channel is read by the owner of the vnode the engine's channel route gives it.

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;

use laminar_core::checkpoint::AssignmentDrainId;
use laminar_core::state::{NodeId, SourceChannelRoute, VnodeAssignmentSnapshot, VnodeRegistry};

use super::{generator_input_channel, validate_generator_checkpoint};
use crate::checkpoint::SourceCheckpoint;
use crate::connector::{SourceDrainOutcome, SourceDrainSettlement};
use crate::error::ConnectorError;

pub(super) const SPLIT_CHECKPOINT_VERSION: &str = "3";
pub(super) const SPLITS_METADATA: &str = "splits";
pub(super) const ROUTE: SourceChannelRoute = SourceChannelRoute::V1;

/// Cluster ownership installed by `set_vnode_assignment`.
#[derive(Clone)]
pub(super) struct SplitAssignment {
    pub source_identity: String,
    pub registry: Arc<VnodeRegistry>,
    pub node: NodeId,
}

pub(super) struct SplitInput {
    count: NonZeroU32,
    /// Encoded identity of each channel, in channel order.
    channels: Vec<Vec<u8>>,
    /// Next sequence number of each channel this node reads.
    owned: BTreeMap<u32, u64>,
    /// Next sequence number of every channel when the source started.
    start: Vec<u64>,
    /// Position in `owned` the next poll tries first.
    next_slot: usize,
    cluster: Option<ClusterOwnership>,
}

struct ClusterOwnership {
    assignment: SplitAssignment,
    /// Assignment version `owned` follows; 0 until the first complete publication.
    reconciled: u64,
    /// Channels this source gave up since it started. Its start cursor is older than its own
    /// reads of them, so it can take them back only from a committed handoff cursor.
    released: BTreeSet<u32>,
    drain: Option<SplitDrain>,
    fault: Option<String>,
}

struct SplitDrain {
    round: AssignmentDrainId,
    /// The committed handoff cursor, once the round commits.
    committed: Option<Arc<SourceCheckpoint>>,
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
        let channels = (0..count.get())
            .map(|channel| generator_input_channel(source_name, channel))
            .collect::<Result<Vec<_>, _>>()?;
        let start = match start {
            None => (0..count.get()).map(u64::from).collect(),
            Some(checkpoint) => complete_cursor(checkpoint, count, &channels)?,
        };
        let owned = if assignment.is_some() {
            BTreeMap::new()
        } else {
            (0..count.get()).zip(start.iter().copied()).collect()
        };
        Ok(Self {
            count,
            channels,
            owned,
            start,
            next_slot: 0,
            cluster: assignment.map(|assignment| ClusterOwnership {
                assignment,
                reconciled: 0,
                released: BTreeSet::new(),
                drain: None,
                fault: None,
            }),
        })
    }

    pub fn is_cluster(&self) -> bool {
        self.cluster.is_some()
    }

    pub fn channel(&self, channel: u32) -> &[u8] {
        &self.channels[channel as usize]
    }

    /// Whether `owned` follows the published assignment. A cluster source reads nothing and
    /// admits no barrier until it does.
    pub fn ready(&self) -> Result<bool, ConnectorError> {
        let Some(cluster) = &self.cluster else {
            return Ok(true);
        };
        if let Some(fault) = &cluster.fault {
            return Err(ConnectorError::InvalidState {
                expected: "generator split ownership".into(),
                actual: fault.clone(),
            });
        }
        let version = cluster.assignment.registry.assignment_version();
        Ok(version != 0 && cluster.reconciled == version)
    }

    pub fn is_draining(&self) -> bool {
        self.cluster
            .as_ref()
            .is_some_and(|cluster| cluster.drain.is_some())
    }

    /// The lowest next sequence number among owned channels.
    pub fn min_next(&self) -> Option<u64> {
        self.owned.values().copied().min()
    }

    /// Take up to `limit` rows below `allowed` from the next owned channel that has any.
    pub fn take(&mut self, allowed: u64, limit: usize) -> Option<(u32, Vec<u64>)> {
        let owned: Vec<u32> = self.owned.keys().copied().collect();
        let step = u64::from(self.count.get());
        for offset in 0..owned.len() {
            let slot = (self.next_slot + offset) % owned.len();
            let channel = owned[slot];
            let next = self.owned[&channel];
            if next >= allowed {
                continue;
            }
            let sequences: Vec<u64> = (next..allowed)
                .step_by(usize::try_from(step).unwrap_or(usize::MAX))
                .take(limit)
                .collect();
            let last = *sequences.last()?;
            self.owned.insert(channel, last.saturating_add(step));
            self.next_slot = (slot + 1) % owned.len();
            return Some((channel, sequences));
        }
        None
    }

    pub fn checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        for (channel, next) in &self.owned {
            checkpoint.set_offset(sequence_key(*channel), next.to_string());
        }
        checkpoint.set_metadata("connector", super::GENERATOR_CHECKPOINT_CONNECTOR);
        checkpoint.set_metadata(super::CHECKPOINT_VERSION_METADATA, SPLIT_CHECKPOINT_VERSION);
        checkpoint.set_metadata(SPLITS_METADATA, self.count.to_string());
        checkpoint
            .set_input_channels(
                self.owned
                    .keys()
                    .map(|channel| self.channels[*channel as usize].clone())
                    .collect::<Vec<_>>(),
            )
            .expect("generator channels share a prefix and sort by their big-endian index");
        if let Some(version) = self
            .cluster
            .as_ref()
            .and_then(|cluster| NonZeroU64::new(cluster.reconciled))
        {
            checkpoint.bind_assignment_version(version);
        }
        checkpoint
    }

    /// Follow a new assignment publication. Outside a drain, an acquired channel starts from the
    /// start cursor: the source is new to this assignment, as at startup or after recovery.
    pub fn drive(&mut self) {
        let Some(cluster) = &mut self.cluster else {
            return;
        };
        if cluster.fault.is_some() {
            return;
        }
        let snapshot = cluster.assignment.registry.versioned_snapshot();
        let version = snapshot.version();
        if version == 0
            || version == cluster.reconciled
            || snapshot.owners().iter().any(NodeId::is_unassigned)
        {
            return;
        }
        let committed = match &cluster.drain {
            None => None,
            Some(drain) if version == drain.round.predecessor_version => None,
            Some(drain) if version == drain.round.target_version => match &drain.committed {
                // The target is published before its round resolves; wait for the settlement.
                None => return,
                Some(committed) => Some(Arc::clone(committed)),
            },
            Some(drain) => {
                cluster.fault = Some(format!(
                    "assignment {version} was published during drain {:?}",
                    drain.round
                ));
                return;
            }
        };
        let settled = committed.is_some();
        if let Err(error) = self.reconcile(&snapshot, committed.as_deref()) {
            if let Some(cluster) = &mut self.cluster {
                cluster.fault = Some(error.to_string());
            }
            return;
        }
        if let Some(cluster) = self.cluster.as_mut().filter(|_| settled) {
            cluster.drain = None;
        }
    }

    fn reconcile(
        &mut self,
        snapshot: &VnodeAssignmentSnapshot,
        committed: Option<&SourceCheckpoint>,
    ) -> Result<(), ConnectorError> {
        let cluster = self
            .cluster
            .as_ref()
            .expect("reconcile runs only with a cluster assignment");
        let owners = snapshot.owners();
        let vnode_count = u32::try_from(owners.len())
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| invalid("generator assignment has no vnodes"))?;
        let committed = committed
            .map(|checkpoint| complete_cursor(checkpoint, self.count, &self.channels))
            .transpose()?;
        let mut owned = BTreeMap::new();
        for channel in 0..self.count.get() {
            let vnode = ROUTE.vnode(&cluster.assignment.source_identity, channel, vnode_count);
            if owners[vnode as usize] != cluster.assignment.node {
                continue;
            }
            let next = match (self.owned.get(&channel), &committed) {
                (Some(next), _) => *next,
                (None, Some(committed)) => committed[channel as usize],
                (None, None) if cluster.released.contains(&channel) => {
                    return Err(invalid(format!(
                        "generator channel {channel} moved back to this node outside a drain; \
                         its start cursor is older than this node's own reads"
                    )));
                }
                (None, None) => self.start[channel as usize],
            };
            owned.insert(channel, next);
        }
        let released: Vec<u32> = self
            .owned
            .keys()
            .filter(|channel| !owned.contains_key(channel))
            .copied()
            .collect();
        let cluster = self
            .cluster
            .as_mut()
            .expect("reconcile runs only with a cluster assignment");
        cluster.released.extend(released);
        cluster.reconciled = snapshot.version();
        self.owned = owned;
        self.next_slot = 0;
        Ok(())
    }

    pub fn begin_drain(&mut self, round: AssignmentDrainId) -> Result<(), ConnectorError> {
        let cluster = self
            .cluster
            .as_mut()
            .ok_or_else(|| invalid("generator drain requires a cluster assignment"))?;
        match &cluster.drain {
            Some(active) if active.round != round => Err(ConnectorError::InvalidState {
                expected: format!("generator drain {:?}", active.round),
                actual: format!("drain {round:?}"),
            }),
            Some(_) => Ok(()),
            None => {
                cluster.drain = Some(SplitDrain {
                    round,
                    committed: None,
                });
                Ok(())
            }
        }
    }

    pub fn drain_ready(&self, round: AssignmentDrainId) -> Result<bool, ConnectorError> {
        match self
            .cluster
            .as_ref()
            .and_then(|cluster| cluster.drain.as_ref())
        {
            Some(active) if active.round == round => Ok(true),
            active => Err(ConnectorError::InvalidState {
                expected: format!("generator drain {round:?}"),
                actual: format!("{:?}", active.map(|active| active.round)),
            }),
        }
    }

    /// Resolve the drain. A commit moves acquired channels to the committed handoff cursor once
    /// the target assignment is published.
    pub fn finish_drain(
        &mut self,
        settlement: &SourceDrainSettlement,
    ) -> Result<(), ConnectorError> {
        let resolution = settlement.resolution();
        let cluster = self
            .cluster
            .as_mut()
            .ok_or_else(|| invalid("generator drain requires a cluster assignment"))?;
        let drain = cluster
            .drain
            .as_mut()
            .filter(|drain| drain.round == resolution.round)
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: format!("generator drain {:?}", resolution.round),
                actual: "no matching drain".into(),
            })?;
        match (resolution.outcome, settlement.committed()) {
            (SourceDrainOutcome::Abort, None) => {
                cluster.drain = None;
                Ok(())
            }
            (SourceDrainOutcome::Commit, Some(committed)) => {
                complete_cursor(committed, self.count, &self.channels)?;
                drain.committed = Some(Arc::clone(committed));
                self.drive();
                self.ready().map(|_| ())
            }
            (outcome, committed) => Err(ConnectorError::InvalidState {
                expected: format!("{outcome:?} settlement"),
                actual: format!("settlement with cursor: {}", committed.is_some()),
            }),
        }
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
