//! The input channels of a split source that this node reads: every channel on one node, or in a
//! cluster the channels the engine's channel route gives to vnodes this node owns.

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;

use laminar_core::checkpoint::AssignmentDrainId;
use laminar_core::state::{
    NodeId, SourceChannelRoute, VnodeAssignmentSnapshot, VnodeRegistry, MAX_KEY_GROUP_COUNT,
};

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{SourceDrainOutcome, SourceDrainSettlement};
use crate::error::ConnectorError;

/// The route from a split source's channel to the vnode whose owner reads it.
pub(crate) const SPLIT_ROUTE: SourceChannelRoute = SourceChannelRoute::V1;

/// The split count the option `key` sets, if any. More splits than the largest vnode count add
/// no parallelism.
pub(crate) fn configured_split_count(
    config: &ConnectorConfig,
    key: &str,
) -> Result<Option<NonZeroU32>, ConnectorError> {
    match config.get_parsed::<u32>(key)? {
        None => Ok(None),
        Some(splits) if splits > MAX_KEY_GROUP_COUNT => Err(invalid(format!(
            "{key} must be at most {MAX_KEY_GROUP_COUNT}"
        ))),
        Some(splits) => NonZeroU32::new(splits)
            .map(Some)
            .ok_or_else(|| invalid(format!("{key} must be > 0"))),
    }
}

/// The channel of `path` among `splits`, hashed after `domain`: a versioned separator that keeps
/// each connector's assignment of paths to channels its own.
#[cfg(any(feature = "files", feature = "iceberg-core"))]
pub(crate) fn path_channel(domain: &[u8], path: &str, splits: NonZeroU32) -> u32 {
    use laminar_core::state::key_hash;

    let mut key = Vec::with_capacity(domain.len() + path.len());
    key.extend_from_slice(domain);
    key.extend_from_slice(path.as_bytes());
    // The remainder of a division by a u32 count fits in u32.
    #[allow(clippy::cast_possible_truncation)]
    let channel = (key_hash(&key) % u64::from(splits.get())) as u32;
    channel
}

/// Cluster ownership installed by `set_vnode_assignment`.
#[derive(Clone)]
pub(crate) struct SplitAssignment {
    pub source_identity: String,
    pub registry: Arc<VnodeRegistry>,
    pub node: NodeId,
}

impl SplitAssignment {
    pub fn new(
        source_identity: &str,
        registry: Arc<VnodeRegistry>,
        node: NodeId,
    ) -> Result<Self, ConnectorError> {
        if source_identity.is_empty() || node.is_unassigned() {
            return Err(invalid(
                "a vnode assignment requires a source and a node identity",
            ));
        }
        Ok(Self {
            source_identity: source_identity.to_owned(),
            registry,
            node,
        })
    }
}

/// What following an assignment publication did to the owned channels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnershipChange {
    /// No new complete publication to follow.
    Unchanged,
    /// A new assignment version that keeps the same channels.
    Rebound,
    /// The owned channels changed.
    Moved,
}

/// Identity of input channel `index` of the source named `source_name`: channels of one source
/// share a prefix and sort by their big-endian index.
pub(crate) fn source_input_channel(
    source_name: &str,
    index: u32,
) -> Result<Vec<u8>, ConnectorError> {
    let source_len = u32::try_from(source_name.len()).map_err(|_| {
        ConnectorError::ConfigurationError(
            "source identity exceeds the input-channel encoding limit".into(),
        )
    })?;
    let mut channel = Vec::with_capacity(source_name.len() + 8);
    channel.extend_from_slice(&source_len.to_be_bytes());
    channel.extend_from_slice(source_name.as_bytes());
    channel.extend_from_slice(&index.to_be_bytes());
    Ok(channel)
}

/// Identities of channels `0..count` of the source named `source_name`, in channel order.
pub(crate) fn split_input_channels(
    source_name: &str,
    count: NonZeroU32,
) -> Result<Vec<Vec<u8>>, ConnectorError> {
    (0..count.get())
        .map(|channel| source_input_channel(source_name, channel))
        .collect()
}

/// Positions of the owned channels, `P` being the connector's per-channel cursor.
///
/// A connector decodes positions from a cursor that covers every channel (a single-node
/// checkpoint or a cluster cut merged from every participant) with a function of the checkpoint
/// and the channel identities.
pub(crate) struct SplitOwnership<P> {
    count: NonZeroU32,
    /// Encoded identity of each channel, in channel order.
    channels: Vec<Vec<u8>>,
    owned: BTreeMap<u32, P>,
    /// Position of every channel when a cluster source started; empty on one node.
    start: Vec<P>,
    cluster: Option<ClusterOwnership>,
}

struct ClusterOwnership {
    assignment: SplitAssignment,
    /// Assignment version `owned` follows; 0 until the first complete publication.
    reconciled: u64,
    /// Channels this source gave up since it started. Its start position is older than its own
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

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::ConfigurationError(message.into())
}

impl<P: Clone> SplitOwnership<P> {
    /// Start every channel at `start`, one position per channel. Without a cluster assignment
    /// this node reads every channel.
    pub fn new(
        source_name: &str,
        start: Vec<P>,
        assignment: Option<SplitAssignment>,
    ) -> Result<Self, ConnectorError> {
        let count = u32::try_from(start.len())
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| invalid("a split source needs between 1 and u32::MAX channels"))?;
        let channels = split_input_channels(source_name, count)?;
        // Only a cluster source takes channels back at their start position; one node owns every
        // channel from the start and needs no second copy.
        let (owned, start) = if assignment.is_some() {
            (BTreeMap::new(), start)
        } else {
            ((0..).zip(start).collect(), Vec::new())
        };
        Ok(Self {
            count,
            channels,
            owned,
            start,
            cluster: assignment.map(|assignment| ClusterOwnership {
                assignment,
                reconciled: 0,
                released: BTreeSet::new(),
                drain: None,
                fault: None,
            }),
        })
    }

    pub fn count(&self) -> NonZeroU32 {
        self.count
    }

    pub fn is_cluster(&self) -> bool {
        self.cluster.is_some()
    }

    pub fn channel(&self, channel: u32) -> &[u8] {
        &self.channels[channel as usize]
    }

    pub fn owned(&self) -> &BTreeMap<u32, P> {
        &self.owned
    }

    pub fn advance(&mut self, channel: u32, position: P) -> Result<(), ConnectorError> {
        *self.position_mut(channel)? = position;
        Ok(())
    }

    pub fn position_mut(&mut self, channel: u32) -> Result<&mut P, ConnectorError> {
        self.owned.get_mut(&channel).ok_or_else(|| {
            ConnectorError::Internal(format!("split channel {channel} is not owned here"))
        })
    }

    /// Whether `owned` follows the published assignment. A cluster source reads nothing and
    /// admits no barrier until it does.
    pub fn ready(&self) -> Result<bool, ConnectorError> {
        let Some(cluster) = &self.cluster else {
            return Ok(true);
        };
        if let Some(fault) = &cluster.fault {
            return Err(ConnectorError::InvalidState {
                expected: "split channel ownership".into(),
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

    /// Stop the source: `ready` reports `message` from now on.
    pub fn fail(&mut self, message: String) {
        if let Some(cluster) = &mut self.cluster {
            cluster.fault.get_or_insert(message);
        }
    }

    /// Bind a checkpoint of the owned positions to their channels and, in a cluster, to the
    /// assignment version they follow.
    pub fn bind_checkpoint(&self, checkpoint: &mut SourceCheckpoint) {
        checkpoint
            .set_input_channels(
                self.owned
                    .keys()
                    .map(|channel| self.channels[*channel as usize].clone())
                    .collect::<Vec<_>>(),
            )
            .expect("split channels share a prefix and sort by their big-endian index");
        if let Some(version) = self
            .cluster
            .as_ref()
            .and_then(|cluster| NonZeroU64::new(cluster.reconciled))
        {
            checkpoint.bind_assignment_version(version);
        }
    }

    /// Follow a new assignment publication. Outside a drain, an acquired channel starts from the
    /// start position: the source is new to this assignment, as at startup or after recovery.
    pub fn drive<D>(&mut self, decode: D) -> OwnershipChange
    where
        D: Fn(&SourceCheckpoint, &[Vec<u8>]) -> Result<Vec<P>, ConnectorError>,
    {
        let Some(cluster) = self.cluster.as_mut() else {
            return OwnershipChange::Unchanged;
        };
        if cluster.fault.is_some() {
            return OwnershipChange::Unchanged;
        }
        let snapshot = cluster.assignment.registry.versioned_snapshot();
        let version = snapshot.version();
        if version == 0
            || version == cluster.reconciled
            || snapshot.owners().iter().any(NodeId::is_unassigned)
        {
            return OwnershipChange::Unchanged;
        }
        let committed = match &cluster.drain {
            None => None,
            Some(drain) if version == drain.round.predecessor_version => None,
            Some(drain) if version == drain.round.target_version => match &drain.committed {
                // The target is published before its round resolves; wait for the settlement.
                None => return OwnershipChange::Unchanged,
                Some(committed) => Some(Arc::clone(committed)),
            },
            Some(drain) => {
                cluster.fault = Some(format!(
                    "assignment {version} was published during drain {:?}",
                    drain.round
                ));
                return OwnershipChange::Unchanged;
            }
        };
        let settled = committed.is_some();
        let before: Vec<u32> = self.owned.keys().copied().collect();
        if let Err(error) = self.reconcile(&snapshot, committed.as_deref(), &decode) {
            self.fail(error.to_string());
            return OwnershipChange::Unchanged;
        }
        if let Some(cluster) = self.cluster.as_mut().filter(|_| settled) {
            cluster.drain = None;
        }
        if self.owned.keys().copied().eq(before) {
            OwnershipChange::Rebound
        } else {
            OwnershipChange::Moved
        }
    }

    fn reconcile<D>(
        &mut self,
        snapshot: &VnodeAssignmentSnapshot,
        committed: Option<&SourceCheckpoint>,
        decode: &D,
    ) -> Result<(), ConnectorError>
    where
        D: Fn(&SourceCheckpoint, &[Vec<u8>]) -> Result<Vec<P>, ConnectorError>,
    {
        let cluster = self
            .cluster
            .as_ref()
            .expect("reconcile runs only with a cluster assignment");
        let owners = snapshot.owners();
        let vnode_count = u32::try_from(owners.len())
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| invalid("split source assignment has no vnodes"))?;
        let committed = committed
            .map(|checkpoint| decode(checkpoint, &self.channels))
            .transpose()?;
        let mut owned = BTreeMap::new();
        for (channel, start) in (0..).zip(&self.start) {
            let vnode =
                SPLIT_ROUTE.vnode(&cluster.assignment.source_identity, channel, vnode_count);
            if owners[vnode as usize] != cluster.assignment.node {
                continue;
            }
            let position = match (self.owned.get(&channel), &committed) {
                (Some(position), _) => position.clone(),
                (None, Some(committed)) => committed[channel as usize].clone(),
                (None, None) if cluster.released.contains(&channel) => {
                    return Err(invalid(format!(
                        "split channel {channel} moved back to this node outside a drain; \
                         its start position is older than this node's own reads"
                    )));
                }
                (None, None) => start.clone(),
            };
            owned.insert(channel, position);
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
        Ok(())
    }

    pub fn begin_drain(&mut self, round: AssignmentDrainId) -> Result<(), ConnectorError> {
        let cluster = self
            .cluster
            .as_mut()
            .ok_or_else(|| invalid("a split source drains only with a cluster assignment"))?;
        match &cluster.drain {
            Some(active) if active.round != round => Err(ConnectorError::InvalidState {
                expected: format!("split source drain {:?}", active.round),
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

    pub fn ensure_active_drain(&self, round: AssignmentDrainId) -> Result<(), ConnectorError> {
        match self
            .cluster
            .as_ref()
            .and_then(|cluster| cluster.drain.as_ref())
        {
            Some(active) if active.round == round => Ok(()),
            active => Err(ConnectorError::InvalidState {
                expected: format!("split source drain {round:?}"),
                actual: format!("{:?}", active.map(|active| active.round)),
            }),
        }
    }

    /// Resolve the drain. A commit moves acquired channels to the committed handoff cursor once
    /// the target assignment is published.
    pub fn finish_drain<D>(
        &mut self,
        settlement: &SourceDrainSettlement,
        decode: D,
    ) -> Result<OwnershipChange, ConnectorError>
    where
        D: Fn(&SourceCheckpoint, &[Vec<u8>]) -> Result<Vec<P>, ConnectorError>,
    {
        let resolution = settlement.resolution();
        let cluster = self
            .cluster
            .as_mut()
            .ok_or_else(|| invalid("a split source drains only with a cluster assignment"))?;
        let drain = cluster
            .drain
            .as_mut()
            .filter(|drain| drain.round == resolution.round)
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: format!("split source drain {:?}", resolution.round),
                actual: "no matching drain".into(),
            })?;
        match (resolution.outcome, settlement.committed()) {
            (SourceDrainOutcome::Abort, None) => {
                cluster.drain = None;
                Ok(OwnershipChange::Unchanged)
            }
            (SourceDrainOutcome::Commit, Some(committed)) => {
                decode(committed, &self.channels)?;
                drain.committed = Some(Arc::clone(committed));
                let change = self.drive(decode);
                self.ready().map(|_| change)
            }
            (outcome, committed) => Err(ConnectorError::InvalidState {
                expected: format!("{outcome:?} settlement"),
                actual: format!("settlement with cursor: {}", committed.is_some()),
            }),
        }
    }
}

/// Cluster assignments and drain rounds for split-source tests.
#[cfg(test)]
pub(crate) mod test_support {
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::Arc;

    use laminar_core::checkpoint::AssignmentDrainId;
    use laminar_core::state::{NodeId, VnodeRegistry};

    use super::SPLIT_ROUTE;
    use crate::checkpoint::SourceCheckpoint;

    /// Publish `owners`, one node per vnode, as assignment `version`.
    pub fn publish(registry: &VnodeRegistry, owners: &[u64], version: u64) {
        registry.set_assignment_and_version(
            owners
                .iter()
                .copied()
                .map(NodeId)
                .collect::<Vec<_>>()
                .into(),
            version,
        );
    }

    /// The channels of `source` that the route gives `node` under `owners`.
    pub fn routed(source: &str, owners: &[u64], node: u64, channels: u32) -> Vec<u32> {
        let vnodes = NonZeroU32::new(u32::try_from(owners.len()).unwrap()).unwrap();
        (0..channels)
            .filter(|channel| owners[SPLIT_ROUTE.vnode(source, *channel, vnodes) as usize] == node)
            .collect()
    }

    /// The cursor the commit merges from every participant's checkpoint.
    pub fn merge(parts: &[SourceCheckpoint], version: u64) -> Arc<SourceCheckpoint> {
        let mut merged = SourceCheckpoint::new();
        let mut channels = Vec::new();
        for part in parts {
            for (key, value) in part.offsets() {
                merged.set_offset(key.clone(), value.clone());
            }
            channels.extend(part.input_channels().unwrap().iter().cloned());
        }
        for (key, value) in parts[0].metadata() {
            merged.set_metadata(key.clone(), value.clone());
        }
        channels.sort_unstable();
        merged.set_input_channels(channels).unwrap();
        merged.bind_assignment_version(NonZeroU64::new(version).unwrap());
        Arc::new(merged)
    }

    /// The drain round from assignment `predecessor` to the next version.
    pub fn round(predecessor: u64) -> AssignmentDrainId {
        AssignmentDrainId {
            predecessor_version: predecessor,
            target_version: predecessor + 1,
            digest: [7; 32],
        }
    }
}
