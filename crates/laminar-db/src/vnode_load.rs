//! The cluster vnode-load report: how each keyed operator's state spreads over vnodes, live on
//! this node and in every participant's frames at the newest committed checkpoint.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use laminar_core::checkpoint::{CheckpointManifest, StateFrameKey};

use crate::db::LaminarDB;
use crate::error::DbError;
use crate::operator_graph::VnodeStateLoad;

const MANIFEST_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// The per-vnode state of every operator that keeps it, as of the graph's last cold sample.
#[derive(Default)]
pub(crate) struct VnodeLoadBoard(parking_lot::Mutex<Option<Arc<VnodeLoadSample>>>);

pub(crate) struct VnodeLoadSample {
    taken_at: Instant,
    operators: Vec<(Arc<str>, Vec<VnodeStateLoad>)>,
}

impl VnodeLoadBoard {
    pub(crate) fn publish(&self, operators: Vec<(Arc<str>, Vec<VnodeStateLoad>)>) {
        let sample = Arc::new(VnodeLoadSample {
            taken_at: Instant::now(),
            operators,
        });
        *self.0.lock() = Some(sample);
    }

    fn latest(&self) -> Option<Arc<VnodeLoadSample>> {
        self.0.lock().clone()
    }

    #[cfg(test)]
    pub(crate) fn latest_for_test(&self, operator: &str) -> Vec<VnodeStateLoad> {
        self.latest()
            .and_then(|sample| {
                sample
                    .operators
                    .iter()
                    .find(|(name, _)| &**name == operator)
                    .map(|(_, vnodes)| vnodes.clone())
            })
            .unwrap_or_default()
    }
}

/// How keyed operator state spreads over vnodes, as one cluster node sees it.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct VnodeLoadReport {
    /// Milliseconds since this node last sampled its live state; `None` before the first
    /// sample. A running pipeline samples every 32 cycles.
    pub live_sample_age_ms: Option<u64>,
    /// The committed checkpoint the frame sizes come from; `None` before the first commit.
    pub committed: Option<VnodeLoadCheckpoint>,
    /// Operators in name order, each with its vnodes in ascending order.
    pub operators: Vec<OperatorVnodeLoad>,
}

/// The committed checkpoint a [`VnodeLoadReport`]'s frame sizes come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct VnodeLoadCheckpoint {
    /// Checkpoint id.
    pub checkpoint_id: u64,
    /// Checkpoint epoch.
    pub epoch: u64,
}

/// One operator's state per vnode.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct OperatorVnodeLoad {
    /// Operator (graph node) name.
    pub operator: String,
    /// Vnodes with live state on this node or a committed frame, in ascending order.
    pub vnodes: Vec<VnodeLoadEntry>,
}

/// One vnode of one operator.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct VnodeLoadEntry {
    /// Vnode id.
    pub vnode: u32,
    /// This node's live state for the vnode; `None` when it holds none.
    pub live: Option<LiveVnodeLoad>,
    /// The vnode's frames at the committed checkpoint, one per participant that wrote one.
    pub committed_frames: Vec<CommittedVnodeFrame>,
}

/// One vnode's state on this node at the last sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct LiveVnodeLoad {
    /// Accounted state bytes, a lower-bound charge.
    pub state_bytes: u64,
    /// Groups; a window key counts once per open window or session.
    pub groups: u64,
    /// Input rows applied to the vnode since the operator was built on this node (start,
    /// recovery or topology change). A row counts once per window it falls in; a global
    /// aggregate counts none.
    pub applied_rows: u64,
}

/// One vnode's state frame in a committed checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct CommittedVnodeFrame {
    /// Participant that wrote the frame.
    pub participant_id: u64,
    /// Frame length in bytes.
    pub frame_bytes: u64,
}

impl LaminarDB {
    /// How keyed operator state spreads over vnodes: this node's live state at the graph's last
    /// metrics sample, and every participant's frame sizes at the newest committed checkpoint
    /// this node knows. Live state needs [`Self::set_engine_metrics`]; frame sizes need
    /// checkpointing.
    ///
    /// # Errors
    ///
    /// Fails when the committed checkpoint's manifests cannot be read within 10 seconds.
    pub async fn cluster_vnode_load(&self) -> Result<VnodeLoadReport, DbError> {
        let sample = self
            .engine_metrics
            .lock()
            .as_ref()
            .and_then(|metrics| metrics.flow.vnode_loads.latest());
        let views = self.mv_store.read().committed_views();
        let manifests = match views {
            Some(views) => tokio::time::timeout(MANIFEST_READ_TIMEOUT, views.latest_manifests())
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "timed out reading the committed checkpoint's manifests".into(),
                    )
                })??,
            None => None,
        };
        Ok(build_report(sample.as_deref(), manifests.as_deref()))
    }
}

type OperatorEntries = BTreeMap<String, BTreeMap<u32, VnodeLoadEntry>>;

fn entry<'a>(
    operators: &'a mut OperatorEntries,
    operator: &str,
    vnode: u32,
) -> &'a mut VnodeLoadEntry {
    operators
        .entry(operator.to_owned())
        .or_default()
        .entry(vnode)
        .or_insert_with(|| VnodeLoadEntry {
            vnode,
            live: None,
            committed_frames: Vec::new(),
        })
}

fn build_report(
    sample: Option<&VnodeLoadSample>,
    manifests: Option<&[CheckpointManifest]>,
) -> VnodeLoadReport {
    let mut operators = OperatorEntries::new();
    for (operator, vnodes) in sample.map_or(&[][..], |sample| &sample.operators) {
        for load in vnodes {
            entry(&mut operators, operator, load.vnode).live = Some(LiveVnodeLoad {
                state_bytes: u64::try_from(load.state_bytes).unwrap_or(u64::MAX),
                groups: u64::try_from(load.groups).unwrap_or(u64::MAX),
                applied_rows: load.applied_rows,
            });
        }
    }
    let manifests = manifests.unwrap_or_default();
    for manifest in manifests {
        for frame in &manifest.state_frames {
            let StateFrameKey::Vnode { operator_id, vnode } = &frame.key else {
                continue;
            };
            let operator = operator_id.strip_prefix("graph:").unwrap_or(operator_id);
            entry(&mut operators, operator, u32::from(*vnode))
                .committed_frames
                .push(CommittedVnodeFrame {
                    participant_id: manifest.participant_id,
                    frame_bytes: frame.range.length,
                });
        }
    }
    VnodeLoadReport {
        live_sample_age_ms: sample
            .map(|sample| u64::try_from(sample.taken_at.elapsed().as_millis()).unwrap_or(u64::MAX)),
        committed: manifests.first().map(|manifest| VnodeLoadCheckpoint {
            checkpoint_id: manifest.checkpoint_id,
            epoch: manifest.epoch,
        }),
        operators: operators
            .into_iter()
            .map(|(operator, vnodes)| OperatorVnodeLoad {
                operator,
                vnodes: vnodes.into_values().collect(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use laminar_core::checkpoint::{ByteRange, StateChunkId, StateFrame};

    use super::*;

    fn manifest(participant_id: u64, frames: &[(StateFrameKey, u64)]) -> CheckpointManifest {
        let mut manifest = CheckpointManifest::new(7, 7);
        manifest.participant_id = participant_id;
        manifest.state_frames = frames
            .iter()
            .map(|(key, length)| StateFrame {
                key: key.clone(),
                chunk: StateChunkId {
                    participant_id,
                    checkpoint_id: 7,
                },
                range: ByteRange {
                    offset: 0,
                    length: *length,
                },
                sha256: String::new(),
            })
            .collect();
        manifest
    }

    fn vnode_frame(operator: &str, vnode: u16) -> StateFrameKey {
        StateFrameKey::Vnode {
            operator_id: format!("graph:{operator}"),
            vnode,
        }
    }

    #[test]
    fn report_joins_live_vnodes_with_every_participants_committed_frames() {
        let sample = VnodeLoadSample {
            taken_at: Instant::now(),
            operators: vec![(
                Arc::from("agg"),
                vec![VnodeStateLoad {
                    vnode: 5,
                    state_bytes: 300,
                    groups: 2,
                    applied_rows: 9,
                }],
            )],
        };
        let manifests = [
            manifest(
                1,
                &[
                    (
                        StateFrameKey::OperatorWhole {
                            operator_id: "graph:agg".into(),
                        },
                        11,
                    ),
                    (vnode_frame("agg", 5), 120),
                    (vnode_frame("view", 1), 40),
                ],
            ),
            manifest(2, &[(vnode_frame("agg", 2), 80)]),
        ];

        let report = build_report(Some(&sample), Some(&manifests));

        assert!(report.live_sample_age_ms.is_some());
        assert_eq!(
            report.committed,
            Some(VnodeLoadCheckpoint {
                checkpoint_id: 7,
                epoch: 7,
            })
        );
        let frame = |participant_id, frame_bytes| CommittedVnodeFrame {
            participant_id,
            frame_bytes,
        };
        assert_eq!(
            report.operators,
            vec![
                OperatorVnodeLoad {
                    operator: "agg".into(),
                    vnodes: vec![
                        VnodeLoadEntry {
                            vnode: 2,
                            live: None,
                            committed_frames: vec![frame(2, 80)],
                        },
                        VnodeLoadEntry {
                            vnode: 5,
                            live: Some(LiveVnodeLoad {
                                state_bytes: 300,
                                groups: 2,
                                applied_rows: 9,
                            }),
                            committed_frames: vec![frame(1, 120)],
                        },
                    ],
                },
                OperatorVnodeLoad {
                    operator: "view".into(),
                    vnodes: vec![VnodeLoadEntry {
                        vnode: 1,
                        live: None,
                        committed_frames: vec![frame(1, 40)],
                    }],
                },
            ]
        );
    }

    #[test]
    fn report_is_empty_before_a_sample_or_a_commit() {
        assert_eq!(build_report(None, None), VnodeLoadReport::default());
    }
}
