//! Prepare acknowledgement collection and deterministic quorum outcomes.

use super::super::{
    canonical_expected_roster, mark_capture_quorum_reached, validate_announcement_attempt,
    BarrierAck, BarrierAckDisposition, BarrierAnnouncement, BarrierCoordinator,
    CheckpointWatermark, Duration, FxHashSet, Instant, NodeId, PeerFailure, PrepareFanoutBatch,
    PrepareFanoutState, QuorumOutcome, ACK_KEY,
};

#[cfg(feature = "cluster")]
use super::super::GrpcState;

fn failed_at(peer: NodeId, error: impl Into<String>) -> QuorumOutcome {
    QuorumOutcome::Failed {
        failures: vec![(peer, error.into())],
    }
}

#[cfg(feature = "cluster")]
fn claim_prepare_fanout(
    state: &GrpcState,
    prepare: &BarrierAnnouncement,
    expected: &[NodeId],
) -> Result<PrepareFanoutBatch, QuorumOutcome> {
    let first = expected.first().copied().unwrap_or(NodeId::UNASSIGNED);
    let mut pending = state.prepare_fanout.lock();
    match pending.take() {
        Some(PrepareFanoutState::Pending(batch))
            if batch.announcement == *prepare && batch.expected == expected =>
        {
            *pending = Some(PrepareFanoutState::Claimed(prepare.clone()));
            Ok(batch)
        }
        Some(current @ PrepareFanoutState::Pending(_)) => {
            let same_announcement = current.announcement() == prepare;
            let claimed = current.announcement().clone();
            *pending = Some(PrepareFanoutState::Claimed(claimed));
            let error = if same_announcement {
                "Prepare quorum roster does not match the announced assignment"
            } else {
                "Prepare quorum does not match the exact announced fan-out"
            };
            Err(failed_at(first, error))
        }
        Some(
            current
            @ (PrepareFanoutState::Claimed(_) | PrepareFanoutState::CaptureQuorumReached(_)),
        ) => {
            let exact = current.announcement() == prepare;
            *pending = Some(current);
            let error = if exact {
                "clustered Prepare fan-out was already claimed or completed"
            } else {
                "Prepare quorum does not match the claimed fan-out"
            };
            Err(failed_at(first, error))
        }
        None => Err(failed_at(
            first,
            "clustered Prepare has no in-flight announced fan-out",
        )),
    }
}

#[cfg(feature = "cluster")]
async fn collect_direct_quorum(
    state: &GrpcState,
    prepare: &BarrierAnnouncement,
    expected: &[NodeId],
    mut batch: PrepareFanoutBatch,
    deadline: Duration,
) -> QuorumOutcome {
    let prepare_deadline = tokio::time::Instant::now() + deadline;
    debug_assert_eq!(batch.tasks.len(), expected.len());
    let mut results = Vec::with_capacity(expected.len());
    loop {
        match tokio::time::timeout_at(prepare_deadline, batch.tasks.join_next()).await {
            Ok(Some(Ok(Err((peer, PeerFailure::Nack(message)))))) => {
                return failed_at(peer, message);
            }
            Ok(Some(Ok(result))) => results.push(result),
            Ok(Some(Err(error))) => {
                return failed_at(
                    NodeId::UNASSIGNED,
                    format!("Prepare RPC task failed: {error}"),
                );
            }
            Ok(None) | Err(_) => break,
        }
    }

    let mut successful = Vec::new();
    let mut failures = Vec::new();
    let mut follower_watermark = None;
    let mut handoff_replay_pending = false;
    let mut timed_out = Vec::new();
    for result in results {
        match result {
            Ok((peer, watermark, replay_pending)) => {
                successful.push(peer);
                handoff_replay_pending |= replay_pending;
                follower_watermark = Some(follower_watermark.map_or(watermark, |current| {
                    CheckpointWatermark::cluster_min(current, watermark)
                }));
            }
            Err((peer, PeerFailure::Unreachable)) => timed_out.push(peer),
            Err((peer, PeerFailure::Nack(message))) => failures.push((peer, message)),
        }
    }
    successful.sort_unstable_by_key(|peer| peer.0);
    failures.sort_unstable_by_key(|(peer, _)| peer.0);
    if !failures.is_empty() {
        return QuorumOutcome::Failed { failures };
    }

    let completed: FxHashSet<NodeId> = successful
        .iter()
        .copied()
        .chain(timed_out.iter().copied())
        .collect();
    timed_out.extend(
        expected
            .iter()
            .copied()
            .filter(|peer| !completed.contains(peer)),
    );
    timed_out.sort_unstable_by_key(|peer| peer.0);
    if !timed_out.is_empty() || successful.len() < expected.len() {
        return QuorumOutcome::TimedOut {
            got: successful,
            missing: timed_out,
        };
    }
    if let Err(error) = mark_capture_quorum_reached(state, prepare) {
        return failed_at(
            expected.first().copied().unwrap_or(NodeId::UNASSIGNED),
            error,
        );
    }
    QuorumOutcome::Reached {
        acks: successful,
        follower_watermark: follower_watermark.unwrap_or(CheckpointWatermark::Uninitialized),
        handoff_replay_pending,
    }
}

impl BarrierCoordinator {
    /// Leader-side: wait until quorum or `deadline`.
    pub async fn wait_for_quorum(
        &self,
        prepare: &BarrierAnnouncement,
        expected: &[NodeId],
        deadline: Duration,
    ) -> QuorumOutcome {
        if let Err(error) = validate_announcement_attempt(prepare) {
            return failed_at(
                expected.first().copied().unwrap_or(NodeId::UNASSIGNED),
                error,
            );
        }
        if prepare.assignment_fence.is_none() {
            return if expected.is_empty() {
                QuorumOutcome::Reached {
                    acks: Vec::new(),
                    follower_watermark: CheckpointWatermark::Uninitialized,
                    handoff_replay_pending: false,
                }
            } else {
                failed_at(
                    expected[0],
                    "remote checkpoint quorum requires an assignment certificate",
                )
            };
        }

        #[cfg(feature = "cluster")]
        let grpc = self.grpc.lock().clone();
        #[cfg(feature = "cluster")]
        if let Some(state) = grpc {
            let expected_roster = match canonical_expected_roster(expected) {
                Ok(roster) => roster,
                Err(error) => {
                    return failed_at(
                        expected.first().copied().unwrap_or(NodeId::UNASSIGNED),
                        error,
                    );
                }
            };
            let batch = match claim_prepare_fanout(&state, prepare, &expected_roster) {
                Ok(batch) => batch,
                Err(outcome) => return outcome,
            };
            return collect_direct_quorum(&state, prepare, &expected_roster, batch, deadline).await;
        }

        self.wait_for_kv_quorum(prepare, expected, deadline).await
    }

    async fn wait_for_kv_quorum(
        &self,
        prepare: &BarrierAnnouncement,
        expected: &[NodeId],
        deadline: Duration,
    ) -> QuorumOutcome {
        let start = Instant::now();
        let expected_set: FxHashSet<NodeId> = expected.iter().copied().collect();
        let assignment_digest = prepare
            .assignment_fence
            .as_ref()
            .map(super::super::super::CheckpointAssignmentFence::digest);
        loop {
            let mut successful = Vec::new();
            let mut failures = Vec::new();
            let mut follower_watermark = None;
            let mut handoff_replay_pending = false;
            for (from, json) in self.kv.scan(ACK_KEY).await {
                if !expected_set.contains(&from) {
                    continue;
                }
                let Ok(ack) = serde_json::from_str::<BarrierAck>(&json) else {
                    continue;
                };
                if ack.epoch != prepare.epoch
                    || ack.checkpoint_id != prepare.checkpoint_id
                    || ack.assignment_digest != assignment_digest
                    || ack.flags != prepare.flags
                {
                    continue;
                }
                record_kv_ack(
                    from,
                    ack,
                    prepare.flags,
                    &mut successful,
                    &mut failures,
                    &mut follower_watermark,
                    &mut handoff_replay_pending,
                );
            }

            if !failures.is_empty() {
                failures.sort_unstable_by_key(|(peer, _)| peer.0);
                return QuorumOutcome::Failed { failures };
            }
            if successful.len() == expected.len() {
                successful.sort_unstable_by_key(|peer| peer.0);
                return QuorumOutcome::Reached {
                    acks: successful,
                    follower_watermark: follower_watermark
                        .unwrap_or(CheckpointWatermark::Uninitialized),
                    handoff_replay_pending,
                };
            }
            if start.elapsed() >= deadline {
                let got_set: FxHashSet<NodeId> = successful.iter().copied().collect();
                let missing = expected
                    .iter()
                    .copied()
                    .filter(|peer| !got_set.contains(peer))
                    .collect();
                let mut got = got_set.into_iter().collect::<Vec<_>>();
                got.sort_unstable_by_key(|peer| peer.0);
                return QuorumOutcome::TimedOut { got, missing };
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn record_kv_ack(
    from: NodeId,
    ack: BarrierAck,
    prepare_flags: u64,
    successful: &mut Vec<NodeId>,
    failures: &mut Vec<(NodeId, String)>,
    follower_watermark: &mut Option<CheckpointWatermark>,
    handoff_replay_pending: &mut bool,
) {
    match ack.disposition {
        BarrierAckDisposition::Captured | BarrierAckDisposition::CapturedWithReplay => {
            let retained_replay = ack.disposition == BarrierAckDisposition::CapturedWithReplay;
            if retained_replay && prepare_flags & crate::checkpoint::flags::HANDOFF == 0 {
                failures.push((
                    from,
                    "Prepare acknowledgement retained replay without the HANDOFF flag".into(),
                ));
            } else if let Err(error) = ack.watermark.validate() {
                failures.push((from, error));
            } else {
                *handoff_replay_pending |= retained_replay;
                successful.push(from);
                *follower_watermark = Some(
                    follower_watermark
                        .map_or(ack.watermark, |current| current.cluster_min(ack.watermark)),
                );
            }
        }
        BarrierAckDisposition::Failed => failures.push((
            from,
            ack.error
                .unwrap_or_else(|| "Prepare acknowledgement has no reason".to_string()),
        )),
        BarrierAckDisposition::Prepared | BarrierAckDisposition::PreparedWithReplay => {
            failures.push((
                from,
                "Prepare requires an explicit Captured acknowledgement from every participant"
                    .into(),
            ));
        }
    }
}
