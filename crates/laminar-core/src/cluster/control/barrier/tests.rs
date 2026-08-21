#[cfg(feature = "cluster")]
use super::grpc::{
    ack_disposition_to_wire, assignment_fence_from_wire, leader_proof_challenge_from_wire,
    leader_proof_from_wire, send_phase_rpc, validate_phase_ack, BarrierClientEntry,
};
#[cfg(feature = "cluster")]
use super::prepare::{retryable_prepare_status, validate_capture_ack};
use super::protocol::announcement_attempt;
#[cfg(feature = "cluster")]
use super::protocol::BarrierEndpointRecord;
use super::*;

fn kv(id: NodeId) -> Arc<InMemoryKv> {
    Arc::new(InMemoryKv::new(id))
}

fn test_fence(
    assignment_version: u64,
    owners: &[u64],
    participants: &[(u64, u128)],
) -> crate::checkpoint::CheckpointAssignmentFence {
    crate::checkpoint::CheckpointAssignmentFence::from_owner_map(
        assignment_version,
        owners,
        participants
            .iter()
            .map(
                |(node_id, incarnation)| crate::checkpoint::CheckpointParticipant {
                    node_id: *node_id,
                    boot_incarnation: uuid::Uuid::from_u128(*incarnation),
                },
            )
            .collect(),
    )
    .unwrap()
}

#[cfg(feature = "cluster")]
#[test]
fn leader_proof_challenge_and_ack_require_one_exact_fresh_id() {
    let challenge = uuid::Uuid::from_u128(17);
    assert_eq!(
        leader_proof_challenge_from_wire(challenge.as_bytes()).unwrap(),
        challenge
    );
    assert!(leader_proof_ack_matches(challenge, challenge.as_bytes()));
    assert!(!leader_proof_ack_matches(
        challenge,
        uuid::Uuid::from_u128(18).as_bytes()
    ));
    assert!(!leader_proof_ack_matches(challenge, &[1; 15]));
    assert!(leader_proof_challenge_from_wire(&[0; 16]).is_err());
    assert!(leader_proof_challenge_from_wire(&[1; 15]).is_err());
}

#[cfg(feature = "cluster")]
#[test]
fn process_bound_endpoint_advertisement_is_strict_and_bounded() {
    let process = BarrierProcessIdentity {
        node_id: 7,
        boot_incarnation: uuid::Uuid::from_u128(70),
        process_term: 9,
    };
    let encoded = BarrierEndpointRecord::new("127.0.0.1:9000".into(), process)
        .unwrap()
        .encode()
        .unwrap();
    assert_eq!(
        decode_barrier_endpoint(&encoded).unwrap(),
        ("127.0.0.1:9000".into(), Some(process))
    );
    assert_eq!(
        decode_barrier_endpoint("127.0.0.1:9001").unwrap(),
        ("127.0.0.1:9001".into(), None)
    );

    let mut wrong_version: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    wrong_version["version"] = serde_json::json!(2);
    assert!(decode_barrier_endpoint(&wrong_version.to_string()).is_err());

    let mut unknown_field: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    unknown_field["unexpected"] = serde_json::json!(true);
    assert!(decode_barrier_endpoint(&unknown_field.to_string()).is_err());

    let mut nil_boot: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    nil_boot["process"]["boot_incarnation"] = serde_json::json!(uuid::Uuid::nil());
    assert!(decode_barrier_endpoint(&nil_boot.to_string()).is_err());
    assert!(decode_barrier_endpoint(&"x".repeat(MAX_BARRIER_ENDPOINT_BYTES + 1)).is_err());
}

#[cfg(feature = "cluster")]
#[test]
fn wire_watermark_requires_an_exact_status_value_shape() {
    use barrier_v1::CheckpointWatermarkStatus as WireStatus;

    assert_eq!(
        checkpoint_watermark_from_wire(WireStatus::CheckpointWatermarkUninitialized as i32, None,)
            .unwrap(),
        CheckpointWatermark::Uninitialized
    );
    assert_eq!(
        checkpoint_watermark_from_wire(WireStatus::CheckpointWatermarkIdle as i32, None,).unwrap(),
        CheckpointWatermark::Idle
    );
    assert_eq!(
        checkpoint_watermark_from_wire(WireStatus::CheckpointWatermarkActive as i32, Some(10),)
            .unwrap(),
        CheckpointWatermark::Active(10)
    );
    assert!(
        checkpoint_watermark_from_wire(WireStatus::CheckpointWatermarkIdle as i32, Some(10),)
            .is_err()
    );
    assert!(
        checkpoint_watermark_from_wire(WireStatus::CheckpointWatermarkActive as i32, None,)
            .is_err()
    );
    assert!(checkpoint_watermark_from_wire(99, None).is_err());
}

#[cfg(feature = "cluster")]
#[test]
fn prepare_retry_classification_keeps_fence_failures_semantic() {
    for code in [
        tonic::Code::Unknown,
        tonic::Code::Unavailable,
        tonic::Code::DeadlineExceeded,
        tonic::Code::Cancelled,
        tonic::Code::Aborted,
    ] {
        assert!(retryable_prepare_status(&tonic::Status::new(code, "")));
    }

    for code in [
        tonic::Code::PermissionDenied,
        tonic::Code::FailedPrecondition,
        tonic::Code::InvalidArgument,
        tonic::Code::ResourceExhausted,
        tonic::Code::Internal,
    ] {
        assert!(!retryable_prepare_status(&tonic::Status::new(code, "")));
    }
}

#[cfg(feature = "cluster")]
#[test]
fn eager_prepare_retry_budget_keeps_exact_attempt_deadline_and_short_rpc_cadence() {
    let attempt_deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let default = prepare_fanout_budget(attempt_deadline, Duration::from_secs(3)).unwrap();
    assert_eq!(default.per_attempt, Duration::from_millis(1_500));
    assert_eq!(default.deadline, attempt_deadline);

    let extended = prepare_fanout_budget(attempt_deadline, Duration::from_secs(40)).unwrap();
    assert_eq!(extended.per_attempt, Duration::from_secs(20));
    assert_eq!(extended.deadline, attempt_deadline);
    assert!(prepare_fanout_budget(attempt_deadline, Duration::ZERO).is_err());
    assert!(prepare_fanout_budget(attempt_deadline, Duration::from_nanos(1)).is_err());
    assert!(prepare_fanout_budget(tokio::time::Instant::now(), Duration::from_secs(3)).is_err());
}

#[cfg(feature = "cluster")]
#[test]
fn assignment_fence_wire_round_trip_preserves_exact_map_and_processes() {
    let fence = test_fence(17, &[1, 2, 1, 2], &[(1, 11), (2, 22)]);
    let wire = assignment_fence_to_wire(Some(&fence));
    let decoded = assignment_fence_from_wire(
        wire.version,
        wire.vnode_count,
        wire.map_digest,
        wire.participants,
    )
    .unwrap();
    assert_eq!(decoded, Some(fence));

    assert_eq!(
        assignment_fence_from_wire(0, 0, Vec::new(), Vec::new()).unwrap(),
        None
    );
}

#[cfg(feature = "cluster")]
#[test]
fn successor_terminal_targets_historical_proof_owner_and_excludes_actual_sender() {
    let announcement = BarrierAnnouncement {
        epoch: 9,
        checkpoint_id: 9,
        assignment_fence: Some(test_fence(17, &[1, 2, 3], &[(1, 11), (2, 22), (3, 33)])),
        leader_proof: Some(crate::cluster::control::LeaderProof {
            owner: crate::checkpoint::LeaderProofOwner {
                node_id: 1,
                boot_id: uuid::Uuid::from_u128(11),
                process_term: 7,
            },
            fencing_token: 9,
        }),
        phase: Phase::Commit,
        flags: 0,
    };

    assert_eq!(
        clustered_phase_roster(
            &announcement,
            Some(BarrierProcessIdentity {
                node_id: 3,
                boot_incarnation: uuid::Uuid::from_u128(33),
                process_term: 8,
            }),
        )
        .unwrap(),
        Some(vec![NodeId(1), NodeId(2)])
    );
}

#[cfg(feature = "cluster")]
#[test]
fn restarted_same_node_terminal_skips_the_unaddressable_predecessor() {
    let announcement = BarrierAnnouncement {
        epoch: 9,
        checkpoint_id: 9,
        assignment_fence: Some(test_fence(17, &[1, 2], &[(1, 11), (2, 22)])),
        leader_proof: Some(crate::cluster::control::LeaderProof {
            owner: crate::checkpoint::LeaderProofOwner {
                node_id: 1,
                boot_id: uuid::Uuid::from_u128(11),
                process_term: 7,
            },
            fencing_token: 9,
        }),
        phase: Phase::Commit,
        flags: 0,
    };

    assert_eq!(
        clustered_phase_roster(
            &announcement,
            Some(BarrierProcessIdentity {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(111),
                process_term: 8,
            }),
        )
        .unwrap(),
        Some(vec![NodeId(2)])
    );
}

#[cfg(feature = "cluster")]
#[test]
fn restarted_same_node_cannot_send_aligned_with_the_predecessor_proof() {
    let announcement = BarrierAnnouncement {
        epoch: 9,
        checkpoint_id: 9,
        assignment_fence: Some(test_fence(17, &[1, 2], &[(1, 11), (2, 22)])),
        leader_proof: Some(crate::cluster::control::LeaderProof {
            owner: crate::checkpoint::LeaderProofOwner {
                node_id: 1,
                boot_id: uuid::Uuid::from_u128(11),
                process_term: 7,
            },
            fencing_token: 9,
        }),
        phase: Phase::Aligned,
        flags: 0,
    };

    let error = clustered_phase_roster(
        &announcement,
        Some(BarrierProcessIdentity {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(111),
            process_term: 8,
        }),
    )
    .unwrap_err();
    assert!(
        error.contains("does not own its live leader proof"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[test]
fn assignment_fence_wire_rejects_partial_and_noncanonical_certificates() {
    let fence = test_fence(17, &[1, 2], &[(1, 11), (2, 22)]);

    let mut wrong_digest_length = assignment_fence_to_wire(Some(&fence));
    wrong_digest_length.map_digest.pop();
    assert!(assignment_fence_from_wire(
        wrong_digest_length.version,
        wrong_digest_length.vnode_count,
        wrong_digest_length.map_digest,
        wrong_digest_length.participants,
    )
    .is_err());

    let mut wrong_incarnation_length = assignment_fence_to_wire(Some(&fence));
    wrong_incarnation_length.participants[0]
        .boot_incarnation
        .pop();
    assert!(assignment_fence_from_wire(
        wrong_incarnation_length.version,
        wrong_incarnation_length.vnode_count,
        wrong_incarnation_length.map_digest,
        wrong_incarnation_length.participants,
    )
    .is_err());

    let mut unordered = assignment_fence_to_wire(Some(&fence));
    unordered.participants.swap(0, 1);
    assert!(assignment_fence_from_wire(
        unordered.version,
        unordered.vnode_count,
        unordered.map_digest,
        unordered.participants,
    )
    .is_err());

    assert!(assignment_fence_from_wire(17, 0, Vec::new(), Vec::new()).is_err());
}

#[cfg(feature = "cluster")]
#[test]
fn assignment_fence_wire_rejects_oversized_forged_certificate() {
    let maximum = u64::try_from(crate::checkpoint::MAX_CHECKPOINT_PARTICIPANTS).unwrap();
    let participants = (1..=maximum + 1)
        .map(|node_id| barrier_v1::CheckpointParticipant {
            node_id,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id))
                .as_bytes()
                .to_vec(),
        })
        .collect();

    let error = assignment_fence_from_wire(17, 1, vec![1; 32], participants).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("Non-canonical"));
}

#[cfg(feature = "cluster")]
#[test]
fn leader_proof_wire_round_trip_preserves_exact_process_term() {
    let proof = super::super::LeaderProof {
        owner: crate::checkpoint::LeaderProofOwner {
            node_id: 7,
            boot_id: uuid::Uuid::from_u128(70),
            process_term: 9,
        },
        fencing_token: 11,
    };
    let decoded = leader_proof_from_wire(leader_proof_to_wire(Some(&proof))).unwrap();
    assert_eq!(decoded.as_ref(), Some(&proof));
    assert_eq!(leader_proof_from_wire(None).unwrap(), None);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn reversible_barrier_rejects_same_node_new_boot_and_old_token() {
    use object_store::memory::InMemory;

    let store = Arc::new(super::super::LeaderLeaseStore::new(
        Arc::new(InMemory::new()),
        1,
    ));
    let original = super::super::LeaderLeaseOwner {
        node: NodeId(7),
        boot: uuid::Uuid::from_u128(70),
        process_term: 1,
    };
    let replacement = super::super::LeaderLeaseOwner {
        node: NodeId(7),
        boot: uuid::Uuid::from_u128(71),
        process_term: 2,
    };
    let original_lease = match store.begin_new_term(&original, 1).await.unwrap() {
        super::super::LeaseOutcome::Acquired(lease) => lease,
        super::super::LeaseOutcome::Held(_) => unreachable!(),
    };
    let observation = store.observe_rival(&replacement, &original_lease).unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let replacement_lease = match store
        .try_takeover(&replacement, &observation, 2)
        .await
        .unwrap()
    {
        super::super::LeaseOutcome::Acquired(lease) => lease,
        super::super::LeaseOutcome::Held(_) => unreachable!(),
    };

    let (incoming_tx, _incoming_rx) = crossfire::mpsc::bounded_async::<BarrierAnnouncement>(1);
    let server = GrpcBarrierServer {
        incoming_tx,
        prepare_acks: Arc::new(parking_lot::Mutex::new(PrepareAckState::default())),
        leader_lease_store: Arc::new(parking_lot::Mutex::new(Some(Arc::clone(&store)))),
        local_leader_proof: Arc::new(parking_lot::Mutex::new(None)),
        local_process: Arc::new(std::sync::OnceLock::new()),
        process_lease_deadline: Arc::new(std::sync::OnceLock::new()),
    };
    assert!(server
        .require_latest_proof(&replacement_lease.proof())
        .await
        .is_ok());

    assert!(server
        .require_latest_proof(&original_lease.proof())
        .await
        .is_err());
    let wrong_boot = super::super::LeaderProof {
        owner: crate::checkpoint::LeaderProofOwner {
            node_id: replacement_lease.owner.node.0,
            boot_id: uuid::Uuid::from_u128(72),
            process_term: replacement_lease.owner.process_term,
        },
        fencing_token: replacement_lease.token,
    };
    assert!(server.require_latest_proof(&wrong_boot).await.is_err());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn reversible_announcement_uses_only_an_exact_live_local_proof() {
    use object_store::memory::InMemory;

    let coordinator = BarrierCoordinator::new(kv(NodeId(1)));
    coordinator.set_leader_lease_store(Arc::new(super::super::LeaderLeaseStore::new(
        Arc::new(InMemory::new()),
        1_000,
    )));
    let proof = super::super::LeaderProof {
        owner: crate::checkpoint::LeaderProofOwner {
            node_id: 1,
            boot_id: uuid::Uuid::from_u128(11),
            process_term: 3,
        },
        fencing_token: 7,
    };
    let local = proof.clone();
    coordinator.set_local_leader_proof_provider(Arc::new(move || Some(local.clone())));
    let mut announcement = BarrierAnnouncement {
        epoch: 20,
        checkpoint_id: 20,
        assignment_fence: Some(test_fence(9, &[1], &[(1, 11)])),
        leader_proof: Some(proof),
        phase: Phase::Prepare,
        flags: 0,
    };

    coordinator
        .validate_reversible_announcement(&announcement)
        .await
        .expect("the exact locally live proof must avoid a remote authority read");

    announcement.leader_proof.as_mut().unwrap().fencing_token += 1;
    let error = coordinator
        .validate_reversible_announcement(&announcement)
        .await
        .expect_err("a different token must fall through to durable validation");
    assert!(error.contains("no durable leader lease exists"), "{error}");
}

#[cfg(feature = "cluster")]
#[test]
fn phase_ack_rejects_same_map_from_a_restarted_process() {
    let expected = test_fence(17, &[1, 2], &[(1, 11), (2, 22)]);
    let restarted = test_fence(17, &[1, 2], &[(1, 111), (2, 22)]);
    let announcement = BarrierAnnouncement {
        epoch: 20,
        checkpoint_id: 20,
        assignment_fence: Some(expected),
        leader_proof: None,
        phase: Phase::Commit,
        flags: 0,
    };
    let ack = barrier_v1::Ack {
        epoch: announcement.epoch,
        disposition: ack_disposition_to_wire(BarrierAckDisposition::Prepared),
        error: None,
        local_watermark_ms: None,
        checkpoint_id: announcement.checkpoint_id,
        assignment_digest: restarted.digest().to_vec(),
        watermark_status: 0,
        flags: announcement.flags,
    };

    assert!(validate_phase_ack(&ack, &announcement).is_err());
}

#[cfg(feature = "cluster")]
#[test]
fn phase_ack_and_capture_cache_bind_flags_and_failure_precedence() {
    assert!(ack_disposition_from_wire(0).is_err());

    let announcement = BarrierAnnouncement {
        epoch: 21,
        checkpoint_id: 21,
        assignment_fence: None,
        leader_proof: None,
        phase: Phase::Commit,
        flags: crate::checkpoint::flags::HANDOFF,
    };
    let ack = barrier_v1::Ack {
        epoch: announcement.epoch,
        disposition: ack_disposition_to_wire(BarrierAckDisposition::Prepared),
        error: None,
        local_watermark_ms: None,
        checkpoint_id: announcement.checkpoint_id,
        assignment_digest: Vec::new(),
        watermark_status: 0,
        flags: 0,
    };
    assert!(validate_phase_ack(&ack, &announcement).is_err());

    let regular = BarrierIdentity::from_announcement(&BarrierAnnouncement {
        flags: 0,
        ..announcement.clone()
    });
    let handoff = BarrierIdentity::from_announcement(&announcement);
    assert_ne!(regular, handoff);

    let captured = BarrierAck {
        epoch: announcement.epoch,
        checkpoint_id: announcement.checkpoint_id,
        assignment_digest: None,
        flags: announcement.flags,
        disposition: BarrierAckDisposition::Captured,
        error: None,
        watermark: CheckpointWatermark::Uninitialized,
    };
    let replay = BarrierAck {
        disposition: BarrierAckDisposition::CapturedWithReplay,
        ..captured.clone()
    };
    let failed = BarrierAck {
        disposition: BarrierAckDisposition::Failed,
        error: Some("durable prepare failed".into()),
        ..captured.clone()
    };
    let legacy_prepared_with_replay = BarrierAck {
        disposition: BarrierAckDisposition::PreparedWithReplay,
        ..captured.clone()
    };
    let mut cache = PrepareAckState::default();
    assert_eq!(cache.record_ack(handoff, &captured), captured);
    assert_eq!(
        cache.record_ack(handoff, &legacy_prepared_with_replay),
        captured
    );
    assert_eq!(cache.record_ack(handoff, &replay), replay);
    assert_eq!(cache.record_ack(handoff, &captured), replay);
    assert_eq!(cache.record_ack(handoff, &failed), failed);
    assert_eq!(cache.record_ack(handoff, &replay), failed);

    let regular_ack = BarrierAck {
        flags: 0,
        ..captured
    };
    assert_eq!(cache.record_ack(regular, &regular_ack), regular_ack);
}

#[cfg(feature = "cluster")]
#[test]
fn prepare_quorum_requires_an_explicit_captured_ack() {
    use barrier_v1::CheckpointWatermarkStatus as WireStatus;

    let fence = test_fence(17, &[1, 2], &[(1, 11), (2, 22)]);
    let mut prepare = BarrierAnnouncement {
        epoch: 22,
        checkpoint_id: 22,
        assignment_fence: Some(fence.clone()),
        leader_proof: None,
        phase: Phase::Prepare,
        flags: 0,
    };
    let mut ack = barrier_v1::Ack {
        epoch: prepare.epoch,
        disposition: ack_disposition_to_wire(BarrierAckDisposition::Captured),
        error: None,
        local_watermark_ms: Some(91),
        checkpoint_id: prepare.checkpoint_id,
        assignment_digest: fence.digest().to_vec(),
        watermark_status: WireStatus::CheckpointWatermarkActive as i32,
        flags: prepare.flags,
    };

    assert_eq!(
        validate_capture_ack(NodeId(2), &prepare, Some(&fence.digest()), &ack).unwrap(),
        (NodeId(2), CheckpointWatermark::Active(91), false)
    );

    ack.disposition = ack_disposition_to_wire(BarrierAckDisposition::Prepared);
    let (_, failure) =
        validate_capture_ack(NodeId(2), &prepare, Some(&fence.digest()), &ack).unwrap_err();
    assert!(matches!(failure, PeerFailure::Nack(message) if message.contains("explicit Captured")));

    ack.disposition = ack_disposition_to_wire(BarrierAckDisposition::CapturedWithReplay);
    let (_, failure) =
        validate_capture_ack(NodeId(2), &prepare, Some(&fence.digest()), &ack).unwrap_err();
    assert!(matches!(failure, PeerFailure::Nack(message) if message.contains("HANDOFF")));

    prepare.flags = crate::checkpoint::flags::HANDOFF;
    ack.flags = prepare.flags;
    assert_eq!(
        validate_capture_ack(NodeId(2), &prepare, Some(&fence.digest()), &ack).unwrap(),
        (NodeId(2), CheckpointWatermark::Active(91), true)
    );
}

#[cfg(all(test, feature = "cluster"))]
mod grpc_tests {
    use super::*;
    use crate::cluster::discovery::NodeState;
    use object_store::memory::InMemory;
    use std::net::SocketAddr;

    async fn lease_authority() -> (
        Arc<crate::cluster::control::LeaderLeaseStore>,
        crate::cluster::control::LeaderProof,
    ) {
        let store = Arc::new(crate::cluster::control::LeaderLeaseStore::new(
            Arc::new(InMemory::new()),
            1_000,
        ));
        let owner = crate::cluster::control::LeaderLeaseOwner {
            node: NodeId(1),
            boot: uuid::Uuid::from_u128(1),
            process_term: 1,
        };
        let lease = match store.begin_new_term(&owner, 1).await.unwrap() {
            crate::cluster::control::LeaseOutcome::Acquired(lease) => lease,
            crate::cluster::control::LeaseOutcome::Held(_) => unreachable!(),
        };
        (store, lease.proof())
    }

    fn coordinator(
        kv: Arc<dyn ClusterKv>,
        store: Arc<crate::cluster::control::LeaderLeaseStore>,
    ) -> BarrierCoordinator {
        let coordinator = BarrierCoordinator::new(kv);
        coordinator.set_leader_lease_store(store);
        coordinator
    }

    #[derive(Debug)]
    struct RejectAnnouncementKv {
        inner: Arc<InMemoryKv>,
        rejected_phase: Option<Phase>,
    }

    #[async_trait]
    impl ClusterKv for RejectAnnouncementKv {
        async fn write(&self, key: &str, value: String) {
            let _ = self.write_checked(key, value).await;
        }

        async fn write_checked(&self, key: &str, value: String) -> Result<(), String> {
            let reject = self.rejected_phase.is_none_or(|phase| {
                serde_json::from_str::<BarrierAnnouncement>(&value)
                    .is_ok_and(|announcement| announcement.phase == phase)
            });
            if key == ANNOUNCEMENT_KEY && reject {
                return Err(self.rejected_phase.map_or_else(
                    || "injected durable write failure".to_string(),
                    |phase| format!("injected {phase:?} durable write failure"),
                ));
            }
            self.inner.write(key, value).await;
            Ok(())
        }

        async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
            self.inner.read_from(who, key).await
        }

        async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
            self.inner.scan(key).await
        }
    }

    struct GateNextAnnouncementKv {
        inner: Arc<InMemoryKv>,
        write_started: tokio::sync::Notify,
        release_write: tokio::sync::Notify,
        gate_next: AtomicBool,
    }

    impl GateNextAnnouncementKv {
        fn new(inner: Arc<InMemoryKv>) -> Self {
            Self {
                inner,
                write_started: tokio::sync::Notify::new(),
                release_write: tokio::sync::Notify::new(),
                gate_next: AtomicBool::new(false),
            }
        }

        fn arm(&self) {
            self.gate_next
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    #[async_trait]
    impl ClusterKv for GateNextAnnouncementKv {
        async fn write(&self, key: &str, value: String) {
            let _ = self.write_checked(key, value).await;
        }

        async fn write_checked(&self, key: &str, value: String) -> Result<(), String> {
            if key == ANNOUNCEMENT_KEY
                && self
                    .gate_next
                    .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                self.write_started.notify_one();
                self.release_write.notified().await;
            }
            self.inner.write(key, value).await;
            Ok(())
        }

        async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
            self.inner.read_from(who, key).await
        }

        async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
            self.inner.scan(key).await
        }
    }

    fn test_process_lease(
        node_id: u64,
        boot: u128,
        term: u64,
    ) -> crate::cluster::control::ProcessLease {
        crate::cluster::control::ProcessLease {
            node: NodeId(node_id),
            owner: uuid::Uuid::from_u128(boot),
            term,
            seq: term,
            expires_at_ms: i64::MAX,
        }
    }

    fn bind_process(coordinator: &BarrierCoordinator, node_id: u64, boot: u128, term: u64) {
        coordinator
            .install_process_lease_deadline(Arc::new(
                crate::cluster::control::LeaseDeadline::live_for(Duration::from_secs(60)),
            ))
            .unwrap();
        coordinator
            .install_local_process_lease(&test_process_lease(node_id, boot, term))
            .unwrap();
    }

    fn install_local_proof(
        coordinator: &BarrierCoordinator,
        proof: &crate::cluster::control::LeaderProof,
    ) {
        let proof = proof.clone();
        coordinator.set_local_leader_proof_provider(Arc::new(move || Some(proof.clone())));
    }

    #[tokio::test]
    async fn process_bound_server_requires_a_live_shared_deadline() {
        let coordinator = BarrierCoordinator::new(kv(NodeId(2)));
        coordinator
            .install_local_process_lease(&test_process_lease(2, 22, 1))
            .unwrap();
        let error = coordinator
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap_err();
        assert!(error.contains("not installed"), "{error}");

        let deadline = Arc::new(crate::cluster::control::LeaseDeadline::fenced());
        coordinator
            .install_process_lease_deadline(Arc::clone(&deadline))
            .unwrap();
        coordinator
            .install_process_lease_deadline(Arc::clone(&deadline))
            .unwrap();
        assert!(coordinator
            .install_process_lease_deadline(Arc::new(
                crate::cluster::control::LeaseDeadline::live_for(Duration::from_secs(60)),
            ))
            .is_err());
        let error = coordinator
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap_err();
        assert!(error.contains("expired"), "{error}");
    }

    #[tokio::test]
    async fn assignment_less_server_cannot_be_promoted_after_first_publication() {
        let control = kv(NodeId(2));
        let coordinator = BarrierCoordinator::new(control.clone());
        coordinator
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let original = control
            .read_from(NodeId(2), BARRIER_ADDR_KEY)
            .await
            .unwrap();
        assert_eq!(decode_barrier_endpoint(&original).unwrap().1, None);

        let error = coordinator
            .install_local_process_lease(&test_process_lease(2, 22, 1))
            .unwrap_err();

        assert!(error.contains("cannot be promoted"), "{error}");
        assert_eq!(
            control.read_from(NodeId(2), BARRIER_ADDR_KEY).await,
            Some(original)
        );
    }

    #[tokio::test]
    async fn invalid_advertisement_fails_before_server_state_or_publication() {
        let control = kv(NodeId(2));
        let coordinator = BarrierCoordinator::new(control.clone());

        let error = coordinator
            .start_server(
                "127.0.0.1:0".parse().unwrap(),
                Some("x".repeat(MAX_BARRIER_ENDPOINT_BYTES)),
            )
            .await
            .unwrap_err();

        assert!(error.contains("oversized"), "{error}");
        assert!(coordinator.grpc.lock().is_none());
        assert!(control
            .read_from(NodeId(2), BARRIER_ADDR_KEY)
            .await
            .is_none());
    }

    fn endpoint_advertisement(address: SocketAddr, node_id: u64, boot: u128, term: u64) -> String {
        BarrierEndpointRecord::new(
            address.to_string(),
            BarrierProcessIdentity {
                node_id,
                boot_incarnation: uuid::Uuid::from_u128(boot),
                process_term: term,
            },
        )
        .unwrap()
        .encode()
        .unwrap()
    }

    #[tokio::test]
    async fn process_bound_client_pool_is_bounded_and_eviction_is_incarnation_safe() {
        let kv = kv(NodeId(999));
        let kv_dyn: Arc<dyn ClusterKv> = kv.clone();
        let pool: BarrierClientPool = Arc::new(parking_lot::Mutex::new(FxHashMap::default()));
        let count = u64::try_from(crate::checkpoint::MAX_CHECKPOINT_PARTICIPANTS).unwrap() + 5;
        for node_id in 1..=count {
            let boot = u128::from(node_id) + 1_000;
            kv.seed(
                NodeId(node_id),
                BARRIER_ADDR_KEY,
                endpoint_advertisement("127.0.0.1:1".parse().unwrap(), node_id, boot, 1),
            );
            assert!(get_barrier_client(
                NodeId(node_id),
                Some(ExpectedBarrierProcess::participant(
                    node_id,
                    uuid::Uuid::from_u128(boot),
                )),
                &pool,
                &kv_dyn,
            )
            .await
            .unwrap()
            .is_some());
        }
        assert_eq!(
            pool.lock().len(),
            crate::checkpoint::MAX_CHECKPOINT_PARTICIPANTS
        );

        let peer = NodeId(count);
        let current = ExpectedBarrierProcess::participant(
            peer.0,
            uuid::Uuid::from_u128(u128::from(peer.0) + 1_000),
        );
        let predecessor = ExpectedBarrierProcess::participant(
            peer.0,
            uuid::Uuid::from_u128(u128::from(peer.0) + 999),
        );
        assert!(matches!(
            get_barrier_client(peer, None, &pool, &kv_dyn).await,
            Err(BarrierClientResolutionError::ProcessMismatch)
        ));
        evict_barrier_client(&pool, peer, None);
        assert!(pool.lock().contains_key(&peer));
        evict_barrier_client(&pool, peer, Some(predecessor));
        assert!(pool.lock().contains_key(&peer));
        evict_barrier_client(&pool, peer, Some(current));
        assert!(!pool.lock().contains_key(&peer));

        let mismatched_peer = NodeId(count + 1);
        kv.seed(
            mismatched_peer,
            BARRIER_ADDR_KEY,
            endpoint_advertisement(
                "127.0.0.1:1".parse().unwrap(),
                mismatched_peer.0 + 1,
                9_999,
                1,
            ),
        );
        let started = std::time::Instant::now();
        assert!(matches!(
            get_barrier_client(
                mismatched_peer,
                Some(ExpectedBarrierProcess::participant(
                    mismatched_peer.0,
                    uuid::Uuid::from_u128(9_999),
                )),
                &pool,
                &kv_dyn,
            )
            .await,
            Err(BarrierClientResolutionError::Invalid(_))
        ));
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn every_certified_phase_rejects_a_wrong_recipient_before_mutation() {
        use barrier_v1::barrier_sync_server::BarrierSync;

        let (incoming_tx, incoming_rx) = crossfire::mpsc::bounded_async::<BarrierAnnouncement>(8);
        let local_process = Arc::new(std::sync::OnceLock::new());
        local_process
            .set(BarrierProcessIdentity {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(22),
                process_term: 1,
            })
            .unwrap();
        let process_lease_deadline = Arc::new(std::sync::OnceLock::new());
        process_lease_deadline
            .set(Arc::new(crate::cluster::control::LeaseDeadline::live_for(
                Duration::from_secs(60),
            )))
            .unwrap();
        let prepare_acks = Arc::new(parking_lot::Mutex::new(PrepareAckState::default()));
        let server = GrpcBarrierServer {
            incoming_tx,
            prepare_acks: Arc::clone(&prepare_acks),
            leader_lease_store: Arc::new(parking_lot::Mutex::new(None)),
            local_leader_proof: Arc::new(parking_lot::Mutex::new(None)),
            local_process,
            process_lease_deadline,
        };
        let wrong = assignment_fence_to_wire(Some(&test_fence(1, &[1, 2], &[(1, 11), (2, 23)])));

        let status = server
            .prepare(tonic::Request::new(barrier_v1::PrepareRequest {
                epoch: 1,
                checkpoint_id: 1,
                flags: 0,
                assignment_version: wrong.version,
                assignment_participants: wrong.participants.clone(),
                assignment_vnode_count: wrong.vnode_count,
                assignment_map_digest: wrong.map_digest.clone(),
                leader_proof: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);

        let status = server
            .aligned(tonic::Request::new(barrier_v1::AlignedRequest {
                epoch: 1,
                checkpoint_id: 1,
                flags: 0,
                assignment_version: wrong.version,
                assignment_participants: wrong.participants.clone(),
                assignment_vnode_count: wrong.vnode_count,
                assignment_map_digest: wrong.map_digest.clone(),
                leader_proof: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);

        let status = server
            .commit(tonic::Request::new(barrier_v1::CommitRequest {
                epoch: 1,
                checkpoint_id: 1,
                flags: 0,
                assignment_version: wrong.version,
                assignment_participants: wrong.participants.clone(),
                assignment_vnode_count: wrong.vnode_count,
                assignment_map_digest: wrong.map_digest.clone(),
                leader_proof: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);

        let status = server
            .abort(tonic::Request::new(barrier_v1::AbortRequest {
                epoch: 1,
                checkpoint_id: 1,
                flags: 0,
                assignment_version: wrong.version,
                assignment_participants: wrong.participants,
                assignment_vnode_count: wrong.vnode_count,
                assignment_map_digest: wrong.map_digest,
                leader_proof: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(incoming_rx.try_recv().is_err());
        assert!(prepare_acks.lock().pending.is_empty());
        assert!(prepare_acks.lock().received_at.is_empty());
    }

    async fn assert_fenced_phase_rejections(
        server: &GrpcBarrierServer,
        assignment: WireAssignmentFence,
        proof: Option<barrier_v1::LeaderProof>,
        epoch: u64,
        checkpoint_id: u64,
    ) {
        use barrier_v1::barrier_sync_server::BarrierSync;

        let status = server
            .aligned(tonic::Request::new(barrier_v1::AlignedRequest {
                epoch,
                checkpoint_id,
                flags: 0,
                assignment_version: assignment.version,
                assignment_participants: assignment.participants.clone(),
                assignment_vnode_count: assignment.vnode_count,
                assignment_map_digest: assignment.map_digest.clone(),
                leader_proof: proof.clone(),
            }))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        let status = server
            .commit(tonic::Request::new(barrier_v1::CommitRequest {
                epoch,
                checkpoint_id,
                flags: 0,
                assignment_version: assignment.version,
                assignment_participants: assignment.participants.clone(),
                assignment_vnode_count: assignment.vnode_count,
                assignment_map_digest: assignment.map_digest.clone(),
                leader_proof: proof.clone(),
            }))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        let status = server
            .abort(tonic::Request::new(barrier_v1::AbortRequest {
                epoch,
                checkpoint_id,
                flags: 0,
                assignment_version: assignment.version,
                assignment_participants: assignment.participants,
                assignment_vnode_count: assignment.vnode_count,
                assignment_map_digest: assignment.map_digest,
                leader_proof: proof,
            }))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn fenced_process_rejects_cached_prepare_and_every_phase_before_mutation() {
        use barrier_v1::barrier_sync_server::BarrierSync;

        let (store, proof) = lease_authority().await;
        let (incoming_tx, incoming_rx) = crossfire::mpsc::bounded_async::<BarrierAnnouncement>(8);
        let local_process = Arc::new(std::sync::OnceLock::new());
        local_process
            .set(BarrierProcessIdentity {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(22),
                process_term: 1,
            })
            .unwrap();
        let deadline = Arc::new(crate::cluster::control::LeaseDeadline::live_for(
            Duration::from_secs(60),
        ));
        let process_lease_deadline = Arc::new(std::sync::OnceLock::new());
        process_lease_deadline.set(Arc::clone(&deadline)).unwrap();

        let epoch = 7;
        let checkpoint_id = 7;
        let fence = test_fence(9, &[1, 2], &[(1, 1), (2, 22)]);
        let assignment_digest = Some(fence.digest());
        let identity = BarrierIdentity {
            attempt: CheckpointAttempt::new(epoch, checkpoint_id),
            assignment_digest,
            flags: 0,
        };
        let cached_ack = BarrierAck {
            epoch,
            checkpoint_id,
            assignment_digest,
            flags: 0,
            disposition: BarrierAckDisposition::Captured,
            error: None,
            watermark: CheckpointWatermark::Active(17),
        };
        let mut ack_state = PrepareAckState::default();
        ack_state.completed.insert(identity, cached_ack.clone());
        let prepare_acks = Arc::new(parking_lot::Mutex::new(ack_state));
        let server = GrpcBarrierServer {
            incoming_tx,
            prepare_acks: Arc::clone(&prepare_acks),
            leader_lease_store: Arc::new(parking_lot::Mutex::new(Some(store))),
            local_leader_proof: Arc::new(parking_lot::Mutex::new(None)),
            local_process,
            process_lease_deadline,
        };
        let assignment = assignment_fence_to_wire(Some(&fence));
        let proof = leader_proof_to_wire(Some(&proof));
        deadline.fence();

        let status = server
            .prepare(tonic::Request::new(barrier_v1::PrepareRequest {
                epoch,
                checkpoint_id,
                flags: 0,
                assignment_version: assignment.version,
                assignment_participants: assignment.participants.clone(),
                assignment_vnode_count: assignment.vnode_count,
                assignment_map_digest: assignment.map_digest.clone(),
                leader_proof: proof.clone(),
            }))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);

        assert_fenced_phase_rejections(&server, assignment, proof, epoch, checkpoint_id).await;

        let state = prepare_acks.lock();
        assert_eq!(state.completed.get(&identity), Some(&cached_ack));
        assert!(state.pending.is_empty());
        assert!(state.received_at.is_empty());
        assert!(incoming_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn process_fence_wakes_an_in_flight_prepare_without_an_ack() {
        use barrier_v1::barrier_sync_server::BarrierSync;

        let (store, proof) = lease_authority().await;
        let (incoming_tx, incoming_rx) = crossfire::mpsc::bounded_async::<BarrierAnnouncement>(1);
        let local_process = Arc::new(std::sync::OnceLock::new());
        local_process
            .set(BarrierProcessIdentity {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(22),
                process_term: 1,
            })
            .unwrap();
        let deadline = Arc::new(crate::cluster::control::LeaseDeadline::live_for(
            Duration::from_secs(60),
        ));
        let process_lease_deadline = Arc::new(std::sync::OnceLock::new());
        process_lease_deadline.set(Arc::clone(&deadline)).unwrap();
        let prepare_acks = Arc::new(parking_lot::Mutex::new(PrepareAckState::default()));
        let server = GrpcBarrierServer {
            incoming_tx,
            prepare_acks: Arc::clone(&prepare_acks),
            leader_lease_store: Arc::new(parking_lot::Mutex::new(Some(store))),
            local_leader_proof: Arc::new(parking_lot::Mutex::new(None)),
            local_process,
            process_lease_deadline,
        };
        let fence = test_fence(9, &[1, 2], &[(1, 1), (2, 22)]);
        let assignment = assignment_fence_to_wire(Some(&fence));
        let request = tonic::Request::new(barrier_v1::PrepareRequest {
            epoch: 8,
            checkpoint_id: 8,
            flags: 0,
            assignment_version: assignment.version,
            assignment_participants: assignment.participants,
            assignment_vnode_count: assignment.vnode_count,
            assignment_map_digest: assignment.map_digest,
            leader_proof: leader_proof_to_wire(Some(&proof)),
        });
        let call = server.prepare(request);
        tokio::pin!(call);
        tokio::select! {
            result = &mut call => panic!("Prepare returned before fencing: {result:?}"),
            announcement = incoming_rx.recv() => {
                assert_eq!(announcement.unwrap().phase, Phase::Prepare);
            }
        }

        deadline.fence();
        let status = tokio::time::timeout(Duration::from_secs(1), &mut call)
            .await
            .expect("fencing did not wake Prepare")
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(prepare_acks.lock().pending.is_empty());
        assert!(prepare_acks.lock().completed.is_empty());
    }

    fn proof(
        node_id: u64,
        boot: u128,
        process_term: u64,
        token: u64,
    ) -> crate::cluster::control::LeaderProof {
        crate::cluster::control::LeaderProof {
            owner: crate::checkpoint::LeaderProofOwner {
                node_id,
                boot_id: uuid::Uuid::from_u128(boot),
                process_term,
            },
            fencing_token: token,
        }
    }

    #[tokio::test]
    async fn prepare_validation_precedes_transport_start_and_durable_publication() {
        let leader_kv = kv(NodeId(1));
        let (store, leader_proof) = lease_authority().await;
        let leader = coordinator(leader_kv.clone(), store);
        let valid = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(test_fence(9, &[1, 2], &[(1, 1), (2, 22)])),
            leader_proof: Some(leader_proof.clone()),
            phase: Phase::Prepare,
            flags: 0,
        };

        assert!(leader
            .announce_prepare(&valid, Duration::ZERO)
            .await
            .is_err());
        assert!(leader_kv
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .is_none());

        let leader_outside_roster = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(test_fence(10, &[2], &[(2, 22)])),
            leader_proof: Some(leader_proof),
            ..valid
        };
        assert!(leader
            .announce_prepare(&leader_outside_roster, Duration::from_secs(1))
            .await
            .is_err());
        assert!(leader_kv
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn remote_proof_confirmation_acknowledges_only_the_exact_live_provider_value() {
        let caller_kv = kv(NodeId(1));
        let remote_kv = kv(NodeId(2));
        let caller = BarrierCoordinator::new(caller_kv.clone());
        let remote = BarrierCoordinator::new(remote_kv);
        let expected = proof(2, 22, 7, 41);
        bind_process(&remote, 2, 22, 7);

        caller
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let remote_addr = remote
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        caller_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(remote_addr, 2, 22, 7),
        );
        caller_kv.seed(
            NodeId(3),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(remote_addr, 3, 22, 7),
        );

        let deadline = || tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        assert!(
            !caller
                .confirm_remote_leader_proof(&expected, deadline())
                .await
                .unwrap(),
            "an absent provider must fail closed"
        );

        let live = Arc::new(parking_lot::Mutex::new(Some(expected.clone())));
        let provider = Arc::clone(&live);
        remote.set_local_leader_proof_provider(Arc::new(move || provider.lock().clone()));
        assert!(caller
            .confirm_remote_leader_proof(&expected, deadline())
            .await
            .unwrap());

        let mut wrong_token = expected.clone();
        wrong_token.fencing_token += 1;
        assert!(
            !caller
                .confirm_remote_leader_proof(&wrong_token, deadline())
                .await
                .unwrap(),
            "the acknowledgement must bind the fencing token"
        );

        let mut wrong_process = expected.clone();
        wrong_process.owner.process_term += 1;
        caller_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(remote_addr, 2, 22, 8),
        );
        assert!(
            !caller
                .confirm_remote_leader_proof(&wrong_process, deadline())
                .await
                .unwrap(),
            "the acknowledgement must bind the process term"
        );
        caller_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(remote_addr, 2, 22, 7),
        );

        let mut wrong_boot = expected.clone();
        wrong_boot.owner.boot_id = uuid::Uuid::from_u128(23);
        assert!(
            !caller
                .confirm_remote_leader_proof(&wrong_boot, deadline())
                .await
                .unwrap(),
            "the acknowledgement must bind the boot incarnation"
        );

        let mut wrong_node = expected.clone();
        wrong_node.owner.node_id = 3;
        assert!(
            !caller
                .confirm_remote_leader_proof(&wrong_node, deadline())
                .await
                .unwrap(),
            "the acknowledgement must bind the stable node identity"
        );

        *live.lock() = None;
        assert!(
            !caller
                .confirm_remote_leader_proof(&expected, deadline())
                .await
                .unwrap(),
            "an expired process-local grant must fail closed"
        );
    }

    #[tokio::test]
    async fn proof_confirmation_rotates_a_same_node_endpoint_without_stale_eviction() {
        let caller_kv = kv(NodeId(1));
        let caller = BarrierCoordinator::new(caller_kv.clone());
        caller
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();

        let predecessor = BarrierCoordinator::new(kv(NodeId(2)));
        let successor = BarrierCoordinator::new(kv(NodeId(2)));
        let predecessor_proof = proof(2, 22, 7, 41);
        let successor_proof = proof(2, 23, 8, 42);
        bind_process(&predecessor, 2, 22, 7);
        bind_process(&successor, 2, 23, 8);
        let predecessor_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let successor_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = Arc::clone(&predecessor_calls);
        let live = predecessor_proof.clone();
        predecessor.set_local_leader_proof_provider(Arc::new(move || {
            calls.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            Some(live.clone())
        }));
        let calls = Arc::clone(&successor_calls);
        let live = successor_proof.clone();
        successor.set_local_leader_proof_provider(Arc::new(move || {
            calls.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            Some(live.clone())
        }));
        let predecessor_addr = predecessor
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let successor_addr = successor
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        caller_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(predecessor_addr, 2, 22, 7),
        );
        let deadline = || tokio::time::Instant::now() + Duration::from_secs(1);
        assert!(caller
            .confirm_remote_leader_proof(&predecessor_proof, deadline())
            .await
            .unwrap());

        caller_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(successor_addr, 2, 23, 8),
        );
        assert!(caller
            .confirm_remote_leader_proof(&successor_proof, deadline())
            .await
            .unwrap());
        assert_eq!(
            predecessor_calls.load(std::sync::atomic::Ordering::Acquire),
            1
        );
        assert_eq!(
            successor_calls.load(std::sync::atomic::Ordering::Acquire),
            1
        );

        assert!(!caller
            .confirm_remote_leader_proof(&predecessor_proof, deadline())
            .await
            .unwrap());
        assert_eq!(
            predecessor_calls.load(std::sync::atomic::Ordering::Acquire),
            1
        );
        assert_eq!(
            successor_calls.load(std::sync::atomic::Ordering::Acquire),
            1
        );
        let cached = caller.grpc.lock().clone().unwrap();
        assert_eq!(
            cached.clients.lock().get(&NodeId(2)).unwrap().process,
            Some(BarrierProcessIdentity {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(23),
                process_term: 8,
            })
        );
    }

    /// Observation is latest-wins (non-destructive), so wait for the
    /// expected phase specifically — earlier phases may linger.
    async fn wait_observe(
        coord: &BarrierCoordinator,
        leader: NodeId,
        phase: Phase,
    ) -> BarrierAnnouncement {
        for _ in 0..100 {
            if let Some(ann) = coord.observe_hint(leader).await.unwrap() {
                if ann.phase == phase {
                    return ann;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for {phase:?} announcement from leader {leader:?}");
    }

    async fn wait_observe_exact(
        coord: &BarrierCoordinator,
        leader: NodeId,
        expected: CheckpointAttempt,
        phase: Phase,
    ) -> BarrierAnnouncement {
        for _ in 0..100 {
            if let Some(announcement) = coord.observe_hint(leader).await.unwrap() {
                if announcement_attempt(&announcement) == expected && announcement.phase == phase {
                    return announcement;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for {phase:?} announcement {expected:?} from leader {leader:?}");
    }

    fn pending_prepare_waiters(
        coordinator: &BarrierCoordinator,
        prepare: &BarrierAnnouncement,
    ) -> usize {
        let identity = BarrierIdentity::from_announcement(prepare);
        let state = coordinator.grpc.lock().clone().unwrap();
        let waiters = state
            .prepare_acks
            .lock()
            .pending
            .get(&identity)
            .map_or(0, Vec::len);
        waiters
    }

    async fn wait_for_direct_prepare(
        coordinator: &BarrierCoordinator,
        prepare: &BarrierAnnouncement,
    ) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let relayed = coordinator
                    .grpc
                    .lock()
                    .as_ref()
                    .and_then(|state| state.latest_rx.borrow().clone())
                    .as_ref()
                    == Some(prepare);
                if coordinator.prepare_received_at(prepare).is_some() && relayed {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("direct Prepare was not delivered");
    }

    async fn started_barrier_pair() -> (
        BarrierCoordinator,
        BarrierCoordinator,
        crate::cluster::control::LeaderProof,
    ) {
        let leader_kv = kv(NodeId(1));
        let follower_kv = kv(NodeId(2));
        let (store, proof) = lease_authority().await;
        let leader = coordinator(leader_kv.clone(), Arc::clone(&store));
        let follower = coordinator(follower_kv, store);
        bind_process(&leader, 1, 1, 1);
        bind_process(&follower, 2, 22, 1);
        install_local_proof(&leader, &proof);
        leader
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let follower_addr = follower
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        leader_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(follower_addr, 2, 22, 1),
        );
        (leader, follower, proof)
    }

    async fn reach_two_node_prepare_quorum(
        leader: &BarrierCoordinator,
        follower: &BarrierCoordinator,
        prepare: &BarrierAnnouncement,
        watermark: CheckpointWatermark,
    ) {
        leader
            .announce_prepare(prepare, Duration::from_secs(1))
            .await
            .unwrap();
        wait_for_direct_prepare(follower, prepare).await;
        follower
            .ack(&BarrierAck {
                epoch: prepare.epoch,
                checkpoint_id: prepare.checkpoint_id,
                assignment_digest: prepare
                    .assignment_fence
                    .as_ref()
                    .map(crate::checkpoint::CheckpointAssignmentFence::digest),
                flags: prepare.flags,
                disposition: BarrierAckDisposition::Captured,
                error: None,
                watermark,
            })
            .await
            .unwrap();
        let outcome = leader
            .wait_for_quorum(prepare, &[NodeId(2)], Duration::from_secs(1))
            .await;
        assert!(
            matches!(outcome, QuorumOutcome::Reached { .. }),
            "Prepare did not reach its exact quorum: {outcome:?}"
        );
    }

    #[tokio::test]
    async fn failed_durable_prepare_publication_prevents_direct_delivery() {
        let leader_inner = kv(NodeId(1));
        let leader_kv = Arc::new(RejectAnnouncementKv {
            inner: Arc::clone(&leader_inner),
            rejected_phase: None,
        });
        let follower_kv = kv(NodeId(2));
        let (store, proof) = lease_authority().await;
        let leader = coordinator(leader_kv, Arc::clone(&store));
        let follower = coordinator(follower_kv, store);
        bind_process(&leader, 1, 1, 1);
        bind_process(&follower, 2, 22, 1);
        install_local_proof(&leader, &proof);
        leader
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let follower_addr = follower
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        leader_inner.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(follower_addr, 2, 22, 1),
        );

        let prepare = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(test_fence(9, &[1, 2], &[(1, 1), (2, 22)])),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: crate::checkpoint::flags::FULL_SNAPSHOT,
        };
        let mut follower_watch = follower.announcement_watch().unwrap();

        let error = leader
            .announce_prepare(&prepare, Duration::from_secs(1))
            .await
            .unwrap_err();

        assert!(error.contains("injected durable write failure"), "{error}");
        assert!(leader_inner
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), follower_watch.changed())
                .await
                .is_err(),
            "failed durable publication must not deliver a direct announcement"
        );
        assert!(follower.prepare_received_at(&prepare).is_none());
        assert!(leader
            .grpc
            .lock()
            .as_ref()
            .unwrap()
            .prepare_fanout
            .lock()
            .is_none());
    }

    #[tokio::test]
    async fn certified_prepare_rechecks_leadership_after_publication_lock_contention() {
        let (leader, follower, proof) = started_barrier_pair().await;
        let live_proof = Arc::new(parking_lot::Mutex::new(Some(proof.clone())));
        let provider = Arc::clone(&live_proof);
        leader.set_local_leader_proof_provider(Arc::new(move || provider.lock().clone()));
        let prepare = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(test_fence(9, &[1, 2], &[(1, 1), (2, 22)])),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        let mut follower_watch = follower.announcement_watch().unwrap();
        let publication_guard = leader.publication.lock().await;
        let announce = leader.announce_prepare(&prepare, Duration::from_secs(1));
        tokio::pin!(announce);
        let first_poll = std::future::poll_fn(|context| {
            std::task::Poll::Ready(std::future::Future::poll(announce.as_mut(), context))
        })
        .await;
        assert!(first_poll.is_pending());

        live_proof.lock().take();
        drop(publication_guard);
        let error = announce.await.unwrap_err();

        assert!(
            error.contains("no longer owns its exact leader proof"),
            "{error}"
        );
        assert!(leader
            .kv
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), follower_watch.changed())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn certified_prepare_does_not_fan_out_after_leadership_loss_during_write() {
        let leader_inner = kv(NodeId(1));
        let leader_kv = Arc::new(GateNextAnnouncementKv::new(Arc::clone(&leader_inner)));
        let (store, proof) = lease_authority().await;
        let leader = Arc::new(coordinator(leader_kv.clone(), store));
        bind_process(&leader, 1, 1, 1);
        let live_proof = Arc::new(parking_lot::Mutex::new(Some(proof.clone())));
        let provider = Arc::clone(&live_proof);
        leader.set_local_leader_proof_provider(Arc::new(move || provider.lock().clone()));
        leader
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let prepare = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(test_fence(9, &[1], &[(1, 1)])),
            leader_proof: Some(proof.clone()),
            phase: Phase::Prepare,
            flags: 0,
        };
        leader_kv.arm();
        let write_started = leader_kv.write_started.notified();
        tokio::pin!(write_started);
        let announce_task = tokio::spawn({
            let leader = Arc::clone(&leader);
            let prepare = prepare.clone();
            async move {
                leader
                    .announce_prepare(&prepare, Duration::from_secs(1))
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), &mut write_started)
            .await
            .expect("Prepare durable write did not reach the gate");

        live_proof.lock().take();
        leader_kv.release_write.notify_one();
        let error = announce_task.await.unwrap().unwrap_err();

        assert!(
            error.contains("no longer owns its exact leader proof"),
            "{error}"
        );
        let durable = leader_inner
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<BarrierAnnouncement>(&durable).unwrap(),
            prepare
        );
        assert!(leader
            .grpc
            .lock()
            .as_ref()
            .unwrap()
            .prepare_fanout
            .lock()
            .is_none());
    }

    #[tokio::test]
    async fn failed_terminal_write_keeps_the_attempt_closed_locally() {
        for terminal_phase in [Phase::Commit, Phase::Abort] {
            let inner = kv(NodeId(1));
            let coordinator = BarrierCoordinator::new(Arc::new(RejectAnnouncementKv {
                inner: Arc::clone(&inner),
                rejected_phase: Some(terminal_phase),
            }));
            let prepare = BarrierAnnouncement {
                epoch: 1,
                checkpoint_id: 1,
                assignment_fence: None,
                leader_proof: None,
                phase: Phase::Prepare,
                flags: 0,
            };
            coordinator.announce(&prepare).await.unwrap();
            let terminal = BarrierAnnouncement {
                phase: terminal_phase,
                ..prepare.clone()
            };

            let error = coordinator.announce(&terminal).await.unwrap_err();
            assert!(error.contains("injected"), "{error}");
            let error = coordinator.announce(&prepare).await.unwrap_err();
            assert!(error.contains("cannot regress"), "{error}");
            let durable = inner.read_from(NodeId(1), ANNOUNCEMENT_KEY).await.unwrap();
            assert_eq!(
                serde_json::from_str::<BarrierAnnouncement>(&durable).unwrap(),
                prepare
            );
        }
    }

    #[tokio::test]
    async fn publication_order_rehydrates_observed_history_after_restart() {
        let shared = kv(NodeId(1));
        let first = BarrierCoordinator::new(shared.clone());
        let prepare = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Prepare,
            flags: 0,
        };
        first.announce(&prepare).await.unwrap();
        let commit = BarrierAnnouncement {
            phase: Phase::Commit,
            ..prepare.clone()
        };
        first.announce(&commit).await.unwrap();
        drop(first);

        let restarted = BarrierCoordinator::new(shared.clone());
        for late in [
            prepare,
            BarrierAnnouncement {
                phase: Phase::Aligned,
                ..commit.clone()
            },
        ] {
            let error = restarted.announce(&late).await.unwrap_err();
            assert!(error.contains("cannot regress"), "{error}");
        }
        let durable = shared.read_from(NodeId(1), ANNOUNCEMENT_KEY).await.unwrap();
        assert_eq!(
            serde_json::from_str::<BarrierAnnouncement>(&durable).unwrap(),
            commit
        );
    }

    #[tokio::test]
    async fn certified_aligned_requires_the_exact_reached_prepare_quorum() {
        let (leader, follower, proof) = started_barrier_pair().await;
        let prepare = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(test_fence(9, &[1, 2], &[(1, 1), (2, 22)])),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        leader
            .announce_prepare(&prepare, Duration::from_secs(1))
            .await
            .unwrap();
        wait_for_direct_prepare(&follower, &prepare).await;
        let mut leader_watch = leader.announcement_watch().unwrap();
        let mut follower_watch = follower.announcement_watch().unwrap();
        let _ = follower_watch.borrow_and_update();
        let aligned = BarrierAnnouncement {
            phase: Phase::Aligned,
            ..prepare.clone()
        };

        let error = leader.announce(&aligned).await.unwrap_err();

        assert!(error.contains("successful exact capture quorum"), "{error}");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), leader_watch.changed())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), follower_watch.changed())
                .await
                .is_err()
        );
        let durable = leader
            .kv
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<BarrierAnnouncement>(&durable).unwrap(),
            prepare
        );
    }

    #[tokio::test]
    async fn leased_cluster_sender_enforces_certified_runtime_boundaries() {
        let leader_kv = kv(NodeId(1));
        let (store, proof) = lease_authority().await;
        let leader = coordinator(leader_kv.clone(), store);
        bind_process(&leader, 1, 1, 1);
        install_local_proof(&leader, &proof);
        let prepare = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(test_fence(9, &[1], &[(1, 1)])),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: 0,
        };

        let prepare_error = leader
            .announce_prepare(&prepare, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(prepare_error.contains("started leased barrier server"));
        let aligned_error = leader
            .announce(&BarrierAnnouncement {
                phase: Phase::Aligned,
                ..prepare.clone()
            })
            .await
            .unwrap_err();
        assert!(aligned_error.contains("started leased barrier server"));
        assert!(leader_kv
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .is_none());

        let commit = BarrierAnnouncement {
            phase: Phase::Commit,
            ..prepare.clone()
        };
        leader.announce(&commit).await.unwrap();
        leader
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();

        for late in [
            prepare,
            BarrierAnnouncement {
                phase: Phase::Aligned,
                ..commit.clone()
            },
        ] {
            let error = if late.phase == Phase::Prepare {
                leader
                    .announce_prepare(&late, Duration::from_secs(1))
                    .await
                    .unwrap_err()
            } else {
                leader.announce(&late).await.unwrap_err()
            };
            assert!(error.contains("cannot regress"), "{error}");
        }

        for phase in [Phase::Prepare, Phase::Aligned, Phase::Commit, Phase::Abort] {
            let error = leader
                .announce(&BarrierAnnouncement {
                    epoch: 2,
                    checkpoint_id: 2,
                    assignment_fence: None,
                    leader_proof: None,
                    phase,
                    flags: 0,
                })
                .await
                .unwrap_err();
            assert!(
                error.contains("requires an assignment certificate"),
                "{error}"
            );
        }
        let durable = leader_kv
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<BarrierAnnouncement>(&durable).unwrap(),
            commit
        );

        let unbound_kv = kv(NodeId(3));
        let unbound = BarrierCoordinator::new(unbound_kv.clone());
        let error = unbound.announce(&commit).await.unwrap_err();
        assert!(error.contains("process-bound leased endpoint"), "{error}");
        assert!(unbound_kv
            .read_from(NodeId(3), ANNOUNCEMENT_KEY)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn certified_aligned_rechecks_the_local_process_lease_after_lock_contention() {
        let leader_kv = kv(NodeId(1));
        let follower_kv = kv(NodeId(2));
        let (store, proof) = lease_authority().await;
        let leader = coordinator(leader_kv.clone(), Arc::clone(&store));
        let follower = coordinator(follower_kv, store);
        let process_deadline = Arc::new(crate::cluster::control::LeaseDeadline::live_for(
            Duration::from_secs(60),
        ));
        leader
            .install_process_lease_deadline(Arc::clone(&process_deadline))
            .unwrap();
        leader
            .install_local_process_lease(&test_process_lease(1, 1, 1))
            .unwrap();
        bind_process(&follower, 2, 22, 1);
        install_local_proof(&leader, &proof);
        leader
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let follower_addr = follower
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        leader_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(follower_addr, 2, 22, 1),
        );
        let prepare = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(test_fence(9, &[1, 2], &[(1, 1), (2, 22)])),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        reach_two_node_prepare_quorum(
            &leader,
            &follower,
            &prepare,
            CheckpointWatermark::Uninitialized,
        )
        .await;
        let mut leader_watch = leader.announcement_watch().unwrap();
        let mut follower_watch = follower.announcement_watch().unwrap();
        let _ = follower_watch.borrow_and_update();
        let announcement_guard = leader.publication.lock().await;
        let aligned = BarrierAnnouncement {
            phase: Phase::Aligned,
            ..prepare.clone()
        };
        let aligned_call = leader.announce(&aligned);
        tokio::pin!(aligned_call);
        let first_poll = std::future::poll_fn(|context| {
            std::task::Poll::Ready(std::future::Future::poll(aligned_call.as_mut(), context))
        })
        .await;
        assert!(first_poll.is_pending());

        process_deadline.fence();
        drop(announcement_guard);
        let error = aligned_call.await.unwrap_err();

        assert!(
            error.contains("process lease deadline has expired"),
            "{error}"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), leader_watch.changed())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), follower_watch.changed())
                .await
                .is_err()
        );
        let durable = leader_kv
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<BarrierAnnouncement>(&durable).unwrap(),
            prepare
        );
    }

    #[tokio::test]
    async fn certified_aligned_rechecks_local_leadership_after_lock_contention() {
        let (leader, follower, proof) = started_barrier_pair().await;
        let prepare = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(test_fence(9, &[1, 2], &[(1, 1), (2, 22)])),
            leader_proof: Some(proof.clone()),
            phase: Phase::Prepare,
            flags: 0,
        };
        reach_two_node_prepare_quorum(
            &leader,
            &follower,
            &prepare,
            CheckpointWatermark::Uninitialized,
        )
        .await;
        let live_proof = Arc::new(parking_lot::Mutex::new(Some(proof)));
        let provider = Arc::clone(&live_proof);
        leader.set_local_leader_proof_provider(Arc::new(move || provider.lock().clone()));
        let mut leader_watch = leader.announcement_watch().unwrap();
        let mut follower_watch = follower.announcement_watch().unwrap();
        let _ = follower_watch.borrow_and_update();
        let announcement_guard = leader.publication.lock().await;
        let aligned = BarrierAnnouncement {
            phase: Phase::Aligned,
            ..prepare.clone()
        };
        let aligned_call = leader.announce(&aligned);
        tokio::pin!(aligned_call);
        let first_poll = std::future::poll_fn(|context| {
            std::task::Poll::Ready(std::future::Future::poll(aligned_call.as_mut(), context))
        })
        .await;
        assert!(first_poll.is_pending());

        live_proof.lock().take();
        drop(announcement_guard);
        let error = aligned_call.await.unwrap_err();

        assert!(
            error.contains("no longer owns its exact leader proof"),
            "{error}"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), leader_watch.changed())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), follower_watch.changed())
                .await
                .is_err()
        );
        let durable = leader
            .kv
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<BarrierAnnouncement>(&durable).unwrap(),
            prepare
        );
    }

    #[tokio::test]
    async fn assignment_less_single_node_aligned_remains_durable_first() {
        let leader_inner = kv(NodeId(1));
        let leader_kv = Arc::new(GateNextAnnouncementKv::new(Arc::clone(&leader_inner)));
        let leader = Arc::new(BarrierCoordinator::new(leader_kv.clone()));
        leader
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let mut local_watch = leader.announcement_watch().unwrap();
        let aligned = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Aligned,
            flags: 0,
        };
        leader_kv.arm();
        let write_started = leader_kv.write_started.notified();
        tokio::pin!(write_started);
        let announce_task = tokio::spawn({
            let leader = Arc::clone(&leader);
            let aligned = aligned.clone();
            async move { leader.announce(&aligned).await }
        });

        tokio::time::timeout(Duration::from_secs(1), &mut write_started)
            .await
            .expect("assignment-less Aligned write did not reach the gate");
        assert!(leader_inner
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), local_watch.changed())
                .await
                .is_err()
        );

        leader_kv.release_write.notify_one();
        announce_task.await.unwrap().unwrap();
        let durable = leader_inner
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<BarrierAnnouncement>(&durable).unwrap(),
            aligned
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), local_watch.changed())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn leader_only_cluster_closes_prepare_quorum_before_aligned() {
        let leader_kv = kv(NodeId(1));
        let (store, proof) = lease_authority().await;
        let leader = coordinator(leader_kv.clone(), store);
        bind_process(&leader, 1, 1, 1);
        install_local_proof(&leader, &proof);
        leader
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let prepare = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(test_fence(9, &[1], &[(1, 1)])),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        leader
            .announce_prepare(&prepare, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(matches!(
            leader
                .wait_for_quorum(&prepare, &[], Duration::from_secs(1))
                .await,
            QuorumOutcome::Reached { ref acks, .. } if acks.is_empty()
        ));
        let late_prepare = prepare.clone();
        let error = leader
            .announce_prepare(&late_prepare, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(error.contains("quorum-ready"), "{error}");
        let aligned = BarrierAnnouncement {
            phase: Phase::Aligned,
            ..prepare
        };
        let mut local_watch = leader.announcement_watch().unwrap();

        leader.announce(&aligned).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), local_watch.changed())
            .await
            .expect("leader-only cluster did not observe its local Aligned")
            .unwrap();
        assert_eq!(local_watch.borrow().as_ref(), Some(&aligned));

        let commit = BarrierAnnouncement {
            phase: Phase::Commit,
            ..aligned.clone()
        };
        leader.announce(&commit).await.unwrap();
        let durable_commit = leader_kv
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .unwrap();
        for error in [
            leader
                .announce_prepare(&late_prepare, Duration::from_secs(1))
                .await
                .unwrap_err(),
            leader.announce(&aligned).await.unwrap_err(),
        ] {
            assert!(error.contains("cannot regress"), "{error}");
        }
        assert_eq!(
            leader_kv.read_from(NodeId(1), ANNOUNCEMENT_KEY).await,
            Some(durable_commit)
        );
    }

    #[tokio::test]
    async fn certified_aligned_notifies_all_participants_while_durable_write_is_pending() {
        let leader_inner = kv(NodeId(1));
        let leader_kv = Arc::new(GateNextAnnouncementKv::new(Arc::clone(&leader_inner)));
        let follower_kv = kv(NodeId(2));
        let (store, proof) = lease_authority().await;
        let leader = Arc::new(coordinator(leader_kv.clone(), Arc::clone(&store)));
        let follower = coordinator(follower_kv, store);
        bind_process(&leader, 1, 1, 1);
        bind_process(&follower, 2, 22, 1);
        install_local_proof(&leader, &proof);
        leader
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let follower_addr = follower
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        leader_inner.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(follower_addr, 2, 22, 1),
        );

        let prepare = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(test_fence(9, &[1, 2], &[(1, 1), (2, 22)])),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: crate::checkpoint::flags::FULL_SNAPSHOT,
        };
        reach_two_node_prepare_quorum(
            &leader,
            &follower,
            &prepare,
            CheckpointWatermark::Active(100),
        )
        .await;
        let aligned = BarrierAnnouncement {
            phase: Phase::Aligned,
            ..prepare.clone()
        };
        let mut leader_watch = leader.announcement_watch().unwrap();
        let mut follower_watch = follower.announcement_watch().unwrap();
        let _ = follower_watch.borrow_and_update();
        leader_kv.arm();
        let write_started = leader_kv.write_started.notified();
        tokio::pin!(write_started);
        let announce_task = tokio::spawn({
            let leader = Arc::clone(&leader);
            let aligned = aligned.clone();
            async move { leader.announce(&aligned).await }
        });

        tokio::time::timeout(Duration::from_secs(1), &mut write_started)
            .await
            .expect("Aligned durable write did not reach the injected gate");
        tokio::time::timeout(Duration::from_secs(1), leader_watch.changed())
            .await
            .expect("leader did not receive its local Aligned notification")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), follower_watch.changed())
            .await
            .expect("follower did not receive direct Aligned notification")
            .unwrap();
        assert_eq!(leader_watch.borrow().as_ref(), Some(&aligned));
        assert_eq!(follower_watch.borrow().as_ref(), Some(&aligned));
        let pending_durable = leader_inner
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<BarrierAnnouncement>(&pending_durable).unwrap(),
            prepare
        );
        assert!(!announce_task.is_finished());

        let abort = BarrierAnnouncement {
            phase: Phase::Abort,
            ..aligned.clone()
        };
        let abort_call = leader.announce(&abort);
        tokio::pin!(abort_call);
        let first_poll = std::future::poll_fn(|context| {
            std::task::Poll::Ready(std::future::Future::poll(abort_call.as_mut(), context))
        })
        .await;
        assert!(
            first_poll.is_pending(),
            "terminal publication overtook the pending Aligned durable write"
        );
        assert_eq!(
            leader_inner
                .read_from(NodeId(1), ANNOUNCEMENT_KEY)
                .await
                .as_deref(),
            Some(pending_durable.as_str())
        );

        leader_kv.release_write.notify_one();
        announce_task.await.unwrap().unwrap();
        abort_call.await.unwrap();
        let durable = leader_inner
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<BarrierAnnouncement>(&durable).unwrap(),
            abort
        );
    }

    #[tokio::test]
    async fn failed_durable_aligned_publication_still_reports_after_reversible_delivery() {
        let leader_inner = kv(NodeId(1));
        let leader_kv = Arc::new(RejectAnnouncementKv {
            inner: Arc::clone(&leader_inner),
            rejected_phase: Some(Phase::Aligned),
        });
        let follower_kv = kv(NodeId(2));
        let (store, proof) = lease_authority().await;
        let leader = coordinator(leader_kv, Arc::clone(&store));
        let follower = coordinator(follower_kv, store);
        bind_process(&leader, 1, 1, 1);
        bind_process(&follower, 2, 22, 1);
        install_local_proof(&leader, &proof);
        leader
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let follower_addr = follower
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        leader_inner.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(follower_addr, 2, 22, 1),
        );

        let prepare = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(test_fence(9, &[1, 2], &[(1, 1), (2, 22)])),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: crate::checkpoint::flags::FULL_SNAPSHOT,
        };
        reach_two_node_prepare_quorum(
            &leader,
            &follower,
            &prepare,
            CheckpointWatermark::Active(100),
        )
        .await;
        let aligned = BarrierAnnouncement {
            phase: Phase::Aligned,
            ..prepare.clone()
        };
        let mut leader_watch = leader.announcement_watch().unwrap();
        let mut follower_watch = follower.announcement_watch().unwrap();
        let _ = follower_watch.borrow_and_update();

        let error = leader.announce(&aligned).await.unwrap_err();

        assert!(
            error.contains("injected Aligned durable write failure"),
            "{error}"
        );
        tokio::time::timeout(Duration::from_secs(1), leader_watch.changed())
            .await
            .expect("leader did not retain reversible Aligned delivery")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), follower_watch.changed())
            .await
            .expect("follower did not retain reversible Aligned delivery")
            .unwrap();
        assert_eq!(leader_watch.borrow().as_ref(), Some(&aligned));
        assert_eq!(follower_watch.borrow().as_ref(), Some(&aligned));
        let durable = leader_inner
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<BarrierAnnouncement>(&durable).unwrap(),
            prepare
        );
    }

    #[tokio::test]
    async fn announce_starts_one_prepare_rpc_before_quorum_wait() {
        let (leader, follower, proof) = started_barrier_pair().await;

        let fence = test_fence(9, &[1, 2], &[(1, 1), (2, 22)]);
        let prepare = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(fence.clone()),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: crate::checkpoint::flags::FULL_SNAPSHOT,
        };

        assert!(leader.announce(&prepare).await.is_err());
        assert!(
            leader
                .kv
                .read_from(NodeId(1), ANNOUNCEMENT_KEY)
                .await
                .is_none(),
            "generic announcement must reject certified Prepare before durable publication"
        );
        tokio::time::timeout(
            Duration::from_millis(500),
            leader.announce_prepare(&prepare, Duration::from_secs(1)),
        )
        .await
        .expect("announce must not wait for the follower acknowledgement")
        .unwrap();
        wait_for_direct_prepare(&follower, &prepare).await;
        let direct = wait_observe_exact(
            &follower,
            NodeId(1),
            CheckpointAttempt::new(prepare.epoch, prepare.checkpoint_id),
            Phase::Prepare,
        )
        .await;
        assert_eq!(direct, prepare);
        assert_eq!(pending_prepare_waiters(&follower, &prepare), 1);
        let accepted_json = leader
            .kv
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .unwrap();
        leader
            .announce_prepare(&prepare, Duration::from_secs(1))
            .await
            .unwrap();
        let conflicting = BarrierAnnouncement {
            flags: 0,
            ..prepare.clone()
        };
        assert!(leader
            .announce_prepare(&conflicting, Duration::from_secs(1))
            .await
            .is_err());
        assert_eq!(
            leader.kv.read_from(NodeId(1), ANNOUNCEMENT_KEY).await,
            Some(accepted_json),
            "rejected equivocation must not overwrite the durable announcement"
        );
        assert_eq!(
            pending_prepare_waiters(&follower, &prepare),
            1,
            "idempotent and conflicting publications must not issue another RPC"
        );

        let quorum = leader.wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_secs(1));
        tokio::pin!(quorum);
        tokio::select! {
            outcome = &mut quorum => panic!("silent follower completed quorum early: {outcome:?}"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        assert_eq!(
            pending_prepare_waiters(&follower, &prepare),
            1,
            "wait_for_quorum must consume the eager task instead of issuing a duplicate RPC"
        );

        follower
            .ack(&BarrierAck {
                epoch: prepare.epoch,
                checkpoint_id: prepare.checkpoint_id,
                assignment_digest: Some(fence.digest()),
                flags: prepare.flags,
                disposition: BarrierAckDisposition::Captured,
                error: None,
                watermark: CheckpointWatermark::Active(91),
            })
            .await
            .unwrap();
        assert!(matches!(
            quorum.await,
            QuorumOutcome::Reached {
                acks,
                follower_watermark: CheckpointWatermark::Active(91),
                handoff_replay_pending: false,
            } if acks == vec![NodeId(2)]
        ));
    }

    #[tokio::test]
    async fn captured_ack_reaches_quorum_while_durable_tail_is_blocked() {
        let (leader, follower, proof) = started_barrier_pair().await;
        let fence = test_fence(10, &[1, 2], &[(1, 1), (2, 22)]);
        let prepare = BarrierAnnouncement {
            epoch: 2,
            checkpoint_id: 2,
            assignment_fence: Some(fence.clone()),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: 0,
        };

        leader
            .announce_prepare(&prepare, Duration::from_secs(1))
            .await
            .unwrap();
        wait_for_direct_prepare(&follower, &prepare).await;
        let (durable_release_tx, durable_release_rx) = tokio::sync::oneshot::channel::<()>();
        let durable_tail = tokio::spawn(async move {
            let _ = durable_release_rx.await;
        });
        follower
            .ack(&BarrierAck {
                epoch: prepare.epoch,
                checkpoint_id: prepare.checkpoint_id,
                assignment_digest: Some(fence.digest()),
                flags: prepare.flags,
                disposition: BarrierAckDisposition::Captured,
                error: None,
                watermark: CheckpointWatermark::Active(92),
            })
            .await
            .unwrap();
        assert!(!durable_tail.is_finished());

        let outcome = leader
            .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_secs(1))
            .await;
        assert!(matches!(
            outcome,
            QuorumOutcome::Reached {
                acks,
                follower_watermark: CheckpointWatermark::Active(92),
                handoff_replay_pending: false,
            } if acks == vec![NodeId(2)]
        ));
        assert!(!durable_tail.is_finished());
        durable_release_tx.send(()).unwrap();
        durable_tail.await.unwrap();
    }

    #[tokio::test]
    async fn quorum_deadline_aborts_a_silent_eager_prepare_rpc() {
        let (leader, follower, proof) = started_barrier_pair().await;

        let fence = test_fence(10, &[1, 2], &[(1, 1), (2, 22)]);
        let prepare = BarrierAnnouncement {
            epoch: 2,
            checkpoint_id: 2,
            assignment_fence: Some(fence),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        leader
            .announce_prepare(&prepare, Duration::from_millis(50))
            .await
            .unwrap();
        wait_for_direct_prepare(&follower, &prepare).await;

        let started = std::time::Instant::now();
        let outcome = leader
            .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_millis(50))
            .await;
        assert!(matches!(
            outcome,
            QuorumOutcome::TimedOut {
                got,
                missing,
            } if got.is_empty() && missing == vec![NodeId(2)]
        ));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the caller deadline must bound the eager task's longer transport deadline"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while pending_prepare_waiters(&follower, &prepare) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("caller timeout did not cancel the follower Prepare waiter");
    }

    #[tokio::test]
    async fn fatal_prepare_ack_does_not_wait_for_a_silent_peer() {
        let leader_kv = kv(NodeId(1));
        let (store, proof) = lease_authority().await;
        let leader = coordinator(leader_kv.clone(), Arc::clone(&store));
        let follower_two = coordinator(kv(NodeId(2)), Arc::clone(&store));
        let follower_three = coordinator(kv(NodeId(3)), store);
        bind_process(&leader, 1, 1, 1);
        bind_process(&follower_two, 2, 22, 1);
        bind_process(&follower_three, 3, 33, 1);
        install_local_proof(&leader, &proof);
        leader
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let follower_two_addr = follower_two
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let follower_three_addr = follower_three
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        leader_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(follower_two_addr, 2, 22, 1),
        );
        leader_kv.seed(
            NodeId(3),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(follower_three_addr, 3, 33, 1),
        );

        let fence = test_fence(11, &[1, 2, 3], &[(1, 1), (2, 22), (3, 33)]);
        let prepare = BarrierAnnouncement {
            epoch: 3,
            checkpoint_id: 3,
            assignment_fence: Some(fence.clone()),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        leader
            .announce_prepare(&prepare, Duration::from_secs(2))
            .await
            .unwrap();
        wait_for_direct_prepare(&follower_two, &prepare).await;
        wait_for_direct_prepare(&follower_three, &prepare).await;
        follower_two
            .ack(&BarrierAck {
                epoch: prepare.epoch,
                checkpoint_id: prepare.checkpoint_id,
                assignment_digest: Some(fence.digest()),
                flags: prepare.flags,
                disposition: BarrierAckDisposition::Failed,
                error: Some("injected prepare failure".into()),
                watermark: CheckpointWatermark::Uninitialized,
            })
            .await
            .unwrap();

        let started = std::time::Instant::now();
        let outcome = leader
            .wait_for_quorum(&prepare, &[NodeId(2), NodeId(3)], Duration::from_secs(2))
            .await;
        assert!(matches!(outcome, QuorumOutcome::Failed { failures }
                if failures == vec![(NodeId(2), "injected prepare failure".into())]));
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn publication_order_rejects_a_stale_terminal_and_terminal_cancels_tasks() {
        let (leader, follower, proof) = started_barrier_pair().await;
        let fence = test_fence(11, &[1, 2], &[(1, 1), (2, 22)]);
        let first = BarrierAnnouncement {
            epoch: 3,
            checkpoint_id: 3,
            assignment_fence: Some(fence.clone()),
            leader_proof: Some(proof.clone()),
            phase: Phase::Prepare,
            flags: 0,
        };
        leader
            .announce_prepare(&first, Duration::from_secs(1))
            .await
            .unwrap();
        wait_for_direct_prepare(&follower, &first).await;
        assert_eq!(pending_prepare_waiters(&follower, &first), 1);

        let successor = BarrierAnnouncement {
            epoch: 4,
            checkpoint_id: 4,
            ..first.clone()
        };
        leader
            .announce_prepare(&successor, Duration::from_secs(1))
            .await
            .unwrap();
        wait_for_direct_prepare(&follower, &successor).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while pending_prepare_waiters(&follower, &first) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the newer Prepare did not cancel its predecessor task");
        assert_eq!(pending_prepare_waiters(&follower, &successor), 1);

        let durable_successor = leader
            .kv
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .unwrap();
        let stale_abort = BarrierAnnouncement {
            phase: Phase::Abort,
            ..first
        };
        let error = leader.announce(&stale_abort).await.unwrap_err();
        assert!(error.contains("stale barrier publication"), "{error}");
        assert_eq!(
            leader.kv.read_from(NodeId(1), ANNOUNCEMENT_KEY).await,
            Some(durable_successor)
        );
        assert_eq!(pending_prepare_waiters(&follower, &successor), 1);

        let abort = BarrierAnnouncement {
            phase: Phase::Abort,
            ..successor.clone()
        };
        leader.announce(&abort).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while pending_prepare_waiters(&follower, &successor) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the terminal announcement did not cancel its Prepare task");
        let state = leader.grpc.lock().clone().unwrap();
        assert!(state.prepare_fanout.lock().is_none());
    }

    #[tokio::test]
    async fn newer_prepare_cancels_obsolete_fanout_before_its_durable_write() {
        let leader_inner = kv(NodeId(1));
        let leader_kv = Arc::new(GateNextAnnouncementKv::new(Arc::clone(&leader_inner)));
        let follower_kv = kv(NodeId(2));
        let (store, proof) = lease_authority().await;
        let leader = Arc::new(coordinator(leader_kv.clone(), Arc::clone(&store)));
        let follower = coordinator(follower_kv, store);
        bind_process(&leader, 1, 1, 1);
        bind_process(&follower, 2, 22, 1);
        install_local_proof(&leader, &proof);
        leader
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let follower_addr = follower
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        leader_inner.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(follower_addr, 2, 22, 1),
        );
        let first = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(test_fence(9, &[1, 2], &[(1, 1), (2, 22)])),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        leader
            .announce_prepare(&first, Duration::from_secs(1))
            .await
            .unwrap();
        wait_for_direct_prepare(&follower, &first).await;
        assert_eq!(pending_prepare_waiters(&follower, &first), 1);
        let successor = BarrierAnnouncement {
            epoch: 2,
            checkpoint_id: 2,
            ..first.clone()
        };
        leader_kv.arm();
        let write_started = leader_kv.write_started.notified();
        tokio::pin!(write_started);
        let announce_task = tokio::spawn({
            let leader = Arc::clone(&leader);
            let successor = successor.clone();
            async move {
                leader
                    .announce_prepare(&successor, Duration::from_secs(1))
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), &mut write_started)
            .await
            .expect("successor Prepare durable write did not reach the gate");
        tokio::time::timeout(Duration::from_secs(1), async {
            while pending_prepare_waiters(&follower, &first) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("admitting the successor did not cancel the obsolete Prepare RPC");
        let durable = leader_inner
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<BarrierAnnouncement>(&durable).unwrap(),
            first
        );

        leader_kv.release_write.notify_one();
        announce_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn quorum_roster_mismatch_aborts_the_eager_batch() {
        let (leader, follower, proof) = started_barrier_pair().await;
        let fence = test_fence(12, &[1, 2], &[(1, 1), (2, 22)]);
        let prepare = BarrierAnnouncement {
            epoch: 5,
            checkpoint_id: 5,
            assignment_fence: Some(fence),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        leader
            .announce_prepare(&prepare, Duration::from_millis(100))
            .await
            .unwrap();
        wait_for_direct_prepare(&follower, &prepare).await;

        let outcome = leader
            .wait_for_quorum(&prepare, &[NodeId(3)], Duration::from_millis(100))
            .await;
        assert!(matches!(outcome, QuorumOutcome::Failed { .. }));
        tokio::time::timeout(Duration::from_secs(1), async {
            while pending_prepare_waiters(&follower, &prepare) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("roster mismatch did not abort the eager Prepare task");

        let retry = leader
            .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_millis(100))
            .await;
        assert!(
            matches!(retry, QuorumOutcome::Failed { .. }),
            "a rejected batch must not be silently recreated: {retry:?}"
        );
    }

    #[tokio::test]
    async fn test_grpc_barrier_flow() {
        let leader_kv = kv(NodeId(1));
        let follower_kv = kv(NodeId(2));
        let (store, proof) = lease_authority().await;
        let leader_coord = coordinator(leader_kv.clone(), Arc::clone(&store));
        let follower_coord = coordinator(follower_kv.clone(), store);
        install_local_proof(&leader_coord, &proof);
        bind_process(&leader_coord, 1, 1, 1);
        bind_process(&follower_coord, 2, 22, 1);

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let leader_addr = leader_coord.start_server(addr, None).await.unwrap();
        let bound_addr = follower_coord.start_server(addr, None).await.unwrap();

        leader_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(bound_addr, 2, 22, 1),
        );
        follower_kv.seed(
            NodeId(1),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(leader_addr, 1, 1, 1),
        );

        // Sequencing handshake: observation is latest-wins, so the
        // leader must not announce Commit until the follower has
        // observed Aligned (otherwise Commit may overwrite it).
        let (aligned_seen_tx, aligned_seen_rx) = tokio::sync::oneshot::channel::<()>();
        let assignment_fence = test_fence(9, &[1, 2, 1, 2], &[(1, 1), (2, 22)]);
        let follower_fence = assignment_fence.clone();

        let follower_task = tokio::spawn(async move {
            let ann = wait_observe(&follower_coord, NodeId(1), Phase::Prepare).await;
            assert_eq!(ann.epoch, 1);
            assert_eq!(ann.checkpoint_id, 1);
            assert_eq!(ann.assignment_fence.as_ref(), Some(&follower_fence));

            follower_coord
                .ack(&BarrierAck {
                    epoch: 1,
                    checkpoint_id: 1,
                    assignment_digest: Some(follower_fence.digest()),
                    flags: 0,
                    disposition: BarrierAckDisposition::Captured,
                    error: None,
                    watermark: CheckpointWatermark::Active(100),
                })
                .await
                .unwrap();

            let aligned_ann = wait_observe(&follower_coord, NodeId(1), Phase::Aligned).await;
            assert_eq!(aligned_ann.epoch, 1);
            assert_eq!(aligned_ann.assignment_fence.as_ref(), Some(&follower_fence));
            aligned_seen_tx.send(()).unwrap();

            let commit_ann = wait_observe(&follower_coord, NodeId(1), Phase::Commit).await;
            assert_eq!(commit_ann.assignment_fence.as_ref(), Some(&follower_fence));
        });

        let prepare = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(assignment_fence),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        leader_coord
            .announce_prepare(&prepare, Duration::from_secs(5))
            .await
            .unwrap();

        let outcome = leader_coord
            .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_secs(5))
            .await;
        match outcome {
            QuorumOutcome::Reached {
                acks,
                follower_watermark,
                handoff_replay_pending: false,
            } => {
                assert_eq!(acks, vec![NodeId(2)]);
                assert_eq!(follower_watermark, CheckpointWatermark::Active(100));

                // Two-level completion: resume gate first…
                leader_coord
                    .announce(&BarrierAnnouncement {
                        epoch: 1,
                        checkpoint_id: 1,
                        assignment_fence: prepare.assignment_fence.clone(),
                        leader_proof: prepare.leader_proof.clone(),
                        phase: Phase::Aligned,
                        flags: 0,
                    })
                    .await
                    .unwrap();
                aligned_seen_rx.await.unwrap();

                // …then the restorable decision.
                leader_coord
                    .announce(&BarrierAnnouncement {
                        epoch: 1,
                        checkpoint_id: 1,
                        assignment_fence: prepare.assignment_fence.clone(),
                        leader_proof: prepare.leader_proof.clone(),
                        phase: Phase::Commit,
                        flags: 0,
                    })
                    .await
                    .unwrap();
            }
            other => panic!("expected Reached, got {other:?}"),
        }

        follower_task.await.unwrap();
    }

    #[tokio::test]
    async fn certified_phase_uses_frozen_roster_not_active_membership() {
        let leader_kv = kv(NodeId(1));
        let follower_kv = kv(NodeId(2));
        let outsider_kv = kv(NodeId(3));
        let (store, leader_proof) = lease_authority().await;
        let mut leader = coordinator(leader_kv.clone(), Arc::clone(&store));
        let follower = coordinator(follower_kv, Arc::clone(&store));
        let outsider = coordinator(outsider_kv, store);
        bind_process(&leader, 1, 1, 1);
        bind_process(&follower, 2, 22, 1);
        bind_process(&outsider, 3, 33, 1);
        install_local_proof(&leader, &leader_proof);

        let member = |node_id: u64, state| NodeInfo {
            id: NodeId(node_id),
            name: format!("node-{node_id}"),
            rpc_address: String::new(),
            state,
            metadata: crate::cluster::discovery::NodeMetadata::default(),
            last_heartbeat_ms: 0,
        };
        let (_members_tx, members_rx) = watch::channel(vec![
            member(1, NodeState::Active),
            member(2, NodeState::Draining),
            member(3, NodeState::Active),
        ]);
        leader.set_leader_election(NodeId(1), members_rx, Arc::new(AtomicBool::new(true)));

        leader
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let follower_addr = follower
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let outsider_addr = outsider
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        leader_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(follower_addr, 2, 22, 1),
        );
        leader_kv.seed(
            NodeId(3),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(outsider_addr, 3, 33, 1),
        );

        let fence = test_fence(9, &[1, 2], &[(1, 1), (2, 22)]);
        let prepare = BarrierAnnouncement {
            epoch: 4,
            checkpoint_id: 4,
            assignment_fence: Some(fence),
            leader_proof: Some(leader_proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        reach_two_node_prepare_quorum(
            &leader,
            &follower,
            &prepare,
            CheckpointWatermark::Active(100),
        )
        .await;
        let aligned = BarrierAnnouncement {
            phase: Phase::Aligned,
            ..prepare
        };
        leader.announce(&aligned).await.unwrap();
        let observed = wait_observe_exact(
            &follower,
            NodeId(1),
            CheckpointAttempt::canonical(4),
            Phase::Aligned,
        )
        .await;
        assert_eq!(observed, aligned);
        assert!(outsider
            .grpc
            .lock()
            .as_ref()
            .unwrap()
            .latest_rx
            .borrow()
            .is_none());
    }

    #[tokio::test]
    async fn prepare_reconnects_a_stale_client_within_the_same_quorum_deadline() {
        let leader_kv = kv(NodeId(1));
        let follower_kv = kv(NodeId(2));
        let (store, proof) = lease_authority().await;
        let leader_coord = coordinator(leader_kv.clone(), Arc::clone(&store));
        let follower_coord = coordinator(follower_kv.clone(), store);
        install_local_proof(&leader_coord, &proof);
        bind_process(&leader_coord, 1, 1, 1);
        bind_process(&follower_coord, 2, 22, 1);
        let leader_addr = leader_coord
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let follower_addr = follower_coord
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        leader_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(follower_addr, 2, 22, 1),
        );
        follower_kv.seed(
            NodeId(1),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(leader_addr, 1, 1, 1),
        );

        let dead_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let dead_addr = dead_listener.local_addr().unwrap();
        drop(dead_listener);
        let dead_channel = tonic::transport::Endpoint::from_shared(format!("http://{dead_addr}"))
            .unwrap()
            .connect_lazy();
        let state = leader_coord.grpc.lock().clone().unwrap();
        state.clients.lock().insert(
            NodeId(2),
            BarrierClientEntry {
                process: Some(BarrierProcessIdentity {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(22),
                    process_term: 1,
                }),
                client: barrier_v1::barrier_sync_client::BarrierSyncClient::new(dead_channel),
            },
        );

        let assignment_fence = test_fence(9, &[1, 2], &[(1, 1), (2, 22)]);
        let follower_fence = assignment_fence.clone();
        let prepare = BarrierAnnouncement {
            epoch: 2,
            checkpoint_id: 2,
            assignment_fence: Some(assignment_fence),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        leader_coord
            .announce_prepare(&prepare, Duration::from_secs(2))
            .await
            .unwrap();

        let follower = async {
            let announcement = wait_observe(&follower_coord, NodeId(1), Phase::Prepare).await;
            follower_coord
                .ack(&BarrierAck {
                    epoch: announcement.epoch,
                    checkpoint_id: announcement.checkpoint_id,
                    assignment_digest: Some(follower_fence.digest()),
                    flags: announcement.flags,
                    disposition: BarrierAckDisposition::Captured,
                    error: None,
                    watermark: CheckpointWatermark::Active(101),
                })
                .await
                .unwrap();
        };
        let leader = leader_coord.wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_secs(2));
        let (outcome, ()) = tokio::join!(leader, follower);

        assert!(
            matches!(
                &outcome,
                QuorumOutcome::Reached {
                    acks,
                    follower_watermark: CheckpointWatermark::Active(101),
                    handoff_replay_pending: false,
                } if acks.as_slice() == [NodeId(2)]
            ),
            "the stale transport client must be evicted and re-resolved: {outcome:?}"
        );
    }

    #[tokio::test]
    async fn certificate_conflict_cannot_steal_an_exact_prepare_waiter() {
        let leader_kv = kv(NodeId(1));
        let follower_kv = kv(NodeId(2));
        let (store, proof) = lease_authority().await;
        let leader_coord = coordinator(leader_kv.clone(), Arc::clone(&store));
        let follower_coord = coordinator(follower_kv.clone(), store);
        install_local_proof(&leader_coord, &proof);
        bind_process(&leader_coord, 1, 1, 1);
        bind_process(&follower_coord, 2, 22, 1);
        let leader_addr = leader_coord
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let follower_addr = follower_coord
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        leader_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(follower_addr, 2, 22, 1),
        );
        follower_kv.seed(
            NodeId(1),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(leader_addr, 1, 1, 1),
        );

        let accepted_fence = test_fence(9, &[1, 2, 1, 2], &[(1, 1), (2, 22)]);
        let conflicting_fence = test_fence(9, &[2, 1, 1, 2], &[(1, 1), (2, 22)]);
        let accepted = BarrierAnnouncement {
            epoch: 12,
            checkpoint_id: 12,
            assignment_fence: Some(accepted_fence.clone()),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        let conflicting = BarrierAnnouncement {
            assignment_fence: Some(conflicting_fence),
            ..accepted.clone()
        };
        leader_coord
            .announce_prepare(&accepted, Duration::from_secs(2))
            .await
            .unwrap();

        let accepted_wait =
            leader_coord.wait_for_quorum(&accepted, &[NodeId(2)], Duration::from_secs(2));
        let follower = async {
            let _ = wait_observe(&follower_coord, NodeId(1), Phase::Prepare).await;
            let conflicting_outcome = leader_coord
                .wait_for_quorum(&conflicting, &[NodeId(2)], Duration::from_millis(300))
                .await;
            follower_coord
                .ack(&BarrierAck {
                    epoch: accepted.epoch,
                    checkpoint_id: accepted.checkpoint_id,
                    assignment_digest: Some(accepted_fence.digest()),
                    flags: accepted.flags,
                    disposition: BarrierAckDisposition::Captured,
                    error: None,
                    watermark: CheckpointWatermark::Uninitialized,
                })
                .await
                .unwrap();
            conflicting_outcome
        };
        let (accepted_outcome, conflicting_outcome) = tokio::join!(accepted_wait, follower);
        assert!(
            matches!(accepted_outcome, QuorumOutcome::Reached { .. }),
            "{accepted_outcome:?}"
        );
        assert!(
            matches!(conflicting_outcome, QuorumOutcome::Failed { .. }),
            "a different certificate must not consume the accepted ACK: {conflicting_outcome:?}"
        );

        // A terminal notification alone cannot mutate authoritative Prepare state.
        leader_coord
            .announce(&BarrierAnnouncement {
                phase: Phase::Abort,
                ..conflicting
            })
            .await
            .unwrap();
        let state = follower_coord.grpc.lock().clone().unwrap();
        assert!(state.prepare_acks.lock().pending.is_empty());
    }

    #[tokio::test]
    async fn successor_abort_does_not_poison_the_grpc_relay() {
        let authority = Arc::new(crate::cluster::control::LeaderLeaseStore::new(
            Arc::new(InMemory::new()),
            1,
        ));
        let original_owner = crate::cluster::control::LeaderLeaseOwner {
            node: NodeId(1),
            boot: uuid::Uuid::from_u128(1),
            process_term: 1,
        };
        let successor_owner = crate::cluster::control::LeaderLeaseOwner {
            node: NodeId(3),
            boot: uuid::Uuid::from_u128(3),
            process_term: 2,
        };
        let original_lease = match authority.begin_new_term(&original_owner, 1).await.unwrap() {
            crate::cluster::control::LeaseOutcome::Acquired(lease) => lease,
            crate::cluster::control::LeaseOutcome::Held(_) => unreachable!(),
        };
        let takeover_observation = authority
            .observe_rival(&successor_owner, &original_lease)
            .unwrap();
        let successor_proof = proof(3, 3, 2, original_lease.token + 1);

        let leader_kv = kv(NodeId(1));
        let follower_kv = kv(NodeId(2));
        let successor_kv = kv(NodeId(3));
        let leader = coordinator(leader_kv.clone(), Arc::clone(&authority));
        let follower = coordinator(follower_kv.clone(), Arc::clone(&authority));
        let successor = coordinator(successor_kv.clone(), Arc::clone(&authority));
        bind_process(&leader, 1, 1, 1);
        bind_process(&follower, 2, 2, 1);
        bind_process(&successor, 3, 3, 2);
        install_local_proof(&leader, &original_lease.proof());
        install_local_proof(&successor, &successor_proof);
        leader
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let follower_addr = follower
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        successor
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        leader_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(follower_addr, 2, 2, 1),
        );
        successor_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(follower_addr, 2, 2, 1),
        );

        let prepare = BarrierAnnouncement {
            epoch: 12,
            checkpoint_id: 12,
            assignment_fence: Some(test_fence(1, &[1, 2], &[(1, 1), (2, 2)])),
            leader_proof: Some(original_lease.proof()),
            phase: Phase::Prepare,
            flags: 0,
        };
        reach_two_node_prepare_quorum(
            &leader,
            &follower,
            &prepare,
            CheckpointWatermark::Uninitialized,
        )
        .await;
        let aligned = BarrierAnnouncement {
            phase: Phase::Aligned,
            ..prepare
        };
        let abort = BarrierAnnouncement {
            assignment_fence: Some(test_fence(2, &[2, 3], &[(2, 2), (3, 3)])),
            leader_proof: Some(successor_proof.clone()),
            phase: Phase::Abort,
            ..aligned.clone()
        };

        leader.announce(&aligned).await.unwrap();
        wait_observe_exact(
            &follower,
            NodeId(1),
            CheckpointAttempt::new(12, 12),
            Phase::Aligned,
        )
        .await;
        successor.announce(&abort).await.unwrap();
        wait_observe_exact(
            &follower,
            NodeId(1),
            CheckpointAttempt::new(12, 12),
            Phase::Abort,
        )
        .await;
        leader.announce(&aligned).await.unwrap();

        tokio::time::sleep(Duration::from_millis(2)).await;
        let successor_lease = match authority
            .try_takeover(&successor_owner, &takeover_observation, 2)
            .await
            .unwrap()
        {
            crate::cluster::control::LeaseOutcome::Acquired(lease) => lease,
            crate::cluster::control::LeaseOutcome::Held(_) => unreachable!(),
        };
        assert_eq!(successor_lease.proof(), successor_proof);

        let successor_prepare = BarrierAnnouncement {
            epoch: 13,
            checkpoint_id: 13,
            phase: Phase::Prepare,
            ..abort
        };
        reach_two_node_prepare_quorum(
            &successor,
            &follower,
            &successor_prepare,
            CheckpointWatermark::Uninitialized,
        )
        .await;
        let successor_aligned = BarrierAnnouncement {
            phase: Phase::Aligned,
            ..successor_prepare
        };
        successor.announce(&successor_aligned).await.unwrap();
        wait_observe_exact(
            &follower,
            NodeId(1),
            CheckpointAttempt::new(13, 13),
            Phase::Aligned,
        )
        .await;
        let state = follower.grpc.lock().clone().unwrap();
        assert!(state.merge_error.lock().is_none());
    }

    #[tokio::test]
    async fn phase_rpc_deadline_bounds_a_live_handler() {
        use object_store::throttle::{ThrottleConfig, ThrottledStore};

        let durable = Arc::new(ThrottledStore::new(
            InMemory::new(),
            ThrottleConfig::default(),
        ));
        let store = Arc::new(crate::cluster::control::LeaderLeaseStore::new(
            durable.clone(),
            1_000,
        ));
        let owner = crate::cluster::control::LeaderLeaseOwner {
            node: NodeId(1),
            boot: uuid::Uuid::from_u128(1),
            process_term: 1,
        };
        let lease = match store.begin_new_term(&owner, 1).await.unwrap() {
            crate::cluster::control::LeaseOutcome::Acquired(lease) => lease,
            crate::cluster::control::LeaseOutcome::Held(_) => unreachable!(),
        };
        durable.config_mut(|config| config.wait_get_per_call = Duration::from_secs(5));

        let follower_coord = coordinator(kv(NodeId(2)), store);
        bind_process(&follower_coord, 2, 22, 1);
        let bound_addr = follower_coord
            .start_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();

        let leader_kv = kv(NodeId(1));
        leader_kv.seed(
            NodeId(2),
            BARRIER_ADDR_KEY,
            endpoint_advertisement(bound_addr, 2, 22, 1),
        );
        let clients = Arc::new(parking_lot::Mutex::new(FxHashMap::default()));
        let started = tokio::time::Instant::now();
        let error = send_phase_rpc(
            NodeId(2),
            Arc::clone(&clients),
            leader_kv,
            BarrierAnnouncement {
                epoch: 7,
                checkpoint_id: 7,
                assignment_fence: Some(test_fence(1, &[1, 2], &[(1, 11), (2, 22)])),
                leader_proof: Some(lease.proof()),
                phase: Phase::Aligned,
                flags: 0,
            },
            started + Duration::from_millis(500),
        )
        .await
        .unwrap_err();

        assert!(error.contains("request deadline"), "{error}");
        assert!(started.elapsed() >= Duration::from_millis(400));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "phase RPC exceeded its absolute deadline"
        );
        assert!(
            clients.lock().is_empty(),
            "a timed-out client must be evicted"
        );
    }
}

#[cfg(feature = "cluster")]
#[test]
fn direct_phase_merge_is_monotonic_for_an_exact_attempt() {
    let base = BarrierAnnouncement {
        epoch: 20,
        checkpoint_id: 20,
        assignment_fence: None,
        leader_proof: None,
        phase: Phase::Prepare,
        flags: 0,
    };

    for terminal in [Phase::Commit, Phase::Abort] {
        let decided = BarrierAnnouncement {
            phase: terminal,
            ..base.clone()
        };
        for delayed in [Phase::Prepare, Phase::Aligned] {
            let merged = merge_direct_announcement(
                decided.clone(),
                BarrierAnnouncement {
                    phase: delayed,
                    ..base.clone()
                },
            )
            .unwrap();
            assert_eq!(merged.phase, terminal);
        }
    }

    let commit = BarrierAnnouncement {
        phase: Phase::Commit,
        ..base.clone()
    };
    let conflicting_abort = BarrierAnnouncement {
        phase: Phase::Abort,
        ..base.clone()
    };
    assert!(merge_direct_announcement(commit, conflicting_abort).is_err());

    let newer_attempt = BarrierAnnouncement {
        epoch: base.epoch + 1,
        checkpoint_id: base.checkpoint_id + 1,
        ..base
    };
    assert_eq!(
        merge_direct_announcement(
            BarrierAnnouncement {
                epoch: 20,
                checkpoint_id: 20,
                phase: Phase::Commit,
                ..newer_attempt.clone()
            },
            newer_attempt.clone(),
        )
        .unwrap()
        .checkpoint_id,
        21,
        "the canonical checkpoint order advanced"
    );

    for conflicting in [
        BarrierAnnouncement {
            epoch: 20,
            checkpoint_id: 21,
            phase: Phase::Prepare,
            ..newer_attempt.clone()
        },
        BarrierAnnouncement {
            epoch: 21,
            checkpoint_id: 20,
            phase: Phase::Prepare,
            ..newer_attempt.clone()
        },
    ] {
        assert!(merge_direct_announcement(newer_attempt.clone(), conflicting).is_err());
    }
}

#[cfg(feature = "cluster")]
#[test]
fn successor_terminal_supersedes_a_reversible_direct_certificate() {
    let (aligned, abort) = failover_aligned_and_abort();

    assert_eq!(
        merge_direct_announcement(aligned.clone(), abort.clone()).unwrap(),
        abort
    );
    assert_eq!(
        merge_direct_announcement(abort.clone(), aligned.clone()).unwrap(),
        abort
    );
    assert!(merge_direct_announcement(
        BarrierAnnouncement {
            phase: Phase::Abort,
            ..aligned.clone()
        },
        abort.clone(),
    )
    .is_err());
    assert!(merge_direct_announcement(
        aligned,
        BarrierAnnouncement {
            phase: Phase::Prepare,
            ..abort
        },
    )
    .is_err());
}

#[test]
fn durable_terminal_is_authoritative_during_channel_merge() {
    let base = BarrierAnnouncement {
        epoch: 21,
        checkpoint_id: 21,
        assignment_fence: None,
        leader_proof: None,
        phase: Phase::Prepare,
        flags: 0,
    };

    for (direct, durable) in [(Phase::Commit, Phase::Abort), (Phase::Abort, Phase::Commit)] {
        let merged = merge_observed_announcement(
            BarrierAnnouncement {
                phase: direct,
                ..base.clone()
            },
            BarrierAnnouncement {
                phase: durable,
                ..base.clone()
            },
        )
        .unwrap();
        assert_eq!(merged.phase, durable);
    }

    let merged = merge_observed_announcement(
        BarrierAnnouncement {
            phase: Phase::Commit,
            ..base.clone()
        },
        base.clone(),
    )
    .unwrap();
    assert_eq!(
        merged.phase,
        Phase::Commit,
        "lagging durable Prepare must not hide a delivered terminal phase"
    );

    let (aligned, abort) = failover_aligned_and_abort();
    assert_eq!(
        merge_observed_announcement(aligned.clone(), abort.clone()).unwrap(),
        abort
    );
    for direct in [Phase::Commit, Phase::Abort] {
        assert_eq!(
            merge_observed_announcement(
                BarrierAnnouncement {
                    phase: direct,
                    ..aligned.clone()
                },
                abort.clone(),
            )
            .unwrap(),
            abort,
            "the durable terminal must override every exact direct hint"
        );
    }
    assert_eq!(
        merge_observed_announcement(abort.clone(), aligned).unwrap(),
        abort,
        "a delivered successor terminal must beat the predecessor's durable reversible phase"
    );

    for durable in [
        BarrierAnnouncement {
            epoch: base.epoch,
            checkpoint_id: base.checkpoint_id + 1,
            ..base.clone()
        },
        BarrierAnnouncement {
            epoch: base.epoch + 1,
            checkpoint_id: base.checkpoint_id,
            ..base.clone()
        },
        BarrierAnnouncement {
            epoch: base.epoch - 1,
            checkpoint_id: base.checkpoint_id + 1,
            ..base.clone()
        },
        BarrierAnnouncement {
            epoch: base.epoch + 1,
            checkpoint_id: base.checkpoint_id - 1,
            ..base.clone()
        },
    ] {
        assert!(merge_observed_announcement(base.clone(), durable).is_err());
    }

    assert!(merge_observed_announcement(
        base.clone(),
        BarrierAnnouncement {
            flags: crate::checkpoint::flags::FULL_SNAPSHOT,
            ..base
        },
    )
    .is_err());
}

#[tokio::test]
async fn leader_announces_follower_observes() {
    for terminal_phase in [Phase::Commit, Phase::Abort] {
        let leader_kv = kv(NodeId(1));
        let coord = BarrierCoordinator::new(leader_kv.clone());
        let prepare = BarrierAnnouncement {
            epoch: 5,
            checkpoint_id: 5,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Prepare,
            flags: 0,
        };
        coord.announce(&prepare).await.unwrap();
        let got = coord.observe_hint(NodeId(1)).await.unwrap().unwrap();
        assert_eq!(got.epoch, 5);
        assert_eq!(got.checkpoint_id, 5);

        let terminal = BarrierAnnouncement {
            phase: terminal_phase,
            ..prepare.clone()
        };
        coord.announce(&terminal).await.unwrap();
        let error = coord.announce(&prepare).await.unwrap_err();
        assert!(error.contains("cannot regress"), "{error}");
        let durable = leader_kv
            .read_from(NodeId(1), ANNOUNCEMENT_KEY)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<BarrierAnnouncement>(&durable).unwrap(),
            terminal
        );
    }
}

#[tokio::test]
async fn observe_returns_none_when_leader_silent() {
    let k = kv(NodeId(1));
    let coord = BarrierCoordinator::new(k);
    assert!(coord.observe_hint(NodeId(1)).await.unwrap().is_none());
}

#[tokio::test]
async fn coordinator_rejects_noncanonical_attempts_before_publication() {
    for (epoch, checkpoint_id) in [(0, 0), (5, 0), (0, 5), (5, 6)] {
        let store = kv(NodeId(1));
        let coordinator = BarrierCoordinator::new(store.clone());
        let announcement = BarrierAnnouncement {
            epoch,
            checkpoint_id,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Prepare,
            flags: 0,
        };
        assert!(coordinator.announce(&announcement).await.is_err());
        assert!(store.read_from(NodeId(1), ANNOUNCEMENT_KEY).await.is_none());

        let acknowledgement = BarrierAck {
            epoch,
            checkpoint_id,
            assignment_digest: None,
            flags: 0,
            disposition: BarrierAckDisposition::Captured,
            error: None,
            watermark: CheckpointWatermark::Uninitialized,
        };
        assert!(coordinator.ack(&acknowledgement).await.is_err());
        assert!(store.read_from(NodeId(1), ACK_KEY).await.is_none());
    }
}

#[cfg(feature = "cluster")]
#[test]
fn wire_requests_require_a_canonical_attempt() {
    validate_wire_checkpoint_attempt(7, 7).unwrap();
    for (epoch, checkpoint_id) in [(0, 0), (7, 0), (0, 7), (7, 8)] {
        let error = validate_wire_checkpoint_attempt(epoch, checkpoint_id).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}

fn announcement_json(epoch: u64, checkpoint_id: u64) -> String {
    serde_json::to_string(&BarrierAnnouncement {
        epoch,
        checkpoint_id,
        assignment_fence: None,
        leader_proof: None,
        phase: Phase::Prepare,
        flags: 0,
    })
    .unwrap()
}

#[tokio::test]
async fn scan_latest_rejects_noncanonical_attempts() {
    let valid = kv(NodeId(1));
    valid.seed(NodeId(1), ANNOUNCEMENT_KEY, announcement_json(5, 5));
    valid.seed(NodeId(2), ANNOUNCEMENT_KEY, announcement_json(6, 6));
    assert_eq!(
        BarrierCoordinator::new(valid)
            .scan_latest_announcement()
            .await
            .unwrap()
            .as_ref()
            .map(announcement_attempt),
        Some(CheckpointAttempt::canonical(6))
    );

    for (epoch, checkpoint_id) in [(0, 0), (5, 0), (0, 5), (5, 6), (6, 5)] {
        let invalid = kv(NodeId(1));
        invalid.seed(
            NodeId(1),
            ANNOUNCEMENT_KEY,
            announcement_json(epoch, checkpoint_id),
        );
        let error = BarrierCoordinator::new(invalid)
            .scan_latest_announcement()
            .await
            .unwrap_err();
        assert!(
            error.contains("one nonzero canonical checkpoint ID"),
            "{error}"
        );
    }
}

fn certified_announcement(
    fence: crate::checkpoint::CheckpointAssignmentFence,
    proof: crate::cluster::control::LeaderProof,
    phase: Phase,
) -> BarrierAnnouncement {
    BarrierAnnouncement {
        epoch: 5,
        checkpoint_id: 5,
        assignment_fence: Some(fence),
        leader_proof: Some(proof),
        phase,
        flags: 0,
    }
}

fn failover_aligned_and_abort() -> (BarrierAnnouncement, BarrierAnnouncement) {
    let aligned = certified_announcement(
        test_fence(9, &[1, 2], &[(1, 11), (2, 22)]),
        crate::cluster::control::LeaderProof {
            owner: crate::checkpoint::LeaderProofOwner {
                node_id: 2,
                boot_id: uuid::Uuid::from_u128(22),
                process_term: 3,
            },
            fencing_token: 7,
        },
        Phase::Aligned,
    );
    let abort = certified_announcement(
        test_fence(10, &[1], &[(1, 11)]),
        crate::cluster::control::LeaderProof {
            fencing_token: 8,
            ..test_leader_proof()
        },
        Phase::Abort,
    );
    (aligned, abort)
}

fn test_leader_proof() -> crate::cluster::control::LeaderProof {
    crate::cluster::control::LeaderProof {
        owner: crate::checkpoint::LeaderProofOwner {
            node_id: 1,
            boot_id: uuid::Uuid::from_u128(11),
            process_term: 3,
        },
        fencing_token: 7,
    }
}

fn plain_announcement(epoch: u64, checkpoint_id: u64) -> BarrierAnnouncement {
    BarrierAnnouncement {
        epoch,
        checkpoint_id,
        assignment_fence: None,
        leader_proof: None,
        phase: Phase::Prepare,
        flags: 0,
    }
}

#[tokio::test]
async fn scan_latest_rejects_exact_attempt_equivocation() {
    let fence = test_fence(9, &[1, 2], &[(1, 11), (2, 22)]);
    let proof = test_leader_proof();

    let cases = [
        (
            certified_announcement(fence.clone(), proof.clone(), Phase::Prepare),
            certified_announcement(
                test_fence(10, &[1, 2], &[(1, 11), (2, 22)]),
                proof.clone(),
                Phase::Prepare,
            ),
        ),
        (
            certified_announcement(fence.clone(), proof.clone(), Phase::Prepare),
            certified_announcement(
                fence.clone(),
                crate::cluster::control::LeaderProof {
                    fencing_token: proof.fencing_token + 1,
                    ..proof.clone()
                },
                Phase::Prepare,
            ),
        ),
        (
            certified_announcement(fence.clone(), proof.clone(), Phase::Commit),
            certified_announcement(fence.clone(), proof.clone(), Phase::Abort),
        ),
        (
            certified_announcement(fence.clone(), proof.clone(), Phase::Abort),
            certified_announcement(
                test_fence(10, &[1, 2], &[(1, 11), (2, 22)]),
                proof.clone(),
                Phase::Abort,
            ),
        ),
        (
            certified_announcement(fence.clone(), proof.clone(), Phase::Prepare),
            BarrierAnnouncement {
                flags: crate::checkpoint::flags::FULL_SNAPSHOT,
                ..certified_announcement(fence.clone(), proof.clone(), Phase::Prepare)
            },
        ),
    ];

    for (left, right) in cases {
        let conflicting = kv(NodeId(1));
        conflicting.seed(
            NodeId(1),
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&left).unwrap(),
        );
        conflicting.seed(
            NodeId(2),
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&right).unwrap(),
        );
        assert!(BarrierCoordinator::new(conflicting)
            .scan_latest_announcement()
            .await
            .is_err());
    }

    let progressing = kv(NodeId(1));
    progressing.seed(
        NodeId(1),
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&certified_announcement(
            fence.clone(),
            proof.clone(),
            Phase::Prepare,
        ))
        .unwrap(),
    );
    progressing.seed(
        NodeId(2),
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&certified_announcement(fence, proof, Phase::Commit)).unwrap(),
    );
    assert_eq!(
        BarrierCoordinator::new(progressing)
            .scan_latest_announcement()
            .await
            .unwrap()
            .as_ref()
            .map(announcement_attempt),
        Some(CheckpointAttempt::canonical(5))
    );
}

#[tokio::test]
async fn scan_latest_accepts_successor_settlement_and_later_attempt() {
    let (aligned, abort) = failover_aligned_and_abort();
    let history = kv(NodeId(1));
    history.seed(
        NodeId(1),
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&aligned).unwrap(),
    );
    history.seed(
        NodeId(2),
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&abort).unwrap(),
    );
    let coordinator = BarrierCoordinator::new(history.clone());
    assert_eq!(
        coordinator
            .scan_latest_announcement()
            .await
            .unwrap()
            .as_ref()
            .map(announcement_attempt),
        Some(CheckpointAttempt::canonical(5))
    );

    history.seed(NodeId(3), ANNOUNCEMENT_KEY, announcement_json(6, 6));
    assert_eq!(
        coordinator
            .scan_latest_announcement()
            .await
            .unwrap()
            .as_ref()
            .map(announcement_attempt),
        Some(CheckpointAttempt::canonical(6))
    );
}

#[test]
fn scanned_successor_terminal_cannot_hide_reversible_equivocation() {
    let (aligned, abort) = failover_aligned_and_abort();
    let conflicting = BarrierAnnouncement {
        phase: Phase::Prepare,
        ..abort.clone()
    };
    let records = [aligned, conflicting, abort];

    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let history = order
            .into_iter()
            .map(|index| records[index].clone())
            .collect();
        assert!(validate_scanned_announcements(history).is_err());
    }
}

#[test]
fn scanned_history_cannot_hide_earlier_conflicts_behind_a_newer_attempt() {
    let fence = test_fence(9, &[1, 2], &[(1, 11), (2, 22)]);
    let proof = test_leader_proof();
    let newer = plain_announcement(6, 6);
    let cases = [
        vec![
            plain_announcement(5, 5),
            newer.clone(),
            BarrierAnnouncement {
                flags: crate::checkpoint::flags::FULL_SNAPSHOT,
                ..plain_announcement(5, 5)
            },
        ],
        vec![
            certified_announcement(fence.clone(), proof.clone(), Phase::Prepare),
            newer.clone(),
            certified_announcement(
                test_fence(10, &[1, 2], &[(1, 11), (2, 22)]),
                proof.clone(),
                Phase::Prepare,
            ),
        ],
        vec![
            certified_announcement(fence.clone(), proof.clone(), Phase::Commit),
            newer,
            certified_announcement(fence, proof, Phase::Abort),
        ],
    ];

    for history in cases {
        assert!(validate_scanned_announcements(history).is_err());
    }
}

#[tokio::test]
async fn scan_latest_rejects_malformed_history() {
    let malformed = kv(NodeId(1));
    malformed.seed(NodeId(2), ANNOUNCEMENT_KEY, "not-json".to_string());
    assert!(BarrierCoordinator::new(malformed)
        .scan_latest_announcement()
        .await
        .is_err());
}

#[derive(Debug)]
struct FailingScanKv;

#[async_trait]
impl ClusterKv for FailingScanKv {
    async fn write(&self, _key: &str, _value: String) {}

    async fn read_from(&self, _who: NodeId, _key: &str) -> Option<String> {
        None
    }

    async fn scan(&self, _key: &str) -> Vec<(NodeId, String)> {
        Vec::new()
    }

    async fn scan_checked(&self, _key: &str) -> Result<Vec<(NodeId, String)>, String> {
        Err("injected scan failure".to_string())
    }
}

#[tokio::test]
async fn scan_latest_propagates_scan_failure() {
    let coordinator = BarrierCoordinator::new(Arc::new(FailingScanKv));
    let error = coordinator.scan_latest_announcement().await.unwrap_err();
    assert_eq!(error, "injected scan failure");
}

fn certified_kv_prepare(attempt: u64, followers: &[NodeId], flags: u64) -> BarrierAnnouncement {
    let mut owners = vec![1];
    owners.extend(followers.iter().map(|node| node.0));
    let participants = owners
        .iter()
        .map(|node| (*node, u128::from(*node) * 11))
        .collect::<Vec<_>>();
    BarrierAnnouncement {
        epoch: attempt,
        checkpoint_id: attempt,
        assignment_fence: Some(test_fence(attempt, &owners, &participants)),
        leader_proof: None,
        phase: Phase::Prepare,
        flags,
    }
}

#[tokio::test]
async fn remote_quorum_requires_an_assignment_certificate() {
    let outcome = BarrierCoordinator::new(kv(NodeId(1)))
        .wait_for_quorum(
            &plain_announcement(7, 7),
            &[NodeId(2)],
            Duration::from_secs(1),
        )
        .await;
    assert!(matches!(outcome, QuorumOutcome::Failed { .. }));
}

#[tokio::test]
async fn quorum_reached_when_all_ack_success() {
    let k = kv(NodeId(1));
    let prepare = certified_kv_prepare(7, &[NodeId(2), NodeId(3)], 0);
    let assignment_digest = prepare
        .assignment_fence
        .as_ref()
        .map(crate::checkpoint::CheckpointAssignmentFence::digest);
    let ack_json = serde_json::to_string(&BarrierAck {
        epoch: 7,
        checkpoint_id: 7,
        assignment_digest,
        flags: 0,
        disposition: BarrierAckDisposition::Captured,
        error: None,
        watermark: CheckpointWatermark::Uninitialized,
    })
    .unwrap();
    k.seed(NodeId(2), ACK_KEY, ack_json.clone());
    k.seed(NodeId(3), ACK_KEY, ack_json);

    let coord = BarrierCoordinator::new(k);
    let outcome = coord
        .wait_for_quorum(
            &prepare,
            &[NodeId(2), NodeId(3)],
            Duration::from_millis(200),
        )
        .await;
    match outcome {
        QuorumOutcome::Reached {
            mut acks,
            follower_watermark,
            handoff_replay_pending: false,
        } => {
            acks.sort_by_key(|n| n.0);
            assert_eq!(acks, vec![NodeId(2), NodeId(3)]);
            assert_eq!(follower_watermark, CheckpointWatermark::Uninitialized);
        }
        other => panic!("expected Reached, got {other:?}"),
    }
}

#[tokio::test]
async fn uninitialized_participant_blocks_cluster_watermark_advancement() {
    let k = kv(NodeId(1));
    let prepare = certified_kv_prepare(7, &[NodeId(2), NodeId(3)], 0);
    let assignment_digest = prepare
        .assignment_fence
        .as_ref()
        .map(crate::checkpoint::CheckpointAssignmentFence::digest);
    for (node, watermark) in [
        (NodeId(2), CheckpointWatermark::Active(100)),
        (NodeId(3), CheckpointWatermark::Uninitialized),
    ] {
        k.seed(
            node,
            ACK_KEY,
            serde_json::to_string(&BarrierAck {
                epoch: 7,
                checkpoint_id: 7,
                assignment_digest,
                flags: 0,
                disposition: BarrierAckDisposition::Captured,
                error: None,
                watermark,
            })
            .unwrap(),
        );
    }

    let outcome = BarrierCoordinator::new(k)
        .wait_for_quorum(
            &prepare,
            &[NodeId(2), NodeId(3)],
            Duration::from_millis(200),
        )
        .await;

    assert!(matches!(
        outcome,
        QuorumOutcome::Reached {
            follower_watermark: CheckpointWatermark::Uninitialized,
            ..
        }
    ));
}

#[tokio::test]
async fn idle_participant_is_excluded_from_cluster_watermark_minimum() {
    let k = kv(NodeId(1));
    let prepare = certified_kv_prepare(7, &[NodeId(2), NodeId(3)], 0);
    let assignment_digest = prepare
        .assignment_fence
        .as_ref()
        .map(crate::checkpoint::CheckpointAssignmentFence::digest);
    for (node, watermark) in [
        (NodeId(2), CheckpointWatermark::Active(100)),
        (NodeId(3), CheckpointWatermark::Idle),
    ] {
        k.seed(
            node,
            ACK_KEY,
            serde_json::to_string(&BarrierAck {
                epoch: 7,
                checkpoint_id: 7,
                assignment_digest,
                flags: 0,
                disposition: BarrierAckDisposition::Captured,
                error: None,
                watermark,
            })
            .unwrap(),
        );
    }

    let outcome = BarrierCoordinator::new(k)
        .wait_for_quorum(
            &prepare,
            &[NodeId(2), NodeId(3)],
            Duration::from_millis(200),
        )
        .await;

    assert!(matches!(
        outcome,
        QuorumOutcome::Reached {
            follower_watermark: CheckpointWatermark::Active(100),
            ..
        }
    ));
}

#[tokio::test]
async fn quorum_timeout_dominates_prepared_with_replay_when_follower_silent() {
    let k = kv(NodeId(1));
    let prepare = certified_kv_prepare(
        8,
        &[NodeId(2), NodeId(3)],
        crate::checkpoint::flags::HANDOFF,
    );
    let ack_json = serde_json::to_string(&BarrierAck {
        epoch: 8,
        checkpoint_id: 8,
        assignment_digest: prepare
            .assignment_fence
            .as_ref()
            .map(crate::checkpoint::CheckpointAssignmentFence::digest),
        flags: crate::checkpoint::flags::HANDOFF,
        disposition: BarrierAckDisposition::CapturedWithReplay,
        error: None,
        watermark: CheckpointWatermark::Uninitialized,
    })
    .unwrap();
    k.seed(NodeId(2), ACK_KEY, ack_json);

    let coord = BarrierCoordinator::new(k);
    let outcome = coord
        .wait_for_quorum(
            &prepare,
            &[NodeId(2), NodeId(3)],
            Duration::from_millis(150),
        )
        .await;
    match outcome {
        QuorumOutcome::TimedOut { got, missing } => {
            assert_eq!(got, vec![NodeId(2)]);
            assert_eq!(missing, vec![NodeId(3)]);
        }
        other => panic!("expected TimedOut, got {other:?}"),
    }
}

#[tokio::test]
async fn fatal_prepare_ack_dominates_prepared_with_replay() {
    let k = kv(NodeId(1));
    let flags = crate::checkpoint::flags::HANDOFF;
    let prepare = certified_kv_prepare(9, &[NodeId(2), NodeId(3)], flags);
    let assignment_digest = prepare
        .assignment_fence
        .as_ref()
        .map(crate::checkpoint::CheckpointAssignmentFence::digest);
    let replay = BarrierAck {
        epoch: 9,
        checkpoint_id: 9,
        assignment_digest,
        flags,
        disposition: BarrierAckDisposition::CapturedWithReplay,
        error: None,
        watermark: CheckpointWatermark::Uninitialized,
    };
    let failed = BarrierAck {
        epoch: 9,
        checkpoint_id: 9,
        assignment_digest,
        flags,
        disposition: BarrierAckDisposition::Failed,
        error: Some("state snapshot failed: disk full".into()),
        watermark: CheckpointWatermark::Uninitialized,
    };
    k.seed(NodeId(2), ACK_KEY, serde_json::to_string(&replay).unwrap());
    k.seed(NodeId(3), ACK_KEY, serde_json::to_string(&failed).unwrap());

    let coord = BarrierCoordinator::new(k.clone());
    let outcome = coord
        .wait_for_quorum(&prepare, &[NodeId(2), NodeId(3)], Duration::from_secs(2))
        .await;
    match outcome {
        QuorumOutcome::Failed { failures } => {
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].0, NodeId(3));
            assert!(failures[0].1.contains("disk full"));
        }
        other => panic!("expected Failed, got {other:?}"),
    }

    k.seed(
        NodeId(3),
        ACK_KEY,
        serde_json::to_string(&BarrierAck {
            disposition: BarrierAckDisposition::Captured,
            ..replay
        })
        .unwrap(),
    );
    assert_eq!(
        coord
            .wait_for_quorum(&prepare, &[NodeId(2), NodeId(3)], Duration::from_secs(2))
            .await,
        QuorumOutcome::Reached {
            acks: vec![NodeId(2), NodeId(3)],
            follower_watermark: CheckpointWatermark::Uninitialized,
            handoff_replay_pending: true,
        }
    );
}

#[tokio::test]
async fn wrong_epoch_ack_is_ignored() {
    let k = kv(NodeId(1));
    let prepare = certified_kv_prepare(10, &[NodeId(2)], 0);
    let stale = serde_json::to_string(&BarrierAck {
        epoch: 9,
        checkpoint_id: 9,
        assignment_digest: prepare
            .assignment_fence
            .as_ref()
            .map(crate::checkpoint::CheckpointAssignmentFence::digest),
        flags: 0,
        disposition: BarrierAckDisposition::Captured,
        error: None,
        watermark: CheckpointWatermark::Uninitialized,
    })
    .unwrap();
    k.seed(NodeId(2), ACK_KEY, stale);

    let coord = BarrierCoordinator::new(k);
    let outcome = coord
        .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_millis(100))
        .await;
    assert!(
        matches!(outcome, QuorumOutcome::TimedOut { .. }),
        "stale-epoch ack must not satisfy quorum"
    );
}

#[tokio::test]
async fn wrong_attempt_or_assignment_ack_is_ignored() {
    let expected_fence = test_fence(4, &[1, 2], &[(1, 11), (2, 22)]);
    let prepare = BarrierAnnouncement {
        epoch: 10,
        checkpoint_id: 10,
        assignment_fence: Some(expected_fence.clone()),
        leader_proof: None,
        phase: Phase::Prepare,
        flags: 0,
    };
    let wrong_fence = test_fence(4, &[1, 2], &[(1, 111), (2, 22)]);

    for ack in [
        BarrierAck {
            epoch: 9,
            checkpoint_id: 9,
            assignment_digest: Some(expected_fence.digest()),
            flags: 0,
            disposition: BarrierAckDisposition::Captured,
            error: None,
            watermark: CheckpointWatermark::Uninitialized,
        },
        BarrierAck {
            epoch: 10,
            checkpoint_id: 10,
            assignment_digest: Some(wrong_fence.digest()),
            flags: 0,
            disposition: BarrierAckDisposition::Captured,
            error: None,
            watermark: CheckpointWatermark::Uninitialized,
        },
    ] {
        let k = kv(NodeId(1));
        k.seed(NodeId(2), ACK_KEY, serde_json::to_string(&ack).unwrap());
        let coord = BarrierCoordinator::new(k);
        let outcome = coord
            .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_millis(50))
            .await;
        assert!(
            matches!(outcome, QuorumOutcome::TimedOut { .. }),
            "ack for a different exact attempt/certificate must not satisfy quorum"
        );
    }
}
