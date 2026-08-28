use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use laminar_core::checkpoint::{
    CheckpointAttempt, CheckpointManifest, CheckpointScope, CheckpointWatermark,
    CommittedCheckpointIndex, CommittedCheckpointRef, LeaderProof,
};

#[cfg(feature = "cluster")]
use super::{publish_terminal_hint_until, subscription_output, BarrierAnnouncement, Phase};
use super::{
    require_canonical_attempt, sink_epoch_admission, CheckpointCoordinator,
    CheckpointFailureDisposition, CheckpointPhase, CheckpointRequest, CheckpointResult, DbError,
    QuorumStage, SinkEpochPublication,
};

struct CertifiedQuorum {
    scope: CheckpointScope,
    leader_proof: Option<LeaderProof>,
    quorum_watermark: Option<CheckpointWatermark>,
}

enum QuorumCertification {
    Certified(CertifiedQuorum),
    #[cfg(feature = "cluster")]
    Return(DbError),
    #[cfg(feature = "cluster")]
    Settle {
        error: DbError,
        leader_proof: Option<LeaderProof>,
    },
}

struct AdmittedCheckpoint {
    request: CheckpointRequest,
    flags: u64,
    assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
    terminal_handoff: bool,
    quorum: CertifiedQuorum,
}

type AttemptAdmission = Result<AdmittedCheckpoint, CheckpointResult>;

struct PreparedCheckpoint {
    manifests: Vec<(CheckpointManifest, Bytes)>,
}

struct PublishedCheckpointIndex {
    index: CommittedCheckpointIndex,
    reference: CommittedCheckpointRef,
    #[cfg(feature = "cluster")]
    subscription_commit_stats: Option<subscription_output::SubscriptionCommitStats>,
}

struct InstalledCheckpoint {
    index: CommittedCheckpointIndex,
    predecessor_checkpoint_id: u64,
}

impl CheckpointCoordinator {
    fn validate_attempt_admission(
        &self,
        request: &CheckpointRequest,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        if self.failure_requires_recovery {
            return Err(DbError::Checkpoint(
                "a prior checkpoint has unresolved durable or sink state".into(),
            ));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(DbError::Checkpoint(
                "checkpoint deadline expired before durable work".into(),
            ));
        }
        self.validate_request(request)
    }

    fn certify_attempt_scope(
        &self,
        quorum: QuorumStage,
        assignment_fence: Option<&laminar_core::checkpoint::CheckpointAssignmentFence>,
        validation_proof: Option<LeaderProof>,
    ) -> QuorumCertification {
        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.clone() {
            let Some(fence) = assignment_fence else {
                return QuorumCertification::Return(DbError::Checkpoint(
                    "cluster checkpoint has no assignment fence".into(),
                ));
            };
            let (proof, participants, cluster_watermark) = match quorum {
                QuorumStage::RunInline => {
                    return QuorumCertification::Settle {
                        error: DbError::Checkpoint(
                            "cluster checkpoint reached durable execution without a precomputed \
                             certified quorum"
                                .into(),
                        ),
                        leader_proof: validation_proof,
                    };
                }
                QuorumStage::Captured {
                    cluster_watermark,
                    participants,
                    leader_proof,
                } => (leader_proof, participants, cluster_watermark),
            };
            if let Err(error) =
                self.validate_captured_quorum(&controller, fence, participants, &proof)
            {
                return QuorumCertification::Settle {
                    error,
                    leader_proof: Some(proof),
                };
            }
            return QuorumCertification::Certified(CertifiedQuorum {
                scope: CheckpointScope::Cluster,
                leader_proof: Some(proof),
                quorum_watermark: Some(cluster_watermark),
            });
        }

        let _ = (quorum, assignment_fence, validation_proof);
        QuorumCertification::Certified(CertifiedQuorum {
            scope: CheckpointScope::Local,
            leader_proof: None,
            quorum_watermark: None,
        })
    }

    async fn admit_checkpoint_attempt_until(
        &mut self,
        request: CheckpointRequest,
        attempt: CheckpointAttempt,
        quorum: QuorumStage,
        started: Instant,
        deadline: tokio::time::Instant,
        sink_epoch_publication: SinkEpochPublication,
    ) -> Result<AttemptAdmission, DbError> {
        require_canonical_attempt(attempt, "checkpoint admission")?;
        let flags = request.flags;
        let failure_fence = request.assignment_fence.clone();
        let terminal_handoff = sink_epoch_admission::is_terminal_handoff(
            request.flags,
            request.handoff_replay_pending,
        );
        #[cfg(feature = "cluster")]
        let validation_proof = match &quorum {
            QuorumStage::Captured { leader_proof, .. } => Some(leader_proof.clone()),
            QuorumStage::RunInline => self
                .cluster_controller
                .as_ref()
                .and_then(|controller| controller.capture_leader_proof()),
        };
        #[cfg(not(feature = "cluster"))]
        let validation_proof = None;

        if let Err(error) = self.validate_attempt_admission(&request, deadline) {
            let result = self
                .fail_before_commit(
                    attempt,
                    started,
                    error,
                    flags,
                    failure_fence,
                    validation_proof,
                    deadline,
                    sink_epoch_publication,
                )
                .await;
            return Ok(Err(result));
        }

        let assignment_fence = request.assignment_fence.clone();
        #[cfg(feature = "cluster")]
        let quorum =
            match self.certify_attempt_scope(quorum, assignment_fence.as_ref(), validation_proof) {
                QuorumCertification::Certified(quorum) => quorum,
                #[cfg(feature = "cluster")]
                QuorumCertification::Return(error) => return Err(error),
                #[cfg(feature = "cluster")]
                QuorumCertification::Settle {
                    error,
                    leader_proof,
                } => {
                    let result = self
                        .fail_before_commit(
                            attempt,
                            started,
                            error,
                            flags,
                            assignment_fence,
                            leader_proof,
                            deadline,
                            sink_epoch_publication,
                        )
                        .await;
                    return Ok(Err(result));
                }
            };
        #[cfg(not(feature = "cluster"))]
        let QuorumCertification::Certified(quorum) =
            self.certify_attempt_scope(quorum, assignment_fence.as_ref(), validation_proof);
        Ok(Ok(AdmittedCheckpoint {
            request,
            flags,
            assignment_fence,
            terminal_handoff,
            quorum,
        }))
    }

    async fn prepare_checkpoint_until(
        &mut self,
        attempt: CheckpointAttempt,
        request: CheckpointRequest,
        assignment_fence: Option<&laminar_core::checkpoint::CheckpointAssignmentFence>,
        deadline: tokio::time::Instant,
    ) -> Result<PreparedCheckpoint, DbError> {
        self.phase = CheckpointPhase::PreCommitting;
        let descriptors = self.pre_commit_sinks_until(attempt.epoch, deadline).await?;
        let packed = self
            .pack_checkpoint(attempt, request, descriptors, deadline)
            .await?;
        let local_manifest_bytes = self.persist_checkpoint_until(&packed, deadline).await?;
        let manifests = self
            .await_prepared_participant_manifests(
                attempt,
                assignment_fence,
                (packed.manifest.clone(), local_manifest_bytes),
                deadline,
            )
            .await?;
        Ok(PreparedCheckpoint { manifests })
    }

    async fn publish_committed_index_until(
        &mut self,
        attempt: CheckpointAttempt,
        scope: CheckpointScope,
        assignment_fence: Option<&laminar_core::checkpoint::CheckpointAssignmentFence>,
        quorum_watermark: Option<CheckpointWatermark>,
        prepared: &PreparedCheckpoint,
        deadline: tokio::time::Instant,
    ) -> Result<PublishedCheckpointIndex, DbError> {
        let predecessor = self
            .authoritative_committed_predecessor_until(scope, deadline)
            .await?;
        let predecessor_source_watermarks = self
            .predecessor_source_watermarks_until(predecessor.as_ref(), deadline)
            .await?;
        let index = self
            .build_validated_committed_index_until(
                attempt,
                scope,
                assignment_fence.cloned(),
                predecessor.clone(),
                &predecessor_source_watermarks,
                &prepared.manifests,
                quorum_watermark,
                deadline,
            )
            .await?;
        #[cfg(feature = "cluster")]
        let subscription_commit_stats = (scope == CheckpointScope::Cluster)
            .then(|| subscription_output::subscription_commit_stats(&prepared.manifests));
        #[cfg(all(debug_assertions, feature = "cluster"))]
        super::checkpoint_kill_gate(
            "leader",
            attempt,
            predecessor
                .as_ref()
                .map(|reference| (reference.checkpoint_id, reference.epoch)),
        )
        .await;
        let reference = self.create_committed_index_until(&index, deadline).await?;
        Ok(PublishedCheckpointIndex {
            index,
            reference,
            #[cfg(feature = "cluster")]
            subscription_commit_stats,
        })
    }

    async fn record_commit_outcome_until(
        &mut self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<&LeaderProof>,
        published: &PublishedCheckpointIndex,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        self.phase = CheckpointPhase::Deciding;
        #[cfg(feature = "cluster")]
        let commit_visibility_started = Instant::now();
        self.record_outcome_until(
            attempt,
            laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
            Some(published.reference.clone()),
            assignment_fence.cloned(),
            leader_proof.cloned(),
            deadline,
        )
        .await?;
        #[cfg(feature = "cluster")]
        self.record_subscription_commit(
            published.subscription_commit_stats,
            commit_visibility_started.elapsed(),
            attempt,
        );
        Ok(())
    }

    fn install_committed_checkpoint(
        &mut self,
        attempt: CheckpointAttempt,
        prepared: &PreparedCheckpoint,
        published: PublishedCheckpointIndex,
    ) -> Result<InstalledCheckpoint, DbError> {
        let PublishedCheckpointIndex {
            index,
            reference,
            #[cfg(feature = "cluster")]
                subscription_commit_stats: _,
        } = published;
        let predecessor_checkpoint_id = index
            .predecessor
            .as_ref()
            .map_or(0, |reference| reference.checkpoint_id);
        self.last_committed_ref = Some(reference.clone());
        self.last_committed_source_watermarks = Some((reference, index.source_watermarks.clone()));
        self.last_committed_manifest = prepared
            .manifests
            .iter()
            .find(|(manifest, _)| manifest.participant_id == self.store.participant_id())
            .map(|(manifest, _)| Arc::new(manifest.clone()));
        self.prepared.remove(&attempt);
        self.allocator
            .advance_epoch_to(super::checked_successor_epoch(
                attempt.epoch,
                "closing a committed checkpoint",
            )?);
        Ok(InstalledCheckpoint {
            index,
            predecessor_checkpoint_id,
        })
    }

    #[cfg(feature = "cluster")]
    async fn publish_committed_checkpoint_until(
        &self,
        attempt: CheckpointAttempt,
        installed: &InstalledCheckpoint,
        assignment_fence: Option<&laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<&LeaderProof>,
        flags: u64,
    ) -> Result<(), DbError> {
        if let Some(controller) = self.cluster_controller.as_ref() {
            controller
                .publish_committed_checkpoint_progress(
                    &installed.index.channel_progress,
                    &installed.index.source_watermarks,
                )
                .map_err(DbError::Checkpoint)?;
            // The durable Commit is already immutable. Its cluster hint is best-effort and must
            // not delay sink continuation or the terminal caller reply without bound.
            let notification_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
            publish_terminal_hint_until(
                notification_deadline,
                controller.announce_barrier(&BarrierAnnouncement {
                    epoch: attempt.epoch,
                    checkpoint_id: attempt.checkpoint_id,
                    assignment_fence: assignment_fence.cloned(),
                    leader_proof: leader_proof.cloned(),
                    phase: Phase::Commit,
                    flags,
                }),
            )
            .await;
        }
        Ok(())
    }

    async fn continue_committed_checkpoint_until(
        &mut self,
        attempt: CheckpointAttempt,
        prepared: &PreparedCheckpoint,
        installed: &InstalledCheckpoint,
        leader_proof: Option<&LeaderProof>,
        terminal_handoff: bool,
        sink_epoch_publication: SinkEpochPublication,
    ) -> Result<(), DbError> {
        let continuation_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        let fencing_token = leader_proof.map_or(1, |proof| proof.fencing_token);
        let participant_manifests = prepared
            .manifests
            .iter()
            .map(|(manifest, _)| manifest)
            .collect::<Vec<_>>();
        let continuation = self
            .commit_external_sinks_until(
                attempt,
                &participant_manifests,
                fencing_token,
                installed.predecessor_checkpoint_id,
                continuation_deadline,
            )
            .await;
        self.continue_committed_sink_epoch_until(
            continuation,
            &installed.index,
            leader_proof,
            terminal_handoff,
            continuation_deadline,
            sink_epoch_publication,
        )
        .await
    }

    fn finish_committed_checkpoint(
        &mut self,
        attempt: CheckpointAttempt,
        started: Instant,
        continuation: Result<(), DbError>,
    ) -> CheckpointResult {
        let duration = started.elapsed();
        self.phase = CheckpointPhase::Idle;
        let checkpoint_bytes = self
            .last_committed_manifest
            .as_ref()
            .map(|manifest| manifest.node_data.object_length);
        self.record_checkpoint_outcome(true, attempt, duration, checkpoint_bytes);
        let continuation_error = continuation.err().map(|error| {
            self.failure_requires_recovery = true;
            format!(
                "checkpoint {} committed, but sink continuation requires recovery: {error}",
                attempt.checkpoint_id
            )
        });
        CheckpointResult {
            success: true,
            checkpoint_id: attempt.checkpoint_id,
            epoch: attempt.epoch,
            duration,
            error: continuation_error,
            failure_disposition: None,
        }
    }

    async fn run_checkpoint_attempt(
        &mut self,
        request: CheckpointRequest,
        attempt: CheckpointAttempt,
        quorum: QuorumStage,
        started: Instant,
        deadline: tokio::time::Instant,
        sink_epoch_publication: SinkEpochPublication,
    ) -> Result<CheckpointResult, DbError> {
        let admitted = match self
            .admit_checkpoint_attempt_until(
                request,
                attempt,
                quorum,
                started,
                deadline,
                sink_epoch_publication,
            )
            .await?
        {
            Ok(admitted) => admitted,
            Err(result) => return Ok(result),
        };
        let AdmittedCheckpoint {
            request,
            flags,
            assignment_fence,
            terminal_handoff,
            mut quorum,
        } = admitted;

        let prepared = match self
            .prepare_checkpoint_until(attempt, request, assignment_fence.as_ref(), deadline)
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        flags,
                        assignment_fence,
                        quorum.leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };
        let published = match self
            .publish_committed_index_until(
                attempt,
                quorum.scope,
                assignment_fence.as_ref(),
                quorum.quorum_watermark.take(),
                &prepared,
                deadline,
            )
            .await
        {
            Ok(published) => published,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        flags,
                        assignment_fence,
                        quorum.leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };
        if let Err(error) = self
            .record_commit_outcome_until(
                attempt,
                assignment_fence.as_ref(),
                quorum.leader_proof.as_ref(),
                &published,
                deadline,
            )
            .await
        {
            return Ok(self.failed_result(
                attempt,
                started,
                format!("commit outcome is in-doubt: {error}"),
                CheckpointFailureDisposition::RequiresRecovery,
            ));
        }
        let installed = self.install_committed_checkpoint(attempt, &prepared, published)?;
        #[cfg(feature = "cluster")]
        self.publish_committed_checkpoint_until(
            attempt,
            &installed,
            assignment_fence.as_ref(),
            quorum.leader_proof.as_ref(),
            flags,
        )
        .await?;
        let continuation = self
            .continue_committed_checkpoint_until(
                attempt,
                &prepared,
                &installed,
                quorum.leader_proof.as_ref(),
                terminal_handoff,
                sink_epoch_publication,
            )
            .await;
        Ok(self.finish_committed_checkpoint(attempt, started, continuation))
    }

    pub async fn checkpoint(
        &mut self,
        request: CheckpointRequest,
    ) -> Result<CheckpointResult, DbError> {
        let started = Instant::now();
        let deadline = tokio::time::Instant::from_std(started) + self.config.checkpoint_timeout;
        #[cfg(feature = "cluster")]
        let local = self.cluster_controller.is_none();
        #[cfg(not(feature = "cluster"))]
        let local = true;
        if !local {
            return Err(DbError::Checkpoint(
                "cluster checkpoints require reserved pipeline admission and certified Prepare"
                    .into(),
            ));
        }
        let attempt = self.allocate_attempt_until(deadline).await?;
        if let Err(error) = self
            .begin_checkpoint_artifacts_until(attempt, None, None, deadline)
            .await
        {
            return Ok(self
                .fail_before_commit(
                    attempt,
                    started,
                    error,
                    request.flags,
                    request.assignment_fence.clone(),
                    None,
                    deadline,
                    SinkEpochPublication::Immediate,
                )
                .await);
        }
        self.run_checkpoint_attempt(
            request,
            attempt,
            QuorumStage::RunInline,
            started,
            deadline,
            SinkEpochPublication::Immediate,
        )
        .await
    }

    pub async fn checkpoint_with_offsets(
        &mut self,
        request: CheckpointRequest,
    ) -> Result<CheckpointResult, DbError> {
        self.checkpoint(request).await
    }

    pub(crate) async fn checkpoint_preallocated_started(
        &mut self,
        request: CheckpointRequest,
        attempt: CheckpointAttempt,
        quorum: QuorumStage,
        started: Instant,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointResult, DbError> {
        self.run_checkpoint_attempt(
            request,
            attempt,
            quorum,
            started,
            deadline,
            SinkEpochPublication::DeferredToTail,
        )
        .await
    }

    pub(crate) async fn abandon_epoch_until(
        &mut self,
        checkpoint_id: u64,
        epoch: u64,
        error: String,
        flags: u64,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        deadline: tokio::time::Instant,
        sink_epoch_publication: SinkEpochPublication,
    ) -> Result<CheckpointResult, DbError> {
        let attempt = require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "checkpoint abandonment",
        )?;
        let started = Instant::now();
        Ok(self
            .fail_before_commit(
                attempt,
                started,
                DbError::Checkpoint(error),
                flags,
                assignment_fence,
                leader_proof,
                deadline,
                sink_epoch_publication,
            )
            .await)
    }
}
