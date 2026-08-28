#[cfg(feature = "cluster")]
use super::QuorumPeer;
use super::{
    channel_progress_frontiers_by_source, checkpoint_manifest_bytes, classify_channel_progress,
    Arc, BTreeMap, Bytes, ChannelProgress, CheckpointAttempt, CheckpointCoordinator,
    CheckpointManifest, CheckpointScope, CheckpointWatermark, CommittedCheckpointIndex,
    CommittedCheckpointRef, CommittedParticipantRef, ConnectorCheckpoint, DbError, Duration,
    FuturesUnordered, LeaderProof, StreamExt, COMMITTED_CHECKPOINT_INDEX_VERSION,
};

const PARTICIPANT_MANIFEST_POLL_INITIAL: Duration = Duration::from_millis(10);
const PARTICIPANT_MANIFEST_POLL_MAX: Duration = Duration::from_millis(250);

async fn await_participant_manifest_until<F, Fut>(
    participant_id: u64,
    attempt: CheckpointAttempt,
    deadline: tokio::time::Instant,
    mut load: F,
) -> Result<CheckpointManifest, DbError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<CheckpointManifest>, DbError>>,
{
    let mut backoff = PARTICIPANT_MANIFEST_POLL_INITIAL;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(DbError::Checkpoint(format!(
                "participant {participant_id} checkpoint {} epoch {} manifest readiness timed out",
                attempt.checkpoint_id, attempt.epoch
            )));
        }
        match tokio::time::timeout_at(deadline, load()).await {
            Ok(Ok(Some(manifest))) => {
                if manifest.participant_id != participant_id
                    || manifest.checkpoint_id != attempt.checkpoint_id
                    || manifest.epoch != attempt.epoch
                {
                    return Err(DbError::Checkpoint(format!(
                        "participant {participant_id} published an invalid manifest readiness marker for checkpoint {} epoch {}",
                        attempt.checkpoint_id, attempt.epoch
                    )));
                }
                return Ok(manifest);
            }
            Ok(Ok(None)) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(DbError::Checkpoint(format!(
                    "participant {participant_id} checkpoint {} epoch {} manifest read timed out",
                    attempt.checkpoint_id, attempt.epoch
                )));
            }
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(DbError::Checkpoint(format!(
                "participant {participant_id} checkpoint {} epoch {} manifest readiness timed out",
                attempt.checkpoint_id, attempt.epoch
            )));
        }
        tokio::time::sleep_until((now + backoff).min(deadline)).await;
        backoff = backoff.saturating_mul(2).min(PARTICIPANT_MANIFEST_POLL_MAX);
    }
}

impl CheckpointCoordinator {
    pub(super) async fn authoritative_committed_predecessor_until(
        &self,
        scope: CheckpointScope,
        deadline: tokio::time::Instant,
    ) -> Result<Option<CommittedCheckpointRef>, DbError> {
        #[cfg(feature = "cluster")]
        if scope == CheckpointScope::Cluster {
            let controller = self.cluster_controller.as_ref().ok_or_else(|| {
                DbError::Checkpoint("cluster checkpoint has no cluster controller".into())
            })?;
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
            })?;
            let outcome =
                tokio::time::timeout_at(deadline, authority.highest_cluster_committed_outcome())
                    .await
                    .map_err(|_| DbError::Checkpoint("cluster predecessor read timed out".into()))?
                    .map_err(|error| {
                        DbError::Checkpoint(format!("cluster predecessor read failed: {error}"))
                    })?;
            return outcome
                .map(|outcome| {
                    outcome.committed_checkpoint.ok_or_else(|| {
                        DbError::Checkpoint(
                            "cluster predecessor Commit has no checkpoint reference".into(),
                        )
                    })
                })
                .transpose();
        }

        if scope != CheckpointScope::Local {
            return Err(DbError::Checkpoint(
                "cluster checkpointing requires the cluster feature".into(),
            ));
        }
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("local predecessor read requires a decision store".into())
        })?;
        let head = tokio::time::timeout_at(deadline, store.checkpoint_decision_head())
            .await
            .map_err(|_| DbError::Checkpoint("local predecessor read timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("local predecessor read failed: {error}"))
            })?;
        head.and_then(|head| head.latest_commit)
            .map(|outcome| {
                outcome.committed_checkpoint.ok_or_else(|| {
                    DbError::Checkpoint(
                        "local predecessor Commit has no checkpoint reference".into(),
                    )
                })
            })
            .transpose()
    }

    pub(super) async fn predecessor_source_watermarks_until(
        &self,
        predecessor: Option<&CommittedCheckpointRef>,
        deadline: tokio::time::Instant,
    ) -> Result<BTreeMap<String, i64>, DbError> {
        let Some(predecessor) = predecessor else {
            return Ok(BTreeMap::new());
        };
        if let Some((cached_reference, source_watermarks)) =
            self.last_committed_source_watermarks.as_ref()
        {
            if cached_reference == predecessor {
                return Ok(source_watermarks.clone());
            }
        }
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "committed predecessor source-watermark read requires a decision store".into(),
            )
        })?;
        let committed =
            tokio::time::timeout_at(deadline, store.load_committed_checkpoint(predecessor))
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "committed predecessor source-watermark read timed out".into(),
                    )
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "committed predecessor source-watermark read failed: {error}"
                    ))
                })?;
        committed
            .effective_source_watermarks()
            .map_err(DbError::Checkpoint)
    }

    pub(super) async fn await_prepared_participant_manifests(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&laminar_core::checkpoint::CheckpointAssignmentFence>,
        local: (CheckpointManifest, Bytes),
        deadline: tokio::time::Instant,
    ) -> Result<Vec<(CheckpointManifest, Bytes)>, DbError> {
        let participant_ids = assignment_fence.map_or_else(
            || vec![self.store.participant_id()],
            laminar_core::checkpoint::CheckpointAssignmentFence::participant_ids,
        );
        let expected_assignment_fence = assignment_fence.cloned();
        let expected_deployment_id = self.expected_deployment_id()?.to_owned();
        let expected_pipeline_identity = self.expected_pipeline_identity()?;
        if local.0.participant_id != self.store.participant_id()
            || !participant_ids.contains(&local.0.participant_id)
            || local.0.checkpoint_id != attempt.checkpoint_id
            || local.0.epoch != attempt.epoch
            || local.0.assignment_fence != expected_assignment_fence
            || local.0.deployment_id != expected_deployment_id
            || local.0.pipeline_identity != expected_pipeline_identity
        {
            return Err(DbError::Checkpoint(format!(
                "local participant published an invalid manifest readiness marker for checkpoint {} epoch {}",
                attempt.checkpoint_id, attempt.epoch
            )));
        }
        let mut loaded = BTreeMap::from([(local.0.participant_id, local)]);
        let mut reads = participant_ids
            .into_iter()
            .filter(|participant_id| *participant_id != self.store.participant_id())
            .map(|participant_id| {
                let store = Arc::clone(&self.store);
                let expected_assignment_fence = expected_assignment_fence.clone();
                let expected_deployment_id = expected_deployment_id.clone();
                let expected_pipeline_identity = expected_pipeline_identity.clone();
                async move {
                    let manifest = await_participant_manifest_until(
                        participant_id,
                        attempt,
                        deadline,
                        || {
                            let store = Arc::clone(&store);
                            async move {
                                store
                                    .load_manifest_for_participant(
                                        participant_id,
                                        attempt.checkpoint_id,
                                    )
                                    .await
                                    .map_err(DbError::from)
                            }
                        },
                    )
                    .await?;
                    if manifest.epoch != attempt.epoch
                        || manifest.assignment_fence != expected_assignment_fence
                        || manifest.deployment_id != expected_deployment_id
                        || manifest.pipeline_identity != expected_pipeline_identity
                    {
                        return Err(DbError::Checkpoint(format!(
                            "participant {participant_id} published an invalid manifest readiness marker for checkpoint {} epoch {}",
                            attempt.checkpoint_id, attempt.epoch
                        )));
                    }
                    let encoded =
                        Bytes::from(checkpoint_manifest_bytes(&manifest).map_err(|error| {
                            DbError::Checkpoint(format!(
                                "encode participant {participant_id} manifest: {error}"
                            ))
                        })?);
                    Ok::<_, DbError>((participant_id, manifest, encoded))
                }
            })
            .collect::<FuturesUnordered<_>>();
        while let Some(result) = reads.next().await {
            let (participant_id, manifest, encoded) = result?;
            loaded.insert(participant_id, (manifest, encoded));
        }
        Ok(loaded.into_values().collect())
    }

    fn merge_source_checkpoint(
        source: &str,
        destination: &mut ConnectorCheckpoint,
        incoming: &ConnectorCheckpoint,
    ) -> Result<(), DbError> {
        match (
            destination.source_assignment_version,
            incoming.source_assignment_version,
        ) {
            (None, None) => {}
            (Some(left), Some(right)) if left == right => {}
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' participant offsets disagree on assignment version"
                )));
            }
        }
        for (key, value) in &incoming.offsets {
            if destination
                .offsets
                .insert(key.clone(), value.clone())
                .is_some_and(|previous| previous != *value)
            {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' has conflicting offset '{key}'"
                )));
            }
        }
        for (key, value) in &incoming.metadata {
            if destination
                .metadata
                .insert(key.clone(), value.clone())
                .is_some_and(|previous| previous != *value)
            {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' has conflicting metadata '{key}'"
                )));
            }
        }
        match (&mut destination.input_channels, &incoming.input_channels) {
            (None, None) => {}
            (Some(destination), Some(incoming)) => {
                if incoming
                    .iter()
                    .any(|channel| destination.binary_search(channel).is_ok())
                {
                    return Err(DbError::Checkpoint(format!(
                        "source '{source}' input channel is owned by multiple participants"
                    )));
                }
                destination.extend(incoming.iter().cloned());
                destination.sort_unstable();
            }
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' participant checkpoints disagree on whether input channels are declared"
                )));
            }
        }
        Ok(())
    }

    pub(super) fn build_committed_index(
        &self,
        attempt: CheckpointAttempt,
        scope: CheckpointScope,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        predecessor: Option<CommittedCheckpointRef>,
        predecessor_source_watermarks: &BTreeMap<String, i64>,
        manifests: &[(CheckpointManifest, Bytes)],
        quorum_watermark: Option<CheckpointWatermark>,
    ) -> Result<CommittedCheckpointIndex, DbError> {
        let mut participants = Vec::with_capacity(manifests.len());
        let mut source_offsets = BTreeMap::<String, ConnectorCheckpoint>::new();
        let mut channels = BTreeMap::<(u64, String, Vec<u8>), ChannelProgress>::new();
        for (manifest, encoded) in manifests {
            participants.push(
                CommittedParticipantRef::from_manifest(manifest, encoded)
                    .map_err(DbError::Checkpoint)?,
            );
            for (source, checkpoint) in &manifest.source_offsets {
                match source_offsets.entry(source.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(checkpoint.clone());
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        Self::merge_source_checkpoint(source, entry.get_mut(), checkpoint)?;
                    }
                }
            }
            for channel in &manifest.channel_progress {
                if channels
                    .insert(
                        (
                            channel.participant_id,
                            channel.source_name.clone(),
                            channel.input_channel.clone(),
                        ),
                        channel.clone(),
                    )
                    .is_some()
                {
                    return Err(DbError::Checkpoint(format!(
                        "participant {} source '{}' input channel appears more than once",
                        channel.participant_id, channel.source_name
                    )));
                }
            }
        }
        participants.sort_unstable_by_key(|participant| participant.participant_id);
        let channel_progress = channels.into_values().collect::<Vec<_>>();
        let classification =
            classify_channel_progress(&channel_progress).map_err(DbError::Checkpoint)?;
        if let Some(quorum_watermark) = quorum_watermark {
            if quorum_watermark != classification {
                return Err(DbError::Checkpoint(format!(
                    "checkpoint quorum watermark {quorum_watermark:?} does not match merged channel progress {classification:?}"
                )));
            }
        }
        let checkpoint_watermark = classification.active_value();
        let source_names = manifests
            .first()
            .map_or_else(Vec::new, |(manifest, _)| manifest.source_names.clone());
        let mut source_watermarks = predecessor_source_watermarks
            .iter()
            .filter(|(source, _)| source_names.binary_search(source).is_ok())
            .map(|(source, watermark)| (source.clone(), *watermark))
            .collect::<BTreeMap<_, _>>();
        for (source, frontier) in
            channel_progress_frontiers_by_source(&channel_progress).map_err(DbError::Checkpoint)?
        {
            let Some(frontier) = frontier else {
                continue;
            };
            if source_watermarks
                .get(source)
                .is_some_and(|predecessor| *predecessor > frontier)
            {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' decision watermark regressed below its committed predecessor"
                )));
            }
            source_watermarks.insert(source.to_owned(), frontier);
        }
        let reassignment_portable = manifests
            .first()
            .is_some_and(|(manifest, _)| manifest.reassignment_portable);
        let index = CommittedCheckpointIndex {
            version: COMMITTED_CHECKPOINT_INDEX_VERSION,
            deployment_id: self.expected_deployment_id()?.to_owned(),
            pipeline_identity: self.expected_pipeline_identity()?,
            epoch: attempt.epoch,
            checkpoint_id: attempt.checkpoint_id,
            scope,
            vnode_count: self.store.key_group_count().get(),
            assignment_fence,
            reassignment_portable,
            predecessor,
            participants,
            source_names,
            source_offsets,
            channel_progress,
            source_watermarks,
            checkpoint_watermark,
        };
        let manifest_views = manifests
            .iter()
            .map(|(manifest, encoded)| (manifest, encoded.as_ref()))
            .collect::<Vec<_>>();
        index
            .validate_participant_manifests(&manifest_views)
            .map_err(DbError::Checkpoint)?;
        Ok(index)
    }

    // COMPAT: cluster builds await shared-store continuity validation; keep one caller shape.
    #[cfg_attr(
        not(feature = "cluster"),
        allow(unknown_lints, clippy::unused_async, clippy::unused_async_trait_impl)
    )]
    pub(super) async fn build_validated_committed_index_until(
        &self,
        attempt: CheckpointAttempt,
        scope: CheckpointScope,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        predecessor: Option<CommittedCheckpointRef>,
        predecessor_source_watermarks: &BTreeMap<String, i64>,
        manifests: &[(CheckpointManifest, Bytes)],
        quorum_watermark: Option<CheckpointWatermark>,
        deadline: tokio::time::Instant,
    ) -> Result<CommittedCheckpointIndex, DbError> {
        let index = self.build_committed_index(
            attempt,
            scope,
            assignment_fence.clone(),
            predecessor.clone(),
            predecessor_source_watermarks,
            manifests,
            quorum_watermark,
        )?;
        #[cfg(feature = "cluster")]
        let subscription_validation = self
            .validate_subscription_continuity_until(
                attempt,
                assignment_fence.as_ref(),
                predecessor.as_ref(),
                manifests,
                deadline,
            )
            .await;
        #[cfg(feature = "cluster")]
        if let Err(error) = &subscription_validation {
            self.record_cluster_subscription_error(error);
        }
        #[cfg(feature = "cluster")]
        subscription_validation?;
        #[cfg(not(feature = "cluster"))]
        let _ = deadline;
        Ok(index)
    }

    #[cfg(feature = "cluster")]
    pub(super) fn validate_captured_quorum(
        &self,
        controller: &laminar_core::cluster::control::ClusterController,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        participants: Vec<QuorumPeer>,
        proof: &LeaderProof,
    ) -> Result<(), DbError> {
        let mut expected = fence
            .participant_ids()
            .into_iter()
            .filter(|participant| *participant != self.store.participant_id())
            .collect::<Vec<_>>();
        let mut actual = participants
            .into_iter()
            .map(|participant| participant.0)
            .collect::<Vec<_>>();
        expected.sort_unstable();
        actual.sort_unstable();
        if actual != expected || !controller.proof_is_live(proof) {
            return Err(DbError::Checkpoint(
                "checkpoint quorum does not match its assignment or leader proof".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn create_committed_index_until(
        &self,
        index: &CommittedCheckpointIndex,
        deadline: tokio::time::Instant,
    ) -> Result<CommittedCheckpointRef, DbError> {
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("committed checkpoint publication requires a decision store".into())
        })?;
        if tokio::time::Instant::now() >= deadline {
            return Err(DbError::Checkpoint(
                "committed checkpoint index create timed out".into(),
            ));
        }
        tokio::time::timeout_at(deadline, store.create_committed_checkpoint(index))
            .await
            .map_err(|_| DbError::Checkpoint("committed checkpoint index create timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("committed checkpoint index create failed: {error}"))
            })
    }
}

#[cfg(test)]
mod participant_manifest_readiness_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(start_paused = true)]
    async fn missing_manifest_is_retried_until_it_becomes_ready() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let mut manifest = CheckpointManifest::new(7, 7);
        manifest.bind_participant(2);
        let loaded = await_participant_manifest_until(
            2,
            CheckpointAttempt::canonical(7),
            tokio::time::Instant::now() + Duration::from_secs(1),
            {
                let attempts = Arc::clone(&attempts);
                let manifest = manifest.clone();
                move || {
                    let attempts = Arc::clone(&attempts);
                    let manifest = manifest.clone();
                    async move {
                        if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                            Ok(None)
                        } else {
                            Ok(Some(manifest))
                        }
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(loaded.checkpoint_id, 7);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn wrong_manifest_identity_fails_without_polling() {
        for (manifest, field) in [
            (CheckpointManifest::new(7, 7), "participant"),
            (
                {
                    let mut manifest = CheckpointManifest::new(8, 7);
                    manifest.bind_participant(2);
                    manifest
                },
                "checkpoint",
            ),
            (
                {
                    let mut manifest = CheckpointManifest::new(7, 8);
                    manifest.bind_participant(2);
                    manifest
                },
                "epoch",
            ),
        ] {
            let attempts = Arc::new(AtomicUsize::new(0));
            let started = tokio::time::Instant::now();
            let error = await_participant_manifest_until(
                2,
                CheckpointAttempt::canonical(7),
                started + Duration::from_secs(1),
                {
                    let attempts = Arc::clone(&attempts);
                    move || {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        let manifest = manifest.clone();
                        async move { Ok(Some(manifest)) }
                    }
                },
            )
            .await
            .unwrap_err();

            assert!(
                error
                    .to_string()
                    .contains("invalid manifest readiness marker"),
                "wrong {field} returned {error}"
            );
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
            assert_eq!(tokio::time::Instant::now(), started);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_manifest_read_fails_without_polling() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let started = tokio::time::Instant::now();
        let error = await_participant_manifest_until(
            2,
            CheckpointAttempt::canonical(7),
            started + Duration::from_secs(1),
            {
                let attempts = Arc::clone(&attempts);
                move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async {
                        Err(DbError::Checkpoint(
                            "invalid participant manifest readiness marker".into(),
                        ))
                    }
                }
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("invalid participant manifest"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(tokio::time::Instant::now(), started);
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_missing_and_blocked_reads_share_the_exact_deadline() {
        let started = tokio::time::Instant::now();
        let deadline = started + Duration::from_millis(100);
        let missing = await_participant_manifest_until(
            2,
            CheckpointAttempt::canonical(7),
            deadline,
            || async { Ok(None) },
        );
        let blocked =
            await_participant_manifest_until(3, CheckpointAttempt::canonical(7), deadline, || {
                std::future::pending::<Result<Option<CheckpointManifest>, DbError>>()
            });

        let (missing, blocked) = tokio::join!(missing, blocked);

        assert!(missing
            .unwrap_err()
            .to_string()
            .contains("readiness timed out"));
        assert!(blocked.unwrap_err().to_string().contains("read timed out"));
        assert_eq!(tokio::time::Instant::now(), deadline);
    }
}
