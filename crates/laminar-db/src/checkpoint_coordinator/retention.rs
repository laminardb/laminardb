use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use laminar_core::checkpoint::{
    checkpoint_manifest_bytes, CheckpointManifest, CheckpointScope, CheckpointStore,
    CommittedCheckpointIndex, CommittedCheckpointRef, LeaderProof, StateChunkId,
};
use tracing::warn;

#[cfg(feature = "cluster")]
use super::subscription_output;
use super::{CheckpointCoordinator, MAX_RETENTION_IO_CONCURRENCY};
use crate::error::DbError;

const RETENTION_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(super) enum GcAuthority {
    Local,
    #[cfg(feature = "cluster")]
    Cluster {
        authority: Arc<laminar_core::cluster::control::LeaderLeaseStore>,
        proof: LeaderProof,
        controller: std::sync::Weak<laminar_core::cluster::control::ClusterController>,
    },
}

impl GcAuthority {
    fn can_retry(&self) -> bool {
        match self {
            Self::Local => true,
            #[cfg(feature = "cluster")]
            Self::Cluster {
                proof, controller, ..
            } => controller
                .upgrade()
                .is_some_and(|controller| controller.proof_is_live(proof)),
        }
    }
}

#[derive(Clone)]
pub(super) struct GcRequest {
    pub(super) requested: Option<CommittedCheckpointIndex>,
    pub(super) decision_store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    pub(super) authority: GcAuthority,
    #[cfg(feature = "cluster")]
    pub(super) metrics: Option<Arc<crate::engine_metrics::EngineMetrics>>,
}

pub(super) async fn load_index_manifests(
    store: &dyn CheckpointStore,
    index: &CommittedCheckpointIndex,
) -> Result<Vec<CheckpointManifest>, DbError> {
    let checkpoint_id = index.checkpoint_id;
    let reads = index
        .participants
        .clone()
        .into_iter()
        .map(|participant| async move {
            let manifest = store
                .load_manifest_verified(
                    participant.participant_id,
                    checkpoint_id,
                    participant.manifest_len,
                    &participant.manifest_sha256,
                )
                .await
                .map_err(DbError::from)?
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "checkpoint {} participant {} manifest is missing",
                        checkpoint_id, participant.participant_id
                    ))
                })?;
            let encoded = checkpoint_manifest_bytes(&manifest).map_err(|error| {
                DbError::Checkpoint(format!("encode checkpoint manifest: {error}"))
            })?;
            participant
                .verify_manifest(&manifest, &encoded)
                .map_err(DbError::Checkpoint)?;
            Ok::<_, DbError>((participant.participant_id, manifest, encoded))
        });
    let mut loaded = futures::stream::iter(reads)
        .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    loaded.sort_unstable_by_key(|(participant_id, _, _)| *participant_id);
    if let Some((participant_id, _, _)) = loaded.iter().find(|(_, manifest, _)| {
        manifest.epoch != index.epoch
            || manifest.checkpoint_id != index.checkpoint_id
            || manifest.deployment_id != index.deployment_id
            || manifest.pipeline_identity != index.pipeline_identity
            || manifest.vnode_count != index.vnode_count
            || manifest.assignment_fence != index.assignment_fence
    }) {
        return Err(DbError::Checkpoint(format!(
            "checkpoint {} participant {} manifest belongs to a different committed cut",
            index.checkpoint_id, participant_id
        )));
    }
    let views = loaded
        .iter()
        .map(|(_, manifest, bytes)| (manifest, bytes.as_slice()))
        .collect::<Vec<_>>();
    index
        .validate_participant_manifests(&views)
        .map_err(DbError::Checkpoint)?;
    Ok(loaded
        .into_iter()
        .map(|(_, manifest, _)| manifest)
        .collect())
}

pub(super) struct LiveChunkInventory {
    references: BTreeSet<StateChunkId>,
    pinned: BTreeSet<StateChunkId>,
    subscription_segments: BTreeSet<String>,
}

pub(super) fn live_chunk_inventory(manifests: &[CheckpointManifest]) -> LiveChunkInventory {
    let mut references = BTreeSet::new();
    let mut pinned = BTreeSet::new();
    let mut subscription_segments = BTreeSet::new();
    for manifest in manifests {
        pinned.insert(manifest.node_data.chunk);
        for reference in &manifest.referenced_chunks {
            references.insert(reference.chunk);
        }
        if let Some(output) = &manifest.subscription_output {
            for stream in &output.streams {
                subscription_segments.extend(
                    stream
                        .segments
                        .iter()
                        .map(|segment| segment.object_key.clone()),
                );
            }
        }
    }
    LiveChunkInventory {
        references,
        pinned,
        subscription_segments,
    }
}

pub(super) async fn delete_retired_data(
    store: &dyn CheckpointStore,
    manifests: &[CheckpointManifest],
    live: &LiveChunkInventory,
) -> Result<(), DbError> {
    let mut candidates = BTreeSet::new();
    for manifest in manifests {
        candidates.insert(manifest.node_data.chunk);
        candidates.extend(
            manifest
                .referenced_chunks
                .iter()
                .map(|reference| reference.chunk),
        );
    }
    let deletions = candidates
        .into_iter()
        .filter(|chunk| !live.pinned.contains(chunk) && !live.references.contains(chunk));
    let results = futures::stream::iter(deletions)
        .map(|chunk| async move { store.delete_node_data(chunk).await })
        .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    for result in results {
        result.map_err(DbError::from)?;
    }
    let subscription_candidates = manifests
        .iter()
        .filter_map(|manifest| manifest.subscription_output.as_ref())
        .flat_map(|output| &output.streams)
        .flat_map(|stream| &stream.segments)
        .map(|segment| segment.object_key.clone())
        .collect::<BTreeSet<_>>();
    let results = futures::stream::iter(subscription_candidates)
        .filter(|object_key| std::future::ready(!live.subscription_segments.contains(object_key)))
        .map(|object_key| async move { store.delete_subscription_segment(&object_key).await })
        .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    for result in results {
        result.map_err(DbError::from)?;
    }
    Ok(())
}

async fn delete_retired_manifests(
    store: &dyn CheckpointStore,
    checkpoint_id: u64,
    participant_ids: &[u64],
) -> Result<(), DbError> {
    let results = futures::stream::iter(participant_ids.to_vec())
        .map(|participant_id| async move {
            store
                .delete_manifest(StateChunkId {
                    participant_id,
                    checkpoint_id,
                })
                .await
        })
        .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    for result in results {
        result.map_err(DbError::from)?;
    }
    Ok(())
}

pub(super) struct ProtectedCheckpoint {
    pub(super) index: CommittedCheckpointIndex,
    pub(super) live: LiveChunkInventory,
}

pub(super) async fn load_protected_checkpoint(
    store: &dyn CheckpointStore,
    decisions: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    reference: &CommittedCheckpointRef,
) -> Result<ProtectedCheckpoint, DbError> {
    let index = decisions
        .load_committed_checkpoint(reference)
        .await
        .map_err(|error| DbError::Checkpoint(format!("load retained checkpoint index: {error}")))?;
    let manifests = load_index_manifests(store, &index).await?;
    let live = live_chunk_inventory(&manifests);
    Ok(ProtectedCheckpoint { index, live })
}

pub(super) async fn load_cleanup_target(
    store: &dyn CheckpointStore,
    decisions: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    protected: &CommittedCheckpointIndex,
    current: &CommittedCheckpointRef,
    next: Option<&CommittedCheckpointRef>,
    participant_ids: Option<&[u64]>,
) -> Result<(CommittedCheckpointIndex, Vec<CheckpointManifest>), DbError> {
    let index = decisions
        .load_committed_checkpoint(current)
        .await
        .map_err(|error| DbError::Checkpoint(format!("load retired checkpoint index: {error}")))?;
    if index.deployment_id != protected.deployment_id
        || index.pipeline_identity != protected.pipeline_identity
        || index.scope != protected.scope
        || index.vnode_count != protected.vnode_count
        || index.epoch >= protected.epoch
        || index.predecessor.as_ref() != next
    {
        return Err(DbError::Checkpoint(format!(
            "checkpoint {} retention cursor breaks committed-cut continuity",
            current.checkpoint_id
        )));
    }
    if let Some(expected) = participant_ids {
        let actual = index
            .participants
            .iter()
            .map(|participant| participant.participant_id)
            .collect::<Vec<_>>();
        if actual != expected {
            return Err(DbError::Checkpoint(format!(
                "checkpoint {} retention cursor has a different participant roster",
                current.checkpoint_id
            )));
        }
    }
    let manifests = load_index_manifests(store, &index).await?;
    Ok((index, manifests))
}

fn local_retention_update_state(
    result: laminar_core::checkpoint_decision::CheckpointRetentionUpdateResult,
) -> Result<laminar_core::checkpoint_decision::CheckpointRetentionState, DbError> {
    use laminar_core::checkpoint_decision::CheckpointRetentionUpdateResult;
    match result {
        CheckpointRetentionUpdateResult::Applied(state)
        | CheckpointRetentionUpdateResult::Unchanged(state)
        | CheckpointRetentionUpdateResult::Conflict {
            current: Some(state),
        } => Ok(state),
        CheckpointRetentionUpdateResult::Conflict { current: None } => Err(DbError::Checkpoint(
            "checkpoint retention head disappeared during a conditional update".into(),
        )),
    }
}

async fn run_local_gc_request(
    store: &dyn CheckpointStore,
    request: &GcRequest,
) -> Result<(), DbError> {
    use laminar_core::checkpoint_decision::CheckpointRetentionState;

    let requested = request.requested.as_ref().ok_or_else(|| {
        DbError::Checkpoint("local checkpoint retention requires a committed cut".into())
    })?;
    let (_, requested) = requested
        .encode_and_reference()
        .map_err(DbError::Checkpoint)?;
    let mut state = local_retention_update_state(
        request
            .decision_store
            .begin_checkpoint_retention(&requested)
            .await
            .map_err(|error| DbError::Checkpoint(format!("begin checkpoint retention: {error}")))?,
    )?;
    let mut protected = None::<(CommittedCheckpointRef, ProtectedCheckpoint)>;

    loop {
        match &state {
            CheckpointRetentionState::Idle {
                protected: retained,
            } if retained == &requested || retained.epoch > requested.epoch => return Ok(()),
            CheckpointRetentionState::Idle { .. } => {
                state = local_retention_update_state(
                    request
                        .decision_store
                        .begin_checkpoint_retention(&requested)
                        .await
                        .map_err(|error| {
                            DbError::Checkpoint(format!("begin checkpoint retention: {error}"))
                        })?,
                )?;
            }
            CheckpointRetentionState::DeleteData { cursor } => {
                if protected
                    .as_ref()
                    .is_none_or(|(reference, _)| reference != &cursor.protected)
                {
                    protected = Some((
                        cursor.protected.clone(),
                        load_protected_checkpoint(
                            store,
                            request.decision_store.as_ref(),
                            &cursor.protected,
                        )
                        .await?,
                    ));
                }
                let retained = &protected
                    .as_ref()
                    .expect("retained checkpoint was loaded")
                    .1;
                let (_, manifests) = load_cleanup_target(
                    store,
                    request.decision_store.as_ref(),
                    &retained.index,
                    &cursor.current,
                    cursor.next.as_ref(),
                    Some(&[laminar_core::state::LOCAL_NODE_ID.0]),
                )
                .await?;
                delete_retired_data(store, &manifests, &retained.live).await?;
                state = local_retention_update_state(
                    request
                        .decision_store
                        .advance_checkpoint_retention(&state)
                        .await
                        .map_err(|error| {
                            DbError::Checkpoint(format!("advance checkpoint retention: {error}"))
                        })?,
                )?;
            }
            CheckpointRetentionState::DeleteMetadata { cursor } => {
                delete_retired_manifests(
                    store,
                    cursor.current.checkpoint_id,
                    &[laminar_core::state::LOCAL_NODE_ID.0],
                )
                .await?;
                request
                    .decision_store
                    .delete_committed_checkpoint(&cursor.current)
                    .await
                    .map_err(|error| {
                        DbError::Checkpoint(format!("delete retired checkpoint index: {error}"))
                    })?;
                state = local_retention_update_state(
                    request
                        .decision_store
                        .advance_checkpoint_retention(&state)
                        .await
                        .map_err(|error| {
                            DbError::Checkpoint(format!("advance checkpoint retention: {error}"))
                        })?,
                )?;
            }
        }
    }
}

#[cfg(feature = "cluster")]
async fn begin_cluster_cleanup(
    store: Arc<dyn CheckpointStore>,
    decisions: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    authority: &laminar_core::cluster::control::LeaderLeaseStore,
    proof: &LeaderProof,
    protected: CommittedCheckpointRef,
) -> Result<Option<laminar_core::cluster::control::ClusterArtifactCleanupCursor>, DbError> {
    authority
        .begin_cluster_artifact_cleanup(proof, protected, move |outcome| {
            let store = Arc::clone(&store);
            let decisions = Arc::clone(&decisions);
            async move {
                let reference = outcome
                    .committed_checkpoint
                    .as_ref()
                    .ok_or_else(|| "retained Commit has no checkpoint index".to_owned())?;
                load_protected_checkpoint(store.as_ref(), decisions.as_ref(), reference)
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            }
        })
        .await
        .map_err(|error| DbError::Checkpoint(format!("begin cluster retention: {error}")))
}

#[cfg(feature = "cluster")]
async fn run_cluster_gc_protocol(
    store: Arc<dyn CheckpointStore>,
    request: &GcRequest,
    authority: Arc<laminar_core::cluster::control::LeaderLeaseStore>,
    proof: LeaderProof,
    requested: Option<CommittedCheckpointRef>,
) -> Result<(), DbError> {
    use laminar_core::cluster::control::ClusterArtifactCleanupPhase;
    let mut cursor = authority
        .cluster_artifact_cleanup()
        .await
        .map_err(|error| DbError::Checkpoint(format!("load cluster retention: {error}")))?;
    if cursor.is_none() {
        let Some(requested) = requested.as_ref() else {
            return Ok(());
        };
        cursor = begin_cluster_cleanup(
            Arc::clone(&store),
            Arc::clone(&request.decision_store),
            authority.as_ref(),
            &proof,
            requested.clone(),
        )
        .await?;
    }
    let mut protected = None::<(CommittedCheckpointRef, ProtectedCheckpoint)>;

    loop {
        let Some(current) = cursor.clone() else {
            return Ok(());
        };
        match current.phase {
            ClusterArtifactCleanupPhase::DeleteData => {
                if protected
                    .as_ref()
                    .is_none_or(|(reference, _)| reference != &current.protected)
                {
                    protected = Some((
                        current.protected.clone(),
                        load_protected_checkpoint(
                            store.as_ref(),
                            request.decision_store.as_ref(),
                            &current.protected,
                        )
                        .await?,
                    ));
                }
                let retained = &protected
                    .as_ref()
                    .expect("retained checkpoint was loaded")
                    .1;
                let (_, manifests) = load_cleanup_target(
                    store.as_ref(),
                    request.decision_store.as_ref(),
                    &retained.index,
                    &current.current,
                    current.next.as_ref(),
                    Some(&current.participant_ids),
                )
                .await?;
                delete_retired_data(store.as_ref(), &manifests, &retained.live).await?;
                cursor = Some(
                    authority
                        .mark_cluster_artifact_data_deleted(&proof, &current)
                        .await
                        .map_err(|error| {
                            DbError::Checkpoint(format!(
                                "advance cluster retention data phase: {error}"
                            ))
                        })?,
                );
            }
            ClusterArtifactCleanupPhase::DeleteMetadata => {
                delete_retired_manifests(
                    store.as_ref(),
                    current.current.checkpoint_id,
                    &current.participant_ids,
                )
                .await?;
                request
                    .decision_store
                    .delete_committed_checkpoint(&current.current)
                    .await
                    .map_err(|error| {
                        DbError::Checkpoint(format!("delete retired checkpoint index: {error}"))
                    })?;
                let completed = current.protected.clone();
                cursor = authority
                    .mark_cluster_artifact_metadata_deleted(&proof, &current)
                    .await
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "advance cluster retention metadata phase: {error}"
                        ))
                    })?;
                if cursor.is_none() {
                    let Some(requested) = requested.as_ref() else {
                        return Ok(());
                    };
                    if completed.epoch >= requested.epoch {
                        return Ok(());
                    }
                    cursor = begin_cluster_cleanup(
                        Arc::clone(&store),
                        Arc::clone(&request.decision_store),
                        authority.as_ref(),
                        &proof,
                        requested.clone(),
                    )
                    .await?;
                }
            }
        }
    }
}

#[cfg(feature = "cluster")]
async fn run_cluster_gc_request(
    store: Arc<dyn CheckpointStore>,
    request: &GcRequest,
    authority: Arc<laminar_core::cluster::control::LeaderLeaseStore>,
    proof: LeaderProof,
) -> Result<(), DbError> {
    let requested = subscription_output::cluster_subscription_retention_reference(
        store.as_ref(),
        request.decision_store.as_ref(),
        authority.as_ref(),
        &proof,
        request.requested.as_ref(),
    )
    .await?;
    run_cluster_gc_protocol(
        Arc::clone(&store),
        request,
        authority,
        proof,
        requested.clone(),
    )
    .await?;
    let (Some(latest), Some(horizon)) = (request.requested.as_ref(), requested.as_ref()) else {
        subscription_output::record_subscription_cleanup(request.metrics.as_deref(), None);
        return Ok(());
    };
    let grace_ms =
        i64::try_from(std::time::Duration::from_secs(60 * 60).as_millis()).unwrap_or(i64::MAX);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| DbError::Checkpoint(format!("read orphan cleanup clock: {error}")))?
        .as_millis();
    let grace_before_ms = i64::try_from(now_ms)
        .unwrap_or(i64::MAX)
        .saturating_sub(grace_ms);
    let cleanup = subscription_output::cleanup_subscription_orphans(
        store.as_ref(),
        request.decision_store.as_ref(),
        latest,
        horizon,
        grace_before_ms,
    )
    .await?;
    subscription_output::record_subscription_cleanup(request.metrics.as_deref(), Some(&cleanup));
    Ok(())
}

pub(super) async fn run_gc_request(
    store: Arc<dyn CheckpointStore>,
    request: GcRequest,
) -> Result<(), DbError> {
    match request.authority.clone() {
        GcAuthority::Local
            if request
                .requested
                .as_ref()
                .is_some_and(|index| index.scope == CheckpointScope::Local) =>
        {
            run_local_gc_request(store.as_ref(), &request).await
        }
        #[cfg(feature = "cluster")]
        GcAuthority::Cluster {
            authority, proof, ..
        } if request
            .requested
            .as_ref()
            .is_none_or(|index| index.scope == CheckpointScope::Cluster) =>
        {
            run_cluster_gc_request(store, &request, authority, proof).await
        }
        _ => Err(DbError::Checkpoint(
            "checkpoint retention authority does not match the committed scope".into(),
        )),
    }
}

pub(super) async fn run_gc_worker(
    store: Arc<dyn CheckpointStore>,
    mut requests: tokio::sync::watch::Receiver<Option<GcRequest>>,
) {
    while requests.changed().await.is_ok() {
        let Some(mut request) = requests.borrow_and_update().clone() else {
            continue;
        };
        loop {
            match run_gc_request(Arc::clone(&store), request.clone()).await {
                Ok(()) => break,
                Err(error) => {
                    warn!(%error, retry_delay = ?RETENTION_RETRY_DELAY, "checkpoint retention paused at its durable cursor");
                }
            }
            if !request.authority.can_retry() {
                break;
            }
            tokio::select! {
                changed = requests.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let Some(next) = requests.borrow_and_update().clone() else {
                        break;
                    };
                    request = next;
                }
                () = tokio::time::sleep(RETENTION_RETRY_DELAY) => {}
            }
        }
    }
}

impl CheckpointCoordinator {
    pub(super) fn schedule_retention(
        &self,
        current: CommittedCheckpointIndex,
        leader_proof: Option<&LeaderProof>,
    ) {
        let Some(decision_store) = self.decision_store.as_ref() else {
            return;
        };
        let authority = match current.scope {
            CheckpointScope::Local => GcAuthority::Local,
            CheckpointScope::Cluster => {
                #[cfg(feature = "cluster")]
                {
                    let Some(proof) = leader_proof.cloned() else {
                        warn!("cluster checkpoint retention has no live leader proof");
                        return;
                    };
                    let Some(controller) = self.cluster_controller.as_ref() else {
                        warn!("cluster checkpoint retention has no cluster controller");
                        return;
                    };
                    if controller.checkpoint_drain_transition().is_some() {
                        return;
                    }
                    let authority = match controller.checkpoint_authority() {
                        Ok(authority) => authority,
                        Err(error) => {
                            warn!(%error, "cluster checkpoint retention authority is unavailable");
                            return;
                        }
                    };
                    GcAuthority::Cluster {
                        authority,
                        proof,
                        controller: Arc::downgrade(controller),
                    }
                }
                #[cfg(not(feature = "cluster"))]
                {
                    let _ = leader_proof;
                    warn!("cluster checkpoint retention requires the cluster feature");
                    return;
                }
            }
        };
        if self
            .gc_requests
            .send(Some(GcRequest {
                requested: Some(current),
                decision_store: Arc::clone(decision_store),
                authority,
                #[cfg(feature = "cluster")]
                metrics: self.prom.clone(),
            }))
            .is_err()
        {
            warn!("checkpoint retention worker is unavailable");
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn schedule_cluster_retention_resume(
        &self,
        proof: LeaderProof,
    ) -> Result<(), DbError> {
        let decision_store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("cluster checkpoint retention has no decision store".into())
        })?;
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint("cluster checkpoint retention has no controller".into())
        })?;
        if !controller.proof_is_live(&proof) {
            return Err(DbError::Checkpoint(
                "cluster checkpoint retention leader proof is no longer live".into(),
            ));
        }
        if controller.checkpoint_drain_transition().is_some() {
            return Ok(());
        }
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("cluster checkpoint retention authority: {error}"))
        })?;
        let requested = self
            .gc_requests
            .borrow()
            .as_ref()
            .and_then(|request| request.requested.clone());
        self.gc_requests
            .send(Some(GcRequest {
                requested,
                decision_store: Arc::clone(decision_store),
                authority: GcAuthority::Cluster {
                    authority,
                    proof,
                    controller: Arc::downgrade(controller),
                },
                metrics: self.prom.clone(),
            }))
            .map_err(|_| DbError::Checkpoint("checkpoint retention worker is unavailable".into()))
    }
}
