use super::{
    add_retained_output_bytes, merged_subscription_manifests,
    predecessor_subscription_certificates, retained_output_bytes, retention_caps,
    retention_caps_fit, BTreeMap, CheckpointAttempt, ClusterSubscriptionError,
    CommittedCheckpointIndex, CommittedCheckpointRef, DbError, RetainedSubscriptionSegments,
    MAX_RETAINED_SUBSCRIPTION_INTERVALS, MAX_RETAINED_SUBSCRIPTION_SEGMENTS,
};

pub(super) async fn retained_subscription_segments(
    store: &dyn laminar_core::checkpoint::CheckpointStore,
    decisions: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    authority: &laminar_core::cluster::control::LeaderLeaseStore,
    latest: &CommittedCheckpointIndex,
    horizon: &CommittedCheckpointRef,
) -> Result<RetainedSubscriptionSegments, DbError> {
    let mut current = latest.clone();
    let mut retained = RetainedSubscriptionSegments {
        keys: std::collections::BTreeSet::new(),
        bytes: 0,
    };
    for _ in 0..MAX_RETAINED_SUBSCRIPTION_INTERVALS {
        let manifests =
            crate::checkpoint_coordinator::retention::load_index_manifests(store, &current).await?;
        for (object_key, encoded_length) in manifests
            .iter()
            .filter_map(|manifest| manifest.subscription_output.as_ref())
            .flat_map(|output| &output.streams)
            .flat_map(|stream| &stream.segments)
            .map(|segment| (&segment.object_key, segment.encoded_length))
        {
            if !retained.keys.insert(object_key.clone()) {
                return Err(ClusterSubscriptionError::ManifestCorrupt {
                    reason: "one segment object is referenced by multiple retained checkpoints"
                        .into(),
                }
                .into());
            }
            retained.bytes = retained.bytes.checked_add(encoded_length).ok_or_else(|| {
                DbError::Checkpoint("retained subscription byte count overflow".into())
            })?;
            if retained.keys.len() > MAX_RETAINED_SUBSCRIPTION_SEGMENTS {
                return Err(DbError::Checkpoint(format!(
                    "retained subscription segment roster exceeds {MAX_RETAINED_SUBSCRIPTION_SEGMENTS} objects"
                )));
            }
        }
        if current.epoch == horizon.epoch && current.checkpoint_id == horizon.checkpoint_id {
            if current
                .encode_and_reference()
                .map_err(DbError::Checkpoint)?
                .1
                != *horizon
            {
                return Err(DbError::Checkpoint(
                    "subscription cleanup horizon reference changed".into(),
                ));
            }
            return Ok(retained);
        }
        let predecessor = current.predecessor.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "subscription retention horizon is not in the committed chain".into(),
            )
        })?;
        let loaded = decisions
            .load_committed_checkpoint(predecessor)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!("load retained subscription checkpoint: {error}"))
            })?;
        Box::pin(authority.validate_cluster_checkpoint_predecessor(&current, &loaded))
            .await
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        current = loaded;
    }
    Err(DbError::Checkpoint(format!(
        "subscription retention chain exceeds {MAX_RETAINED_SUBSCRIPTION_INTERVALS} checkpoints"
    )))
}

pub(crate) async fn cluster_subscription_retention_horizon(
    store: &dyn laminar_core::checkpoint::CheckpointStore,
    decisions: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    authority: &laminar_core::cluster::control::LeaderLeaseStore,
    latest: &CommittedCheckpointIndex,
    artifact_floor_epoch: u64,
) -> Result<CommittedCheckpointIndex, DbError> {
    let latest_manifests =
        crate::checkpoint_coordinator::retention::load_index_manifests(store, latest).await?;
    let latest_outputs = merged_subscription_manifests(
        CheckpointAttempt::new(latest.epoch, latest.checkpoint_id),
        latest.assignment_fence.as_ref(),
        latest_manifests.iter(),
    )?;
    let mut certificates = latest_outputs
        .iter()
        .map(|output| {
            (
                output.manifest.stream_generation,
                output.manifest.distribution_certificate.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let caps = retention_caps(&certificates);
    if caps.values().all(|cap| *cap == 0) {
        return Ok(latest.clone());
    }

    let mut retained_bytes = retained_output_bytes(&latest_outputs, &certificates)?;
    let mut current = latest.clone();
    let mut horizon = latest.clone();
    for _ in 0..MAX_RETAINED_SUBSCRIPTION_INTERVALS {
        if current.epoch <= artifact_floor_epoch || !retention_caps_fit(&retained_bytes, &caps) {
            break;
        }
        let Some(predecessor_ref) = current.predecessor.as_ref() else {
            break;
        };
        if predecessor_ref.epoch < artifact_floor_epoch {
            break;
        }
        let predecessor = decisions
            .load_committed_checkpoint(predecessor_ref)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!("load subscription retention predecessor: {error}"))
            })?;
        let root =
            Box::pin(authority.validate_cluster_checkpoint_predecessor(&current, &predecessor))
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "subscription retention predecessor is invalid: {error}"
                    ))
                })?;
        if let Some(root) = root {
            certificates = predecessor_subscription_certificates(&certificates, &root)?;
        }
        let manifests =
            crate::checkpoint_coordinator::retention::load_index_manifests(store, &predecessor)
                .await?;
        let outputs = merged_subscription_manifests(
            CheckpointAttempt::new(predecessor.epoch, predecessor.checkpoint_id),
            predecessor.assignment_fence.as_ref(),
            manifests.iter(),
        )?;
        let mut candidate_bytes = retained_bytes.clone();
        add_retained_output_bytes(&mut candidate_bytes, &outputs, &certificates)?;
        if !retention_caps_fit(&candidate_bytes, &caps) {
            break;
        }
        retained_bytes = candidate_bytes;
        horizon = predecessor.clone();
        current = predecessor;
    }
    Ok(horizon)
}
