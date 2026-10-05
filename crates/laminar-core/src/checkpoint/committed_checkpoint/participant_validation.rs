use super::{
    merge_manifest_progress, merge_node_subscription_manifests, BTreeMap, CheckpointManifest,
    CommittedCheckpointIndex, KeyGroupCount,
};

impl CommittedCheckpointIndex {
    /// Verify exact participant manifest bytes and complete, exclusive vnode ownership.
    ///
    /// # Errors
    /// Returns an error when the manifests do not exactly represent this committed cut.
    pub fn validate_participant_manifests(
        &self,
        manifests: &[(&CheckpointManifest, &[u8])],
    ) -> Result<(), String> {
        self.validate()?;
        if manifests.len() != self.participants.len() {
            return Err("participant manifest count differs from the committed index".into());
        }

        let key_group_count = KeyGroupCount::try_from(u32::from(self.vnode_count))
            .map_err(|_| "committed checkpoint vnode count is invalid".to_owned())?;
        let mut owners = vec![None; usize::from(self.vnode_count)];
        let mut sink_names = None;
        for ((manifest, encoded), reference) in manifests.iter().zip(&self.participants) {
            reference.verify_manifest(manifest, encoded)?;
            let errors = manifest.validate(key_group_count);
            if let Some(error) = errors.first() {
                return Err(format!(
                    "participant {} manifest is invalid: {error}",
                    reference.participant_id
                ));
            }
            if manifest.checkpoint_id != self.checkpoint_id
                || manifest.epoch != self.epoch
                || manifest.deployment_id != self.deployment_id
                || manifest.pipeline_identity != self.pipeline_identity
                || manifest.vnode_count != self.vnode_count
                || manifest.assignment_fence != self.assignment_fence
                || manifest.reassignment_portable != self.reassignment_portable
            {
                return Err(format!(
                    "participant {} manifest belongs to a different checkpoint cut",
                    reference.participant_id
                ));
            }

            if manifest.source_names != self.source_names {
                return Err(
                    "participant manifest source topology differs from the committed index".into(),
                );
            }
            match sink_names {
                Some(expected) if expected != manifest.sink_names.as_slice() => {
                    return Err(
                        "participant manifests disagree on the registered sink topology".into(),
                    );
                }
                None => sink_names = Some(manifest.sink_names.as_slice()),
                Some(_) => {}
            }
            for vnode in &manifest.owned_vnodes {
                let owner = owners
                    .get_mut(usize::from(*vnode))
                    .ok_or_else(|| format!("manifest owns out-of-range vnode {vnode}"))?;
                if owner.replace(manifest.participant_id).is_some() {
                    return Err(format!(
                        "vnode {vnode} is owned by more than one participant"
                    ));
                }
            }
        }
        let mut offsets = BTreeMap::new();
        let mut channels = BTreeMap::new();
        for (manifest, _) in manifests {
            merge_manifest_progress(manifest, &mut offsets, &mut channels)?;
        }
        if offsets != self.source_offsets {
            return Err(
                "participant source offsets do not exactly reconstruct the committed source cut"
                    .into(),
            );
        }
        if channels.into_values().collect::<Vec<_>>() != self.channel_progress {
            return Err(
                "participant channel progress does not exactly reconstruct the committed time cut"
                    .into(),
            );
        }

        if owners.iter().any(Option::is_none) {
            return Err("participant manifests do not exactly cover the vnode domain".into());
        }
        if let Some(fence) = &self.assignment_fence {
            let owner_map = owners.into_iter().flatten().collect::<Vec<_>>();
            if !fence.matches_owner_map(&owner_map) {
                return Err(
                    "committed manifest vnode owners do not match the assignment fence".into(),
                );
            }
            let subscription_manifests = manifests
                .iter()
                .filter_map(|(manifest, _)| {
                    manifest
                        .subscription_output
                        .as_ref()
                        .map(|output| (output, manifest.owned_vnodes.as_slice()))
                })
                .collect::<Vec<_>>();
            if !subscription_manifests.is_empty() {
                if subscription_manifests.len() != manifests.len() {
                    return Err(
                        "participant manifests do not agree on subscription output presence".into(),
                    );
                }
                if subscription_manifests.iter().any(|(manifest, _)| {
                    manifest.streams.iter().any(|stream| {
                        stream.distribution_certificate.pipeline_identity != self.pipeline_identity
                    })
                }) {
                    return Err(
                        "subscription output pipeline identity differs from the committed index"
                            .into(),
                    );
                }
                merge_node_subscription_manifests(
                    self.epoch,
                    self.checkpoint_id,
                    fence,
                    &subscription_manifests,
                )
                .map_err(|error| format!("committed subscription output is invalid: {error}"))?;
            }
        }
        Ok(())
    }
}
