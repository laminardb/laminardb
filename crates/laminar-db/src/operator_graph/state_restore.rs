//! Strict checkpoint restoration through certified topology mappings.

use super::{DbError, FxHashMap, OperatorCheckpoint, OperatorGraph};

impl OperatorGraph {
    /// Restore independently checksummed whole-operator and vnode frames into a newly built graph.
    /// The graph is consumed so a late operator failure drops the partial image.
    pub(crate) fn restore_state_frames(
        mut self,
        whole: &[(String, bytes::Bytes)],
        vnodes: &[(String, u32, bytes::Bytes)],
        vnode_count: u32,
    ) -> Result<(Self, usize), DbError> {
        #[cfg(feature = "cluster")]
        let owned_vnodes = self.owned_vnodes_for_managed_state()?;
        #[cfg(not(feature = "cluster"))]
        let owned_vnodes = self.local_owned_vnodes_for_managed_state();
        self.restore_state_frames_inner(
            whole,
            vnodes,
            vnode_count,
            owned_vnodes.as_deref().unwrap_or(&[]),
            &[],
        )
    }

    /// Restore an isolated graph using the frozen local roster from current migration authority.
    /// Existing fenced transport handles are decoding context; this grants no execution ownership.
    #[cfg(feature = "cluster")]
    pub(crate) fn restore_topology_state_frames(
        self,
        recovered: &crate::recovery_manager::RecoveredState,
        input: &laminar_core::cluster::control::TopologyRestoreInput,
    ) -> Result<(Self, usize), DbError> {
        use laminar_core::checkpoint::StateFrameKey;
        use laminar_core::cluster::control::topology::{
            ClusterTopologyObjectTransition, TopologyInitialization,
        };

        if self.pipeline_identity.as_ref() != Some(&input.descriptor().target_pipeline)
            || self.cluster_shuffle.as_ref().is_none_or(|scope| {
                scope.self_id.0 != input.process().participant.node_id
                    || scope.registry.versioned_snapshot().version()
                        != input.assignment().assignment_version
            })
        {
            return Err(DbError::Checkpoint(
                "topology preparation graph differs from its certified target/assignment context"
                    .into(),
            ));
        }
        recovered.validate_topology_assignment(input)?;
        let empty_at_cut = if recovered.committed.pipeline_identity
            == input.descriptor().parent_pipeline
        {
            input
                .descriptor()
                .objects
                .iter()
                .filter(|object| {
                    object.transition == ClusterTopologyObjectTransition::AddFutureOnly
                        && object.initialization == TopologyInitialization::EmptyManagedStateAtCut
                        && input
                            .root()
                            .future_only_objects
                            .binary_search(&object.name)
                            .is_ok()
                })
                .map(|object| object.name.as_str())
                .collect::<Vec<_>>()
        } else if recovered.committed.pipeline_identity == input.descriptor().target_pipeline {
            Vec::new()
        } else {
            return Err(DbError::Checkpoint(
                "topology restore has a foreign checkpoint identity".into(),
            ));
        };
        let frames = select_topology_state_frames(recovered, input)?;
        let mut whole = Vec::new();
        let mut vnodes = Vec::new();
        for frame in &frames {
            let operator_id = match &frame.key {
                StateFrameKey::OperatorWhole { operator_id }
                | StateFrameKey::Vnode { operator_id, .. } => operator_id,
            };
            let name = operator_id
                .strip_prefix("graph:")
                .ok_or_else(|| {
                    DbError::Checkpoint("migration restore has no non-graph state mapping".into())
                })?
                .to_owned();
            match &frame.key {
                StateFrameKey::OperatorWhole { .. } => whole.push((name, frame.payload.clone())),
                StateFrameKey::Vnode { vnode, .. } => {
                    vnodes.push((name, u32::from(*vnode), frame.payload.clone()));
                }
            }
        }
        for node in self
            .nodes
            .iter()
            .filter(|node| !node.removed && node.capability.managed_state.is_some())
        {
            // Zero-vnode processes have no participant state manifest at the selected cut.
            if !input.owned_vnodes().is_empty()
                && !empty_at_cut.contains(&node.name.as_ref())
                && !whole.iter().any(|(name, _)| name == &*node.name)
            {
                return Err(DbError::Checkpoint(format!(
                    "managed operator '{}' has no committed channel/frontier state",
                    node.name
                )));
            }
        }
        if recovered.reassigned {
            self.validate_restore_frame_roster(
                &whole,
                &vnodes,
                input.owned_vnodes(),
                &empty_at_cut,
            )?;
            return self.restore_reassigned_vnode_state_inner(
                recovered
                    .committed
                    .assignment_fence
                    .as_ref()
                    .ok_or(laminar_core::cluster::control::TopologyError::Fenced)?,
                &recovered.predecessor_owners,
                input.assignment(),
                &frames.into_iter().cloned().collect::<Vec<_>>(),
                &empty_at_cut,
            );
        }
        self.restore_state_frames_inner(
            &whole,
            &vnodes,
            input.plan().assignment.vnode_count,
            input.owned_vnodes(),
            &empty_at_cut,
        )
    }

    fn validate_restore_frame_roster(
        &self,
        whole: &[(String, bytes::Bytes)],
        vnodes: &[(String, u32, bytes::Bytes)],
        owned_vnodes: &[u32],
        empty_at_cut: &[&str],
    ) -> Result<(), DbError> {
        let mut whole_names = std::collections::BTreeSet::new();
        for (name, _) in whole {
            if !whole_names.insert(name.as_str()) {
                return Err(DbError::Checkpoint(format!(
                    "checkpoint repeats whole state for operator '{name}'"
                )));
            }
            if !self
                .nodes
                .iter()
                .any(|node| !node.removed && &*node.name == name)
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6029] checkpoint requires missing operator '{name}'"
                )));
            }
        }

        let mut actual_vnodes: FxHashMap<&str, Vec<u32>> = FxHashMap::default();
        for (name, vnode, _) in vnodes {
            let node = self
                .nodes
                .iter()
                .find(|node| !node.removed && &*node.name == name)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "[LDB-6029] checkpoint requires missing operator '{name}'"
                    ))
                })?;
            if node.capability.managed_state.is_none() {
                return Err(DbError::Checkpoint(format!(
                    "checkpoint supplies vnode {vnode} for unmanaged operator '{name}'"
                )));
            }
            actual_vnodes.entry(name).or_default().push(*vnode);
        }
        for vnodes in actual_vnodes.values_mut() {
            vnodes.sort_unstable();
            if vnodes.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(DbError::Checkpoint(
                    "checkpoint repeats a logical operator vnode frame".into(),
                ));
            }
        }
        for node in self.nodes.iter().filter(|node| !node.removed) {
            if empty_at_cut.contains(&node.name.as_ref()) {
                if node.capability.managed_state.is_none()
                    || actual_vnodes.contains_key(&*node.name)
                    || whole_names.contains(&*node.name)
                {
                    return Err(DbError::Checkpoint(format!(
                        "new managed operator '{}' must initialize without parent state",
                        node.name
                    )));
                }
                continue;
            }
            if node.capability.managed_state.is_none() {
                continue;
            }
            let required = Self::required_vnodes_for_capability(node.capability, owned_vnodes)?;
            let actual = actual_vnodes
                .get(&*node.name)
                .map_or(&[][..], Vec::as_slice);
            if actual != required {
                return Err(DbError::Checkpoint(format!(
                    "managed operator '{}' restore has vnode roster {actual:?}; expected {required:?}",
                    node.name
                )));
            }
        }

        Ok(())
    }

    fn restore_state_frames_inner(
        mut self,
        whole: &[(String, bytes::Bytes)],
        vnodes: &[(String, u32, bytes::Bytes)],
        vnode_count: u32,
        owned_vnodes: &[u32],
        empty_at_cut: &[&str],
    ) -> Result<(Self, usize), DbError> {
        if !self.whole_restore_open {
            return Err(DbError::Checkpoint(
                "[LDB-6029] operator graph restore is only valid before the first execution cycle"
                    .into(),
            ));
        }
        if vnode_count != u32::from(self.key_group_count) {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6043] checkpoint vnode domain {vnode_count} does not match graph domain {}",
                u32::from(self.key_group_count)
            )));
        }

        self.validate_restore_frame_roster(whole, vnodes, owned_vnodes, empty_at_cut)?;

        let mut restored = 0;
        for (name, bytes) in whole {
            let node_id = self
                .nodes
                .iter()
                .position(|node| !node.removed && &*node.name == name)
                .expect("whole-frame names were validated");
            let node = &mut self.nodes[node_id];
            node.operator
                .restore(OperatorCheckpoint {
                    data: bytes.to_vec(),
                })
                .map_err(|error| {
                    if error.requires_pipeline_halt() {
                        error
                    } else {
                        DbError::Checkpoint(format!(
                            "[LDB-6029] operator '{}' restore failed: {error}",
                            node.name
                        ))
                    }
                })?;
            restored += 1;
        }
        for (name, vnode, bytes) in vnodes {
            let node_id = self
                .nodes
                .iter()
                .position(|node| !node.removed && &*node.name == name)
                .expect("vnode-frame names were validated");
            let node = &mut self.nodes[node_id];
            node.operator
                .restore_vnode(*vnode, vnode_count, bytes)
                .map_err(|error| {
                    if error.requires_pipeline_halt() {
                        error
                    } else {
                        DbError::Checkpoint(format!(
                            "[LDB-6029] operator '{}' vnode {vnode} restore failed: {error}",
                            node.name
                        ))
                    }
                })?;
            restored += 1;
        }
        #[cfg(feature = "cluster")]
        for (node_id, node) in self.nodes.iter_mut().enumerate() {
            if !node.removed {
                if let Some(frontier) = node.operator.restored_output_frontier() {
                    self.output_watermarks[node_id] = frontier.watermark_or_min();
                    self.output_idle[node_id] = frontier.idle;
                }
            }
        }
        self.validate_managed_state_budget("whole-graph restore")?;
        self.whole_restore_open = false;
        Ok((self, restored))
    }
}

#[cfg(feature = "cluster")]
fn select_topology_state_frames<'a>(
    recovered: &'a crate::recovery_manager::RecoveredState,
    input: &laminar_core::cluster::control::TopologyRestoreInput,
) -> Result<Vec<&'a crate::recovery_manager::RecoveredStateFrame>, DbError> {
    use laminar_core::checkpoint::StateFrameKey;
    use laminar_core::cluster::control::topology::ClusterTopologyObjectTransition;
    use laminar_core::cluster::control::CatalogObjectKind;

    let parent_cut = recovered.committed.pipeline_identity == input.descriptor().parent_pipeline;
    let mut frames = Vec::new();
    for frame in &recovered.state_frames {
        if frame.participant_id != input.process().participant.node_id {
            return Err(laminar_core::cluster::control::TopologyError::Fenced.into());
        }
        let operator_id = match &frame.key {
            StateFrameKey::OperatorWhole { operator_id }
            | StateFrameKey::Vnode { operator_id, .. } => operator_id,
        };
        let object = input
            .descriptor()
            .objects
            .iter()
            .find(|object| {
                object.kind == CatalogObjectKind::Stream
                    && operator_id.strip_prefix("graph:") == Some(object.name.as_str())
                    && if parent_cut {
                        object.transition != ClusterTopologyObjectTransition::AddFutureOnly
                    } else {
                        object.transition != ClusterTopologyObjectTransition::Remove
                    }
            })
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "topology checkpoint has no certified state mapping for '{operator_id}'"
                ))
            })?;
        if object.transition == ClusterTopologyObjectTransition::Remove {
            // The root and recovery reader already verified the complete historical cut and
            // these bytes. Only a certified retirement permits omitting a parent frame.
            continue;
        }
        frames.push(frame);
    }
    Ok(frames)
}
