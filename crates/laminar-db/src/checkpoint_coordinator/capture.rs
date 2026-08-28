use super::{
    checkpoint_descriptor_sha256, checkpoint_sha256, classify_channel_progress, BTreeMap,
    ByteRange, Bytes, CapturedStateFrame, CheckpointAttempt, CheckpointCoordinator,
    CheckpointManifest, CheckpointRequest, DbError, Digest, ManagedVnodeOperator,
    ManagedVnodePlacement, NonZeroU32, PackedCheckpoint, PreparedSinkDescriptor,
    ReferencedStateChunk, Sha256, StateChunkId, StateFrame, StateFrameKey,
    PREPARED_SINK_DESCRIPTOR_VERSION,
};

struct PackedStateFrames {
    node_data: Vec<Bytes>,
    object_length: u64,
    frames: Vec<StateFrame>,
    current_frame_chunks: Vec<(usize, usize)>,
    referenced: BTreeMap<StateChunkId, (u64, String, u32)>,
}

struct PackedArtifact {
    node_data: Vec<Bytes>,
    object_length: u64,
    frames: Vec<StateFrame>,
    current_frame_chunks: Vec<(usize, usize)>,
    referenced: BTreeMap<StateChunkId, (u64, String, u32)>,
    prepared_sinks: Vec<PreparedSinkDescriptor>,
    prepared_sink_chunks: Vec<(usize, usize)>,
}

struct DigestedArtifact {
    node_data: Vec<Bytes>,
    object_length: u64,
    object_sha256: String,
    frames: Vec<StateFrame>,
    referenced: BTreeMap<StateChunkId, (u64, String, u32)>,
    prepared_sinks: Vec<PreparedSinkDescriptor>,
}

impl CheckpointCoordinator {
    pub(super) fn validate_request(&self, request: &CheckpointRequest) -> Result<(), DbError> {
        self.validate_request_flags(request)?;
        self.validate_assignment_capture(request)?;
        self.validate_source_assignment_versions(request)
    }

    fn validate_request_flags(&self, request: &CheckpointRequest) -> Result<(), DbError> {
        let unsupported_flags = request.flags & !laminar_core::checkpoint::flags::HANDOFF;
        if unsupported_flags != 0 {
            return Err(DbError::Checkpoint(format!(
                "checkpoint request carries unsupported flags {unsupported_flags:#x}"
            )));
        }
        if request.handoff_replay_pending
            && request.flags & laminar_core::checkpoint::flags::HANDOFF == 0
        {
            return Err(DbError::Checkpoint(
                "aligned replay may only qualify an assignment handoff checkpoint".into(),
            ));
        }
        if request.handoff_replay_pending && request.reassignment_portable {
            return Err(DbError::Checkpoint(
                "a checkpoint with aligned replay pending cannot claim vnode reassignment portability"
                    .into(),
            ));
        }
        #[cfg(feature = "cluster")]
        if request.flags & laminar_core::checkpoint::flags::HANDOFF != 0
            && self.cluster_controller.is_none()
        {
            return Err(DbError::Checkpoint(
                "assignment handoff checkpoint requires a cluster runtime".into(),
            ));
        }
        #[cfg(not(feature = "cluster"))]
        if request.flags != 0 {
            return Err(DbError::Checkpoint(
                "assignment handoff checkpoint requires cluster support".into(),
            ));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn validate_assignment_capture(&self, request: &CheckpointRequest) -> Result<(), DbError> {
        let vnode_count = u32::from(self.store.key_group_count().get());
        match (
            self.cluster_controller.as_ref(),
            request.assignment_fence.as_ref(),
        ) {
            (None, None) if !request.reassignment_portable => Ok(()),
            (None, None) => Err(DbError::Checkpoint(
                "local checkpoint cannot claim vnode reassignment portability".into(),
            )),
            (None, Some(_)) => Err(DbError::Checkpoint(
                "local checkpoint received an assignment fence".into(),
            )),
            (Some(_), None) => Err(DbError::Checkpoint(
                "cluster checkpoint is missing its assignment fence".into(),
            )),
            (Some(controller), Some(fence)) => {
                if !request.reassignment_portable {
                    return Err(DbError::Checkpoint(
                        "cluster checkpoint requires a vnode-reassignment-portable capture".into(),
                    ));
                }
                if !fence.is_canonical()
                    || !fence.contains(self.store.participant_id())
                    || fence.vnode_count != vnode_count
                    || fence.assignment_version != self.assignment_version
                    || controller
                        .checkpoint_assignment_fence(fence.assignment_version)
                        .as_ref()
                        != Some(fence)
                {
                    return Err(DbError::Checkpoint(
                        "checkpoint assignment fence is stale or incompatible".into(),
                    ));
                }
                Ok(())
            }
        }
    }

    #[cfg(not(feature = "cluster"))]
    fn validate_assignment_capture(&self, request: &CheckpointRequest) -> Result<(), DbError> {
        if request.assignment_fence.is_some() {
            return Err(DbError::Checkpoint(
                "local checkpoint received an assignment fence".into(),
            ));
        }
        if request.reassignment_portable {
            return Err(DbError::Checkpoint(
                "local checkpoint cannot claim vnode reassignment portability".into(),
            ));
        }
        Ok(())
    }

    fn validate_source_assignment_versions(
        &self,
        request: &CheckpointRequest,
    ) -> Result<(), DbError> {
        for source in &self.assignment_scoped_sources {
            let checkpoint = request.source_offset_overrides.get(source).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "assignment-scoped source '{source}' has no captured offset"
                ))
            })?;
            let expected = request
                .assignment_fence
                .as_ref()
                .map_or(self.assignment_version, |fence| fence.assignment_version);
            if checkpoint
                .source_assignment_version
                .map(std::num::NonZeroU64::get)
                != Some(expected)
            {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' offset is not bound to assignment {expected}"
                )));
            }
        }
        if let Some((source, version)) = request
            .source_offset_overrides
            .iter()
            .filter(|(source, _)| !self.assignment_scoped_sources.contains(*source))
            .find_map(|(source, checkpoint)| {
                checkpoint
                    .source_assignment_version
                    .map(|version| (source, version))
            })
        {
            return Err(DbError::Checkpoint(format!(
                "non-assignment source '{source}' carries assignment version {version}"
            )));
        }
        Ok(())
    }
    fn prior_chunk_metadata(
        prior: &CheckpointManifest,
        chunk: StateChunkId,
    ) -> Option<(u64, String)> {
        if prior.node_data.chunk == chunk {
            return Some((
                prior.node_data.object_length,
                prior.node_data.sha256.clone(),
            ));
        }
        prior
            .referenced_chunks
            .binary_search_by_key(&chunk, |reference| reference.chunk)
            .ok()
            .map(|index| &prior.referenced_chunks[index])
            .map(|reference| (reference.object_length, reference.sha256.clone()))
    }

    fn managed_vnode_is_required(
        &self,
        operators: &[ManagedVnodeOperator],
        operator_id: &str,
        vnode: u16,
    ) -> bool {
        let Ok(index) =
            operators.binary_search_by(|operator| operator.operator_id.as_str().cmp(operator_id))
        else {
            return false;
        };
        let vnode = u32::from(vnode);
        if self.owned_vnodes.binary_search(&vnode).is_err() {
            return false;
        }
        match operators[index].placement {
            ManagedVnodePlacement::GlobalSingleton => vnode == 0,
            ManagedVnodePlacement::VnodeKeyed => true,
        }
    }

    pub(super) fn complete_sparse_vnode_captures(
        &self,
        request: &mut CheckpointRequest,
    ) -> Result<(), DbError> {
        Self::validate_managed_vnode_inventory(&mut request.managed_vnode_operators)?;
        self.validate_current_vnode_captures(request)?;
        let expected_vnodes =
            self.expected_vnode_capture_count(&request.managed_vnode_operators)?;
        let current_vnodes = Self::vnode_capture_count(&request.state_frames);
        self.inherit_missing_vnode_captures(request, current_vnodes, expected_vnodes)?;
        self.validate_capture_roster(&request.state_frames)?;

        let actual_vnodes = Self::vnode_capture_count(&request.state_frames);
        if actual_vnodes != expected_vnodes {
            return Err(DbError::Checkpoint(format!(
                "managed vnode checkpoint is incomplete: captured {actual_vnodes} logical frames, expected {expected_vnodes}"
            )));
        }
        Ok(())
    }

    fn validate_managed_vnode_inventory(
        operators: &mut [ManagedVnodeOperator],
    ) -> Result<(), DbError> {
        operators.sort_unstable_by(|left, right| left.operator_id.cmp(&right.operator_id));
        if operators
            .iter()
            .any(|operator| operator.operator_id.is_empty())
            || operators
                .windows(2)
                .any(|pair| pair[0].operator_id == pair[1].operator_id)
        {
            return Err(DbError::Checkpoint(
                "managed vnode operator inventory must have non-empty, unique identifiers".into(),
            ));
        }
        Ok(())
    }

    fn validate_current_vnode_captures(&self, request: &CheckpointRequest) -> Result<(), DbError> {
        for capture in &request.state_frames {
            if let StateFrameKey::Vnode { operator_id, vnode } = &capture.key {
                if !self.managed_vnode_is_required(
                    &request.managed_vnode_operators,
                    operator_id,
                    *vnode,
                ) {
                    return Err(DbError::Checkpoint(format!(
                        "captured vnode frame {:?} is outside the current managed-state inventory or ownership roster",
                        capture.key
                    )));
                }
            }
        }
        Ok(())
    }

    fn expected_vnode_capture_count(
        &self,
        operators: &[ManagedVnodeOperator],
    ) -> Result<usize, DbError> {
        operators.iter().try_fold(0usize, |total, operator| {
            let count = match operator.placement {
                ManagedVnodePlacement::GlobalSingleton => {
                    usize::from(self.owned_vnodes.first() == Some(&0))
                }
                ManagedVnodePlacement::VnodeKeyed => self.owned_vnodes.len(),
            };
            total.checked_add(count).ok_or_else(|| {
                DbError::Checkpoint("managed vnode frame count overflowed usize".into())
            })
        })
    }

    fn vnode_capture_count(captures: &[CapturedStateFrame]) -> usize {
        captures
            .iter()
            .filter(|capture| matches!(capture.key, StateFrameKey::Vnode { .. }))
            .count()
    }

    fn inherit_missing_vnode_captures(
        &self,
        request: &mut CheckpointRequest,
        current_vnodes: usize,
        expected_vnodes: usize,
    ) -> Result<(), DbError> {
        if current_vnodes >= expected_vnodes {
            return Ok(());
        }
        let Some(prior) = self.last_committed_manifest.as_ref() else {
            return Ok(());
        };

        let current_whole_frames = request.state_frames.len() - current_vnodes;
        let merged_capacity = current_whole_frames
            .checked_add(expected_vnodes)
            .ok_or_else(|| {
                DbError::Checkpoint("managed vnode checkpoint frame count overflowed usize".into())
            })?;
        let mut merged = Vec::new();
        merged.try_reserve_exact(merged_capacity).map_err(|error| {
            DbError::Checkpoint(format!(
                "managed vnode checkpoint roster reservation failed: {error}"
            ))
        })?;

        let current = std::mem::take(&mut request.state_frames);
        let mut current = current.into_iter().peekable();
        let mut inherited = prior
            .state_frames
            .iter()
            .filter(|frame| {
                let StateFrameKey::Vnode { operator_id, vnode } = &frame.key else {
                    return false;
                };
                self.managed_vnode_is_required(
                    &request.managed_vnode_operators,
                    operator_id,
                    *vnode,
                )
            })
            .peekable();

        loop {
            match (current.peek(), inherited.peek()) {
                (Some(current_frame), Some(inherited_frame)) => {
                    match current_frame.key.cmp(&inherited_frame.key) {
                        std::cmp::Ordering::Less => {
                            merged.push(current.next().expect("peeked current frame"));
                        }
                        std::cmp::Ordering::Equal => {
                            merged.push(current.next().expect("peeked current frame"));
                            inherited.next();
                        }
                        std::cmp::Ordering::Greater => {
                            let frame = inherited.next().expect("peeked inherited frame");
                            merged.push(CapturedStateFrame {
                                key: frame.key.clone(),
                                state: None,
                            });
                        }
                    }
                }
                (Some(_), None) => {
                    merged.extend(current);
                    break;
                }
                (None, Some(_)) => {
                    merged.extend(inherited.map(|frame| CapturedStateFrame {
                        key: frame.key.clone(),
                        state: None,
                    }));
                    break;
                }
                (None, None) => break,
            }
        }
        request.state_frames = merged;
        Ok(())
    }
    fn validate_capture_roster(&self, captures: &[CapturedStateFrame]) -> Result<(), DbError> {
        if captures.windows(2).any(|pair| pair[0].key >= pair[1].key) {
            return Err(DbError::Checkpoint(
                "captured state frames must be strictly ordered and unique".into(),
            ));
        }
        if captures.iter().any(|capture| {
            matches!(capture.key, StateFrameKey::OperatorWhole { .. }) && capture.state.is_none()
        }) {
            return Err(DbError::Checkpoint(
                "whole-operator state must carry its current payload".into(),
            ));
        }
        Ok(())
    }

    fn canonicalize_checkpoint_request(
        &self,
        request: &mut CheckpointRequest,
    ) -> Result<(), DbError> {
        self.validate_request(request)?;
        request
            .state_frames
            .sort_unstable_by(|left, right| left.key.cmp(&right.key));
        self.validate_capture_roster(&request.state_frames)?;
        self.complete_sparse_vnode_captures(request)?;
        for channel in &mut request.channel_progress {
            channel.participant_id = self.store.participant_id();
        }
        request.channel_progress.sort_unstable_by(|left, right| {
            (
                left.participant_id,
                left.source_name.as_str(),
                left.input_channel.as_slice(),
            )
                .cmp(&(
                    right.participant_id,
                    right.source_name.as_str(),
                    right.input_channel.as_slice(),
                ))
        });
        if request.channel_progress.windows(2).any(|pair| {
            pair[0].participant_id == pair[1].participant_id
                && pair[0].source_name == pair[1].source_name
                && pair[0].input_channel == pair[1].input_channel
        }) {
            return Err(DbError::Checkpoint(
                "channel progress contains duplicate channel identities".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn pack_checkpoint(
        &self,
        attempt: CheckpointAttempt,
        mut request: CheckpointRequest,
        sink_payloads: BTreeMap<String, Option<Vec<u8>>>,
        deadline: tokio::time::Instant,
    ) -> Result<PackedCheckpoint, DbError> {
        if tokio::time::Instant::now() >= deadline {
            return Err(DbError::Checkpoint(
                "checkpoint packing exceeded its end-to-end deadline".into(),
            ));
        }
        self.canonicalize_checkpoint_request(&mut request)?;
        #[cfg(feature = "cluster")]
        let subscription_output = self
            .prepare_subscription_output_until(
                attempt,
                request.assignment_fence.as_ref(),
                request.subscription_output.take(),
                deadline,
            )
            .await?;
        self.validate_sink_payload_roster(&sink_payloads)?;

        let state = self.pack_state_frames(attempt, &mut request)?;
        let artifact = Self::pack_sink_descriptors(state, &sink_payloads)?;
        if artifact.object_length > self.config.max_node_data_bytes {
            return Err(DbError::Checkpoint(format!(
                "checkpoint node data is {} bytes; limit is {}",
                artifact.object_length, self.config.max_node_data_bytes
            )));
        }
        let digested = Self::digest_artifact_until(artifact, deadline).await?;
        let packed = self.assemble_checkpoint(attempt, request, digested)?;
        #[cfg(feature = "cluster")]
        let packed = {
            let mut packed = packed;
            packed.manifest.subscription_output = subscription_output;
            packed
        };
        self.validate_packed_manifest(&packed.manifest)?;
        Ok(packed)
    }

    fn validate_sink_payload_roster(
        &self,
        sink_payloads: &BTreeMap<String, Option<Vec<u8>>>,
    ) -> Result<(), DbError> {
        let expected_sinks = self.committable_sink_names()?;
        if !sink_payloads
            .keys()
            .map(String::as_str)
            .eq(expected_sinks.iter().map(String::as_str))
        {
            return Err(DbError::Checkpoint(
                "phase one did not produce exactly one descriptor per committable sink".into(),
            ));
        }
        Ok(())
    }

    fn pack_state_frames(
        &self,
        attempt: CheckpointAttempt,
        request: &mut CheckpointRequest,
    ) -> Result<PackedStateFrames, DbError> {
        let current_chunk = StateChunkId {
            participant_id: self.store.participant_id(),
            checkpoint_id: attempt.checkpoint_id,
        };
        let mut node_data = Vec::new();
        let mut object_length = 0;
        let mut frames = Vec::new();
        let mut current_frame_chunks = Vec::new();
        let mut referenced = BTreeMap::<StateChunkId, (u64, String, u32)>::new();

        for CapturedStateFrame { key, state } in std::mem::take(&mut request.state_frames) {
            if let Some(bytes) = state {
                let length = u64::try_from(bytes.len()).map_err(|_| {
                    DbError::Checkpoint(format!("state frame {key:?} length exceeds u64"))
                })?;
                if length == 0 {
                    return Err(DbError::Checkpoint(format!(
                        "state frame {key:?} has an empty payload"
                    )));
                }
                let range = ByteRange {
                    offset: object_length,
                    length,
                };
                object_length = range.end().ok_or_else(|| {
                    DbError::Checkpoint("checkpoint node-data length overflow".into())
                })?;
                node_data.push(bytes);
                let node_data_index = node_data.len() - 1;
                let frame_index = frames.len();
                frames.push(StateFrame {
                    key,
                    chunk: current_chunk,
                    range,
                    sha256: String::new(),
                });
                current_frame_chunks.push((frame_index, node_data_index));
            } else {
                let prior = self.last_committed_manifest.as_ref().ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "unchanged state frame {key:?} has no committed predecessor"
                    ))
                })?;
                let frame_index = prior
                    .state_frames
                    .binary_search_by(|frame| frame.key.cmp(&key))
                    .ok()
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "unchanged state frame {key:?} is absent from its committed predecessor"
                        ))
                    })?;
                let prior_frame = &prior.state_frames[frame_index];
                let (length, digest) = Self::prior_chunk_metadata(prior, prior_frame.chunk)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "predecessor frame {:?} references untracked object {:?}",
                            prior_frame.key, prior_frame.chunk
                        ))
                    })?;
                let entry =
                    referenced
                        .entry(prior_frame.chunk)
                        .or_insert((length, digest.clone(), 0));
                if entry.0 != length || entry.1 != digest {
                    return Err(DbError::Checkpoint(format!(
                        "conflicting metadata for referenced object {:?}",
                        prior_frame.chunk
                    )));
                }
                entry.2 = entry
                    .2
                    .checked_add(1)
                    .ok_or_else(|| DbError::Checkpoint("referenced frame count overflow".into()))?;
                frames.push(StateFrame {
                    key,
                    chunk: prior_frame.chunk,
                    range: prior_frame.range,
                    sha256: prior_frame.sha256.clone(),
                });
            }
        }

        Ok(PackedStateFrames {
            node_data,
            object_length,
            frames,
            current_frame_chunks,
            referenced,
        })
    }

    fn pack_sink_descriptors(
        state: PackedStateFrames,
        sink_payloads: &BTreeMap<String, Option<Vec<u8>>>,
    ) -> Result<PackedArtifact, DbError> {
        let PackedStateFrames {
            mut node_data,
            mut object_length,
            frames,
            current_frame_chunks,
            referenced,
        } = state;
        let mut prepared_sinks = Vec::with_capacity(sink_payloads.len());
        let mut prepared_sink_chunks = Vec::new();
        for (sink_name, payload) in sink_payloads {
            let (range, digest) = match payload {
                None => (None, checkpoint_descriptor_sha256(None)),
                Some(payload) => {
                    let length = u64::try_from(payload.len()).map_err(|_| {
                        DbError::Checkpoint(format!(
                            "sink '{sink_name}' descriptor length exceeds u64"
                        ))
                    })?;
                    let range = ByteRange {
                        offset: object_length,
                        length,
                    };
                    object_length = range.end().ok_or_else(|| {
                        DbError::Checkpoint("checkpoint node-data length overflow".into())
                    })?;
                    node_data.push(Bytes::copy_from_slice(payload));
                    prepared_sink_chunks.push((prepared_sinks.len(), node_data.len() - 1));
                    (Some(range), String::new())
                }
            };
            prepared_sinks.push(PreparedSinkDescriptor {
                sink_name: sink_name.clone(),
                format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
                payload: range,
                sha256: digest,
            });
        }

        Ok(PackedArtifact {
            node_data,
            object_length,
            frames,
            current_frame_chunks,
            referenced,
            prepared_sinks,
            prepared_sink_chunks,
        })
    }

    async fn digest_artifact_until(
        artifact: PackedArtifact,
        deadline: tokio::time::Instant,
    ) -> Result<DigestedArtifact, DbError> {
        let PackedArtifact {
            node_data,
            object_length,
            mut frames,
            current_frame_chunks,
            referenced,
            mut prepared_sinks,
            prepared_sink_chunks,
        } = artifact;
        let digest_chunks = node_data.clone();
        let digest_task = tokio::task::spawn_blocking(move || {
            let mut object_digest = Sha256::new();
            for bytes in &digest_chunks {
                object_digest.update(bytes);
            }
            let frame_digests = current_frame_chunks
                .into_iter()
                .map(|(frame, chunk)| (frame, checkpoint_sha256(&digest_chunks[chunk])))
                .collect::<Vec<_>>();
            let sink_digests = prepared_sink_chunks
                .into_iter()
                .map(|(sink, chunk)| {
                    (
                        sink,
                        checkpoint_descriptor_sha256(Some(&digest_chunks[chunk])),
                    )
                })
                .collect::<Vec<_>>();
            (
                format!("{:x}", object_digest.finalize()),
                frame_digests,
                sink_digests,
            )
        });
        let (object_sha256, frame_digests, sink_digests) =
            tokio::time::timeout_at(deadline, digest_task)
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "checkpoint digest task exceeded its end-to-end deadline".into(),
                    )
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!("checkpoint digest task failed: {error}"))
                })?;
        for (frame, digest) in frame_digests {
            frames[frame].sha256 = digest;
        }
        for (sink, digest) in sink_digests {
            prepared_sinks[sink].sha256 = digest;
        }

        Ok(DigestedArtifact {
            node_data,
            object_length,
            object_sha256,
            frames,
            referenced,
            prepared_sinks,
        })
    }

    fn assemble_checkpoint(
        &self,
        attempt: CheckpointAttempt,
        request: CheckpointRequest,
        artifact: DigestedArtifact,
    ) -> Result<PackedCheckpoint, DbError> {
        let DigestedArtifact {
            node_data,
            object_length,
            object_sha256,
            frames,
            referenced,
            prepared_sinks,
        } = artifact;
        let mut manifest = CheckpointManifest::new_with_key_group_count(
            attempt.checkpoint_id,
            attempt.epoch,
            self.store.key_group_count(),
        );
        manifest.bind_participant(self.store.participant_id());
        manifest.pipeline_identity = self.expected_pipeline_identity()?;
        self.expected_deployment_id()?
            .clone_into(&mut manifest.deployment_id);
        manifest.assignment_fence = request.assignment_fence;
        manifest.reassignment_portable = request.reassignment_portable;
        manifest.owned_vnodes = self
            .owned_vnodes
            .iter()
            .map(|vnode| {
                u16::try_from(*vnode).map_err(|_| {
                    DbError::Checkpoint(format!(
                        "owned vnode {vnode} exceeds the configured vnode ID space"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        manifest.source_offsets = request.source_offset_overrides;
        manifest.source_names = request.source_names;
        manifest.sink_names = self.sorted_sink_names()?;
        manifest.channel_progress = request.channel_progress;
        manifest.checkpoint_watermark = classify_channel_progress(&manifest.channel_progress)
            .map_err(DbError::Checkpoint)?
            .active_value();
        manifest.node_data.object_length = object_length;
        manifest.node_data.sha256 = object_sha256;
        manifest.state_frames = frames;
        manifest.prepared_sinks = prepared_sinks;
        manifest.referenced_chunks = referenced
            .into_iter()
            .map(|(chunk, (object_length, sha256, count))| {
                Ok(ReferencedStateChunk {
                    chunk,
                    object_length,
                    sha256,
                    ref_count: NonZeroU32::new(count).ok_or_else(|| {
                        DbError::Checkpoint("referenced object has zero frame references".into())
                    })?,
                })
            })
            .collect::<Result<Vec<_>, DbError>>()?;
        Ok(PackedCheckpoint {
            manifest,
            node_data,
        })
    }

    fn validate_packed_manifest(&self, manifest: &CheckpointManifest) -> Result<(), DbError> {
        let errors = manifest.validate(self.store.key_group_count());
        if !errors.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "checkpoint manifest validation: {}",
                errors
                    .into_iter()
                    .map(|error| error.to_string())
                    .collect::<Vec<_>>()
                    .join("; ")
            )));
        }
        Ok(())
    }
}
