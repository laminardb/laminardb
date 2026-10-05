use super::*;
use crate::checkpoint::{
    checkpoint_manifest_bytes, ByteRange, ChangelogMode, ChannelProgress, CheckpointManifest,
    CheckpointStore, CommittedParticipantRef, ConnectorCheckpoint, NodePartitionRange,
    NodeSubscriptionManifest, NodeSubscriptionStreamManifest, ObjectStoreCheckpointStore,
    OutputDistribution, OutputDistributionCertificate, OutputPartitionId, PartitionSequence,
    PipelineIdentity, StateFrame, StateFrameKey, StreamGeneration, SubscriptionDigest,
    SubscriptionProtocolVersion, OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
};
use crate::cluster::control::topology::{
    ClusterTopologyObjectPlan, ClusterTopologyObjectTransition, ClusterTopologyValidation,
    TopologyActivationRequirement, TopologyAdmissionPhase, TopologyAdmissionPlan,
    TopologyAdmissionStatus, TopologyError, TopologyInitialization, TopologyMigrationRoot,
    TopologyValidationScope, TopologyVersion, TOPOLOGY_PREPARATION_PROTOCOL_VERSION,
};
use crate::cluster::control::CatalogObjectKind;
use crate::state::KeyGroupCount;
use std::collections::{BTreeMap, HashMap};

#[path = "topology_commit_tests.rs"]
mod topology_commit;

#[derive(Clone, Copy)]
enum FixtureChange {
    FutureStream,
    NewSources,
    RemoveSink,
    RemovePipeline,
    ResetPipeline,
}

struct Fixture {
    lease: LeaderLease,
    assignments: AssignmentSnapshotStore,
    store: ObjectStoreCheckpointStore,
    operation: TopologyAdmissionStatus,
    descriptor: ClusterTopologyValidation,
    index: CommittedCheckpointIndex,
    manifests: Vec<CheckpointManifest>,
}

impl Fixture {
    async fn stage(
        &self,
        authority: &LeaderLeaseStore,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        authority
            .stage_topology_migration_root(
                &self.lease.proof(),
                &self.assignments,
                &topology_preparation::processes(authority),
                &self.store,
                self.operation.operation_id,
                &self.operation.plan,
            )
            .await
    }
}

async fn fixture(authority: &LeaderLeaseStore) -> Fixture {
    fixture_with_sources(authority, false).await
}

async fn fixture_with_sources(authority: &LeaderLeaseStore, add_sources: bool) -> Fixture {
    fixture_with_changes(
        authority,
        if add_sources {
            FixtureChange::NewSources
        } else {
            FixtureChange::FutureStream
        },
    )
    .await
}

async fn fixture_with_changes(authority: &LeaderLeaseStore, change: FixtureChange) -> Fixture {
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&incumbent, 0).await.unwrap()
    else {
        panic!("leader");
    };
    let mut parent = catalog("events");
    for (name, kind, ddl) in [
        (
            "totals",
            CatalogObjectKind::Stream,
            "CREATE STREAM totals AS SELECT id, SUM(value) AS total FROM events GROUP BY id",
        ),
        (
            "totals_sink",
            CatalogObjectKind::Sink,
            "CREATE SINK totals_sink FROM totals INTO kafka ('topic' = 'out')",
        ),
    ] {
        parent
            .entries
            .push(crate::cluster::control::CatalogManifestEntry {
                canonical_name: name.into(),
                kind,
                catalog_generation: 7,
                ddl: ddl.into(),
            });
    }
    authority
        .seal_catalog(&lease.proof(), &parent)
        .await
        .unwrap();
    let decisions = CheckpointDecisionStore::new(authority.store.clone());
    let deployment = decisions.load_or_create_deployment_id().await.unwrap();
    authority
        .adopt_legacy_topology(
            &lease.proof(),
            Uuid::from_u128(40).try_into().unwrap(),
            &parent.reference().unwrap(),
            &deployment,
        )
        .await
        .unwrap();
    let assignments = AssignmentSnapshotStore::new(authority.store.clone());
    let snapshot = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&[NodeId(1), NodeId(2)]),
            vec![
                crate::checkpoint::CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: incumbent.boot,
                },
                crate::checkpoint::CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: Uuid::from_u128(22),
                },
            ],
        )
        .unwrap();
    assignments.save_if_absent(&snapshot).await.unwrap();
    let fence = snapshot.assignment_fence().unwrap();
    let mut target = parent.clone();
    if matches!(
        change,
        FixtureChange::RemoveSink | FixtureChange::RemovePipeline
    ) {
        target.entries.retain(|entry| {
            matches!(change, FixtureChange::RemoveSink) && entry.canonical_name != "totals_sink"
        });
    } else if matches!(change, FixtureChange::ResetPipeline) {
        for entry in &mut target.entries {
            entry.catalog_generation += 1;
        }
    } else {
        target
            .entries
            .push(crate::cluster::control::CatalogManifestEntry {
                canonical_name: "later".into(),
                kind: CatalogObjectKind::Stream,
                catalog_generation: 1,
                ddl: "CREATE STREAM later AS SELECT * FROM totals".into(),
            });
    }
    if matches!(change, FixtureChange::NewSources) {
        for (name, kind, ddl) in [
            ("added_source", CatalogObjectKind::Source, "CREATE SOURCE added_source (id BIGINT) FROM kafka ('topic' = 'new', 'startup.mode' = 'latest')"),
            ("added_stream", CatalogObjectKind::Stream, "CREATE STREAM added_stream AS SELECT * FROM added_source"),
            ("added_sink", CatalogObjectKind::Sink, "CREATE SINK added_sink FROM added_stream INTO kafka ('topic' = 'new-output')"),
        ] {
            target.entries.push(crate::cluster::control::CatalogManifestEntry {
                canonical_name: name.into(), kind, catalog_generation: 1, ddl: ddl.into(),
            });
        }
    }
    let mut descriptor = ClusterTopologyValidation {
        validation_format_version: 3,
        scope: TopologyValidationScope::LocalCandidatePlan,
        deployment_id: deployment.clone(),
        parent_version: TopologyVersion::LEGACY_BASELINE,
        target_version: TopologyVersion::new(2).unwrap(),
        parent_manifest: parent.reference().unwrap(),
        target_manifest: target.reference().unwrap(),
        parent_pipeline: PipelineIdentity::empty(),
        target_pipeline: PipelineIdentity {
            canonical_version: 7,
            sha256: "2".repeat(64),
        },
        environment_sha256: "3".repeat(64),
        compatibility_sha256: String::new(),
        statements: if matches!(
            change,
            FixtureChange::RemovePipeline | FixtureChange::ResetPipeline
        ) {
            let mut statements = vec![
                "DROP SINK totals_sink".into(),
                "DROP STREAM totals".into(),
                "DROP SOURCE events".into(),
            ];
            statements.extend(target.entries.iter().map(|entry| entry.ddl.clone()));
            statements
        } else if matches!(change, FixtureChange::RemoveSink) {
            vec!["DROP SINK totals_sink".into()]
        } else {
            target.entries[parent.entries.len()..]
                .iter()
                .map(|entry| entry.ddl.clone())
                .collect()
        },
        objects: parent
            .entries
            .iter()
            .chain(target.entries.iter().filter(|entry| {
                !parent.entries.iter().any(|old| {
                    old.canonical_name == entry.canonical_name
                        && old.catalog_generation == entry.catalog_generation
                })
            }))
            .enumerate()
            .map(|(index, e)| ClusterTopologyObjectPlan {
                name: e.canonical_name.clone(),
                kind: e.kind,
                catalog_generation: e.catalog_generation,
                transition: if !target.entries.iter().any(|entry| {
                    entry.canonical_name == e.canonical_name
                        && entry.catalog_generation == e.catalog_generation
                }) {
                    ClusterTopologyObjectTransition::Remove
                } else if index < parent.entries.len() {
                    ClusterTopologyObjectTransition::Preserve
                } else {
                    ClusterTopologyObjectTransition::AddFutureOnly
                },
                initialization: if !target.entries.iter().any(|entry| {
                    entry.canonical_name == e.canonical_name
                        && entry.catalog_generation == e.catalog_generation
                }) {
                    TopologyInitialization::RetireAtCut
                } else if index < parent.entries.len() {
                    TopologyInitialization::PreserveExactCut
                } else if e.kind == CatalogObjectKind::Source {
                    TopologyInitialization::ResolveSourcePositionsOnce
                } else if e.canonical_name == "totals" {
                    TopologyInitialization::EmptyManagedStateAtCut
                } else {
                    TopologyInitialization::FutureOnlyAtCut
                },
                definition_sha256: digest(1),
                compatibility_sha256: digest(2),
                dependencies: if matches!(
                    change,
                    FixtureChange::RemovePipeline | FixtureChange::ResetPipeline
                ) {
                    match e.kind {
                        CatalogObjectKind::Stream => vec!["events".into()],
                        CatalogObjectKind::Sink => vec!["totals".into()],
                        _ => Vec::new(),
                    }
                } else {
                    Vec::new()
                },
                schema_sha256: (e.kind != CatalogObjectKind::Sink)
                    .then(|| SubscriptionDigest::from_bytes([8; 32]).to_hex()),
                managed_state_contract: (e.canonical_name == "totals")
                    .then(|| "sql_aggregate_v1".into()),
            })
            .collect(),
        requires_processing_pause: true,
        required_before_activation: vec![
            TopologyActivationRequirement::ParticipantPlanAgreement,
            TopologyActivationRequirement::ReconciledCheckpointCut,
            TopologyActivationRequirement::DurableInitializationAndProgress,
            TopologyActivationRequirement::ObservedActorRetirement,
            TopologyActivationRequirement::AtomicTargetCommit,
            TopologyActivationRequirement::InstalledTargetRelease,
        ],
    };
    descriptor
        .objects
        .sort_by(|a, b| (&a.name, a.catalog_generation).cmp(&(&b.name, b.catalog_generation)));
    descriptor.compatibility_sha256 = descriptor.descriptor_digest().unwrap();
    let plan = TopologyAdmissionPlan {
        protocol_version: TOPOLOGY_PREPARATION_PROTOCOL_VERSION,
        operation_id: Uuid::from_u128(41).try_into().unwrap(),
        expected_parent: descriptor.parent_version,
        parent_manifest: descriptor.parent_manifest.clone(),
        target_manifest: descriptor.target_manifest.clone(),
        assignment: fence.clone(),
        compatibility: Some(
            authority
                .stage_topology_compatibility(&descriptor)
                .await
                .unwrap(),
        ),
    };
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    topology_preparation::prepare_all(authority, &assignments, &plan, &admitted).await;
    let inventory = checkpoint_artifact_inventory(authority, &fence, 1).await;
    authority
        .begin_topology_checkpoint_cut(
            &lease.proof(),
            &assignments,
            &topology_preparation::processes(authority),
            plan.operation_id,
            &admitted.plan,
            inventory,
        )
        .await
        .unwrap();
    let mut manifests = Vec::new();
    for (vnode, participant) in fence.participants.iter().enumerate() {
        let mut manifest = CheckpointManifest::new_with_key_group_count(
            1,
            1,
            KeyGroupCount::try_from(2_u32).unwrap(),
        );
        manifest.bind_participant(participant.node_id);
        manifest.deployment_id.clone_from(&deployment);
        manifest.assignment_fence = Some(fence.clone());
        manifest.reassignment_portable = true;
        manifest.owned_vnodes = vec![u16::try_from(vnode).unwrap()];
        manifest.source_names = vec!["events".into()];
        manifest.sink_names = vec!["totals_sink".into()];
        manifest.source_offsets.insert(
            "events".into(),
            ConnectorCheckpoint {
                offsets: HashMap::from([(format!("partition-{vnode}"), "91".into())]),
                metadata: HashMap::from([("connector".into(), "kafka".into())]),
                input_channels: Some(vec![vec![u8::try_from(vnode + 1).unwrap()]]),
                source_assignment_version: std::num::NonZeroU64::new(fence.assignment_version),
            },
        );
        manifest.channel_progress = vec![ChannelProgress {
            participant_id: participant.node_id,
            source_name: "events".into(),
            input_channel: vec![u8::try_from(vnode + 1).unwrap()],
            watermark: Some(10),
            idle: false,
        }];
        manifest.checkpoint_watermark = Some(10);
        let data = Bytes::from_static(b"timer-and-keyed-state");
        manifest.node_data.object_length = data.len() as u64;
        manifest.node_data.sha256 = format!("{:x}", Sha256::digest(&data));
        manifest.state_frames = vec![StateFrame {
            key: StateFrameKey::Vnode {
                operator_id: "graph:totals".into(),
                vnode: u16::try_from(vnode).unwrap(),
            },
            chunk: manifest.node_data.chunk,
            range: ByteRange {
                offset: 0,
                length: data.len() as u64,
            },
            sha256: manifest.node_data.sha256.clone(),
        }];
        let certificate = OutputDistributionCertificate {
            version: OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            stream_id: "totals".into(),
            catalog_generation: 7,
            stream_generation: StreamGeneration::from_digest(SubscriptionDigest::from_bytes(
                [4; 32],
            )),
            final_operator_id: "stream:totals".into(),
            distribution: OutputDistribution::VnodePartitioned {
                key_expressions_fingerprint: SubscriptionDigest::from_bytes([5; 32]),
                partition_abi: crate::state::PARTITIONING_ABI_VERSION,
                vnode_count: 2,
            },
            schema_fingerprint: SubscriptionDigest::from_bytes([8; 32]),
            changelog_mode: ChangelogMode::WeightedRetractInsert,
            history_retention_bytes: 0,
            query_fingerprint: SubscriptionDigest::from_bytes([6; 32]),
            pipeline_identity: PipelineIdentity::empty(),
        };
        let mut subscription = NodeSubscriptionManifest {
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            epoch: 1,
            checkpoint_id: 1,
            participant_id: participant.node_id,
            assignment_certificate: fence.clone(),
            streams: vec![NodeSubscriptionStreamManifest {
                distribution_certificate: certificate,
                segments: Vec::new(),
                ranges: vec![NodePartitionRange {
                    partition: OutputPartitionId::new(u16::try_from(vnode).unwrap()),
                    first_sequence: PartitionSequence::new(7),
                    through_sequence: PartitionSequence::new(7),
                }],
            }],
            manifest_digest: SubscriptionDigest::from_bytes([1; 32]),
        };
        subscription.seal(&manifest.owned_vnodes).unwrap();
        manifest.subscription_output = Some(subscription);
        checkpoint_store(authority, 2)
            .with_participant_id(participant.node_id)
            .save_checkpoint(&manifest, &[data])
            .await
            .unwrap();
        manifests.push(manifest);
    }
    let index = CommittedCheckpointIndex {
        version: crate::checkpoint::COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id: deployment,
        pipeline_identity: PipelineIdentity::empty(),
        epoch: 1,
        checkpoint_id: 1,
        scope: CheckpointScope::Cluster,
        vnode_count: 2,
        assignment_fence: Some(fence.clone()),
        reassignment_portable: true,
        predecessor: None,
        participants: manifests
            .iter()
            .map(|m| {
                CommittedParticipantRef::from_manifest(m, &checkpoint_manifest_bytes(m).unwrap())
                    .unwrap()
            })
            .collect(),
        source_names: vec!["events".into()],
        source_offsets: BTreeMap::from([(
            "events".into(),
            ConnectorCheckpoint {
                offsets: HashMap::from([
                    ("partition-0".into(), "91".into()),
                    ("partition-1".into(), "91".into()),
                ]),
                metadata: HashMap::from([("connector".into(), "kafka".into())]),
                input_channels: Some(vec![vec![1], vec![2]]),
                source_assignment_version: std::num::NonZeroU64::new(fence.assignment_version),
            },
        )]),
        channel_progress: manifests
            .iter()
            .flat_map(|m| m.channel_progress.clone())
            .collect(),
        source_watermarks: BTreeMap::from([("events".into(), 10)]),
        checkpoint_watermark: Some(10),
    };
    let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
    authority
        .record_cluster_outcome(
            &lease.proof(),
            1,
            1,
            fence.clone(),
            CheckpointVerdict::Commit,
            Some(reference),
        )
        .await
        .unwrap();
    let mut prepared = None;
    for participant in &fence.participants {
        prepared = Some(
            authority
                .complete_topology_checkpoint_cut(
                    &lease.proof(),
                    crate::checkpoint::CheckpointAttempt::canonical(1),
                    *participant,
                )
                .await
                .unwrap(),
        );
    }
    Fixture {
        lease,
        assignments,
        store: checkpoint_store(authority, 2),
        operation: prepared.unwrap(),
        descriptor,
        index,
        manifests,
    }
}

#[path = "topology_restore_tests.rs"]
mod restore;
#[path = "topology_source_root_tests.rs"]
mod source_initialization;
#[path = "topology_target_preparation_tests.rs"]
mod target_preparation;

fn checkpoint_store(authority: &LeaderLeaseStore, vnode_count: u32) -> ObjectStoreCheckpointStore {
    ObjectStoreCheckpointStore::new(authority.store.clone(), "")
        .with_key_group_count(KeyGroupCount::try_from(vnode_count).unwrap())
}

#[tokio::test]
async fn topology_root_preserves_state_progress_and_subscription_identity_without_target_authority()
{
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    let before = authority.load().await.unwrap().unwrap();
    let staged = fixture.stage(&authority).await.unwrap();
    assert_eq!(staged.phase, TopologyAdmissionPhase::CutPrepared);
    assert_eq!(staged.cut, fixture.operation.cut);
    assert_eq!(staged.preparation, fixture.operation.preparation);
    let root = authority
        .topology_migration_root(staged.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(root.future_only_objects, ["later"]);
    assert_eq!(root.preserved_objects.len(), 3);
    assert_eq!(
        root.preserved_objects[1].state_operator_id.as_deref(),
        Some("graph:totals")
    );
    let subscription = &root.subscriptions[0];
    assert_eq!(
        subscription.parent_certificate.stream_generation,
        subscription.target_certificate.stream_generation
    );
    assert_eq!(
        subscription.parent_certificate.pipeline_identity,
        fixture.descriptor.parent_pipeline
    );
    assert_eq!(
        subscription.target_certificate.pipeline_identity,
        fixture.descriptor.target_pipeline
    );
    assert!(subscription
        .frontiers
        .iter()
        .all(|f| f.through_sequence == PartitionSequence::new(7)));
    assert_eq!(
        root.cut,
        fixture
            .operation
            .cut
            .as_ref()
            .unwrap()
            .committed
            .clone()
            .unwrap()
    );
    assert_eq!(fixture.stage(&authority).await.unwrap(), staged);
    let after = authority.load().await.unwrap().unwrap();
    assert_eq!(after.seq, before.seq + 1);
    assert_eq!(after.catalog_manifest, before.catalog_manifest);
    assert_eq!(
        authority.load_record().await.unwrap().unwrap().version,
        TOPOLOGY_MIGRATION_ROOT_RECORD_VERSION
    );
    assert_eq!(
        CheckpointDecisionStore::new(authority.store.clone())
            .load_committed_checkpoint(&root.cut.checkpoint)
            .await
            .unwrap(),
        fixture.index
    );
    for manifest in &fixture.manifests {
        assert_eq!(
            fixture
                .store
                .load_manifest_for_participant(manifest.participant_id, 1)
                .await
                .unwrap(),
            Some(manifest.clone())
        );
    }
    let aborted = authority
        .abort_topology_plan(&fixture.lease.proof(), staged.operation_id, &staged.plan)
        .await
        .unwrap();
    assert_eq!(aborted.migration_root, staged.migration_root);
    // After Abort, ordinary checkpoint retention may retire old payload metadata. Historical
    // root/status audit still binds the decision and immutable requirements, never authorizes
    // recovery of the aborted target or invents an artifact that retention removed.
    CheckpointDecisionStore::new(authority.store.clone())
        .delete_committed_checkpoint(&root.cut.checkpoint)
        .await
        .unwrap();
    for manifest in &fixture.manifests {
        fixture
            .store
            .delete_manifest(manifest.node_data.chunk)
            .await
            .unwrap();
    }
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    assert_eq!(
        reopened
            .topology_migration_root(staged.operation_id)
            .await
            .unwrap(),
        Some(root)
    );
    assert!(fixture.stage(&reopened).await.is_err());
}

#[tokio::test]
async fn topology_root_rejects_missing_or_changed_metadata_and_unknown_state_slots() {
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    let before = authority.load().await.unwrap();
    let chunk = fixture.manifests[0].node_data.chunk;
    fixture.store.delete_manifest(chunk).await.unwrap();
    assert!(fixture.stage(&authority).await.is_err());
    assert_eq!(authority.load().await.unwrap(), before);
    let original = &fixture.manifests[0];
    let path = OsPath::from(format!(
        "nodes/{}/checkpoints/{:020}/manifest.json",
        chunk.participant_id, chunk.checkpoint_id
    ));
    let mut changed = original.clone();
    changed
        .source_offsets
        .get_mut("events")
        .unwrap()
        .offsets
        .insert("partition-0".into(), "92".into());
    authority
        .store
        .put(
            &path,
            PutPayload::from(checkpoint_manifest_bytes(&changed).unwrap()),
        )
        .await
        .unwrap();
    assert!(fixture.stage(&authority).await.is_err());
    assert_eq!(authority.load().await.unwrap(), before);
    let mut changed = fixture.manifests.clone();
    changed[0].state_frames[0].key = StateFrameKey::Vnode {
        operator_id: "graph:optimizer_index_9".into(),
        vnode: 0,
    };
    let mut index = fixture.index.clone();
    index.participants[0] = CommittedParticipantRef::from_manifest(
        &changed[0],
        &checkpoint_manifest_bytes(&changed[0]).unwrap(),
    )
    .unwrap();
    let mut operation = fixture.operation.clone();
    operation
        .cut
        .as_mut()
        .unwrap()
        .committed
        .as_mut()
        .unwrap()
        .checkpoint = index.encode_and_reference().unwrap().1;
    assert!(matches!(
        TopologyMigrationRoot::build(&operation, &fixture.descriptor, &index, &changed),
        Err(TopologyError::Unsupported(_))
    ));
}

#[tokio::test]
async fn topology_root_rejects_unresolved_sources_and_wrong_subscription_contracts() {
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    let mut descriptor = fixture.descriptor.clone();
    let added = descriptor
        .objects
        .iter_mut()
        .find(|o| o.name == "later")
        .unwrap();
    added.kind = CatalogObjectKind::Source;
    added.initialization = TopologyInitialization::ResolveSourcePositionsOnce;
    assert!(matches!(
        TopologyMigrationRoot::build(
            &fixture.operation,
            &descriptor,
            &fixture.index,
            &fixture.manifests
        ),
        Err(TopologyError::Unsupported(_))
    ));
    descriptor = fixture.descriptor.clone();
    descriptor
        .objects
        .iter_mut()
        .find(|o| o.name == "totals")
        .unwrap()
        .schema_sha256 = Some(digest(99));
    assert!(TopologyMigrationRoot::build(
        &fixture.operation,
        &descriptor,
        &fixture.index,
        &fixture.manifests
    )
    .is_err());
    let mut root = TopologyMigrationRoot::build(
        &fixture.operation,
        &fixture.descriptor,
        &fixture.index,
        &fixture.manifests,
    )
    .unwrap();
    root.subscriptions[0].target_certificate.catalog_generation += 1;
    assert!(root.encode_and_reference().is_err());
    assert!(fixture.operation.migration_root.is_none());
}

#[tokio::test]
async fn topology_root_new_managed_state_requires_explicit_initialization_and_keeps_parent_state_required(
) {
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    let mut descriptor = fixture.descriptor.clone();
    let added = descriptor
        .objects
        .iter_mut()
        .find(|object| object.name == "later")
        .unwrap();
    added.managed_state_contract = Some("sql_aggregate_v1".into());
    descriptor.compatibility_sha256 = descriptor.descriptor_digest().unwrap();
    assert!(descriptor.encode_and_reference().is_err());
    assert!(TopologyMigrationRoot::build(
        &fixture.operation,
        &descriptor,
        &fixture.index,
        &fixture.manifests
    )
    .is_err());
    descriptor
        .objects
        .iter_mut()
        .find(|object| object.name == "later")
        .unwrap()
        .initialization = TopologyInitialization::EmptyManagedStateAtCut;
    descriptor.compatibility_sha256 = descriptor.descriptor_digest().unwrap();
    descriptor.encode_and_reference().unwrap();
    let root = TopologyMigrationRoot::build(
        &fixture.operation,
        &descriptor,
        &fixture.index,
        &fixture.manifests,
    )
    .unwrap();
    assert_eq!(root.future_only_objects, ["later"]);
    let mut manifests = fixture.manifests.clone();
    let mut index = fixture.index.clone();
    for (manifest, participant) in manifests.iter_mut().zip(&mut index.participants) {
        manifest.state_frames.retain(|frame| !matches!(&frame.key, StateFrameKey::Vnode { operator_id, .. } if operator_id == "graph:totals"));
        manifest.node_data.object_length = 0;
        manifest.node_data.sha256 = format!("{:x}", Sha256::digest([]));
        *participant = CommittedParticipantRef::from_manifest(
            manifest,
            &checkpoint_manifest_bytes(manifest).unwrap(),
        )
        .unwrap();
    }
    let mut operation = fixture.operation.clone();
    operation
        .cut
        .as_mut()
        .unwrap()
        .committed
        .as_mut()
        .unwrap()
        .checkpoint = index.encode_and_reference().unwrap().1;
    let error =
        TopologyMigrationRoot::build(&operation, &descriptor, &index, &manifests).unwrap_err();
    assert!(
        error.to_string().contains("no parent vnode state"),
        "{error}"
    );
}

#[tokio::test]
async fn topology_root_lost_write_response_uses_one_append_and_reads_no_state_payloads() {
    let (raw, authority) = ambiguous_once_at(30_000, lease_path(11));
    let fixture = fixture(&authority).await;
    assert_eq!(fixture.operation.status_sequence, 10);
    raw.clear_authority_io_counts();
    let staged = fixture.stage(&authority).await.unwrap();
    assert_eq!(staged.status_sequence, 11);
    assert_eq!(raw.put_count(&lease_path(11), "create"), 1);
    for manifest in &fixture.manifests {
        let path = OsPath::from(format!(
            "nodes/{}/checkpoints/{:020}/node-data.bin",
            manifest.participant_id, 1
        ));
        assert_eq!(
            raw.get_count(&path),
            0,
            "staging read historical state bytes"
        );
        assert_eq!(
            raw.put_count(&path, "create"),
            0,
            "staging copied state bytes"
        );
    }
    assert_eq!(fixture.stage(&authority).await.unwrap(), staged);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn topology_root_cancelled_successful_append_is_recoverable_and_immutable() {
    let (raw, authority) = delayed_ambiguous_response_once_at(30_000, lease_path(11));
    let fixture = fixture(&authority).await;
    let expected = fixture.operation.clone();
    let task_authority = Arc::clone(&authority);
    let task = tokio::spawn(async move { fixture.stage(&task_authority).await });
    raw.entered.acquire().await.unwrap().forget();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    let staged = reopened
        .topology_operation_status(expected.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(staged.phase, TopologyAdmissionPhase::CutPrepared);
    assert_eq!(
        staged.migration_root.as_ref().unwrap().authority_sequence,
        11
    );
    let mut rewritten = staged.clone();
    rewritten.status_sequence = 12;
    rewritten.migration_root.as_mut().unwrap().root.sha256 = digest(99);
    assert!(staged.validate_successor(&rewritten, 12).is_err());
    rewritten = staged.clone();
    rewritten.migration_root = None;
    assert!(staged.validate_successor(&rewritten, 12).is_err());
    assert_eq!(staged.cut, expected.cut);
}

#[tokio::test(start_paused = true)]
async fn topology_root_deadline_before_append_keeps_cut_and_has_no_target_authority() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(11));
    let fixture = fixture(&authority).await;
    let expected = fixture.operation.clone();
    let task_authority = Arc::clone(&authority);
    let task = tokio::spawn(async move { fixture.stage(&task_authority).await });
    raw.entered.acquire().await.unwrap().forget();
    tokio::time::advance(Duration::from_secs(16)).await;
    assert!(matches!(task.await.unwrap(), Err(TopologyError::Contended)));
    assert_eq!(
        authority
            .topology_operation_status(expected.operation_id)
            .await
            .unwrap(),
        Some(expected)
    );
}

#[tokio::test]
async fn topology_root_stale_leader_loses_append_race_and_retains_parent_cut() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(11));
    let fixture = fixture(&authority).await;
    let expected = fixture.operation.clone();
    let owner = fixture.lease.owner.clone();
    let task_authority = Arc::clone(&authority);
    let task = tokio::spawn(async move { fixture.stage(&task_authority).await });
    raw.entered.acquire().await.unwrap().forget();
    assert!(matches!(
        authority.begin_new_term(&owner, 1).await.unwrap(),
        LeaseOutcome::Acquired(_)
    ));
    raw.release.add_permits(1);
    assert!(matches!(task.await.unwrap(), Err(TopologyError::Fenced)));
    let aborted = authority
        .topology_operation_status(expected.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        aborted.phase,
        TopologyAdmissionPhase::Aborted { .. }
    ));
    assert!(aborted.migration_root.is_none());
    assert_eq!(aborted.cut, expected.cut);
}

#[tokio::test]
async fn topology_root_pins_retention_and_survives_abort_pruning_but_damage_fails_closed() {
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    let staged = fixture.stage(&authority).await.unwrap();
    let cut = fixture
        .operation
        .cut
        .as_ref()
        .unwrap()
        .committed
        .as_ref()
        .unwrap()
        .checkpoint
        .clone();
    let newer = committed_checkpoint_with_predecessor(
        &authority,
        fixture.index.assignment_fence.as_ref().unwrap(),
        2,
        9,
        Some(cut),
    )
    .await;
    assert!(authority
        .begin_cluster_artifact_cleanup(&fixture.lease.proof(), newer, accept_recovery_artifacts)
        .await
        .unwrap()
        .is_none());
    let aborted = authority
        .abort_topology_plan(&fixture.lease.proof(), staged.operation_id, &staged.plan)
        .await
        .unwrap();
    for now in 1..=8 {
        authority
            .renew_exact(&fixture.lease.owner, fixture.lease.token, now)
            .await
            .unwrap();
    }
    LeaderLeaseStore::prune_history(&authority.store, 0)
        .await
        .unwrap();
    assert_eq!(
        authority
            .topology_operation_status(staged.operation_id)
            .await
            .unwrap(),
        Some(aborted)
    );
    let binding = staged.migration_root.as_ref().unwrap();
    let path = OsPath::from(format!(
        "control/topology-migration-roots/v1/{}.json",
        binding.root.sha256
    ));
    let original = authority
        .store
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    authority
        .store
        .put(&path, PutPayload::from(Bytes::from_static(b"{}")))
        .await
        .unwrap();
    assert!(authority
        .topology_operation_status(staged.operation_id)
        .await
        .is_err());
    assert!(LeaderLeaseStore::prune_history(&authority.store, 0)
        .await
        .is_err());
    authority
        .store
        .put(&path, PutPayload::from(original))
        .await
        .unwrap();
    authority
        .store
        .delete(&lease_path(binding.authority_sequence))
        .await
        .unwrap();
    assert!(authority
        .topology_migration_root(staged.operation_id)
        .await
        .is_err());
}

#[tokio::test]
async fn topology_root_rejects_assignment_process_and_payload_changes_without_append() {
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    let before = authority.load().await.unwrap().unwrap();
    let mut wrong = fixture.operation.plan.clone();
    wrong.sha256 = digest(99);
    assert!(authority
        .stage_topology_migration_root(
            &fixture.lease.proof(),
            &fixture.assignments,
            &topology_preparation::processes(&authority),
            &fixture.store,
            fixture.operation.operation_id,
            &wrong
        )
        .await
        .is_err());
    let prior = fixture.assignments.load().await.unwrap().unwrap();
    let next = prior
        .next_for_participants(prior.vnodes.clone(), prior.participants.clone())
        .unwrap();
    fixture
        .assignments
        .save_if_version(&next, prior.version)
        .await
        .unwrap();
    assert!(fixture.stage(&authority).await.is_err());
    assert_eq!(authority.load().await.unwrap().unwrap().seq, before.seq);
    let process =
        crate::cluster::control::ProcessLeaseStore::new(authority.store.clone(), NodeId(2), 30_000);
    process
        .try_acquire(Uuid::from_u128(222), 30_001)
        .await
        .unwrap();
    assert!(matches!(
        fixture.stage(&authority).await,
        Err(TopologyError::Fenced)
    ));
    assert_eq!(authority.load().await.unwrap().unwrap().seq, before.seq);
}

#[tokio::test]
async fn topology_root_metadata_budgets_formats_and_exact_progress_fail_closed() {
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    let mut oversized = fixture.index.clone();
    oversized.participants[0].manifest_len =
        crate::cluster::control::MAX_TOPOLOGY_ROOT_MANIFEST_BYTES;
    assert!(super::super::topology_migration_root::validate_manifest_budget(&oversized).is_err());
    oversized.participants[0].manifest_len = u64::MAX;
    assert!(super::super::topology_migration_root::validate_manifest_budget(&oversized).is_err());
    let original = TopologyMigrationRoot::build(
        &fixture.operation,
        &fixture.descriptor,
        &fixture.index,
        &fixture.manifests,
    )
    .unwrap();
    let mut root = original.clone();
    root.format_version = 2;
    assert!(root.encode_and_reference().is_err());
    root = original.clone();
    root.future_only_objects[0] = "oversized".repeat(150_000);
    assert!(root.encode_and_reference().is_err());
    let mut json = serde_json::to_value(&original).unwrap();
    json["unrecognized_authority"] = serde_json::json!(true);
    assert!(serde_json::from_value::<TopologyMigrationRoot>(json).is_err());
    let mut index = fixture.index.clone();
    index
        .source_offsets
        .get_mut("events")
        .unwrap()
        .offsets
        .insert("partition-0".into(), "92".into());
    let mut operation = fixture.operation.clone();
    operation
        .cut
        .as_mut()
        .unwrap()
        .committed
        .as_mut()
        .unwrap()
        .checkpoint = index.encode_and_reference().unwrap().1;
    assert!(TopologyMigrationRoot::build(
        &operation,
        &fixture.descriptor,
        &index,
        &fixture.manifests
    )
    .is_err());
    let mut record = authority.load_record().await.unwrap().unwrap();
    let staged = fixture.stage(&authority).await.unwrap();
    record.topology_operations[0] = staged;
    record.lease.seq = record.topology_operations[0].status_sequence;
    assert!(
        record.validate().is_err(),
        "format 16 cannot carry a migration root"
    );
}

mod sink_removal {
    //! A sink is retired only through the certified parent cut and irreversible target decision.

    use super::*;

    #[tokio::test]
    async fn topology_sink_removal_requires_explicit_mapping_and_complete_old_sink_inventory() {
        let authority = store(30_000);
        let fixture = fixture_with_changes(&authority, FixtureChange::RemoveSink).await;
        let parent = authority
            .load_catalog_manifest(&fixture.descriptor.parent_manifest)
            .await
            .unwrap();
        let target = authority
            .load_catalog_manifest(&fixture.descriptor.target_manifest)
            .await
            .unwrap();
        fixture
            .descriptor
            .validate_catalogs(&parent, &target)
            .unwrap();
        let mut report = fixture.descriptor.clone();
        report
            .objects
            .iter_mut()
            .find(|object| object.name == "totals_sink")
            .unwrap()
            .transition = ClusterTopologyObjectTransition::Preserve;
        report.compatibility_sha256 = report.descriptor_digest().unwrap();
        assert!(report.validate_catalogs(&parent, &target).is_err());
        report = fixture.descriptor.clone();
        let stream = report
            .objects
            .iter_mut()
            .find(|object| object.name == "totals")
            .unwrap();
        stream.transition = ClusterTopologyObjectTransition::Remove;
        stream.initialization = TopologyInitialization::RetireAtCut;
        report.compatibility_sha256 = report.descriptor_digest().unwrap();
        assert!(report.validate_catalogs(&parent, &target).is_err());
        let mut manifests = fixture.manifests.clone();
        let mut index = fixture.index.clone();
        for (manifest, participant) in manifests.iter_mut().zip(&mut index.participants) {
            manifest.sink_names.clear();
            *participant = CommittedParticipantRef::from_manifest(
                manifest,
                &checkpoint_manifest_bytes(manifest).unwrap(),
            )
            .unwrap();
        }
        let mut operation = fixture.operation.clone();
        operation
            .cut
            .as_mut()
            .unwrap()
            .committed
            .as_mut()
            .unwrap()
            .checkpoint = index.encode_and_reference().unwrap().1;
        let error =
            TopologyMigrationRoot::build(&operation, &fixture.descriptor, &index, &manifests)
                .unwrap_err();
        assert!(
            error.to_string().contains("cut source/sink inventory"),
            "{error}"
        );
        let staged = fixture.stage(&authority).await.unwrap();
        let root = authority
            .topology_migration_root(staged.operation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(root.preserved_objects.len(), 2);
        assert!(root.future_only_objects.is_empty());
        assert_eq!(root.subscriptions.len(), 1);
        assert!(authority.retired_topology_names().await.unwrap().is_empty());
        let aborted = authority
            .abort_topology_plan(&fixture.lease.proof(), staged.operation_id, &staged.plan)
            .await
            .unwrap();
        assert!(matches!(
            aborted.phase,
            TopologyAdmissionPhase::Aborted { .. }
        ));
        assert!(authority.retired_topology_names().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn topology_removal_survives_a_lost_commit_response_and_blocks_name_reuse() {
        for change in [FixtureChange::RemoveSink, FixtureChange::RemovePipeline] {
            assert_retirement_lost_response(change).await;
        }
    }

    async fn assert_retirement_lost_response(change: FixtureChange) {
        let (raw, authority) = ambiguous_once_at(30_000, lease_path(14));
        let fixture = fixture_with_changes(&authority, change).await;
        let (fixture, input) = topology_commit::prepared_fixture(&authority, fixture).await;
        let committed = topology_commit::commit(&authority, &fixture, &input)
            .await
            .unwrap();
        assert!(raw
            .did_return_ambiguous
            .load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(committed.phase, TopologyAdmissionPhase::Committed);
        let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
        assert_eq!(
            reopened.retired_topology_names().await.unwrap(),
            input
                .descriptor()
                .objects
                .iter()
                .filter(|object| object.transition == ClusterTopologyObjectTransition::Remove)
                .map(|object| object.name.clone())
                .collect()
        );
        assert_eq!(
            reopened
                .original_topology_catalog(&input.plan().target_manifest)
                .await
                .unwrap()
                .unwrap(),
            *input.parent()
        );
        let restored = topology_commit::reconstruct(&reopened, &fixture, input.process())
            .await
            .unwrap();
        assert_eq!(restored.parent(), input.parent());
        assert_eq!(restored.target(), input.target());
        assert_eq!(restored.root().subscriptions, input.root().subscriptions);
        assert!(restored
            .target()
            .entries
            .iter()
            .all(|entry| entry.canonical_name != "totals_sink"));
        let mut target = restored.target().clone();
        let sink = input
            .parent()
            .entries
            .iter()
            .find(|entry| entry.canonical_name == "totals_sink")
            .unwrap()
            .clone();
        target.entries.push(sink);
        let record = reopened.load_record().await.unwrap().unwrap();
        let plan = TopologyAdmissionPlan {
            protocol_version: crate::cluster::control::topology::TOPOLOGY_PROTOCOL_VERSION,
            operation_id: Uuid::from_u128(141).try_into().unwrap(),
            expected_parent: TopologyVersion::new(2).unwrap(),
            parent_manifest: restored.target().reference().unwrap(),
            target_manifest: target.reference().unwrap(),
            assignment: restored.plan().assignment.clone(),
            compatibility: None,
        };
        let error = reopened
            .validate_topology_inventory_proposal(&record, &plan, restored.target(), &target)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("retired name"), "{error}");
    }
}

mod pipeline_removal {
    use super::*;

    #[tokio::test]
    async fn topology_removal_accounts_for_parent_state_subscriptions_and_source_inventory() {
        let authority = store(30_000);
        let fixture = fixture_with_changes(&authority, FixtureChange::RemovePipeline).await;
        let parent = authority
            .load_catalog_manifest(&fixture.descriptor.parent_manifest)
            .await
            .unwrap();
        let target = authority
            .load_catalog_manifest(&fixture.descriptor.target_manifest)
            .await
            .unwrap();
        fixture
            .descriptor
            .validate_catalogs(&parent, &target)
            .unwrap();
        assert!(target.entries.is_empty());
        let staged = fixture.stage(&authority).await.unwrap();
        let root = authority
            .topology_migration_root(staged.operation_id)
            .await
            .unwrap()
            .unwrap();
        assert!(root.preserved_objects.is_empty());
        assert!(root.subscriptions.is_empty());
        assert!(root.future_only_objects.is_empty());
        root.validate_restore_cut(
            &staged,
            &fixture.descriptor,
            &fixture.index,
            &fixture.manifests,
        )
        .unwrap();

        let mut report = fixture.descriptor.clone();
        let sink = report
            .objects
            .iter_mut()
            .find(|object| object.name == "totals_sink")
            .unwrap();
        sink.transition = ClusterTopologyObjectTransition::Preserve;
        sink.initialization = TopologyInitialization::PreserveExactCut;
        report.compatibility_sha256 = report.descriptor_digest().unwrap();
        assert!(
            report.encode_and_reference().is_err(),
            "a surviving sink cannot consume a removed stream"
        );

        let mut manifests = fixture.manifests.clone();
        for manifest in &mut manifests {
            manifest.state_frames.clear();
            manifest.node_data.object_length = 0;
            manifest.node_data.sha256 = crate::checkpoint::checkpoint_sha256(b"");
        }
        let mut index = fixture.index.clone();
        index.participants = manifests
            .iter()
            .map(|manifest| {
                CommittedParticipantRef::from_manifest(
                    manifest,
                    &checkpoint_manifest_bytes(manifest).unwrap(),
                )
                .unwrap()
            })
            .collect();
        let mut operation = fixture.operation.clone();
        operation
            .cut
            .as_mut()
            .unwrap()
            .committed
            .as_mut()
            .unwrap()
            .checkpoint = index.encode_and_reference().unwrap().1;
        let error =
            TopologyMigrationRoot::build(&operation, &fixture.descriptor, &index, &manifests)
                .unwrap_err();
        assert!(
            error.to_string().contains("no parent vnode state"),
            "{error}"
        );

        let mut target_index = fixture.index.clone();
        target_index.pipeline_identity = fixture.descriptor.target_pipeline.clone();
        target_index.epoch += 1;
        target_index.checkpoint_id += 1;
        target_index.predecessor = Some(root.cut.checkpoint.clone());
        target_index.source_names.clear();
        target_index.source_offsets.clear();
        target_index.channel_progress.clear();
        target_index.source_watermarks.clear();
        target_index.checkpoint_watermark = None;
        root.validate_target_checkpoint_predecessor(
            &fixture.descriptor,
            &target_index,
            &fixture.index,
        )
        .unwrap();
        target_index.source_names = fixture.index.source_names.clone();
        assert!(root
            .validate_target_checkpoint_predecessor(
                &fixture.descriptor,
                &target_index,
                &fixture.index
            )
            .is_err());
    }
}
