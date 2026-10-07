//! Real old-graph cut and pre-commit abort. Candidate admission uses the core API because SQL
//! migration submission remains guarded. This never installs or activates the reserved candidate.

use super::{topology_adoption, wait_for, Node};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(feature = "kafka")]
use laminar_connectors::{
    config::ConnectorConfig,
    connector::SourceConnector,
    kafka::{KafkaSource, KafkaSourceConfig},
};
use laminar_core::checkpoint::{CheckpointStore, ConnectorCheckpoint, ObjectStoreCheckpointStore};
use laminar_core::checkpoint_decision::CheckpointDecisionStore;
use laminar_core::cluster::control::{
    AssignmentSnapshotStore, CatalogManifestEntry, CatalogManifestStore, CatalogObjectKind,
    CheckpointAssignmentFence, ClusterTopologyValidation, LeaderLeaseStore, LegacyTopologyBaseline,
    ProcessLeaseAuthority, TopologyAbortReason, TopologyAdmissionPhase, TopologyAdmissionPlan,
    TopologyAdmissionStatus, TopologyError, TopologySourceInitialization,
    TOPOLOGY_PREPARATION_PROTOCOL_VERSION,
};

#[cfg(feature = "kafka")]
pub(super) fn prepare_old_cut(
    checkpoint_url: &str,
    brokers: &str,
    nodes: &mut [Node],
    baseline: &LegacyTopologyBaseline,
    assignment: &CheckpointAssignmentFence,
    ceiling: Duration,
    evidence_dir: &Path,
) -> TopologyAdmissionStatus {
    let objects = topology_adoption::objects_for_namespace(checkpoint_url);
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 30_000));
    // This harness configures one shared namespace for process, checkpoint and catalog storage.
    let processes =
        ProcessLeaseAuthority::new(Arc::clone(&objects), Duration::from_secs(30)).unwrap();
    let checkpoint_store = ObjectStoreCheckpointStore::new(Arc::clone(&objects), "")
        .with_key_group_count(
            laminar_core::state::KeyGroupCount::try_from(assignment.vnode_count).unwrap(),
        );
    let decisions = CheckpointDecisionStore::new(Arc::clone(&objects));
    let assignments = AssignmentSnapshotStore::new(Arc::clone(&objects));
    let catalog = CatalogManifestStore::new(Arc::clone(&authority));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let parent = runtime.block_on(catalog.load()).unwrap().unwrap();
    let source = parent
        .entries
        .iter()
        .find(|entry| entry.kind == CatalogObjectKind::Source)
        .unwrap();
    let mut candidate = parent.clone();
    let probe_id = uuid::Uuid::new_v4();
    let probe_topic = format!("topology-source-probe-{probe_id}");
    let probe_output_topic = format!("topology-source-output-{probe_id}");
    let probe_group = format!("topology-source-group-{probe_id}");
    super::kafka_create_topic(brokers, &probe_topic, 3);
    let mut probe_config = ConnectorConfig::new("kafka");
    for (key, value) in [
        ("bootstrap.servers", brokers),
        ("group.id", probe_group.as_str()),
        ("topic", probe_topic.as_str()),
        ("startup.mode", "latest"),
        ("laminar.source.name", "topology_source_probe"),
    ] {
        probe_config.set(key, value);
    }
    // Explicit fixture input, before any candidate validation. Partition two remains never-read
    // and empty. No target actor or sink is started in this pre-commit cut/abort scenario.
    runtime.block_on(async {
        use rdkafka::producer::{FutureProducer, FutureRecord};
        let producer: FutureProducer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .unwrap();
        for (partition, count) in [(0, 2), (1, 3)] {
            for id in 0..count {
                let payload = format!(r#"{{"id":{id},"value":{id}}}"#);
                producer
                    .send(
                        FutureRecord::to(&probe_topic)
                            .partition(partition)
                            .key("probe")
                            .payload(&payload),
                        Duration::from_secs(5),
                    )
                    .await
                    .unwrap();
            }
        }
    });
    // Validate through the public, authenticated read-only API on every running process. The
    // admission below binds this descriptor; every process independently recompiles to certify it.
    candidate.entries.push(CatalogManifestEntry {
        schema_binding: None,
        canonical_name: "topology_cut_probe".into(),
        kind: CatalogObjectKind::Stream,
        catalog_generation: 1,
        ddl: format!(
            "CREATE STREAM topology_cut_probe AS SELECT * FROM \"{}\"",
            source.canonical_name.replace('"', "\"\"")
        ),
    });
    for (name, kind, ddl) in [
        ("topology_source_probe", CatalogObjectKind::Source, format!(
            "CREATE SOURCE topology_source_probe (id BIGINT NOT NULL, value BIGINT NOT NULL) FROM kafka ('bootstrap.servers' = '{}', 'group.id' = '{}', 'topic' = '{}', 'startup.mode' = 'latest')",
            brokers.replace('\'', "''"), probe_group, probe_topic,
        )),
        ("topology_source_stream", CatalogObjectKind::Stream, "CREATE STREAM topology_source_stream AS SELECT id, value FROM topology_source_probe".into()),
        ("topology_source_sink", CatalogObjectKind::Sink, format!(
            "CREATE SINK topology_source_sink FROM topology_source_stream INTO kafka ('bootstrap.servers' = '{}', 'topic' = '{}')",
            brokers.replace('\'', "''"), probe_output_topic,
        )),
    ] {
        candidate.entries.push(CatalogManifestEntry { schema_binding: None, canonical_name: name.into(), kind, catalog_generation: 1, ddl });
    }
    let request = serde_json::json!({
        "expected_parent_version": baseline.topology_version.get(),
        "statements": candidate.entries[parent.entries.len()..].iter().map(|e| &e.ddl).collect::<Vec<_>>(),
    });
    let mut validations = Vec::new();
    let mut expected_validation = None;
    for node in nodes.iter_mut() {
        let validated_at = Instant::now();
        let body = node
            .http_request(
                "POST",
                "/api/v1/cluster/topology/validate",
                Some(&request.to_string()),
                ceiling,
            )
            .expect("running stateful catalog must accept effect-free additive validation");
        let validation_elapsed_ms = validated_at.elapsed().as_secs_f64() * 1000.0;
        let validation: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(validation["scope"], "local_candidate_plan");
        assert_eq!(validation["parent_version"].as_u64(), Some(1));
        assert_eq!(validation["target_version"].as_u64(), Some(2));
        assert_eq!(
            validation["parent_manifest"],
            serde_json::to_value(&baseline.manifest).unwrap()
        );
        assert_eq!(
            validation["target_manifest"],
            serde_json::to_value(candidate.reference().unwrap()).unwrap()
        );
        assert_ne!(validation["parent_pipeline"], validation["target_pipeline"]);
        let mapped_state = validation["objects"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|object| {
                object["transition"] == "preserve" && object["managed_state_contract"].is_string()
            })
            .count();
        assert!(
            mapped_state >= 4,
            "stateful oracle graph lost its compatibility mappings: {validation}"
        );
        if let Some(expected) = &expected_validation {
            assert_eq!(&validation, expected);
        } else {
            expected_validation = Some(validation.clone());
        }
        let active: serde_json::Value =
            serde_json::from_str(&node.http_get("/api/v1/cluster/topology").unwrap()).unwrap();
        assert_eq!(active["committed_version"].as_u64(), Some(1));
        assert_eq!(active["locally_active_version"].as_u64(), Some(1));
        eprintln!("soak: node{} local topology validation completed in {validation_elapsed_ms:.3} ms with intake active", node.id);
        validations.push(serde_json::json!({"node": node.id, "validation_elapsed_ms": validation_elapsed_ms, "validation": validation}));
    }
    assert_eq!(runtime.block_on(catalog.load()).unwrap().unwrap(), parent);
    std::fs::write(
        evidence_dir.join("topology-local-validations.json"),
        serde_json::to_vec_pretty(&validations).unwrap(),
    )
    .unwrap();
    eprintln!("soak: all {} running processes validated the same candidate and preserved managed state contracts; target remains uncommitted", nodes.len());
    let descriptor: ClusterTopologyValidation =
        serde_json::from_value(expected_validation.unwrap()).unwrap();
    let compatibility = runtime
        .block_on(authority.stage_topology_compatibility(&descriptor))
        .unwrap();
    let plan = TopologyAdmissionPlan {
        protocol_version: TOPOLOGY_PREPARATION_PROTOCOL_VERSION,
        operation_id: uuid::Uuid::new_v4().try_into().unwrap(),
        expected_parent: baseline.topology_version,
        parent_manifest: baseline.manifest.clone(),
        target_manifest: candidate.reference().unwrap(),
        assignment: assignment.clone(),
        compatibility: Some(compatibility),
    };
    let mut admitted = None;
    wait_for(
        "core topology reservation after stable checkpoint settlement",
        ceiling,
        || {
            let result = runtime.block_on(async {
                let proof = authority
                    .load()
                    .await?
                    .ok_or(TopologyError::Fenced)?
                    .proof();
                authority
                    .admit_topology_plan(&proof, &assignments, &plan, &candidate)
                    .await
            });
            match result {
                Ok(status) => {
                    admitted = Some(status);
                    true
                }
                Err(
                    TopologyError::Conflict(_) | TopologyError::Contended | TopologyError::Fenced,
                ) => false,
                Err(error) => panic!("old-graph cut admission failed: {error}"),
            }
        },
    );
    assert_eq!(admitted.unwrap().phase, TopologyAdmissionPhase::Planned);
    let preparation_started = Instant::now();
    let mut certified = None;
    let mut preparation_observations = Vec::new();
    for (index, node) in nodes.iter_mut().enumerate() {
        let started = Instant::now();
        let body = node.http_request("POST", &format!("/api/v1/cluster/topology/operations/{}/prepare", plan.operation_id.get()), None, ceiling)
            .expect("every frozen running process must independently compile and durably certify the admitted candidate");
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        let status: TopologyAdmissionStatus = serde_json::from_str(&body).unwrap();
        assert_eq!(status.phase, TopologyAdmissionPhase::Preparing);
        let preparation = status.preparation.as_ref().unwrap();
        assert_eq!(
            preparation.compatibility,
            plan.compatibility.clone().unwrap()
        );
        assert_eq!(preparation.certificates.len(), index + 1);
        assert_eq!(
            preparation.complete_sequence.is_some(),
            index + 1 == plan.assignment.participants.len()
        );
        assert!(preparation.certificates.iter().all(|certificate| plan
            .assignment
            .participant_incarnation(certificate.participant.node_id)
            == Some(certificate.participant.boot_incarnation)));
        let active: serde_json::Value =
            serde_json::from_str(&node.http_get("/api/v1/cluster/topology").unwrap()).unwrap();
        assert_eq!(active["committed_version"].as_u64(), Some(1));
        assert_eq!(active["locally_active_version"].as_u64(), Some(1));
        preparation_observations.push(serde_json::json!({"node": node.id, "preparation_elapsed_ms": elapsed_ms, "status": status}));
        certified = Some(status);
    }
    let certified = certified.unwrap();
    assert_eq!(
        certified
            .preparation
            .as_ref()
            .unwrap()
            .certificates
            .iter()
            .map(|certificate| certificate.participant)
            .collect::<Vec<_>>(),
        plan.assignment.participants
    );
    assert_eq!(
        runtime
            .block_on(authority.topology_operation_status(plan.operation_id))
            .unwrap(),
        Some(certified.clone())
    );
    std::fs::write(
        evidence_dir.join("topology-participant-preparations.json"),
        serde_json::to_vec_pretty(&preparation_observations).unwrap(),
    )
    .unwrap();
    eprintln!("soak: all {} frozen exact processes durably certified descriptor {} in {:?}; target remains uncommitted", plan.assignment.participants.len(), descriptor.compatibility_sha256, preparation_started.elapsed());
    let started = Instant::now();
    // This is the real authenticated management checkpoint path, including leader routing.
    let response = nodes[0]
        .http_request("POST", "/api/v1/checkpoint", None, ceiling)
        .expect("manual old-topology cut must reach its definitive checkpoint result");
    let response: serde_json::Value = serde_json::from_str(&response).unwrap();
    assert_eq!(response["success"].as_bool(), Some(true), "{response}");
    assert!(
        response["error"].is_null(),
        "cut continuation failed: {response}"
    );
    let checkpoint_id = response["checkpoint_id"].as_u64().unwrap();
    let mut prepared = None;
    wait_for(
        "every frozen process to apply and hold the old-topology cut",
        ceiling,
        || {
            for node in nodes.iter_mut() {
                node.assert_running();
            }
            let status = runtime
                .block_on(authority.topology_operation_status(plan.operation_id))
                .unwrap()
                .unwrap();
            match status.phase {
                TopologyAdmissionPhase::CutPrepared => {
                    prepared = Some(status);
                    true
                }
                TopologyAdmissionPhase::Quiescing => false,
                phase => panic!("old-topology cut unexpectedly left preparation: {phase:?}"),
            }
        },
    );
    let prepared = prepared.unwrap();
    let cut_prepared_elapsed = started.elapsed();
    let root_started = Instant::now();
    let initialization_calls = std::sync::atomic::AtomicUsize::new(0);
    let initialization_counter = &initialization_calls;
    let initialization_config = &probe_config;
    let prepared = runtime
        .block_on(async {
            let proof = authority.load().await.unwrap().unwrap().proof();
            authority
                .stage_topology_migration_root_with_initialization(
                    &proof,
                    &assignments,
                    &processes,
                    &checkpoint_store,
                    prepared.operation_id,
                    &prepared.plan,
                    |_, descriptor| async move {
                        initialization_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let object = descriptor
                            .objects
                            .iter()
                            .find(|o| o.name == "topology_source_probe")
                            .unwrap();
                        let mut source = KafkaSource::new(
                            Arc::new(arrow_schema::Schema::empty()),
                            KafkaSourceConfig::from_config(initialization_config).unwrap(),
                            None,
                        );
                        let checkpoint = source
                            .resolve_initial_position(initialization_config)
                            .await
                            .map_err(|e| TopologyError::Unsupported(e.to_string()))?;
                        Ok(vec![TopologySourceInitialization {
                            name: object.name.clone(),
                            catalog_generation: object.catalog_generation,
                            compatibility_sha256: object.compatibility_sha256.clone(),
                            checkpoint: ConnectorCheckpoint {
                                offsets: checkpoint.durable_offsets(),
                                metadata: checkpoint.metadata().clone(),
                                input_channels: checkpoint
                                    .input_channels()
                                    .map(<[Vec<u8>]>::to_vec),
                                source_assignment_version: checkpoint.assignment_version(),
                            },
                        }])
                    },
                )
                .await
        })
        .expect("exact stateful cut metadata must stage preserved mappings before target commit");
    let root_elapsed_ms = root_started.elapsed().as_secs_f64() * 1000.0;
    let root = runtime
        .block_on(authority.topology_migration_root(prepared.operation_id))
        .unwrap()
        .unwrap();
    let index = runtime
        .block_on(decisions.load_committed_checkpoint(&root.cut.checkpoint))
        .unwrap();
    let manifest_metadata_bytes = index
        .participants
        .iter()
        .map(|p| p.manifest_len)
        .sum::<u64>();
    assert_eq!(root.preserved_objects.len(), descriptor.objects.iter().filter(|o| o.transition == laminar_core::cluster::control::topology::ClusterTopologyObjectTransition::Preserve).count());
    assert_eq!(
        root.future_only_objects,
        [
            "topology_cut_probe",
            "topology_source_probe",
            "topology_source_sink",
            "topology_source_stream"
        ]
    );
    assert_eq!(root.format_version, 2);
    assert_eq!(root.source_initializations.len(), 1);
    let initial = &root.source_initializations[0].checkpoint;
    for (partition, next) in [(0, 2), (1, 3), (2, 0)] {
        assert_eq!(
            initial.offsets[&format!("@laminar.kafka.next.v1:{probe_topic}:{partition}")],
            next.to_string()
        );
    }
    assert_eq!(initial.input_channels.as_ref().unwrap().len(), 3);
    assert!(initial.source_assignment_version.is_none());
    assert!(root
        .subscriptions
        .iter()
        .all(
            |s| s.parent_certificate.stream_generation == s.target_certificate.stream_generation
                && s.parent_certificate.pipeline_identity == descriptor.parent_pipeline
                && s.target_certificate.pipeline_identity == descriptor.target_pipeline
        ));
    let retried = runtime.block_on(async {
        let proof = authority.load().await.unwrap().unwrap().proof();
        authority
            .stage_topology_migration_root(
                &proof,
                &assignments,
                &processes,
                &checkpoint_store,
                prepared.operation_id,
                &prepared.plan,
            )
            .await
            .unwrap()
    });
    assert_eq!(retried, prepared);
    assert_eq!(
        initialization_calls.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    // Exercise production root authorization and bounded checksum loading against all three
    // real held participants. The DB operator-decoding/image ownership path has separate tests;
    // these harness reads grant no actor installation or target activation.
    let mut restore_observations = Vec::new();
    for certificate in &prepared.preparation.as_ref().unwrap().certificates {
        let started = Instant::now();
        let input = runtime
            .block_on(authority.topology_restore_input(
                &assignments,
                &processes,
                prepared.operation_id,
                laminar_core::cluster::control::LocalProcessAuthorityIdentity {
                    participant: certificate.participant,
                    process_term: certificate.process_term,
                },
            ))
            .unwrap();
        let reader = ObjectStoreCheckpointStore::new(Arc::clone(&objects), "")
            .with_participant_id(certificate.participant.node_id)
            .with_key_group_count(checkpoint_store.key_group_count());
        let recovered = runtime
            .block_on(
                laminar_db::RecoveryManager::new(
                    &reader,
                    &descriptor.parent_pipeline,
                    &descriptor.deployment_id,
                    laminar_core::checkpoint::CheckpointScope::Cluster,
                )
                .recover_topology_root(&input, 64 * 1024 * 1024),
            )
            .unwrap();
        assert_eq!(recovered.committed, index);
        assert!(recovered
            .state_frames
            .iter()
            .all(|frame| frame.participant_id == certificate.participant.node_id));
        assert!(!recovered.state_frames.is_empty());
        let payload_bytes = recovered
            .state_frames
            .iter()
            .map(|frame| frame.payload.len())
            .sum::<usize>();
        restore_observations.push(serde_json::json!({ "node": certificate.participant.node_id,
            "root_read_and_state_verification_ms": started.elapsed().as_secs_f64() * 1000.0,
            "owned_vnodes": input.owned_vnodes(), "verified_state_frames": recovered.state_frames.len(),
            "verified_payload_bytes": payload_bytes, "target_installed": false }));
    }
    let validation_started = Instant::now();
    runtime.block_on(async {
        let mut checkpoint =
            laminar_connectors::checkpoint::SourceCheckpoint::with_offsets(initial.offsets.clone());
        for (key, value) in &initial.metadata {
            checkpoint.set_metadata(key, value);
        }
        checkpoint
            .set_input_channels(initial.input_channels.clone().unwrap())
            .unwrap();
        let mut source = KafkaSource::new(
            Arc::new(arrow_schema::Schema::empty()),
            KafkaSourceConfig::from_config(&probe_config).unwrap(),
            None,
        );
        source
            .validate_initial_position(&probe_config, &checkpoint)
            .await
            .unwrap();
    });
    std::fs::write(evidence_dir.join("topology-restore-observations.json"), serde_json::to_vec_pretty(
        &serde_json::json!({ "participants": restore_observations, "sealed_cursor_validation_ms": validation_started.elapsed().as_secs_f64() * 1000.0,
            "new_source_next_offsets": [2, 3, 0], "target_committed": false, "target_installed": false })
    ).unwrap()).unwrap();
    eprintln!("soak: all {} held processes' exact local state frames verified through migration-root authorization; sealed Kafka cursor remains [2,3,0]; target remains uncommitted", restore_observations.len());
    std::fs::write(
        evidence_dir.join("topology-migration-root.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "cut_prepared_elapsed_ms": cut_prepared_elapsed.as_secs_f64() * 1000.0,
            "participant_manifest_metadata_bytes": manifest_metadata_bytes,
            "root_staging_elapsed_ms": root_elapsed_ms, "root": root, "status": prepared,
            "source_initialization_calls": 1, "source_initialization_next_offsets": [2, 3, 0],
        }))
        .unwrap(),
    )
    .unwrap();
    eprintln!("soak: immutable migration root staged in {root_elapsed_ms:.3} ms; preserved {} object mappings and {} subscription sequence vectors; target remains uncommitted", root.preserved_objects.len(), root.subscriptions.len());
    let cut = prepared.cut.as_ref().unwrap();
    assert_eq!(cut.inventory.assignment_fence.as_ref(), Some(assignment));
    assert_eq!(cut.completed_participants, assignment.participants);
    assert_eq!(
        cut.committed.as_ref().unwrap().checkpoint.checkpoint_id,
        checkpoint_id
    );
    assert_eq!(cut.inventory.attempt.checkpoint_id, checkpoint_id);
    assert_eq!(runtime.block_on(catalog.load()).unwrap().unwrap(), parent);
    for node in nodes.iter_mut() {
        let body = node
            .http_get("/api/v1/cluster/topology")
            .expect("held cut must leave topology status readable");
        let status: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(status["committed_version"].as_u64(), Some(1));
        assert!(
            status["locally_active_version"].is_null(),
            "node{} reopened intake after the cut",
            node.id
        );
        let body = node
            .http_get(&format!(
                "/api/v1/cluster/topology/operations/{}",
                prepared.operation_id.get()
            ))
            .unwrap();
        let observed: TopologyAdmissionStatus = serde_json::from_str(&body).unwrap();
        assert_eq!(observed, prepared);
    }
    std::fs::write(
        evidence_dir.join("topology-cut-prepared.json"),
        serde_json::to_vec_pretty(&prepared).unwrap(),
    )
    .unwrap();
    eprintln!("soak: old-topology cut {} prepared in {:?}, bound at authority {}, committed at {}, all {} exact processes held; candidate remains uncommitted",
        checkpoint_id, cut_prepared_elapsed, cut.bound_sequence, cut.committed.as_ref().unwrap().authority_sequence, cut.completed_participants.len());
    prepared
}

pub(super) fn assert_aborted_after_restart(
    checkpoint_url: &str,
    nodes: &mut [Node],
    prepared: &TopologyAdmissionStatus,
    ceiling: Duration,
    evidence_dir: &Path,
) {
    let authority = LeaderLeaseStore::new(
        topology_adoption::objects_for_namespace(checkpoint_url),
        30_000,
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let aborted = runtime
        .block_on(authority.topology_operation_status(prepared.operation_id))
        .unwrap()
        .unwrap();
    assert!(matches!(
        aborted.phase,
        TopologyAdmissionPhase::Aborted {
            reason: TopologyAbortReason::LeaderChanged | TopologyAbortReason::Recovery
        }
    ));
    assert_eq!(aborted.preparation, prepared.preparation);
    assert_eq!(aborted.migration_root, prepared.migration_root);
    assert_eq!(
        aborted.cut, prepared.cut,
        "pre-commit abort rewound the irreversible parent cut"
    );
    let old_checkpoint = prepared
        .cut
        .as_ref()
        .unwrap()
        .committed
        .as_ref()
        .unwrap()
        .checkpoint
        .checkpoint_id;
    let latest = runtime
        .block_on(authority.highest_cluster_committed_outcome())
        .unwrap()
        .unwrap();
    assert!(
        latest.checkpoint_id >= old_checkpoint,
        "restart rewound committed progress"
    );
    wait_for(
        "all restarted processes to report the original aborted cut identity",
        ceiling,
        || {
            nodes.iter_mut().all(|node| {
                node.assert_running();
                node.http_get(&format!(
                    "/api/v1/cluster/topology/operations/{}",
                    prepared.operation_id.get()
                ))
                .is_some_and(|body| {
                    let status: TopologyAdmissionStatus = serde_json::from_str(&body).unwrap();
                    assert_eq!(status, aborted);
                    true
                })
            })
        },
    );
    std::fs::write(
        evidence_dir.join("topology-cut-aborted.json"),
        serde_json::to_vec_pretty(&aborted).unwrap(),
    )
    .unwrap();
    eprintln!("soak: restarted processes retained aborted topology operation {} and parent checkpoint {}; committed catalog remains topology one", prepared.operation_id.get(), old_checkpoint);
}
