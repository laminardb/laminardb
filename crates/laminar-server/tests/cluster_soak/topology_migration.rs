//! Public migration inside the existing independent stateful Kafka/S3 soak.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use laminar_core::cluster::control::topology::ClusterTopologyObjectTransition;
use laminar_core::cluster::control::{
    CatalogManifestStore, CheckpointDecisionStore, LeaderLeaseStore, LegacyTopologyBaseline,
    RecoverPhase, RecoveryAnnouncement, TopologyAdmissionPhase, TopologyAdmissionStatus,
    TopologyCatalogState, TopologyOperationId, TopologyVersion,
};
use object_store::ObjectStoreExt;

use super::{topology_adoption, wait_for, DurableCheckpointStatus, KafkaOutputOracle, Node};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

pub(super) fn adopt_inventory(namespace: &str, nodes: &mut [Node]) -> LegacyTopologyBaseline {
    let objects = topology_adoption::objects_for_namespace(namespace);
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 30_000));
    let catalog = CatalogManifestStore::new(Arc::clone(&authority));
    let runtime = runtime();
    let baseline = runtime.block_on(async {
        let TopologyCatalogState::LegacySealed { manifest } =
            catalog.topology_state().await.unwrap()
        else {
            panic!("fresh soak must adopt its exact original sealed inventory");
        };
        let path = object_store::path::Path::from(format!(
            "control/catalog-manifest/v1/{}.json",
            manifest.sha256
        ));
        let before = objects.get(&path).await.unwrap().bytes().await.unwrap();
        let deployment = CheckpointDecisionStore::new(Arc::clone(&objects))
            .load_deployment_id()
            .await
            .unwrap()
            .unwrap();
        let request = laminar_db::ClusterTopologyAdoptionRequest {
            operation_id: uuid::Uuid::new_v4().try_into().unwrap(),
            expected_manifest: manifest.clone(),
            expected_deployment_id: deployment.clone(),
            coordinated_upgrade_complete: true,
        };
        let body = serde_json::to_string(&request).unwrap();
        let response = nodes[0]
            .http_request(
                "POST",
                "/api/v1/cluster/topology/adopt",
                Some(&body),
                Duration::from_secs(45),
            )
            .expect("public explicit adoption must return its receipt");
        let baseline: LegacyTopologyBaseline = serde_json::from_str(&response).unwrap();
        assert_eq!(baseline.manifest, manifest);
        assert_eq!(baseline.deployment_id, deployment);
        let retry: LegacyTopologyBaseline = serde_json::from_str(
            &nodes[1]
                .http_request(
                    "POST",
                    "/api/v1/cluster/topology/adopt",
                    Some(&body),
                    Duration::from_secs(45),
                )
                .unwrap(),
        )
        .unwrap();
        assert_eq!(baseline, retry);
        assert_eq!(
            objects.get(&path).await.unwrap().bytes().await.unwrap(),
            before
        );
        baseline
    });
    eprintln!(
        "soak: public exact-inventory adoption {} at sequence {}",
        baseline.operation_id.get(),
        baseline.authority_sequence
    );
    baseline
}

pub(super) struct MigrationProbe {
    objects: Arc<dyn object_store::ObjectStore>,
    authority: Arc<LeaderLeaseStore>,
    runtime: tokio::runtime::Runtime,
    brokers: String,
    input_topic: String,
    output: KafkaOutputOracle,
    expected: BTreeSet<(u64, u64)>,
    input_started: BTreeMap<(u64, u64), Instant>,
    visible_latencies_ms: Vec<f64>,
    operations: Vec<TopologyOperationId>,
    evidence_dir: std::path::PathBuf,
}

impl MigrationProbe {
    fn recovery_release(&self) -> Option<(u64, RecoveryAnnouncement)> {
        use object_store::path::Path as ObjectPath;
        use sha2::{Digest as _, Sha256};

        self.runtime.block_on(async {
            for _ in 0..4 {
                let pointer_bytes = self.objects.get(&ObjectPath::from("control/leader-lease-head/v1.json"))
                    .await.unwrap().bytes().await.unwrap();
                assert!(pointer_bytes.len() <= 128);
                let pointer: serde_json::Value = serde_json::from_slice(&pointer_bytes).unwrap();
                let sequence = pointer["sequence"].as_u64().unwrap();
                let record = self.objects.get(&ObjectPath::from(format!("control/leader-lease/v{sequence:016}.json")))
                    .await.unwrap().bytes().await.unwrap();
                assert!(record.len() <= 256 * 1024);
                let record: serde_json::Value = serde_json::from_slice(&record).unwrap();
                let link = &record["recovery_release_head"];
                if link.is_null() {
                    return None;
                }
                let reference = &link["terminal"];
                let generation = reference["release"]["generation"].as_u64().unwrap();
                let digest = reference["sha256"].as_str().unwrap();
                let terminal = self.objects.get(&ObjectPath::from(format!(
                    "control/recovery-release-terminals/v2/generation={generation:020}/sha256={digest}.json"
                ))).await.unwrap().bytes().await.unwrap();
                assert_eq!(terminal.len() as u64, reference["encoded_len"].as_u64().unwrap());
                assert_eq!(format!("{:x}", Sha256::digest(&terminal)), digest);
                let terminal: RecoveryAnnouncement = serde_json::from_slice(&terminal).unwrap();
                assert!(matches!(terminal.phase, RecoverPhase::ReleaseCommitted { .. }));
                assert_eq!(terminal.round.id.generation, generation);
                let next = self.objects.get(&ObjectPath::from("control/leader-lease-head/v1.json"))
                    .await.unwrap().bytes().await.unwrap();
                if next == pointer_bytes {
                    return Some((link["sequence"].as_u64().unwrap(), terminal));
                }
                assert!(next.len() <= 128);
                let next: serde_json::Value = serde_json::from_slice(&next).unwrap();
                let next_sequence = next["sequence"].as_u64().unwrap();
                let next_record = self.objects.get(&ObjectPath::from(format!("control/leader-lease/v{next_sequence:016}.json")))
                    .await.unwrap().bytes().await.unwrap();
                assert!(next_record.len() <= 256 * 1024);
                let next_record: serde_json::Value = serde_json::from_slice(&next_record).unwrap();
                if next_record["recovery_release_head"] == *link {
                    return Some((link["sequence"].as_u64().unwrap(), terminal));
                }
            }
            panic!("authority changed during every bounded recovery Release observation");
        })
    }

    fn status(&self, operation: TopologyOperationId) -> TopologyAdmissionStatus {
        self.runtime
            .block_on(self.authority.topology_operation_status(operation))
            .unwrap()
            .unwrap()
    }

    fn submit(
        &self,
        node: &Node,
        request: &laminar_db::ClusterTopologyRequest,
    ) -> TopologyAdmissionStatus {
        let body = serde_json::to_string(request).unwrap();
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut admitted = None;
        wait_for(
            "durable public topology admission receipt",
            Duration::from_secs(120),
            || {
                let response = node.http_request(
                    "POST",
                    "/api/v1/cluster/topology/operations",
                    Some(&body),
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_secs(45)),
                );
                // A disconnected request may have committed or may still be unreserved. Read the
                // exact UUID and retry the same payload while checkpoints contend for admission.
                admitted = response
                    .map(|body| serde_json::from_str(&body).unwrap())
                    .or_else(|| {
                        self.runtime
                            .block_on(
                                self.authority
                                    .topology_operation_status(request.operation_id),
                            )
                            .unwrap()
                    });
                admitted.is_some()
            },
        );
        let operation = admitted.unwrap();
        assert_eq!(operation.operation_id, request.operation_id);
        operation
    }

    fn wait_active(
        &self,
        nodes: &mut [Node],
        operation: TopologyOperationId,
        version: u64,
        ceiling: Duration,
        started: Instant,
        trigger: &str,
    ) {
        let mut observations = Vec::new();
        let mut last_sequence = None;
        wait_for("full-roster public topology Release", ceiling, || {
            for node in nodes.iter_mut() {
                node.assert_running();
            }
            let status = self.status(operation);
            if last_sequence != Some(status.status_sequence) {
                observations.push(serde_json::json!({
                    "elapsed_since_submission_or_restart_ms": started.elapsed().as_millis(),
                    "status_sequence": status.status_sequence,
                    "phase": status.phase,
                }));
                last_sequence = Some(status.status_sequence);
            }
            assert!(
                !matches!(status.phase, TopologyAdmissionPhase::Aborted { .. }),
                "public migration aborted: {status:?}"
            );
            status.phase == TopologyAdmissionPhase::Active
                && nodes.iter().all(|node| {
                    node.http_get("/api/v1/cluster/topology")
                        .is_some_and(|body| {
                            let value: serde_json::Value = serde_json::from_str(&body).unwrap();
                            value["committed_version"].as_u64() == Some(version)
                                && value["locally_active_version"].as_u64() == Some(version)
                                && node.is_ready()
                        })
                })
        });
        let status = self.status(operation);
        assert_eq!(
            status.activation.as_ref().unwrap().processes.len(),
            nodes.len()
        );
        assert!(status.activation.as_ref().unwrap().installation_complete());
        std::fs::write(
            self.evidence_dir
                .join(format!("public-topology-{version}-{trigger}-phase-observations.json")),
            serde_json::to_vec_pretty(&serde_json::json!({
                "scope": "First observed durable status after public submission or restart; polling and IO delay are included. Unobserved transitions have no inferred duration.",
                "operation_id": operation,
                "trigger": trigger,
                "observations": observations,
            })).unwrap(),
        ).unwrap();
        std::fs::write(
            self.evidence_dir
                .join(format!("public-topology-{version}-active.json")),
            serde_json::to_vec_pretty(&status).unwrap(),
        )
        .unwrap();
    }

    fn produce(&mut self, first: u64) -> Instant {
        let started = Instant::now();
        self.runtime.block_on(async {
            use rdkafka::producer::{FutureProducer, FutureRecord};
            let producer: FutureProducer = rdkafka::ClientConfig::new()
                .set("bootstrap.servers", &self.brokers)
                .set("message.timeout.ms", "5000")
                .create()
                .unwrap();
            for partition in 0..3 {
                let id = first + u64::try_from(partition).unwrap();
                let payload = format!(r#"{{"id":{id},"value":{id}}}"#);
                producer
                    .send(
                        FutureRecord::to(&self.input_topic)
                            .partition(partition)
                            .key("topology-oracle")
                            .payload(&payload),
                        Duration::from_secs(5),
                    )
                    .await
                    .unwrap();
                self.expected.insert((id, id));
                self.input_started.insert((id, id), started);
            }
        });
        started
    }

    fn wait_output(&mut self, nodes: &mut [Node], ceiling: Duration) {
        let mut boundary = vec![0; 3];
        wait_for("every deterministic new-pipeline output", ceiling, || {
            for node in nodes.iter_mut() {
                node.assert_running();
            }
            self.output.drain(&self.expected, &mut boundary);
            let observed = Instant::now();
            for pair in &self.output.seen {
                if let Some(sent) = self.input_started.remove(pair) {
                    self.visible_latencies_ms
                        .push(observed.duration_since(sent).as_secs_f64() * 1000.0);
                }
            }
            self.output.is_complete(&self.expected)
        });
    }

    fn visible_latency_evidence(&self) -> serde_json::Value {
        let mut sorted = self.visible_latencies_ms.clone();
        sorted.sort_by(f64::total_cmp);
        let percentile = |percentage: usize| {
            sorted
                .get((sorted.len() * percentage).div_ceil(100).saturating_sub(1))
                .copied()
        };
        serde_json::json!({
            "scope": "Small deterministic new-pipeline oracle; includes producer creation, sending, cut hold, recovery and consumer observation. Separate from the existing steady-state soak latency sample.",
            "samples_ms": self.visible_latencies_ms,
            "sample_count": sorted.len(),
            "quantile_method": "nearest rank",
            "p50_ms": percentile(50), "p95_ms": percentile(95), "p99_ms": percentile(99),
        })
    }

    fn checkpoint(&self, ceiling: Duration, version: u64) -> DurableCheckpointStatus {
        let mut checkpoint = None;
        wait_for(
            "checkpoint under the exact committed target",
            ceiling,
            || {
                let Some(outcome) = self
                    .runtime
                    .block_on(self.authority.highest_cluster_committed_outcome())
                    .unwrap()
                else {
                    return false;
                };
                let index =
                    self.runtime
                        .block_on(self.authority.load_committed_checkpoint(
                            outcome.committed_checkpoint.as_ref().unwrap(),
                        ))
                        .unwrap();
                let (_, _, _, descriptor) = self
                    .runtime
                    .block_on(
                        self.authority
                            .topology_preparation_input(*self.operations.last().unwrap()),
                    )
                    .unwrap();
                if descriptor.target_version.get() != version
                    || index.pipeline_identity != descriptor.target_pipeline
                {
                    return false;
                }
                assert!(index.source_offsets.contains_key("topology_live_source"));
                let progress = &index.source_offsets["topology_live_source"].offsets;
                let required_next = 1 + self.expected.len() / 3;
                if !(0..3).all(|partition| {
                    let consumed = progress
                        .get(&format!("{}:{partition}", self.input_topic))
                        .and_then(|value| value.parse::<usize>().ok())
                        .and_then(|value| value.checked_add(1));
                    let initial = progress
                        .get(&format!(
                            "@laminar.kafka.next.v1:{}:{partition}",
                            self.input_topic
                        ))
                        .and_then(|value| value.parse::<usize>().ok());
                    consumed
                        .or(initial)
                        .is_some_and(|next| next >= required_next)
                }) {
                    return false;
                }
                checkpoint = Some(DurableCheckpointStatus {
                    checkpoint_id: index.checkpoint_id,
                    epoch: index.epoch,
                });
                true
            },
        );
        checkpoint.unwrap()
    }

    pub(super) fn recover_replaced_process(
        &self,
        nodes: &mut [Node],
        victim: usize,
        ceiling: Duration,
        started: Instant,
        round: u32,
    ) {
        let operation = *self.operations.last().unwrap();
        let status = self.status(operation);
        let version = status.commit.unwrap().topology_version.get();
        let before = status.activation.unwrap();
        let prior_recovery = self.recovery_release();
        let executable = nodes[victim].verify_executable_for_spawn();
        nodes[victim].spawn(executable);
        self.wait_active(
            nodes,
            operation,
            version,
            ceiling,
            started,
            &format!("kill-{round}"),
        );
        wait_for("full roster after process replacement", ceiling, || {
            super::has_full_membership(nodes)
        });
        let after = self.status(operation).activation.unwrap();
        assert_eq!(
            after, before,
            "recovery rewrote the original immutable Release"
        );
        let recovered = self
            .recovery_release()
            .expect("ready target requires a durable recovery Release");
        assert_ne!(
            prior_recovery.as_ref().map(|(_, release)| release.round.id),
            Some(recovered.1.round.id)
        );
        let binding = recovered.1.round.topology_binding().unwrap();
        assert_eq!(
            binding.commit(),
            self.status(operation).commit.as_ref().unwrap()
        );
        assert_eq!(binding.processes().len(), nodes.len());
        assert_eq!(
            recovered
                .1
                .round
                .assignment_fence
                .participants
                .iter()
                .map(|participant| participant.node_id)
                .collect::<Vec<_>>(),
            before
                .assignment
                .participants
                .iter()
                .map(|participant| participant.node_id)
                .collect::<Vec<_>>()
        );
        let report = serde_json::json!({
            "scope": "Replacement of the killed process with the complete original owner map; no survivor resharding is certified.",
            "victim": victim, "round": round,
            "kill_to_full_target_release_ms": started.elapsed().as_millis(),
            "original_activation": before, "prior_recovery_release": prior_recovery,
            "recovered_release": recovered,
        });
        std::fs::write(
            self.evidence_dir
                .join(format!("public-topology-kill-{round}.json")),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
    }

    pub(super) fn restart_all(&mut self, nodes: &mut [Node], ceiling: Duration) {
        let operation = *self.operations.last().unwrap();
        let version = self
            .status(operation)
            .commit
            .unwrap()
            .topology_version
            .get();
        let before = self.checkpoint(ceiling, version);
        let started = Instant::now();
        for node in nodes.iter_mut() {
            node.disarm_checkpoint_kill();
            node.kill9();
        }
        for node in nodes.iter_mut() {
            let executable = node.verify_executable_for_spawn();
            node.spawn(executable);
        }
        self.wait_active(nodes, operation, version, ceiling, started, "full-restart");
        wait_for(
            "full membership after committed-target restart",
            ceiling,
            || super::has_full_membership(nodes),
        );
        let sent = self.produce(400);
        for id in 400..403 {
            // The reset aggregate must retain its first post-reset values across both recoveries.
            self.expected.insert((id, id * 2));
            self.input_started.remove(&(id, id));
            self.input_started.insert((id, id * 2), sent);
        }
        self.wait_output(nodes, ceiling);
        let after = self.checkpoint(ceiling, version);
        assert!(after.checkpoint_id > before.checkpoint_id && after.epoch > before.epoch);
        let report = serde_json::json!({"full_restart_to_output_ms": started.elapsed().as_millis(),
            "expected_new_pipeline_pairs": self.expected, "observed_new_pipeline_pairs": self.output.seen,
            "allowed_at_least_once_duplicates": self.output.duplicates,
            "consumer_visible_latency": self.visible_latency_evidence()});
        std::fs::write(
            self.evidence_dir.join("public-topology-restart.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
        eprintln!("soak: reconstructed topology {version} using original bootstrap, retained epochs {} -> {}, new pipeline {} exact logical pairs, {} allowed replay duplicates, freshness {:?}",
            before.epoch, after.epoch, self.expected.len(), self.output.duplicates, started.elapsed());
    }
}

pub(super) fn exercise(
    namespace: &str,
    brokers: &str,
    nodes: &mut [Node],
    ceiling: Duration,
    evidence_dir: &Path,
) -> (MigrationProbe, DurableCheckpointStatus) {
    let id = uuid::Uuid::new_v4();
    let input_topic = format!("topology-live-input-{id}");
    let output_topic = format!("topology-live-output-{id}");
    super::kafka_create_topic(brokers, &input_topic, 3);
    super::kafka_create_topic(brokers, &output_topic, 3);
    let mut probe = MigrationProbe {
        objects: topology_adoption::objects_for_namespace(namespace),
        authority: Arc::new(LeaderLeaseStore::new(
            topology_adoption::objects_for_namespace(namespace),
            30_000,
        )),
        runtime: runtime(),
        brokers: brokers.into(),
        input_topic,
        output: KafkaOutputOracle::new(brokers, &output_topic, 3),
        expected: BTreeSet::new(),
        input_started: BTreeMap::new(),
        visible_latencies_ms: Vec::new(),
        operations: Vec::new(),
        evidence_dir: evidence_dir.to_owned(),
    };
    // Historical records are deliberately excluded by once-resolved latest activation.
    probe.produce(0);
    probe.expected.clear();
    probe.input_started.clear();
    let request = laminar_db::ClusterTopologyRequest {
        operation_id: uuid::Uuid::new_v4().try_into().unwrap(), expected_parent_version: TopologyVersion::LEGACY_BASELINE,
        statements: vec![
            format!("CREATE SOURCE topology_live_source (id BIGINT NOT NULL, value BIGINT NOT NULL) FROM KAFKA ('bootstrap.servers' = '{brokers}', 'group.id' = 'topology-live-{id}', 'topic' = '{}', 'startup.mode' = 'latest')", probe.input_topic),
            "CREATE STREAM topology_live_stream AS SELECT id AS left_id, value AS right_id FROM topology_live_source".into(),
            format!("CREATE SINK topology_live_sink FROM topology_live_stream INTO KAFKA ('bootstrap.servers' = '{brokers}', 'topic' = '{output_topic}')"),
        ],
    };
    let started = Instant::now();
    probe.submit(&nodes[1], &request);
    probe.operations.push(request.operation_id);
    probe.wait_active(
        nodes,
        request.operation_id,
        2,
        ceiling,
        started,
        "submission",
    );
    let activation = started.elapsed();
    let root = probe
        .runtime
        .block_on(
            probe
                .authority
                .topology_migration_root(request.operation_id),
        )
        .unwrap()
        .unwrap();
    for partition in 0..3 {
        assert_eq!(
            root.source_initializations[0].checkpoint.offsets
                [&format!("@laminar.kafka.next.v1:{}:{partition}", probe.input_topic)],
            "1"
        );
    }
    probe.produce(100);
    probe.wait_output(nodes, ceiling);
    let first = probe.checkpoint(ceiling, 2);
    assert_eq!(
        probe.submit(&nodes[2], &request).phase,
        TopologyAdmissionPhase::Active
    );

    let sql = "CREATE STREAM topology_live_downstream AS SELECT join_key, match_count, max_right_id FROM soak_join_aggregate";
    let body = serde_json::json!({"sql":sql}).to_string();
    let downstream_started = Instant::now();
    let response = nodes[0]
        .http_request("POST", "/api/v1/sql", Some(&body), Duration::from_secs(45))
        .unwrap();
    let response: serde_json::Value = serde_json::from_str(&response).unwrap();
    assert_eq!(response["result_type"], "TOPOLOGY MIGRATION");
    let admitted: TopologyAdmissionStatus =
        serde_json::from_value(response["topology_operation"].clone()).unwrap();
    probe.operations.push(admitted.operation_id);
    // Reuse the existing checkpoint gate to pause at a proven fully prepared parent checkpoint.
    // Arm after admission so ordinary checkpoints cannot take this gate ahead of the migration.
    for node in nodes.iter() {
        node.arm_checkpoint_kill("leader");
    }
    wait_for("migration's exact old-cut checkpoint gate", ceiling, || {
        let status = probe.status(admitted.operation_id);
        assert!(
            !matches!(status.phase, TopologyAdmissionPhase::Aborted { .. }),
            "{status:?}"
        );
        status.cut.as_ref().is_some_and(|cut| {
            nodes.iter().any(|node| {
                node.checkpoint_gate_path.as_ref().is_some_and(|path| {
                    std::fs::read_to_string(path.with_extension("ready"))
                        .ok()
                        .is_some_and(|value| {
                            value
                                .split_ascii_whitespace()
                                .nth(1)
                                .and_then(|value| value.parse::<u64>().ok())
                                == Some(cut.inventory.attempt.checkpoint_id)
                        })
                })
            })
        })
    });
    let input_started = probe.produce(200);
    let held = Instant::now();
    while held.elapsed() < Duration::from_secs(2) {
        let mut boundary = vec![0; 3];
        probe.output.drain(&probe.expected, &mut boundary);
        assert!(
            !probe.output.seen.iter().any(|(left, _)| *left >= 200),
            "paused intake exposed post-cut input"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
    let explicit_hold = held.elapsed();
    for node in nodes.iter() {
        node.disarm_checkpoint_kill();
    }
    probe.wait_active(
        nodes,
        admitted.operation_id,
        3,
        ceiling,
        downstream_started,
        "submission",
    );
    probe.wait_output(nodes, ceiling);
    let visible = input_started.elapsed();
    assert!(visible >= Duration::from_secs(2));
    let second = probe.checkpoint(ceiling, 3);
    assert!(second.checkpoint_id > first.checkpoint_id && second.epoch > first.epoch);
    let report = serde_json::json!({"independent_activation_ms": activation.as_millis(),
        "explicit_test_cut_hold_ms": explicit_hold.as_millis(),
        "consumer_visible_latency_including_cut_pause_ms": visible.as_millis(),
        "new_pipeline_expected_pairs": probe.expected, "new_pipeline_observed_pairs": probe.output.seen,
        "target_checkpoint_epochs": [first.epoch, second.epoch], "root_encoded_len": probe.status(request.operation_id).migration_root.unwrap().root.encoded_len,
        "consumer_visible_latency": probe.visible_latency_evidence()});
    std::fs::write(
        evidence_dir.join("public-topology-observations.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("soak: public topology 1 -> 2 -> 3, independent latest source and downstream existing aggregate, exact pairs {}, target epochs {} -> {}, additive activation {:?}, consumer-visible {:?} including explicit two-second cut hold",
        probe.expected.len(), first.epoch, second.epoch, activation, visible);
    let reset_checkpoint = exercise_replacement_and_reset(&mut probe, nodes, &request, ceiling);
    assert!(reset_checkpoint.epoch > second.epoch);
    (probe, reset_checkpoint)
}

fn exercise_replacement_and_reset(
    probe: &mut MigrationProbe,
    nodes: &mut [Node],
    original: &laminar_db::ClusterTopologyRequest,
    ceiling: Duration,
) -> DurableCheckpointStatus {
    let replacement = laminar_db::ClusterTopologyRequest {
        operation_id: uuid::Uuid::new_v4().try_into().unwrap(),
        expected_parent_version: TopologyVersion::new(3).unwrap(),
        statements: vec![
            "DROP SINK topology_live_sink".into(),
            "CREATE OR REPLACE STREAM topology_live_stream AS SELECT id AS left_id, value AS right_id FROM topology_live_source WHERE id > 0".into(),
            original.statements[2].clone(),
            "CREATE STREAM topology_live_state AS SELECT id, SUM(value) AS total FROM topology_live_source GROUP BY id".into(),
        ],
    };
    let started = Instant::now();
    probe.submit(&nodes[0], &replacement);
    probe.operations.push(replacement.operation_id);
    probe.wait_active(
        nodes,
        replacement.operation_id,
        4,
        ceiling,
        started,
        "replacement",
    );
    let (_, _, _, descriptor) = probe
        .runtime
        .block_on(
            probe
                .authority
                .topology_preparation_input(replacement.operation_id),
        )
        .unwrap();
    let stream = descriptor
        .objects
        .iter()
        .find(|object| object.name == "topology_live_stream")
        .unwrap();
    assert_eq!(stream.catalog_generation, 1);
    assert_eq!(stream.transition, ClusterTopologyObjectTransition::Preserve);
    probe.produce(300);
    probe.wait_output(nodes, ceiling);
    let replaced_checkpoint = probe.checkpoint(ceiling, 4);

    let reset = laminar_db::ClusterTopologyRequest {
        operation_id: uuid::Uuid::new_v4().try_into().unwrap(),
        expected_parent_version: TopologyVersion::new(4).unwrap(),
        statements: vec![
            "DROP SINK topology_live_sink".into(),
            "DROP STREAM topology_live_state".into(),
            "DROP STREAM topology_live_stream".into(),
            "DROP SOURCE topology_live_source".into(),
            original.statements[0].replace("value BIGINT NOT NULL)", "value BIGINT NOT NULL, extra BIGINT)")
                .replace("'group.id' = 'topology-live-", "'group.id' = 'topology-reset-"),
            "CREATE STREAM topology_live_stream AS SELECT value AS left_id, SUM(id) AS right_id FROM topology_live_source GROUP BY value".into(),
            "CREATE STREAM topology_live_state AS SELECT value, COUNT(*) AS total FROM topology_live_source GROUP BY value".into(),
            original.statements[2].clone(),
        ],
    };
    let started = Instant::now();
    probe.submit(&nodes[1], &reset);
    probe.operations.push(reset.operation_id);
    probe.wait_active(nodes, reset.operation_id, 5, ceiling, started, "reset");
    let root = probe
        .runtime
        .block_on(probe.authority.topology_migration_root(reset.operation_id))
        .unwrap()
        .unwrap();
    let initialized = root
        .source_initializations
        .iter()
        .find(|source| source.name == "topology_live_source")
        .unwrap();
    assert_eq!(initialized.catalog_generation, 2);
    for partition in 0..3 {
        assert_eq!(
            initialized.checkpoint.offsets
                [&format!("@laminar.kafka.next.v1:{}:{partition}", probe.input_topic)],
            "4"
        );
    }
    assert!(!root
        .preserved_objects
        .iter()
        .any(|object| object.name.starts_with("topology_live_")
            && object.name != "topology_live_downstream"));
    let (_, _, _, reset_descriptor) = probe
        .runtime
        .block_on(
            probe
                .authority
                .topology_preparation_input(reset.operation_id),
        )
        .unwrap();
    for name in [
        "topology_live_source",
        "topology_live_stream",
        "topology_live_state",
        "topology_live_sink",
    ] {
        let generations = reset_descriptor
            .objects
            .iter()
            .filter(|object| object.name == name)
            .map(|object| (object.catalog_generation, object.transition))
            .collect::<Vec<_>>();
        let parent_generation = if name == "topology_live_sink" { 2 } else { 1 };
        assert_eq!(
            generations,
            vec![
                (parent_generation, ClusterTopologyObjectTransition::Remove),
                (
                    parent_generation + 1,
                    ClusterTopologyObjectTransition::AddFutureOnly
                ),
            ]
        );
    }
    probe.produce(400);
    probe.wait_output(nodes, ceiling);
    let reset_checkpoint = probe.checkpoint(ceiling, 5);
    assert!(reset_checkpoint.epoch > replaced_checkpoint.epoch);
    std::fs::write(
        probe.evidence_dir.join("public-topology-reset.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "replacement_descriptor": descriptor,
            "reset_descriptor": reset_descriptor,
            "reset_root": root,
            "target_checkpoint_epochs": [replaced_checkpoint.epoch, reset_checkpoint.epoch],
            "expected_pairs": probe.expected, "observed_pairs": probe.output.seen,
            "consumer_visible_latency": probe.visible_latency_evidence(),
        }))
        .unwrap(),
    )
    .unwrap();
    eprintln!("soak: compatible replacement at topology 4; explicit source/schema/key/state reset at topology 5; original stateful oracle preserved; {} exact logical pairs", probe.expected.len());
    reset_checkpoint
}
