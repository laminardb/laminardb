//! Real old-graph cut and pre-commit abort. Candidate admission uses the core API because SQL
//! migration submission remains guarded. This never installs or activates the reserved candidate.

use super::{topology_adoption, wait_for, Node};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use laminar_core::cluster::control::{
    AssignmentSnapshotStore, CatalogManifestEntry, CatalogManifestStore, CatalogObjectKind,
    CheckpointAssignmentFence, LeaderLeaseStore, LegacyTopologyBaseline, TopologyAbortReason,
    TopologyAdmissionPhase, TopologyAdmissionPlan, TopologyAdmissionStatus, TopologyError,
    TOPOLOGY_PROTOCOL_VERSION,
};

pub(super) fn prepare_old_cut(
    checkpoint_url: &str,
    nodes: &mut [Node],
    baseline: &LegacyTopologyBaseline,
    assignment: &CheckpointAssignmentFence,
    ceiling: Duration,
    evidence_dir: &Path,
) -> TopologyAdmissionStatus {
    let objects = topology_adoption::objects_for_namespace(checkpoint_url);
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 30_000));
    let assignments = AssignmentSnapshotStore::new(objects);
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
    // This staged definition only gives the request a changed payload. It is never compiled,
    // installed or submitted as a supported SQL migration in this control-path scenario.
    candidate.entries.push(CatalogManifestEntry {
        canonical_name: "topology_cut_probe".into(),
        kind: CatalogObjectKind::Stream,
        catalog_generation: 1,
        ddl: format!(
            "CREATE STREAM topology_cut_probe AS SELECT * FROM \"{}\"",
            source.canonical_name.replace('"', "\"\"")
        ),
    });
    let plan = TopologyAdmissionPlan {
        protocol_version: TOPOLOGY_PROTOCOL_VERSION,
        operation_id: uuid::Uuid::new_v4().try_into().unwrap(),
        expected_parent: baseline.topology_version,
        parent_manifest: baseline.manifest.clone(),
        target_manifest: candidate.reference().unwrap(),
        assignment: assignment.clone(),
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
        checkpoint_id, started.elapsed(), cut.bound_sequence, cut.committed.as_ref().unwrap().authority_sequence, cut.completed_participants.len());
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
