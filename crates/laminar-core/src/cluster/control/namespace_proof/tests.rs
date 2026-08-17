use std::collections::HashMap;

use async_trait::async_trait;
use parking_lot::Mutex;

use super::*;

#[derive(Clone)]
struct NamespaceProofTestKv {
    local_id: NodeId,
    values: Arc<Mutex<HashMap<(NodeId, String), String>>>,
}

#[async_trait]
impl ClusterKv for NamespaceProofTestKv {
    async fn write(&self, key: &str, value: String) {
        self.values
            .lock()
            .insert((self.local_id, key.to_string()), value);
    }

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        self.values.lock().get(&(who, key.to_string())).cloned()
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        self.values
            .lock()
            .iter()
            .filter(|((_, stored_key), _)| stored_key == key)
            .map(|((node, _), value)| (*node, value.clone()))
            .collect()
    }
}

fn participants() -> [CheckpointParticipant; 2] {
    [
        CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(11),
        },
        CheckpointParticipant {
            node_id: 2,
            boot_incarnation: uuid::Uuid::from_u128(22),
        },
    ]
}

fn controls() -> [Arc<dyn ClusterKv>; 2] {
    let values = Arc::new(Mutex::new(HashMap::new()));
    [
        Arc::new(NamespaceProofTestKv {
            local_id: NodeId(1),
            values: Arc::clone(&values),
        }),
        Arc::new(NamespaceProofTestKv {
            local_id: NodeId(2),
            values,
        }),
    ]
}

async fn run_two_node_proof(
    participants: &[CheckpointParticipant; 2],
    controls: &[Arc<dyn ClusterKv>; 2],
    checkpoint_stores: [Arc<dyn ObjectStore>; 2],
    timeout: Duration,
) -> [Result<VerifiedClusterNamespaces, NamespaceProofError>; 2] {
    let first = prove_shared_object_store_namespaces(
        participants[0],
        participants,
        Arc::clone(&controls[0]),
        Arc::clone(&checkpoint_stores[0]),
        timeout,
    );
    let second = prove_shared_object_store_namespaces(
        participants[1],
        participants,
        Arc::clone(&controls[1]),
        Arc::clone(&checkpoint_stores[1]),
        timeout,
    );
    let (first, second) = tokio::join!(first, second);
    [first, second]
}

async fn marker_count(store: &Arc<dyn ObjectStore>) -> usize {
    let prefix = object_store::path::Path::from("cluster-checkpoint-namespace-proof/v1/");
    let mut entries = store.list(Some(&prefix));
    let mut count = 0;
    while let Some(entry) = entries.next().await {
        entry.unwrap();
        count += 1;
    }
    count
}

#[tokio::test]
async fn proof_retains_bounded_markers_and_exact_handle() {
    let checkpoint: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let participants = participants();
    let controls = controls();
    let results = run_two_node_proof(
        &participants,
        &controls,
        [Arc::clone(&checkpoint), Arc::clone(&checkpoint)],
        Duration::from_secs(2),
    )
    .await;
    assert!(results.iter().all(Result::is_ok));

    let admitted = results[0].as_ref().unwrap();
    assert!(Arc::ptr_eq(&admitted.checkpoint_store(), &checkpoint));
    assert_eq!(admitted.local_participant(), participants[0]);
    for node_id in [1, 2] {
        let marker = checkpoint
            .get(&namespace_proof_path(node_id))
            .await
            .expect("boot marker must remain available to rolling joiners");
        assert!(marker.meta.size <= NAMESPACE_PROOF_MAX_SENTINEL_BYTES);
    }
}

#[tokio::test]
async fn rolling_restart_uses_active_peers_retained_markers() {
    let checkpoint: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let initial = participants();
    let controls = controls();
    let results = run_two_node_proof(
        &initial,
        &controls,
        [Arc::clone(&checkpoint), Arc::clone(&checkpoint)],
        Duration::from_secs(2),
    )
    .await;
    assert!(results.iter().all(Result::is_ok));
    let active_peer_record = controls[1]
        .read_from(NodeId(2), NAMESPACE_PROOF_KEY)
        .await
        .unwrap();

    let restarted = [
        CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(111),
        },
        initial[1],
    ];
    prove_shared_object_store_namespaces(
        restarted[0],
        &restarted,
        Arc::clone(&controls[0]),
        Arc::clone(&checkpoint),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert_eq!(
        controls[1].read_from(NodeId(2), NAMESPACE_PROOF_KEY).await,
        Some(active_peer_record),
        "the active peer must not rerun startup for a rolling joiner"
    );
    assert_eq!(marker_count(&checkpoint).await, 2);
}

#[tokio::test]
async fn proof_rejects_split_checkpoint_namespaces() {
    let checkpoint_a: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let checkpoint_b: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let participants = participants();
    let controls = controls();
    let results = run_two_node_proof(
        &participants,
        &controls,
        [checkpoint_a, checkpoint_b],
        Duration::from_millis(250),
    )
    .await;
    assert!(results.iter().all(Result::is_err));
}
