//! In-process cluster control for multi-node database tests: a shared in-memory KV, real process
//! and leader leases over one object store, barrier servers, and a loopback shuffle mesh.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use laminar_core::cluster::control::{
    AssignmentSnapshot, ClusterController, ClusterKv, InMemoryKv, LeaderLease, LeaderLeaseOwner,
    LeaderLeaseStore, LeaseDeadline, LeaseOutcome, ProcessLease, ProcessLeaseAuthority,
    ProcessLeaseOutcome,
};
use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use tokio::sync::watch;

pub(crate) const LEASE_TTL: Duration = Duration::from_secs(120);

struct PeerKv {
    node: NodeId,
    shared: Arc<InMemoryKv>,
}

#[async_trait]
impl ClusterKv for PeerKv {
    async fn write(&self, key: &str, value: String) {
        self.shared.seed(self.node, key, value);
    }
    async fn read_from(&self, node: NodeId, key: &str) -> Option<String> {
        self.shared.read_from(node, key).await
    }
    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        self.shared.scan(key).await
    }
}

/// One participant's leased controller and fenced shuffle endpoints.
pub(crate) struct ClusterControl {
    pub controller: Arc<ClusterController>,
    pub sender: Arc<ShuffleSender>,
    pub receiver: Arc<ShuffleReceiver>,
    _membership: watch::Sender<Vec<NodeInfo>>,
}

/// Leased control for every participant of `assignment`; the first participant leads. Keep the
/// returned watch alive for as long as the leader must hold its lease.
pub(crate) async fn prepare_controls(
    assignment: &AssignmentSnapshot,
    vnodes: u32,
    objects: Arc<dyn object_store::ObjectStore>,
) -> (Vec<ClusterControl>, watch::Sender<Option<LeaderLease>>) {
    let leader_node = NodeId(assignment.participants[0].node_id);
    let shared = Arc::new(InMemoryKv::new(leader_node));
    let authority = Arc::new(ProcessLeaseAuthority::new(Arc::clone(&objects), LEASE_TTL).unwrap());
    let leader = Arc::new(LeaderLeaseStore::new(objects, 120_000));
    let members = assignment
        .participants
        .iter()
        .map(|participant| NodeInfo {
            id: NodeId(participant.node_id),
            name: format!("process-{}", participant.node_id),
            rpc_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        })
        .collect::<Vec<_>>();
    let fence = assignment.assignment_fence().unwrap();
    let owners = assignment
        .to_vnode_vec(vnodes)
        .unwrap()
        .iter()
        .map(|owner| owner.0)
        .collect::<Vec<_>>();
    let mut controls = Vec::new();
    let mut leader_watch = None;
    for participant in &assignment.participants {
        let node = NodeId(participant.node_id);
        let (membership, membership_rx) = watch::channel(members.clone());
        let kv: Arc<dyn ClusterKv> = Arc::new(PeerKv {
            node,
            shared: Arc::clone(&shared),
        });
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            node,
            Arc::clone(&kv),
            kv,
            None,
            membership_rx,
            participant.boot_incarnation,
        ));
        let ProcessLeaseOutcome::Acquired(lease) = authority
            .store_for(node)
            .try_acquire(participant.boot_incarnation, 0)
            .await
            .unwrap()
        else {
            panic!("fresh fixture process lease was not acquired");
        };
        controller
            .set_process_lease_authority(Arc::clone(&authority))
            .unwrap();
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(LEASE_TTL)))
            .unwrap();
        controller
            .publish_leased_recovery_incarnation(&lease)
            .await
            .unwrap();
        controller.install_local_leader_proof_provider();
        controller.set_leader_lease_store(Arc::clone(&leader));
        controller.set_active(true);
        controller
            .start_leased_barrier_server("127.0.0.1:0".parse().unwrap(), None, &lease)
            .await
            .unwrap();
        if node == leader_node {
            leader_watch = Some(acquire_leadership(&controller, &lease, &leader).await);
        }
        let sender = Arc::new(ShuffleSender::new(node.0, participant.boot_incarnation));
        let receiver = Arc::new(
            ShuffleReceiver::bind(
                node.0,
                "127.0.0.1:0".parse().unwrap(),
                participant.boot_incarnation,
            )
            .await
            .unwrap(),
        );
        sender
            .bind_process_lease_deadline_pair(
                &receiver,
                controller.process_lease_deadline().unwrap(),
            )
            .unwrap();
        sender.install_assignment_fence(&fence, &owners).unwrap();
        receiver.install_assignment_fence(&fence, &owners).unwrap();
        controls.push(ClusterControl {
            controller,
            sender,
            receiver,
            _membership: membership,
        });
    }
    for control in &controls {
        for other in &controls {
            let other_id = other.controller.instance_id();
            if control.controller.instance_id() != other_id {
                control
                    .sender
                    .register_peer(other_id.0, other.receiver.local_addr());
            }
        }
    }
    (controls, leader_watch.expect("the first participant leads"))
}

async fn acquire_leadership(
    controller: &ClusterController,
    lease: &ProcessLease,
    leader: &LeaderLeaseStore,
) -> watch::Sender<Option<LeaderLease>> {
    let owner = LeaderLeaseOwner {
        node: controller.instance_id(),
        boot: controller.recovery_incarnation(),
        process_term: lease.term,
    };
    let LeaseOutcome::Acquired(grant) = leader.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("fresh fixture leader lease was not acquired");
    };
    let (sender, receiver) = watch::channel(Some(grant));
    controller
        .set_leader_lease_watch(
            receiver,
            owner,
            Arc::new(LeaseDeadline::live_for(LEASE_TTL)),
        )
        .unwrap();
    sender
}
