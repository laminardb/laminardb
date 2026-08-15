use super::*;
use crate::cluster::control::barrier::InMemoryKv;
#[cfg(feature = "cluster")]
use crate::cluster::control::barrier::{Phase, ANNOUNCEMENT_KEY, BARRIER_ADDR_KEY};
use crate::cluster::discovery::{NodeMetadata, NodeState};

struct FailedWriteKv;

#[async_trait::async_trait]
impl ClusterKv for FailedWriteKv {
    async fn write(&self, _key: &str, _value: String) {}

    async fn write_checked(&self, _key: &str, _value: String) -> Result<(), String> {
        Err("injected durable write failure".into())
    }

    async fn read_from(&self, _who: NodeId, _key: &str) -> Option<String> {
        None
    }

    async fn scan(&self, _key: &str) -> Vec<(NodeId, String)> {
        Vec::new()
    }
}

#[cfg(feature = "cluster")]
struct PendingAnnouncementReadKv;

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl ClusterKv for PendingAnnouncementReadKv {
    async fn write(&self, _key: &str, _value: String) {}

    async fn read_from(&self, _who: NodeId, _key: &str) -> Option<String> {
        None
    }

    async fn read_from_checked(&self, _who: NodeId, key: &str) -> Result<Option<String>, String> {
        if key == ANNOUNCEMENT_KEY {
            return std::future::pending::<Result<Option<String>, String>>().await;
        }
        Ok(None)
    }

    async fn scan(&self, _key: &str) -> Vec<(NodeId, String)> {
        Vec::new()
    }
}

struct DelayedRecoveryKv {
    inner: InMemoryKv,
    block_next_recovery_write: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AuthorityIoGateOperation {
    Get,
    Put,
}

struct AuthorityIoGateStore {
    inner: Arc<dyn object_store::ObjectStore>,
    operation: AuthorityIoGateOperation,
    armed: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

impl AuthorityIoGateStore {
    fn new(inner: Arc<dyn object_store::ObjectStore>, operation: AuthorityIoGateOperation) -> Self {
        Self {
            inner,
            operation,
            armed: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        }
    }

    fn arm(&self) {
        assert!(
            !self.armed.swap(true, Ordering::AcqRel),
            "authority I/O gate is already armed"
        );
    }

    async fn wait_until_blocked(&self) {
        self.entered.acquire().await.unwrap().forget();
    }

    fn release_blocked_operation(&self) {
        self.release.add_permits(1);
    }

    async fn block_after(
        &self,
        operation: AuthorityIoGateOperation,
        location: &object_store::path::Path,
    ) -> object_store::Result<()> {
        if self.operation == operation
            && location.as_ref().starts_with("control/leader-lease/")
            && self.armed.swap(false, Ordering::AcqRel)
        {
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "AuthorityIoGateStore",
                    source: Box::new(error),
                })?
                .forget();
        }
        Ok(())
    }
}

impl std::fmt::Debug for AuthorityIoGateStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorityIoGateStore")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for AuthorityIoGateStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthorityIoGateStore")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for AuthorityIoGateStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let result = self.inner.put_opts(location, payload, options).await;
        if result.is_ok() {
            self.block_after(AuthorityIoGateOperation::Put, location)
                .await?;
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let result = self.inner.get_opts(location, options).await;
        if result.is_ok() {
            self.block_after(AuthorityIoGateOperation::Get, location)
                .await?;
        }
        result
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

struct FaultyReadyReadKv {
    inner: InMemoryKv,
    remaining_failures: std::sync::atomic::AtomicUsize,
}

impl FaultyReadyReadKv {
    fn new(local_id: NodeId) -> Self {
        Self {
            inner: InMemoryKv::new(local_id),
            remaining_failures: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn fail_next_ready_reads(&self, failures: usize) {
        self.remaining_failures.store(failures, Ordering::Release);
    }

    fn should_fail_ready_read(&self) -> bool {
        self.remaining_failures
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                (remaining != 0).then(|| remaining.saturating_sub(1))
            })
            .is_ok()
    }
}

struct EvidenceReadGateKv {
    inner: InMemoryKv,
    armed: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

impl EvidenceReadGateKv {
    fn new(local_id: NodeId) -> Self {
        Self {
            inner: InMemoryKv::new(local_id),
            armed: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        }
    }

    fn arm(&self) {
        assert!(!self.armed.swap(true, Ordering::AcqRel));
    }

    async fn wait_until_blocked(&self) {
        self.entered.acquire().await.unwrap().forget();
    }

    fn release_blocked_read(&self) {
        self.release.add_permits(1);
    }
}

#[async_trait::async_trait]
impl ClusterKv for EvidenceReadGateKv {
    async fn write(&self, key: &str, value: String) {
        self.inner.write(key, value).await;
    }

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        self.inner.read_from(who, key).await
    }

    async fn read_from_checked(&self, who: NodeId, key: &str) -> Result<Option<String>, String> {
        if key == ADOPTED_ASSIGNMENT_KEY && self.armed.swap(false, Ordering::AcqRel) {
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .map_err(|error| error.to_string())?
                .forget();
        }
        Ok(self.inner.read_from(who, key).await)
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        self.inner.scan(key).await
    }
}

impl DelayedRecoveryKv {
    fn new(local_id: NodeId) -> Self {
        Self {
            inner: InMemoryKv::new(local_id),
            block_next_recovery_write: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        }
    }

    fn block_next_recovery_write(&self) {
        self.block_next_recovery_write
            .store(true, Ordering::Release);
    }

    async fn wait_until_blocked(&self) {
        self.entered.acquire().await.unwrap().forget();
    }

    fn release_blocked_write(&self) {
        self.release.add_permits(1);
    }
}

#[async_trait::async_trait]
impl ClusterKv for DelayedRecoveryKv {
    async fn write(&self, key: &str, value: String) {
        let _ = self.write_checked(key, value).await;
    }

    async fn write_checked(&self, key: &str, value: String) -> Result<(), String> {
        if (key == "control:recover" || key == RECOVERY_INCARNATION_KEY)
            && self.block_next_recovery_write.swap(false, Ordering::AcqRel)
        {
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .map_err(|error| error.to_string())?
                .forget();
        }
        self.inner.write(key, value).await;
        Ok(())
    }

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        self.inner.read_from(who, key).await
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        self.inner.scan(key).await
    }
}

#[async_trait::async_trait]
impl ClusterKv for FaultyReadyReadKv {
    async fn write(&self, key: &str, value: String) {
        self.inner.write(key, value).await;
    }

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        self.inner.read_from(who, key).await
    }

    async fn read_from_checked(&self, who: NodeId, key: &str) -> Result<Option<String>, String> {
        if key == RELEASE_READY_ACK_KEY && self.should_fail_ready_read() {
            return Err("injected release readiness read failure".into());
        }
        Ok(self.inner.read_from(who, key).await)
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        self.inner.scan(key).await
    }
}

fn info(id: u64) -> NodeInfo {
    NodeInfo {
        id: NodeId(id),
        name: format!("n{id}"),
        rpc_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    }
}

fn ctl(self_id: u64, peers: Vec<NodeInfo>) -> ClusterController {
    let (_tx, rx) = watch::channel(peers);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(NodeId(self_id)));
    ClusterController::new(NodeId(self_id), kv, None, rx)
}

fn assignment_adoption(
    controller: &ClusterController,
    assignment_version: u64,
) -> CheckpointAssignmentAdoption {
    let owners = [controller.instance_id().0; 3];
    CheckpointAssignmentAdoption {
        participant: CheckpointParticipant {
            node_id: controller.instance_id().0,
            boot_incarnation: controller.recovery_incarnation(),
        },
        assignment_version,
        partitioning_abi_version: crate::state::PARTITIONING_ABI_VERSION,
        vnode_count: u32::try_from(owners.len()).unwrap(),
        assignment_digest: CheckpointAssignmentFence::owner_map_digest(3, &owners),
        vnode_state_ready: true,
    }
}

fn assignment_fence_for_adoption(
    adoption: &CheckpointAssignmentAdoption,
) -> CheckpointAssignmentFence {
    let owners = vec![adoption.participant.node_id; usize::try_from(adoption.vnode_count).unwrap()];
    CheckpointAssignmentFence::from_owner_map(
        adoption.assignment_version,
        &owners,
        vec![adoption.participant],
    )
    .unwrap()
}

fn checkpoint_fence_and_drain(
    controller: &ClusterController,
) -> (CheckpointAssignmentFence, AssignmentDrainTransition) {
    let participant = CheckpointParticipant {
        node_id: controller.instance_id().0,
        boot_incarnation: controller.recovery_incarnation(),
    };
    let predecessor =
        CheckpointAssignmentFence::from_owner_map(7, &[participant.node_id], vec![participant])
            .unwrap();
    let target =
        CheckpointAssignmentFence::from_owner_map(8, &[participant.node_id], vec![participant])
            .unwrap();
    let leader = test_leader_proof(participant.node_id, participant.boot_incarnation, 1);
    let transition = AssignmentDrainTransition::new(predecessor.clone(), target, leader).unwrap();
    (predecessor, transition)
}

#[cfg(feature = "cluster")]
#[test]
fn checkpoint_authority_access_is_exact_and_fails_closed_when_unwired() {
    let controller = ctl(1, Vec::new());
    assert!(matches!(
        controller.checkpoint_authority(),
        Err(super::super::ClusterCheckpointAuthorityError::NotConfigured)
    ));
    let authority = Arc::new(super::super::LeaderLeaseStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        1_000,
    ));
    controller.set_leader_lease_store(Arc::clone(&authority));
    assert!(Arc::ptr_eq(
        &controller.checkpoint_authority().unwrap(),
        &authority
    ));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn recovery_authority_rejects_a_structural_but_undurable_process_fence() {
    use crate::cluster::control::{
        AssignmentRecoveryDecision, AssignmentSnapshotRef, LeaderLeaseOwner, LeaseOutcome,
        ProcessLease, ProcessLeaseAuthority, ProcessLeaseFence,
    };

    let controller = ctl(1, Vec::new());
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_millis(1)).unwrap(),
    );
    controller
        .set_process_lease_authority(process_authority)
        .unwrap();
    let authority = Arc::new(super::super::LeaderLeaseStore::new(
        Arc::clone(&backing),
        1_000,
    ));
    let owner = LeaderLeaseOwner {
        node: NodeId(1),
        boot: controller.recovery_incarnation(),
        process_term: 1,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty leader authority must be acquired");
    };
    controller.set_leader_lease_store(Arc::clone(&authority));

    let removed_boot = Uuid::from_u128(2);
    let predecessor = CheckpointAssignmentFence::from_owner_map(
        1,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: owner.boot,
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: removed_boot,
            },
        ],
    )
    .unwrap();
    let target = CheckpointAssignmentFence::from_owner_map(
        2,
        &[1, 1],
        vec![CheckpointParticipant {
            node_id: 1,
            boot_incarnation: owner.boot,
        }],
    )
    .unwrap();
    let forged = ProcessLeaseFence::new(
        ProcessLease {
            node: NodeId(2),
            owner: removed_boot,
            term: 1,
            seq: 1,
            expires_at_ms: 1,
        },
        ProcessLease {
            node: NodeId(2),
            owner: Uuid::from_u128(3),
            term: 2,
            seq: 2,
            expires_at_ms: 2,
        },
    )
    .unwrap();
    let decision = AssignmentRecoveryDecision::new(
        predecessor,
        target,
        AssignmentSnapshotRef {
            version: 2,
            sha256: "0".repeat(64),
            encoded_len: 1,
        },
        vec![forged],
        crate::checkpoint::CommittedCheckpointRef {
            epoch: 1,
            checkpoint_id: 1,
            sha256: "0".repeat(64),
            len: 1,
        },
        lease.proof(),
    )
    .unwrap();

    let error = controller
        .record_assignment_recovery_decision(
            &lease.proof(),
            decision,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert!(
        error.contains("process fence verification failed"),
        "{error}"
    );
    assert!(authority
        .assignment_recovery_decision(2)
        .await
        .unwrap()
        .is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn assignment_leader_audit_accepts_only_the_exact_local_grant_and_process_term() {
    use crate::cluster::control::{
        LeaderLeaseOwner, LeaderLeaseStore, LeaseDeadline, LeaseOutcome, ProcessLeaseAuthority,
        ProcessLeaseOutcome,
    };

    let node = NodeId(1);
    let boot = Uuid::from_u128(11);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&kv),
        kv,
        None,
        members_rx,
        boot,
    );
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(30))))
        .unwrap();

    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_millis(1)).unwrap(),
    );
    let process_store = process_authority.store_for(node);
    let ProcessLeaseOutcome::Acquired(process_lease) =
        process_store.try_acquire(boot, 0).await.unwrap()
    else {
        panic!("empty process authority must be acquired");
    };
    controller
        .set_process_lease_authority(process_authority)
        .unwrap();

    let leader_authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&backing), 1));
    let owner = LeaderLeaseOwner {
        node,
        boot,
        process_term: process_lease.term,
    };
    let LeaseOutcome::Acquired(leader_lease) =
        leader_authority.begin_new_term(&owner, 0).await.unwrap()
    else {
        panic!("empty leader authority must be acquired");
    };
    controller.set_leader_lease_store(Arc::clone(&leader_authority));
    let (_leader_tx, leader_rx) = watch::channel(Some(leader_lease.clone()));
    controller
        .set_leader_lease_watch(
            leader_rx,
            owner,
            Arc::new(LeaseDeadline::live_for(Duration::from_secs(30))),
        )
        .unwrap();

    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[node.0],
        vec![CheckpointParticipant {
            node_id: node.0,
            boot_incarnation: boot,
        }],
    )
    .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    let deadline = || tokio::time::Instant::now() + Duration::from_secs(1);

    let proof = controller
        .audit_assignment_leader_authority(&fence, None, deadline())
        .await
        .unwrap();
    assert_eq!(proof, leader_lease.proof());
    assert_eq!(
        controller
            .audit_assignment_leader_authority(&fence, Some(&proof), deadline())
            .await
            .unwrap(),
        proof
    );

    let mut stale = proof.clone();
    stale.fencing_token += 1;
    let stale_error = controller
        .audit_assignment_leader_authority(&fence, Some(&stale), deadline())
        .await
        .unwrap_err();
    assert!(
        stale_error.contains("drain-bound expected proof"),
        "{stale_error}"
    );

    let other_fence = CheckpointAssignmentFence::from_owner_map(
        2,
        &[node.0],
        vec![CheckpointParticipant {
            node_id: node.0,
            boot_incarnation: boot,
        }],
    )
    .unwrap();
    let fence_error = controller
        .audit_assignment_leader_authority(&other_fence, None, deadline())
        .await
        .unwrap_err();
    assert!(
        fence_error.contains("exact installed fence"),
        "{fence_error}"
    );

    let observation = process_store.observe_rival(&process_lease).unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    assert!(matches!(
        process_store
            .try_takeover(Uuid::from_u128(12), &observation, 2)
            .await
            .unwrap(),
        ProcessLeaseOutcome::Acquired(_)
    ));
    let process_error = controller
        .audit_assignment_leader_authority(&fence, None, deadline())
        .await
        .unwrap_err();
    assert!(
        process_error.contains("process term is no longer current"),
        "{process_error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assignment_leader_audit_rejects_takeover_after_second_remote_confirmation() {
    use crate::cluster::control::{
        LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome, ProcessLeaseAuthority,
        ProcessLeaseOutcome,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    let leader_node = NodeId(1);
    let observer_node = NodeId(2);
    let leader_boot = Uuid::from_u128(11);
    let observer_boot = Uuid::from_u128(22);
    let observer_kv = Arc::new(InMemoryKv::new(observer_node));
    let observer_control: Arc<dyn ClusterKv> = observer_kv.clone();
    let (_members_tx, members_rx) = watch::channel(vec![info(leader_node.0)]);
    let observer = Arc::new(ClusterController::new_with_recovery_incarnation(
        observer_node,
        Arc::clone(&observer_control),
        observer_control,
        None,
        members_rx,
        observer_boot,
    ));

    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_millis(1)).unwrap(),
    );
    let ProcessLeaseOutcome::Acquired(process_lease) = process_authority
        .store_for(leader_node)
        .try_acquire(leader_boot, 0)
        .await
        .unwrap()
    else {
        panic!("leader process authority must be acquired");
    };
    observer
        .set_process_lease_authority(process_authority)
        .unwrap();

    let leader_authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&backing), 1));
    let initial_owner = LeaderLeaseOwner {
        node: leader_node,
        boot: leader_boot,
        process_term: process_lease.term,
    };
    let LeaseOutcome::Acquired(initial_lease) = leader_authority
        .begin_new_term(&initial_owner, 0)
        .await
        .unwrap()
    else {
        panic!("leader authority must be acquired");
    };
    observer.set_leader_lease_store(Arc::clone(&leader_authority));
    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[leader_node.0],
        vec![CheckpointParticipant {
            node_id: leader_node.0,
            boot_incarnation: leader_boot,
        }],
    )
    .unwrap();
    observer.publish_checkpoint_assignment_fence(Some(fence.clone()));

    observer
        .start_barrier_server("127.0.0.1:0".parse().unwrap(), None)
        .await
        .unwrap();
    let remote_kv = Arc::new(InMemoryKv::new(leader_node));
    let remote_control: Arc<dyn ClusterKv> = remote_kv.clone();
    let remote = BarrierCoordinator::new(remote_control);
    remote
        .install_process_lease_deadline(Arc::new(super::super::LeaseDeadline::live_for(
            Duration::from_secs(30),
        )))
        .unwrap();
    remote.install_local_process_lease(&process_lease).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let (second_tx, mut second_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let provider_calls = Arc::clone(&calls);
    let provider_release = Arc::clone(&release);
    let provider_proof = initial_lease.proof();
    remote.set_local_leader_proof_provider(Arc::new(move || {
        if provider_calls.fetch_add(1, Ordering::AcqRel) == 1 {
            let _ = second_tx.send(());
            let (lock, ready) = &*provider_release;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
        }
        Some(provider_proof.clone())
    }));
    remote
        .start_server("127.0.0.1:0".parse().unwrap(), None)
        .await
        .unwrap();
    let endpoint = remote_kv
        .read_from(leader_node, BARRIER_ADDR_KEY)
        .await
        .unwrap();
    observer_kv.seed(leader_node, BARRIER_ADDR_KEY, endpoint);

    let successor = LeaderLeaseOwner {
        node: observer_node,
        boot: observer_boot,
        process_term: 1,
    };
    let observation = leader_authority
        .observe_rival(&successor, &initial_lease)
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let audit = {
        let observer = Arc::clone(&observer);
        let fence = fence.clone();
        tokio::spawn(async move {
            observer
                .audit_assignment_leader_authority(
                    &fence,
                    None,
                    tokio::time::Instant::now() + Duration::from_secs(2),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), second_rx.recv())
        .await
        .unwrap()
        .expect("the audit must reach its second live confirmation");
    assert!(matches!(
        leader_authority
            .try_takeover(&successor, &observation, 2)
            .await
            .unwrap(),
        LeaseOutcome::Acquired(_)
    ));
    {
        let (lock, ready) = &*release;
        *lock.lock().unwrap() = true;
        ready.notify_all();
    }

    let error = audit.await.unwrap().unwrap_err();
    assert!(
        error.contains("durable grant changed during the audit"),
        "{error}"
    );
    assert_eq!(calls.load(Ordering::Acquire), 2);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn checkpoint_assignment_fence_requires_the_exact_durable_leader_term() {
    use crate::checkpoint::{AssignmentDrainTransition, CheckpointParticipant};
    use crate::cluster::control::{LeaderLeaseOwner, LeaseOutcome};
    use uuid::Uuid;

    let controller = ctl(7, vec![info(1)]);
    let leader_boot = Uuid::from_u128(1);
    let fence = CheckpointAssignmentFence::from_owner_map(
        4,
        &[1, 7],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: leader_boot,
            },
            CheckpointParticipant {
                node_id: 7,
                boot_incarnation: controller.recovery_incarnation(),
            },
        ],
    )
    .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));

    let authority = Arc::new(super::super::LeaderLeaseStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        1_000,
    ));
    let owner = LeaderLeaseOwner {
        node: NodeId(1),
        boot: leader_boot,
        process_term: 3,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty test authority must be acquired");
    };
    controller.set_leader_lease_store(authority);
    let exact = lease.proof();

    assert_eq!(
        controller
            .checkpoint_assignment_fence_for_leader(4, &exact)
            .await,
        Some(fence.clone())
    );

    let mut stale_token = exact.clone();
    stale_token.fencing_token += 1;
    assert!(controller
        .checkpoint_assignment_fence_for_leader(4, &stale_token)
        .await
        .is_none());

    let mut stale_process_term = exact.clone();
    stale_process_term.owner.process_term += 1;
    assert!(controller
        .checkpoint_assignment_fence_for_leader(4, &stale_process_term)
        .await
        .is_none());

    let target = CheckpointAssignmentFence::from_owner_map(
        5,
        &[7, 7],
        vec![CheckpointParticipant {
            node_id: 7,
            boot_incarnation: controller.recovery_incarnation(),
        }],
    )
    .unwrap();
    controller.publish_checkpoint_drain_transition(Some(
        AssignmentDrainTransition::new(fence.clone(), target, exact.clone()).unwrap(),
    ));
    assert_eq!(
        controller
            .checkpoint_assignment_fence_for_leader(4, &exact)
            .await,
        Some(fence)
    );
    assert!(controller
        .checkpoint_assignment_fence_for_leader(4, &stale_token)
        .await
        .is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn missing_checkpoint_assignment_avoids_durable_authority_io() {
    let controller = ctl(1, vec![]);
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let gate = Arc::new(AuthorityIoGateStore::new(
        backing,
        AuthorityIoGateOperation::Get,
    ));
    let gated_backing: Arc<dyn object_store::ObjectStore> = gate.clone();
    let (_authority, proof) =
        install_recovery_authority_with_store(&controller, 1_000, gated_backing).await;

    gate.arm();
    let certified = tokio::time::timeout(
        Duration::from_millis(50),
        controller.checkpoint_assignment_fence_for_leader(4, &proof),
    )
    .await
    .expect("missing local assignment must fail before durable authority I/O");
    assert!(certified.is_none());
}

#[test]
fn is_leader_when_lowest_id() {
    let c = ctl(1, vec![info(5), info(7)]);
    assert!(c.is_leader());
}

#[test]
fn follower_when_peer_has_lower_id() {
    let c = ctl(7, vec![info(3), info(5)]);
    assert!(!c.is_leader());
    assert_eq!(c.current_leader(), Some(NodeId(3)));
}

#[test]
fn solo_instance_is_leader() {
    let c = ctl(42, vec![]);
    assert!(c.is_leader());
}

/// When a lease is wired, the gossip candidate is leader only while it holds
/// an unexpired lease; every other leader-gated path inherits this fencing.
#[cfg(feature = "cluster")]
#[test]
fn is_leader_requires_held_lease_when_wired() {
    use crate::cluster::control::{LeaderLease, LeaderLeaseOwner, LeaderProof, LeaseDeadline};
    let owner = |node, boot, process_term| LeaderLeaseOwner {
        node: NodeId(node),
        boot: Uuid::from_u128(boot),
        process_term,
    };
    let lease = |owner| LeaderLease {
        seq: 1,
        renewal_sequence: 1,
        token: 1,
        owner,
        expires_at_ms: i64::MIN,
        catalog_manifest: None,
    };
    let expected = owner(1, 1, 1);
    let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(10)));

    let c = ctl(1, vec![info(5)]); // lowest id → gossip candidate
    assert!(c.is_leader(), "gossip-only leadership when no lease wired");
    assert!(!c.has_leader_lease_fencing());
    assert_eq!(c.leader_fencing_token(), None);

    let (tx, rx) = watch::channel(None);
    c.set_leader_lease_watch(rx, expected.clone(), Arc::clone(&deadline))
        .unwrap();
    assert!(c.has_leader_lease_fencing());
    assert!(!c.is_leader(), "fenced out until a lease is held");
    assert_eq!(c.leader_fencing_token(), None);

    tx.send(Some(lease(owner(2, 2, 1)))).unwrap();
    assert!(!c.is_leader(), "another node holds the lease");
    assert_eq!(c.leader_fencing_token(), None);

    tx.send(Some(lease(expected))).unwrap();
    assert!(c.is_leader(), "the exact process owns a live local lease");
    assert!(
        c.capture_leader_proof().is_none(),
        "durable proof also requires the stable-node process deadline"
    );
    c.set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(10))))
        .unwrap();
    assert_eq!(c.leader_fencing_token(), Some(1));
    let proof = c.capture_leader_proof().unwrap();
    assert_eq!(proof.fencing_token, 1);
    assert!(c.proof_is_live(&proof));

    assert!(c.is_gossip_leader());
    deadline.fence();
    assert!(!c.is_leader(), "the local monotonic lease expired");
    assert_eq!(c.leader_fencing_token(), None);
    assert!(!c.proof_is_live(&proof));

    let invalid = LeaderProof {
        owner: proof.owner,
        fencing_token: 0,
    };
    assert!(!c.proof_is_live(&invalid));
}

#[cfg(feature = "cluster")]
#[test]
fn leader_proof_rejects_restarted_owner_and_stale_token() {
    use crate::cluster::control::{LeaderLease, LeaderLeaseOwner, LeaseDeadline};
    let owner = |boot, process_term| LeaderLeaseOwner {
        node: NodeId(1),
        boot: Uuid::from_u128(boot),
        process_term,
    };
    let lease = |token, owner| LeaderLease {
        seq: token,
        renewal_sequence: token,
        token,
        owner,
        expires_at_ms: i64::MIN,
        catalog_manifest: None,
    };
    let expected = owner(1, 7);
    let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(10)));
    let c = ctl(1, vec![info(5)]);
    c.set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(10))))
        .unwrap();
    let (tx, rx) = watch::channel(Some(lease(11, expected.clone())));
    c.set_leader_lease_watch(rx, expected.clone(), deadline)
        .unwrap();
    let mut grant_changes = c.leader_grant_watch().unwrap();

    let stale = c.capture_leader_proof().unwrap();
    assert!(c.proof_is_live(&stale));

    tx.send(Some(lease(12, expected))).unwrap();
    assert!(grant_changes.has_changed().unwrap());
    grant_changes.borrow_and_update();
    assert!(
        !c.proof_is_live(&stale),
        "a superseded token must fail closed"
    );
    assert_eq!(c.capture_leader_proof().unwrap().fencing_token, 12);

    tx.send(Some(lease(13, owner(1, 8)))).unwrap();
    assert!(grant_changes.has_changed().unwrap());
    grant_changes.borrow_and_update();
    assert!(
        c.capture_leader_proof().is_none(),
        "a newer process term on the same node and boot is a different owner"
    );

    tx.send(Some(lease(14, owner(2, 8)))).unwrap();
    assert!(grant_changes.has_changed().unwrap());
    assert!(
        c.capture_leader_proof().is_none(),
        "a new boot on the same stable node is a different owner"
    );
    assert!(!c.proof_is_live(&stale));
}

#[cfg(feature = "cluster")]
#[test]
fn leader_gate_tracks_deadline_generations_without_reviving_a_stale_proof() {
    use crate::cluster::control::{LeaderLease, LeaderLeaseOwner, LeaseDeadline};

    let owner = LeaderLeaseOwner {
        node: NodeId(1),
        boot: Uuid::from_u128(1),
        process_term: 7,
    };
    let lease = |token| LeaderLease {
        seq: token,
        renewal_sequence: token,
        token,
        owner: owner.clone(),
        expires_at_ms: i64::MIN,
        catalog_manifest: None,
    };
    let c = ctl(1, vec![info(5)]);
    c.set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(10))))
        .unwrap();
    let old_deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(10)));
    let (lease_tx, lease_rx) = watch::channel(Some(lease(1)));
    let (deadline_tx, deadline_rx) = watch::channel(Arc::clone(&old_deadline));
    c.set_leader_lease_runtime_watches(lease_rx, owner.clone(), deadline_rx)
        .unwrap();

    let stale = c.capture_leader_proof().expect("initial live proof");
    old_deadline.fence();
    lease_tx.send_replace(None);
    let next_deadline = Arc::new(LeaseDeadline::uninitialized());
    deadline_tx.send_replace(Arc::clone(&next_deadline));
    assert!(!c.is_leader());
    assert!(!c.proof_is_live(&stale));

    next_deadline.extend(Duration::from_secs(10));
    lease_tx.send_replace(Some(lease(2)));
    let current = c.capture_leader_proof().expect("rotated live proof");
    assert_eq!(current.fencing_token, 2);
    assert!(c.proof_is_live(&current));
    assert!(!c.proof_is_live(&stale));
    assert!(!old_deadline.is_live());
    assert!(next_deadline.is_live());
}

#[test]
fn assignable_instances_excludes_draining_peer_and_self_on_drain() {
    let mut draining_peer = info(5);
    draining_peer.state = NodeState::Draining;
    let c = ctl(1, vec![info(3), draining_peer]);

    // Active peers + self; the Draining peer is shed.
    assert_eq!(c.assignable_instances(), vec![NodeId(1), NodeId(3)]);
    assert!(!c.is_draining());

    // After begin_drain, self drops out too.
    c.begin_drain();
    assert!(c.is_draining());
    assert_eq!(c.assignable_instances(), vec![NodeId(3)]);
    assert_eq!(c.current_leader(), Some(NodeId(3)));
    assert!(!c.is_leader(), "a draining owner must yield leadership");
}

#[test]
fn checkpoint_instances_keep_draining_owners_and_exclude_unavailable_nodes() {
    let mut draining = info(2);
    draining.state = NodeState::Draining;
    let mut joining = info(3);
    joining.state = NodeState::Joining;
    let mut suspected = info(4);
    suspected.state = NodeState::Suspected;
    let mut left = info(5);
    left.state = NodeState::Left;
    let c = ctl(1, vec![draining, joining, suspected, left, info(6)]);

    assert_eq!(
        c.checkpoint_instances(),
        vec![NodeId(1), NodeId(2), NodeId(6)]
    );
    assert_eq!(c.assignable_instances(), vec![NodeId(1), NodeId(6)]);

    c.begin_drain();
    assert!(
        c.checkpoint_instances().contains(&NodeId(1)),
        "self remains responsible for its old vnodes while draining"
    );
    assert!(!c.assignable_instances().contains(&NodeId(1)));
}

#[test]
fn checkpoint_assignment_fence_allows_active_workers_without_vnodes() {
    let self_id = NodeId(1);
    let (_members_tx, members_rx) = watch::channel(vec![info(2), info(3)]);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let c = ClusterController::new(self_id, kv, None, members_rx);
    let expected = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: c.recovery_incarnation(),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: Uuid::new_v4(),
            },
        ],
    )
    .unwrap();
    c.publish_checkpoint_assignment_fence(Some(expected.clone()));

    assert_eq!(
        c.checkpoint_assignment_fence(7),
        Some(expected),
        "an active worker that owns no vnode must not expand the checkpoint quorum"
    );
}

#[test]
fn process_lease_fence_withdraws_checkpoint_authority() {
    let controller = ctl(1, Vec::new());
    let (fence, transition) = checkpoint_fence_and_drain(&controller);
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    controller.publish_checkpoint_drain_transition(Some(transition.clone()));

    assert_eq!(controller.checkpoint_assignment_fence(7), Some(fence));
    assert_eq!(controller.checkpoint_drain_transition(), Some(transition));

    controller.fence_process_lease();

    assert!(!controller.process_lease_is_live());
    assert_eq!(
        controller.checkpoint_assignment_watch().borrow().clone(),
        None
    );
    assert_eq!(controller.checkpoint_drain_transition(), None);
}

#[test]
fn checkpoint_drain_transition_compare_and_clear_preserves_a_nonmatch() {
    let controller = ctl(1, Vec::new());
    let (_, installed) = checkpoint_fence_and_drain(&controller);
    let other_controller = ctl(2, Vec::new());
    let (_, nonmatching) = checkpoint_fence_and_drain(&other_controller);
    controller.publish_checkpoint_drain_transition(Some(installed.clone()));

    assert!(!controller.clear_checkpoint_drain_transition_if_matches(&nonmatching));
    assert_eq!(
        controller.checkpoint_drain_transition(),
        Some(installed.clone())
    );

    assert!(controller.clear_checkpoint_drain_transition_if_matches(&installed));
    assert_eq!(controller.checkpoint_drain_transition(), None);
}

#[test]
fn terminal_process_fence_rejects_checkpoint_authority_republication() {
    let controller = ctl(1, Vec::new());
    let (fence, transition) = checkpoint_fence_and_drain(&controller);
    controller.fence_process_lease();

    controller.publish_checkpoint_assignment_fence(Some(fence));
    controller.publish_checkpoint_drain_transition(Some(transition));
    controller.set_active(true);
    controller.set_recovering(false);

    assert_eq!(
        controller.checkpoint_assignment_watch().borrow().clone(),
        None
    );
    assert_eq!(controller.checkpoint_drain_transition(), None);
    assert!(controller.leadership_participants.read().is_none());
    assert!(!controller.active.load(Ordering::Acquire));
    assert!(!controller.leader_eligible.load(Ordering::Acquire));
    assert!(controller.is_recovering());
}

#[test]
fn expired_process_deadline_rejects_authority_publication_before_async_fencing() {
    let controller = ctl(1, Vec::new());
    controller
        .set_process_lease_deadline(Arc::new(super::super::LeaseDeadline::fenced()))
        .unwrap();
    let (fence, transition) = checkpoint_fence_and_drain(&controller);

    controller.publish_checkpoint_assignment_fence(Some(fence));
    controller.publish_checkpoint_drain_transition(Some(transition));

    assert_eq!(
        controller.checkpoint_assignment_watch().borrow().clone(),
        None
    );
    assert_eq!(controller.checkpoint_drain_transition(), None);
    assert!(controller.leadership_participants.read().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn leased_recovery_identity_requires_a_local_monotonic_deadline() {
    use super::super::{ProcessLeaseAuthority, ProcessLeaseOutcome};

    let node = NodeId(1);
    let boot = Uuid::from_u128(91);
    let kv = Arc::new(InMemoryKv::new(node));
    let controller = ClusterController::new_with_recovery_incarnation(
        node,
        kv.clone(),
        kv.clone(),
        None,
        watch::channel(Vec::new()).1,
        boot,
    );
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let authority = Arc::new(ProcessLeaseAuthority::new(backing, Duration::from_secs(60)).unwrap());
    let ProcessLeaseOutcome::Acquired(lease) = authority
        .store_for(node)
        .try_acquire(boot, 0)
        .await
        .unwrap()
    else {
        panic!("empty process authority must be acquired");
    };
    controller.set_process_lease_authority(authority).unwrap();
    controller
        .start_barrier_server("127.0.0.1:0".parse().unwrap(), None)
        .await
        .unwrap();
    let unbound_endpoint = kv.read_from(node, BARRIER_ADDR_KEY).await.unwrap();

    let error = controller
        .publish_leased_recovery_incarnation(&lease)
        .await
        .unwrap_err();

    assert!(error.contains("deadline"), "{error}");
    assert_eq!(controller.recovery_process_term.load(Ordering::Acquire), 0);
    assert!(kv.read_from(node, RECOVERY_INCARNATION_KEY).await.is_none());
    assert_eq!(
        kv.read_from(node, BARRIER_ADDR_KEY).await.as_deref(),
        Some(unbound_endpoint.as_str()),
        "failed lease publication must not process-bind an assignment-less endpoint"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn superseded_live_process_lease_cannot_replace_endpoint_advertisement() {
    use super::super::{ProcessLeaseAuthority, ProcessLeaseOutcome};

    let node = NodeId(1);
    let boot = Uuid::from_u128(91);
    let kv = Arc::new(InMemoryKv::new(node));
    let controller = ClusterController::new_with_recovery_incarnation(
        node,
        kv.clone(),
        kv.clone(),
        None,
        watch::channel(Vec::new()).1,
        boot,
    );
    controller
        .set_process_lease_deadline(Arc::new(super::super::LeaseDeadline::live_for(
            Duration::from_secs(30),
        )))
        .unwrap();
    controller
        .start_barrier_server("127.0.0.1:0".parse().unwrap(), None)
        .await
        .unwrap();
    let original_endpoint = kv.read_from(node, BARRIER_ADDR_KEY).await.unwrap();

    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let authority =
        Arc::new(ProcessLeaseAuthority::new(backing, Duration::from_millis(1)).unwrap());
    let store = authority.store_for(node);
    let ProcessLeaseOutcome::Acquired(incumbent) = store.try_acquire(boot, 0).await.unwrap() else {
        panic!("empty process authority must be acquired");
    };
    controller.set_process_lease_authority(authority).unwrap();
    let observation = store.observe_rival(&incumbent).unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let ProcessLeaseOutcome::Acquired(successor) = store
        .try_takeover(Uuid::from_u128(92), &observation, 2)
        .await
        .unwrap()
    else {
        panic!("expired process lease must be superseded");
    };
    assert_ne!(successor.owner, incumbent.owner);
    assert!(controller.recovery_process_lease_is_live());

    let error = controller
        .publish_leased_recovery_incarnation(&incumbent)
        .await
        .unwrap_err();

    assert!(error.contains("not the current durable"), "{error}");
    assert_eq!(
        kv.read_from(node, BARRIER_ADDR_KEY).await.as_deref(),
        Some(original_endpoint.as_str()),
        "a superseded term must not replace the published endpoint"
    );
    assert_eq!(controller.recovery_process_term.load(Ordering::Acquire), 0);
    assert!(kv.read_from(node, RECOVERY_INCARNATION_KEY).await.is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn takeover_during_recovery_identity_publication_terminally_fences_the_process() {
    use super::super::{ProcessLeaseAuthority, ProcessLeaseOutcome};

    let node = NodeId(1);
    let boot = Uuid::from_u128(91);
    let control = Arc::new(InMemoryKv::new(node));
    let recovery = Arc::new(DelayedRecoveryKv::new(node));
    let control_kv: Arc<dyn ClusterKv> = control.clone();
    let recovery_kv: Arc<dyn ClusterKv> = recovery.clone();
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node,
        control_kv,
        recovery_kv,
        None,
        watch::channel(Vec::new()).1,
        boot,
    ));
    let deadline = Arc::new(super::super::LeaseDeadline::live_for(Duration::from_secs(
        30,
    )));
    controller
        .set_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();

    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let authority =
        Arc::new(ProcessLeaseAuthority::new(backing, Duration::from_millis(1)).unwrap());
    let store = authority.store_for(node);
    let ProcessLeaseOutcome::Acquired(incumbent) = store.try_acquire(boot, 0).await.unwrap() else {
        panic!("empty process authority must be acquired");
    };
    controller
        .set_process_lease_authority(Arc::clone(&authority))
        .unwrap();
    controller
        .start_leased_barrier_server("127.0.0.1:0".parse().unwrap(), None, &incumbent)
        .await
        .unwrap();

    recovery.block_next_recovery_write();
    let publishing = {
        let controller = Arc::clone(&controller);
        let incumbent = incumbent.clone();
        tokio::spawn(async move {
            controller
                .publish_leased_recovery_incarnation(&incumbent)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), recovery.wait_until_blocked())
        .await
        .expect("recovery identity publication did not reach the injected write gate");

    let observation = store.observe_rival(&incumbent).unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let ProcessLeaseOutcome::Acquired(successor) = store
        .try_takeover(Uuid::from_u128(92), &observation, 2)
        .await
        .unwrap()
    else {
        panic!("expired process lease must be superseded");
    };
    assert_ne!(successor.owner, incumbent.owner);
    recovery.release_blocked_write();

    let error = publishing.await.unwrap().unwrap_err();
    assert!(error.contains("changed while publishing"), "{error}");
    assert!(!controller.process_lease_is_live());
    assert!(!deadline.is_live());
    assert_eq!(controller.recovery_process_term.load(Ordering::Acquire), 0);
    assert!(recovery
        .read_from(node, RECOVERY_INCARNATION_KEY)
        .await
        .is_some());
    assert!(control.read_from(node, BARRIER_ADDR_KEY).await.is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn leased_barrier_server_first_publication_contains_exact_process_identity() {
    let node = NodeId(7);
    let boot = Uuid::from_u128(77);
    let kv = Arc::new(InMemoryKv::new(node));
    let controller = ClusterController::new_with_recovery_incarnation(
        node,
        kv.clone(),
        kv.clone(),
        None,
        watch::channel(Vec::new()).1,
        boot,
    );
    let lease = super::super::ProcessLease {
        node,
        owner: boot,
        term: 9,
        seq: 9,
        expires_at_ms: i64::MAX,
    };
    let error = controller
        .start_leased_barrier_server("127.0.0.1:0".parse().unwrap(), None, &lease)
        .await
        .unwrap_err();
    assert!(error.contains("deadline"), "{error}");
    assert!(kv.read_from(node, BARRIER_ADDR_KEY).await.is_none());

    controller
        .set_process_lease_deadline(Arc::new(super::super::LeaseDeadline::live_for(
            Duration::from_secs(30),
        )))
        .unwrap();

    controller
        .start_leased_barrier_server("127.0.0.1:0".parse().unwrap(), None, &lease)
        .await
        .unwrap();
    let raw = kv.read_from(node, BARRIER_ADDR_KEY).await.unwrap();
    let record: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(record["version"], 1);
    assert_eq!(record["process"]["node_id"], node.0);
    assert_eq!(record["process"]["boot_incarnation"], boot.to_string());
    assert_eq!(record["process"]["process_term"], lease.term);
    assert!(record["address"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));
}

#[test]
fn process_deadline_installation_is_idempotent_only_for_the_same_clock() {
    let controller = ctl(1, Vec::new());
    let deadline = Arc::new(super::super::LeaseDeadline::live_for(Duration::from_secs(
        10,
    )));

    controller
        .set_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    controller
        .set_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    let error = controller
        .set_process_lease_deadline(Arc::new(super::super::LeaseDeadline::live_for(
            Duration::from_secs(10),
        )))
        .unwrap_err();

    assert!(error.contains("already installed"), "{error}");
    assert!(Arc::ptr_eq(
        &controller.process_lease_deadline().unwrap(),
        &deadline
    ));
}

#[test]
fn terminal_process_fence_wins_concurrent_authority_publication() {
    let controller = Arc::new(ctl(1, Vec::new()));
    let (fence, transition) = checkpoint_fence_and_drain(&controller);
    let barrier = Arc::new(std::sync::Barrier::new(3));
    std::thread::scope(|scope| {
        let publisher = Arc::clone(&controller);
        let publisher_barrier = Arc::clone(&barrier);
        scope.spawn(move || {
            publisher_barrier.wait();
            publisher.publish_checkpoint_assignment_fence(Some(fence));
            publisher.publish_checkpoint_drain_transition(Some(transition));
            publisher.set_active(true);
            publisher.set_recovering(false);
        });
        let fencer = Arc::clone(&controller);
        let fencer_barrier = Arc::clone(&barrier);
        scope.spawn(move || {
            fencer_barrier.wait();
            fencer.fence_process_lease();
        });
        barrier.wait();
    });

    assert_eq!(
        controller.checkpoint_assignment_watch().borrow().clone(),
        None
    );
    assert_eq!(controller.checkpoint_drain_transition(), None);
    assert!(controller.leadership_participants.read().is_none());
    assert!(!controller.active.load(Ordering::Acquire));
    assert!(!controller.leader_eligible.load(Ordering::Acquire));
    assert!(controller.is_recovering());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn certified_leader_retains_local_candidacy_to_coordinate_its_drain() {
    use crate::cluster::control::{LeaderLease, LeaderLeaseOwner, LeaseDeadline};

    let self_id = NodeId(1);
    let idle = NodeId(2);
    let (members_tx, members_rx) = watch::channel(vec![info(idle.0)]);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let c = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
    let fence = CheckpointAssignmentFence::from_owner_map(
        7,
        &[self_id.0],
        vec![CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: c.recovery_incarnation(),
        }],
    )
    .unwrap();
    c.publish_checkpoint_assignment_fence(Some(fence.clone()));
    let owner = LeaderLeaseOwner {
        node: self_id,
        boot: c.recovery_incarnation(),
        process_term: 1,
    };
    let (_lease_tx, lease_rx) = watch::channel(Some(LeaderLease {
        seq: 1,
        renewal_sequence: 1,
        token: 1,
        owner: owner.clone(),
        expires_at_ms: i64::MIN,
        catalog_manifest: None,
    }));
    c.set_leader_lease_watch(
        lease_rx,
        owner,
        Arc::new(LeaseDeadline::live_for(Duration::from_secs(10))),
    )
    .unwrap();
    let (idle_members_tx, idle_members_rx) = watch::channel(vec![info(self_id.0)]);
    let idle_kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(idle));
    let idle_observer = ClusterController::new(idle, idle_kv, None, idle_members_rx);
    idle_observer.publish_checkpoint_assignment_fence(Some(fence));
    assert_eq!(idle_observer.current_leader(), Some(self_id));
    let mut candidacy = c.leader_candidacy_watch();
    assert!(candidacy.borrow_and_update().is_eligible());
    for _ in 0..64 {
        members_tx.send_replace(vec![info(idle.0)]);
    }
    tokio::task::yield_now().await;
    assert!(
        !candidacy.has_changed().unwrap(),
        "unchanged membership must not starve leader renewal with candidacy wakeups"
    );

    assert!(c.begin_drain());
    let mut advertised_draining = info(self_id.0);
    advertised_draining.state = NodeState::Draining;
    idle_members_tx.send(vec![advertised_draining]).unwrap();
    assert!(c.is_draining());
    assert_eq!(c.assignable_instances(), vec![idle]);
    assert_eq!(c.current_leader(), Some(self_id));
    assert!(c.is_gossip_leader());
    assert_eq!(idle_observer.current_leader(), None);
    assert!(!idle_observer.is_gossip_leader());
    assert!(
        !candidacy.has_changed().unwrap(),
        "coordinating the predecessor cut must not revoke the current leader's lease"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "current_thread")]
async fn coalesced_controller_candidacy_loss_advances_the_generation() {
    let self_id = NodeId(1);
    let peer = NodeId(2);
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let controller = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
    let mut candidacy = controller.leader_candidacy_watch();
    let initial = *candidacy.borrow_and_update();
    assert!(initial.is_eligible());

    let peer_fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[peer.0],
        vec![CheckpointParticipant {
            node_id: peer.0,
            boot_incarnation: Uuid::new_v4(),
        }],
    )
    .unwrap();
    let local_fence = CheckpointAssignmentFence::from_owner_map(
        2,
        &[self_id.0],
        vec![CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: controller.recovery_incarnation(),
        }],
    )
    .unwrap();

    // No await between loss and reacquisition: a watch receiver observes only the final
    // eligible value, but its generation must still fence the prior leader token.
    controller.publish_checkpoint_assignment_fence(Some(peer_fence));
    controller.publish_checkpoint_assignment_fence(Some(local_fence));

    assert!(candidacy.has_changed().unwrap());
    let resumed = *candidacy.borrow_and_update();
    assert!(resumed.is_eligible());
    assert_ne!(resumed, initial);
}

#[test]
fn certified_draining_peer_does_not_displace_an_active_participant() {
    let observer = NodeId(3);
    let draining = NodeId(1);
    let active = NodeId(2);
    let mut draining_info = info(draining.0);
    draining_info.state = NodeState::Draining;
    let (_members_tx, members_rx) = watch::channel(vec![info(active.0), draining_info]);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(observer));
    let c = ClusterController::new(observer, kv, None, members_rx);
    let fence = CheckpointAssignmentFence::from_owner_map(
        7,
        &[active.0, draining.0],
        vec![
            CheckpointParticipant {
                node_id: draining.0,
                boot_incarnation: Uuid::new_v4(),
            },
            CheckpointParticipant {
                node_id: active.0,
                boot_incarnation: Uuid::new_v4(),
            },
        ],
    )
    .unwrap();
    c.publish_checkpoint_assignment_fence(Some(fence));

    assert_eq!(c.current_leader(), Some(active));
    assert!(!c.is_gossip_leader());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn certified_idle_worker_yields_leadership_to_an_assignment_participant() {
    let self_id = NodeId(1);
    let owner = NodeId(2);
    let (_members_tx, members_rx) = watch::channel(vec![info(owner.0)]);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let c = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
    let mut candidacy = c.leader_candidacy_watch();
    assert!(candidacy.borrow_and_update().is_eligible());
    assert_eq!(c.current_leader(), Some(self_id));

    let fence = CheckpointAssignmentFence::from_owner_map(
        7,
        &[owner.0],
        vec![CheckpointParticipant {
            node_id: owner.0,
            boot_incarnation: Uuid::new_v4(),
        }],
    )
    .unwrap();
    c.publish_checkpoint_assignment_fence(Some(fence.clone()));

    assert_eq!(c.checkpoint_assignment_fence(7), Some(fence.clone()));
    assert_eq!(c.current_leader(), Some(owner));
    assert!(!c.is_gossip_leader());
    tokio::time::timeout(Duration::from_secs(1), candidacy.changed())
        .await
        .expect("leader candidacy relay did not observe the assignment roster")
        .unwrap();
    assert!(!candidacy.borrow_and_update().is_eligible());

    c.publish_checkpoint_assignment_fence(None);
    assert_eq!(
        c.current_leader(),
        Some(owner),
        "transient authority suspension must not make an idle worker leader"
    );

    let newer = CheckpointAssignmentFence::from_owner_map(
        8,
        &[self_id.0],
        vec![CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: c.recovery_incarnation(),
        }],
    )
    .unwrap();
    c.publish_checkpoint_assignment_fence(Some(newer));
    assert_eq!(c.current_leader(), Some(self_id));
    c.publish_checkpoint_assignment_fence(Some(fence));
    assert_eq!(
        c.current_leader(),
        Some(self_id),
        "a delayed older certificate must not restore an obsolete leadership roster"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn durably_fenced_idle_worker_leads_when_every_assignment_owner_is_unavailable() {
    let self_id = NodeId(1);
    let owner = NodeId(2);
    let (_members_tx, members_rx) = watch::channel(vec![info(owner.0)]);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let c = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
    let owner_process = CheckpointParticipant {
        node_id: owner.0,
        boot_incarnation: Uuid::from_u128(22),
    };
    c.publish_checkpoint_assignment_fence(Some(
        CheckpointAssignmentFence::from_owner_map(7, &[owner.0], vec![owner_process]).unwrap(),
    ));
    let (_authority, _proof) = install_recovery_authority(&c, 1_000).await;
    let mut candidacy = c.leader_candidacy_watch();

    assert_eq!(c.current_leader(), Some(owner));
    assert!(!candidacy.borrow_and_update().is_eligible());
    c.note_unresponsive(&[owner]);
    tokio::time::timeout(Duration::from_secs(1), candidacy.changed())
        .await
        .expect("placement fallback candidacy did not update")
        .unwrap();

    assert!(candidacy.borrow_and_update().is_eligible());
    assert_eq!(c.current_leader(), Some(self_id));
    assert!(c.is_leader());
    assert!(!c
        .checkpoint_assignment_fence(7)
        .unwrap()
        .contains(self_id.0));
}

#[test]
fn checkpoint_assignment_fence_rejects_missing_or_suspected_participant() {
    let self_id = NodeId(1);
    let (members_tx, members_rx) = watch::channel(vec![info(2), info(3)]);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let c = ClusterController::new(self_id, kv, None, members_rx);
    let expected = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: c.recovery_incarnation(),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: Uuid::new_v4(),
            },
        ],
    )
    .unwrap();
    c.publish_checkpoint_assignment_fence(Some(expected.clone()));

    assert_eq!(c.checkpoint_assignment_fence(7), Some(expected.clone()));
    assert_eq!(c.checkpoint_assignment_fence(6), None);
    c.note_unresponsive(&[NodeId(2)]);
    assert_eq!(
        c.checkpoint_assignment_fence(7),
        Some(expected.clone()),
        "node-local quorum history must not make the shared assignment proof diverge"
    );

    let mut suspected = info(2);
    suspected.state = NodeState::Suspected;
    members_tx.send(vec![suspected, info(3)]).unwrap();
    assert_eq!(
        c.checkpoint_assignment_fence(7),
        None,
        "a cached fence must close immediately when a participant becomes unavailable"
    );

    members_tx.send(vec![info(2), info(3)]).unwrap();
    assert_eq!(c.checkpoint_assignment_fence(7), Some(expected));
    members_tx.send(vec![info(3)]).unwrap();
    assert_eq!(
        c.checkpoint_assignment_fence(7),
        None,
        "a missing vnode owner must invalidate the cached fence"
    );
}

#[test]
fn quorum_miss_quarantine_requires_an_ack_or_a_different_boot() {
    let c = ctl(1, vec![info(2)]);
    let failed = CheckpointParticipant {
        node_id: 2,
        boot_incarnation: Uuid::from_u128(22),
    };
    let fence = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: c.recovery_incarnation(),
            },
            failed,
        ],
    )
    .unwrap();
    c.publish_checkpoint_assignment_fence(Some(fence));
    c.note_unresponsive(&[NodeId(2)]);

    assert!(c.is_unresponsive(NodeId(2)));
    assert!(!c.admit_successor_process(failed));
    assert!(c.admit_successor_process(CheckpointParticipant {
        node_id: 2,
        boot_incarnation: Uuid::from_u128(23),
    }));
    assert!(!c.is_unresponsive(NodeId(2)));
}

#[test]
fn recovery_ack_clears_only_the_exact_quarantined_boot() {
    let c = ctl(1, vec![info(2)]);
    let failed = CheckpointParticipant {
        node_id: 2,
        boot_incarnation: Uuid::from_u128(22),
    };
    let successor = CheckpointParticipant {
        node_id: 2,
        boot_incarnation: Uuid::from_u128(23),
    };
    let fence = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: c.recovery_incarnation(),
            },
            failed,
        ],
    )
    .unwrap();
    c.publish_checkpoint_assignment_fence(Some(fence));
    c.note_unresponsive(&[NodeId(2)]);

    c.note_recovery_responsive(&[successor]);
    assert!(
        c.is_unresponsive(NodeId(2)),
        "an acknowledgement from another boot must not clear the failed boot"
    );
    c.note_recovery_responsive(&[failed]);
    assert!(!c.is_unresponsive(NodeId(2)));

    c.publish_checkpoint_assignment_fence(None);
    c.note_unresponsive(&[NodeId(2)]);
    c.note_recovery_responsive(&[failed]);
    assert!(
        c.is_unresponsive(NodeId(2)),
        "an unbound quarantine requires an ordinary checkpoint acknowledgement"
    );
}

#[test]
fn assignable_with_locality_attaches_self_and_peer_domains() {
    let mut peer = info(3);
    peer.metadata.failure_domain = Some("region=r;zone=z2".to_string());
    let c = ctl(1, vec![peer]);
    c.set_self_locality(Locality::parse("region=r;zone=z1"));

    let pairs = c.assignable_with_locality();
    // Same set as assignable_instances (self + active peer), sorted by id.
    let ids: Vec<NodeId> = pairs.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![NodeId(1), NodeId(3)]);
    // Self's locality comes from set_self_locality; peer's from gossip.
    let self_loc = &pairs.iter().find(|(id, _)| *id == NodeId(1)).unwrap().1;
    let peer_loc = &pairs.iter().find(|(id, _)| *id == NodeId(3)).unwrap().1;
    assert_eq!(self_loc.domain_at(1), "r;z1");
    assert_eq!(peer_loc.domain_at(1), "r;z2");
}

#[test]
fn assignable_with_locality_defaults_unlabeled_to_empty_domain() {
    // A peer with no failure_domain and unset self locality both collapse
    // to the empty "unknown" domain — safe degradation, never a panic.
    let c = ctl(1, vec![info(3)]);
    let pairs = c.assignable_with_locality();
    assert_eq!(pairs.len(), 2);
    assert!(pairs.iter().all(|(_, loc)| loc.domain_at(0).is_empty()));
}

#[tokio::test]
async fn announce_observe_roundtrip_when_alone() {
    // Single-instance: self == leader; own announcement is visible
    // to own observe.
    let c = ctl(1, vec![]);
    c.announce_barrier(&BarrierAnnouncement {
        epoch: 5,
        checkpoint_id: 5,
        assignment_fence: None,
        leader_proof: None,
        phase: crate::cluster::control::Phase::Prepare,
        flags: 0,
    })
    .await
    .unwrap();
    let got = c.observe_barrier_matching(|_| true).await.unwrap().unwrap();
    assert_eq!(got.epoch, 5);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn wait_for_barrier_propagates_observation_failure_immediately() {
    let c = ctl(1, vec![]);
    c.kv.write(ANNOUNCEMENT_KEY, "not-json".into()).await;

    let error = c
        .wait_for_barrier(|_| true, Duration::from_secs(10))
        .await
        .expect_err("malformed control history must fail instead of timing out");

    assert!(
        error.contains("malformed durable barrier announcement"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn wait_for_barrier_bounds_a_pending_initial_observation() {
    let kv: Arc<dyn ClusterKv> = Arc::new(PendingAnnouncementReadKv);
    let (_members_tx, members_rx) = watch::channel(vec![info(1)]);
    let follower = ClusterController::new(NodeId(2), kv, None, members_rx);
    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_millis(100);

    let observed = follower
        .wait_for_barrier(|_| true, Duration::from_millis(100))
        .await
        .expect("a pending observation must become an ordinary bounded miss");

    assert!(observed.is_none());
    assert_eq!(tokio::time::Instant::now(), deadline);
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn wait_for_barrier_validates_authority_only_after_predicate_match() {
    let follower_kv = Arc::new(InMemoryKv::new(NodeId(2)));
    let (_members_tx, members_rx) = watch::channel(vec![info(1)]);
    let follower = ClusterController::new(NodeId(2), follower_kv.clone(), None, members_rx);
    follower.set_leader_lease_store(Arc::new(super::super::LeaderLeaseStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        1_000,
    )));
    let proof = test_leader_proof(1, Uuid::from_u128(11), 1);
    let fence = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: proof.owner.boot_id,
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: follower.recovery_incarnation(),
            },
        ],
    )
    .unwrap();
    let mut announcement = BarrierAnnouncement {
        epoch: 9,
        checkpoint_id: 9,
        assignment_fence: Some(fence),
        leader_proof: Some(proof),
        phase: Phase::Prepare,
        flags: 0,
    };
    follower_kv.seed(
        NodeId(1),
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&announcement).unwrap(),
    );

    let observed = follower
        .wait_for_barrier(
            |candidate| candidate.phase == Phase::Aligned,
            Duration::from_millis(30),
        )
        .await
        .expect("a nonmatching reversible hint must not read authority");
    assert!(observed.is_none());

    announcement.phase = Phase::Aligned;
    follower_kv.seed(
        NodeId(1),
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&announcement).unwrap(),
    );
    let error = follower
        .wait_for_barrier(
            |candidate| candidate.phase == Phase::Aligned,
            Duration::from_millis(30),
        )
        .await
        .expect_err("a matching reversible hint must validate current authority");
    assert!(error.contains("no durable leader lease exists"), "{error}");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn checkpoint_prepare_without_assignment_still_requires_durable_authority() {
    let follower_kv = Arc::new(InMemoryKv::new(NodeId(2)));
    let (_members_tx, members_rx) = watch::channel(vec![info(1)]);
    let follower = ClusterController::new(NodeId(2), follower_kv.clone(), None, members_rx);
    follower.set_leader_lease_store(Arc::new(super::super::LeaderLeaseStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        1_000,
    )));
    follower_kv.seed(
        NodeId(1),
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&BarrierAnnouncement {
            epoch: 9,
            checkpoint_id: 9,
            assignment_fence: None,
            leader_proof: Some(test_leader_proof(1, Uuid::from_u128(11), 1)),
            phase: Phase::Prepare,
            flags: 0,
        })
        .unwrap(),
    );

    let error = follower
        .observe_checkpoint_prepare()
        .await
        .expect_err("a malformed Prepare must not bypass durable leader validation");
    assert!(error.contains("no durable leader lease exists"), "{error}");
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn checkpoint_prepare_hint_observation_stops_at_caller_timeout() {
    let kv: Arc<dyn ClusterKv> = Arc::new(PendingAnnouncementReadKv);
    let (_members_tx, members_rx) = watch::channel(vec![info(1)]);
    let follower = ClusterController::new(NodeId(2), kv, None, members_rx);
    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_millis(100);

    let error = follower
        .observe_checkpoint_prepare_until(Duration::from_millis(100))
        .await
        .expect_err("a stalled Prepare hint read must respect the caller timeout");

    assert!(error.contains("hint observation timed out"), "{error}");
    assert_eq!(tokio::time::Instant::now(), deadline);
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn stale_direct_prepare_does_not_shorten_newer_gossip_observation() {
    use crate::cluster::control::{LeaderLeaseOwner, LeaseOutcome};

    let follower_kv = Arc::new(InMemoryKv::new(NodeId(2)));
    let control_kv: Arc<dyn ClusterKv> = follower_kv.clone();
    let (_members_tx, members_rx) = watch::channel(vec![info(1)]);
    let follower = ClusterController::new(NodeId(2), control_kv, None, members_rx);

    let authority = Arc::new(super::super::LeaderLeaseStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        1_000,
    ));
    let leader_boot = Uuid::from_u128(11);
    let owner = LeaderLeaseOwner {
        node: NodeId(1),
        boot: leader_boot,
        process_term: 3,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty leader authority must be acquired");
    };
    follower.set_leader_lease_store(authority);

    let fence = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: leader_boot,
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: follower.recovery_incarnation(),
            },
        ],
    )
    .unwrap();
    follower.publish_checkpoint_assignment_fence(Some(fence.clone()));
    follower
        .start_barrier_server("127.0.0.1:0".parse().unwrap(), None)
        .await
        .unwrap();

    let old_prepare = BarrierAnnouncement {
        epoch: 9,
        checkpoint_id: 9,
        assignment_fence: Some(fence.clone()),
        leader_proof: Some(lease.proof()),
        phase: Phase::Prepare,
        flags: 0,
    };
    let old_received_at = tokio::time::Instant::now();
    follower
        .barrier
        .inject_direct_prepare_observation_for_test(old_prepare.clone(), old_received_at.into_std())
        .await
        .unwrap();
    assert_eq!(
        follower.checkpoint_prepare_received_at(&old_prepare),
        Some(old_received_at.into_std())
    );

    let checkpoint_timeout = Duration::from_millis(100);
    tokio::time::advance(checkpoint_timeout + Duration::from_millis(1)).await;
    let newer_prepare = BarrierAnnouncement {
        epoch: 10,
        checkpoint_id: 10,
        ..old_prepare
    };
    follower_kv.seed(
        NodeId(1),
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&newer_prepare).unwrap(),
    );

    let observed_at = tokio::time::Instant::now();
    let observed = follower
        .observe_checkpoint_prepare_until(checkpoint_timeout)
        .await
        .expect("an unrelated expired direct Prepare must not block newer gossip")
        .expect("the newer gossip Prepare must be observed");
    let CheckpointPrepareObservation::AssignmentReady(observed) = observed else {
        panic!("the newer gossip Prepare must match the installed assignment");
    };
    assert_eq!(observed, newer_prepare);
    assert_eq!(
        follower.checkpoint_prepare_received_at(&observed),
        Some(observed_at.into_std()),
        "the newer identity must receive its own non-refreshed observation clock"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn checkpoint_prepare_authority_timeout_does_not_refresh_gossip_identity_clock() {
    use crate::cluster::control::{LeaderLeaseOwner, LeaseOutcome};

    let follower_kv = Arc::new(InMemoryKv::new(NodeId(2)));
    let control_kv: Arc<dyn ClusterKv> = follower_kv.clone();
    let (_members_tx, members_rx) = watch::channel(vec![info(1)]);
    let follower = ClusterController::new(NodeId(2), control_kv, None, members_rx);

    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let gate = Arc::new(AuthorityIoGateStore::new(
        backing,
        AuthorityIoGateOperation::Get,
    ));
    let gated_backing: Arc<dyn object_store::ObjectStore> = gate.clone();
    let authority = Arc::new(super::super::LeaderLeaseStore::new(gated_backing, 1_000));
    let leader_boot = Uuid::from_u128(11);
    let owner = LeaderLeaseOwner {
        node: NodeId(1),
        boot: leader_boot,
        process_term: 3,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty leader authority must be acquired");
    };
    follower.set_leader_lease_store(authority);

    let fence = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: leader_boot,
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: follower.recovery_incarnation(),
            },
        ],
    )
    .unwrap();
    follower.publish_checkpoint_assignment_fence(Some(fence.clone()));
    let announcement = BarrierAnnouncement {
        epoch: 9,
        checkpoint_id: 9,
        assignment_fence: Some(fence),
        leader_proof: Some(lease.proof()),
        phase: Phase::Prepare,
        flags: 0,
    };
    follower_kv.seed(
        NodeId(1),
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&announcement).unwrap(),
    );

    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_millis(100);
    gate.arm();
    let observed = follower.observe_checkpoint_prepare_until(Duration::from_millis(100));
    tokio::pin!(observed);
    tokio::select! {
        () = gate.wait_until_blocked() => {}
        result = &mut observed => panic!("authority validation returned before its gate: {result:?}"),
    }
    tokio::time::advance(Duration::from_millis(100)).await;
    let error = observed
        .await
        .expect_err("a stalled authority read must respect the exact Prepare deadline");
    assert!(error.contains("authority validation timed out"), "{error}");
    assert_eq!(tokio::time::Instant::now(), deadline);
    assert_eq!(
        follower.checkpoint_prepare_received_at(&announcement),
        Some(started.into_std())
    );

    let retry_started = tokio::time::Instant::now();
    let retry_error = follower
        .observe_checkpoint_prepare_until(Duration::from_millis(100))
        .await
        .expect_err("re-observation must not refresh the exact identity deadline");
    assert!(
        retry_error.contains("deadline elapsed before authority validation"),
        "{retry_error}"
    );
    assert_eq!(tokio::time::Instant::now(), retry_started);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn checkpoint_prepare_reports_exact_local_assignment_disposition() {
    use crate::cluster::control::{LeaderLeaseOwner, LeaseOutcome};

    let follower_kv = Arc::new(InMemoryKv::new(NodeId(2)));
    let (_members_tx, members_rx) = watch::channel(vec![info(1)]);
    let follower = ClusterController::new(NodeId(2), follower_kv.clone(), None, members_rx);
    let authority = Arc::new(super::super::LeaderLeaseStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        1_000,
    ));
    let leader_boot = Uuid::from_u128(11);
    let owner = LeaderLeaseOwner {
        node: NodeId(1),
        boot: leader_boot,
        process_term: 3,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty leader authority must be acquired");
    };
    follower.set_leader_lease_store(authority);

    let participants = vec![
        CheckpointParticipant {
            node_id: 1,
            boot_incarnation: leader_boot,
        },
        CheckpointParticipant {
            node_id: 2,
            boot_incarnation: follower.recovery_incarnation(),
        },
    ];
    let announced =
        CheckpointAssignmentFence::from_owner_map(7, &[1, 2], participants.clone()).unwrap();
    follower.publish_checkpoint_assignment_fence(Some(announced.clone()));
    let announcement = BarrierAnnouncement {
        epoch: 9,
        checkpoint_id: 9,
        assignment_fence: Some(announced.clone()),
        leader_proof: Some(lease.proof()),
        phase: Phase::Prepare,
        flags: 0,
    };
    follower_kv.seed(
        NodeId(1),
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&announcement).unwrap(),
    );

    let ready = follower
        .observe_checkpoint_prepare()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ready,
        CheckpointPrepareObservation::AssignmentReady(announcement.clone())
    );

    let different_local =
        CheckpointAssignmentFence::from_owner_map(7, &[2, 1], participants).unwrap();
    follower.publish_checkpoint_assignment_fence(Some(different_local));
    let rejected = follower
        .observe_checkpoint_prepare()
        .await
        .unwrap()
        .unwrap();
    let CheckpointPrepareObservation::AssignmentRejected {
        announcement: rejected_announcement,
        error,
    } = rejected
    else {
        panic!("a different local owner map must be rejected");
    };
    assert_eq!(rejected_announcement, announcement);
    assert!(error.contains("follower assignment differs"), "{error}");

    let mut stale = announcement;
    stale.leader_proof.as_mut().unwrap().fencing_token += 1;
    follower_kv.seed(
        NodeId(1),
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&stale).unwrap(),
    );
    let error = follower
        .observe_checkpoint_prepare()
        .await
        .expect_err("a stale leader token must fail before assignment disposition");
    assert!(
        error.contains("does not match the latest durable leader lease"),
        "{error}"
    );
}

#[tokio::test]
async fn adopted_assignment_report_binds_the_current_process_and_exact_map() {
    let c = ctl(1, vec![]);
    c.publish_recovery_incarnation().await.unwrap();
    let owners = [1, 1, 1];
    let report = CheckpointAssignmentAdoption {
        participant: CheckpointParticipant {
            node_id: 1,
            boot_incarnation: c.recovery_incarnation(),
        },
        assignment_version: 7,
        partitioning_abi_version: crate::state::PARTITIONING_ABI_VERSION,
        vnode_count: u32::try_from(owners.len()).unwrap(),
        assignment_digest: CheckpointAssignmentFence::owner_map_digest(3, &owners),
        vnode_state_ready: true,
    };
    c.announce_adopted_assignment(&report).await.unwrap();
    assert_eq!(
        c.read_adopted_assignments().await.unwrap(),
        vec![(NodeId(1), report.clone())]
    );

    let mut withdrawn = report;
    withdrawn.vnode_state_ready = false;
    c.announce_adopted_assignment(&withdrawn).await.unwrap();
    assert_eq!(
        c.read_adopted_assignments().await.unwrap(),
        vec![(NodeId(1), withdrawn.clone())]
    );

    let mut restarted = withdrawn;
    restarted.participant.boot_incarnation = Uuid::new_v4();
    assert!(c.announce_adopted_assignment(&restarted).await.is_err());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn local_process_authority_evidence_is_exact_compact_and_round_trips() {
    let node = NodeId(1);
    let boot = Uuid::from_u128(101);
    let kv = Arc::new(InMemoryKv::new(node));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv.clone();
    let controller = ClusterController::new_with_recovery_incarnation(
        node,
        control,
        recovery,
        None,
        watch::channel(Vec::new()).1,
        boot,
    );
    let (_authority, proof) = install_recovery_authority(&controller, 1_000).await;
    let adoption = assignment_adoption(&controller, 7);
    controller
        .announce_adopted_assignment(&adoption)
        .await
        .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(assignment_fence_for_adoption(&adoption)));

    let evidence = controller
        .read_local_process_authority_evidence()
        .await
        .unwrap();
    assert_eq!(evidence.participant, adoption.participant);
    assert_eq!(evidence.process_term, proof.owner.process_term);
    assert_eq!(evidence.adopted_assignment, adoption);

    let identity = controller
        .try_live_local_process_authority_identity()
        .unwrap();
    assert!(identity.is_canonical());
    assert_eq!(identity.participant, evidence.participant);
    assert_eq!(identity.process_term, evidence.process_term);

    let transition = controller.process_authority_transition.lock();
    let error = controller
        .try_live_local_process_authority_identity()
        .expect_err("the nonblocking sampler must not wait behind an authority transition");
    assert!(error.contains("transition is in progress"), "{error}");
    drop(transition);

    let encoded = serde_json::to_vec(&evidence).unwrap();
    assert!(encoded.len() <= 4 * 1_024, "{} bytes", encoded.len());
    assert_eq!(
        serde_json::from_slice::<LocalProcessAuthorityEvidence>(&encoded).unwrap(),
        evidence
    );

    controller.fence_process_lease();
    let error = controller
        .try_live_local_process_authority_identity()
        .expect_err("a terminally fenced process must not expose live identity");
    assert!(error.contains("not live"), "{error}");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn local_process_authority_evidence_requires_the_live_exact_assignment_fence() {
    let node = NodeId(1);
    let boot = Uuid::from_u128(151);
    let kv = Arc::new(InMemoryKv::new(node));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv;
    let controller = ClusterController::new_with_recovery_incarnation(
        node,
        control,
        recovery,
        None,
        watch::channel(Vec::new()).1,
        boot,
    );
    install_recovery_authority(&controller, 1_000).await;
    let error = controller
        .read_local_process_authority_evidence()
        .await
        .unwrap_err();
    assert!(
        matches!(
            &error,
            LocalProcessAuthorityEvidenceError::Unavailable(reason)
                if reason.contains("no durable assignment adoption")
        ),
        "{error}"
    );
    let adoption = assignment_adoption(&controller, 7);
    controller
        .announce_adopted_assignment(&adoption)
        .await
        .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(assignment_fence_for_adoption(&adoption)));
    assert_eq!(
        controller
            .read_local_process_authority_evidence()
            .await
            .unwrap()
            .adopted_assignment,
        adoption.clone()
    );

    controller.publish_checkpoint_assignment_fence(None);
    let error = controller
        .read_local_process_authority_evidence()
        .await
        .unwrap_err();
    assert!(
        matches!(
            &error,
            LocalProcessAuthorityEvidenceError::Unavailable(reason)
                if reason.contains("audited assignment fence is unavailable")
        ),
        "durable publication alone must not survive local watcher suspension: {error}"
    );

    let mismatched_fence = CheckpointAssignmentFence::from_owner_map(
        adoption.assignment_version,
        &[node.0, node.0],
        vec![adoption.participant],
    )
    .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(mismatched_fence));
    let error = controller
        .read_local_process_authority_evidence()
        .await
        .unwrap_err();
    assert!(
        matches!(
            &error,
            LocalProcessAuthorityEvidenceError::Invalid(reason)
                if reason.contains("contradicts the same-version")
        ),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn local_process_authority_evidence_ignores_canonical_prior_boot_adoption() {
    let node = NodeId(1);
    let boot = Uuid::from_u128(201);
    let prior_boot = Uuid::from_u128(200);
    let kv = Arc::new(InMemoryKv::new(node));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv.clone();
    let controller = ClusterController::new_with_recovery_incarnation(
        node,
        control,
        recovery,
        None,
        watch::channel(Vec::new()).1,
        boot,
    );
    install_recovery_authority(&controller, 1_000).await;

    let current_adoption = assignment_adoption(&controller, 6);
    controller.publish_checkpoint_assignment_fence(Some(assignment_fence_for_adoption(
        &current_adoption,
    )));
    let mut stale_adoption = current_adoption;
    stale_adoption.participant.boot_incarnation = prior_boot;
    kv.seed(
        node,
        ADOPTED_ASSIGNMENT_KEY,
        serde_json::to_string(&stale_adoption).unwrap(),
    );
    let error = controller
        .read_local_process_authority_evidence()
        .await
        .unwrap_err();
    assert!(
        matches!(
            &error,
            LocalProcessAuthorityEvidenceError::Unavailable(reason)
                if reason.contains("prior local boot")
        ),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn local_process_authority_evidence_rejects_noncanonical_or_oversized_adoption() {
    let node = NodeId(1);
    let boot = Uuid::from_u128(301);
    let kv = Arc::new(InMemoryKv::new(node));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv.clone();
    let controller = ClusterController::new_with_recovery_incarnation(
        node,
        control,
        recovery,
        None,
        watch::channel(Vec::new()).1,
        boot,
    );
    install_recovery_authority(&controller, 1_000).await;
    let adoption = assignment_adoption(&controller, 7);
    let mut adoption_with_unknown_field = serde_json::to_value(&adoption).unwrap();
    adoption_with_unknown_field
        .as_object_mut()
        .unwrap()
        .insert("unexpected".into(), serde_json::json!(true));
    kv.seed(
        node,
        ADOPTED_ASSIGNMENT_KEY,
        serde_json::to_string(&adoption_with_unknown_field).unwrap(),
    );
    let error = controller
        .read_local_process_authority_evidence()
        .await
        .unwrap_err();
    assert!(
        matches!(
            &error,
            LocalProcessAuthorityEvidenceError::Invalid(reason)
                if reason.contains("unknown field")
        ),
        "{error}"
    );

    kv.seed(
        node,
        ADOPTED_ASSIGNMENT_KEY,
        "x".repeat(MAX_ADOPTED_ASSIGNMENT_BYTES + 1),
    );
    let error = controller
        .read_local_process_authority_evidence()
        .await
        .unwrap_err();
    assert!(
        matches!(
            &error,
            LocalProcessAuthorityEvidenceError::Invalid(reason)
                if reason.contains("expected 1..=")
        ),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn local_process_authority_evidence_revalidates_lease_after_point_reads() {
    let node = NodeId(1);
    let boot = Uuid::from_u128(401);
    let recovery = Arc::new(EvidenceReadGateKv::new(node));
    let control: Arc<dyn ClusterKv> = recovery.clone();
    let recovery_kv: Arc<dyn ClusterKv> = recovery.clone();
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node,
        control,
        recovery_kv,
        None,
        watch::channel(Vec::new()).1,
        boot,
    ));
    install_recovery_authority(&controller, 1_000).await;
    let adoption = assignment_adoption(&controller, 7);
    controller
        .announce_adopted_assignment(&adoption)
        .await
        .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(assignment_fence_for_adoption(&adoption)));

    recovery.arm();
    let reading = {
        let controller = Arc::clone(&controller);
        tokio::spawn(async move { controller.read_local_process_authority_evidence().await })
    };
    tokio::time::timeout(Duration::from_secs(1), recovery.wait_until_blocked())
        .await
        .expect("local evidence read did not reach the injected point-read gate");
    controller.fence_process_lease();
    recovery.release_blocked_read();

    let error = reading.await.unwrap().unwrap_err();
    assert!(
        matches!(
            &error,
            LocalProcessAuthorityEvidenceError::Unavailable(reason)
                if reason.contains("lease authority")
        ),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn drain_quorum_requires_the_exact_current_boot_roster_and_certificate() {
    let self_id = NodeId(1);
    let peer_id = NodeId(2);
    let self_boot = Uuid::from_u128(11);
    let peer_boot = Uuid::from_u128(22);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv.clone();
    let assignment_store = Arc::new(AssignmentSnapshotStore::new(Arc::new(
        object_store::memory::InMemory::new(),
    )));
    let (_members_tx, members_rx) = watch::channel(vec![info(peer_id.0)]);
    let controller = ClusterController::new_with_recovery_incarnation(
        self_id,
        control,
        recovery,
        Some(Arc::clone(&assignment_store)),
        members_rx,
        self_boot,
    );
    controller.publish_recovery_incarnation().await.unwrap();
    kv.seed(peer_id, RECOVERY_INCARNATION_KEY, peer_boot.to_string());
    let participants = vec![
        CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: self_boot,
        },
        CheckpointParticipant {
            node_id: peer_id.0,
            boot_incarnation: peer_boot,
        },
    ];
    let first = super::super::AssignmentSnapshot::empty()
        .next_for_participants(
            std::collections::BTreeMap::from([(0, self_id), (1, peer_id)]),
            participants.clone(),
        )
        .unwrap();
    assignment_store.save_if_absent(&first).await.unwrap();
    let predecessor_snapshot = first
        .next_for_participants(
            std::collections::BTreeMap::from([(0, self_id), (1, peer_id)]),
            participants.clone(),
        )
        .unwrap();
    assert!(matches!(
        assignment_store
            .save_if_version(&predecessor_snapshot, first.version)
            .await
            .unwrap(),
        super::super::RotateOutcome::Rotated
    ));
    let authority = Arc::new(super::super::LeaderLeaseStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        1_000,
    ));
    let owner = super::super::LeaderLeaseOwner {
        node: self_id,
        boot: self_boot,
        process_term: 1,
    };
    let super::super::LeaseOutcome::Acquired(lease) =
        authority.begin_new_term(&owner, 1).await.unwrap()
    else {
        panic!("empty authority must be acquired");
    };
    controller.set_leader_lease_store(authority);
    let draining = predecessor_snapshot
        .next_draining(
            std::collections::BTreeMap::from([(0, peer_id), (1, self_id)]),
            participants.clone(),
            lease.proof(),
        )
        .unwrap();
    assert!(matches!(
        assignment_store
            .save_if_version(&draining, predecessor_snapshot.version)
            .await
            .unwrap(),
        super::super::RotateOutcome::Rotated
    ));
    let transition = draining.drain_transition.clone().unwrap();
    controller.publish_checkpoint_drain_transition(Some(transition.clone()));
    controller.announce_drain_ack(&transition).await.unwrap();
    assert!(
        !controller
            .drain_ack_quorum_reached(&transition)
            .await
            .unwrap(),
        "one acknowledgement cannot satisfy a two-process roster"
    );

    let seed_peer_ack =
        |ack: DrainAck| kv.seed(peer_id, DRAIN_ACK_KEY, encode_drain_ack(&ack).unwrap());
    seed_peer_ack(DrainAck::for_transition(
        CheckpointParticipant {
            node_id: peer_id.0,
            boot_incarnation: Uuid::from_u128(21),
        },
        &transition,
    ));
    assert!(!controller
        .drain_ack_quorum_reached(&transition)
        .await
        .unwrap());

    let stale_predecessor = first.assignment_fence().unwrap();
    let stale_transition = AssignmentDrainTransition::new(
        stale_predecessor,
        transition.predecessor.clone(),
        transition.leader.clone(),
    )
    .unwrap();
    seed_peer_ack(DrainAck::for_transition(participants[1], &stale_transition));
    assert!(!controller
        .drain_ack_quorum_reached(&transition)
        .await
        .unwrap());

    let future_target = CheckpointAssignmentFence::from_owner_map(
        transition.target.assignment_version + 1,
        &[self_id.0, peer_id.0],
        participants.clone(),
    )
    .unwrap();
    let future_transition = AssignmentDrainTransition::new(
        transition.target.clone(),
        future_target,
        transition.leader.clone(),
    )
    .unwrap();
    seed_peer_ack(DrainAck::for_transition(
        participants[1],
        &future_transition,
    ));
    assert!(!controller
        .drain_ack_quorum_reached(&transition)
        .await
        .unwrap());

    let other_target = CheckpointAssignmentFence::from_owner_map(
        transition.target.assignment_version,
        &[self_id.0, peer_id.0],
        participants.clone(),
    )
    .unwrap();
    let other_transition = AssignmentDrainTransition::new(
        transition.predecessor.clone(),
        other_target,
        transition.leader.clone(),
    )
    .unwrap();
    seed_peer_ack(DrainAck::for_transition(participants[1], &other_transition));
    assert!(!controller
        .drain_ack_quorum_reached(&transition)
        .await
        .unwrap());

    seed_peer_ack(DrainAck::for_transition(participants[1], &transition));
    assert!(controller
        .drain_ack_quorum_reached(&transition)
        .await
        .unwrap());
    controller
        .announce_drain_ack(&transition)
        .await
        .expect("an exact retry is idempotent");
    assert!(controller
        .drain_ack_quorum_reached(&transition)
        .await
        .unwrap());

    kv.seed(
        peer_id,
        RECOVERY_INCARNATION_KEY,
        Uuid::from_u128(23).to_string(),
    );
    assert!(
        !controller
            .drain_ack_quorum_reached(&transition)
            .await
            .unwrap(),
        "a restart invalidates an acknowledgement from the previous boot"
    );
    kv.seed(peer_id, RECOVERY_INCARNATION_KEY, peer_boot.to_string());
    assert!(controller
        .drain_ack_quorum_reached(&transition)
        .await
        .unwrap());

    let committed = draining.committed_target().unwrap();
    assert!(matches!(
        assignment_store
            .finalize_drain(&draining, &committed)
            .await
            .unwrap(),
        super::super::RotateOutcome::Rotated
    ));
    assert!(
        !controller
            .drain_ack_quorum_reached(&transition)
            .await
            .unwrap(),
        "immutable receipts cannot authorize HANDOFF after terminal materialization"
    );
}

#[test]
fn drain_ack_encoding_is_canonical_and_bounded() {
    let predecessor = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1],
        vec![CheckpointParticipant {
            node_id: 1,
            boot_incarnation: Uuid::from_u128(11),
        }],
    )
    .unwrap();
    let target =
        CheckpointAssignmentFence::from_owner_map(8, &[1], predecessor.participants.clone())
            .unwrap();
    let transition = AssignmentDrainTransition::new(
        predecessor.clone(),
        target,
        crate::checkpoint::LeaderProof {
            owner: crate::checkpoint::LeaderProofOwner {
                node_id: 1,
                boot_id: Uuid::from_u128(11),
                process_term: 1,
            },
            fencing_token: 1,
        },
    )
    .unwrap();
    let ack = DrainAck::for_transition(predecessor.participants[0], &transition);
    let encoded = encode_drain_ack(&ack).unwrap();
    assert!(encoded.len() <= MAX_DRAIN_ACK_BYTES);
    assert_eq!(parse_drain_ack(&encoded, NodeId(1)).unwrap(), ack);
    assert!(parse_drain_ack(&format!(" {encoded}"), NodeId(1)).is_err());
    assert!(parse_drain_ack(&"x".repeat(MAX_DRAIN_ACK_BYTES + 1), NodeId(1)).is_err());
    let mut noncanonical = ack;
    noncanonical.round.target_version += 1;
    assert!(encode_drain_ack(&noncanonical).is_err());
    assert!(parse_drain_ack(&serde_json::to_string(&noncanonical).unwrap(), NodeId(1)).is_err());

    let mut prior_protocol = DrainAck::for_transition(predecessor.participants[0], &transition);
    prior_protocol.protocol_version = DRAIN_ACK_PROTOCOL_VERSION - 1;
    assert!(encode_drain_ack(&prior_protocol).is_err());
    assert!(parse_drain_ack(&serde_json::to_string(&prior_protocol).unwrap(), NodeId(1)).is_err());
}

#[test]
fn publish_cluster_min_watermark_is_monotonic() {
    // Both leaders and followers install only decision-bound recovery frontiers here.
    let c = ctl(1, vec![]);
    assert_eq!(c.cluster_min_watermark(), None);

    c.publish_cluster_min_watermark(100);
    assert_eq!(c.cluster_min_watermark(), Some(100));

    // Higher value advances.
    c.publish_cluster_min_watermark(250);
    assert_eq!(c.cluster_min_watermark(), Some(250));

    // Lower value must not regress.
    c.publish_cluster_min_watermark(42);
    assert_eq!(c.cluster_min_watermark(), Some(250));

    // Equal value is a no-op; still Some(250).
    c.publish_cluster_min_watermark(250);
    assert_eq!(c.cluster_min_watermark(), Some(250));
}

#[test]
fn committed_channel_progress_publishes_source_specific_frontiers() {
    use crate::checkpoint::ChannelProgress;

    let channel = |source_name: &str, input_channel: u8, watermark: Option<i64>, idle: bool| {
        ChannelProgress {
            participant_id: 1,
            source_name: source_name.into(),
            input_channel: vec![input_channel],
            watermark,
            idle,
        }
    };
    let c = ctl(1, vec![]);
    c.publish_committed_channel_progress(&[
        channel("fast", 0, Some(1_000), false),
        channel("fast", 1, Some(900), false),
        channel("slow", 0, Some(100), false),
        channel("all_idle", 0, Some(300), true),
        channel("all_idle", 1, Some(400), true),
        channel("mixed", 0, Some(250), false),
        channel("mixed", 1, Some(900), true),
    ])
    .unwrap();

    assert_eq!(c.cluster_min_watermark(), Some(100));
    let first = c.committed_source_watermarks_snapshot();
    assert_eq!(first.get("fast"), Some(&900));
    assert_eq!(first.get("slow"), Some(&100));
    assert_eq!(first.get("all_idle"), Some(&400));
    assert_eq!(first.get("mixed"), Some(&250));
    assert_eq!(first.get("absent"), None);

    // A later publication cannot regress an already installed source frontier.
    c.publish_committed_channel_progress(&[
        channel("fast", 0, Some(800), false),
        channel("slow", 0, Some(200), false),
    ])
    .unwrap();
    assert_eq!(c.cluster_min_watermark(), Some(200));
    let second = c.committed_source_watermarks_snapshot();
    assert_eq!(second.get("fast"), Some(&900));
    assert_eq!(second.get("slow"), Some(&200));
    assert_eq!(first.get("slow"), Some(&100));

    let withheld = ctl(1, vec![]);
    withheld
        .publish_committed_channel_progress(&[
            channel("withheld", 0, None, false),
            channel("withheld", 1, Some(700), true),
        ])
        .unwrap();
    assert_eq!(withheld.cluster_min_watermark(), None);
    assert!(
        !withheld
            .committed_source_watermarks_snapshot()
            .contains_key("withheld"),
        "an idle initialized channel must not override an active uninitialized sibling"
    );
}

#[test]
fn committed_checkpoint_progress_restores_an_empty_sources_retained_frontier() {
    let controller = ctl(1, vec![]);
    let retained = std::collections::BTreeMap::from([("orders".to_owned(), 900)]);

    controller
        .publish_committed_checkpoint_progress(&[], &retained)
        .unwrap();

    assert_eq!(controller.cluster_min_watermark(), None);
    assert_eq!(
        controller
            .committed_source_watermarks_snapshot()
            .get("orders"),
        Some(&900)
    );
}

#[test]
fn recovered_checkpoint_progress_exactly_replaces_a_newer_live_cut_and_genesis_clears_it() {
    use crate::checkpoint::ChannelProgress;

    let channel = |watermark| ChannelProgress {
        participant_id: 1,
        source_name: "orders".into(),
        input_channel: vec![0],
        watermark: Some(watermark),
        idle: false,
    };
    let controller = ctl(1, vec![]);
    controller
        .publish_committed_checkpoint_progress(
            &[channel(900)],
            &std::collections::BTreeMap::from([("orders".to_owned(), 900)]),
        )
        .unwrap();
    let newer_snapshot = controller.committed_source_watermarks_snapshot();

    controller
        .replace_recovered_checkpoint_progress(
            &[channel(100)],
            &std::collections::BTreeMap::from([("orders".to_owned(), 100)]),
        )
        .unwrap();
    assert_eq!(controller.cluster_min_watermark(), Some(100));
    assert_eq!(
        controller
            .committed_source_watermarks_snapshot()
            .get("orders"),
        Some(&100)
    );
    assert_eq!(newer_snapshot.get("orders"), Some(&900));

    controller
        .replace_recovered_checkpoint_progress(&[], &std::collections::BTreeMap::new())
        .unwrap();
    assert_eq!(controller.cluster_min_watermark(), None);
    assert!(controller.committed_source_watermarks_snapshot().is_empty());
}

async fn install_recovery_authority(
    controller: &ClusterController,
    ttl_ms: i64,
) -> (Arc<super::super::LeaderLeaseStore>, LeaderProof) {
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    install_recovery_authority_with_store(controller, ttl_ms, backing).await
}

async fn install_recovery_authority_with_store(
    controller: &ClusterController,
    ttl_ms: i64,
    backing: Arc<dyn object_store::ObjectStore>,
) -> (Arc<super::super::LeaderLeaseStore>, LeaderProof) {
    use super::super::{
        LeaderLeaseOwner, LeaseDeadline, LeaseOutcome, ProcessLeaseAuthority, ProcessLeaseOutcome,
    };

    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_millis(1)).unwrap(),
    );
    let ProcessLeaseOutcome::Acquired(process_lease) = process_authority
        .store_for(controller.instance_id())
        .try_acquire(controller.recovery_incarnation(), 0)
        .await
        .unwrap()
    else {
        panic!("empty process authority must be acquired");
    };
    controller
        .set_process_lease_authority(process_authority)
        .unwrap();
    let authority = Arc::new(super::super::LeaderLeaseStore::new(backing, ttl_ms));
    let owner = LeaderLeaseOwner {
        node: controller.instance_id(),
        boot: controller.recovery_incarnation(),
        process_term: process_lease.term,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty recovery authority must be acquired");
    };
    let (_lease_tx, lease_rx) = watch::channel(Some(lease.clone()));
    let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(10)));
    controller
        .set_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    controller
        .publish_leased_recovery_incarnation(&process_lease)
        .await
        .unwrap();
    controller
        .set_leader_lease_watch(lease_rx, owner, deadline)
        .unwrap();
    controller.set_leader_lease_store(Arc::clone(&authority));
    (authority, lease.proof())
}

fn test_leader_proof(node_id: u64, boot_id: Uuid, process_term: u64) -> LeaderProof {
    LeaderProof {
        owner: crate::checkpoint::LeaderProofOwner {
            node_id,
            boot_id,
            process_term,
        },
        fencing_token: process_term,
    }
}

fn recovery_round(
    controller: &ClusterController,
    generation: u64,
    leader_proof: &LeaderProof,
    participants: &[u64],
) -> RecoveryRound {
    recovery_round_with_evidence(
        controller,
        generation,
        leader_proof,
        participants,
        Vec::new(),
    )
}

fn recovery_round_with_evidence(
    controller: &ClusterController,
    generation: u64,
    leader_proof: &LeaderProof,
    participants: &[u64],
    evidence_participants: Vec<CheckpointParticipant>,
) -> RecoveryRound {
    let mut faults = vec![RecoveryFault {
        reporter: NodeId(leader_proof.owner.node_id),
        sequence: generation,
        disposition: RecoveryFaultDisposition::Recoverable,
    }];
    faults.extend(
        evidence_participants
            .iter()
            .map(|participant| RecoveryFault {
                reporter: NodeId(participant.node_id),
                sequence: generation,
                disposition: RecoveryFaultDisposition::Recoverable,
            }),
    );
    faults.sort_unstable_by_key(|fault| fault.reporter);
    faults.dedup_by_key(|fault| fault.reporter);
    recovery_round_with_fault_inventory(
        controller,
        generation,
        leader_proof,
        participants,
        evidence_participants,
        generation,
        faults,
    )
}

fn recovery_round_with_fault_inventory(
    controller: &ClusterController,
    generation: u64,
    leader_proof: &LeaderProof,
    participants: &[u64],
    evidence_participants: Vec<CheckpointParticipant>,
    fault_revision: u64,
    faults: Vec<RecoveryFault>,
) -> RecoveryRound {
    let participant_roster = participants
        .iter()
        .map(|node_id| CheckpointParticipant {
            node_id: *node_id,
            boot_incarnation: if *node_id == leader_proof.owner.node_id {
                leader_proof.owner.boot_id
            } else if *node_id == controller.instance_id().0 {
                controller.recovery_incarnation()
            } else {
                Uuid::from_u128((u128::from(generation) << 64) | u128::from(*node_id))
            },
        })
        .collect::<Vec<_>>();
    RecoveryRound::new(
        generation,
        leader_proof.clone(),
        CheckpointAssignmentFence::from_owner_map(7, participants, participant_roster).unwrap(),
        evidence_participants,
        fault_revision,
        faults,
    )
    .unwrap()
}

async fn recovery_round_from_current_faults(
    controller: &ClusterController,
    generation: u64,
    leader_proof: &LeaderProof,
    participants: &[u64],
) -> RecoveryRound {
    let inventory = controller.read_recovery_fault_inventory().await.unwrap();
    recovery_round_with_fault_inventory(
        controller,
        generation,
        leader_proof,
        participants,
        Vec::new(),
        inventory.revision(),
        inventory.faults().to_vec(),
    )
}

async fn report_remote_fault(
    controller: &ClusterController,
    participant: CheckpointParticipant,
    request_sequence: u64,
) {
    assert_eq!(
        controller
            .checkpoint_authority()
            .unwrap()
            .record_recovery_fault(
                RecoveryFaultPublisher {
                    participant,
                    process_term: 1,
                },
                request_sequence,
            )
            .await
            .unwrap(),
        super::super::leader_lease::RecordRecoveryFaultResult::Active
    );
}

async fn report_new_local_fault(controller: &ClusterController) {
    let request = controller.next_recovery_fault_request().unwrap();
    controller.report_fault(request).await.unwrap();
}

#[tokio::test]
async fn terminal_fault_arriving_after_prepare_blocks_start_and_preserves_prepare() {
    let controller = ctl(1, vec![]);
    let (_authority, proof) = install_recovery_authority(&controller, 10_000).await;
    let request = controller.next_recovery_fault_request().unwrap();
    assert_eq!(
        controller.report_fault(request).await.unwrap(),
        RecoveryFaultReportOutcome::Active
    );
    let round = recovery_round_from_current_faults(&controller, 47, &proof, &[1]).await;
    controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
    controller.announce_recover_prepare(&round).await.unwrap();

    assert_eq!(
        controller.report_terminal_fault(request).await.unwrap(),
        RecoveryFaultReportOutcome::Active
    );
    let error = controller
        .announce_recover_start(&round, 9)
        .await
        .unwrap_err();
    assert!(error.contains("fault set changed"), "{error}");
    assert_eq!(
        controller.observe_recover().await.unwrap(),
        Some(RecoveryAnnouncement {
            round,
            phase: RecoverPhase::Prepare,
        })
    );
}

async fn supersede_test_process_lease(
    controller: &ClusterController,
) -> super::super::ProcessLease {
    use super::super::ProcessLeaseOutcome;

    let authority = controller
        .process_lease_authority
        .get()
        .expect("test controller has process lease authority");
    let store = authority.store_for(controller.instance_id());
    let incumbent = store
        .load()
        .await
        .unwrap()
        .expect("test process lease is durable");
    let observation = store.observe_rival(&incumbent).unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let replacement = Uuid::new_v4();
    let ProcessLeaseOutcome::Acquired(successor) = store
        .try_takeover(replacement, &observation, 10)
        .await
        .unwrap()
    else {
        panic!("expired test process lease must be superseded");
    };
    assert_ne!(successor.owner, incumbent.owner);
    assert_eq!(successor.term, incumbent.term.checked_add(1).unwrap());
    successor
}

fn stopped_report(controller: &ClusterController, round: &RecoveryRound) -> RecoveryStoppedReport {
    RecoveryStoppedReport::new(
        round,
        CheckpointParticipant {
            node_id: controller.instance_id().0,
            boot_incarnation: controller.recovery_incarnation(),
        },
    )
    .unwrap()
}

fn seed_release_ready(
    kv: &InMemoryKv,
    participant: CheckpointParticipant,
    release: &RecoveryAnnouncement,
) {
    let encoded = encode_release_ready_ack(&RecoveryReleaseReadyAck {
        release: RecoveryReleaseId::for_pending(release).unwrap(),
        participant,
    })
    .unwrap();
    kv.seed(NodeId(participant.node_id), RELEASE_READY_ACK_KEY, encoded);
}

async fn two_owner_pending_release() -> (
    ClusterController,
    Arc<InMemoryKv>,
    RecoveryAnnouncement,
    CheckpointParticipant,
) {
    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let (_members_tx, members_rx) = watch::channel(vec![info(2)]);
    let controller = ClusterController::new(self_id, kv.clone(), None, members_rx);
    controller.publish_recovery_incarnation().await.unwrap();
    let (_authority, proof) = install_recovery_authority(&controller, 10_000).await;
    report_new_local_fault(&controller).await;
    let round = recovery_round_from_current_faults(&controller, 41, &proof, &[1, 2]).await;
    let remote = round.assignment_fence.participants[1];
    kv.seed(
        NodeId(remote.node_id),
        RECOVERY_INCARNATION_KEY,
        remote.boot_incarnation.to_string(),
    );
    controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
    controller.announce_recover_prepare(&round).await.unwrap();
    controller.announce_recover_start(&round, 8).await.unwrap();
    controller
        .announce_recover_release(&round, 8)
        .await
        .unwrap();
    let release = RecoveryAnnouncement {
        round,
        phase: RecoverPhase::Release { epoch: 8 },
    };
    (controller, kv, release, remote)
}

async fn faulty_single_owner_pending_release() -> (
    ClusterController,
    Arc<FaultyReadyReadKv>,
    RecoveryAnnouncement,
) {
    let self_id = NodeId(1);
    let kv = Arc::new(FaultyReadyReadKv::new(self_id));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = ClusterController::new(self_id, kv.clone(), None, members_rx);
    controller.publish_recovery_incarnation().await.unwrap();
    let (_authority, proof) = install_recovery_authority(&controller, 10_000).await;
    report_new_local_fault(&controller).await;
    let round = recovery_round_from_current_faults(&controller, 43, &proof, &[1]).await;
    controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
    controller.announce_recover_prepare(&round).await.unwrap();
    controller.announce_recover_start(&round, 9).await.unwrap();
    controller
        .announce_recover_release(&round, 9)
        .await
        .unwrap();
    let release = RecoveryAnnouncement {
        round,
        phase: RecoverPhase::Release { epoch: 9 },
    };
    controller.announce_release_ready(&release).await.unwrap();
    (controller, kv, release)
}

#[test]
fn recovery_round_requires_a_canonical_nonzero_fault_set() {
    let c = ctl(1, vec![]);
    let proof = test_leader_proof(1, c.recovery_incarnation(), 1);
    let exact = recovery_round(&c, 11, &proof, &[1]);
    assert_eq!(exact.fault_sequence(NodeId(1)), Some(11));

    let mut mismatched_driver = exact.clone();
    mismatched_driver.id.driver = NodeId(2);
    assert!(mismatched_driver.validate().is_err());

    let mut mismatched_boot = exact.clone();
    mismatched_boot.leader_proof.owner.boot_id = Uuid::new_v4();
    assert!(mismatched_boot.validate().is_err());

    let mut empty = exact.clone();
    empty.faults.clear();
    assert!(empty.validate().is_err());

    let mut zero = exact.clone();
    zero.faults[0].sequence = 0;
    assert!(zero.validate().is_err());

    let mut duplicate = exact;
    duplicate.faults.push(duplicate.faults[0]);
    assert!(duplicate.validate().is_err());
}

#[tokio::test]
async fn fault_report_after_exact_process_lease_supersession_has_no_durable_effect() {
    let controller = ctl(1, vec![]);
    controller.publish_recovery_incarnation().await.unwrap();
    let (authority, _proof) = install_recovery_authority(&controller, 1_000).await;
    let request = controller.next_recovery_fault_request().unwrap();
    let authority_head = authority.load().await.unwrap();

    supersede_test_process_lease(&controller).await;
    let error = controller.report_fault(request).await.unwrap_err();

    assert!(
        error.contains("process lease is no longer current"),
        "{error}"
    );
    let inventory = controller.read_recovery_fault_inventory().await.unwrap();
    assert_eq!(inventory.revision(), 0);
    assert!(inventory.faults().is_empty());
    assert_eq!(authority.load().await.unwrap(), authority_head);
}

#[tokio::test]
async fn fault_report_is_fenced_when_process_term_changes_after_authority_commit() {
    use super::super::leader_lease::RecordRecoveryFaultResult;

    let controller = ctl(1, vec![]);
    controller.publish_recovery_incarnation().await.unwrap();
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let gate = Arc::new(AuthorityIoGateStore::new(
        backing,
        AuthorityIoGateOperation::Put,
    ));
    let gated_backing: Arc<dyn object_store::ObjectStore> = gate.clone();
    let (authority, _proof) =
        install_recovery_authority_with_store(&controller, 1_000, gated_backing).await;
    let request = controller.next_recovery_fault_request().unwrap();
    let request_sequence = request.sequence();
    let stale_publisher = controller.recovery_fault_publisher().unwrap();

    gate.arm();
    let mut report = Box::pin(controller.report_fault(request));
    tokio::select! {
        _ = &mut report => panic!("fault report completed before the authority commit gate"),
        () = gate.wait_until_blocked() => {}
        () = tokio::time::sleep(Duration::from_secs(1)) => {
            panic!("fault report did not reach the authority commit gate");
        }
    }
    let successor = supersede_test_process_lease(&controller).await;
    gate.release_blocked_operation();

    let error = report.await.unwrap_err();
    assert!(
        error.contains("process lease changed while publishing recovery fault"),
        "{error}"
    );
    let stale_inventory = controller.read_recovery_fault_inventory().await.unwrap();
    assert_eq!(stale_inventory.faults().len(), 1);
    assert_eq!(
        stale_inventory.faults()[0].reporter,
        controller.instance_id()
    );
    assert_eq!(
        authority
            .record_recovery_fault(stale_publisher, request_sequence)
            .await
            .unwrap(),
        RecordRecoveryFaultResult::Active,
        "an exact retry must identify the conservatively persisted stale-process fault"
    );

    let successor_publisher = RecoveryFaultPublisher {
        participant: CheckpointParticipant {
            node_id: successor.node.0,
            boot_incarnation: successor.owner,
        },
        process_term: successor.term,
    };
    assert_eq!(
        authority
            .record_recovery_fault(successor_publisher, 1)
            .await
            .unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let successor_inventory = controller.read_recovery_fault_inventory().await.unwrap();
    assert_eq!(successor_inventory.faults().len(), 1);
    assert_eq!(
        successor_inventory.faults()[0].reporter,
        controller.instance_id()
    );
    assert!(successor_inventory.revision() > stale_inventory.revision());
    assert!(successor_inventory.faults()[0].sequence > stale_inventory.faults()[0].sequence);
    assert_eq!(
        authority
            .record_recovery_fault(stale_publisher, request_sequence)
            .await
            .unwrap(),
        RecordRecoveryFaultResult::Superseded
    );
}

#[tokio::test]
async fn recovery_release_after_process_lease_loss_has_no_guard_or_authority_side_effect() {
    let controller = ctl(1, vec![]);
    controller.publish_recovery_incarnation().await.unwrap();
    let (authority, proof) = install_recovery_authority(&controller, 1_000).await;
    report_new_local_fault(&controller).await;
    let round = recovery_round_from_current_faults(&controller, 13, &proof, &[1]).await;
    controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
    controller.announce_recover_prepare(&round).await.unwrap();
    controller.announce_recover_start(&round, 8).await.unwrap();
    controller
        .announce_recover_release(&round, 8)
        .await
        .unwrap();
    let release = RecoveryAnnouncement {
        round,
        phase: RecoverPhase::Release { epoch: 8 },
    };
    controller.announce_release_ready(&release).await.unwrap();
    let ReleaseCommitStatus::Committed { terminal } = controller
        .try_commit_recover_release(&release)
        .await
        .unwrap()
    else {
        panic!("single-owner recovery Release must commit");
    };
    let authority_head = authority.load().await.unwrap();
    let fault_inventory = controller.read_recovery_fault_inventory().await.unwrap();

    supersede_test_process_lease(&controller).await;
    let error = controller
        .begin_recovery_release(&terminal)
        .await
        .unwrap_err();

    assert!(matches!(error, RecoveryControlError::Superseded(_)));
    assert_eq!(authority.load().await.unwrap(), authority_head);
    assert_eq!(
        controller.read_recovery_fault_inventory().await.unwrap(),
        fault_inventory
    );
    assert_eq!(
        authority.latest_recovery_release_terminal().await.unwrap(),
        Some(terminal)
    );
}

#[tokio::test]
async fn recovery_release_is_fenced_when_process_term_changes_after_authorization() {
    let controller = ctl(1, vec![]);
    controller.publish_recovery_incarnation().await.unwrap();
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let gate = Arc::new(AuthorityIoGateStore::new(
        backing,
        AuthorityIoGateOperation::Get,
    ));
    let gated_backing: Arc<dyn object_store::ObjectStore> = gate.clone();
    let (authority, proof) =
        install_recovery_authority_with_store(&controller, 1_000, gated_backing).await;
    report_new_local_fault(&controller).await;
    let round = recovery_round_from_current_faults(&controller, 14, &proof, &[1]).await;
    controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
    controller.announce_recover_prepare(&round).await.unwrap();
    controller.announce_recover_start(&round, 9).await.unwrap();
    controller
        .announce_recover_release(&round, 9)
        .await
        .unwrap();
    let release = RecoveryAnnouncement {
        round,
        phase: RecoverPhase::Release { epoch: 9 },
    };
    controller.announce_release_ready(&release).await.unwrap();
    let ReleaseCommitStatus::Committed { terminal } = controller
        .try_commit_recover_release(&release)
        .await
        .unwrap()
    else {
        panic!("single-owner recovery Release must commit");
    };
    let authority_head = authority.load().await.unwrap();
    let fault_inventory = controller.read_recovery_fault_inventory().await.unwrap();
    let latest_terminal = authority.latest_recovery_release_terminal().await.unwrap();

    gate.arm();
    let mut release_guard = Box::pin(controller.begin_recovery_release(&terminal));
    tokio::select! {
        _ = &mut release_guard => {
            panic!("recovery release completed before the authorization read gate");
        }
        () = gate.wait_until_blocked() => {}
        () = tokio::time::sleep(Duration::from_secs(1)) => {
            panic!("recovery release did not reach the authorization read gate");
        }
    }
    supersede_test_process_lease(&controller).await;
    gate.release_blocked_operation();

    let error = release_guard.await.unwrap_err();
    assert!(matches!(error, RecoveryControlError::Superseded(_)));
    assert_eq!(authority.load().await.unwrap(), authority_head);
    assert_eq!(
        controller.read_recovery_fault_inventory().await.unwrap(),
        fault_inventory
    );
    assert_eq!(
        authority.latest_recovery_release_terminal().await.unwrap(),
        latest_terminal
    );
}

#[tokio::test]
async fn terminal_clear_never_overwrites_a_newer_local_fault() {
    let c = ctl(1, vec![]);
    c.publish_recovery_incarnation().await.unwrap();
    let (_authority, proof) = install_recovery_authority(&c, 1_000).await;
    report_new_local_fault(&c).await;
    let round = recovery_round_from_current_faults(&c, 12, &proof, &[1]).await;
    c.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
    c.announce_recover_prepare(&round).await.unwrap();
    c.announce_recover_start(&round, 8).await.unwrap();
    c.announce_recover_release(&round, 8).await.unwrap();
    let release = RecoveryAnnouncement {
        round,
        phase: RecoverPhase::Release { epoch: 8 },
    };
    c.announce_release_ready(&release).await.unwrap();
    let ReleaseCommitStatus::Committed { terminal } =
        c.try_commit_recover_release(&release).await.unwrap()
    else {
        panic!("the single-owner Release must commit");
    };

    report_new_local_fault(&c).await;

    assert!(c.begin_recovery_release(&terminal).await.unwrap().is_none());
    let reports = c.read_fault_reports().await.unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].0, NodeId(1));
    assert!(
        reports[0].1 > terminal.round.fault_sequence(NodeId(1)).unwrap(),
        "the successor fault must have a newer global authority sequence"
    );
}

#[tokio::test]
async fn recovery_start_and_clear_require_the_identical_prepared_round() {
    let c = ctl(1, vec![]);
    c.publish_recovery_incarnation().await.unwrap();
    let (_authority, proof) = install_recovery_authority(&c, 1_000).await;
    let round = recovery_round(&c, 11, &proof, &[1]);
    let other = recovery_round(&c, 12, &proof, &[1]);
    c.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));

    c.announce_recover_prepare(&round).await.unwrap();
    assert_eq!(
        c.observe_recover().await.unwrap().unwrap(),
        RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Prepare,
        }
    );
    assert!(c.announce_recover_start(&other, 9).await.is_err());
    assert!(!c.clear_recover(&other).await.unwrap());

    c.announce_recover_start(&round, 9).await.unwrap();
    let start = c.observe_recover().await.unwrap().unwrap();
    assert_eq!(start.round, round);
    assert_eq!(start.phase, RecoverPhase::Start { epoch: 9 });
    assert!(c.clear_recover(&start.round).await.unwrap());
    assert!(c.observe_recover().await.unwrap().is_none());
}

#[tokio::test]
async fn stale_noncurrent_recovery_slot_does_not_mask_the_current_driver() {
    let self_id = NodeId(2);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let mut old_driver = info(1);
    old_driver.state = NodeState::Suspected;
    let (_tx, rx) = watch::channel(vec![old_driver]);
    let c = ClusterController::new(self_id, kv.clone(), None, rx);
    c.publish_recovery_incarnation().await.unwrap();
    let (_authority, local_proof) = install_recovery_authority(&c, 1_000).await;
    let stale_proof = test_leader_proof(1, Uuid::from_u128(99), 99);
    let local = recovery_round(&c, 17, &local_proof, &[2]);
    let stale = recovery_round(&c, 99, &stale_proof, &[1, 2]);
    c.publish_checkpoint_assignment_fence(Some(local.assignment_fence.clone()));
    c.announce_recover_prepare(&local).await.unwrap();
    kv.seed(
        NodeId(1),
        "control:recover",
        serde_json::to_string(&RecoveryAnnouncement {
            round: stale,
            phase: RecoverPhase::Start { epoch: 9 },
        })
        .unwrap(),
    );

    assert_eq!(
        c.observe_recover().await.unwrap(),
        Some(RecoveryAnnouncement {
            round: local,
            phase: RecoverPhase::Prepare,
        })
    );
}

#[tokio::test]
async fn malformed_current_driver_recovery_slot_fails_closed() {
    let self_id = NodeId(2);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let (_tx, rx) = watch::channel(Vec::new());
    let c = ClusterController::new(self_id, kv.clone(), None, rx);
    let (_authority, _proof) = install_recovery_authority(&c, 1_000).await;
    kv.seed(self_id, "control:recover", "not-json".into());

    let error = c.observe_recover().await.unwrap_err();
    assert!(error.contains("invalid recovery announcement"), "{error}");
}

#[tokio::test]
async fn recovered_ack_binds_the_start_target() {
    let c = ctl(1, vec![]);
    c.publish_recovery_incarnation().await.unwrap();
    let proof = test_leader_proof(1, c.recovery_incarnation(), 1);
    let round = recovery_round(&c, 21, &proof, &[1]);
    let start = RecoveryAnnouncement {
        round: round.clone(),
        phase: RecoverPhase::Start { epoch: 4 },
    };
    c.announce_recovered(&start).await.unwrap();
    assert_eq!(c.read_recovered().await.unwrap(), vec![(NodeId(1), start)]);
    assert!(c
        .announce_recovered(&RecoveryAnnouncement {
            round,
            phase: RecoverPhase::Prepare,
        })
        .await
        .is_err());
}

#[tokio::test]
async fn release_terminal_is_authoritative_and_hint_is_retirable() {
    let c = ctl(1, vec![]);
    c.publish_recovery_incarnation().await.unwrap();
    let (_authority, proof) = install_recovery_authority(&c, 1_000).await;
    report_new_local_fault(&c).await;
    let round = recovery_round_from_current_faults(&c, 31, &proof, &[1]).await;
    c.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
    c.announce_recover_prepare(&round).await.unwrap();
    c.announce_recover_start(&round, 8).await.unwrap();

    c.announce_recover_release(&round, 8).await.unwrap();
    let release = RecoveryAnnouncement {
        round: round.clone(),
        phase: RecoverPhase::Release { epoch: 8 },
    };
    assert_eq!(c.observe_recover().await.unwrap(), Some(release.clone()));
    assert!(!c.clear_recover(&round).await.unwrap());
    c.announce_release_ready(&release).await.unwrap();
    assert_eq!(
        c.read_release_ready(&release).await.unwrap(),
        ReleaseReadyStatus::Complete
    );
    let ReleaseCommitStatus::Committed { terminal } =
        c.try_commit_recover_release(&release).await.unwrap()
    else {
        panic!("exact readiness must commit the pending Release");
    };
    assert_eq!(terminal.phase, RecoverPhase::ReleaseCommitted { epoch: 8 });
    assert_eq!(c.observe_recover().await.unwrap(), Some(release.clone()));
    assert_eq!(
        c.observe_committed_recover_release(&round, 8)
            .await
            .unwrap(),
        Some(terminal)
    );
    assert!(!c.clear_recover(&round).await.unwrap());
    assert!(c
        .retire_committed_recover_release_hint(&round, 8)
        .await
        .unwrap());
    assert_eq!(c.observe_recover().await.unwrap(), None);
}

#[tokio::test]
async fn release_commit_requires_the_exact_compact_ready_roster() {
    let (controller, kv, release, remote) = two_owner_pending_release().await;
    controller.announce_release_ready(&release).await.unwrap();

    assert_eq!(
        controller.read_release_ready(&release).await.unwrap(),
        ReleaseReadyStatus::Pending {
            missing: vec![NodeId(remote.node_id)]
        }
    );
    kv.seed(
        NodeId(99),
        RELEASE_READY_ACK_KEY,
        "unrelated malformed value".into(),
    );
    assert!(matches!(
        controller
            .try_commit_recover_release(&release)
            .await
            .unwrap(),
        ReleaseCommitStatus::Pending { .. }
    ));

    seed_release_ready(&kv, remote, &release);
    assert_eq!(
        controller.read_release_ready(&release).await.unwrap(),
        ReleaseReadyStatus::Complete
    );
    let ReleaseCommitStatus::Committed { terminal } = controller
        .try_commit_recover_release(&release)
        .await
        .unwrap()
    else {
        panic!("the exact compact readiness roster must commit");
    };
    assert_eq!(terminal.phase, RecoverPhase::ReleaseCommitted { epoch: 8 });
    let local_ack = kv
        .read_from(NodeId(1), RELEASE_READY_ACK_KEY)
        .await
        .unwrap();
    assert!(local_ack.len() < 512);
    assert!(!local_ack.contains("assignment_fence"));
    assert!(!local_ack.contains("faults"));
}

#[tokio::test]
async fn changed_fault_set_supersedes_release_before_missing_readiness() {
    let (controller, _kv, release, remote) = two_owner_pending_release().await;
    controller.announce_release_ready(&release).await.unwrap();
    report_remote_fault(&controller, remote, 1).await;

    let RecoveryControlError::Superseded(reason) = controller
        .try_commit_recover_release(&release)
        .await
        .unwrap_err()
    else {
        panic!("a newer fault must supersede the pending Release");
    };
    assert!(reason.contains("fault set changed"), "{reason}");
    assert_eq!(
        controller.observe_recover().await.unwrap(),
        Some(release.clone())
    );
    assert_eq!(
        controller
            .observe_committed_recover_release(&release.round, 8)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test(start_paused = true)]
async fn pending_release_fault_audits_are_coalesced_and_expire() {
    let (controller, _kv, release, remote) = two_owner_pending_release().await;
    controller.announce_release_ready(&release).await.unwrap();

    for _ in 0..20 {
        assert!(matches!(
            controller
                .try_commit_recover_release(&release)
                .await
                .unwrap(),
            ReleaseCommitStatus::Pending { .. }
        ));
    }

    report_remote_fault(&controller, remote, 1).await;
    assert!(matches!(
        controller
            .try_commit_recover_release(&release)
            .await
            .unwrap(),
        ReleaseCommitStatus::Pending { .. }
    ));

    tokio::time::advance(
        PENDING_RELEASE_FAULT_AUDIT_INTERVAL
            .checked_sub(Duration::from_nanos(1))
            .unwrap(),
    )
    .await;
    assert!(matches!(
        controller
            .try_commit_recover_release(&release)
            .await
            .unwrap(),
        ReleaseCommitStatus::Pending { .. }
    ));

    tokio::time::advance(Duration::from_nanos(1)).await;
    assert!(matches!(
        controller.try_commit_recover_release(&release).await,
        Err(RecoveryControlError::Superseded(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn complete_release_bypasses_the_pending_fault_audit_cache() {
    let (controller, kv, release, remote) = two_owner_pending_release().await;
    controller.announce_release_ready(&release).await.unwrap();
    assert!(matches!(
        controller
            .try_commit_recover_release(&release)
            .await
            .unwrap(),
        ReleaseCommitStatus::Pending { .. }
    ));

    seed_release_ready(&kv, remote, &release);
    report_remote_fault(&controller, remote, 1).await;
    assert!(matches!(
        controller.try_commit_recover_release(&release).await,
        Err(RecoveryControlError::Superseded(_))
    ));
    assert!(controller
        .observe_committed_recover_release(&release.round, 8)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn restarted_ready_owner_cannot_commit_its_old_process_vote() {
    let (controller, kv, release, remote) = two_owner_pending_release().await;
    controller.announce_release_ready(&release).await.unwrap();
    seed_release_ready(&kv, remote, &release);
    kv.seed(
        NodeId(remote.node_id),
        RECOVERY_INCARNATION_KEY,
        Uuid::new_v4().to_string(),
    );

    let RecoveryControlError::Superseded(reason) = controller
        .try_commit_recover_release(&release)
        .await
        .unwrap_err()
    else {
        panic!("a restarted owner must invalidate its old process vote");
    };
    assert!(reason.contains("process-incarnation roster changed"));
    assert_eq!(controller.observe_recover().await.unwrap(), Some(release));
}

#[tokio::test]
async fn fail_once_ready_read_retries_the_same_pending_release() {
    let (controller, kv, release) = faulty_single_owner_pending_release().await;
    kv.fail_next_ready_reads(1);

    assert!(controller
        .try_commit_recover_release(&release)
        .await
        .is_err());
    assert_eq!(
        controller.observe_recover().await.unwrap(),
        Some(release.clone())
    );
    assert!(matches!(
        controller
            .try_commit_recover_release(&release)
            .await
            .unwrap(),
        ReleaseCommitStatus::Committed { .. }
    ));
}

#[tokio::test]
async fn persistent_ready_read_failure_leaves_the_release_pending_at_deadline() {
    let (controller, kv, release) = faulty_single_owner_pending_release().await;
    kv.fail_next_ready_reads(usize::MAX);
    let deadline = tokio::time::Instant::now() + Duration::from_millis(25);

    while tokio::time::Instant::now() < deadline {
        assert!(controller
            .try_commit_recover_release(&release)
            .await
            .is_err());
        tokio::task::yield_now().await;
    }

    assert_eq!(controller.observe_recover().await.unwrap(), Some(release));
}

#[tokio::test]
async fn durable_recovery_state_survives_fast_kv_reconstruction() {
    let node = NodeId(1);
    let fast = Arc::new(InMemoryKv::new(node));
    let durable = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = ClusterController::new_with_recovery_kv(
        node,
        fast.clone(),
        durable.clone(),
        None,
        members_rx,
    );
    controller.publish_recovery_incarnation().await.unwrap();
    let (authority, proof) = install_recovery_authority(&controller, 1_000).await;
    report_new_local_fault(&controller).await;
    let round = recovery_round_from_current_faults(&controller, 51, &proof, &[1]).await;
    controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
    controller.adopt_recovery_generation(51).await.unwrap();
    controller.announce_recover_prepare(&round).await.unwrap();
    controller.announce_recover_start(&round, 13).await.unwrap();
    controller
        .announce_recover_release(&round, 13)
        .await
        .unwrap();

    assert!(fast.read_from(node, "control:recover").await.is_none());
    drop(controller);

    let replacement_fast = Arc::new(InMemoryKv::new(node));
    let (_replacement_tx, replacement_rx) = watch::channel(Vec::new());
    let replacement = ClusterController::new_with_recovery_kv(
        node,
        replacement_fast,
        durable,
        None,
        replacement_rx,
    );
    replacement.set_leader_lease_store(authority);
    assert_eq!(replacement.max_recovery_generation().await.unwrap(), 51);
    assert_eq!(
        replacement.observe_recover().await.unwrap(),
        Some(RecoveryAnnouncement {
            round,
            phase: RecoverPhase::Release { epoch: 13 },
        })
    );
}

#[tokio::test]
async fn delayed_old_phase_cannot_clobber_same_process_new_leader_term() {
    use super::super::{LeaderLeaseOwner, LeaseDeadline, LeaseOutcome};

    let node = NodeId(1);
    let delayed = Arc::new(DelayedRecoveryKv::new(node));
    let recovery_kv: Arc<dyn ClusterKv> = delayed.clone();
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node, recovery_kv, None, members_rx));
    controller.publish_recovery_incarnation().await.unwrap();

    let authority = Arc::new(super::super::LeaderLeaseStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        1,
    ));
    let owner = LeaderLeaseOwner {
        node,
        boot: controller.recovery_incarnation(),
        process_term: 1,
    };
    let LeaseOutcome::Acquired(old_lease) = authority.begin_new_term(&owner, 0).await.unwrap()
    else {
        panic!("empty recovery authority must be acquired");
    };
    let (lease_tx, lease_rx) = watch::channel(Some(old_lease.clone()));
    let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(10)));
    controller
        .set_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    controller
        .set_leader_lease_watch(lease_rx, owner.clone(), deadline)
        .unwrap();
    controller.set_leader_lease_store(Arc::clone(&authority));

    assert_eq!(
        authority
            .record_recovery_fault(
                RecoveryFaultPublisher {
                    participant: CheckpointParticipant {
                        node_id: node.0,
                        boot_incarnation: controller.recovery_incarnation(),
                    },
                    process_term: owner.process_term,
                },
                1,
            )
            .await
            .unwrap(),
        super::super::leader_lease::RecordRecoveryFaultResult::Active
    );

    let old_proof = old_lease.proof();
    let old_round = recovery_round_from_current_faults(&controller, 61, &old_proof, &[1]).await;
    controller.publish_checkpoint_assignment_fence(Some(old_round.assignment_fence.clone()));
    controller
        .announce_recover_prepare(&old_round)
        .await
        .unwrap();

    delayed.block_next_recovery_write();
    let stale_start = {
        let controller = Arc::clone(&controller);
        let round = old_round.clone();
        tokio::spawn(async move { controller.announce_recover_start(&round, 4).await })
    };
    delayed.wait_until_blocked().await;

    let rival = LeaderLeaseOwner {
        node: NodeId(2),
        boot: Uuid::new_v4(),
        process_term: 1,
    };
    let rival_observation = authority.observe_rival(&rival, &old_lease).unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    let LeaseOutcome::Acquired(rival_lease) = authority
        .try_takeover(&rival, &rival_observation, 10)
        .await
        .unwrap()
    else {
        panic!("rival must take over the expired old term");
    };
    let return_observation = authority.observe_rival(&owner, &rival_lease).unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    let LeaseOutcome::Acquired(current_lease) = authority
        .try_takeover(&owner, &return_observation, 20)
        .await
        .unwrap()
    else {
        panic!("original process must acquire a higher leader term");
    };
    assert_eq!(current_lease.owner, old_lease.owner);
    assert!(current_lease.token > old_lease.token);
    lease_tx.send_replace(Some(current_lease.clone()));

    let current_round =
        recovery_round_from_current_faults(&controller, 62, &current_lease.proof(), &[1]).await;
    controller.publish_checkpoint_assignment_fence(Some(current_round.assignment_fence.clone()));
    let current_prepare = {
        let controller = Arc::clone(&controller);
        let round = current_round.clone();
        tokio::spawn(async move { controller.announce_recover_prepare(&round).await })
    };
    tokio::task::yield_now().await;
    assert!(
        !current_prepare.is_finished(),
        "the replacement phase must serialize behind the in-flight old write"
    );

    delayed.release_blocked_write();
    let stale_error = stale_start.await.unwrap().unwrap_err();
    assert!(
        stale_error.contains("proof is no longer live at Start read-back"),
        "{stale_error}"
    );
    current_prepare.await.unwrap().unwrap();
    assert_eq!(
        controller.observe_recover().await.unwrap(),
        Some(RecoveryAnnouncement {
            round: current_round,
            phase: RecoverPhase::Prepare,
        })
    );
}

#[tokio::test]
async fn committed_release_terminal_survives_leader_takeover() {
    use super::super::{
        LeaderLeaseOwner, LeaseDeadline, LeaseOutcome, ProcessLeaseAuthority, ProcessLeaseOutcome,
    };

    let node = NodeId(1);
    let delayed = Arc::new(DelayedRecoveryKv::new(node));
    let recovery_kv: Arc<dyn ClusterKv> = delayed.clone();
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node, recovery_kv, None, members_rx));
    controller.publish_recovery_incarnation().await.unwrap();
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_millis(1)).unwrap(),
    );
    let ProcessLeaseOutcome::Acquired(process_lease) = process_authority
        .store_for(node)
        .try_acquire(controller.recovery_incarnation(), 0)
        .await
        .unwrap()
    else {
        panic!("empty process authority must be acquired");
    };
    controller
        .set_process_lease_authority(process_authority)
        .unwrap();
    let authority = Arc::new(super::super::LeaderLeaseStore::new(backing, 1));
    let owner = LeaderLeaseOwner {
        node,
        boot: controller.recovery_incarnation(),
        process_term: process_lease.term,
    };
    let LeaseOutcome::Acquired(old_lease) = authority.begin_new_term(&owner, 0).await.unwrap()
    else {
        panic!("empty recovery authority must be acquired");
    };
    let (_lease_tx, lease_rx) = watch::channel(Some(old_lease.clone()));
    let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(10)));
    controller
        .set_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    controller
        .publish_leased_recovery_incarnation(&process_lease)
        .await
        .unwrap();
    controller
        .set_leader_lease_watch(lease_rx, owner, deadline)
        .unwrap();
    controller.set_leader_lease_store(Arc::clone(&authority));

    report_new_local_fault(&controller).await;
    let round = recovery_round_from_current_faults(&controller, 63, &old_lease.proof(), &[1]).await;
    controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
    controller.announce_recover_prepare(&round).await.unwrap();
    controller.announce_recover_start(&round, 5).await.unwrap();
    controller
        .announce_recover_release(&round, 5)
        .await
        .unwrap();
    let pending = RecoveryAnnouncement {
        round,
        phase: RecoverPhase::Release { epoch: 5 },
    };
    controller.announce_release_ready(&pending).await.unwrap();

    let ReleaseCommitStatus::Committed { terminal } = controller
        .try_commit_recover_release(&pending)
        .await
        .unwrap()
    else {
        panic!("exact readiness must commit the authority terminal");
    };
    let raw = delayed.read_from(node, "control:recover").await.unwrap();
    assert!(raw.contains("\"Release\""));
    assert!(!raw.contains("ReleaseCommitted"));

    let rival = LeaderLeaseOwner {
        node: NodeId(2),
        boot: Uuid::new_v4(),
        process_term: 1,
    };
    let committed_lease = authority.load().await.unwrap().unwrap();
    let observation = authority.observe_rival(&rival, &committed_lease).unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    let LeaseOutcome::Acquired(rival_lease) = authority
        .try_takeover(&rival, &observation, 10)
        .await
        .unwrap()
    else {
        panic!("rival must take over the expired release driver");
    };

    let (_replacement_tx, replacement_rx) = watch::channel(Vec::new());
    let replacement = ClusterController::new(rival.node, delayed.clone(), None, replacement_rx);
    let (_rival_lease_tx, rival_lease_rx) = watch::channel(Some(rival_lease));
    let rival_deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(10)));
    replacement
        .set_process_lease_deadline(Arc::clone(&rival_deadline))
        .unwrap();
    replacement
        .set_leader_lease_watch(rival_lease_rx, rival, rival_deadline)
        .unwrap();
    replacement.set_leader_lease_store(authority);
    assert_eq!(
        replacement
            .observe_committed_recover_release(&pending.round, 5)
            .await
            .unwrap(),
        Some(terminal)
    );
    assert_eq!(
        delayed.read_from(node, "control:recover").await.unwrap(),
        raw
    );
}

#[tokio::test]
async fn durable_recovery_write_failure_is_returned() {
    let node = NodeId(1);
    let fast: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let durable: Arc<dyn ClusterKv> = Arc::new(FailedWriteKv);
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = ClusterController::new_with_recovery_kv(node, fast, durable, None, members_rx);

    let error = controller.publish_recovery_incarnation().await.unwrap_err();
    assert!(error.contains("injected durable write failure"), "{error}");
}

#[test]
fn recovery_round_separates_owners_from_bounded_evidence_participants() {
    let controller = ctl(1, Vec::new());
    let proof = test_leader_proof(1, controller.recovery_incarnation(), 1);
    let evidence = CheckpointParticipant {
        node_id: 2,
        boot_incarnation: Uuid::from_u128(2),
    };
    let round = recovery_round_with_evidence(&controller, 41, &proof, &[1], vec![evidence]);

    assert_eq!(round.owners(), vec![NodeId(1)]);
    assert_eq!(round.stopped_participants(), vec![NodeId(1), NodeId(2)]);
    assert!(round.contains_owner(NodeId(1)));
    assert!(!round.contains_owner(NodeId(2)));
    assert!(round.contains_stopped_participant(NodeId(2)));
    assert_eq!(round.owner_incarnation(NodeId(2)), None);
    assert_eq!(
        round.stopped_participant_incarnation(NodeId(2)),
        Some(evidence.boot_incarnation)
    );

    let owner_evidence = RecoveryRound::new(
        42,
        proof.clone(),
        round.assignment_fence.clone(),
        vec![round.assignment_fence.participants[0]],
        42,
        vec![RecoveryFault {
            reporter: NodeId(1),
            sequence: 42,
            disposition: RecoveryFaultDisposition::Recoverable,
        }],
    )
    .unwrap_err();
    assert!(
        owner_evidence.contains("non-owner fault reporters"),
        "{owner_evidence}"
    );

    let missing_fault = RecoveryRound::new(
        43,
        proof.clone(),
        round.assignment_fence.clone(),
        vec![evidence],
        43,
        vec![RecoveryFault {
            reporter: NodeId(1),
            sequence: 43,
            disposition: RecoveryFaultDisposition::Recoverable,
        }],
    )
    .unwrap_err();
    assert!(
        missing_fault.contains("non-owner fault reporters"),
        "{missing_fault}"
    );

    let future_fault = RecoveryRound::new(
        43,
        proof.clone(),
        round.assignment_fence.clone(),
        Vec::new(),
        43,
        vec![RecoveryFault {
            reporter: NodeId(1),
            sequence: 44,
            disposition: RecoveryFaultDisposition::Recoverable,
        }],
    )
    .unwrap_err();
    assert!(
        future_fault.contains("fault set is not canonical"),
        "{future_fault}"
    );

    let unsorted = RecoveryRound::new(
        43,
        proof.clone(),
        round.assignment_fence.clone(),
        vec![
            CheckpointParticipant {
                node_id: 3,
                boot_incarnation: Uuid::from_u128(3),
            },
            evidence,
        ],
        43,
        vec![
            RecoveryFault {
                reporter: NodeId(1),
                sequence: 43,
                disposition: RecoveryFaultDisposition::Recoverable,
            },
            RecoveryFault {
                reporter: NodeId(2),
                sequence: 43,
                disposition: RecoveryFaultDisposition::Recoverable,
            },
            RecoveryFault {
                reporter: NodeId(3),
                sequence: 43,
                disposition: RecoveryFaultDisposition::Recoverable,
            },
        ],
    )
    .unwrap_err();
    assert!(unsorted.contains("roster is not canonical"), "{unsorted}");

    let owner_count = MAX_CHECKPOINT_PARTICIPANTS;
    let owners = (1..=u64::try_from(owner_count).unwrap()).collect::<Vec<_>>();
    let participants = owners
        .iter()
        .map(|node_id| CheckpointParticipant {
            node_id: *node_id,
            boot_incarnation: Uuid::from_u128(u128::from(*node_id)),
        })
        .collect::<Vec<_>>();
    let full_fence = CheckpointAssignmentFence::from_owner_map(8, &owners, participants).unwrap();
    let outsider = CheckpointParticipant {
        node_id: u64::try_from(owner_count).unwrap() + 1,
        boot_incarnation: Uuid::from_u128(u128::try_from(owner_count).unwrap() + 1),
    };
    let oversized = RecoveryRound::new(
        44,
        test_leader_proof(1, Uuid::from_u128(1), 1),
        full_fence,
        vec![outsider],
        44,
        vec![
            RecoveryFault {
                reporter: NodeId(1),
                sequence: 44,
                disposition: RecoveryFaultDisposition::Recoverable,
            },
            RecoveryFault {
                reporter: NodeId(outsider.node_id),
                sequence: 44,
                disposition: RecoveryFaultDisposition::Recoverable,
            },
        ],
    )
    .unwrap_err();
    assert!(oversized.contains("stopped roster has"), "{oversized}");

    let oversized_fault_count = MAX_RECOVERY_ANNOUNCEMENT_BYTES / 16;
    let too_many_faults = (1..=u64::try_from(oversized_fault_count).unwrap())
        .map(|node_id| RecoveryFault {
            reporter: NodeId(node_id),
            sequence: 44,
            disposition: RecoveryFaultDisposition::Recoverable,
        })
        .collect();
    let oversized_fault_set = RecoveryRound::new(
        44,
        proof,
        round.assignment_fence,
        Vec::new(),
        44,
        too_many_faults,
    )
    .unwrap_err();
    assert!(
        oversized_fault_set.contains("maximum"),
        "{oversized_fault_set}"
    );
}

#[test]
fn recovery_announcement_wire_is_bounded_strict_and_canonical() {
    let controller = ctl(1, Vec::new());
    let proof = test_leader_proof(1, controller.recovery_incarnation(), 1);
    let announcement = RecoveryAnnouncement {
        round: recovery_round(&controller, 45, &proof, &[1]),
        phase: RecoverPhase::Prepare,
    };
    let canonical = encode_recovery_announcement(&announcement).unwrap();
    assert_eq!(
        parse_recovery_announcement(&canonical).unwrap(),
        Some(announcement)
    );

    let mut unknown = serde_json::from_str::<serde_json::Value>(&canonical).unwrap();
    unknown["unknown"] = serde_json::json!(true);
    let error = parse_recovery_announcement(&serde_json::to_string(&unknown).unwrap()).unwrap_err();
    assert!(error.contains("unknown field"), "{error}");

    let error = parse_recovery_announcement(&format!(" {canonical}")).unwrap_err();
    assert!(error.contains("not canonically encoded"), "{error}");

    let oversized = " ".repeat(MAX_RECOVERY_ANNOUNCEMENT_BYTES + 1);
    let error = parse_recovery_announcement(&oversized).unwrap_err();
    assert!(error.contains("maximum"), "{error}");
}

#[test]
fn recoverable_fault_wire_remains_legacy_byte_identical() {
    let legacy = r#"{"reporter":1,"sequence":7}"#;
    let fault: RecoveryFault = serde_json::from_str(legacy).unwrap();

    assert_eq!(fault.disposition, RecoveryFaultDisposition::Recoverable);
    assert_eq!(serde_json::to_string(&fault).unwrap(), legacy);
}

#[test]
fn terminal_prepare_wire_is_explicit_and_cannot_advance() {
    let controller = ctl(1, Vec::new());
    let proof = test_leader_proof(1, controller.recovery_incarnation(), 1);
    let mut round = recovery_round(&controller, 46, &proof, &[1]);
    round.faults[0].disposition = RecoveryFaultDisposition::Terminal;
    let prepare = RecoveryAnnouncement {
        round: round.clone(),
        phase: RecoverPhase::Prepare,
    };

    let encoded = encode_recovery_announcement(&prepare).unwrap();
    assert!(encoded.contains(r#""disposition":"terminal""#));
    assert_eq!(
        parse_recovery_announcement(&encoded).unwrap(),
        Some(prepare)
    );

    let start = RecoveryAnnouncement {
        round,
        phase: RecoverPhase::Start { epoch: 1 },
    };
    let error = encode_recovery_announcement(&start).unwrap_err();
    assert!(error.contains("may only be retained in Prepare"), "{error}");
}

#[tokio::test]
async fn evidence_boot_incarnation_is_part_of_the_frozen_stopped_roster() {
    let owner = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(owner));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = ClusterController::new(owner, kv.clone(), None, members_rx);
    controller.publish_recovery_incarnation().await.unwrap();
    let proof = test_leader_proof(1, controller.recovery_incarnation(), 1);
    let evidence = CheckpointParticipant {
        node_id: 2,
        boot_incarnation: Uuid::from_u128(2),
    };
    let round = recovery_round_with_evidence(&controller, 41, &proof, &[1], vec![evidence]);
    kv.seed(
        NodeId(evidence.node_id),
        RECOVERY_INCARNATION_KEY,
        evidence.boot_incarnation.to_string(),
    );

    assert!(controller
        .recovery_stopped_incarnations_match(&round)
        .await
        .unwrap());
    kv.seed(
        NodeId(evidence.node_id),
        RECOVERY_INCARNATION_KEY,
        Uuid::new_v4().to_string(),
    );
    assert!(!controller
        .recovery_stopped_incarnations_match(&round)
        .await
        .unwrap());
    assert!(
        controller
            .recovery_incarnations_match(&round)
            .await
            .unwrap(),
        "evidence reporters do not join the owner restore/release quorum"
    );
}

#[tokio::test]
async fn stopped_report_point_reads_ignore_outsiders_and_admit_evidence_publishers() {
    let evidence_node = NodeId(2);
    let kv = Arc::new(InMemoryKv::new(evidence_node));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = ClusterController::new(evidence_node, kv.clone(), None, members_rx);
    controller.publish_recovery_incarnation().await.unwrap();
    let owner_boot = Uuid::from_u128(1);
    let proof = test_leader_proof(1, owner_boot, 1);
    let evidence = CheckpointParticipant {
        node_id: evidence_node.0,
        boot_incarnation: controller.recovery_incarnation(),
    };
    let round = recovery_round_with_evidence(&controller, 41, &proof, &[1], vec![evidence]);
    controller.announce_stopped(&round).await.unwrap();
    kv.seed(
        NodeId(99),
        RECOVERY_STOPPED_REPORT_KEY,
        "malformed outsider report".into(),
    );

    let reports = controller
        .read_stopped(&round, &round.stopped_participants())
        .await
        .unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].publisher, evidence);

    kv.seed(
        NodeId(1),
        RECOVERY_STOPPED_REPORT_KEY,
        "malformed exact-roster report".into(),
    );
    let error = controller
        .read_stopped(&round, &round.stopped_participants())
        .await
        .unwrap_err();
    assert!(
        matches!(
            &error,
            RecoveryControlError::Conflict(reason)
                if reason.contains("invalid recovery stopped report from node-1")
        ),
        "{error}"
    );
}

#[tokio::test]
async fn stopped_report_announcement_reads_back_exact_publisher() {
    let node = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = ClusterController::new(node, kv, None, members_rx);
    controller.publish_recovery_incarnation().await.unwrap();
    let proof = test_leader_proof(1, controller.recovery_incarnation(), 1);
    let round = recovery_round(&controller, 41, &proof, &[1]);
    controller.announce_stopped(&round).await.unwrap();
    assert_eq!(
        controller
            .read_stopped(&round, &round.stopped_participants())
            .await
            .unwrap(),
        vec![stopped_report(&controller, &round)]
    );
}

#[tokio::test]
async fn stopped_report_point_reads_require_an_adopted_newer_generation() {
    let node = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = ClusterController::new(node, kv.clone(), None, members_rx);
    let proof = test_leader_proof(1, controller.recovery_incarnation(), 1);
    let old_round = recovery_round(&controller, 40, &proof, &[1]);
    let current_round = recovery_round(&controller, 41, &proof, &[1]);
    let newer_round = recovery_round(&controller, 42, &proof, &[1]);

    let old = stopped_report(&controller, &old_round);
    kv.seed(
        node,
        RECOVERY_STOPPED_REPORT_KEY,
        encode_recovery_stopped_report(&old, &old_round).unwrap(),
    );
    assert!(controller
        .read_stopped(&current_round, &current_round.stopped_participants())
        .await
        .unwrap()
        .is_empty());

    let newer = stopped_report(&controller, &newer_round);
    kv.seed(
        node,
        RECOVERY_STOPPED_REPORT_KEY,
        encode_recovery_stopped_report(&newer, &newer_round).unwrap(),
    );
    assert!(controller
        .read_stopped(&current_round, &current_round.stopped_participants())
        .await
        .unwrap()
        .is_empty());
    kv.seed(
        node,
        RECOVERY_INCARNATION_KEY,
        controller.recovery_incarnation().to_string(),
    );
    kv.seed(node, "control:recovery-gen", "42".into());
    let reports = controller
        .read_stopped(&current_round, &current_round.stopped_participants())
        .await
        .unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].round_id, newer_round.id);
}

#[test]
fn stopped_report_wire_round_trips_compact_round_identity() {
    let controller = ctl(1, Vec::new());
    let proof = test_leader_proof(1, controller.recovery_incarnation(), 1);
    let round = recovery_round(&controller, 41, &proof, &[1]);

    let report = stopped_report(&controller, &round);
    let raw = encode_recovery_stopped_report(&report, &round).unwrap();
    assert!(
        raw.len() < 512,
        "compact stopped report was {} bytes",
        raw.len()
    );
    assert_eq!(
        parse_recovery_stopped_report(&raw, NodeId(1), &round).unwrap(),
        report
    );
    let mut divergent_round = round.clone();
    divergent_round.faults[0].sequence += 1;
    divergent_round.fault_revision += 1;
    let error = parse_recovery_stopped_report(&raw, NodeId(1), &divergent_round).unwrap_err();
    assert!(error.contains("exact frozen round"), "{error}");

    assert!(raw.len() <= MAX_RECOVERY_STOPPED_REPORT_BYTES);
    assert!(!raw.contains("assignment_fence"));
    assert!(!raw.contains("evidence_participants"));
    assert!(!raw.contains("faults"));
}

#[test]
fn stopped_report_rejects_stale_boot_and_wrong_slot_publisher() {
    let controller = ctl(1, Vec::new());
    let proof = test_leader_proof(1, controller.recovery_incarnation(), 1);
    let round = recovery_round(&controller, 41, &proof, &[1]);
    let report = stopped_report(&controller, &round);

    let raw = encode_recovery_stopped_report(&report, &round).unwrap();
    let error = parse_recovery_stopped_report(&raw, NodeId(2), &round).unwrap_err();
    assert!(error.contains("names publisher 1"), "{error}");

    let mut stale = report;
    stale.publisher.boot_incarnation = Uuid::new_v4();
    let raw = serde_json::to_string(&stale).unwrap();
    let error = parse_recovery_stopped_report(&raw, NodeId(1), &round).unwrap_err();
    assert!(error.contains("does not match the frozen round"), "{error}");
}

#[test]
fn stopped_report_wire_rejects_oversize_and_unknown_fields() {
    let controller = ctl(1, Vec::new());
    let proof = test_leader_proof(1, controller.recovery_incarnation(), 1);
    let round = recovery_round(&controller, 41, &proof, &[1]);
    let report = stopped_report(&controller, &round);
    let raw = encode_recovery_stopped_report(&report, &round).unwrap();

    let oversize = format!(
        "{raw}{}",
        " ".repeat(MAX_RECOVERY_STOPPED_REPORT_BYTES + 1 - raw.len())
    );
    let error = parse_recovery_stopped_report(&oversize, NodeId(1), &round).unwrap_err();
    assert!(error.contains("bytes; maximum"), "{error}");

    let mut value = serde_json::to_value(report).unwrap();
    value["unknown"] = serde_json::json!(true);
    let raw = serde_json::to_string(&value).unwrap();
    let error = parse_recovery_stopped_report(&raw, NodeId(1), &round).unwrap_err();
    assert!(error.contains("unknown field"), "{error}");

    let unsupported_round = recovery_round(&controller, 42, &proof, &[1]);
    let mut unsupported = stopped_report(&controller, &unsupported_round);
    unsupported.protocol_version = RECOVERY_STOPPED_REPORT_PROTOCOL_VERSION + 1;
    let raw = serde_json::to_string(&unsupported).unwrap();
    let error = parse_recovery_stopped_report(&raw, NodeId(1), &unsupported_round).unwrap_err();
    assert!(
        error.contains("unsupported recovery stopped report version"),
        "{error}"
    );
}

#[tokio::test]
async fn superseded_same_id_process_cannot_ack_an_old_round() {
    let node = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(node));
    let (_old_tx, old_rx) = watch::channel(Vec::new());
    let old = ClusterController::new(node, kv.clone(), None, old_rx);
    old.publish_recovery_incarnation().await.unwrap();
    let proof = test_leader_proof(1, old.recovery_incarnation(), 1);
    let old_round = recovery_round(&old, 41, &proof, &[1]);

    let (_new_tx, new_rx) = watch::channel(Vec::new());
    let replacement = ClusterController::new(node, kv, None, new_rx);
    replacement.publish_recovery_incarnation().await.unwrap();

    let error = old.announce_stopped(&old_round).await.unwrap_err();
    assert!(error.contains("superseded local process"), "{error}");
}
