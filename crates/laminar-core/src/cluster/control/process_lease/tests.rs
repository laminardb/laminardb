use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};

use super::*;
use async_trait::async_trait;
use object_store::memory::InMemory;

fn store(node: NodeId, ttl_ms: i64) -> ProcessLeaseStore {
    ProcessLeaseStore::new(Arc::new(InMemory::new()), node, ttl_ms)
}

#[tokio::test]
async fn first_acquire() {
    let store = store(NodeId(7), 1_000);
    let owner = Uuid::from_u128(1);
    let ProcessLeaseOutcome::Acquired(lease) = store.try_acquire(owner, 10).await.unwrap() else {
        panic!("empty store must be acquired");
    };
    assert_eq!(lease.node, NodeId(7));
    assert_eq!(lease.owner, owner);
    assert_eq!(lease.term, 1);
    assert_eq!(lease.seq, 1);
    assert_eq!(lease.expires_at_ms, 1_010);
}

#[tokio::test]
async fn same_incarnation_renews_without_changing_term() {
    let store = store(NodeId(7), 1_000);
    let owner = Uuid::from_u128(1);
    store.try_acquire(owner, 10).await.unwrap();
    let ProcessLeaseOutcome::Acquired(lease) = store.try_acquire(owner, 500).await.unwrap() else {
        panic!("live owner must renew");
    };
    assert_eq!(lease.term, 1);
    assert_eq!(lease.seq, 2);
    assert_eq!(lease.expires_at_ms, 1_500);
}

#[tokio::test]
async fn live_rival_is_denied() {
    let store = store(NodeId(7), 1_000);
    let incumbent = Uuid::from_u128(1);
    store.try_acquire(incumbent, 10).await.unwrap();
    let ProcessLeaseOutcome::Held(lease) =
        store.try_acquire(Uuid::from_u128(2), 500).await.unwrap()
    else {
        panic!("live incumbent must not be replaced");
    };
    assert_eq!(lease.owner, incumbent);
    assert_eq!(lease.term, 1);
}

#[tokio::test]
async fn expired_takeover_advances_term() {
    let store = store(NodeId(7), 10);
    store.try_acquire(Uuid::from_u128(1), 10).await.unwrap();
    let replacement = Uuid::from_u128(2);
    let ProcessLeaseOutcome::Held(incumbent) =
        store.try_acquire(replacement, 10_000).await.unwrap()
    else {
        panic!("rival timestamps must not authorize takeover");
    };
    let observation = store.observe_rival(&incumbent).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let ProcessLeaseOutcome::Acquired(lease) = store
        .try_takeover(replacement, &observation, 20)
        .await
        .unwrap()
    else {
        panic!("expired identity must be replaceable");
    };
    assert_eq!(lease.owner, replacement);
    assert_eq!(lease.term, 2);
    assert_eq!(lease.seq, 2);
}

#[tokio::test]
async fn shared_fencing_authority_fails_when_the_predecessor_renews() {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let authority = Arc::new(
        ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_millis(50)).unwrap(),
    );
    let store = authority.store_for(NodeId(7));
    let owner = Uuid::from_u128(71);
    store.try_acquire(owner, 1).await.unwrap();
    let participant = crate::checkpoint::CheckpointParticipant {
        node_id: 7,
        boot_incarnation: owner,
    };
    let fencing = authority.fence_incarnation(
        participant,
        tokio::time::Instant::now() + Duration::from_secs(1),
    );
    tokio::pin!(fencing);
    tokio::select! {
        biased;
        result = &mut fencing => panic!("fencing completed before its full TTL: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    assert!(matches!(
        store.try_acquire(owner, 2).await.unwrap(),
        ProcessLeaseOutcome::Acquired(ProcessLease { seq: 2, .. })
    ));

    let error = fencing.await.unwrap_err();
    assert!(error.to_string().contains("renewed"), "{error}");
}

#[tokio::test]
async fn shared_fencing_authority_recovers_and_verifies_its_exact_takeover() {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let authority =
        ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_millis(5)).unwrap();
    let store = authority.store_for(NodeId(7));
    let owner = Uuid::from_u128(71);
    store.try_acquire(owner, 1).await.unwrap();
    let participant = crate::checkpoint::CheckpointParticipant {
        node_id: 7,
        boot_incarnation: owner,
    };

    let fence = authority
        .fence_incarnation(
            participant,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(fence.predecessor.owner, owner);
    assert_ne!(fence.successor.owner, owner);
    assert!(authority
        .verify_fence(&fence, tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap());

    let recovered = authority
        .fence_incarnation(
            participant,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(recovered, fence);

    store
        .try_acquire(fence.successor.owner, now_millis())
        .await
        .unwrap();
    assert!(authority
        .verify_fence(&fence, tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap());
    assert_eq!(
        authority
            .fence_incarnation(
                participant,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap(),
        fence
    );
}

#[tokio::test]
async fn pruning_preserves_every_takeover_boundary_but_removes_routine_renewals() {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = ProcessLeaseStore::new(Arc::clone(&backing), NodeId(7), 1);
    let first = Uuid::from_u128(71);
    let second = Uuid::from_u128(72);
    let third = Uuid::from_u128(73);

    store.try_acquire(first, 1).await.unwrap();
    store.try_acquire(first, 2).await.unwrap();
    let ProcessLeaseOutcome::Acquired(first_head) = store.try_acquire(first, 3).await.unwrap()
    else {
        panic!("first process must renew");
    };
    let first_observation = store.observe_rival(&first_head).unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let ProcessLeaseOutcome::Acquired(second_start) = store
        .try_takeover(second, &first_observation, 4)
        .await
        .unwrap()
    else {
        panic!("second process must take over");
    };
    store.try_acquire(second, 5).await.unwrap();
    let ProcessLeaseOutcome::Acquired(second_head) = store.try_acquire(second, 6).await.unwrap()
    else {
        panic!("second process must renew");
    };
    let second_observation = store.observe_rival(&second_head).unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let ProcessLeaseOutcome::Acquired(third_start) = store
        .try_takeover(third, &second_observation, 7)
        .await
        .unwrap()
    else {
        panic!("third process must take over");
    };
    let ProcessLeaseOutcome::Acquired(third_head) = store.try_acquire(third, 8).await.unwrap()
    else {
        panic!("third process must renew");
    };

    ProcessLeaseStore::prune_history_batch(&backing, NodeId(7))
        .await
        .unwrap();
    assert_eq!(store.list_seqs().await.unwrap(), vec![7, 8]);
    let first_fence = ProcessLeaseFence::new(first_head, second_start).unwrap();
    assert_eq!(
        store.find_takeover_from(first).await.unwrap().unwrap(),
        first_fence
    );
    assert_eq!(
        store.find_takeover_from(second).await.unwrap().unwrap(),
        ProcessLeaseFence::new(second_head, third_start).unwrap()
    );
    assert_eq!(store.load().await.unwrap(), Some(third_head));
    let authority = ProcessLeaseAuthority::new(backing, Duration::from_millis(1)).unwrap();
    assert!(authority
        .verify_fence(
            &first_fence,
            tokio::time::Instant::now() + Duration::from_secs(1)
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn oversized_history_can_prune_back_below_the_normal_scan_bound() {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let node = NodeId(7);
    let owner = Uuid::from_u128(71);
    let record_count = PROCESS_LEASE_MAX_LIST_RECORDS + 4;
    let expected_head = u64::try_from(record_count).unwrap();
    for sequence in 1..=record_count {
        let sequence = u64::try_from(sequence).unwrap();
        let lease = ProcessLease {
            node,
            owner,
            term: 1,
            seq: sequence,
            expires_at_ms: i64::try_from(sequence).unwrap(),
        };
        backing
            .put_opts(
                &lease_path(node, sequence),
                PutPayload::from(Bytes::from(serde_json::to_vec(&lease).unwrap())),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
            .unwrap();
    }
    let store = ProcessLeaseStore::new(Arc::clone(&backing), node, 1);
    assert!(store.list_seqs().await.is_err());

    assert!(!ProcessLeaseStore::prune_history_batch(&backing, node)
        .await
        .unwrap());

    let sequences = store.list_seqs().await.unwrap();
    assert_eq!(sequences.first(), Some(&257));
    assert_eq!(sequences.last(), Some(&expected_head));
    assert_eq!(store.load().await.unwrap().unwrap().seq, expected_head);
}

#[tokio::test]
async fn shared_fencing_authority_rejects_missing_predecessor_history() {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let authority =
        ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_millis(5)).unwrap();
    let store = authority.store_for(NodeId(7));
    let owner = Uuid::from_u128(71);
    store.try_acquire(owner, 1).await.unwrap();
    let participant = crate::checkpoint::CheckpointParticipant {
        node_id: 7,
        boot_incarnation: owner,
    };
    let fence = authority
        .fence_incarnation(
            participant,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    ProcessLeaseStore::prune_history_batch(&backing, NodeId(7))
        .await
        .unwrap();
    backing
        .delete(&fence_path(NodeId(7), fence.predecessor.owner))
        .await
        .unwrap();
    backing
        .delete(&successor_fence_path(
            NodeId(7),
            fence.successor.owner,
            fence.successor.term,
        ))
        .await
        .unwrap();
    backing
        .delete(&lease_path(NodeId(7), fence.predecessor.seq))
        .await
        .unwrap();

    let error = authority
        .fence_incarnation(
            participant,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("missing"), "{error}");
}

#[tokio::test]
async fn delayed_previous_owner_renewal_cannot_overwrite_takeover() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let authority = ProcessLeaseStore::new(Arc::clone(&object_store), NodeId(7), 1);
    let first_owner = Uuid::from_u128(1);
    let ProcessLeaseOutcome::Acquired(first) = authority.try_acquire(first_owner, 1).await.unwrap()
    else {
        panic!("first owner must acquire the lease");
    };
    let delayed_renewal = ProcessLease {
        node: first.node,
        owner: first.owner,
        term: first.term,
        seq: 2,
        expires_at_ms: 100,
    };
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let delayed_server_put = {
        let object_store = Arc::clone(&object_store);
        let release = Arc::clone(&release);
        tokio::spawn(async move {
            release.acquire().await.unwrap().forget();
            object_store
                .put_opts(
                    &lease_path(NodeId(7), 2),
                    PutPayload::from(Bytes::from(serde_json::to_vec(&delayed_renewal).unwrap())),
                    PutOptions {
                        mode: PutMode::Create,
                        ..PutOptions::default()
                    },
                )
                .await
        })
    };

    let replacement = Uuid::from_u128(2);
    let ProcessLeaseOutcome::Held(incumbent) =
        authority.try_acquire(replacement, 10).await.unwrap()
    else {
        panic!("replacement must observe the incumbent");
    };
    let observation = authority.observe_rival(&incumbent).unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    let ProcessLeaseOutcome::Acquired(takeover) = authority
        .try_takeover(replacement, &observation, 20)
        .await
        .unwrap()
    else {
        panic!("replacement must win sequence two");
    };
    release.add_permits(1);
    assert!(matches!(
        delayed_server_put.await.unwrap(),
        Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. })
    ));
    assert_eq!(authority.load().await.unwrap(), Some(takeover));
}

struct PutBarrierStore {
    inner: Arc<dyn ObjectStore>,
    path: OsPath,
    arrivals: Option<tokio::sync::Barrier>,
    delay_response: bool,
    committed: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    conflict_as_precondition: bool,
}

impl std::fmt::Debug for PutBarrierStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PutBarrierStore")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for PutBarrierStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PutBarrierStore")
    }
}

#[async_trait]
impl ObjectStore for PutBarrierStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if location == &self.path {
            if let Some(arrivals) = &self.arrivals {
                arrivals.wait().await;
            }
        }
        let result = self.inner.put_opts(location, payload, options).await;
        if location == &self.path && self.delay_response {
            self.committed.add_permits(1);
            self.release
                .acquire()
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "PutBarrierStore",
                    source: Box::new(error),
                })?
                .forget();
        }
        if self.conflict_as_precondition
            && matches!(&result, Err(object_store::Error::AlreadyExists { .. }))
        {
            return Err(object_store::Error::Precondition {
                path: location.to_string(),
                source: Box::new(std::io::Error::other("injected create precondition")),
            });
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &OsPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&OsPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&OsPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &OsPath,
        to: &OsPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

struct GetBarrierStore {
    inner: Arc<dyn ObjectStore>,
    path: OsPath,
    armed: AtomicBool,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

impl std::fmt::Debug for GetBarrierStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GetBarrierStore")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for GetBarrierStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("GetBarrierStore")
    }
}

#[async_trait]
impl ObjectStore for GetBarrierStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &OsPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if location == &self.path && self.armed.swap(false, Ordering::AcqRel) {
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "GetBarrierStore",
                    source: Box::new(error),
                })?
                .forget();
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&OsPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&OsPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &OsPath,
        to: &OsPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn participant_term_verification_rechecks_the_head_after_fence_evidence() {
    let node = NodeId(7);
    let first = Uuid::from_u128(71);
    let second = Uuid::from_u128(72);
    let third = Uuid::from_u128(73);
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = ProcessLeaseStore::new(Arc::clone(&backing), node, 1);
    let ProcessLeaseOutcome::Acquired(first_head) = store.try_acquire(first, 1).await.unwrap()
    else {
        panic!("first process must acquire the lease");
    };
    let first_observation = store.observe_rival(&first_head).unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let ProcessLeaseOutcome::Acquired(second_head) = store
        .try_takeover(second, &first_observation, 2)
        .await
        .unwrap()
    else {
        panic!("second process must take over the lease");
    };
    let second_observation = store.observe_rival(&second_head).unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;

    let gated = Arc::new(GetBarrierStore {
        inner: Arc::clone(&backing),
        path: successor_fence_path(node, second, second_head.term),
        armed: AtomicBool::new(true),
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    let authority_store: Arc<dyn ObjectStore> = gated.clone();
    let authority =
        Arc::new(ProcessLeaseAuthority::new(authority_store, Duration::from_millis(1)).unwrap());
    let participant = crate::checkpoint::CheckpointParticipant {
        node_id: node.0,
        boot_incarnation: second,
    };
    let verify = {
        let authority = Arc::clone(&authority);
        tokio::spawn(async move {
            authority
                .verify_current_participant_term(
                    participant,
                    second_head.term,
                    tokio::time::Instant::now() + Duration::from_secs(1),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), gated.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(matches!(
        store
            .try_takeover(third, &second_observation, 3)
            .await
            .unwrap(),
        ProcessLeaseOutcome::Acquired(ProcessLease { owner, term: 3, .. }) if owner == third
    ));
    gated.release.add_permits(1);

    assert!(!verify.await.unwrap().unwrap());
}

#[tokio::test]
async fn create_cas_has_one_winner() {
    let node = NodeId(7);
    let racing: Arc<dyn ObjectStore> = Arc::new(PutBarrierStore {
        inner: Arc::new(InMemory::new()),
        path: lease_path(node, 1),
        arrivals: Some(tokio::sync::Barrier::new(2)),
        delay_response: false,
        committed: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        conflict_as_precondition: true,
    });
    let first = ProcessLeaseStore::new(Arc::clone(&racing), node, 1_000);
    let second = ProcessLeaseStore::new(racing, node, 1_000);
    let (left, right) = tokio::join!(
        first.try_acquire(Uuid::from_u128(1), 10),
        second.try_acquire(Uuid::from_u128(2), 10)
    );
    let (left, right) = (left.unwrap(), right.unwrap());
    let winners = usize::from(matches!(&left, ProcessLeaseOutcome::Acquired(_)))
        + usize::from(matches!(&right, ProcessLeaseOutcome::Acquired(_)));
    assert_eq!(winners, 1);
    let durable = first.load().await.unwrap().unwrap();
    assert!(matches!(
        (left, right),
        (ProcessLeaseOutcome::Acquired(ref won), ProcessLeaseOutcome::Held(ref held))
            | (ProcessLeaseOutcome::Held(ref held), ProcessLeaseOutcome::Acquired(ref won))
            if won == &durable && held == &durable
    ));
}

#[tokio::test]
async fn local_filesystem_supports_create_only_renewal() {
    let temp = tempfile::tempdir().unwrap();
    let filesystem: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(temp.path()).unwrap());
    let store = ProcessLeaseStore::new(filesystem, NodeId(7), 1_000);
    let owner = Uuid::from_u128(1);
    assert!(matches!(
        store.try_acquire(owner, 10).await.unwrap(),
        ProcessLeaseOutcome::Acquired(_)
    ));
    assert!(matches!(
        store.try_acquire(owner, 500).await.unwrap(),
        ProcessLeaseOutcome::Acquired(ProcessLease { seq: 2, .. })
    ));
}

#[tokio::test]
async fn renewal_history_keeps_only_latest_and_predecessor() {
    let store = store(NodeId(7), 1_000);
    let owner = Uuid::from_u128(1);
    for now in 0..8 {
        assert!(matches!(
            store.try_acquire(owner, now).await.unwrap(),
            ProcessLeaseOutcome::Acquired(_)
        ));
    }
    ProcessLeaseStore::prune_history(&store.store, NodeId(7))
        .await
        .unwrap();
    assert_eq!(store.list_seqs().await.unwrap(), vec![7, 8]);
    assert_eq!(store.load().await.unwrap().unwrap().seq, 8);
}

#[tokio::test]
async fn an_unhealthy_pruner_is_repaired_before_the_next_renewal() {
    let store = store(NodeId(7), 1_000);
    let owner = Uuid::from_u128(1);
    for now in 0..8 {
        store.try_acquire(owner, now).await.unwrap();
    }
    store.prune_healthy.store(false, Ordering::Release);

    assert!(matches!(
        store.try_acquire(owner, 9).await.unwrap(),
        ProcessLeaseOutcome::Acquired(ProcessLease { seq: 9, .. })
    ));
    assert_eq!(store.list_seqs().await.unwrap(), vec![7, 8, 9]);
    assert!(store.prune_healthy.load(Ordering::Acquire));
}

#[test]
fn renewal_manager_requires_the_exact_store_ttl() {
    let store = Arc::new(ProcessLeaseStore::new(
        Arc::new(InMemory::new()),
        NodeId(7),
        10,
    ));
    let owner = Uuid::from_u128(1);
    let initial = ProcessLease {
        node: NodeId(7),
        owner,
        term: 1,
        seq: 1,
        expires_at_ms: 10,
    };
    for ttl in [
        Duration::from_millis(20),
        Duration::from_millis(10) + Duration::from_nanos(1),
    ] {
        let Err(error) = ProcessLeaseManager::new(
            Arc::clone(&store),
            owner,
            ProcessLeaseConfig {
                ttl,
                renew_interval: Duration::from_millis(2),
            },
            Instant::now(),
            &initial,
        ) else {
            panic!("mismatched manager TTL must be rejected");
        };
        assert!(error.to_string().contains("exact store TTL"), "{error}");
    }
}

#[tokio::test]
async fn delayed_acquisition_response_cannot_publish_a_fresh_local_deadline() {
    let node = NodeId(7);
    let owner = Uuid::from_u128(1);
    let ttl = Duration::from_millis(30);
    let delayed = Arc::new(PutBarrierStore {
        inner: Arc::new(InMemory::new()),
        path: lease_path(node, 1),
        arrivals: None,
        delay_response: true,
        committed: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        conflict_as_precondition: false,
    });
    let object_store: Arc<dyn ObjectStore> = delayed.clone();
    let store = Arc::new(ProcessLeaseStore::new(object_store, node, 30));
    let acquisition_store = Arc::clone(&store);
    let acquisition_started_at = Instant::now();
    let acquisition = tokio::spawn(async move { acquisition_store.try_acquire(owner, 0).await });

    tokio::time::timeout(Duration::from_secs(1), delayed.committed.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(matches!(
        store.load().await.unwrap(),
        Some(ProcessLease { owner: current, .. }) if current == owner
    ));
    tokio::time::sleep(ttl + Duration::from_millis(20)).await;
    delayed.release.add_permits(1);
    let ProcessLeaseOutcome::Acquired(initial) =
        tokio::time::timeout(Duration::from_secs(1), acquisition)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    else {
        panic!("delayed durable acquisition must still return its committed lease");
    };

    let error = ProcessLeaseManager::new(
        store,
        owner,
        ProcessLeaseConfig {
            ttl,
            renew_interval: Duration::from_millis(5),
        },
        acquisition_started_at,
        &initial,
    )
    .err()
    .expect("an acquisition response after its TTL must fail closed");
    assert!(
        error.to_string().contains("response arrived after"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn delayed_first_poll_cannot_renew_after_initial_deadline() {
    let store = Arc::new(ProcessLeaseStore::new(
        Arc::new(InMemory::new()),
        NodeId(7),
        10,
    ));
    let owner = Uuid::from_u128(1);
    let acquisition_started_at = Instant::now();
    let ProcessLeaseOutcome::Acquired(initial) = store.try_acquire(owner, 0).await.unwrap() else {
        panic!("initial process lease must be acquired");
    };
    let manager = ProcessLeaseManager::new(
        Arc::clone(&store),
        owner,
        ProcessLeaseConfig {
            ttl: Duration::from_millis(10),
            renew_interval: Duration::from_millis(2),
        },
        acquisition_started_at,
        &initial,
    )
    .unwrap();
    let deadline = manager.deadline();
    let live = manager.live_watch();

    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!deadline.is_live());
    tokio::time::timeout(
        Duration::from_millis(100),
        manager.spawn(tokio_util::sync::CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(!*live.borrow());
    assert_eq!(store.load().await.unwrap(), Some(initial));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn shutdown_fences_the_published_process_grant() {
    let store = Arc::new(ProcessLeaseStore::new(
        Arc::new(InMemory::new()),
        NodeId(7),
        100,
    ));
    let owner = Uuid::from_u128(1);
    let acquisition_started_at = Instant::now();
    let ProcessLeaseOutcome::Acquired(initial) = store.try_acquire(owner, 0).await.unwrap() else {
        panic!("initial process lease must be acquired");
    };
    let manager = ProcessLeaseManager::new(
        store,
        owner,
        ProcessLeaseConfig {
            ttl: Duration::from_millis(100),
            renew_interval: Duration::from_millis(20),
        },
        acquisition_started_at,
        &initial,
    )
    .unwrap();
    let deadline = manager.deadline();
    let live = manager.live_watch();
    let shutdown = tokio_util::sync::CancellationToken::new();
    shutdown.cancel();

    manager.spawn(shutdown).await.unwrap();

    assert!(!deadline.is_live());
    assert!(!*live.borrow());
}
