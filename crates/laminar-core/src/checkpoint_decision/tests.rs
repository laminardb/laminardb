use super::*;
use async_trait::async_trait;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use tempfile::tempdir;

fn store_in(dir: &std::path::Path) -> CheckpointDecisionStore {
    CheckpointDecisionStore::local_filesystem(dir).unwrap()
}

async fn record_local_commits(store: &CheckpointDecisionStore, last_epoch: u64) {
    for epoch in 1..=last_epoch {
        store
            .record_outcome(
                epoch,
                epoch,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                None,
            )
            .await
            .unwrap();
    }
}

fn highest_commit_epoch(outcomes: &[CheckpointOutcome]) -> Option<u64> {
    outcomes
        .iter()
        .rev()
        .find(|outcome| outcome.is_commit())
        .map(|outcome| outcome.epoch)
}

struct DeferredPutStore {
    inner: Arc<dyn ObjectStore>,
    target: std::sync::Mutex<Option<OsPath>>,
    intercepted: std::sync::atomic::AtomicBool,
    apply_before_error: std::sync::atomic::AtomicBool,
    pending: std::sync::Mutex<Option<(OsPath, PutPayload, PutOptions)>>,
    reverse_lists: bool,
    reject_updates: std::sync::atomic::AtomicBool,
    blocked_overwrite: std::sync::Mutex<Option<OsPath>>,
    overwrite_entered: tokio::sync::Semaphore,
    overwrite_release: tokio::sync::Semaphore,
    strip_cas_tokens: std::sync::atomic::AtomicBool,
    forged_get: std::sync::Mutex<Option<(OsPath, ForgedGet)>>,
    requested_range_end: std::sync::atomic::AtomicU64,
    get_count: std::sync::atomic::AtomicU64,
}

#[derive(Clone, Copy)]
enum ForgedGet {
    InconsistentRange,
    OversizedStream,
}

impl DeferredPutStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            target: std::sync::Mutex::new(None),
            intercepted: std::sync::atomic::AtomicBool::new(false),
            apply_before_error: std::sync::atomic::AtomicBool::new(false),
            pending: std::sync::Mutex::new(None),
            reverse_lists: false,
            reject_updates: std::sync::atomic::AtomicBool::new(false),
            blocked_overwrite: std::sync::Mutex::new(None),
            overwrite_entered: tokio::sync::Semaphore::new(0),
            overwrite_release: tokio::sync::Semaphore::new(0),
            strip_cas_tokens: std::sync::atomic::AtomicBool::new(false),
            forged_get: std::sync::Mutex::new(None),
            requested_range_end: std::sync::atomic::AtomicU64::new(0),
            get_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    #[cfg(feature = "cluster")]
    fn with_reversed_lists(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            target: std::sync::Mutex::new(None),
            intercepted: std::sync::atomic::AtomicBool::new(false),
            apply_before_error: std::sync::atomic::AtomicBool::new(false),
            pending: std::sync::Mutex::new(None),
            reverse_lists: true,
            reject_updates: std::sync::atomic::AtomicBool::new(false),
            blocked_overwrite: std::sync::Mutex::new(None),
            overwrite_entered: tokio::sync::Semaphore::new(0),
            overwrite_release: tokio::sync::Semaphore::new(0),
            strip_cas_tokens: std::sync::atomic::AtomicBool::new(false),
            forged_get: std::sync::Mutex::new(None),
            requested_range_end: std::sync::atomic::AtomicU64::new(0),
            get_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn intercept(&self, target: OsPath) {
        *self.target.lock().unwrap() = Some(target);
        self.intercepted
            .store(false, std::sync::atomic::Ordering::Release);
        self.apply_before_error
            .store(false, std::sync::atomic::Ordering::Release);
        *self.pending.lock().unwrap() = None;
    }

    fn intercept_after_apply(&self, target: OsPath) {
        self.intercept(target);
        self.apply_before_error
            .store(true, std::sync::atomic::Ordering::Release);
    }

    async fn apply_pending(&self) -> object_store::Result<object_store::PutResult> {
        let (location, payload, options) =
            self.pending.lock().unwrap().take().expect("deferred put");
        self.inner.put_opts(&location, payload, options).await
    }

    fn reject_conditional_updates(&self) {
        self.reject_updates
            .store(true, std::sync::atomic::Ordering::Release);
    }

    fn block_next_overwrite(&self, target: OsPath) {
        *self.blocked_overwrite.lock().unwrap() = Some(target);
    }

    async fn wait_for_blocked_overwrite(&self) {
        self.overwrite_entered
            .acquire()
            .await
            .expect("overwrite gate is open")
            .forget();
    }

    fn release_blocked_overwrite(&self) {
        self.overwrite_release.add_permits(1);
    }

    fn strip_cas_tokens(&self) {
        self.strip_cas_tokens
            .store(true, std::sync::atomic::Ordering::Release);
    }

    fn forge_get(&self, target: OsPath, forged: ForgedGet) {
        *self.forged_get.lock().unwrap() = Some((target, forged));
        self.requested_range_end
            .store(0, std::sync::atomic::Ordering::Release);
    }

    fn requested_range_end(&self) -> u64 {
        self.requested_range_end
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn reset_get_count(&self) {
        self.get_count
            .store(0, std::sync::atomic::Ordering::Release);
    }

    fn get_count(&self) -> u64 {
        self.get_count.load(std::sync::atomic::Ordering::Acquire)
    }
}

impl std::fmt::Debug for DeferredPutStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeferredPutStore")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for DeferredPutStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DeferredPutStore")
    }
}

#[async_trait]
impl ObjectStore for DeferredPutStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if self
            .reject_updates
            .load(std::sync::atomic::Ordering::Acquire)
            && matches!(&options.mode, PutMode::Update(_))
        {
            return Err(object_store::Error::NotImplemented {
                operation: "put_opts with Update".into(),
                implementer: "DeferredPutStore".into(),
            });
        }
        let block_overwrite = matches!(&options.mode, PutMode::Overwrite)
            && self.blocked_overwrite.lock().unwrap().as_ref() == Some(location);
        if block_overwrite {
            *self.blocked_overwrite.lock().unwrap() = None;
            self.overwrite_entered.add_permits(1);
            self.overwrite_release
                .acquire()
                .await
                .expect("overwrite release gate is open")
                .forget();
        }
        let target = self.target.lock().unwrap().clone();
        if target.as_ref() == Some(location)
            && !self
                .intercepted
                .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            if self
                .apply_before_error
                .load(std::sync::atomic::Ordering::Acquire)
            {
                self.inner.put_opts(location, payload, options).await?;
                return Err(object_store::Error::Generic {
                    store: "DeferredPutStore",
                    source: Box::new(std::io::Error::other(
                        "injected response loss after remote visibility",
                    )),
                });
            }
            *self.pending.lock().unwrap() =
                Some((location.clone(), payload.clone(), options.clone()));
            return Err(object_store::Error::Generic {
                store: "DeferredPutStore",
                source: Box::new(std::io::Error::other(
                    "injected response loss before remote visibility",
                )),
            });
        }
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
        self.get_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if let Some(GetRange::Bounded(range)) = options.range.as_ref() {
            self.requested_range_end
                .store(range.end, std::sync::atomic::Ordering::Release);
        }
        let mut result = self.inner.get_opts(location, options).await?;
        if self
            .strip_cas_tokens
            .load(std::sync::atomic::Ordering::Acquire)
        {
            result.meta.e_tag = None;
            result.meta.version = None;
        }
        let forged = self
            .forged_get
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(target, _)| target == location)
            .map(|(_, forged)| *forged);
        match forged {
            Some(ForgedGet::InconsistentRange) => {
                result.meta.size = 1;
                result.range = 0..2;
            }
            Some(ForgedGet::OversizedStream) => {
                result.meta.size = 1;
                result.range = 0..1;
                result.payload = object_store::GetResultPayload::Stream(
                    futures::stream::iter([
                        Ok::<Bytes, object_store::Error>(Bytes::from_static(b"x")),
                        Ok::<Bytes, object_store::Error>(Bytes::from_static(b"y")),
                    ])
                    .boxed(),
                );
            }
            None => {}
        }
        Ok(result)
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
        let listed = self.inner.list(prefix);
        if !self.reverse_lists {
            return listed;
        }
        futures::stream::once(async move {
            let mut entries = listed.collect::<Vec<_>>().await;
            entries.sort_by(|left, right| match (left, right) {
                (Ok(left), Ok(right)) => right.location.as_ref().cmp(left.location.as_ref()),
                (Err(_), Ok(_)) => std::cmp::Ordering::Less,
                (Ok(_), Err(_)) => std::cmp::Ordering::Greater,
                (Err(_), Err(_)) => std::cmp::Ordering::Equal,
            });
            futures::stream::iter(entries)
        })
        .flatten()
        .boxed()
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

fn assignment_fence(assignment_version: u64, participant_ids: &[u64]) -> CheckpointAssignmentFence {
    let participants = participant_ids
        .iter()
        .map(|node_id| crate::checkpoint::CheckpointParticipant {
            node_id: *node_id,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(*node_id) + 1_000),
        })
        .collect();
    CheckpointAssignmentFence::from_owner_map(assignment_version, participant_ids, participants)
        .unwrap()
}

fn leader_proof(
    fence: &CheckpointAssignmentFence,
    node_id: u64,
    process_term: u64,
    fencing_token: u64,
) -> LeaderProof {
    LeaderProof {
        owner: crate::checkpoint::LeaderProofOwner {
            node_id,
            boot_id: fence.participant_incarnation(node_id).unwrap(),
            process_term,
        },
        fencing_token,
    }
}

fn digest(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn test_sink_open_witness(
    deployment_id: String,
    checkpoint_id: u64,
    create_token: u128,
) -> CheckpointSinkOpenWitness {
    CheckpointSinkOpenWitness {
        version: CHECKPOINT_SINK_OPEN_WITNESS_VERSION,
        deployment_id,
        pipeline_identity: PipelineIdentity::empty(),
        participant_id: 0,
        attempt: CheckpointAttempt::canonical(checkpoint_id),
        committable_sinks: vec!["sink-a".into(), "sink-b".into()],
        create_token: uuid::Uuid::from_u128(create_token).to_string(),
    }
}

async fn test_capsule(
    store: &CheckpointDecisionStore,
    epoch: u64,
    checkpoint_id: u64,
    fence: &CheckpointAssignmentFence,
) -> ClusterRecoveryCapsule {
    let deployment_id = store.load_or_create_deployment_id().await.unwrap();
    let portable_state_sha256 = digest(9);
    let participants = fence
        .participant_ids()
        .into_iter()
        .map(|participant_id| crate::checkpoint::ParticipantRecoveryRef {
            participant_id,
            readiness_sha256: digest(3),
            manifest_sha256: digest(4),
            portable_state_sha256: portable_state_sha256.clone(),
        })
        .collect();
    ClusterRecoveryCapsule {
        version: crate::checkpoint::CLUSTER_RECOVERY_CAPSULE_VERSION,
        attempt: crate::state::CheckpointAttempt::new(epoch, checkpoint_id),
        deployment_id,
        pipeline_identity: crate::checkpoint::PipelineIdentity::empty(),
        assignment_fence: fence.clone(),
        seal_inventory_sha256: digest(2),
        vnode_restore_contract:
            crate::checkpoint::recovery_capsule::vnode_restore_contract_for_test(fence.vnode_count),
        participants,
        source_offsets: std::collections::BTreeMap::new(),
        source_metadata: std::collections::BTreeMap::new(),
        source_assignment_versions: std::collections::BTreeMap::new(),
        source_watermarks: std::collections::BTreeMap::new(),
        cluster_watermark: crate::checkpoint::CheckpointWatermark::Uninitialized,
        recovery_watermark_frontier: None,
        portable_state_sha256,
    }
}

async fn create_capsule_ref(
    store: &CheckpointDecisionStore,
    epoch: u64,
    checkpoint_id: u64,
    fence: &CheckpointAssignmentFence,
) -> RecoveryCapsuleRef {
    let capsule = test_capsule(store, epoch, checkpoint_id, fence).await;
    store.create_recovery_capsule(&capsule).await.unwrap()
}

#[test]
fn inventory_paths_require_canonical_protocol_names() {
    assert_eq!(
        CheckpointDecisionStore::outcome_epoch_segment("checkpoint-outcomes/epoch=5/outcome"),
        Some("5")
    );
    assert_eq!(
        CheckpointDecisionStore::outcome_epoch_segment("checkpoint-outcomes/epoch=5/other"),
        None
    );
    for malformed in [
        "checkpoint-outcomes/epoch=0/outcome",
        "checkpoint-outcomes/epoch=05/outcome",
        "checkpoint-outcomes/epoch=+5/outcome",
        "checkpoint-outcomes/epoch=5//outcome",
    ] {
        assert_eq!(
            CheckpointDecisionStore::outcome_epoch_segment(malformed),
            None,
            "accepted noncanonical outcome path {malformed}"
        );
    }
}

async fn put_oversized_control_record(store: &Arc<dyn ObjectStore>, path: &OsPath, maximum: u64) {
    let size = usize::try_from(maximum + 1).unwrap();
    store
        .put(path, PutPayload::from(Bytes::from(vec![b'x'; size])))
        .await
        .unwrap();
}

fn assert_oversized_control_record(error: &DecisionError, record: &str, maximum: u64) {
    let message = error.to_string();
    assert!(message.contains(record), "{message}");
    assert!(
        message.contains(&format!("maximum is {maximum}")),
        "{message}"
    );
}

#[tokio::test]
async fn control_record_reads_reject_oversized_objects() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));

    put_oversized_control_record(
        &object_store,
        &CheckpointDecisionStore::deployment_identity_path(),
        DEPLOYMENT_IDENTITY_MAX_BYTES,
    )
    .await;
    let error = store.read_deployment_identity().await.unwrap_err();
    assert_oversized_control_record(&error, "deployment identity", DEPLOYMENT_IDENTITY_MAX_BYTES);

    put_oversized_control_record(
        &object_store,
        &CheckpointDecisionStore::sink_open_witness_path(),
        CHECKPOINT_SINK_OPEN_WITNESS_MAX_BYTES,
    )
    .await;
    let error = store.read_sink_open_witness_record().await.unwrap_err();
    assert_oversized_control_record(
        &error,
        "sink-open witness",
        CHECKPOINT_SINK_OPEN_WITNESS_MAX_BYTES,
    );

    let outcome_path = CheckpointDecisionStore::outcome_path(1);
    put_oversized_control_record(&object_store, &outcome_path, CHECKPOINT_OUTCOME_MAX_BYTES).await;
    let error = store
        .read_outcome_record(&outcome_path, 1)
        .await
        .unwrap_err();
    assert_oversized_control_record(&error, "checkpoint outcome", CHECKPOINT_OUTCOME_MAX_BYTES);

    let deployment_id = uuid::Uuid::from_u128(1).to_string();
    let floor_path = CheckpointDecisionStore::outcome_gc_floor_path(&deployment_id);
    put_oversized_control_record(&object_store, &floor_path, OUTCOME_GC_FLOOR_MAX_BYTES).await;
    let error = store
        .read_outcome_gc_floor(&deployment_id)
        .await
        .unwrap_err();
    assert_oversized_control_record(&error, "outcome GC floor", OUTCOME_GC_FLOOR_MAX_BYTES);

    let reference = RecoveryCapsuleRef {
        epoch: 1,
        checkpoint_id: 1,
        sha256: digest(1),
        len: 1,
    };
    let capsule_path = CheckpointDecisionStore::recovery_capsule_path(&reference);
    let capsule_maximum = u64::try_from(crate::checkpoint::MAX_RECOVERY_CAPSULE_BYTES).unwrap();
    put_oversized_control_record(&object_store, &capsule_path, capsule_maximum).await;
    let error = store.load_recovery_capsule(&reference).await.unwrap_err();
    assert_oversized_control_record(&error, "recovery capsule", capsule_maximum);
}

#[tokio::test]
async fn control_record_reads_bound_the_requested_range_and_streamed_body() {
    for (forged, expected_error) in [
        (ForgedGet::InconsistentRange, "inconsistent with advertised"),
        (ForgedGet::OversizedStream, "exceeded its advertised"),
    ] {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = CheckpointDecisionStore::deployment_identity_path();
        inner
            .put(&path, PutPayload::from_static(b"x"))
            .await
            .unwrap();
        let fault = Arc::new(DeferredPutStore::new(inner));
        fault.forge_get(path, forged);
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let store = CheckpointDecisionStore::new(object_store);

        let error = store.read_deployment_identity().await.unwrap_err();
        assert!(error.to_string().contains(expected_error), "{error}");
        assert_eq!(
            fault.requested_range_end(),
            DEPLOYMENT_IDENTITY_MAX_BYTES + 1,
            "control metadata must never request an unbounded body"
        );
    }
}

#[tokio::test]
async fn native_cas_metadata_requires_an_update_authority() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let deployment_id = uuid::Uuid::from_u128(1).to_string();
    let floor = OutcomeGcFloor {
        version: OUTCOME_GC_FLOOR_VERSION,
        deployment_id: deployment_id.clone(),
        before_epoch: 1,
        terminal_anchor: None,
        committed_anchor: None,
    };
    inner
        .put(
            &CheckpointDecisionStore::outcome_gc_floor_path(&deployment_id),
            PutPayload::from(Bytes::from(serde_json::to_vec(&floor).unwrap())),
        )
        .await
        .unwrap();
    let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
    fault.strip_cas_tokens();
    let object_store: Arc<dyn ObjectStore> = fault.clone();
    let store = CheckpointDecisionStore::new(object_store);

    let error = store
        .read_outcome_gc_floor(&deployment_id)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("outcome GC floor"), "{error}");
    assert!(error.to_string().contains("neither an ETag"), "{error}");

    #[cfg(feature = "cluster")]
    {
        let cursor = RecoveryCapsuleGcCursor {
            version: RECOVERY_CAPSULE_GC_CURSOR_VERSION,
            deployment_id: deployment_id.clone(),
            offset: None,
        };
        inner
            .put(
                &CheckpointDecisionStore::recovery_capsule_gc_cursor_path(&deployment_id),
                PutPayload::from(Bytes::from(serde_json::to_vec(&cursor).unwrap())),
            )
            .await
            .unwrap();
        let error = store
            .read_recovery_capsule_gc_cursor(&deployment_id)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("recovery capsule GC cursor"),
            "{error}"
        );
        assert!(error.to_string().contains("neither an ETag"), "{error}");
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn recovery_capsule_cursor_read_rejects_oversized_object() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let deployment_id = uuid::Uuid::from_u128(1).to_string();
    put_oversized_control_record(
        &object_store,
        &CheckpointDecisionStore::recovery_capsule_gc_cursor_path(&deployment_id),
        RECOVERY_CAPSULE_GC_CURSOR_MAX_BYTES,
    )
    .await;

    let error = store
        .read_recovery_capsule_gc_cursor(&deployment_id)
        .await
        .unwrap_err();
    assert_oversized_control_record(
        &error,
        "recovery capsule GC cursor",
        RECOVERY_CAPSULE_GC_CURSOR_MAX_BYTES,
    );
}

#[tokio::test]
async fn recovery_capsule_create_is_idempotent_and_load_verifies_reference() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(DeferredPutStore::new(inner));
    let object_store: Arc<dyn ObjectStore> = fault.clone();
    let store = CheckpointDecisionStore::new(object_store);
    let fence = assignment_fence(12, &[2, 7]);
    let capsule = test_capsule(&store, 4, 4, &fence).await;

    fault.reset_get_count();
    let reference = store.create_recovery_capsule(&capsule).await.unwrap();
    assert_eq!(
        fault.get_count(),
        0,
        "a confirmed immutable create must not read back its body"
    );

    fault.reset_get_count();
    assert_eq!(
        store.create_recovery_capsule(&capsule).await.unwrap(),
        reference
    );
    assert!(
        fault.get_count() > 0,
        "an existing immutable body must be reconciled after create conflict"
    );
    assert_eq!(
        store.load_recovery_capsule(&reference).await.unwrap(),
        capsule
    );

    let mut wrong_length = reference.clone();
    wrong_length.len += 1;
    assert!(store.load_recovery_capsule(&wrong_length).await.is_err());

    let mut wrong_digest = reference;
    wrong_digest.sha256 = digest(0xaa);
    assert!(store.load_recovery_capsule(&wrong_digest).await.is_err());
}

#[tokio::test]
async fn recovery_capsule_create_reconciles_a_lost_success_response() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
    let object_store: Arc<dyn ObjectStore> = fault.clone();
    let store = CheckpointDecisionStore::new(object_store);
    let fence = assignment_fence(12, &[2, 7]);
    let capsule = test_capsule(&store, 4, 4, &fence).await;
    let (_, expected) = capsule.encode_and_reference().unwrap();
    fault.intercept_after_apply(CheckpointDecisionStore::recovery_capsule_path(&expected));

    fault.reset_get_count();
    let observed = store.create_recovery_capsule(&capsule).await.unwrap();

    assert_eq!(observed, expected);
    assert!(
        fault.get_count() > 0,
        "an ambiguous create response must reconcile the remote body"
    );
    inner
        .head(&CheckpointDecisionStore::recovery_capsule_path(&observed))
        .await
        .expect("the lost response followed a remotely visible create");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cyclic_capsule_cleanup_finds_a_create_visible_after_client_failure() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
    let object_store: Arc<dyn ObjectStore> = fault.clone();
    let store = CheckpointDecisionStore::new(object_store);
    let fence = assignment_fence(12, &[2, 7]);
    let capsule = test_capsule(&store, 1, 1, &fence).await;
    let (_, reference) = capsule.encode_and_reference().unwrap();
    let path = CheckpointDecisionStore::recovery_capsule_path(&reference);
    fault.intercept(path.clone());

    fault.reset_get_count();
    let error = store
        .create_recovery_capsule(&capsule)
        .await
        .expect_err("the client must observe the injected ambiguous failure");
    assert!(matches!(error, DecisionError::Io(_)));
    assert!(
        fault.get_count() > 0,
        "an ambiguous failed create must reconcile before returning"
    );
    assert!(matches!(
        inner.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    let live = std::collections::BTreeSet::new();
    assert!(
        store
            .sweep_recovery_capsules_step(2, &live)
            .await
            .unwrap()
            .pending
    );
    fault.apply_pending().await.unwrap();
    inner
        .head(&path)
        .await
        .expect("deferred server-side create became visible");

    let step = store.sweep_recovery_capsules_step(2, &live).await.unwrap();
    assert_eq!(step.deleted, 1);
    assert!(step.pending);
    assert!(matches!(
        inner.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn capsule_cleanup_progress_is_independent_of_list_order() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let reversed: Arc<dyn ObjectStore> =
        Arc::new(DeferredPutStore::with_reversed_lists(Arc::clone(&inner)));
    let store = CheckpointDecisionStore::new(reversed);
    let fence = assignment_fence(12, &[2, 7]);
    let oldest = create_capsule_ref(&store, 1, 1, &fence).await;
    let oldest_path = CheckpointDecisionStore::recovery_capsule_path(&oldest);
    let mut newest_path = None;
    for epoch in 2..=70u64 {
        let reference = RecoveryCapsuleRef {
            epoch,
            checkpoint_id: epoch,
            sha256: digest(u8::try_from(epoch).unwrap()),
            len: 1,
        };
        let path = CheckpointDecisionStore::recovery_capsule_path(&reference);
        inner
            .put(&path, PutPayload::from(Bytes::from_static(b"x")))
            .await
            .unwrap();
        newest_path = Some(path);
    }

    let step = store
        .sweep_recovery_capsules_step(2, &std::collections::BTreeSet::new())
        .await
        .unwrap();
    assert_eq!(step.examined, RECOVERY_CAPSULE_GC_BATCH_SIZE);
    assert_eq!(step.deleted, 1);
    assert!(matches!(
        inner.head(&oldest_path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    inner
        .head(&newest_path.unwrap())
        .await
        .expect("newer retained capsule must survive unordered maintenance");
}

#[tokio::test]
async fn recovery_capsule_load_rejects_tampered_body() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let fence = assignment_fence(12, &[2, 7]);
    let capsule = test_capsule(&store, 4, 4, &fence).await;
    let reference = store.create_recovery_capsule(&capsule).await.unwrap();
    let path = CheckpointDecisionStore::recovery_capsule_path(&reference);
    let mut encoded = crate::checkpoint::canonical_json_bytes(&capsule).unwrap();
    let position = encoded
        .iter()
        .position(|byte| *byte == b'4')
        .expect("test capsule contains a digit");
    encoded[position] = b'5';
    object_store
        .put(&path, PutPayload::from(Bytes::from(encoded)))
        .await
        .unwrap();

    let error = store.load_recovery_capsule(&reference).await.unwrap_err();
    assert!(matches!(error, DecisionError::Conflict(_)));
}

#[tokio::test]
async fn outcome_shape_requires_capsule_only_for_cluster_commit() {
    let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
    let fence = assignment_fence(12, &[2, 7]);
    let proof = leader_proof(&fence, 2, 3, 4);
    let reference = create_capsule_ref(&store, 4, 4, &fence).await;

    let missing = store
        .canonical_outcome(
            4,
            4,
            CheckpointScope::Cluster,
            Some(fence.clone()),
            Some(proof.clone()),
            CheckpointVerdict::Commit,
            None,
        )
        .await
        .unwrap_err();
    assert!(missing.to_string().contains("requires a recovery capsule"));

    let abort_with_capsule = store
        .canonical_outcome(
            4,
            4,
            CheckpointScope::Cluster,
            Some(fence.clone()),
            Some(proof),
            CheckpointVerdict::Abort,
            Some(reference.clone()),
        )
        .await
        .unwrap_err();
    assert!(abort_with_capsule
        .to_string()
        .contains("abort outcome for epoch 4 cannot carry"));

    let local_with_capsule = store
        .canonical_outcome(
            4,
            4,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            Some(reference),
        )
        .await
        .unwrap_err();
    assert!(local_with_capsule
        .to_string()
        .contains("local outcome for epoch 4 cannot carry"));
}

#[tokio::test]
async fn outcome_rejects_capsule_for_a_different_attempt() {
    let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
    let fence = assignment_fence(12, &[2, 7]);
    let proof = leader_proof(&fence, 2, 3, 4);
    let reference = create_capsule_ref(&store, 4, 4, &fence).await;

    let error = store
        .canonical_outcome(
            5,
            5,
            CheckpointScope::Cluster,
            Some(fence),
            Some(proof),
            CheckpointVerdict::Commit,
            Some(reference),
        )
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("does not match recovery capsule"));
}

#[tokio::test]
async fn standalone_outcome_objects_reject_cluster_authority() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let fence = assignment_fence(12, &[2, 7]);
    let proof = leader_proof(&fence, 2, 3, 4);
    let error = store
        .record_outcome(
            4,
            4,
            CheckpointScope::Cluster,
            Some(fence.clone()),
            Some(proof.clone()),
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("shared leader authority"));

    let forged = store
        .canonical_outcome(
            4,
            4,
            CheckpointScope::Cluster,
            Some(fence),
            Some(proof),
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
    object_store
        .put(
            &CheckpointDecisionStore::outcome_path(4),
            PutPayload::from(Bytes::from(serde_json::to_vec(&forged).unwrap())),
        )
        .await
        .unwrap();
    let error = store.outcome(4).await.unwrap_err();
    assert!(error
        .to_string()
        .contains("outside the shared leader authority"));
}

#[tokio::test]
async fn terminal_outcome_retry_is_idempotent_and_conflict_returns_winner() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let commit = CheckpointVerdict::Commit;

    let created = store
        .record_outcome(
            7,
            7,
            CheckpointScope::Local,
            None,
            None,
            commit.clone(),
            None,
        )
        .await
        .unwrap();
    let RecordOutcomeResult::Created(winner) = created else {
        panic!("first create must win");
    };

    assert_eq!(
        store
            .record_outcome(7, 7, CheckpointScope::Local, None, None, commit, None)
            .await
            .unwrap(),
        RecordOutcomeResult::Unchanged(winner.clone())
    );
    assert_eq!(
        store
            .record_outcome(
                7,
                7,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap(),
        RecordOutcomeResult::Conflict {
            winner: winner.clone()
        }
    );
    assert_eq!(store.outcome(7).await.unwrap(), Some(winner));
}

#[tokio::test]
async fn recovery_abort_wins_against_a_delayed_commit_create() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
    let object_store: Arc<dyn ObjectStore> = fault.clone();
    let store = CheckpointDecisionStore::new(object_store);
    fault.intercept(CheckpointDecisionStore::outcome_path(2));

    let error = store
        .record_outcome(
            2,
            2,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            None,
        )
        .await
        .expect_err("the delayed create is not yet durably visible");
    assert!(matches!(error, DecisionError::Io(_)));

    let RecordOutcomeResult::Created(abort) = store
        .record_outcome(
            2,
            2,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap()
    else {
        panic!("recovery Abort must win the create-once race");
    };
    assert!(matches!(
        fault.apply_pending().await,
        Err(object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. })
    ));
    assert_eq!(store.outcome(2).await.unwrap(), Some(abort));
}

#[tokio::test]
async fn delayed_commit_wins_before_recovery_abort_create() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
    let object_store: Arc<dyn ObjectStore> = fault.clone();
    let store = CheckpointDecisionStore::new(object_store);
    fault.intercept(CheckpointDecisionStore::outcome_path(2));

    let error = store
        .record_outcome(
            2,
            2,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            None,
        )
        .await
        .expect_err("the delayed create is not yet durably visible");
    assert!(matches!(error, DecisionError::Io(_)));
    fault.apply_pending().await.unwrap();

    let RecordOutcomeResult::Conflict { winner } = store
        .record_outcome(
            2,
            2,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap()
    else {
        panic!("the visible Commit must remain the terminal winner");
    };
    assert!(winner.is_commit());
    assert_eq!(store.outcome(2).await.unwrap(), Some(winner));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_commit_and_abort_converge_on_one_terminal_winner() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let commit_store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let commit = tokio::spawn(async move {
        commit_store
            .record_outcome(
                8,
                8,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                None,
            )
            .await
            .unwrap()
    });

    let abort_store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let abort = tokio::spawn(async move {
        abort_store
            .record_outcome(
                8,
                8,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap()
    });

    let results = [commit.await.unwrap(), abort.await.unwrap()];
    let restarted = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let winner = restarted.outcome(8).await.unwrap().unwrap();
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, RecordOutcomeResult::Created(_)))
            .count(),
        1
    );
    for result in results {
        match result {
            RecordOutcomeResult::Created(observed)
            | RecordOutcomeResult::Unchanged(observed)
            | RecordOutcomeResult::Conflict { winner: observed } => {
                assert_eq!(observed, winner);
            }
        }
    }
}

#[tokio::test]
async fn terminal_outcomes_survive_restart_and_absence_is_not_abort() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    store
        .record_outcome(
            4,
            4,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            None,
        )
        .await
        .unwrap();
    store
        .record_outcome(
            5,
            5,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
    drop(store);

    let restarted = CheckpointDecisionStore::new(object_store);
    assert!(matches!(
        restarted.outcome(5).await.unwrap().unwrap().verdict,
        CheckpointVerdict::Abort
    ));
    assert_eq!(restarted.outcome(6).await.unwrap(), None);
    let outcomes = restarted.outcomes().await.unwrap();
    assert_eq!(highest_commit_epoch(&outcomes), Some(4));
}

#[tokio::test]
async fn abort_after_commit_advances_terminal_without_replacing_live_commit() {
    let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
    store
        .record_outcome(
            4,
            4,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            None,
        )
        .await
        .unwrap();
    store
        .record_outcome(
            5,
            5,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        store
            .highest_terminal_outcome()
            .await
            .unwrap()
            .unwrap()
            .epoch,
        5
    );
    let outcomes = store.outcomes().await.unwrap();
    assert_eq!(highest_commit_epoch(&outcomes), Some(4));
}

#[tokio::test]
async fn outcome_floor_rejects_late_create_and_survives_restart() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    for (epoch, checkpoint_id, verdict) in [
        (1, 1, CheckpointVerdict::Commit),
        (2, 2, CheckpointVerdict::Abort),
        (5, 5, CheckpointVerdict::Commit),
    ] {
        store
            .record_outcome(
                epoch,
                checkpoint_id,
                CheckpointScope::Local,
                None,
                None,
                verdict,
                None,
            )
            .await
            .unwrap();
    }
    assert_eq!(store.prune_outcomes_before(4).await.unwrap(), 4);

    let error = store
        .record_outcome(
            3,
            3,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, DecisionError::Conflict(_)));
    assert!(error
        .to_string()
        .contains("below durable outcome GC horizon 4"));
    assert_eq!(store.outcome(3).await.unwrap(), None);
    drop(store);

    let restarted = CheckpointDecisionStore::new(object_store);
    assert_eq!(restarted.outcome_gc_floor_horizon().await.unwrap(), 4);
    let live = restarted.outcomes().await.unwrap();
    assert_eq!(
        live.iter()
            .map(|outcome| (outcome.epoch, outcome.checkpoint_id))
            .collect::<Vec<_>>(),
        vec![(5, 5)]
    );
    let continuity = restarted.audited_outcomes().await.unwrap();
    assert_eq!(
        continuity
            .iter()
            .map(|outcome| (outcome.epoch, outcome.checkpoint_id))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 2), (5, 5)]
    );
    assert!(matches!(&continuity[1].verdict, CheckpointVerdict::Abort));
    assert_eq!(highest_commit_epoch(&live), Some(5));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_outcome_floor_advancement_is_monotonic() {
    const LAST_EPOCH: u64 = 32;

    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = CheckpointDecisionStore::new(Arc::clone(&object_store));
    for epoch in 1..=LAST_EPOCH {
        writer
            .record_outcome(
                epoch,
                epoch,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                None,
            )
            .await
            .unwrap();
    }

    let mut tasks = tokio::task::JoinSet::new();
    for before in 2..=LAST_EPOCH {
        let object_store = Arc::clone(&object_store);
        tasks.spawn(async move {
            let store = CheckpointDecisionStore::new(object_store);
            (before, store.prune_outcomes_before(before).await)
        });
    }
    while let Some(result) = tasks.join_next().await {
        let (requested, horizon) = result.unwrap();
        assert!(
            horizon.unwrap() >= requested,
            "a concurrent floor winner may supersede but never regress a request"
        );
    }

    let restarted = CheckpointDecisionStore::new(object_store);
    assert_eq!(
        restarted.outcome_gc_floor_horizon().await.unwrap(),
        LAST_EPOCH
    );
    assert_eq!(
        restarted.outcome_retention_boundary().await.unwrap(),
        OutcomeRetentionBoundary {
            before_epoch: LAST_EPOCH,
            committed_checkpoint_id: Some(LAST_EPOCH - 1),
            highest_closed_epoch: Some(LAST_EPOCH - 1),
        }
    );
    assert_eq!(
        restarted
            .outcomes()
            .await
            .unwrap()
            .into_iter()
            .map(|outcome| outcome.epoch)
            .collect::<Vec<_>>(),
        vec![LAST_EPOCH]
    );
}

#[tokio::test]
async fn local_filesystem_outcome_floor_advances_repeatedly_and_survives_restart() {
    const LAST_EPOCH: u64 = 24;

    let dir = tempdir().unwrap();
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let store = CheckpointDecisionStore::local_single_writer(Arc::clone(&object_store));
    let deployment_id = store.load_or_create_deployment_id().await.unwrap();
    record_local_commits(&store, LAST_EPOCH).await;

    for before in 2..=LAST_EPOCH {
        assert_eq!(store.prune_outcomes_before(before).await.unwrap(), before);
    }
    assert_eq!(store.outcome_gc_floor_horizon().await.unwrap(), LAST_EPOCH);
    assert_eq!(
        store
            .outcomes()
            .await
            .unwrap()
            .into_iter()
            .map(|outcome| outcome.epoch)
            .collect::<Vec<_>>(),
        vec![LAST_EPOCH]
    );

    let mut floors = object_store.list(Some(&OsPath::from("checkpoint-outcome-gc/")));
    let mut floor_paths = Vec::new();
    while let Some(entry) = floors.next().await {
        floor_paths.push(entry.unwrap().location);
    }
    assert_eq!(
        floor_paths,
        vec![CheckpointDecisionStore::outcome_gc_floor_path(
            &deployment_id
        )]
    );

    drop(store);
    let restarted = CheckpointDecisionStore::local_single_writer(object_store);
    assert_eq!(
        restarted.outcome_gc_floor_horizon().await.unwrap(),
        LAST_EPOCH
    );
    let outcomes = restarted.outcomes().await.unwrap();
    assert_eq!(highest_commit_epoch(&outcomes), Some(LAST_EPOCH));
}

#[test]
fn independently_opened_local_filesystem_stores_share_the_namespace_rmw_lock() {
    let dir = tempdir().unwrap();
    let first = CheckpointDecisionStore::local_filesystem(dir.path()).unwrap();
    let second = CheckpointDecisionStore::local_filesystem(dir.path()).unwrap();

    assert!(Arc::ptr_eq(
        first.local_metadata_rmw_lock.as_ref().unwrap(),
        second.local_metadata_rmw_lock.as_ref().unwrap()
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_filesystem_concurrent_floor_advancement_is_monotonic() {
    const LAST_EPOCH: u64 = 16;

    let dir = tempdir().unwrap();
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let store = Arc::new(CheckpointDecisionStore::local_single_writer(object_store));
    record_local_commits(store.as_ref(), LAST_EPOCH).await;

    let mut tasks = tokio::task::JoinSet::new();
    for before in 2..=LAST_EPOCH {
        let store = Arc::clone(&store);
        tasks.spawn(async move { (before, store.prune_outcomes_before(before).await) });
    }
    while let Some(result) = tasks.join_next().await {
        let (requested, horizon) = result.unwrap();
        assert!(
            horizon.unwrap() >= requested,
            "a serialized local winner may supersede but never regress a request"
        );
    }
    assert_eq!(store.outcome_gc_floor_horizon().await.unwrap(), LAST_EPOCH);
    assert_eq!(
        store
            .outcomes()
            .await
            .unwrap()
            .into_iter()
            .map(|outcome| outcome.epoch)
            .collect::<Vec<_>>(),
        vec![LAST_EPOCH]
    );
}

#[tokio::test]
async fn local_floor_rmw_is_ordered_across_store_instances() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(DeferredPutStore::new(inner));
    let object_store: Arc<dyn ObjectStore> = fault.clone();
    let first_store = Arc::new(CheckpointDecisionStore::local_single_writer(Arc::clone(
        &object_store,
    )));
    let second_store = Arc::new(CheckpointDecisionStore::local_single_writer(object_store));
    assert!(Arc::ptr_eq(
        first_store.local_metadata_rmw_lock.as_ref().unwrap(),
        second_store.local_metadata_rmw_lock.as_ref().unwrap()
    ));
    record_local_commits(first_store.as_ref(), 5).await;
    assert_eq!(first_store.prune_outcomes_before(2).await.unwrap(), 2);

    fault.reject_conditional_updates();
    let deployment_id = first_store.load_or_create_deployment_id().await.unwrap();
    fault.block_next_overwrite(CheckpointDecisionStore::outcome_gc_floor_path(
        &deployment_id,
    ));
    let advancing_store = Arc::clone(&first_store);
    let advance = tokio::spawn(async move { advancing_store.prune_outcomes_before(3).await });
    fault.wait_for_blocked_overwrite().await;
    assert!(
        second_store
            .local_metadata_rmw_lock
            .as_ref()
            .unwrap()
            .try_lock()
            .is_err(),
        "a second store instance entered a local floor RMW transition"
    );
    fault.release_blocked_overwrite();
    assert!(advance.await.unwrap().unwrap() >= 3);

    assert_eq!(second_store.prune_outcomes_before(4).await.unwrap(), 4);
    assert_eq!(first_store.outcome_gc_floor_horizon().await.unwrap(), 4);
}

#[tokio::test]
async fn shared_local_filesystem_requires_native_conditional_floor_updates() {
    let dir = tempdir().unwrap();
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let store = CheckpointDecisionStore::new(object_store);
    record_local_commits(&store, 4).await;

    assert_eq!(store.prune_outcomes_before(2).await.unwrap(), 2);
    let error = store.prune_outcomes_before(3).await.unwrap_err();
    assert!(error.to_string().contains("PutMode::Update"), "{error}");
    assert_eq!(store.outcome_gc_floor_horizon().await.unwrap(), 2);
    assert_eq!(
        store
            .outcomes()
            .await
            .unwrap()
            .into_iter()
            .map(|outcome| outcome.epoch)
            .collect::<Vec<_>>(),
        vec![2, 3, 4],
        "a failed shared update must not delete outcomes beyond the durable floor"
    );
}

#[tokio::test]
async fn outcome_floor_object_count_is_bounded_across_many_horizons() {
    const LAST_EPOCH: u64 = 64;

    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let deployment_id = store.load_or_create_deployment_id().await.unwrap();
    for epoch in 1..=LAST_EPOCH {
        store
            .record_outcome(
                epoch,
                epoch,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                None,
            )
            .await
            .unwrap();
    }
    for before in 2..=LAST_EPOCH {
        assert_eq!(store.prune_outcomes_before(before).await.unwrap(), before);
    }

    let mut entries = object_store.list(Some(&OsPath::from("checkpoint-outcome-gc/")));
    let mut locations = Vec::new();
    while let Some(entry) = entries.next().await {
        locations.push(entry.unwrap().location);
    }
    assert_eq!(
        locations,
        vec![CheckpointDecisionStore::outcome_gc_floor_path(
            &deployment_id
        )],
        "retention must overwrite one canonical floor instead of leaking horizon history"
    );
}

#[tokio::test]
async fn outcome_retention_boundary_preserves_commit_cursor_and_closed_epoch() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    for (epoch, checkpoint_id, verdict) in [
        (1, 1, CheckpointVerdict::Commit),
        (2, 2, CheckpointVerdict::Abort),
        (3, 3, CheckpointVerdict::Commit),
    ] {
        store
            .record_outcome(
                epoch,
                checkpoint_id,
                CheckpointScope::Local,
                None,
                None,
                verdict,
                None,
            )
            .await
            .unwrap();
    }
    assert_eq!(store.prune_outcomes_before(3).await.unwrap(), 3);
    drop(store);

    let restarted = CheckpointDecisionStore::new(object_store);
    assert_eq!(
        restarted.outcome_retention_boundary().await.unwrap(),
        OutcomeRetentionBoundary {
            before_epoch: 3,
            committed_checkpoint_id: Some(1),
            highest_closed_epoch: Some(2),
        }
    );
    assert_eq!(
        restarted
            .outcomes()
            .await
            .unwrap()
            .into_iter()
            .map(|outcome| (outcome.epoch, outcome.checkpoint_id))
            .collect::<Vec<_>>(),
        vec![(3, 3)]
    );
}

#[tokio::test]
async fn outcome_inventory_rejects_noncanonical_attempt_identity() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let forged = CheckpointOutcome {
        version: CHECKPOINT_OUTCOME_VERSION,
        scope: CheckpointScope::Local,
        epoch: 9,
        checkpoint_id: 79,
        deployment_id: store.load_or_create_deployment_id().await.unwrap(),
        assignment_fence: None,
        leader_proof: None,
        recovery_capsule: None,
        verdict: CheckpointVerdict::Abort,
    };
    object_store
        .put(
            &CheckpointDecisionStore::outcome_path(9),
            PutPayload::from(Bytes::from(serde_json::to_vec(&forged).unwrap())),
        )
        .await
        .unwrap();

    let error = store.outcomes().await.unwrap_err();
    assert!(matches!(error, DecisionError::Conflict(_)));
    assert!(error.to_string().contains("non-canonical checkpoint ID 79"));
}

#[tokio::test]
async fn outcome_prune_cannot_remove_last_live_commit() {
    let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
    store
        .record_outcome(
            4,
            4,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            None,
        )
        .await
        .unwrap();
    store
        .record_outcome(
            5,
            5,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();

    let error = store.prune_outcomes_before(5).await.unwrap_err();
    assert!(matches!(error, DecisionError::Conflict(_)));
    assert!(error.to_string().contains("no live commit recovery cut"));
    assert_eq!(store.outcome_gc_floor_horizon().await.unwrap(), 0);
    let outcomes = store.outcomes().await.unwrap();
    assert_eq!(highest_commit_epoch(&outcomes), Some(4));
}

#[tokio::test]
async fn deployment_identity_is_create_once_across_store_instances() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let second = CheckpointDecisionStore::new(object_store);

    let first_id = first.load_or_create_deployment_id().await.unwrap();
    let second_id = second.load_or_create_deployment_id().await.unwrap();

    assert_eq!(first_id, second_id);
    assert!(!uuid::Uuid::parse_str(&first_id).unwrap().is_nil());
}

#[tokio::test]
async fn independent_decision_stores_get_distinct_deployment_identities() {
    let first = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
    let second = CheckpointDecisionStore::new(Arc::new(InMemory::new()));

    assert_ne!(
        first.load_or_create_deployment_id().await.unwrap(),
        second.load_or_create_deployment_id().await.unwrap()
    );
}

#[tokio::test]
async fn sink_open_witness_roundtrips_across_restart_and_clears_exactly() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let witness = store
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(7),
            vec!["sink-a".into(), "sink-b".into()],
        )
        .await
        .unwrap();
    assert_eq!(
        store.sink_open_witness().await.unwrap(),
        Some(witness.clone())
    );
    drop(store);

    let restarted = CheckpointDecisionStore::new(object_store);
    assert_eq!(
        restarted.sink_open_witness().await.unwrap(),
        Some(witness.clone())
    );
    restarted.clear_sink_open_witness(&witness).await.unwrap();
    assert_eq!(restarted.sink_open_witness().await.unwrap(), None);
    let tombstone = restarted
        .read_sink_open_witness_record()
        .await
        .unwrap()
        .expect("closed slot remains durable");
    assert!(matches!(
        tombstone.slot.state,
        CheckpointSinkOpenWitnessSlotState::Closed {
            witness: ref closed,
            ..
        } if closed == &witness
    ));
    restarted.clear_sink_open_witness(&witness).await.unwrap();
}

#[tokio::test]
async fn sink_open_witness_rejects_malformed_body_and_input() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let deployment_id = store.load_or_create_deployment_id().await.unwrap();
    let witness = test_sink_open_witness(deployment_id, 8, 8);
    let mut body = serde_json::to_value(CheckpointSinkOpenWitnessSlot::open(witness)).unwrap();
    body.as_object_mut()
        .unwrap()
        .insert("unexpected".into(), serde_json::Value::Bool(true));
    object_store
        .put(
            &CheckpointDecisionStore::sink_open_witness_path(),
            PutPayload::from(Bytes::from(serde_json::to_vec(&body).unwrap())),
        )
        .await
        .unwrap();

    let error = store.sink_open_witness().await.unwrap_err();
    assert!(error.to_string().contains("unknown field"), "{error}");

    let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
    let error = store
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(9),
            vec!["sink-b".into(), "sink-a".into()],
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("strictly sorted"), "{error}");
}

#[tokio::test]
async fn sink_open_witness_rejects_oversized_body_before_create() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let error = store
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(10),
            vec!["x".repeat(CHECKPOINT_SINK_OPEN_WITNESS_MAX_BYTES as usize)],
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("maximum is"), "{error}");
    assert!(object_store
        .head(&CheckpointDecisionStore::sink_open_witness_path())
        .await
        .is_err());
}

#[tokio::test]
async fn sink_open_witness_rejects_foreign_deployment() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let current = store.load_or_create_deployment_id().await.unwrap();
    let foreign = test_sink_open_witness(uuid::Uuid::from_u128(0x1234).to_string(), 11, 0x5678);
    let foreign_slot = CheckpointSinkOpenWitnessSlot::closed(foreign.clone());
    object_store
        .put(
            &CheckpointDecisionStore::sink_open_witness_path(),
            PutPayload::from(Bytes::from(serde_json::to_vec(&foreign_slot).unwrap())),
        )
        .await
        .unwrap();

    let error = store.sink_open_witness().await.unwrap_err();
    assert!(
        error.to_string().contains(&foreign.deployment_id),
        "{error}"
    );
    assert!(error.to_string().contains(&current), "{error}");
}

#[tokio::test]
async fn sink_open_witness_singleton_linearizes_concurrent_attempts() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let second = CheckpointDecisionStore::new(object_store);
    let (left, right) = tokio::join!(
        first.create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(12),
            vec!["sink-a".into()],
        ),
        second.create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(13),
            vec!["sink-a".into()],
        )
    );
    assert_ne!(left.is_ok(), right.is_ok());
    let winner = left.or(right).unwrap();
    assert_eq!(first.sink_open_witness().await.unwrap(), Some(winner));
}

#[tokio::test]
async fn sink_open_witness_does_not_adopt_another_proposals_token() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let second = CheckpointDecisionStore::new(object_store);
    let winner = first
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(14),
            vec!["sink-a".into()],
        )
        .await
        .unwrap();

    let error = second
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(14),
            vec!["sink-a".into()],
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("conflicting checkpoint 14"),
        "{error}"
    );
    assert_eq!(first.sink_open_witness().await.unwrap(), Some(winner));
}

#[tokio::test]
async fn sink_open_witness_reconciles_lost_create_response_by_token() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
    let object_store: Arc<dyn ObjectStore> = fault.clone();
    let store = CheckpointDecisionStore::new(object_store);
    store.load_or_create_deployment_id().await.unwrap();
    let attempt = CheckpointAttempt::canonical(14);
    fault.intercept_after_apply(CheckpointDecisionStore::sink_open_witness_path());

    let witness = store
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            attempt,
            vec!["sink-a".into(), "sink-b".into()],
        )
        .await
        .unwrap();
    assert_eq!(witness.attempt, attempt);
    assert_eq!(store.sink_open_witness().await.unwrap(), Some(witness));
}

#[tokio::test]
async fn sink_open_witness_reconciles_lost_close_response_by_tombstone() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let attempt = CheckpointAttempt::canonical(15);
    let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
    let object_store: Arc<dyn ObjectStore> = fault.clone();
    let store = CheckpointDecisionStore::new(object_store);
    let witness = store
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            attempt,
            vec!["sink-a".into(), "sink-b".into()],
        )
        .await
        .unwrap();

    fault.intercept_after_apply(CheckpointDecisionStore::sink_open_witness_path());
    store.clear_sink_open_witness(&witness).await.unwrap();
    assert_eq!(store.sink_open_witness().await.unwrap(), None);
    assert!(inner
        .head(&CheckpointDecisionStore::sink_open_witness_path())
        .await
        .is_ok());
    let tombstone = store
        .read_sink_open_witness_record()
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        tombstone.slot.state,
        CheckpointSinkOpenWitnessSlotState::Closed {
            witness: ref closed,
            ..
        } if closed == &witness
    ));
}

#[tokio::test]
async fn stale_sink_open_witness_close_cannot_erase_a_successor() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
    let object_store: Arc<dyn ObjectStore> = fault.clone();
    let stale = CheckpointDecisionStore::new(object_store);
    let first = stale
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(16),
            vec!["sink-a".into()],
        )
        .await
        .unwrap();

    fault.intercept(CheckpointDecisionStore::sink_open_witness_path());
    let error = stale.clear_sink_open_witness(&first).await.unwrap_err();
    assert!(matches!(&error, DecisionError::Io(_)), "{error}");

    let current = CheckpointDecisionStore::new(Arc::clone(&inner));
    current.clear_sink_open_witness(&first).await.unwrap();
    let successor = current
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(17),
            vec!["sink-a".into()],
        )
        .await
        .unwrap();

    assert!(
        fault.apply_pending().await.is_err(),
        "the stale close must fail its old object-version precondition"
    );
    assert_eq!(current.sink_open_witness().await.unwrap(), Some(successor));
}

#[tokio::test]
async fn closed_sink_open_witness_slot_linearizes_successor_open() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let owner = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let first = owner
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(18),
            vec!["sink-a".into()],
        )
        .await
        .unwrap();
    owner.clear_sink_open_witness(&first).await.unwrap();

    let left_store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let right_store = CheckpointDecisionStore::new(object_store);
    let (left, right) = tokio::join!(
        left_store.create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(19),
            vec!["sink-a".into()],
        ),
        right_store.create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(20),
            vec!["sink-a".into()],
        )
    );
    assert_ne!(left.is_ok(), right.is_ok());
    let winner = left.or(right).unwrap();
    assert_eq!(owner.sink_open_witness().await.unwrap(), Some(winner));
}

#[tokio::test]
async fn local_single_writer_reuses_closed_sink_open_witness_slot() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::local_single_writer(object_store);
    let first = store
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(21),
            vec!["sink-a".into()],
        )
        .await
        .unwrap();
    store.clear_sink_open_witness(&first).await.unwrap();
    let error = store
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(21),
            vec!["sink-a".into()],
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("does not advance"), "{error}");
    let successor = store
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(22),
            vec!["sink-a".into()],
        )
        .await
        .unwrap();
    assert_eq!(store.sink_open_witness().await.unwrap(), Some(successor));
}

#[tokio::test]
async fn local_sink_witness_rmw_is_ordered_across_store_instances() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(DeferredPutStore::new(inner));
    let object_store: Arc<dyn ObjectStore> = fault.clone();
    let first_store = Arc::new(CheckpointDecisionStore::local_single_writer(Arc::clone(
        &object_store,
    )));
    let second_store = Arc::new(CheckpointDecisionStore::local_single_writer(object_store));
    assert!(Arc::ptr_eq(
        first_store.local_metadata_rmw_lock.as_ref().unwrap(),
        second_store.local_metadata_rmw_lock.as_ref().unwrap()
    ));

    let first = first_store
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(23),
            vec!["sink-a".into()],
        )
        .await
        .unwrap();
    fault.block_next_overwrite(CheckpointDecisionStore::sink_open_witness_path());
    let closing_store = Arc::clone(&first_store);
    let closing_witness = first.clone();
    let close = tokio::spawn(async move {
        closing_store
            .clear_sink_open_witness(&closing_witness)
            .await
    });
    fault.wait_for_blocked_overwrite().await;
    assert!(
        second_store
            .local_metadata_rmw_lock
            .as_ref()
            .unwrap()
            .try_lock()
            .is_err(),
        "a second store instance entered a local witness RMW transition"
    );
    fault.release_blocked_overwrite();
    close.await.unwrap().unwrap();

    second_store.clear_sink_open_witness(&first).await.unwrap();
    let successor = second_store
        .create_sink_open_witness(
            PipelineIdentity::empty(),
            0,
            CheckpointAttempt::canonical(24),
            vec!["sink-a".into()],
        )
        .await
        .unwrap();
    assert_eq!(
        first_store.sink_open_witness().await.unwrap(),
        Some(successor)
    );
}

#[tokio::test]
async fn legacy_deployment_identity_is_rejected() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    object_store
        .put(
            &CheckpointDecisionStore::deployment_identity_path(),
            PutPayload::from_bytes(Bytes::from_static(
                br#"{"version":1,"id":"018f0000-0000-7000-8000-000000000001","allocator_mode":"native_cas","checkpoint_id":0,"allocation_id":"018f0000-0000-7000-8000-000000000002"}"#,
            )),
        )
        .await
        .unwrap();

    let store = CheckpointDecisionStore::new(object_store);
    let error = store.load_or_create_deployment_id().await.unwrap_err();
    assert!(
        error.to_string().contains("version 1 is unsupported"),
        "{error}"
    );
}

#[tokio::test]
async fn allocator_protocol_cannot_change_inside_a_deployment_namespace() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shared = CheckpointDecisionStore::new(Arc::clone(&object_store));
    assert_eq!(shared.allocate_checkpoint_id().await.unwrap(), 1);
    drop(shared);

    let local = CheckpointDecisionStore::local_single_writer(object_store);
    let error = local.allocate_checkpoint_id().await.unwrap_err();
    assert!(error.to_string().contains("allocator mode"), "{error}");
    assert!(error.to_string().contains("NativeCas"), "{error}");
    assert!(error.to_string().contains("LocalSingleWriter"), "{error}");
}

#[tokio::test]
async fn local_allocator_protocol_cannot_be_reopened_as_shared_cas() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let local = CheckpointDecisionStore::local_single_writer(Arc::clone(&object_store));
    assert_eq!(local.allocate_checkpoint_id().await.unwrap(), 1);
    drop(local);

    let shared = CheckpointDecisionStore::new(object_store);
    let error = shared.allocate_checkpoint_id().await.unwrap_err();
    assert!(error.to_string().contains("allocator mode"), "{error}");
    assert!(error.to_string().contains("LocalSingleWriter"), "{error}");
    assert!(error.to_string().contains("NativeCas"), "{error}");
}

#[tokio::test]
async fn checkpoint_ids_start_at_one_and_increase() {
    let dir = tempdir().unwrap();
    let s = store_in(dir.path());

    assert_eq!(s.allocate_checkpoint_id().await.unwrap(), 1);
    assert_eq!(s.allocate_checkpoint_id().await.unwrap(), 2);
    assert_eq!(s.allocate_checkpoint_id_at_least(10).await.unwrap(), 10);
    assert_eq!(s.allocate_checkpoint_id().await.unwrap(), 11);
    drop(s);

    let restarted = store_in(dir.path());
    assert_eq!(
        restarted.allocate_checkpoint_id().await.unwrap(),
        LOCAL_RESERVATION_BLOCK_SIZE + 1,
        "restart must burn the unconsumed tail of the prior durable block"
    );
    assert!(restarted.allocate_checkpoint_id_at_least(0).await.is_err());
}

#[tokio::test]
async fn local_checkpoint_allocation_uses_one_durable_object_per_large_block() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::local_single_writer(Arc::clone(&object_store));

    for expected in 1..=4_096 {
        assert_eq!(store.allocate_checkpoint_id().await.unwrap(), expected);
    }

    let mut locations = object_store
        .list(None)
        .map_ok(|meta| meta.location.to_string())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    locations.sort_unstable();
    assert_eq!(
        locations,
        vec![
            "checkpoint-deployment/identity.json".to_owned(),
            "checkpoint-id-blocks/block=00000000000000000000".to_owned(),
        ]
    );
}

#[tokio::test]
async fn local_checkpoint_floor_jump_claims_a_nonoverlapping_block() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::local_single_writer(object_store);

    assert_eq!(store.allocate_checkpoint_id().await.unwrap(), 1);
    let minimum = LOCAL_RESERVATION_BLOCK_SIZE * 3 + 17;
    assert_eq!(
        store
            .allocate_checkpoint_id_at_least(minimum)
            .await
            .unwrap(),
        minimum
    );
    assert_eq!(store.allocate_checkpoint_id().await.unwrap(), minimum + 1);
}

#[tokio::test]
async fn stale_shared_counter_cache_cannot_allocate_below_a_floor_jump() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stale = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let jumper = CheckpointDecisionStore::new(object_store);

    assert_eq!(stale.allocate_checkpoint_id().await.unwrap(), 1);
    assert_eq!(
        jumper.allocate_checkpoint_id_at_least(100).await.unwrap(),
        100
    );
    assert_eq!(
        stale.allocate_checkpoint_id().await.unwrap(),
        101,
        "a stale cached ETag must reload the durable winner before returning"
    );
}

#[tokio::test]
async fn shared_counter_object_count_is_bounded() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));

    for expected in 1..=256 {
        assert_eq!(store.allocate_checkpoint_id().await.unwrap(), expected);
    }

    let mut locations = object_store
        .list(None)
        .map_ok(|meta| meta.location.to_string())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    locations.sort_unstable();
    assert_eq!(
        locations,
        vec!["checkpoint-deployment/identity.json".to_owned()]
    );
}

#[tokio::test]
async fn shared_counter_authority_deletion_changes_deployment_before_id_reuse() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let old_deployment = store.load_or_create_deployment_id().await.unwrap();
    assert_eq!(store.allocate_checkpoint_id().await.unwrap(), 1);

    object_store
        .delete(&CheckpointDecisionStore::deployment_identity_path())
        .await
        .unwrap();
    let error = store.allocate_checkpoint_id().await.unwrap_err();
    assert!(error.to_string().contains("head disappeared"), "{error}");
    drop(store);

    let restarted = CheckpointDecisionStore::new(object_store);
    let new_deployment = restarted.load_or_create_deployment_id().await.unwrap();
    assert_ne!(new_deployment, old_deployment);
    assert_eq!(restarted.allocate_checkpoint_id().await.unwrap(), 1);
}

#[tokio::test]
async fn missing_shared_authority_is_not_recreated_from_a_cached_deployment_id() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
    let deployment = store.load_or_create_deployment_id().await.unwrap();
    store.cache_checkpoint_id_head(None);
    object_store
        .delete(&CheckpointDecisionStore::deployment_identity_path())
        .await
        .unwrap();

    let error = store.allocate_checkpoint_id().await.unwrap_err();
    assert!(error.to_string().contains(&deployment), "{error}");
    assert!(error.to_string().contains("disappeared"), "{error}");
    assert!(object_store
        .head(&CheckpointDecisionStore::deployment_identity_path())
        .await
        .is_err());
}

#[tokio::test]
async fn shared_counter_reconciles_a_lost_success_response_by_token() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
    let object_store: Arc<dyn ObjectStore> = fault.clone();
    let store = CheckpointDecisionStore::new(object_store);
    store.load_or_create_deployment_id().await.unwrap();
    fault.intercept_after_apply(CheckpointDecisionStore::deployment_identity_path());

    assert_eq!(store.allocate_checkpoint_id().await.unwrap(), 1);
    assert_eq!(store.allocate_checkpoint_id().await.unwrap(), 2);
}

#[tokio::test]
async fn uncertain_shared_counter_write_is_burned_after_late_visibility() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
    let object_store: Arc<dyn ObjectStore> = fault.clone();
    let store = CheckpointDecisionStore::new(object_store);
    store.load_or_create_deployment_id().await.unwrap();
    fault.intercept(CheckpointDecisionStore::deployment_identity_path());

    let error = store.allocate_checkpoint_id().await.unwrap_err();
    assert!(matches!(error, DecisionError::Io(_)));
    fault.apply_pending().await.unwrap();
    drop(store);

    let restarted = CheckpointDecisionStore::new(inner);
    assert_eq!(
        restarted.allocate_checkpoint_id().await.unwrap(),
        2,
        "the uncertain ID must never be returned again after it becomes visible"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_checkpoint_id_allocations_are_unique_and_monotonic() {
    const ALLOCATIONS: u64 = 64;

    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..ALLOCATIONS {
        let object_store = Arc::clone(&object_store);
        tasks.spawn(async move {
            CheckpointDecisionStore::new(object_store)
                .allocate_checkpoint_id()
                .await
                .unwrap()
        });
    }

    let mut allocated = Vec::with_capacity(usize::try_from(ALLOCATIONS).unwrap());
    while let Some(result) = tasks.join_next().await {
        allocated.push(result.unwrap());
    }
    allocated.sort_unstable();

    assert_eq!(allocated, (1..=ALLOCATIONS).collect::<Vec<_>>());
    assert!(allocated.iter().all(|id| *id != 0));
}
