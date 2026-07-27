use super::*;
use futures::StreamExt as _;

/// Records retention publication and deletion order while delegating storage to the real
/// local backend.
struct RetentionLogStore {
    inner: Arc<dyn ObjectStore>,
    operations: Arc<parking_lot::Mutex<Vec<String>>>,
    delete_calls: Arc<AtomicU64>,
    fail_delete_call: u64,
}

impl std::fmt::Debug for RetentionLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetentionLogStore").finish_non_exhaustive()
    }
}

impl std::fmt::Display for RetentionLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RetentionLogStore")
    }
}

#[async_trait]
impl ObjectStore for RetentionLogStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if location == &ObjectStoreBackend::prune_floor_path() {
            self.operations
                .lock()
                .push(format!("floor:{:?}", opts.mode));
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
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
        let inner = Arc::clone(&self.inner);
        let operations = Arc::clone(&self.operations);
        let delete_calls = Arc::clone(&self.delete_calls);
        let fail_delete_call = self.fail_delete_call;
        locations
            .then(move |location| {
                let inner = Arc::clone(&inner);
                let operations = Arc::clone(&operations);
                let delete_calls = Arc::clone(&delete_calls);
                async move {
                    let location = location?;
                    operations.lock().push(format!("delete:{location}"));
                    let delete_call = delete_calls.fetch_add(1, Ordering::AcqRel) + 1;
                    if delete_call == fail_delete_call {
                        return Err(object_store::Error::Generic {
                            store: "retention-test",
                            source: Box::new(std::io::Error::other("injected delete failure")),
                        });
                    }
                    inner.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&OsPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.operations.lock().push(format!(
            "list:{}",
            prefix.map_or("<root>", |path| path.as_ref())
        ));
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&OsPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.operations.lock().push(format!(
            "delimiter:{}",
            prefix.map_or("<root>", |path| path.as_ref())
        ));
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

/// Pauses the first create of one seal immediately before it reaches shared storage.
struct SealPublishGateStore {
    inner: Arc<dyn ObjectStore>,
    seal_path: OsPath,
    gated: std::sync::atomic::AtomicBool,
    reached: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

impl std::fmt::Debug for SealPublishGateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealPublishGateStore")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for SealPublishGateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SealPublishGateStore")
    }
}

#[async_trait]
impl ObjectStore for SealPublishGateStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if location == &self.seal_path
            && matches!(&opts.mode, PutMode::Create)
            && self
                .gated
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.reached.add_permits(1);
            self.release
                .acquire()
                .await
                .expect("test gate remains open")
                .forget();
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
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

/// Reports invalid metadata for one full GET and records whether its body is polled.
struct FullGetMetadataProbeStore {
    inner: Arc<dyn ObjectStore>,
    target: OsPath,
    body_polls: Arc<AtomicU64>,
}

impl std::fmt::Debug for FullGetMetadataProbeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FullGetMetadataProbeStore")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for FullGetMetadataProbeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FullGetMetadataProbeStore")
    }
}

#[async_trait]
impl ObjectStore for FullGetMetadataProbeStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &OsPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let probe_body = location == &self.target && !options.head && options.range.is_none();
        let mut result = self.inner.get_opts(location, options).await?;
        if probe_body {
            result.meta.size = result.meta.size.saturating_add(1);
            let body_polls = Arc::clone(&self.body_polls);
            result.payload = object_store::GetResultPayload::Stream(
                futures::stream::once(async move {
                    body_polls.fetch_add(1, Ordering::AcqRel);
                    Ok(Bytes::new())
                })
                .boxed(),
            );
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

fn attempt(checkpoint_id: u64) -> CheckpointAttempt {
    CheckpointAttempt::canonical(checkpoint_id)
}

fn assignment_fence(version: u64, vnode_count: usize) -> CheckpointAssignmentFence {
    CheckpointAssignmentFence::from_owner_map(
        version,
        &vec![1; vnode_count],
        vec![crate::checkpoint::CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(1),
        }],
    )
    .unwrap()
}

fn leader_proof(node_id: u64, boot_id: uuid::Uuid, token: u64) -> LeaderProof {
    LeaderProof {
        owner: LeaderProofOwner {
            node_id,
            boot_id,
            process_term: token,
        },
        fencing_token: token,
    }
}
use object_store::local::LocalFileSystem;
use tempfile::tempdir;

fn make_store(dir: &std::path::Path) -> Arc<dyn ObjectStore> {
    Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap())
}

fn make_durable_store(dir: &std::path::Path) -> Arc<dyn ObjectStore> {
    Arc::new(crate::durable_local_store::DurableLocalObjectStore::new(dir).unwrap())
}

fn pipeline_identity(byte: u8) -> PipelineIdentity {
    PipelineIdentity {
        canonical_version: crate::checkpoint::PIPELINE_IDENTITY_VERSION,
        sha256: format!("{byte:02x}").repeat(32),
    }
}

#[tokio::test]
async fn namespace_binding_is_atomic_and_idempotent() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let deployment = uuid::Uuid::from_u128(1).to_string();
    let identity = pipeline_identity(0x11);
    let first = ObjectStoreBackend::node_durable(Arc::clone(&store), "node-0", 1);
    first
        .bind_state_namespace(&deployment, &identity)
        .await
        .unwrap();

    let restarted = ObjectStoreBackend::node_durable(store, "node-0", 1);
    restarted
        .bind_state_namespace(&deployment, &identity)
        .await
        .unwrap();
}

#[tokio::test]
async fn concurrent_first_namespace_binders_accept_the_same_identity() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let deployment = uuid::Uuid::from_u128(1).to_string();
    let identity = pipeline_identity(0x11);
    let first = ObjectStoreBackend::node_durable(Arc::clone(&store), "node-0", 1);
    let second = ObjectStoreBackend::node_durable(store, "node-1", 1);

    let (first_result, second_result) = tokio::join!(
        first.bind_state_namespace(&deployment, &identity),
        second.bind_state_namespace(&deployment, &identity),
    );

    first_result.unwrap();
    second_result.unwrap();
}

#[tokio::test]
async fn namespace_binding_rejects_an_unbound_nonempty_state_root() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let legacy_path = OsPath::from("state-v2/epoch=1/checkpoint=1/vnode=0/partial.bin");
    store
        .put(
            &legacy_path,
            PutPayload::from(Bytes::from_static(b"legacy")),
        )
        .await
        .unwrap();
    let backend = ObjectStoreBackend::node_durable(Arc::clone(&store), "node-0", 1);

    let error = backend
        .bind_state_namespace(
            &uuid::Uuid::from_u128(1).to_string(),
            &pipeline_identity(0x11),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("contains unbound artifact"));
    assert!(matches!(
        store.get(&ObjectStoreBackend::namespace_path()).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn namespace_binding_rejects_deployment_mismatch() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreBackend::node_durable(store, "node-0", 1);
    let identity = pipeline_identity(0x11);
    backend
        .bind_state_namespace(&uuid::Uuid::from_u128(1).to_string(), &identity)
        .await
        .unwrap();

    let error = backend
        .bind_state_namespace(&uuid::Uuid::from_u128(2).to_string(), &identity)
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("belongs to deployment"));
}

#[tokio::test]
async fn namespace_binding_rejects_pipeline_mismatch() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreBackend::node_durable(store, "node-0", 1);
    let deployment = uuid::Uuid::from_u128(1).to_string();
    backend
        .bind_state_namespace(&deployment, &pipeline_identity(0x11))
        .await
        .unwrap();

    let error = backend
        .bind_state_namespace(&deployment, &pipeline_identity(0x22))
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("pipeline identity"));
}

#[tokio::test]
async fn namespace_binding_rejects_malformed_and_oversized_markers() {
    for (poison, expected) in [
        (Bytes::from_static(b"{"), "malformed"),
        (
            Bytes::from(vec![b'x'; STATE_NAMESPACE_MAX_BYTES as usize + 1]),
            "expected 1..=",
        ),
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        store
            .put(
                &ObjectStoreBackend::namespace_path(),
                PutPayload::from(poison),
            )
            .await
            .unwrap();
        let backend = ObjectStoreBackend::node_durable(store, "node-0", 1);
        let error = backend
            .bind_state_namespace(
                &uuid::Uuid::from_u128(1).to_string(),
                &pipeline_identity(0x11),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[tokio::test]
async fn write_read_roundtrip() {
    let dir = tempdir().unwrap();
    let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
    backend
        .write_partial(attempt(1), 0, 0, Bytes::from_static(b"hello"))
        .await
        .unwrap();
    let got = backend.read_partial(attempt(1), 0).await.unwrap().unwrap();
    assert_eq!(&got[..], b"hello");
}

#[tokio::test]
async fn sealed_partial_read_rejects_a_valid_replacement_envelope() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-1", 1);
    let checkpoint = attempt(1);
    let fence = assignment_fence(7, 1);
    backend.set_authoritative_version(7);
    backend
        .write_certified_partial(checkpoint, 0, &fence, 1, Bytes::from_static(b"state"))
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(checkpoint, Some(&fence), &[0], &[])
        .await
        .unwrap());
    let inventory = backend
        .checkpoint_seal_inventory(checkpoint)
        .await
        .unwrap()
        .unwrap();
    let sealed = &inventory.sealed_partials[0];

    assert_eq!(
        backend
            .read_sealed_partial_bounded(checkpoint, sealed, 5)
            .await
            .unwrap(),
        Some(Bytes::from_static(b"state"))
    );
    let error = backend
        .read_sealed_partial_bounded(checkpoint, sealed, 4)
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("read bound is 4"));

    let replacement = ObjectStoreBackend::encode_partial(
        checkpoint,
        0,
        fence.assignment_version,
        None,
        &Bytes::from_static(b"state"),
    );
    store
        .put(
            &ObjectStoreBackend::partial_path(checkpoint, 0),
            PutPayload::from(replacement),
        )
        .await
        .unwrap();
    assert_eq!(
        backend.read_partial(checkpoint, 0).await.unwrap(),
        Some(Bytes::from_static(b"state")),
        "the replacement must be a self-consistent partial envelope"
    );

    let error = backend
        .read_sealed_partial_bounded(checkpoint, sealed, 5)
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error
        .to_string()
        .contains("does not match the checkpoint seal"));
}

#[tokio::test]
async fn sealed_partial_read_rejects_bad_metadata_before_polling_body() {
    let checkpoint = attempt(1);
    let target = ObjectStoreBackend::partial_path(checkpoint, 0);
    let body_polls = Arc::new(AtomicU64::new(0));
    let store: Arc<dyn ObjectStore> = Arc::new(FullGetMetadataProbeStore {
        inner: Arc::new(object_store::memory::InMemory::new()),
        target,
        body_polls: Arc::clone(&body_polls),
    });
    let backend = ObjectStoreBackend::new(store, "node-0", 1);
    backend
        .write_partial(checkpoint, 0, 0, Bytes::from_static(b"state"))
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(checkpoint, None, &[0], &[])
        .await
        .unwrap());
    let inventory = backend
        .checkpoint_seal_inventory(checkpoint)
        .await
        .unwrap()
        .unwrap();

    let error = backend
        .read_sealed_partial_bounded(checkpoint, &inventory.sealed_partials[0], 5)
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("including its header"));
    assert_eq!(body_polls.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn noncanonical_attempt_is_rejected_before_object_creation() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-0", 4);
    let invalid = CheckpointAttempt::new(1, 2);

    let error = backend
        .write_partial(invalid, 0, 0, Bytes::from_static(b"invalid"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("canonical checkpoint ID"));
    assert!(matches!(
        store
            .head(&ObjectStoreBackend::partial_path(invalid, 0))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert!(backend.read_partial(invalid, 0).await.is_err());
}

#[test]
fn decode_partial_realigns_an_unaligned_transport_buffer() {
    const ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;

    let checkpoint = attempt(1);
    let payload = Bytes::from_static(b"archived vnode state");
    let encoded = ObjectStoreBackend::encode_partial(checkpoint, 0, 0, None, &payload);

    let mut transport = bytes::BytesMut::zeroed(encoded.len() + ARCHIVE_ALIGNMENT);
    let offset = (0..ARCHIVE_ALIGNMENT)
        .find(|offset| {
            !(transport.as_ptr() as usize + offset + VNODE_PARTIAL_HEADER_LEN)
                .is_multiple_of(ARCHIVE_ALIGNMENT)
        })
        .expect("an unaligned offset exists");
    transport[offset..offset + encoded.len()].copy_from_slice(&encoded);
    let transport = transport.freeze().slice(offset..offset + encoded.len());
    assert!(!(transport[VNODE_PARTIAL_HEADER_LEN..].as_ptr() as usize)
        .is_multiple_of(ARCHIVE_ALIGNMENT));

    let decoded = ObjectStoreBackend::decode_partial(&transport, checkpoint, 0).unwrap();
    assert_eq!(decoded, payload);
    assert!((decoded.as_ptr() as usize).is_multiple_of(ARCHIVE_ALIGNMENT));
}

#[tokio::test]
async fn immutable_artifact_accepts_identical_retry_and_rejects_conflict() {
    let dir = tempdir().unwrap();
    let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
    let checkpoint = attempt(1);
    backend
        .write_partial(checkpoint, 0, 0, Bytes::from_static(b"first"))
        .await
        .unwrap();
    backend
        .write_partial(checkpoint, 0, 0, Bytes::from_static(b"first"))
        .await
        .unwrap();
    assert!(matches!(
        backend
            .write_partial(checkpoint, 0, 0, Bytes::from_static(b"different"))
            .await,
        Err(StateBackendError::Conflict { .. })
    ));
    assert_eq!(
        backend.read_partial(checkpoint, 0).await.unwrap().unwrap(),
        Bytes::from_static(b"first")
    );
}

#[tokio::test]
async fn immutable_retry_rejects_size_poison_from_metadata() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let path = OsPath::from("immutable-poison");
    store
        .put(
            &path,
            PutPayload::from(Bytes::from_static(b"oversized-poison")),
        )
        .await
        .unwrap();
    let backend = ObjectStoreBackend::new(store, "node-0", 1);

    let error = backend
        .put_immutable(&path, Bytes::from_static(b"retry"))
        .await
        .unwrap_err();

    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error
        .to_string()
        .contains("existing immutable artifact is 16 bytes; retry is 5 bytes"));
}

#[test]
fn checkpoint_seal_size_ceiling_covers_the_maximum_vnode_inventory() {
    assert!(MAX_CHECKPOINT_SEAL_BYTES >= u64::from(crate::state::MAX_KEY_GROUP_COUNT) * 768);
    let path = ObjectStoreBackend::seal_path(attempt(1));
    ObjectStoreBackend::check_seal_encoded_size(&path, MAX_CHECKPOINT_SEAL_BYTES).unwrap();

    let error = ObjectStoreBackend::check_seal_encoded_size(&path, MAX_CHECKPOINT_SEAL_BYTES + 1)
        .unwrap_err();

    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("checkpoint seal is"));
}

#[tokio::test]
async fn oversized_seal_poison_is_rejected_from_metadata_on_read_and_retry() {
    let dir = tempdir().unwrap();
    let checkpoint = attempt(1);
    let object_path = ObjectStoreBackend::seal_path(checkpoint);
    let filesystem_path = dir.path().join(object_path.as_ref());
    std::fs::create_dir_all(filesystem_path.parent().unwrap()).unwrap();
    std::fs::File::create(&filesystem_path)
        .unwrap()
        .set_len(MAX_CHECKPOINT_SEAL_BYTES + 1)
        .unwrap();
    let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 1);

    let read_error = backend
        .checkpoint_seal_inventory(checkpoint)
        .await
        .unwrap_err();
    let retry_error = backend
        .seal_checkpoint(checkpoint, None, &[], &[])
        .await
        .unwrap_err();

    for error in [read_error, retry_error] {
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("checkpoint seal is"));
    }
}

#[tokio::test]
async fn checkpoint_attempts_are_isolated() {
    let dir = tempdir().unwrap();
    let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
    let old = CheckpointAttempt::canonical(5);
    let new = CheckpointAttempt::canonical(99);
    backend
        .write_partial(old, 0, 0, Bytes::from_static(b"old"))
        .await
        .unwrap();
    backend
        .write_partial(new, 0, 0, Bytes::from_static(b"new"))
        .await
        .unwrap();
    assert_eq!(
        backend.read_partial(old, 0).await.unwrap().unwrap(),
        Bytes::from_static(b"old")
    );
    assert_eq!(
        backend.read_partial(new, 0).await.unwrap().unwrap(),
        Bytes::from_static(b"new")
    );
}

#[tokio::test]
async fn seal_checkpoint_cas_is_idempotent_for_same_execution() {
    let dir = tempdir().unwrap();
    let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
    let vnodes = [0u32, 1, 2];

    assert!(!backend
        .seal_checkpoint(attempt(1), None, &vnodes, &[])
        .await
        .unwrap());
    for v in &vnodes {
        backend
            .write_partial(attempt(1), *v, 0, Bytes::from_static(b"y"))
            .await
            .unwrap();
    }
    assert!(backend
        .seal_checkpoint(attempt(1), None, &vnodes, &[])
        .await
        .unwrap());
    // Idempotent — same committer id in the audit body.
    assert!(backend
        .seal_checkpoint(attempt(1), None, &vnodes, &[])
        .await
        .unwrap());
}

#[tokio::test]
async fn sealed_artifact_metadata_rejects_a_missing_or_wrong_sized_vnode_object() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-0", 1);
    let checkpoint = attempt(1);
    backend
        .write_partial(checkpoint, 0, 0, Bytes::from_static(b"state"))
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(checkpoint, None, &[0], &[])
        .await
        .unwrap());
    let inventory = backend
        .checkpoint_seal_inventory(checkpoint)
        .await
        .unwrap()
        .unwrap();
    backend
        .verify_checkpoint_artifact_metadata(&inventory)
        .await
        .unwrap();

    let path = ObjectStoreBackend::partial_path(checkpoint, 0);
    store.delete(&path).await.unwrap();
    let missing = backend
        .verify_checkpoint_artifact_metadata(&inventory)
        .await
        .unwrap_err();
    assert!(
        missing
            .to_string()
            .contains("sealed artifact is missing from storage metadata"),
        "{missing}"
    );

    store
        .put(&path, PutPayload::from(Bytes::from_static(b"wrong")))
        .await
        .unwrap();
    let wrong_size = backend
        .verify_checkpoint_artifact_metadata(&inventory)
        .await
        .unwrap_err();
    assert!(
        wrong_size
            .to_string()
            .contains("sealed artifact is 5 bytes in storage metadata"),
        "{wrong_size}"
    );
}

#[tokio::test]
async fn node_durable_seal_uses_local_authoritative_assignment_without_cluster_fence() {
    let dir = tempdir().unwrap();
    let backend = ObjectStoreBackend::node_durable(make_store(dir.path()), "node-0", 2);
    let checkpoint = attempt(1);
    backend.set_authoritative_version(2);
    backend
        .write_partial(checkpoint, 0, 2, Bytes::from_static(b"state"))
        .await
        .unwrap();

    assert!(backend
        .seal_checkpoint(checkpoint, None, &[0], &[])
        .await
        .unwrap());
    let inventory = backend
        .checkpoint_seal_inventory(checkpoint)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inventory.sealed_partials[0].assignment_version, 2);
}

#[tokio::test]
async fn seal_body_binds_attempt_writer_fence_and_artifact_inventory() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());
    let backend = ObjectStoreBackend::new(Arc::clone(&store), "stable-node", 2);
    let checkpoint = CheckpointAttempt::canonical(401);
    let fence = assignment_fence(7, 2);
    backend.set_authoritative_version(7);
    backend
        .write_certified_partial(checkpoint, 0, &fence, 1, Bytes::from_static(b"state"))
        .await
        .unwrap();
    let descriptors = ["participant=7/sink=orders".to_string()];
    let authority = leader_proof(1, uuid::Uuid::from_u128(1), 11);
    backend
        .write_certified_commit_descriptor(
            checkpoint,
            &descriptors[0],
            &fence,
            1,
            &authority,
            Bytes::from_static(b"marker"),
        )
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(checkpoint, Some(&fence), &[0], &descriptors)
        .await
        .unwrap());

    let bytes = store
        .get(&ObjectStoreBackend::seal_path(checkpoint))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let seal: CheckpointSeal = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(seal.version, CHECKPOINT_SEAL_VERSION);
    assert_eq!(seal.attempt, checkpoint);
    assert_eq!(seal.instance_id, "stable-node");
    assert_eq!(seal.execution_id, backend.execution_id());
    assert_eq!(seal.assignment_fence.as_ref(), Some(&fence));
    assert_eq!(seal.required_vnodes, vec![0]);
    assert_eq!(seal.sealed_partials.len(), 1);
    assert_eq!(seal.sealed_partials[0].vnode, 0);
    assert_eq!(seal.sealed_partials[0].assignment_version, 7);
    assert_eq!(
        seal.sealed_partials[0]
            .writer
            .as_ref()
            .map(|writer| (writer.node_id, writer.boot_incarnation)),
        Some((1, uuid::Uuid::from_u128(1)))
    );
    assert_eq!(
        seal.sealed_partials[0]
            .writer
            .as_ref()
            .map(|writer| writer.assignment_certificate_digest),
        Some(fence.digest())
    );
    assert_eq!(seal.sealed_partials[0].payload_len, 5);
    assert_eq!(seal.required_descriptors, descriptors);
    assert_eq!(seal.sealed_descriptors.len(), 1);
    assert_eq!(seal.sealed_descriptors[0].key, descriptors[0]);
    assert_eq!(seal.sealed_descriptors[0].assignment_version, 7);
    assert_eq!(seal.sealed_descriptors[0].payload_len, 6);
    assert_eq!(
        seal.sealed_descriptors[0]
            .writer
            .as_ref()
            .map(|writer| &writer.leader_proof),
        Some(&authority)
    );
    assert_eq!(
        backend
            .checkpoint_seal_inventory(checkpoint)
            .await
            .unwrap()
            .unwrap(),
        seal.inventory()
    );

    let error = backend
        .seal_checkpoint(checkpoint, Some(&fence), &[0, 1], &descriptors)
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
}

#[tokio::test]
async fn seal_checkpoint_requires_commit_descriptors() {
    let dir = tempdir().unwrap();
    let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
    let vnodes = [0u32];
    let key = "node=node-0/sink=ice";
    let need = [key.to_string()];

    backend
        .write_partial(attempt(1), 0, 0, Bytes::from_static(b"s"))
        .await
        .unwrap();
    // Partial present but the descriptor is missing → epoch not sealed.
    assert!(!backend
        .seal_checkpoint(attempt(1), None, &vnodes, &need)
        .await
        .unwrap());

    backend
        .write_commit_descriptor(attempt(1), key, Bytes::from_static(b"df"))
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(attempt(1), None, &vnodes, &need)
        .await
        .unwrap());

    let inventory = backend
        .checkpoint_seal_inventory(attempt(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inventory.required_descriptors, need);
    assert_eq!(inventory.sealed_descriptors.len(), 1);
    assert_eq!(inventory.sealed_descriptors[0].key, key);
    assert_eq!(inventory.sealed_descriptors[0].assignment_version, 0);
    assert_eq!(inventory.sealed_descriptors[0].writer, None);
    assert_eq!(inventory.sealed_descriptors[0].payload_len, 2);
    assert_eq!(
        inventory.sealed_descriptors[0].payload_sha256,
        digest_hex(&sha256(b"df"))
    );
    assert_eq!(
        backend
            .read_commit_descriptor(attempt(1), key)
            .await
            .unwrap(),
        Some(Bytes::from_static(b"df"))
    );
}

#[tokio::test]
async fn bounded_descriptor_read_rejects_from_object_metadata() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreBackend::new(store, "node-0", 1);
    let checkpoint = attempt(1);
    backend
        .write_commit_descriptor(
            checkpoint,
            "ready",
            Bytes::from_static(b"oversized-control-record"),
        )
        .await
        .unwrap();

    let error = backend
        .read_commit_descriptor_bounded(checkpoint, "ready", 8)
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("read bound is 8"));
}

#[tokio::test]
async fn sealed_descriptor_read_rejects_a_valid_replacement_envelope() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-1", 1);
    let checkpoint = attempt(1);
    let fence = assignment_fence(7, 1);
    let authority = leader_proof(1, uuid::Uuid::from_u128(1), 11);
    let key = "participant=1/ready";
    backend.set_authoritative_version(7);
    backend
        .write_certified_commit_descriptor(
            checkpoint,
            key,
            &fence,
            1,
            &authority,
            Bytes::from_static(b"ready"),
        )
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(checkpoint, Some(&fence), &[], &[key.to_owned()])
        .await
        .unwrap());
    let inventory = backend
        .checkpoint_seal_inventory(checkpoint)
        .await
        .unwrap()
        .unwrap();
    let sealed = inventory.sealed_descriptor(key).unwrap();

    let replacement = ObjectStoreBackend::encode_commit_descriptor(
        checkpoint,
        key,
        fence.assignment_version,
        sealed.writer.as_ref(),
        &Bytes::from_static(b"evil!"),
    );
    store
        .put(
            &ObjectStoreBackend::descriptor_path(checkpoint, key),
            PutPayload::from(replacement),
        )
        .await
        .unwrap();
    assert_eq!(
        backend
            .read_commit_descriptor(checkpoint, key)
            .await
            .unwrap(),
        Some(Bytes::from_static(b"evil!")),
        "the replacement must be a self-consistent descriptor envelope"
    );

    let error = backend
        .read_sealed_commit_descriptor_bounded(checkpoint, sealed, 5)
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error
        .to_string()
        .contains("does not match the checkpoint seal"));
}

#[tokio::test]
async fn cluster_shared_local_runtime_descriptor_is_valid_without_assignment_authority() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreBackend::cluster_shared(store, "single-node", 1);
    let checkpoint = attempt(1);

    backend
        .write_commit_descriptor(checkpoint, "ready", Bytes::from_static(b"local"))
        .await
        .unwrap();

    assert_eq!(backend.authoritative_version(), 0);
    assert_eq!(
        backend
            .read_commit_descriptor(checkpoint, "ready")
            .await
            .unwrap(),
        Some(Bytes::from_static(b"local"))
    );
}

#[tokio::test]
async fn installed_assignment_rejects_uncertified_descriptor_before_publication() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreBackend::new(store, "node-1", 1);
    let checkpoint = attempt(1);
    let fence = assignment_fence(7, 1);
    let authority = leader_proof(1, uuid::Uuid::from_u128(1), 11);
    let key = "participant=1/ready";
    backend.set_authoritative_version(7);
    backend
        .write_certified_partial(checkpoint, 0, &fence, 1, Bytes::from_static(b"state"))
        .await
        .unwrap();
    let error = backend
        .write_commit_descriptor(checkpoint, key, Bytes::from_static(b"uncertified"))
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("uncertified commit descriptor"));
    assert_eq!(
        backend
            .read_commit_descriptor(checkpoint, key)
            .await
            .unwrap(),
        None
    );

    backend
        .write_certified_commit_descriptor(
            checkpoint,
            key,
            &fence,
            1,
            &authority,
            Bytes::from_static(b"certified"),
        )
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(checkpoint, Some(&fence), &[0], &[key.to_string()])
        .await
        .unwrap());
}

#[tokio::test]
async fn cluster_seal_rejects_stale_boot_descriptor_poison() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreBackend::new(store, "node-1", 1);
    let checkpoint = attempt(1);
    let stale = assignment_fence(7, 1);
    let current = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1],
        vec![crate::checkpoint::CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(2),
        }],
    )
    .unwrap();
    let stale_authority = leader_proof(1, uuid::Uuid::from_u128(1), 11);
    let current_authority = leader_proof(1, uuid::Uuid::from_u128(2), 12);
    let key = "participant=1/ready";
    backend.set_authoritative_version(7);
    backend
        .write_certified_partial(checkpoint, 0, &current, 1, Bytes::from_static(b"state"))
        .await
        .unwrap();
    backend
        .write_certified_commit_descriptor(
            checkpoint,
            key,
            &stale,
            1,
            &stale_authority,
            Bytes::from_static(b"stale"),
        )
        .await
        .unwrap();

    let poison = backend
        .write_certified_commit_descriptor(
            checkpoint,
            key,
            &current,
            1,
            &current_authority,
            Bytes::from_static(b"current"),
        )
        .await
        .unwrap_err();
    assert!(matches!(poison, StateBackendError::Conflict { .. }));
    let error = backend
        .seal_checkpoint(checkpoint, Some(&current), &[0], &[key.to_string()])
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("writer certificate"));
}

#[tokio::test]
async fn cluster_seal_rejects_descriptors_from_different_leader_terms() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreBackend::new(store, "node-1", 1);
    let checkpoint = attempt(1);
    let fence = assignment_fence(7, 1);
    backend.set_authoritative_version(7);
    backend
        .write_certified_partial(checkpoint, 0, &fence, 1, Bytes::from_static(b"state"))
        .await
        .unwrap();
    for (key, token) in [("participant=1/ready", 11), ("coordinator", 12)] {
        backend
            .write_certified_commit_descriptor(
                checkpoint,
                key,
                &fence,
                1,
                &leader_proof(1, uuid::Uuid::from_u128(1), token),
                Bytes::from_static(b"ready"),
            )
            .await
            .unwrap();
    }

    let error = backend
        .seal_checkpoint(
            checkpoint,
            Some(&fence),
            &[0],
            &["participant=1/ready".into(), "coordinator".into()],
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("different leader terms"));
}

#[tokio::test]
async fn descriptor_read_rejects_key_mismatch_and_payload_corruption() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-1", 1);
    let checkpoint = attempt(1);
    let encoded = ObjectStoreBackend::encode_commit_descriptor(
        checkpoint,
        "expected-key",
        0,
        None,
        &Bytes::from_static(b"payload"),
    );
    store
        .put(
            &ObjectStoreBackend::descriptor_path(checkpoint, "wrong-key"),
            PutPayload::from(encoded),
        )
        .await
        .unwrap();
    let mismatch = backend
        .read_commit_descriptor(checkpoint, "wrong-key")
        .await
        .unwrap_err();
    assert!(matches!(mismatch, StateBackendError::Conflict { .. }));
    assert!(mismatch.to_string().contains("key digest"));

    let other_attempt = attempt(2);
    let encoded = ObjectStoreBackend::encode_commit_descriptor(
        checkpoint,
        "attempt-bound",
        0,
        None,
        &Bytes::from_static(b"payload"),
    );
    store
        .put(
            &ObjectStoreBackend::descriptor_path(other_attempt, "attempt-bound"),
            PutPayload::from(encoded),
        )
        .await
        .unwrap();
    let mismatch = backend
        .read_commit_descriptor(other_attempt, "attempt-bound")
        .await
        .unwrap_err();
    assert!(matches!(mismatch, StateBackendError::Conflict { .. }));
    assert!(mismatch.to_string().contains("names attempt"));

    let mut corrupted = ObjectStoreBackend::encode_commit_descriptor(
        checkpoint,
        "corrupted",
        0,
        None,
        &Bytes::from_static(b"payload"),
    )
    .to_vec();
    *corrupted.last_mut().unwrap() ^= 0xff;
    store
        .put(
            &ObjectStoreBackend::descriptor_path(checkpoint, "corrupted"),
            PutPayload::from(Bytes::from(corrupted)),
        )
        .await
        .unwrap();
    let corruption = backend
        .read_commit_descriptor(checkpoint, "corrupted")
        .await
        .unwrap_err();
    assert!(matches!(corruption, StateBackendError::Serialization(_)));
    assert!(corruption.to_string().contains("checksum mismatch"));
}

#[tokio::test]
async fn seal_rejects_descriptor_with_truncated_payload() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-1", 1);
    let checkpoint = attempt(1);
    let key = "ready";
    backend
        .write_partial(checkpoint, 0, 0, Bytes::from_static(b"state"))
        .await
        .unwrap();
    let mut encoded = ObjectStoreBackend::encode_commit_descriptor(
        checkpoint,
        key,
        0,
        None,
        &Bytes::from_static(b"payload"),
    )
    .to_vec();
    encoded.pop();
    store
        .put(
            &ObjectStoreBackend::descriptor_path(checkpoint, key),
            PutPayload::from(Bytes::from(encoded)),
        )
        .await
        .unwrap();

    let error = backend
        .seal_checkpoint(checkpoint, None, &[0], &[key.to_string()])
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("declared 7 payload bytes"));
}

/// The CAS-create `AlreadyExists` branch must not silently agree it committed:
/// the loser reads the marker, sees a mismatched audit body, and fails loud.
#[tokio::test]
async fn seal_rejects_different_execution_incarnation() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());
    let winner = ObjectStoreBackend::new(Arc::clone(&store), "winner", 4);
    let loser = ObjectStoreBackend::new(Arc::clone(&store), "loser", 4);

    let vnodes = [0u32, 1];
    // Both "nodes" wrote partials for the epoch.
    for v in &vnodes {
        winner
            .write_partial(attempt(7), *v, 0, Bytes::from_static(b"w"))
            .await
            .unwrap();
    }

    // Winner CAS-creates the state seal first.
    assert!(winner
        .seal_checkpoint(attempt(7), None, &vnodes, &[])
        .await
        .unwrap());

    // Loser finds a seal created by a different execution incarnation.
    let err = loser
        .seal_checkpoint(attempt(7), None, &vnodes, &[])
        .await
        .unwrap_err();
    assert!(matches!(err, StateBackendError::Conflict { .. }));

    // And the winner's repeated call is still idempotent Ok(true).
    assert!(winner
        .seal_checkpoint(attempt(7), None, &vnodes, &[])
        .await
        .unwrap());
}

/// Same contract on the CAS-loser path: if the marker doesn't exist
/// at HEAD time but a peer sneaks in between our vnode-presence
/// check and our own PUT, our `put_opts` fails with `AlreadyExists`.
/// That branch must also compare committers, not silently succeed.
#[tokio::test]
async fn seal_cas_loser_rejects_different_execution() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());
    let winner = ObjectStoreBackend::new(Arc::clone(&store), "winner", 4);
    let loser = ObjectStoreBackend::new(Arc::clone(&store), "loser", 4);

    let vnodes = [0u32, 1];
    for v in &vnodes {
        winner
            .write_partial(attempt(3), *v, 0, Bytes::from_static(b"w"))
            .await
            .unwrap();
    }
    // Manually pre-seed a structured seal under "winner" to
    // simulate the TOCTOU race deterministically — the loser's
    // put_opts will hit AlreadyExists on its own PUT attempt.
    let commit = ObjectStoreBackend::seal_path(attempt(3));
    let mut sealed_partials = Vec::new();
    for &vnode in &vnodes {
        sealed_partials.push(
            winner
                .read_partial_attestation(attempt(3), vnode)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let seal = CheckpointSeal::new(
        "winner".into(),
        winner.execution_id(),
        CheckpointSealInventory {
            attempt: attempt(3),
            assignment_fence: None,
            assignment_version: 0,
            required_vnodes: vnodes.to_vec(),
            sealed_partials,
            required_descriptors: Vec::new(),
            sealed_descriptors: Vec::new(),
        },
    );
    store
        .put(
            &commit,
            PutPayload::from(Bytes::from(serde_json::to_vec(&seal).unwrap())),
        )
        .await
        .unwrap();

    let err = loser
        .seal_checkpoint(attempt(3), None, &vnodes, &[])
        .await
        .unwrap_err();
    assert!(matches!(err, StateBackendError::Conflict { .. }));
}

#[tokio::test]
async fn stale_version_rejected() {
    // Force two "nodes" (backend instances wrapping the same store)
    // to claim the same vnode at different generations. The stale
    // writer must be rejected.
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());
    let stale = ObjectStoreBackend::new(Arc::clone(&store), "node-stale", 4);
    let fresh = ObjectStoreBackend::new(Arc::clone(&store), "node-fresh", 4);

    // Fresh learns about a new assignment generation — e.g. a new
    // snapshot rotated in after a leader election.
    fresh.set_authoritative_version(2);

    // Fresh writes at the current version: accepted.
    fresh
        .write_partial(attempt(1), 0, 2, Bytes::from_static(b"fresh"))
        .await
        .unwrap();

    // Stale tries to write at version 1 — but only IF it's also
    // learned of the rotation. Model that by promoting stale's
    // view too; the check is intra-backend here because the
    // durable version-broadcast channel is out of scope for this test.
    stale.set_authoritative_version(2);
    let err = stale
        .write_partial(attempt(1), 0, 1, Bytes::from_static(b"stale"))
        .await
        .unwrap_err();
    match err {
        StateBackendError::StaleVersion {
            caller,
            authoritative,
        } => {
            assert_eq!(caller, 1);
            assert_eq!(authoritative, 2);
        }
        other => panic!("expected StaleVersion, got {other:?}"),
    }

    // Fence-disabled backend (authoritative stays at 0) accepts
    // any version — preserves legacy single-instance behavior.
    let unfenced = ObjectStoreBackend::new(Arc::clone(&store), "node-unfenced", 4);
    unfenced
        .write_partial(attempt(1), 1, 0, Bytes::from_static(b"ok"))
        .await
        .unwrap();
}

#[tokio::test]
async fn future_assignment_version_is_rejected_before_publication() {
    let backend =
        ObjectStoreBackend::new(Arc::new(object_store::memory::InMemory::new()), "node-1", 1);
    backend.set_authoritative_version(7);
    let future = assignment_fence(8, 1);

    let error = backend
        .write_certified_partial(attempt(1), 0, &future, 1, Bytes::from_static(b"future"))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        StateBackendError::FutureVersion {
            caller: 8,
            authoritative: 7
        }
    ));
    assert!(backend.read_partial(attempt(1), 0).await.unwrap().is_none());

    // A future partial that landed before fencing was configured must still fail the seal
    // after this backend adopts the current generation.
    let bypass =
        ObjectStoreBackend::new(Arc::new(object_store::memory::InMemory::new()), "node-1", 1);
    bypass
        .write_certified_partial(attempt(2), 0, &future, 1, Bytes::from_static(b"future"))
        .await
        .unwrap();
    bypass.set_authoritative_version(7);
    let current = assignment_fence(7, 1);
    let error = bypass
        .seal_checkpoint(attempt(2), Some(&current), &[0], &[])
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("cannot satisfy seal version 7"));
}

#[tokio::test]
async fn seal_rejects_stale_boot_writer_certificate() {
    let backend =
        ObjectStoreBackend::new(Arc::new(object_store::memory::InMemory::new()), "node-1", 1);
    backend.set_authoritative_version(7);
    let stale = assignment_fence(7, 1);
    let current = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1],
        vec![crate::checkpoint::CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(2),
        }],
    )
    .unwrap();
    backend
        .write_certified_partial(attempt(1), 0, &stale, 1, Bytes::from_static(b"stale-boot"))
        .await
        .unwrap();

    let error = backend
        .seal_checkpoint(attempt(1), Some(&current), &[0], &[])
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("writer certificate"));
}

#[tokio::test]
async fn seal_rejects_assignment_certificate_digest_mismatch() {
    use crate::checkpoint::CheckpointParticipant;

    let backend =
        ObjectStoreBackend::new(Arc::new(object_store::memory::InMemory::new()), "node-1", 2);
    backend.set_authoritative_version(7);
    let participants = vec![
        CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(1),
        },
        CheckpointParticipant {
            node_id: 2,
            boot_incarnation: uuid::Uuid::from_u128(2),
        },
    ];
    let written =
        CheckpointAssignmentFence::from_owner_map(7, &[1, 2], participants.clone()).unwrap();
    let sealing = CheckpointAssignmentFence::from_owner_map(7, &[2, 1], participants).unwrap();
    backend
        .write_certified_partial(attempt(1), 0, &written, 1, Bytes::from_static(b"wrong-map"))
        .await
        .unwrap();

    let error = backend
        .seal_checkpoint(attempt(1), Some(&sealing), &[0], &[])
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("writer certificate"));
}

#[tokio::test]
async fn stale_generation_partial_cannot_satisfy_fresh_seal() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());
    let stale = ObjectStoreBackend::new(Arc::clone(&store), "node-stale", 4);
    let fresh = ObjectStoreBackend::new(store, "node-fresh", 4);
    let checkpoint = CheckpointAttempt::canonical(901);

    // The stale process has not learned generation 2 and wins the create-once path first.
    stale
        .write_partial(checkpoint, 0, 1, Bytes::from_static(b"stale-state"))
        .await
        .unwrap();
    fresh.set_authoritative_version(2);
    let fence = assignment_fence(2, 4);

    let error = fresh
        .seal_checkpoint(checkpoint, Some(&fence), &[0], &[])
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(error.to_string().contains("cannot satisfy seal version 2"));
    assert!(fresh
        .checkpoint_seal_inventory(checkpoint)
        .await
        .unwrap()
        .is_none());
}

#[test]
fn authoritative_version_is_monotonic() {
    let dir = tempdir().unwrap();
    let b = ObjectStoreBackend::new(make_store(dir.path()), "node", 2);
    assert_eq!(b.authoritative_version(), 0);
    b.set_authoritative_version(3);
    assert_eq!(b.authoritative_version(), 3);
    // Attempts to lower the version are no-ops.
    b.set_authoritative_version(1);
    assert_eq!(b.authoritative_version(), 3);
    b.set_authoritative_version(4);
    assert_eq!(b.authoritative_version(), 4);
}

#[test]
fn durability_scope_requires_explicit_storage_topology() {
    let dir = tempdir().unwrap();
    assert_eq!(
        ObjectStoreBackend::new(make_store(dir.path()), "uncertified", 2).durability_scope(),
        StateBackendDurability::Volatile
    );
    assert_eq!(
        ObjectStoreBackend::node_durable(make_store(dir.path()), "local", 2).durability_scope(),
        StateBackendDurability::NodeDurable
    );
    assert_eq!(
        ObjectStoreBackend::cluster_shared(make_store(dir.path()), "shared", 2).durability_scope(),
        StateBackendDurability::ClusterShared
    );
}

#[tokio::test]
async fn object_safe_behind_arc() {
    let dir = tempdir().unwrap();
    let _: Arc<dyn StateBackend> =
        Arc::new(ObjectStoreBackend::new(make_store(dir.path()), "node-0", 2));
}

#[tokio::test]
async fn prune_before_deletes_old_epochs() {
    let dir = tempdir().unwrap();
    let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);

    // Seed epochs 1..=5 with one vnode each.
    for epoch in 1..=5u64 {
        backend
            .write_partial(attempt(epoch), 0, 0, Bytes::from_static(b"x"))
            .await
            .unwrap();
    }

    backend.prune_before(4).await.unwrap();

    for epoch in 1..=3 {
        assert!(
            backend
                .read_partial(attempt(epoch), 0)
                .await
                .unwrap()
                .is_none(),
            "epoch {epoch} should be pruned",
        );
    }
    for epoch in 4..=5 {
        assert!(
            backend
                .read_partial(attempt(epoch), 0)
                .await
                .unwrap()
                .is_some(),
            "epoch {epoch} should be retained",
        );
    }
}

#[tokio::test]
async fn prune_before_completes_on_durable_local_store() {
    let dir = tempdir().unwrap();
    let backend = ObjectStoreBackend::node_durable(make_durable_store(dir.path()), "node-0", 4);
    for epoch in 1..=3_u64 {
        for vnode in 0..4 {
            backend
                .write_partial(attempt(epoch), vnode, 0, Bytes::from_static(b"state"))
                .await
                .unwrap();
        }
    }

    tokio::time::timeout(std::time::Duration::from_secs(5), backend.prune_before(3))
        .await
        .expect("durable-local state retention deadlocked")
        .unwrap();

    for epoch in 1..=2_u64 {
        for vnode in 0..4 {
            assert!(backend
                .read_partial(attempt(epoch), vnode)
                .await
                .unwrap()
                .is_none());
        }
    }
    for vnode in 0..4 {
        assert!(backend
            .read_partial(attempt(3), vnode)
            .await
            .unwrap()
            .is_some());
    }
    let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
    assert_eq!(floor.before_epoch, 3);
    assert_eq!(floor.swept_before_epoch, 3);
}

#[tokio::test]
async fn prune_before_repairs_empty_durable_local_prefixes() {
    let directory = tempdir().unwrap();
    let store = Arc::new(
        crate::durable_local_store::DurableLocalObjectStore::new(directory.path()).unwrap(),
    );
    let backend =
        ObjectStoreBackend::node_durable_with_empty_prefix_cleanup(Arc::clone(&store), "node-0", 1);
    let empty_leaf = directory
        .path()
        .join("state-v2/epoch=1/checkpoint=1/vnode=0");
    std::fs::create_dir_all(&empty_leaf).unwrap();
    std::fs::write(empty_leaf.join(".laminardb-object#123"), b"orphan").unwrap();
    assert!(store
        .list(Some(&OsPath::from("state-v2/epoch=1")))
        .next()
        .await
        .is_none());

    backend.prune_before(2).await.unwrap();

    assert!(!directory.path().join("state-v2/epoch=1").exists());
    let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
    assert_eq!(floor.before_epoch, 2);
    assert_eq!(floor.swept_before_epoch, 2);
}

#[tokio::test]
async fn prune_before_repeats_bounded_deletes_until_large_prefix_is_empty() {
    let directory = tempdir().unwrap();
    let store = make_durable_store(directory.path());
    let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-0", 1);
    let retired = attempt(1);
    let prefix = ObjectStoreBackend::attempt_prefix(retired);
    let physical_prefix = directory.path().join(prefix.trim_end_matches('/'));
    std::fs::create_dir_all(&physical_prefix).unwrap();

    // Seed without 1,025 durable puts: this test targets the local filesystem's lazy 1,024
    // entry listing boundary and the prune sweep, not publication durability.
    for artifact in 0..1_025 {
        std::fs::write(
            physical_prefix.join(format!("artifact={artifact:04}.bin")),
            b"state",
        )
        .unwrap();
    }

    tokio::time::timeout(std::time::Duration::from_secs(30), backend.prune_before(2))
        .await
        .expect("large durable-local prune exceeded its latency bound")
        .unwrap();

    let retired_prefix = OsPath::from(prefix);
    assert!(store.list(Some(&retired_prefix)).next().await.is_none());
    assert!(!directory.path().join("state-v2/epoch=1").exists());
    let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
    assert_eq!(floor.before_epoch, 2);
    assert_eq!(floor.swept_before_epoch, 2);
}

#[tokio::test]
async fn prune_before_discovers_sparse_epochs_without_scanning_the_id_gap() {
    let dir = tempdir().unwrap();
    let operations = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let physical_store = make_store(dir.path());
    let store: Arc<dyn ObjectStore> = Arc::new(RetentionLogStore {
        inner: Arc::clone(&physical_store),
        operations: Arc::clone(&operations),
        delete_calls: Arc::new(AtomicU64::new(0)),
        fail_delete_call: 0,
    });
    let backend = ObjectStoreBackend::new(store, "node-0", 1);
    let retired = attempt(1);
    let retained = attempt(65_537);

    backend
        .write_partial(retired, 0, 0, Bytes::from_static(b"retired"))
        .await
        .unwrap();
    backend
        .write_partial(retained, 0, 0, Bytes::from_static(b"retained"))
        .await
        .unwrap();

    operations.lock().clear();
    backend.prune_before(65_537).await.unwrap();

    assert!(matches!(
        physical_store
            .head(&ObjectStoreBackend::partial_path(retired, 0))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert!(physical_store
        .head(&ObjectStoreBackend::partial_path(retained, 0))
        .await
        .is_ok());

    let listings = operations
        .lock()
        .iter()
        .filter(|operation| operation.starts_with("delimiter:") || operation.starts_with("list:"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        listings,
        vec![
            "delimiter:state-v2".to_string(),
            "list:state-v2/epoch=1".to_string(),
            "list:state-v2/epoch=1".to_string(),
        ],
        "retention must collect and then verify only materialized retired epoch prefixes"
    );
}

#[tokio::test]
async fn cluster_shared_prune_requires_native_conditional_update() {
    let dir = tempdir().unwrap();
    let backend = ObjectStoreBackend::cluster_shared(make_store(dir.path()), "cluster", 2);
    backend
        .write_partial(attempt(1), 0, 0, Bytes::from_static(b"state"))
        .await
        .unwrap();

    let error = backend.prune_before(2).await.unwrap_err();
    assert!(error.to_string().contains("PutMode::Update"), "{error}");
    let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
    assert_eq!(floor.before_epoch, 2);
    assert_eq!(floor.swept_before_epoch, 0);
    assert!(backend.read_partial(attempt(1), 0).await.unwrap().is_none());
}

#[tokio::test]
async fn retention_publishes_one_floor_before_deleting_attested_artifacts() {
    let dir = tempdir().unwrap();
    let operations = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let store: Arc<dyn ObjectStore> = Arc::new(RetentionLogStore {
        inner: make_store(dir.path()),
        operations: Arc::clone(&operations),
        delete_calls: Arc::new(AtomicU64::new(0)),
        fail_delete_call: 0,
    });
    let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-0", 4);
    let ready_key = "participant-ready/0.json";

    for epoch in 1..=3u64 {
        backend
            .write_partial(attempt(epoch), 0, 0, Bytes::from_static(b"state"))
            .await
            .unwrap();
        backend
            .write_commit_descriptor(attempt(epoch), ready_key, Bytes::from_static(b"ready"))
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(attempt(epoch), None, &[0], &[ready_key.to_string()])
            .await
            .unwrap());
    }

    operations.lock().clear();
    backend.prune_before(2).await.unwrap();

    let recorded = operations.lock().clone();
    let first_floor = recorded
        .iter()
        .position(|operation| operation.starts_with("floor:"))
        .unwrap_or_else(|| panic!("prune_before did not publish its floor: {recorded:?}"));
    let first_artifact = recorded
        .iter()
        .position(|operation| operation.starts_with("delete:"))
        .unwrap_or_else(|| panic!("prune_before did not delete artifacts: {recorded:?}"));
    assert!(
        first_floor < first_artifact,
        "prune_before deleted artifacts before publishing its floor: {recorded:?}"
    );

    let seal_path = ObjectStoreBackend::seal_path(attempt(1));
    assert!(matches!(
        store.get(&seal_path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert!(backend
        .checkpoint_seal_inventory(attempt(1))
        .await
        .unwrap()
        .is_none());
    let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
    assert_eq!(floor.before_epoch, 2);
    assert_eq!(floor.swept_before_epoch, 2);
}

#[tokio::test]
async fn prune_wins_after_sealer_verifies_but_before_seal_publication() {
    let dir = tempdir().unwrap();
    let checkpoint = attempt(1);
    let reached = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let store: Arc<dyn ObjectStore> = Arc::new(SealPublishGateStore {
        inner: make_store(dir.path()),
        seal_path: ObjectStoreBackend::seal_path(checkpoint),
        gated: std::sync::atomic::AtomicBool::new(false),
        reached: Arc::clone(&reached),
        release: Arc::clone(&release),
    });
    let sealer = Arc::new(ObjectStoreBackend::new(Arc::clone(&store), "sealer", 2));
    let collector = Arc::new(ObjectStoreBackend::new(store, "collector", 2));

    sealer
        .write_partial(checkpoint, 0, 0, Bytes::from_static(b"state"))
        .await
        .unwrap();

    let seal_task = tokio::spawn({
        let sealer = Arc::clone(&sealer);
        async move { sealer.seal_checkpoint(checkpoint, None, &[0], &[]).await }
    });
    reached
        .acquire()
        .await
        .expect("test gate remains open")
        .forget();

    // The sealer has listed and attested the partial, but its create has not reached shared
    // storage. Retention publishes the global floor while that create is suspended.
    collector.prune_before(2).await.unwrap();
    assert!(collector
        .read_partial(checkpoint, 0)
        .await
        .unwrap()
        .is_none());
    assert!(collector
        .checkpoint_seal_inventory(checkpoint)
        .await
        .unwrap()
        .is_none());

    release.add_permits(1);
    let error = seal_task.await.unwrap().unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));

    // The durable floor rejects a retry without retaining one tombstone per attempt.
    let error = sealer
        .seal_checkpoint(checkpoint, None, &[0], &[])
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
}

#[tokio::test]
async fn prune_failure_preserves_completed_prefix_progress_and_repairs_the_rest() {
    let dir = tempdir().unwrap();
    let physical_store = make_store(dir.path());
    let store: Arc<dyn ObjectStore> = Arc::new(RetentionLogStore {
        inner: Arc::clone(&physical_store),
        operations: Arc::new(parking_lot::Mutex::new(Vec::new())),
        delete_calls: Arc::new(AtomicU64::new(0)),
        fail_delete_call: 2,
    });
    let backend = ObjectStoreBackend::new(store, "node-0", 2);
    for checkpoint_id in 1..=2 {
        backend
            .write_partial(attempt(checkpoint_id), 0, 0, Bytes::from_static(b"state"))
            .await
            .unwrap();
    }

    let error = backend.prune_before(3).await.unwrap_err();
    assert!(matches!(error, StateBackendError::Io(_)));
    assert!(error.to_string().contains("injected delete failure"));
    let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
    assert_eq!(floor.before_epoch, 3);
    assert_eq!(floor.swept_before_epoch, 2);
    assert!(matches!(
        physical_store
            .head(&ObjectStoreBackend::partial_path(attempt(1), 0))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert!(physical_store
        .head(&ObjectStoreBackend::partial_path(attempt(2), 0))
        .await
        .is_ok());

    backend.prune_before(3).await.unwrap();
    let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
    assert_eq!(floor.before_epoch, 3);
    assert_eq!(floor.swept_before_epoch, 3);
    assert!(matches!(
        physical_store
            .head(&ObjectStoreBackend::partial_path(attempt(2), 0))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
}

/// The durable sweep cursor advances so later retention lists only newly retired epochs.
#[tokio::test]
async fn prune_before_is_incremental_and_advances_horizon() {
    let dir = tempdir().unwrap();
    let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);

    // Seed epochs 1..=6, two vnodes each so deletes touch >1 object.
    for epoch in 1..=6u64 {
        for v in 0..2u32 {
            backend
                .write_partial(attempt(epoch), v, 0, Bytes::from_static(b"x"))
                .await
                .unwrap();
        }
    }

    backend.prune_before(3).await.unwrap();
    let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
    assert_eq!(floor.before_epoch, 3);
    assert_eq!(floor.swept_before_epoch, 3);

    backend.prune_before(5).await.unwrap();
    let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
    assert_eq!(floor.before_epoch, 5);
    assert_eq!(floor.swept_before_epoch, 5);

    for epoch in 1..=4u64 {
        for v in 0..2u32 {
            assert!(
                backend
                    .read_partial(attempt(epoch), v)
                    .await
                    .unwrap()
                    .is_none(),
                "epoch {epoch} vnode {v} should be pruned",
            );
        }
    }
    for epoch in 5..=6u64 {
        for v in 0..2u32 {
            assert!(
                backend
                    .read_partial(attempt(epoch), v)
                    .await
                    .unwrap()
                    .is_some(),
                "epoch {epoch} vnode {v} should be retained",
            );
        }
    }

    // Idempotent re-prune at the same horizon is a no-op.
    backend.prune_before(5).await.unwrap();
    let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
    assert_eq!(floor.before_epoch, 5);
    assert_eq!(floor.swept_before_epoch, 5);
    assert!(backend.read_partial(attempt(5), 0).await.unwrap().is_some());
}

#[tokio::test]
async fn durable_floor_rejects_late_writes_after_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-0", 4);

    backend
        .write_partial(attempt(1), 0, 0, Bytes::from_static(b"x"))
        .await
        .unwrap();
    backend.prune_before(3).await.unwrap();
    assert!(backend.read_partial(attempt(1), 0).await.unwrap().is_none());

    let restarted = ObjectStoreBackend::new(store, "node-0", 4);
    let error = restarted
        .write_partial(attempt(1), 0, 0, Bytes::from_static(b"late"))
        .await
        .unwrap_err();
    assert!(matches!(error, StateBackendError::Conflict { .. }));
    assert!(restarted
        .read_partial(attempt(1), 0)
        .await
        .unwrap()
        .is_none());
    assert!(restarted
        .write_partial(attempt(3), 0, 0, Bytes::from_static(b"live"))
        .await
        .is_ok());
}
