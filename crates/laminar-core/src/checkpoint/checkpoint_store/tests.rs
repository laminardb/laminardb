use super::*;
use crate::checkpoint::checkpoint_manifest::{ConnectorCheckpoint, OperatorCheckpoint};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn make_store(dir: &Path) -> FileSystemCheckpointStore {
    FileSystemCheckpointStore::new(dir)
}

fn make_prepared_manifest(id: u64) -> CheckpointManifest {
    let mut manifest = CheckpointManifest::new(id, id);
    manifest.deployment_id = uuid::Uuid::from_u128(1).to_string();
    manifest
}

fn make_manifest(id: u64) -> CheckpointManifest {
    let mut manifest = make_prepared_manifest(id);
    manifest.durable_phase = DurableCheckpointPhase::Finalized;
    manifest
}

fn declare_external_state(manifest: &mut CheckpointManifest, length: usize) {
    manifest.operator_states.insert(
        "external".into(),
        OperatorCheckpoint::external(0, u64::try_from(length).unwrap()),
    );
}

fn make_external_manifest(id: u64, length: usize) -> CheckpointManifest {
    let mut manifest = make_manifest(id);
    declare_external_state(&mut manifest, length);
    manifest
}

#[test]
fn checkpoint_stores_default_to_local_key_group_count() {
    let dir = tempfile::tempdir().unwrap();
    let filesystem = FileSystemCheckpointStore::new(dir.path());
    let object_store = ObjectStoreCheckpointStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        String::new(),
    );

    assert_eq!(filesystem.key_group_count(), LOCAL_KEY_GROUP_COUNT);
    assert_eq!(object_store.key_group_count(), LOCAL_KEY_GROUP_COUNT);
    assert_eq!(
        filesystem.max_state_data_bytes(),
        DEFAULT_MAX_CHECKPOINT_STATE_BYTES
    );
    assert_eq!(
        object_store.max_state_data_bytes(),
        DEFAULT_MAX_CHECKPOINT_STATE_BYTES
    );
}

#[test]
fn checkpoint_state_budget_rejects_unusable_limits() {
    assert!(matches!(
        validate_max_checkpoint_state_bytes(0),
        Err(CheckpointStoreError::Invalid(error)) if error.contains("must be greater than zero")
    ));
    assert!(matches!(
        validate_max_checkpoint_state_bytes(u64::MAX),
        Err(CheckpointStoreError::Invalid(error)) if error.contains("one-byte overflow probe")
    ));
    if let Some(above_isize) = u64::try_from(isize::MAX)
        .ok()
        .and_then(|maximum| maximum.checked_add(1))
    {
        assert!(matches!(
            validate_max_checkpoint_state_bytes(above_isize),
            Err(CheckpointStoreError::Invalid(error)) if error.contains("process address space")
        ));
    }

    let dir = tempfile::tempdir().unwrap();
    assert!(matches!(
        FileSystemCheckpointStore::new(dir.path()).with_max_state_data_bytes(0),
        Err(CheckpointStoreError::Invalid(_))
    ));
    assert!(matches!(
        ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
        )
        .with_max_state_data_bytes(0),
        Err(CheckpointStoreError::Invalid(_))
    ));
}

#[test]
fn checkpoint_stores_accept_explicit_key_group_count() {
    let key_group_count = KeyGroupCount::try_from(256_u16).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let filesystem =
        FileSystemCheckpointStore::new(dir.path()).with_key_group_count(key_group_count);
    let object_store = ObjectStoreCheckpointStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        String::new(),
    )
    .with_key_group_count(key_group_count);

    assert_eq!(filesystem.key_group_count(), key_group_count);
    assert_eq!(object_store.key_group_count(), key_group_count);
}

#[tokio::test]
async fn checkpoint_stores_reject_noncanonical_manifest_before_sidecar_write() {
    let invalid = CheckpointManifest::new(7, 8);
    let chunks = [bytes::Bytes::from_static(b"must-not-persist")];

    let dir = tempfile::tempdir().unwrap();
    let filesystem = FileSystemCheckpointStore::new(dir.path());
    let error = filesystem
        .save_with_state(&invalid, Some(&chunks))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("canonical checkpoint ID"));
    assert!(!filesystem.state_path(7).exists());
    assert!(filesystem.list_ids().await.unwrap().is_empty());

    let object_store = ObjectStoreCheckpointStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        String::new(),
    );
    let error = object_store
        .save_with_state(&invalid, Some(&chunks))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("canonical checkpoint ID"));
    assert!(object_store.load_state_data(7).await.unwrap().is_none());
    assert!(object_store.list_ids().await.unwrap().is_empty());
}

#[derive(Debug)]
struct GetCountingStore {
    inner: Arc<dyn ObjectStore>,
    manifest_gets: AtomicUsize,
    state_gets: AtomicUsize,
    lost_ack: Option<LostAckTarget>,
    lost_ack_remaining: AtomicUsize,
    fail_manifest_delete: bool,
    state_delete_fault: Option<StateDeleteFault>,
    state_delete_fault_remaining: Arc<AtomicUsize>,
    get_fault: Option<GetFault>,
    bounded_latest_get_seen: AtomicBool,
}

#[derive(Debug, Clone, Copy)]
enum LostAckTarget {
    LatestCreate,
    LatestUpdate,
    ManifestUpdate,
}

#[derive(Debug, Clone, Copy)]
enum StateDeleteFault {
    HardFailure,
    LostAck,
}

#[derive(Debug, Clone, Copy)]
enum GetFault {
    MisreportedRange,
    MisreportedSize,
    ShortBody,
    LongBody,
    MissingVersion,
}

impl GetCountingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            manifest_gets: AtomicUsize::new(0),
            state_gets: AtomicUsize::new(0),
            lost_ack: None,
            lost_ack_remaining: AtomicUsize::new(0),
            fail_manifest_delete: false,
            state_delete_fault: None,
            state_delete_fault_remaining: Arc::new(AtomicUsize::new(0)),
            get_fault: None,
            bounded_latest_get_seen: AtomicBool::new(false),
        }
    }

    fn with_lost_ack(inner: Arc<dyn ObjectStore>, target: LostAckTarget) -> Self {
        Self {
            inner,
            manifest_gets: AtomicUsize::new(0),
            state_gets: AtomicUsize::new(0),
            lost_ack: Some(target),
            lost_ack_remaining: AtomicUsize::new(1),
            fail_manifest_delete: false,
            state_delete_fault: None,
            state_delete_fault_remaining: Arc::new(AtomicUsize::new(0)),
            get_fault: None,
            bounded_latest_get_seen: AtomicBool::new(false),
        }
    }

    fn with_manifest_delete_failure(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            manifest_gets: AtomicUsize::new(0),
            state_gets: AtomicUsize::new(0),
            lost_ack: None,
            lost_ack_remaining: AtomicUsize::new(0),
            fail_manifest_delete: true,
            state_delete_fault: None,
            state_delete_fault_remaining: Arc::new(AtomicUsize::new(0)),
            get_fault: None,
            bounded_latest_get_seen: AtomicBool::new(false),
        }
    }

    fn with_state_delete_fault(
        inner: Arc<dyn ObjectStore>,
        state_delete_fault: StateDeleteFault,
    ) -> Self {
        Self {
            inner,
            manifest_gets: AtomicUsize::new(0),
            state_gets: AtomicUsize::new(0),
            lost_ack: None,
            lost_ack_remaining: AtomicUsize::new(0),
            fail_manifest_delete: false,
            state_delete_fault: Some(state_delete_fault),
            state_delete_fault_remaining: Arc::new(AtomicUsize::new(1)),
            get_fault: None,
            bounded_latest_get_seen: AtomicBool::new(false),
        }
    }

    fn with_get_fault(inner: Arc<dyn ObjectStore>, get_fault: GetFault) -> Self {
        Self {
            inner,
            manifest_gets: AtomicUsize::new(0),
            state_gets: AtomicUsize::new(0),
            lost_ack: None,
            lost_ack_remaining: AtomicUsize::new(0),
            fail_manifest_delete: false,
            state_delete_fault: None,
            state_delete_fault_remaining: Arc::new(AtomicUsize::new(0)),
            get_fault: Some(get_fault),
            bounded_latest_get_seen: AtomicBool::new(false),
        }
    }

    fn should_lose_ack(&self, location: &object_store::path::Path, mode: &PutMode) -> bool {
        let matches = match self.lost_ack {
            Some(LostAckTarget::LatestCreate) => {
                location.as_ref().ends_with("manifests/latest.json")
                    && matches!(mode, PutMode::Create)
            }
            Some(LostAckTarget::LatestUpdate) => {
                location.as_ref().ends_with("manifests/latest.json")
                    && matches!(mode, PutMode::Update(_))
            }
            Some(LostAckTarget::ManifestUpdate) => {
                location.as_ref().contains("manifests/manifest-")
                    && matches!(mode, PutMode::Update(_))
            }
            None => false,
        };
        matches
            && self
                .lost_ack_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
    }

    fn reset(&self) {
        self.manifest_gets.store(0, Ordering::Relaxed);
        self.state_gets.store(0, Ordering::Relaxed);
    }

    fn counts(&self) -> (usize, usize) {
        (
            self.manifest_gets.load(Ordering::Relaxed),
            self.state_gets.load(Ordering::Relaxed),
        )
    }
}

impl std::fmt::Display for GetCountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GetCountingStore")
    }
}

#[async_trait]
impl ObjectStore for GetCountingStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let lose_ack = self.should_lose_ack(location, &options.mode);
        let result = self.inner.put_opts(location, payload, options).await?;
        if lose_ack {
            return Err(object_store::Error::Generic {
                store: "lost-ack-test",
                source: Box::new(std::io::Error::other(
                    "injected response loss after successful conditional write",
                )),
            });
        }
        Ok(result)
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
        options: GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let path = location.as_ref();
        if path.contains("manifests/manifest-") {
            self.manifest_gets.fetch_add(1, Ordering::Relaxed);
        } else if path.contains("checkpoints/state-") {
            self.state_gets.fetch_add(1, Ordering::Relaxed);
        }
        let is_latest = path.ends_with("manifests/latest.json");
        let is_state = path.contains("checkpoints/state-");
        if is_latest
            && matches!(
                options.range.as_ref(),
                Some(GetRange::Bounded(range))
                    if range.start == 0 && range.end == MAX_LATEST_POINTER_BYTES + 1
            )
        {
            self.bounded_latest_get_seen.store(true, Ordering::Relaxed);
        }
        let is_head = options.head;
        let result = self.inner.get_opts(location, options).await?;
        let Some(fault) = self
            .get_fault
            .filter(|_| is_latest || (is_state && !is_head))
        else {
            return Ok(result);
        };

        use futures::StreamExt;
        let mut meta = result.meta.clone();
        let attributes = result.attributes.clone();
        let bytes = result.bytes().await?;
        let size = bytes.len() as u64;
        let (range, body) = match fault {
            GetFault::MisreportedRange => (1..size + 1, bytes),
            GetFault::MisreportedSize => {
                meta.size = size.saturating_add(1);
                (0..size, bytes)
            }
            GetFault::ShortBody => (0..size, bytes.slice(..bytes.len().saturating_sub(1))),
            GetFault::LongBody => {
                let mut body = BytesMut::from(bytes.as_ref());
                body.extend_from_slice(b"x");
                (0..size, body.freeze())
            }
            GetFault::MissingVersion => {
                meta.e_tag = None;
                meta.version = None;
                (0..size, bytes)
            }
        };
        Ok(object_store::GetResult {
            payload: object_store::GetResultPayload::Stream(
                futures::stream::once(async move { Ok(body) }).boxed(),
            ),
            meta,
            range,
            attributes,
        })
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        use futures::StreamExt;

        let fail_manifest_delete = self.fail_manifest_delete;
        let state_delete_fault = self.state_delete_fault;
        let state_delete_fault_remaining = Arc::clone(&self.state_delete_fault_remaining);
        let inner = Arc::clone(&self.inner);
        locations
            .then(move |result| {
                let inner = Arc::clone(&inner);
                let state_delete_fault_remaining = Arc::clone(&state_delete_fault_remaining);
                async move {
                    let location = result?;
                    if fail_manifest_delete && location.as_ref().contains("manifests/manifest-") {
                        return Err(object_store::Error::Generic {
                            store: "manifest-delete-test",
                            source: Box::new(std::io::Error::other(
                                "injected manifest deletion failure",
                            )),
                        });
                    }

                    let inject_state_fault = state_delete_fault.is_some()
                        && location.as_ref().contains("checkpoints/state-")
                        && state_delete_fault_remaining
                            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                                remaining.checked_sub(1)
                            })
                            .is_ok();
                    if inject_state_fault
                        && matches!(state_delete_fault, Some(StateDeleteFault::HardFailure))
                    {
                        return Err(object_store::Error::Generic {
                            store: "state-delete-test",
                            source: Box::new(std::io::Error::other(
                                "injected state sidecar deletion failure",
                            )),
                        });
                    }

                    inner.delete(&location).await?;
                    if inject_state_fault
                        && matches!(state_delete_fault, Some(StateDeleteFault::LostAck))
                    {
                        return Err(object_store::Error::Generic {
                            store: "state-delete-test",
                            source: Box::new(std::io::Error::other(
                                "injected response loss after state sidecar deletion",
                            )),
                        });
                    }
                    Ok(location)
                }
            })
            .boxed()
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

#[tokio::test]
async fn test_save_and_load_latest() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let m = make_manifest(1);
    store.save(&m).await.unwrap();

    let loaded = store.load_latest().await.unwrap().unwrap();
    assert_eq!(loaded.checkpoint_id, 1);
    assert_eq!(loaded.epoch, 1);
}

#[tokio::test]
async fn prepared_is_invisible_until_finalize() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let prepared = make_prepared_manifest(1);

    let persisted = store.save_with_state(&prepared, None).await.unwrap();
    assert_eq!(persisted.durable_phase, DurableCheckpointPhase::Prepared);
    assert!(store.load_latest().await.unwrap().is_none());
    assert_eq!(store.list_ids().await.unwrap(), vec![1]);

    let finalized = store.finalize(1).await.unwrap();
    assert_eq!(finalized.durable_phase, DurableCheckpointPhase::Finalized);
    assert_eq!(store.load_latest().await.unwrap().unwrap().checkpoint_id, 1);
    std::fs::remove_file(dir.path().join("checkpoints/latest.txt")).unwrap();
    assert_eq!(store.finalize(1).await.unwrap(), finalized);
    assert_eq!(store.load_latest().await.unwrap(), Some(finalized));
}

#[tokio::test]
async fn only_the_exact_prepared_to_finalized_manifest_transition_is_accepted() {
    let prepared = make_prepared_manifest(7);
    let mut changed = make_manifest(7);
    changed.watermark = Some(42);

    let dir = tempfile::tempdir().unwrap();
    let filesystem = FileSystemCheckpointStore::new(dir.path());
    filesystem.save(&prepared).await.unwrap();
    assert!(filesystem.save(&changed).await.is_err());
    assert_eq!(filesystem.load_by_id(7).await.unwrap().unwrap(), prepared);
    filesystem.finalize(7).await.unwrap();

    let object_store = make_obj_store();
    object_store.save(&prepared).await.unwrap();
    assert!(object_store.save(&changed).await.is_err());
    assert_eq!(object_store.load_by_id(7).await.unwrap().unwrap(), prepared);
    object_store.finalize(7).await.unwrap();
}

#[tokio::test]
async fn validated_recovery_skips_newer_prepared_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());
    store.save(&make_manifest(1)).await.unwrap();
    store
        .save_with_state(&make_prepared_manifest(2), None)
        .await
        .unwrap();

    let report = store.recover_latest_validated().await.unwrap();
    assert_eq!(report.chosen_id, Some(1));
    assert_eq!(
        report.skipped,
        vec![(2, "checkpoint is prepared but not finalized".into())]
    );
}

#[tokio::test]
async fn test_load_latest_returns_none_when_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    assert!(store.load_latest().await.unwrap().is_none());
}

#[tokio::test]
async fn test_load_latest_returns_most_recent() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());

    for i in 1..=5 {
        store.save(&make_manifest(i)).await.unwrap();
    }

    let latest = store.load_latest().await.unwrap().unwrap();
    assert_eq!(latest.checkpoint_id, 5);
    assert_eq!(latest.epoch, 5);
}

#[tokio::test]
async fn test_load_by_id() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());

    store.save(&make_manifest(1)).await.unwrap();
    store.save(&make_manifest(2)).await.unwrap();

    let m = store.load_by_id(1).await.unwrap().unwrap();
    assert_eq!(m.epoch, 1);

    let m = store.load_by_id(2).await.unwrap().unwrap();
    assert_eq!(m.epoch, 2);

    assert!(store.load_by_id(99).await.unwrap().is_none());
}

#[tokio::test]
async fn test_list() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());

    store.save(&make_manifest(1)).await.unwrap();
    store.save(&make_manifest(3)).await.unwrap();
    store.save(&make_manifest(2)).await.unwrap();

    let list = store.list().await.unwrap();
    assert_eq!(list, vec![(1, 1), (2, 2), (3, 3)]);
}

#[tokio::test]
async fn filesystem_inventory_bounds_all_entries_and_rejects_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());
    store.save(&make_manifest(1)).await.unwrap();

    let existing_entries = std::fs::read_dir(store.checkpoints_dir()).unwrap().count();
    assert_eq!(
        store
            .sorted_checkpoint_ids_with_limit(existing_entries)
            .await
            .unwrap(),
        vec![1]
    );
    std::fs::write(store.checkpoints_dir().join("unrelated"), b"noise").unwrap();
    let error = store
        .sorted_checkpoint_ids_with_limit(existing_entries)
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains(&format!("{existing_entries}-entry safety limit")));

    let alias_dir = tempfile::tempdir().unwrap();
    let alias_store = FileSystemCheckpointStore::new(alias_dir.path());
    let alias = alias_store.checkpoints_dir().join("checkpoint_01");
    std::fs::create_dir_all(&alias).unwrap();
    std::fs::write(alias.join("manifest.json"), b"{}").unwrap();
    let error = alias_store
        .sorted_checkpoint_ids_with_limit(10)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("non-canonical"));
}

#[tokio::test]
async fn filesystem_inventory_rejects_a_non_regular_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());
    let manifest_path = store.manifest_path(1);
    std::fs::create_dir_all(&manifest_path).unwrap();

    let error = store.list_ids().await.unwrap_err();
    assert!(error.to_string().contains("is not a regular file"));
}

#[tokio::test]
async fn test_save_does_not_run_retention_inline() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());

    for i in 1..=5 {
        store.save(&make_manifest(i)).await.unwrap();
    }

    let list = store.list().await.unwrap();
    assert_eq!(list.len(), 5);
}

#[tokio::test]
async fn epoch_prune_preserves_latest_recovery_cut() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());
    store.save(&make_manifest(1)).await.unwrap();
    for id in 2..=5 {
        store.save(&make_prepared_manifest(id)).await.unwrap();
    }

    assert_eq!(store.prune_before(10).await.unwrap(), 4);
    assert_eq!(store.list_ids().await.unwrap(), vec![1]);
    assert_eq!(store.load_latest().await.unwrap().unwrap().checkpoint_id, 1);
}

#[tokio::test]
async fn test_save_and_load_state_data() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    store.save(&make_manifest(1)).await.unwrap();

    let data = b"large operator state binary blob";
    store
        .save_state_data(1, &[bytes::Bytes::from_static(data)])
        .await
        .unwrap();

    let loaded = store.load_state_data(1).await.unwrap().unwrap();
    assert_eq!(loaded, data);
}

#[tokio::test]
async fn test_load_state_data_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    assert!(store.load_state_data(99).await.unwrap().is_none());
}

#[tokio::test]
async fn filesystem_state_budget_is_exact_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path())
        .with_max_state_data_bytes(8)
        .unwrap();
    let exact = [
        bytes::Bytes::from_static(b"1234"),
        bytes::Bytes::from_static(b"5678"),
    ];

    store.save_state_data(1, &exact).await.unwrap();
    assert_eq!(
        store.load_state_data(1).await.unwrap().unwrap(),
        b"12345678"
    );
    assert_eq!(
        store.state_data_len_for_participant(0, 1).await.unwrap(),
        Some(8)
    );
    assert!(matches!(
        store
            .save_state_data(2, &[bytes::Bytes::from_static(b"123456789")])
            .await,
        Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 8-byte safety limit")
    ));
    assert!(!store.state_path(2).exists());

    let lowered = FileSystemCheckpointStore::new(dir.path())
        .with_max_state_data_bytes(7)
        .unwrap();
    assert!(matches!(
        lowered.load_state_data(1).await,
        Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 7-byte safety limit")
    ));
    assert!(matches!(
        lowered.state_data_len_for_participant(0, 1).await,
        Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 7-byte safety limit")
    ));
}

#[tokio::test]
async fn filesystem_state_read_rejects_non_regular_and_oversized_sidecars() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path()).with_max_state_data_bytes(8).unwrap();

    let non_regular = store.state_path(1);
    assert!(matches!(
        store
            .save_state_data(1, &[bytes::Bytes::from_static(b"123456789")])
            .await,
        Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 8-byte safety limit")
    ));
    assert!(!non_regular.exists());

    std::fs::create_dir_all(&non_regular).unwrap();
    assert!(matches!(
        store.load_state_data(1).await,
        Err(CheckpointStoreError::Invalid(error)) if error.contains("not a regular file")
    ));
    assert!(matches!(
        store.state_data_len_for_participant(0, 1).await,
        Err(CheckpointStoreError::Invalid(error)) if error.contains("not a regular file")
    ));

    std::fs::remove_dir(&non_regular).unwrap();
    std::fs::write(&non_regular, b"123456789").unwrap();
    assert!(matches!(
        store.load_state_data(1).await,
        Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 8-byte safety limit")
    ));
    assert!(matches!(
        store.state_data_len_for_participant(0, 1).await,
        Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 8-byte safety limit")
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn filesystem_state_read_rejects_a_symlink_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let state_path = store.state_path(1);
    std::fs::create_dir_all(state_path.parent().unwrap()).unwrap();
    let target = dir.path().join("outside-state.bin");
    std::fs::write(&target, b"outside").unwrap();
    std::os::unix::fs::symlink(&target, &state_path).unwrap();

    assert!(matches!(
        store.load_state_data(1).await,
        Err(CheckpointStoreError::Invalid(error)) if error.contains("not a regular file")
    ));
    assert!(matches!(
        store.state_data_len_for_participant(0, 1).await,
        Err(CheckpointStoreError::Invalid(error)) if error.contains("not a regular file")
    ));
}

#[test]
fn bounded_state_reader_rejects_body_growth_and_truncation() {
    use std::io::{Seek, SeekFrom};

    let dir = tempfile::tempdir().unwrap();
    let growing = dir.path().join("growing.bin");
    std::fs::write(&growing, b"1234").unwrap();
    let mut growing_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&growing)
        .unwrap();
    growing_file.seek(SeekFrom::End(0)).unwrap();
    growing_file.write_all(b"56789012").unwrap();
    growing_file.seek(SeekFrom::Start(0)).unwrap();
    assert!(matches!(
        read_bounded_open_file(&mut growing_file, 4, 8, "test sidecar"),
        Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 8-byte safety limit")
    ));

    let exact_limit = dir.path().join("exact-limit.bin");
    std::fs::write(&exact_limit, b"12345678").unwrap();
    let mut exact_limit_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&exact_limit)
        .unwrap();
    exact_limit_file.seek(SeekFrom::End(0)).unwrap();
    exact_limit_file.write_all(b"9").unwrap();
    exact_limit_file.seek(SeekFrom::Start(0)).unwrap();
    assert!(matches!(
        read_bounded_open_file(&mut exact_limit_file, 8, 8, "test sidecar"),
        Err(CheckpointStoreError::Invalid(error)) if error.contains("is 9 bytes, exceeding the 8-byte safety limit")
    ));

    let truncating = dir.path().join("truncating.bin");
    std::fs::write(&truncating, b"12345678").unwrap();
    let mut truncating_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&truncating)
        .unwrap();
    truncating_file.set_len(3).unwrap();
    truncating_file.seek(SeekFrom::Start(0)).unwrap();
    assert!(matches!(
        read_bounded_open_file(&mut truncating_file, 8, 8, "test sidecar"),
        Err(CheckpointStoreError::Invalid(error)) if error.contains("body length changed from 8 to 3 bytes")
    ));
}

#[tokio::test]
async fn participant_state_read_delegates_to_local_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path()).with_participant_id(11);
    let mut manifest = make_manifest(7);
    manifest.participant_id = 11;
    declare_external_state(&mut manifest, b"local-state".len());
    store
        .save_with_state(
            &manifest,
            Some(&[bytes::Bytes::from_static(b"local-state")]),
        )
        .await
        .unwrap();

    assert_eq!(
        store
            .load_state_data_for_participant(11, 7)
            .await
            .unwrap()
            .unwrap(),
        b"local-state"
    );
    let artifacts = store
        .load_checkpoint_artifacts_for_participant(11, 7)
        .await
        .unwrap()
        .unwrap();
    let (_, validation) = artifacts
        .validate(7, 11, store.key_group_count(), store.max_state_data_bytes())
        .await
        .unwrap();
    assert!(validation.valid);
}

#[tokio::test]
async fn filesystem_rejects_foreign_participant_state_read() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path()).with_participant_id(11);

    let error = store
        .load_state_data_for_participant(22, 7)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "invalid checkpoint: checkpoint store participant 11 cannot read participant 22"
    );

    let error = store
        .load_checkpoint_artifacts_for_participant(22, 7)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "invalid checkpoint: checkpoint store participant 11 cannot read participant 22"
    );
}

#[tokio::test]
async fn test_full_manifest_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let mut m = make_manifest(1);
    m.source_offsets.insert(
        "kafka-src".into(),
        ConnectorCheckpoint::with_offsets(HashMap::from([
            ("events:0".into(), "1000".into()),
            ("events:1".into(), "2000".into()),
        ])),
    );
    m.table_offsets.insert(
        "instruments".into(),
        ConnectorCheckpoint::with_offsets(HashMap::from([("lsn".into(), "0/AB".into())])),
    );
    m.operator_states
        .insert("window".into(), OperatorCheckpoint::inline(b"data"));
    m.watermark = Some(999_000);

    store.save_with_state(&m, None).await.unwrap();

    let loaded = store.load_latest().await.unwrap().unwrap();
    assert_eq!(loaded.checkpoint_id, 1);
    assert_eq!(loaded.epoch, 1);
    assert_eq!(loaded.watermark, Some(999_000));

    let src = loaded.source_offsets.get("kafka-src").unwrap();
    assert_eq!(src.offsets.get("events:0"), Some(&"1000".into()));

    let tbl = loaded.table_offsets.get("instruments").unwrap();
    assert_eq!(tbl.offsets.get("lsn"), Some(&"0/AB".into()));

    let op = loaded.operator_states.get("window").unwrap();
    assert_eq!(op.try_decode_inline().unwrap().unwrap(), b"data");
}

#[tokio::test]
async fn test_empty_latest_txt_is_invalid() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let cp_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&cp_dir).unwrap();
    std::fs::write(cp_dir.join("latest.txt"), "").unwrap();

    let error = store.load_latest().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("invalid checkpoint recovery pointer"));
}

#[tokio::test]
async fn test_latest_points_to_missing_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let cp_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&cp_dir).unwrap();
    std::fs::write(cp_dir.join("latest.txt"), "checkpoint_000099").unwrap();

    let error = store.load_latest().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("recovery pointer references missing checkpoint 99"));
}

#[tokio::test]
async fn test_save_with_state_writes_sidecar_before_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let state = b"large-operator-state-blob";
    let m = make_external_manifest(1, state.len());
    store
        .save_with_state(&m, Some(&[bytes::Bytes::from_static(state)]))
        .await
        .unwrap();

    // Both manifest and state should be present.
    let loaded = store.load_latest().await.unwrap().unwrap();
    assert_eq!(loaded.checkpoint_id, 1);

    let loaded_state = store.load_state_data(1).await.unwrap().unwrap();
    assert_eq!(loaded_state, state);
}

#[tokio::test]
async fn filesystem_save_establishes_nested_publication_directories() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("missing").join("checkpoint-root");
    let store = make_store(&base);
    let state = b"durable-state";

    store
        .save_with_state(
            &make_external_manifest(1, state.len()),
            Some(&[bytes::Bytes::from_static(state)]),
        )
        .await
        .unwrap();

    let checkpoint = base.join("checkpoints").join("checkpoint_000001");
    assert_eq!(std::fs::read(checkpoint.join("state.bin")).unwrap(), state);
    assert!(checkpoint.join("manifest.json").is_file());
    assert_eq!(
        std::fs::read_to_string(base.join("checkpoints").join("latest.txt")).unwrap(),
        "checkpoint_000001"
    );
    assert!(!checkpoint.join("state.bin.tmp").exists());
    assert!(!checkpoint.join("manifest.json.tmp").exists());
    assert!(!base.join("checkpoints").join("latest.txt.tmp").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_filesystem_save_cannot_regress_a_later_pointer_update() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(FilesystemLatestPublicationGate::default());
    let delayed =
        FileSystemCheckpointStore::new(dir.path()).with_latest_publication_gate(Arc::clone(&gate));
    let delayed_task = tokio::spawn(async move { delayed.save(&make_manifest(5)).await });

    assert!(gate.wait_until_entered(std::time::Duration::from_secs(5)));
    delayed_task.abort();
    let _ = delayed_task.await;

    let base = dir.path().to_path_buf();
    let later = tokio::spawn(async move {
        FileSystemCheckpointStore::new(base)
            .save(&make_manifest(10))
            .await
    });
    gate.release();
    later.await.unwrap().unwrap();

    let loaded = FileSystemCheckpointStore::new(dir.path())
        .load_latest()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.checkpoint_id, 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_filesystem_state_publication_has_one_immutable_winner() {
    let dir = tempfile::tempdir().unwrap();
    let base_a = dir.path().to_path_buf();
    let base_b = base_a.clone();
    let first = tokio::spawn(async move {
        FileSystemCheckpointStore::new(base_a)
            .save_with_state(
                &make_external_manifest(7, b"first".len()),
                Some(&[bytes::Bytes::from_static(b"first")]),
            )
            .await
    });
    let second = tokio::spawn(async move {
        FileSystemCheckpointStore::new(base_b)
            .save_with_state(
                &make_external_manifest(7, b"second".len()),
                Some(&[bytes::Bytes::from_static(b"second")]),
            )
            .await
    });
    let (first, second) = tokio::join!(first, second);
    let results = [first.unwrap(), second.unwrap()];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);

    let store = FileSystemCheckpointStore::new(dir.path());
    let latest = store.load_latest().await.unwrap().unwrap();
    let state = store.load_state_data(7).await.unwrap().unwrap();
    assert_eq!(latest.checkpoint_id, 7);
    assert!(state == b"first" || state == b"second");
    assert!(store.validate_checkpoint(7).await.unwrap().valid);
}

#[tokio::test]
async fn filesystem_public_loads_reject_oversize_and_misrouted_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());
    let checkpoint_dir = store.checkpoint_dir(7);
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let manifest_path = store.manifest_path(7);
    let wrong = make_manifest(8);
    std::fs::write(&manifest_path, serde_json::to_vec(&wrong).unwrap()).unwrap();
    let error = store.load_by_id(7).await.unwrap_err();
    assert!(error
        .to_string()
        .contains("storage checkpoint 7 contains manifest checkpoint 8"));
    assert!(store.list().await.is_err());

    let oversized = File::create(&manifest_path).unwrap();
    oversized.set_len(MAX_MANIFEST_BYTES + 1).unwrap();
    let error = store.load_by_id(7).await.unwrap_err();
    assert!(error.to_string().contains("exceeding the"));

    std::fs::create_dir_all(store.checkpoints_dir()).unwrap();
    let pointer = File::create(store.latest_path()).unwrap();
    pointer.set_len(MAX_LATEST_POINTER_BYTES + 1).unwrap();
    let error = store.load_latest().await.unwrap_err();
    assert!(error.to_string().contains("exceeding the"));
}

#[tokio::test]
async fn test_save_with_state_none_is_same_as_save() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let m = make_manifest(1);
    store.save_with_state(&m, None).await.unwrap();

    let loaded = store.load_latest().await.unwrap().unwrap();
    assert_eq!(loaded.checkpoint_id, 1);
    assert!(store.load_state_data(1).await.unwrap().is_none());
}

#[tokio::test]
async fn test_orphaned_state_without_manifest_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    // Write only sidecar state, no manifest (simulates crash after
    // state write but before manifest write).
    store
        .save_state_data(1, &[bytes::Bytes::from_static(b"orphaned")])
        .await
        .unwrap();

    // load_latest should return None — the orphan is not visible.
    assert!(store.load_latest().await.unwrap().is_none());

    // list should not include the orphan (no manifest.json).
    assert!(store.list().await.unwrap().is_empty());
}

// -----------------------------------------------------------------------
// ObjectStoreCheckpointStore tests (using InMemory backend)
// -----------------------------------------------------------------------

fn make_obj_store() -> ObjectStoreCheckpointStore {
    let store = Arc::new(object_store::memory::InMemory::new());
    ObjectStoreCheckpointStore::new(store, String::new())
}

#[tokio::test]
async fn conditional_put_probes_are_capability_specific_and_clean_up() {
    use futures::TryStreamExt;

    let memory = Arc::new(object_store::memory::InMemory::new());
    let timeout = std::time::Duration::from_secs(1);
    probe_object_store_conditional_create(memory.as_ref(), "test/", timeout)
        .await
        .unwrap();
    probe_object_store_conditional_update(memory.as_ref(), "test/", timeout)
        .await
        .unwrap();
    assert!(memory.list(None).try_next().await.unwrap().is_none());

    let directory = tempfile::tempdir().unwrap();
    let local = object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap();
    probe_object_store_conditional_create(&local, "test/", timeout)
        .await
        .unwrap();
    let error = probe_object_store_conditional_update(&local, "test/", timeout)
        .await
        .expect_err("local filesystems do not offer token-based conditional update");
    assert!(error.to_string().contains("PutMode::Update"), "{error}");
    assert!(local.list(None).try_next().await.unwrap().is_none());
}

#[tokio::test]
async fn test_obj_save_and_load_latest() {
    let store = make_obj_store();
    let m = make_manifest(1);
    store.save(&m).await.unwrap();

    let loaded = store.load_latest().await.unwrap().unwrap();
    assert_eq!(loaded.checkpoint_id, 1);
    assert_eq!(loaded.epoch, 1);
}

#[tokio::test]
async fn test_obj_load_latest_returns_none_when_empty() {
    let store = make_obj_store();
    assert!(store.load_latest().await.unwrap().is_none());
}

#[tokio::test]
async fn test_obj_load_by_id() {
    let store = ObjectStoreCheckpointStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        String::new(),
    );

    store.save(&make_manifest(1)).await.unwrap();
    store.save(&make_manifest(2)).await.unwrap();

    let m = store.load_by_id(1).await.unwrap().unwrap();
    assert_eq!(m.epoch, 1);
    let m = store.load_by_id(2).await.unwrap().unwrap();
    assert_eq!(m.epoch, 2);
    assert!(store.load_by_id(99).await.unwrap().is_none());
}

#[tokio::test]
async fn object_store_manifest_is_immutable_and_idempotent() {
    let store = make_obj_store();
    let manifest = make_manifest(7);

    store.save(&manifest).await.unwrap();
    store.save(&manifest).await.unwrap();

    let mut conflicting = manifest.clone();
    conflicting.watermark = Some(6);
    let error = store
        .save(&conflicting)
        .await
        .expect_err("one checkpoint ID cannot name two manifests");
    assert!(error.to_string().contains("different immutable content"));
    assert_eq!(store.load_by_id(7).await.unwrap().unwrap(), manifest);
}

#[tokio::test]
async fn test_obj_list() {
    let store = ObjectStoreCheckpointStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        String::new(),
    );

    store.save(&make_manifest(1)).await.unwrap();
    store.save(&make_manifest(3)).await.unwrap();
    store.save(&make_manifest(2)).await.unwrap();

    let list = store.list().await.unwrap();
    assert_eq!(list, vec![(1, 1), (2, 2), (3, 3)]);
}

#[tokio::test]
async fn object_inventory_bounds_all_entries_and_rejects_aliases() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());
    store.save(&make_manifest(1)).await.unwrap();

    assert_eq!(
        store.list_checkpoint_ids_with_limit(2).await.unwrap(),
        vec![1]
    );
    inner
        .put_opts(
            &object_store::path::Path::from("manifests/unrelated"),
            PutPayload::from_bytes(bytes::Bytes::from_static(b"noise")),
            PutOptions::default(),
        )
        .await
        .unwrap();
    let error = store.list_checkpoint_ids_with_limit(2).await.unwrap_err();
    assert!(error.to_string().contains("2-entry safety limit"));

    let alias_inner = Arc::new(object_store::memory::InMemory::new());
    let alias_store = ObjectStoreCheckpointStore::new(alias_inner.clone(), String::new());
    alias_inner
        .put_opts(
            &object_store::path::Path::from("manifests/manifest-01.json"),
            PutPayload::from_bytes(bytes::Bytes::from_static(b"{}")),
            PutOptions::default(),
        )
        .await
        .unwrap();
    let error = alias_store
        .list_checkpoint_ids_with_limit(10)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("non-canonical"));
}

#[tokio::test]
async fn test_obj_save_does_not_run_retention_inline() {
    let store = ObjectStoreCheckpointStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        String::new(),
    );

    for i in 1..=5 {
        store.save(&make_manifest(i)).await.unwrap();
    }

    let list = store.list().await.unwrap();
    assert_eq!(list.len(), 5);
}

#[tokio::test]
async fn obj_epoch_prune_preserves_latest_recovery_cut() {
    let store = ObjectStoreCheckpointStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        String::new(),
    );
    store.save(&make_manifest(1)).await.unwrap();
    for id in 2..=5 {
        store.save(&make_prepared_manifest(id)).await.unwrap();
    }

    assert_eq!(store.prune_before(10).await.unwrap(), 4);
    assert_eq!(store.list_ids().await.unwrap(), vec![1]);
    assert_eq!(store.load_latest().await.unwrap().unwrap().checkpoint_id, 1);
}

#[tokio::test]
async fn object_prune_keeps_inventory_when_manifest_delete_fails() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let faulting = Arc::new(GetCountingStore::with_manifest_delete_failure(
        inner.clone(),
    ));
    let store = ObjectStoreCheckpointStore::new(faulting, String::new());

    let state = [bytes::Bytes::from_static(b"state-must-survive")];
    store
        .save_with_state(&make_external_manifest(1, state[0].len()), Some(&state))
        .await
        .unwrap();
    store.save(&make_manifest(2)).await.unwrap();

    let error = store.prune_before(2).await.unwrap_err();
    assert!(error.to_string().contains("manifest deletion failure"));
    assert!(store.load_by_id(1).await.unwrap().is_some());
    assert!(store.load_state_data(1).await.unwrap().is_none());
}

#[tokio::test]
async fn object_prune_retries_state_delete_failure_from_manifest_inventory() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let faulting = Arc::new(GetCountingStore::with_state_delete_fault(
        inner,
        StateDeleteFault::HardFailure,
    ));
    let store = ObjectStoreCheckpointStore::new(faulting, String::new());

    let state = [bytes::Bytes::from_static(b"state-must-survive")];
    store
        .save_with_state(&make_external_manifest(1, state[0].len()), Some(&state))
        .await
        .unwrap();
    store.save(&make_manifest(2)).await.unwrap();

    let error = store.prune_before(2).await.unwrap_err();
    assert!(error.to_string().contains("state sidecar deletion failure"));
    assert!(store.load_by_id(1).await.unwrap().is_some());
    assert_eq!(
        store.load_state_data(1).await.unwrap().as_deref(),
        Some(b"state-must-survive".as_slice())
    );

    assert_eq!(store.prune_before(2).await.unwrap(), 1);
    assert!(store.load_by_id(1).await.unwrap().is_none());
    assert!(store.load_state_data(1).await.unwrap().is_none());
}

#[tokio::test]
async fn object_prune_retries_lost_state_delete_ack_without_orphaning_state() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let faulting = Arc::new(GetCountingStore::with_state_delete_fault(
        inner,
        StateDeleteFault::LostAck,
    ));
    let store = ObjectStoreCheckpointStore::new(faulting, String::new());

    let state = [bytes::Bytes::from_static(b"state-deleted-before-ack-loss")];
    store
        .save_with_state(&make_external_manifest(1, state[0].len()), Some(&state))
        .await
        .unwrap();
    store.save(&make_manifest(2)).await.unwrap();

    let error = store.prune_before(2).await.unwrap_err();
    assert!(error
        .to_string()
        .contains("response loss after state sidecar deletion"));
    assert!(store.load_by_id(1).await.unwrap().is_some());
    assert!(store.load_state_data(1).await.unwrap().is_none());

    assert_eq!(store.prune_before(2).await.unwrap(), 1);
    assert!(store.load_by_id(1).await.unwrap().is_none());
    assert!(store.load_state_data(1).await.unwrap().is_none());
}

#[tokio::test]
async fn test_obj_save_and_load_state_data() {
    let store = make_obj_store();
    store.save(&make_manifest(1)).await.unwrap();

    let data = b"large operator state binary blob";
    store
        .save_state_data(1, &[bytes::Bytes::from_static(data)])
        .await
        .unwrap();

    let loaded = store.load_state_data(1).await.unwrap().unwrap();
    assert_eq!(loaded, data);
}

#[tokio::test]
async fn object_store_state_sidecar_is_immutable_and_idempotent() {
    let store = make_obj_store();
    let original = [bytes::Bytes::from_static(b"state-blob")];

    store.save_state_data(1, &original).await.unwrap();
    store.save_state_data(1, &original).await.unwrap();

    let error = store
        .save_state_data(1, &[bytes::Bytes::from_static(b"different")])
        .await
        .expect_err("one checkpoint ID cannot overwrite its state sidecar");
    assert!(error.to_string().contains("different immutable content"));
    assert_eq!(
        store.load_state_data(1).await.unwrap().unwrap(),
        b"state-blob"
    );
}

#[tokio::test]
async fn test_obj_load_state_data_returns_none() {
    let store = make_obj_store();
    assert!(store.load_state_data(99).await.unwrap().is_none());
}

#[tokio::test]
async fn object_state_budget_is_exact_and_survives_restart() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new())
        .with_max_state_data_bytes(8)
        .unwrap();
    let exact = [
        bytes::Bytes::from_static(b"1234"),
        bytes::Bytes::from_static(b"5678"),
    ];

    store.save_state_data(1, &exact).await.unwrap();
    assert_eq!(
        store.load_state_data(1).await.unwrap().unwrap(),
        b"12345678"
    );
    assert_eq!(
        store.state_data_len_for_participant(0, 1).await.unwrap(),
        Some(8)
    );
    assert!(matches!(
        store
            .save_state_data(2, &[bytes::Bytes::from_static(b"123456789")])
            .await,
        Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 8-byte safety limit")
    ));
    assert!(inner.head(&store.state_path(2)).await.is_err());

    let lowered = ObjectStoreCheckpointStore::new(inner, String::new())
        .with_max_state_data_bytes(7)
        .unwrap();
    assert!(matches!(
        lowered.load_state_data(1).await,
        Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 7-byte safety limit")
    ));
    assert!(matches!(
        lowered.state_data_len_for_participant(0, 1).await,
        Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 7-byte safety limit")
    ));
}

#[tokio::test]
async fn bounded_object_state_reader_rejects_metadata_and_body_faults() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let path = object_store::path::Path::from("checkpoints/state-000001.bin");
    inner
        .put_opts(
            &path,
            PutPayload::from_bytes(bytes::Bytes::from_static(b"12345678")),
            PutOptions::default(),
        )
        .await
        .unwrap();

    for (fault, limit, expected) in [
        (GetFault::MisreportedSize, 16, "length changed"),
        (GetFault::ShortBody, 16, "body length"),
        (GetFault::LongBody, 16, "body exceeded"),
        (GetFault::LongBody, 8, "exceeding the 8-byte safety limit"),
    ] {
        let faulting = Arc::new(GetCountingStore::with_get_fault(inner.clone(), fault));
        let store = ObjectStoreCheckpointStore::new(faulting, String::new())
            .with_max_state_data_bytes(limit)
            .unwrap();
        let error = store.load_state_data(1).await.unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "fault {fault:?}: {error}"
        );
    }
}

#[tokio::test]
async fn test_obj_with_prefix() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(inner, "nodes/abc123/".to_string());

    store.save(&make_manifest(1)).await.unwrap();
    let loaded = store.load_latest().await.unwrap().unwrap();
    assert_eq!(loaded.checkpoint_id, 1);
    assert_eq!(loaded.epoch, 1);
}

#[tokio::test]
async fn test_obj_participant_namespaces_are_isolated() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let participant_11 =
        ObjectStoreCheckpointStore::new(inner.clone(), "participants/11/".to_string())
            .with_participant_id(11);
    let participant_22 = ObjectStoreCheckpointStore::new(inner, "participants/22/".to_string())
        .with_participant_id(22);

    let mut manifest_11 = make_manifest(7);
    manifest_11.participant_id = 11;
    let mut manifest_22 = make_manifest(7);
    manifest_22.participant_id = 22;

    participant_11.save(&manifest_11).await.unwrap();
    participant_22.save(&manifest_22).await.unwrap();

    let loaded_11 = participant_11.load_by_id(7).await.unwrap().unwrap();
    let loaded_22 = participant_22.load_by_id(7).await.unwrap().unwrap();
    assert_eq!((loaded_11.participant_id, loaded_11.epoch), (11, 7));
    assert_eq!((loaded_22.participant_id, loaded_22.epoch), (22, 7));
    assert_eq!(participant_11.list().await.unwrap(), vec![(7, 7)]);
    assert_eq!(participant_22.list().await.unwrap(), vec![(7, 7)]);
}

#[tokio::test]
async fn object_store_reads_and_validates_peer_participant_state() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let participant_11 = ObjectStoreCheckpointStore::new(inner.clone(), "nodes/11/".to_string())
        .with_participant_id(11);
    let participant_22 = ObjectStoreCheckpointStore::new(inner.clone(), "nodes/22/".to_string())
        .with_participant_id(22);
    let mut manifest = make_manifest(7);
    manifest.participant_id = 22;
    declare_external_state(&mut manifest, b"peer-state".len());
    participant_22
        .save_with_state(&manifest, Some(&[bytes::Bytes::from_static(b"peer-state")]))
        .await
        .unwrap();

    assert_eq!(
        participant_11
            .load_state_data_for_participant(22, 7)
            .await
            .unwrap()
            .unwrap(),
        b"peer-state"
    );
    assert!(participant_11
        .load_state_data_for_participant(22, 8)
        .await
        .unwrap()
        .is_none());
    let artifacts = participant_11
        .load_checkpoint_artifacts_for_participant(22, 7)
        .await
        .unwrap()
        .unwrap();
    let (_, validation) = artifacts
        .validate(
            7,
            22,
            participant_11.key_group_count(),
            participant_11.max_state_data_bytes(),
        )
        .await
        .unwrap();
    assert!(validation.valid);

    inner
        .put_opts(
            &participant_22.state_path(7),
            PutPayload::from_bytes(bytes::Bytes::from_static(b"other-data")),
            PutOptions::default(),
        )
        .await
        .unwrap();

    let artifacts = participant_11
        .load_checkpoint_artifacts_for_participant(22, 7)
        .await
        .unwrap()
        .unwrap();
    let (_, validation) = artifacts
        .validate(
            7,
            22,
            participant_11.key_group_count(),
            participant_11.max_state_data_bytes(),
        )
        .await
        .unwrap();
    assert!(!validation.valid);
    assert!(validation
        .issues
        .iter()
        .any(|issue| issue.message().contains("state.bin checksum mismatch")));
}

#[tokio::test]
async fn external_artifact_load_reads_manifest_and_sidecar_once_and_rejects_tamper() {
    let raw = Arc::new(object_store::memory::InMemory::new());
    let raw_store: Arc<dyn ObjectStore> = raw.clone();
    let counting = Arc::new(GetCountingStore::new(raw_store));
    let counted_store: Arc<dyn ObjectStore> = counting.clone();
    let reader =
        ObjectStoreCheckpointStore::new(Arc::clone(&counted_store), "nodes/11/".to_string())
            .with_participant_id(11);
    let writer = ObjectStoreCheckpointStore::new(counted_store, "nodes/22/".to_string())
        .with_participant_id(22);
    let state = b"peer-external-state";
    let mut manifest = make_manifest(7);
    manifest.participant_id = 22;
    manifest.operator_states.insert(
        "external".into(),
        OperatorCheckpoint::external(0, state.len() as u64),
    );
    writer
        .save_with_state(&manifest, Some(&[bytes::Bytes::from_static(state)]))
        .await
        .unwrap();

    counting.reset();
    let artifacts = reader
        .load_checkpoint_artifacts_for_participant(22, 7)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(artifacts.state_data.as_deref(), Some(state.as_slice()));
    let (_, validation) = artifacts
        .validate(
            7,
            22,
            reader.key_group_count(),
            reader.max_state_data_bytes(),
        )
        .await
        .unwrap();
    assert!(validation.valid);
    assert_eq!(counting.counts(), (1, 2));

    raw.put_opts(
        &writer.state_path(7),
        PutPayload::from_bytes(bytes::Bytes::from_static(b"evil-external-state")),
        PutOptions::default(),
    )
    .await
    .unwrap();
    counting.reset();
    let artifacts = reader
        .load_checkpoint_artifacts_for_participant(22, 7)
        .await
        .unwrap()
        .unwrap();
    let (_, validation) = artifacts
        .validate(
            7,
            22,
            reader.key_group_count(),
            reader.max_state_data_bytes(),
        )
        .await
        .unwrap();
    assert!(!validation.valid);
    assert!(validation
        .issues
        .iter()
        .any(|issue| issue.message().contains("state.bin checksum mismatch")));
    assert_eq!(counting.counts(), (1, 2));
}

#[tokio::test]
async fn inline_validation_reads_manifest_once_and_never_reads_sidecar() {
    let raw = Arc::new(object_store::memory::InMemory::new());
    let raw_store: Arc<dyn ObjectStore> = raw.clone();
    let counting = Arc::new(GetCountingStore::new(raw_store));
    let counted_store: Arc<dyn ObjectStore> = counting.clone();
    let reader =
        ObjectStoreCheckpointStore::new(Arc::clone(&counted_store), "nodes/11/".to_string())
            .with_participant_id(11);
    let writer = ObjectStoreCheckpointStore::new(counted_store, "nodes/22/".to_string())
        .with_participant_id(22);
    let mut manifest = make_manifest(8);
    manifest.participant_id = 22;
    manifest
        .operator_states
        .insert("inline".into(), OperatorCheckpoint::inline(b"inline-state"));
    let mut persisted = writer.save_with_state(&manifest, None).await.unwrap();

    counting.reset();
    let artifacts = reader
        .load_checkpoint_artifacts_for_participant(22, 8)
        .await
        .unwrap()
        .unwrap();
    let (_, validation) = artifacts
        .validate(
            8,
            22,
            reader.key_group_count(),
            reader.max_state_data_bytes(),
        )
        .await
        .unwrap();
    assert!(validation.valid, "inline artifact: {:?}", validation.issues);
    assert_eq!(counting.counts(), (1, 0));

    persisted.operator_states.insert(
        "inline".into(),
        OperatorCheckpoint::inline(b"tampered-inline-state"),
    );
    raw.put_opts(
        &writer.manifest_path(8),
        PutPayload::from_bytes(bytes::Bytes::from(
            serde_json::to_vec_pretty(&persisted).unwrap(),
        )),
        PutOptions::default(),
    )
    .await
    .unwrap();
    counting.reset();
    let artifacts = reader
        .load_checkpoint_artifacts_for_participant(22, 8)
        .await
        .unwrap()
        .unwrap();
    let (_, validation) = artifacts
        .validate(
            8,
            22,
            reader.key_group_count(),
            reader.max_state_data_bytes(),
        )
        .await
        .unwrap();
    assert!(!validation.valid);
    assert!(validation
        .issues
        .iter()
        .any(|issue| issue.message().contains("inline state checksum mismatch")));
    assert_eq!(counting.counts(), (1, 0));
}

#[tokio::test]
async fn test_obj_rejects_manifest_for_wrong_participant() {
    let store = ObjectStoreCheckpointStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        "participants/11/".to_string(),
    )
    .with_participant_id(11);
    let mut manifest = make_manifest(7);
    manifest.participant_id = 22;

    let error = store.save(&manifest).await.unwrap_err();
    assert!(matches!(error, CheckpointStoreError::Invalid(_)));
    assert_eq!(
        error.to_string(),
        "invalid checkpoint: manifest participant 22 does not match store participant 11"
    );
    assert!(store.list_ids().await.unwrap().is_empty());
}

// -----------------------------------------------------------------------
// Object-store layout and conditional-publication tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_obj_layout_paths() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());

    store.save(&make_manifest(1)).await.unwrap();

    let result = inner
        .get_opts(
            &object_store::path::Path::from("manifests/manifest-000001.json"),
            GetOptions::default(),
        )
        .await;
    assert!(result.is_ok(), "manifest path should exist");

    let result = inner
        .get_opts(
            &object_store::path::Path::from("manifests/latest.json"),
            GetOptions::default(),
        )
        .await;
    assert!(result.is_ok(), "latest.json should exist");
}

#[tokio::test]
async fn test_obj_conditional_put_idempotent() {
    let store = ObjectStoreCheckpointStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        String::new(),
    );

    let m = make_manifest(1);
    store.save(&m).await.unwrap();

    // Second save with same ID should succeed (logs warning, skips write)
    store.save(&m).await.unwrap();

    let loaded = store.load_latest().await.unwrap().unwrap();
    assert_eq!(loaded.checkpoint_id, 1);
    assert_eq!(loaded.epoch, 1);
}

#[tokio::test]
async fn object_store_finalize_only_publishes_the_stored_prepared_manifest() {
    let store = make_obj_store();
    let prepared = make_prepared_manifest(7);
    store.save_with_state(&prepared, None).await.unwrap();
    assert!(store.load_latest().await.unwrap().is_none());

    let finalized = store.finalize(7).await.unwrap();

    assert_eq!(finalized.durable_phase, DurableCheckpointPhase::Finalized);
    assert_eq!(store.load_latest().await.unwrap(), Some(finalized.clone()));
    store
        .store
        .delete(&store.latest_pointer_path())
        .await
        .unwrap();
    assert!(store.load_latest().await.unwrap().is_none());
    assert_eq!(store.finalize(7).await.unwrap(), finalized);
    assert_eq!(store.load_latest().await.unwrap(), Some(finalized));
}

#[tokio::test]
async fn test_obj_state_paths() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());

    store.save(&make_manifest(1)).await.unwrap();
    store
        .save_state_data(1, &[bytes::Bytes::from_static(b"state-blob")])
        .await
        .unwrap();

    let result = inner
        .get_opts(
            &object_store::path::Path::from("checkpoints/state-000001.bin"),
            GetOptions::default(),
        )
        .await;
    assert!(result.is_ok(), "state path should exist");
}

#[tokio::test]
async fn test_obj_latest_json_format() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());

    store.save(&make_manifest(5)).await.unwrap();

    let data = inner
        .get_opts(
            &object_store::path::Path::from("manifests/latest.json"),
            GetOptions::default(),
        )
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    let pointer: super::LatestPointer = serde_json::from_slice(&data).unwrap();
    assert_eq!(pointer.checkpoint_id, 5);
}

#[tokio::test]
async fn test_obj_latest_pointing_to_missing_checkpoint_is_invalid() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());
    let pointer = serde_json::to_vec(&LatestPointer { checkpoint_id: 99 }).unwrap();
    inner
        .put_opts(
            &object_store::path::Path::from("manifests/latest.json"),
            PutPayload::from_bytes(bytes::Bytes::from(pointer)),
            PutOptions::default(),
        )
        .await
        .unwrap();

    let error = store.load_latest().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("recovery pointer references missing checkpoint 99"));
}

#[tokio::test]
async fn test_obj_latest_pointing_to_prepared_checkpoint_is_invalid() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());
    store.save(&make_prepared_manifest(7)).await.unwrap();
    let pointer = serde_json::to_vec(&LatestPointer { checkpoint_id: 7 }).unwrap();
    inner
        .put_opts(
            &object_store::path::Path::from("manifests/latest.json"),
            PutPayload::from_bytes(bytes::Bytes::from(pointer)),
            PutOptions::default(),
        )
        .await
        .unwrap();

    let error = store.load_latest().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("recovery pointer references non-finalized checkpoint 7"));
}

#[tokio::test]
async fn test_obj_latest_monotonic_guard_skips_regression() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());

    store.save(&make_manifest(10)).await.unwrap();
    // A delayed writer (e.g., paused ex-leader) tries to write id=5
    // after the current leader already advanced to id=10. The pointer
    // must not regress.
    store.save(&make_manifest(5)).await.unwrap();

    let loaded = store.load_latest().await.unwrap().unwrap();
    assert_eq!(
        loaded.checkpoint_id, 10,
        "latest pointer should not regress to an older id"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_object_store_publication_keeps_the_highest_checkpoint() {
    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut tasks = Vec::new();
    for checkpoint_id in 1..=32 {
        let inner = Arc::clone(&inner);
        tasks.push(tokio::spawn(async move {
            ObjectStoreCheckpointStore::new(inner, String::new())
                .save(&make_manifest(checkpoint_id))
                .await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }

    let store = ObjectStoreCheckpointStore::new(inner, String::new());
    assert_eq!(
        store.load_latest().await.unwrap().unwrap().checkpoint_id,
        32
    );
}

#[tokio::test]
async fn object_store_reconciles_lost_ack_for_finalize_and_latest_cas() {
    let create_raw: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let create_fault: Arc<dyn ObjectStore> = Arc::new(GetCountingStore::with_lost_ack(
        Arc::clone(&create_raw),
        LostAckTarget::LatestCreate,
    ));
    let create_store = ObjectStoreCheckpointStore::new(create_fault, String::new());
    create_store.save(&make_manifest(1)).await.unwrap();
    assert_eq!(
        create_store
            .load_latest()
            .await
            .unwrap()
            .unwrap()
            .checkpoint_id,
        1
    );

    let raw: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let finalize_fault: Arc<dyn ObjectStore> = Arc::new(GetCountingStore::with_lost_ack(
        Arc::clone(&raw),
        LostAckTarget::ManifestUpdate,
    ));
    let finalize_store = ObjectStoreCheckpointStore::new(finalize_fault, String::new());
    finalize_store
        .save(&make_prepared_manifest(1))
        .await
        .unwrap();
    let finalized = finalize_store.finalize(1).await.unwrap();
    assert_eq!(finalized.durable_phase, DurableCheckpointPhase::Finalized);
    assert_eq!(
        finalize_store
            .load_latest()
            .await
            .unwrap()
            .unwrap()
            .checkpoint_id,
        1
    );

    let latest_fault: Arc<dyn ObjectStore> = Arc::new(GetCountingStore::with_lost_ack(
        Arc::clone(&raw),
        LostAckTarget::LatestUpdate,
    ));
    let latest_store = ObjectStoreCheckpointStore::new(latest_fault, String::new());
    latest_store.save(&make_manifest(2)).await.unwrap();
    assert_eq!(
        latest_store
            .load_latest()
            .await
            .unwrap()
            .unwrap()
            .checkpoint_id,
        2
    );
}

#[tokio::test]
async fn object_store_public_loads_reject_oversize_and_misrouted_metadata() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());

    inner
        .put_opts(
            &store.manifest_path(7),
            PutPayload::from_bytes(bytes::Bytes::from(
                serde_json::to_vec(&make_manifest(8)).unwrap(),
            )),
            PutOptions::default(),
        )
        .await
        .unwrap();
    let error = store.load_by_id(7).await.unwrap_err();
    assert!(error
        .to_string()
        .contains("storage checkpoint 7 contains manifest checkpoint 8"));
    assert!(store.list().await.is_err());

    inner
        .put_opts(
            &store.latest_pointer_path(),
            PutPayload::from_bytes(bytes::Bytes::from(vec![
                b'x';
                (MAX_LATEST_POINTER_BYTES + 1)
                    as usize
            ])),
            PutOptions::default(),
        )
        .await
        .unwrap();
    let error = store.load_latest().await.unwrap_err();
    assert!(error.to_string().contains("exceeding the"));
}

#[tokio::test]
async fn object_metadata_reads_bound_requests_and_reject_malformed_responses() {
    let raw: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let raw_store = ObjectStoreCheckpointStore::new(Arc::clone(&raw), String::new());
    raw_store.save(&make_manifest(1)).await.unwrap();

    for (fault, expected) in [
        (GetFault::MisreportedRange, "response range"),
        (GetFault::ShortBody, "body length"),
        (GetFault::LongBody, "body exceeded"),
    ] {
        let faulting = Arc::new(GetCountingStore::with_get_fault(Arc::clone(&raw), fault));
        let store = ObjectStoreCheckpointStore::new(faulting.clone(), String::new());
        let error = store.load_latest().await.unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert!(
            faulting.bounded_latest_get_seen.load(Ordering::Relaxed),
            "latest metadata was not requested with the configured hard range"
        );
    }

    let versionless = Arc::new(GetCountingStore::with_get_fault(
        Arc::clone(&raw),
        GetFault::MissingVersion,
    ));
    let store = ObjectStoreCheckpointStore::new(versionless, String::new());
    let error = store.save(&make_manifest(2)).await.unwrap_err();
    assert!(error.to_string().contains("no ETag or version"), "{error}");
    assert_eq!(
        raw_store
            .load_latest()
            .await
            .unwrap()
            .unwrap()
            .checkpoint_id,
        1
    );
}

#[tokio::test]
async fn test_validate_checkpoint_valid() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let m = make_manifest(1);
    store.save(&m).await.unwrap();

    let result = store.validate_checkpoint(1).await.unwrap();
    assert!(result.valid, "valid checkpoint: {:?}", result.issues);
    assert!(result.issues.is_empty());
}

#[tokio::test]
async fn loaded_artifact_validation_binds_manifest_to_storage_checkpoint_id() {
    let artifacts = CheckpointArtifacts {
        manifest: make_manifest(8),
        state_data: None,
    };
    let participant_id = artifacts.manifest.participant_id;
    let key_group_count = KeyGroupCount::try_from(artifacts.manifest.vnode_count).unwrap();

    let (_, validation) = artifacts
        .validate(
            7,
            participant_id,
            key_group_count,
            DEFAULT_MAX_CHECKPOINT_STATE_BYTES,
        )
        .await
        .unwrap();

    assert!(!validation.valid);
    assert!(validation.issues.iter().any(|issue| {
        issue
            .message()
            .contains("storage checkpoint 7 contains manifest checkpoint 8")
    }));
}

#[tokio::test]
async fn loaded_artifact_validation_preserves_mixed_checksum_rule() {
    let state = bytes::Bytes::from_static(b"external-state");
    let mut manifest = make_manifest(9);
    manifest
        .operator_states
        .insert("inline".into(), OperatorCheckpoint::inline(b"inline-state"));
    manifest.operator_states.insert(
        "external".into(),
        OperatorCheckpoint::external(0, state.len() as u64),
    );
    manifest.state_checksum = Some(stamp_checksum(
        &manifest.operator_states,
        Some(std::slice::from_ref(&state)),
    ));
    let artifacts = CheckpointArtifacts {
        manifest,
        state_data: Some(state.to_vec()),
    };
    let key_group_count = KeyGroupCount::try_from(artifacts.manifest.vnode_count).unwrap();

    let (mut artifacts, validation) = artifacts
        .validate(9, 0, key_group_count, DEFAULT_MAX_CHECKPOINT_STATE_BYTES)
        .await
        .unwrap();
    assert!(validation.valid);
    artifacts.state_data = Some(b"tampered-state".to_vec());
    let (_, validation) = artifacts
        .validate(9, 0, key_group_count, DEFAULT_MAX_CHECKPOINT_STATE_BYTES)
        .await
        .unwrap();
    assert!(!validation.valid);
    assert!(validation
        .issues
        .iter()
        .any(|issue| issue.message().contains("mixed state checksum mismatch")));
}

#[tokio::test]
async fn artifact_validation_enforces_aggregate_raw_logical_state_budget() {
    let state = bytes::Bytes::from_static(b"5678");
    let mut manifest = make_manifest(10);
    manifest
        .operator_states
        .insert("inline".into(), OperatorCheckpoint::inline(b"1234"));
    manifest.operator_states.insert(
        "external".into(),
        OperatorCheckpoint::external(0, state.len() as u64),
    );
    manifest.state_checksum = Some(stamp_checksum(
        &manifest.operator_states,
        Some(std::slice::from_ref(&state)),
    ));
    let key_group_count = KeyGroupCount::try_from(manifest.vnode_count).unwrap();
    let artifacts = CheckpointArtifacts {
        manifest,
        state_data: Some(state.to_vec()),
    };

    let (artifacts, exact) = artifacts.validate(10, 0, key_group_count, 8).await.unwrap();
    assert!(exact.valid, "exact aggregate budget: {:?}", exact.issues);
    assert_eq!(artifacts.state_data.as_deref(), Some(state.as_ref()));

    let (_, over) = artifacts.validate(10, 0, key_group_count, 7).await.unwrap();
    assert!(!over.valid);
    assert!(over.issues.iter().any(|issue| issue
        .message()
        .contains("aggregate logical operator state is 8 bytes")));
}

#[tokio::test]
async fn artifact_validation_rejects_malformed_operator_shapes_and_base64() {
    let mut malformed_inline = make_manifest(11);
    malformed_inline.operator_states.insert(
        "bad-inline".into(),
        OperatorCheckpoint {
            state_b64: Some("%%%".into()),
            external: false,
            external_offset: 0,
            external_length: 0,
        },
    );
    malformed_inline.state_checksum = Some("untrusted".into());
    let key_group_count = KeyGroupCount::try_from(malformed_inline.vnode_count).unwrap();
    let (_, validation) = CheckpointArtifacts {
        manifest: malformed_inline,
        state_data: None,
    }
    .validate(11, 0, key_group_count, 64)
    .await
    .unwrap();
    assert!(validation
        .issues
        .iter()
        .any(|issue| issue.message().contains("invalid base64")));

    let mut malformed_external = make_manifest(12);
    malformed_external.operator_states.insert(
        "bad-external".into(),
        OperatorCheckpoint {
            state_b64: Some("eA==".into()),
            external: true,
            external_offset: 1,
            external_length: 1,
        },
    );
    malformed_external.state_checksum = Some("untrusted".into());
    let key_group_count = KeyGroupCount::try_from(malformed_external.vnode_count).unwrap();
    let (_, validation) = CheckpointArtifacts {
        manifest: malformed_external,
        state_data: Some(vec![0, 1]),
    }
    .validate(12, 0, key_group_count, 64)
    .await
    .unwrap();
    assert!(validation.issues.iter().any(|issue| issue
        .message()
        .contains("external state also contains inline base64")));
    assert!(validation.issues.iter().any(|issue| issue
        .message()
        .contains("sidecar range starts at 1, expected 0")));
}

#[tokio::test]
async fn save_rejects_aggregate_state_over_budget_before_writing_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path())
        .with_max_state_data_bytes(8)
        .unwrap();
    let sidecar = [bytes::Bytes::from_static(b"6789")];
    let mut manifest = make_external_manifest(13, sidecar[0].len());
    manifest
        .operator_states
        .insert("inline".into(), OperatorCheckpoint::inline(b"12345"));

    let error = store
        .save_with_state(&manifest, Some(&sidecar))
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("aggregate logical operator state is 9 bytes"));
    assert!(!store.state_path(13).exists());
    assert!(store.list_ids().await.unwrap().is_empty());
}

#[test]
fn logical_state_length_overflow_is_rejected() {
    let mut manifest = make_manifest(14);
    manifest
        .operator_states
        .insert("first".into(), OperatorCheckpoint::external(0, u64::MAX));
    manifest
        .operator_states
        .insert("second".into(), OperatorCheckpoint::external(u64::MAX, 1));
    manifest.state_checksum = Some("untrusted".into());

    let issues = operator_state_validation_issues(&manifest, None, u64::MAX - 1);
    assert!(issues.iter().any(|issue| issue
        .message()
        .contains("aggregate logical operator state length overflows")));
    assert!(issues
        .iter()
        .any(|issue| issue.message().contains("sidecar range overflows")));
}

#[tokio::test]
async fn test_validate_checkpoint_rejects_zero_noncanonical_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    // Inject malformed storage directly; the production save boundary rejects it.
    let mut m = CheckpointManifest::new(1, 0);
    m.durable_phase = DurableCheckpointPhase::Finalized;
    let checkpoint_dir = store.checkpoint_dir(1);
    std::fs::create_dir_all(&checkpoint_dir).unwrap();
    std::fs::write(
        store.manifest_path(1),
        serde_json::to_vec_pretty(&m).unwrap(),
    )
    .unwrap();

    let result = store.validate_checkpoint(1).await.unwrap();
    assert!(!result.valid, "epoch=0 should be invalid");
    assert!(
        result
            .issues
            .iter()
            .any(|i| i.message().contains("canonical checkpoint ID")),
        "should identify the canonical-attempt violation: {:?}",
        result.issues
    );
}

#[tokio::test]
async fn test_validate_checkpoint_missing_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let result = store.validate_checkpoint(99).await.unwrap();
    assert!(!result.valid);
    assert!(result.issues[0].message().contains("not found"));
}

#[tokio::test]
async fn test_validate_checkpoint_corrupt_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());

    // Create a checkpoint dir with corrupt manifest JSON.
    let cp_dir = dir.path().join("checkpoints/checkpoint_000001");
    std::fs::create_dir_all(&cp_dir).unwrap();
    std::fs::write(cp_dir.join("manifest.json"), "not valid json").unwrap();

    // Corrupt manifest is a validation failure, not an I/O error.
    let result = store.validate_checkpoint(1).await.unwrap();
    assert!(!result.valid);
    assert!(
        result.issues[0].message().contains("corrupt manifest"),
        "expected corrupt manifest issue: {:?}",
        result.issues
    );
}

#[tokio::test]
async fn test_validate_checkpoint_state_checksum_ok() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());

    let state = b"important operator state";
    let m = make_external_manifest(1, state.len());
    store
        .save_with_state(&m, Some(&[bytes::Bytes::from_static(state)]))
        .await
        .unwrap();

    let result = store.validate_checkpoint(1).await.unwrap();
    assert!(result.valid, "checksum should match: {:?}", result.issues);
}

#[tokio::test]
async fn test_validate_checkpoint_state_checksum_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());

    // Save with state to get a checksum.
    let state = b"original state";
    let m = make_external_manifest(1, state.len());
    store
        .save_with_state(&m, Some(&[bytes::Bytes::from_static(state)]))
        .await
        .unwrap();

    // Now corrupt the state.bin on disk.
    let state_path = dir.path().join("checkpoints/checkpoint_000001/state.bin");
    std::fs::write(&state_path, b"tampered state").unwrap();

    let result = store.validate_checkpoint(1).await.unwrap();
    assert!(!result.valid, "corrupted state should be invalid");
    assert!(
        result
            .issues
            .iter()
            .any(|i| i.message().contains("checksum mismatch")),
        "should report checksum mismatch: {:?}",
        result.issues
    );
}

#[tokio::test]
async fn test_validate_checkpoint_state_missing_when_expected() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());

    // Save with state.
    let m = make_external_manifest(1, b"state".len());
    store
        .save_with_state(&m, Some(&[bytes::Bytes::from_static(b"state")]))
        .await
        .unwrap();

    // Delete the state.bin file to simulate partial crash.
    let state_path = dir.path().join("checkpoints/checkpoint_000001/state.bin");
    std::fs::remove_file(&state_path).unwrap();

    let result = store.validate_checkpoint(1).await.unwrap();
    assert!(!result.valid);
    assert!(
        result
            .issues
            .iter()
            .any(|i| i.message().contains("missing")),
        "should report missing state: {:?}",
        result.issues
    );
}

#[tokio::test]
async fn test_recover_latest_validated_skips_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());

    // Save two checkpoints.
    store.save(&make_manifest(1)).await.unwrap();
    store.save(&make_manifest(2)).await.unwrap();

    // Corrupt the latest checkpoint's manifest.
    let cp2_manifest = dir
        .path()
        .join("checkpoints/checkpoint_000002/manifest.json");
    std::fs::write(cp2_manifest, "<<<corrupt>>>").unwrap();

    // Recovery should skip checkpoint 2 and pick checkpoint 1.
    let report = store.recover_latest_validated().await.unwrap();
    assert_eq!(report.chosen_id, Some(1));
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(report.skipped[0].0, 2);
    assert_eq!(report.examined, 2);
}

#[tokio::test]
async fn test_recover_latest_validated_fresh_start() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let report = store.recover_latest_validated().await.unwrap();
    assert!(report.chosen_id.is_none());
    assert_eq!(report.examined, 0);
}

#[tokio::test]
async fn test_recover_latest_validated_all_corrupt_reports_unusable_history() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());

    // Save a checkpoint, then corrupt it.
    store.save(&make_manifest(1)).await.unwrap();
    let cp_manifest = dir
        .path()
        .join("checkpoints/checkpoint_000001/manifest.json");
    std::fs::write(cp_manifest, "corrupt").unwrap();

    let report = store.recover_latest_validated().await.unwrap();
    assert!(report.chosen_id.is_none());
    assert_eq!(report.examined, 1);
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(report.skipped[0].0, 1);
}

#[tokio::test]
async fn test_save_with_state_writes_checksum() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());

    let state = b"state-data-for-checksum";
    let m = make_external_manifest(1, state.len());
    store
        .save_with_state(&m, Some(&[bytes::Bytes::from_static(state)]))
        .await
        .unwrap();

    let loaded = store.load_latest().await.unwrap().unwrap();
    assert!(
        loaded.state_checksum.is_some(),
        "state_checksum should be set"
    );
    let expected = sha256_hex(state);
    assert_eq!(loaded.state_checksum.unwrap(), expected);
}

#[tokio::test]
async fn test_legacy_manifest_is_rejected() {
    let json = r#"{
        "version": 1,
        "checkpoint_id": 1,
        "epoch": 1,
        "timestamp_ms": 1000
    }"#;
    assert!(serde_json::from_str::<CheckpointManifest>(json).is_err());
}

// ObjectStore variants

#[tokio::test]
async fn test_obj_validate_checkpoint_valid() {
    let store = make_obj_store();
    store.save(&make_manifest(1)).await.unwrap();

    let result = store.validate_checkpoint(1).await.unwrap();
    assert!(result.valid, "valid checkpoint: {:?}", result.issues);
}

#[tokio::test]
async fn test_obj_validate_checkpoint_missing() {
    let store = make_obj_store();
    let result = store.validate_checkpoint(99).await.unwrap();
    assert!(!result.valid);
}

#[tokio::test]
async fn test_obj_validate_state_checksum() {
    let store = ObjectStoreCheckpointStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        String::new(),
    );

    let state = b"obj-store-state-data";
    let m = make_external_manifest(1, state.len());
    store
        .save_with_state(&m, Some(&[bytes::Bytes::from_static(state)]))
        .await
        .unwrap();

    let result = store.validate_checkpoint(1).await.unwrap();
    assert!(result.valid, "checksum should match: {:?}", result.issues);
}

#[tokio::test]
async fn test_obj_recover_latest_validated() {
    let store = ObjectStoreCheckpointStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        String::new(),
    );

    store.save(&make_manifest(1)).await.unwrap();
    store.save(&make_manifest(2)).await.unwrap();

    let report = store.recover_latest_validated().await.unwrap();
    assert_eq!(report.chosen_id, Some(2));
    assert!(report.skipped.is_empty());
}
