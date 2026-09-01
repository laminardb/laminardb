//! Durable cluster control KV persistence: static-discovery adapter and the
//! object-store-backed control plane.
//!
//! Responsibility: implement the `ClusterKv` contract twice — a read-only view
//! over static discovery membership metadata, and the durable object-store KV
//! used for cluster control records and the recovery generation marker.
//!
//! Durable format (must not change):
//! - records live under
//!   `cluster-control-kv/v{version}/node={node}/term={term:020}/owner={owner}/key={sha256}/{seq:020}.json`
//!   and are create-only writes with canonical JSON bodies;
//! - the recovery generation lives under
//!   `cluster-control-kv/v2/recovery-generation/v{generation:020}.json` and is
//!   cluster-global: it intentionally escapes process terms so a full-cluster
//!   restart cannot reset the recovery epoch.
//!
//! Invariants:
//! - the local process term is verified before and after every I/O;
//! - writes are create-only; conflicts are resolved by sequence discovery;
//! - pruning is best-effort background work, never on the request path.

use std::collections::BinaryHeap;
use std::sync::Arc;

use object_store::ObjectStoreExt as _;
use tokio::sync::watch;
use tracing::warn;

use laminar_core::cluster::discovery::{NodeId, NodeInfo};

pub(super) struct StaticClusterKv {
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
}

impl StaticClusterKv {
    pub(super) fn new(membership_rx: watch::Receiver<Vec<NodeInfo>>) -> Self {
        Self { membership_rx }
    }
}

#[async_trait::async_trait]
impl laminar_core::cluster::control::ClusterKv for StaticClusterKv {
    async fn write(&self, _key: &str, _value: String) {}

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        let peers = self.membership_rx.borrow();
        let peer = peers.iter().find(|p| p.id == who)?;
        peer.metadata.tags.get(key).cloned()
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        let peers = self.membership_rx.borrow();
        peers
            .iter()
            .filter_map(|p| p.metadata.tags.get(key).map(|v| (p.id, v.clone())))
            .collect()
    }
}

pub(super) const OBJECT_STORE_CONTROL_IO_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(5);
const OBJECT_STORE_CONTROL_MAX_VALUE_BYTES: u64 = 1024 * 1024;
const OBJECT_STORE_CONTROL_MAX_KEY_BYTES: usize = 1024;
pub(super) const OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES: u64 =
    OBJECT_STORE_CONTROL_MAX_VALUE_BYTES * 6 + 16 * 1024;
pub(super) const OBJECT_STORE_CONTROL_VERSION: u8 = 2;
const OBJECT_STORE_CONTROL_HISTORY_TO_RETAIN: usize = 2;
const OBJECT_STORE_CONTROL_MAX_LIST_RECORDS: usize = 4096;
const OBJECT_STORE_CONTROL_MAX_WRITE_ATTEMPTS: usize = 4;
pub(super) const OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS: usize = 256;
const OBJECT_STORE_CONTROL_MAX_PRUNE_BATCHES: usize = 4;
pub(super) const OBJECT_STORE_CONTROL_SCAN_CONCURRENCY: usize = 32;
pub(super) const RECOVERY_GENERATION_KEY: &str = "control:recovery-gen";

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ObjectStoreControlRecord {
    pub(super) version: u8,
    pub(super) node: u64,
    pub(super) owner: uuid::Uuid,
    pub(super) term: u64,
    pub(super) sequence: u64,
    pub(super) key: String,
    pub(super) value: String,
}

impl ObjectStoreControlRecord {
    fn validate(
        &self,
        lease: &laminar_core::cluster::control::ProcessLease,
        key: &str,
        sequence: u64,
    ) -> Result<(), String> {
        if self.version != OBJECT_STORE_CONTROL_VERSION
            || self.node != lease.node.0
            || self.owner != lease.owner
            || self.term != lease.term
            || sequence == 0
            || self.sequence != sequence
            || self.key != key
            || u64::try_from(self.value.len()).unwrap_or(u64::MAX)
                > OBJECT_STORE_CONTROL_MAX_VALUE_BYTES
        {
            return Err("control record does not match its durable path and process lease".into());
        }
        Ok(())
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct RecoveryGenerationRecord {
    version: u8,
    generation: u64,
    writer_node: u64,
    writer_owner: uuid::Uuid,
    writer_term: u64,
}

fn object_store_control_key_digest(key: &str) -> String {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(key.as_bytes());
    let mut encoded = String::with_capacity(digest.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

pub(super) fn object_store_control_key_prefix(
    lease: &laminar_core::cluster::control::ProcessLease,
    key: &str,
) -> String {
    format!(
        "cluster-control-kv/v{OBJECT_STORE_CONTROL_VERSION}/node={}/term={:020}/owner={}/key={}/",
        lease.node.0,
        lease.term,
        lease.owner,
        object_store_control_key_digest(key)
    )
}

pub(super) fn object_store_control_record_path(
    lease: &laminar_core::cluster::control::ProcessLease,
    key: &str,
    sequence: u64,
) -> object_store::path::Path {
    object_store::path::Path::from(format!(
        "{}v{sequence:020}.json",
        object_store_control_key_prefix(lease, key)
    ))
}

pub(super) const RECOVERY_GENERATION_PREFIX: &str = "cluster-control-kv/v2/recovery-generation/";

pub(super) fn recovery_generation_path(generation: u64) -> object_store::path::Path {
    object_store::path::Path::from(format!(
        "{RECOVERY_GENERATION_PREFIX}v{generation:020}.json"
    ))
}

fn sequence_from_path(prefix: &str, path: &object_store::path::Path) -> Result<u64, String> {
    let raw = path
        .as_ref()
        .strip_prefix(prefix)
        .and_then(|suffix| suffix.strip_prefix('v'))
        .and_then(|suffix| suffix.strip_suffix(".json"))
        .ok_or_else(|| format!("invalid control record path {path}"))?;
    if raw.len() != 20 || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("invalid control record sequence in {path}"));
    }
    raw.parse::<u64>()
        .map_err(|error| format!("invalid control record sequence in {path}: {error}"))
}

pub(super) fn retain_oldest_control_record(
    oldest: &mut BinaryHeap<(u64, String)>,
    sequence: u64,
    path: &object_store::path::Path,
) {
    let candidate = (sequence, path.to_string());
    if oldest.len() < OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS {
        oldest.push(candidate);
    } else if oldest.peek().is_some_and(|largest| &candidate < largest) {
        oldest.pop();
        oldest.push(candidate);
    }
}

pub(super) async fn list_control_sequences(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> Result<Vec<u64>, String> {
    use futures::StreamExt;

    let prefix_path = object_store::path::Path::from(prefix);
    let mut entries = store.list(Some(&prefix_path));
    let mut sequences = Vec::new();
    while let Some(entry) = entries.next().await {
        let entry = entry.map_err(|error| error.to_string())?;
        if sequences.len() == OBJECT_STORE_CONTROL_MAX_LIST_RECORDS {
            return Err(format!(
                "control history exceeds the fixed {OBJECT_STORE_CONTROL_MAX_LIST_RECORDS}-record scan bound"
            ));
        }
        sequences.push(sequence_from_path(prefix, &entry.location)?);
    }
    sequences.sort_unstable();
    sequences.dedup();
    Ok(sequences)
}

async fn prune_control_history_batch(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> Result<bool, String> {
    use futures::StreamExt;

    let prefix_path = object_store::path::Path::from(prefix);
    let mut entries = store.list(Some(&prefix_path));
    let mut oldest = BinaryHeap::with_capacity(OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS);
    let mut total = 0usize;
    while let Some(entry) = entries.next().await {
        let entry = entry.map_err(|error| error.to_string())?;
        let sequence = sequence_from_path(prefix, &entry.location)?;
        total = total.saturating_add(1);
        retain_oldest_control_record(&mut oldest, sequence, &entry.location);
    }
    let delete_count = total
        .saturating_sub(OBJECT_STORE_CONTROL_HISTORY_TO_RETAIN)
        .min(OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS);
    let deletions = futures::stream::iter(
        oldest
            .into_sorted_vec()
            .into_iter()
            .take(delete_count)
            .map(|(_, path)| Ok::<_, object_store::Error>(object_store::path::Path::from(path))),
    )
    .boxed();
    let mut results = store.delete_stream(deletions);
    while let Some(result) = results.next().await {
        if let Err(error) = result {
            if !matches!(error, object_store::Error::NotFound { .. }) {
                return Err(error.to_string());
            }
        }
    }
    Ok(total.saturating_sub(delete_count) <= OBJECT_STORE_CONTROL_HISTORY_TO_RETAIN)
}

pub(super) struct ObjectStoreClusterKv {
    local_id: NodeId,
    local_lease: laminar_core::cluster::control::ProcessLease,
    local_lease_deadline: Arc<laminar_core::cluster::control::LeaseDeadline>,
    process_lease_ttl_ms: i64,
    store: Arc<dyn object_store::ObjectStore>,
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
    pub(super) sequence_states:
        std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<Option<u64>>>>>,
    pub(super) prune_states: Arc<std::sync::Mutex<std::collections::HashMap<String, bool>>>,
}

impl ObjectStoreClusterKv {
    pub(super) fn new(
        local_lease: laminar_core::cluster::control::ProcessLease,
        local_lease_deadline: Arc<laminar_core::cluster::control::LeaseDeadline>,
        process_lease_ttl_ms: i64,
        store: Arc<dyn object_store::ObjectStore>,
        membership_rx: watch::Receiver<Vec<NodeInfo>>,
    ) -> Self {
        debug_assert!(!local_lease.node.is_unassigned());
        debug_assert!(!local_lease.owner.is_nil());
        debug_assert!(local_lease.term > 0);
        debug_assert!(process_lease_ttl_ms > 0);
        Self {
            local_id: local_lease.node,
            local_lease,
            local_lease_deadline,
            process_lease_ttl_ms,
            store,
            membership_rx,
            sequence_states: std::sync::Mutex::new(std::collections::HashMap::new()),
            prune_states: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    fn visible_ids(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = {
            let members = self.membership_rx.borrow();
            members.iter().map(|member| member.id).collect()
        };
        ids.push(self.local_id);
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    fn validate_key_and_value(key: &str, value: &str) -> Result<(), String> {
        if key.is_empty() || key.len() > OBJECT_STORE_CONTROL_MAX_KEY_BYTES {
            return Err(format!(
                "control key is {} bytes; expected 1..={OBJECT_STORE_CONTROL_MAX_KEY_BYTES}",
                key.len()
            ));
        }
        // An empty value is intentional: the controller uses it as the durable clear/tombstone
        // signal for a completed recovery round.
        if u64::try_from(value.len()).unwrap_or(u64::MAX) > OBJECT_STORE_CONTROL_MAX_VALUE_BYTES {
            return Err(format!(
                "control value is {} bytes; maximum is {OBJECT_STORE_CONTROL_MAX_VALUE_BYTES}",
                value.len()
            ));
        }
        Ok(())
    }

    async fn load_process_lease(
        &self,
        node: NodeId,
    ) -> Result<Option<laminar_core::cluster::control::ProcessLease>, String> {
        laminar_core::cluster::control::ProcessLeaseStore::new(
            Arc::clone(&self.store),
            node,
            self.process_lease_ttl_ms,
        )
        .load()
        .await
        .map_err(|error| error.to_string())
    }

    fn same_process_term(
        left: &laminar_core::cluster::control::ProcessLease,
        right: &laminar_core::cluster::control::ProcessLease,
    ) -> bool {
        left.node == right.node && left.owner == right.owner && left.term == right.term
    }

    fn require_live_local_deadline(&self) -> Result<(), String> {
        if self.local_lease_deadline.is_live() {
            Ok(())
        } else {
            Err("local process lease deadline expired".into())
        }
    }

    async fn require_local_process_term(&self) -> Result<(), String> {
        self.require_live_local_deadline()?;
        let current = self
            .load_process_lease(self.local_id)
            .await?
            .ok_or_else(|| "local process lease is absent".to_string())?;
        if !Self::same_process_term(&current, &self.local_lease) {
            return Err("local process lease owner or term changed".into());
        }
        self.require_live_local_deadline()
    }

    fn sequence_state(&self, prefix: &str) -> Arc<tokio::sync::Mutex<Option<u64>>> {
        let mut states = self
            .sequence_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            states
                .entry(prefix.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None))),
        )
    }

    async fn put_create(
        &self,
        path: &object_store::path::Path,
        bytes: Vec<u8>,
    ) -> Result<bool, String> {
        let options = object_store::PutOptions {
            mode: object_store::PutMode::Create,
            ..object_store::PutOptions::default()
        };
        match self
            .store
            .put_opts(
                path,
                object_store::PutPayload::from(bytes::Bytes::from(bytes)),
                options,
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(false),
            Err(error) => Err(error.to_string()),
        }
    }

    pub(super) fn schedule_prune(&self, prefix: String) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        {
            let mut states = self
                .prune_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(pending) = states.get_mut(&prefix) {
                *pending = true;
                return;
            }
            states.insert(prefix.clone(), false);
        }
        let store = Arc::clone(&self.store);
        let prune_states = Arc::clone(&self.prune_states);
        runtime.spawn(async move {
            loop {
                let prune = async {
                    for _ in 0..OBJECT_STORE_CONTROL_MAX_PRUNE_BATCHES {
                        match tokio::time::timeout(
                            OBJECT_STORE_CONTROL_IO_TIMEOUT,
                            prune_control_history_batch(&store, &prefix),
                        )
                        .await
                        {
                            Ok(Ok(true)) => return Ok(()),
                            Ok(Ok(false)) => tokio::task::yield_now().await,
                            Ok(Err(error)) => return Err(error),
                            Err(_) => {
                                return Err(format!(
                                    "prune timed out after {OBJECT_STORE_CONTROL_IO_TIMEOUT:?}"
                                ));
                            }
                        }
                    }
                    Err("history still exceeds the bounded prune budget".to_string())
                }
                .await;
                if let Err(error) = prune {
                    warn!(%prefix, %error, "object-store control history prune failed");
                }

                let rerun = {
                    let mut states = prune_states
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    match states.get_mut(&prefix) {
                        Some(pending) if *pending => {
                            *pending = false;
                            true
                        }
                        Some(_) => {
                            states.remove(&prefix);
                            false
                        }
                        None => false,
                    }
                };
                if !rerun {
                    return;
                }
                tokio::task::yield_now().await;
            }
        });
    }

    async fn read_control_record(
        &self,
        lease: &laminar_core::cluster::control::ProcessLease,
        key: &str,
        sequence: u64,
    ) -> Result<ObjectStoreControlRecord, String> {
        let path = object_store_control_record_path(lease, key, sequence);
        let result = self
            .store
            .get(&path)
            .await
            .map_err(|error| error.to_string())?;
        if result.meta.size > OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES {
            return Err(format!(
                "control envelope is {} bytes; maximum is {OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES}",
                result.meta.size
            ));
        }
        let bytes = result.bytes().await.map_err(|error| error.to_string())?;
        let record: ObjectStoreControlRecord =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        record.validate(lease, key, sequence)?;
        let canonical = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err("control record body is not canonically encoded".into());
        }
        Ok(record)
    }

    async fn write_control_value(&self, key: &str, value: String) -> Result<(), String> {
        Self::validate_key_and_value(key, &value)?;
        self.require_local_process_term().await?;
        let prefix = object_store_control_key_prefix(&self.local_lease, key);
        let state = self.sequence_state(&prefix);
        let mut durable_sequence = state.lock().await;
        if durable_sequence.is_none() {
            let sequences = match list_control_sequences(&self.store, &prefix).await {
                Ok(sequences) => sequences,
                Err(error) => {
                    self.schedule_prune(prefix.clone());
                    return Err(error);
                }
            };
            *durable_sequence = Some(sequences.last().copied().unwrap_or(0));
        }

        for _ in 0..OBJECT_STORE_CONTROL_MAX_WRITE_ATTEMPTS {
            let sequence = durable_sequence
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| "control record sequence exhausted".to_string())?;
            *durable_sequence = Some(sequence);
            let record = ObjectStoreControlRecord {
                version: OBJECT_STORE_CONTROL_VERSION,
                node: self.local_id.0,
                owner: self.local_lease.owner,
                term: self.local_lease.term,
                sequence,
                key: key.to_string(),
                value: value.clone(),
            };
            let encoded = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
            if u64::try_from(encoded.len()).unwrap_or(u64::MAX)
                > OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES
            {
                return Err("control envelope exceeds its canonical size bound".into());
            }
            let path = object_store_control_record_path(&self.local_lease, key, sequence);
            if self.put_create(&path, encoded).await? {
                drop(durable_sequence);
                self.schedule_prune(prefix);
                return self.require_local_process_term().await;
            }

            let existing = self
                .read_control_record(&self.local_lease, key, sequence)
                .await?;
            if existing.value == value {
                drop(durable_sequence);
                self.schedule_prune(prefix);
                return self.require_local_process_term().await;
            }
            let sequences = match list_control_sequences(&self.store, &prefix).await {
                Ok(sequences) => sequences,
                Err(error) => {
                    self.schedule_prune(prefix.clone());
                    return Err(error);
                }
            };
            let head = sequences.last().copied().unwrap_or(sequence);
            *durable_sequence = Some((*durable_sequence).unwrap_or(0).max(head));
        }
        self.schedule_prune(prefix);
        Err(format!(
            "control record create conflicted {OBJECT_STORE_CONTROL_MAX_WRITE_ATTEMPTS} times"
        ))
    }

    async fn read_control_value(&self, who: NodeId, key: &str) -> Result<Option<String>, String> {
        Self::validate_key_and_value(key, "")?;
        let Some(before) = self.load_process_lease(who).await? else {
            return Ok(None);
        };
        let prefix = object_store_control_key_prefix(&before, key);
        let sequences = match list_control_sequences(&self.store, &prefix).await {
            Ok(sequences) => sequences,
            Err(error) => {
                self.schedule_prune(prefix);
                return Err(error);
            }
        };
        let value = if let Some(sequence) = sequences.last().copied() {
            Some(
                self.read_control_record(&before, key, sequence)
                    .await?
                    .value,
            )
        } else {
            None
        };
        let after = self
            .load_process_lease(who)
            .await?
            .ok_or_else(|| "process lease vanished during control read".to_string())?;
        if !Self::same_process_term(&before, &after) {
            return Err("process lease owner or term changed during control read".into());
        }
        if !sequences.is_empty() {
            self.schedule_prune(prefix);
        }
        Ok(value)
    }

    async fn read_target_value(&self, who: NodeId, key: &str) -> Result<Option<String>, String> {
        if key == RECOVERY_GENERATION_KEY {
            // Recovery generation is the one cluster-global exception: keeping it outside a
            // process term prevents a full-cluster restart from resetting the recovery epoch.
            self.load_recovery_generation()
                .await
                .map(|generation| generation.map(|value| value.to_string()))
        } else {
            self.read_control_value(who, key).await
        }
    }

    async fn load_recovery_generation(&self) -> Result<Option<u64>, String> {
        let sequences = match list_control_sequences(&self.store, RECOVERY_GENERATION_PREFIX).await
        {
            Ok(sequences) => sequences,
            Err(error) => {
                self.schedule_prune(RECOVERY_GENERATION_PREFIX.to_string());
                return Err(error);
            }
        };
        let Some(generation) = sequences.last().copied() else {
            return Ok(None);
        };
        if generation == 0 {
            return Err("recovery generation record cannot be zero".into());
        }
        let path = recovery_generation_path(generation);
        let result = self
            .store
            .get(&path)
            .await
            .map_err(|error| error.to_string())?;
        if result.meta.size > 512 {
            return Err(format!(
                "recovery generation envelope is {} bytes; maximum is 512",
                result.meta.size
            ));
        }
        let bytes = result.bytes().await.map_err(|error| error.to_string())?;
        let record: RecoveryGenerationRecord =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        if record.version != OBJECT_STORE_CONTROL_VERSION
            || record.generation != generation
            || record.writer_node == 0
            || record.writer_owner.is_nil()
            || record.writer_term == 0
        {
            return Err("recovery generation record does not match its durable path".into());
        }
        let canonical = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err("recovery generation body is not canonically encoded".into());
        }
        Ok(Some(generation))
    }

    async fn write_recovery_generation(&self, value: String) -> Result<(), String> {
        let generation = value
            .parse::<u64>()
            .map_err(|error| format!("invalid recovery generation: {error}"))?;
        if generation == 0 || value != generation.to_string() {
            return Err("recovery generation must be a canonical nonzero u64".into());
        }
        self.require_local_process_term().await?;
        if let Some(current) = self.load_recovery_generation().await? {
            if current > generation {
                return Err(format!(
                    "recovery generation {generation} regresses durable generation {current}"
                ));
            }
            if current == generation {
                return Ok(());
            }
        }
        let record = RecoveryGenerationRecord {
            version: OBJECT_STORE_CONTROL_VERSION,
            generation,
            writer_node: self.local_id.0,
            writer_owner: self.local_lease.owner,
            writer_term: self.local_lease.term,
        };
        let encoded = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        let path = recovery_generation_path(generation);
        let created = self.put_create(&path, encoded).await?;
        if !created {
            let observed = self.load_recovery_generation().await?;
            if observed != Some(generation) {
                return Err("recovery generation create conflicted with a newer marker".into());
            }
        }
        self.schedule_prune(RECOVERY_GENERATION_PREFIX.to_string());
        let observed = self.load_recovery_generation().await?;
        if observed != Some(generation) {
            return Err(format!(
                "recovery generation {generation} was superseded during publication"
            ));
        }
        self.require_local_process_term().await
    }
}

#[async_trait::async_trait]
impl laminar_core::cluster::control::ClusterKv for ObjectStoreClusterKv {
    async fn write(&self, key: &str, value: String) {
        if let Err(error) = self.write_checked(key, value).await {
            warn!(%error, %key, "object-store control write failed");
        }
    }

    async fn write_checked(&self, key: &str, value: String) -> Result<(), String> {
        let write = async {
            if key == RECOVERY_GENERATION_KEY {
                self.write_recovery_generation(value).await
            } else {
                self.write_control_value(key, value).await
            }
        };
        match tokio::time::timeout(OBJECT_STORE_CONTROL_IO_TIMEOUT, write).await {
            Ok(result) => result,
            Err(_) => Err(format!(
                "write timed out after {OBJECT_STORE_CONTROL_IO_TIMEOUT:?}"
            )),
        }
    }

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        match self.read_from_checked(who, key).await {
            Ok(value) => value,
            Err(error) => {
                warn!(node = who.0, %key, %error, "object-store control read failed");
                None
            }
        }
    }

    async fn read_from_checked(&self, who: NodeId, key: &str) -> Result<Option<String>, String> {
        let read = async {
            self.require_local_process_term().await?;
            let value = self.read_target_value(who, key).await?;
            self.require_local_process_term().await?;
            Ok(value)
        };
        match tokio::time::timeout(OBJECT_STORE_CONTROL_IO_TIMEOUT, read).await {
            Ok(value) => value,
            Err(_) => Err(format!(
                "read timed out after {OBJECT_STORE_CONTROL_IO_TIMEOUT:?}"
            )),
        }
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        match self.scan_checked(key).await {
            Ok(values) => values,
            Err(error) => {
                warn!(%key, %error, "object-store control scan failed");
                Vec::new()
            }
        }
    }

    async fn scan_checked(&self, key: &str) -> Result<Vec<(NodeId, String)>, String> {
        use futures::StreamExt;

        let scan = async {
            self.require_local_process_term().await?;
            if key == RECOVERY_GENERATION_KEY {
                let value = self.read_target_value(self.local_id, key).await?;
                self.require_local_process_term().await?;
                return Ok(value.map_or_else(Vec::new, |value| vec![(self.local_id, value)]));
            }
            let mut reads = futures::stream::iter(self.visible_ids())
                .map(|id| async move {
                    let value = self.read_target_value(id, key).await?;
                    Ok::<_, String>((id, value))
                })
                .buffer_unordered(OBJECT_STORE_CONTROL_SCAN_CONCURRENCY);
            let mut results = Vec::new();
            while let Some(result) = reads.next().await {
                let (id, value) = result?;
                if let Some(value) = value {
                    results.push((id, value));
                }
            }
            results.sort_unstable_by_key(|(id, _)| *id);
            self.require_local_process_term().await?;
            Ok(results)
        };
        match tokio::time::timeout(OBJECT_STORE_CONTROL_IO_TIMEOUT, scan).await {
            Ok(result) => result,
            Err(_) => Err(format!(
                "scan timed out after {OBJECT_STORE_CONTROL_IO_TIMEOUT:?}"
            )),
        }
    }
}
