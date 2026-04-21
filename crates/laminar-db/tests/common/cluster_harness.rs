//! Shared 2-node cluster harness for `cluster_e2e_*` tests. See
//! `docs/plans/cluster-production-readiness.md` Phase 0.
//!
//! Included via `#[path = "common/cluster_harness.rs"]` from each test
//! binary so the rdkafka-heavy `common/mod.rs` doesn't leak in.
//!
//! Mirrors `crates/laminar-server/src/cluster.rs::start_cluster`
//! (lines 303-432) for the engine-wiring portion, but inlines a few
//! choices the principal-engineer review of the Phase 0 plan made
//! explicit:
//!
//! - **State backend (shared)**: a single `LocalFileSystem` over one
//!   `TempDir`. Both nodes' `ObjectStoreBackend`s wrap it with distinct
//!   `instance_id`. The path layout (`epoch=N/vnode=V/partial.bin`) is
//!   intentionally shared — `instance_id` lives only in the `_COMMIT`
//!   audit body, not as a path prefix.
//! - **Checkpoint store (per-node)**: each node owns its own `TempDir`
//!   and `object_store_url`. Mirrors production's `nodes/{id}/`
//!   namespace.
//! - **DDL before `db.start()`**: the pipeline only activates when
//!   `start()` is called with at least one source/sink/stream registered
//!   (see `pipeline_lifecycle.rs:237`). The harness returns the engines
//!   built-but-unstarted via [`ClusterEngineHarness::spawn`]; tests run
//!   DDL on every node and then call [`ClusterEngineHarness::start_all`]
//!   to light the pipeline on all engines simultaneously.

#![cfg(feature = "cluster-unstable")]
#![allow(clippy::disallowed_types)]
#![allow(dead_code)] // not every test binary uses every helper

use std::sync::Arc;
use std::time::Duration;

use laminar_core::cluster::control::{AssignmentSnapshot, AssignmentSnapshotStore};
use laminar_core::cluster::testing::MiniCluster;
use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use laminar_core::state::{
    round_robin_assignment, NodeId, ObjectStoreBackend, StateBackend, VnodeRegistry,
};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::LaminarDB;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::TempDir;

const CONVERGENCE_DEADLINE: Duration = Duration::from_secs(10);

/// Per-node engine state. Owned by [`ClusterEngineHarness`].
pub struct NodeRuntime {
    pub db: Arc<LaminarDB>,
    pub instance_id: NodeId,
    pub vnode_registry: Arc<VnodeRegistry>,
}

impl NodeRuntime {
    /// Vnodes this node currently owns.
    #[must_use]
    pub fn owned_vnodes(&self) -> Vec<u32> {
        laminar_core::state::owned_vnodes(&self.vnode_registry, self.instance_id)
    }
}

/// Two-or-more-node cluster of real LaminarDB engines glued to a
/// `MiniCluster` for gossip + control.
pub struct ClusterEngineHarness {
    /// Gossip + ClusterController layer.
    pub cluster: MiniCluster,
    /// Per-node engine state, in `cluster.nodes` order.
    pub nodes: Vec<NodeRuntime>,
    /// Backing dir for the cluster-wide state backend. Kept here so its
    /// lifetime outlives the engines. Survives across [`shutdown_keep_dirs`]
    /// for the restart scenario.
    pub shared_state_dir: TempDir,
    /// One per node, in `nodes` order. Same survive-across-restart
    /// contract as `shared_state_dir`.
    pub checkpoint_dirs: Vec<TempDir>,
}

impl ClusterEngineHarness {
    /// Spawn `n` nodes with `vnode_count` vnodes round-robin distributed
    /// across them. Returns when every engine has completed `db.start()`
    /// and gossip has converged.
    ///
    /// # Panics
    /// Panics if convergence times out, the lowest-id node fails to win
    /// leader election, or any engine fails to build/start.
    pub async fn spawn(n: usize, vnode_count: u32) -> Self {
        let shared_state_dir = tempfile::tempdir().expect("shared state tempdir");
        let checkpoint_dirs: Vec<TempDir> = (0..n)
            .map(|_| tempfile::tempdir().expect("checkpoint tempdir"))
            .collect();
        Self::spawn_with_dirs(n, vnode_count, shared_state_dir, checkpoint_dirs).await
    }

    /// Like [`spawn`], but reuse pre-existing dirs (the restart scenario
    /// hands these back through [`shutdown_keep_dirs`]).
    pub async fn spawn_with_dirs(
        n: usize,
        vnode_count: u32,
        shared_state_dir: TempDir,
        checkpoint_dirs: Vec<TempDir>,
    ) -> Self {
        assert_eq!(checkpoint_dirs.len(), n, "one checkpoint dir per node");

        // Coordinated assignment snapshot store (Phase 1.2). Every
        // node's ClusterController sees the same store, so the vnode
        // assignment becomes the cluster's source of truth rather than
        // each node independently computing round-robin at boot. On
        // first boot the store is empty; whichever node CAS-creates
        // the initial snapshot wins, every other node reads that
        // snapshot back via `load`.
        let snapshot_store: Arc<AssignmentSnapshotStore> = {
            let fs: Arc<dyn ObjectStore> = Arc::new(
                LocalFileSystem::new_with_prefix(shared_state_dir.path())
                    .expect("LocalFileSystem over shared state dir for snapshot"),
            );
            Arc::new(AssignmentSnapshotStore::new(fs))
        };

        let cluster = MiniCluster::spawn_with_snapshot(n, Arc::clone(&snapshot_store)).await;
        cluster
            .wait_for_convergence(CONVERGENCE_DEADLINE)
            .await
            .expect("gossip convergence");

        // Lowest numeric instance_id wins leader election in the
        // current ClusterController. Nodes are in id order; assert the
        // expected leader so a controller change doesn't silently
        // invalidate every test that relies on `nodes[0]` being it.
        assert!(
            cluster.nodes[0].controller.is_leader(),
            "harness assumes nodes[0] is leader; check ClusterController election rule",
        );

        let shared_store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(shared_state_dir.path())
                .expect("LocalFileSystem over shared state dir"),
        );

        let peer_ids: Vec<NodeId> = cluster
            .nodes
            .iter()
            .map(|nh| NodeId(nh.instance_id.0))
            .collect();

        // Resolve the cluster's vnode assignment ONCE, before per-node
        // wiring. Load the stored snapshot; if absent, compute the
        // initial round-robin and CAS-create. All nodes end up with the
        // identical assignment AND the same `assignment_version`
        // whether they read or wrote — Phase 1.2 AC plus the Phase 1.4
        // fence-generation source of truth.
        let (assignment, snapshot_version) =
            resolve_assignment(&snapshot_store, vnode_count, &peer_ids).await;

        // Bind every node's receiver up front so we can cross-register
        // addresses on the senders below.
        let mut receivers: Vec<Arc<ShuffleReceiver>> = Vec::with_capacity(n);
        for nh in &cluster.nodes {
            let recv = ShuffleReceiver::bind(nh.instance_id.0, "127.0.0.1:0".parse().unwrap())
                .await
                .expect("ShuffleReceiver::bind");
            receivers.push(Arc::new(recv));
        }

        let mut node_runtimes = Vec::with_capacity(n);
        for (idx, nh) in cluster.nodes.iter().enumerate() {
            let self_id = nh.instance_id;

            // Outbound shuffle sender: register every other peer's
            // receiver address so `send_to(peer_id, ..)` resolves.
            let sender = ShuffleSender::new(self_id.0);
            for (p_idx, p_nh) in cluster.nodes.iter().enumerate() {
                if p_idx == idx {
                    continue;
                }
                sender
                    .register_peer(p_nh.instance_id.0, receivers[p_idx].local_addr())
                    .await;
            }
            let sender = Arc::new(sender);

            // Concrete backend first, so we can call the Phase 1.4
            // fence setter before erasing to `dyn StateBackend`.
            let concrete_backend = ObjectStoreBackend::new(
                Arc::clone(&shared_store),
                self_id.0.to_string(),
                vnode_count,
            );
            // Every node starts with the same authoritative version —
            // the one that rode into the stored snapshot. Stale peers
            // bringing a smaller version will be rejected at
            // `write_partial`.
            concrete_backend.set_authoritative_version(snapshot_version);
            let state_backend: Arc<dyn StateBackend> = Arc::new(concrete_backend);

            // Install the assignment and set the registry's local version
            // to match the persisted snapshot's in a single atomic step —
            // fence-aware writers then stamp writes with the correct
            // caller version without paying an O(snapshot_version) loop.
            let registry = Arc::new(VnodeRegistry::new(vnode_count));
            registry.set_assignment_and_version(Arc::clone(&assignment), snapshot_version);

            let cp_cfg = StreamCheckpointConfig {
                interval_ms: None, // manual only — tests drive checkpoint() explicitly
                data_dir: Some(checkpoint_dirs[idx].path().to_path_buf()),
                max_retained: Some(3),
            };

            // Profile::Cluster requires `object_store_url` to be set,
            // but when it IS set the engine picks `ObjectStoreCheckpointStore`,
            // whose sync API uses `block_on` and panics from inside a tokio
            // runtime (`checkpoint_store.rs:772`). The production cluster
            // path avoids this because its server `apply_checkpoint_config`
            // routes `file://` URLs to `FileSystemCheckpointStore` and only
            // sets `object_store_url` for remote stores.
            //
            // For the test harness we skip `.profile(Cluster)` so the
            // builder auto-detects `Profile::Embedded` from the `storage_dir`,
            // which has no `object_store_url` requirement. All cluster
            // semantics still activate — they flow from the `cluster-unstable`
            // feature gate plus the explicit `cluster_controller`,
            // `state_backend`, `vnode_registry`, and `physical_optimizer_rule`
            // wiring below, independent of the `Profile` enum.
            let db = LaminarDB::builder()
                .storage_dir(checkpoint_dirs[idx].path().to_path_buf())
                .checkpoint(cp_cfg)
                .cluster_controller(Arc::clone(&nh.controller))
                .state_backend(Arc::clone(&state_backend))
                .vnode_registry(Arc::clone(&registry))
                .shuffle_sender(Arc::clone(&sender))
                .shuffle_receiver(Arc::clone(&receivers[idx]))
                .build()
                .await
                .expect("LaminarDB::builder().build()");
            let db = Arc::new(db);
            // NOTE: `db.start()` is intentionally deferred — see
            // [`ClusterEngineHarness::start_all`]. Running DDL first
            // lets start() activate the streaming coordinator with
            // the sources/MVs already registered.

            node_runtimes.push(NodeRuntime {
                db,
                instance_id: self_id,
                vnode_registry: Arc::clone(&registry),
            });
        }

        Self {
            cluster,
            nodes: node_runtimes,
            shared_state_dir,
            checkpoint_dirs,
        }
    }

    /// `db.start()` every node. Call AFTER issuing DDL on each engine —
    /// the streaming coordinator only activates when start() is called
    /// with at least one registered stream (`pipeline_lifecycle.rs:237`).
    pub async fn start_all(&self) {
        for node in &self.nodes {
            node.db.start().await.expect("db.start()");
        }
    }

    /// Index into `nodes` of the current leader. Always `0` under the
    /// "lowest-id wins" rule asserted in [`spawn_with_dirs`].
    #[must_use]
    pub fn leader_idx(&self) -> usize {
        self.cluster
            .nodes
            .iter()
            .position(|n| n.controller.is_leader())
            .expect("at least one leader after convergence")
    }

    /// Convenience: every non-leader index.
    pub fn follower_idxs(&self) -> Vec<usize> {
        let leader = self.leader_idx();
        (0..self.cluster.nodes.len())
            .filter(|i| *i != leader)
            .collect()
    }

    /// Drop the cluster cleanly, returning the durable dirs so the
    /// caller can hand them to a fresh [`spawn_with_dirs`] for a
    /// restart-scenario test.
    pub async fn shutdown_keep_dirs(self) -> (TempDir, Vec<TempDir>) {
        let Self {
            cluster,
            nodes,
            shared_state_dir,
            checkpoint_dirs,
        } = self;
        for node in nodes {
            let _ = node.db.shutdown().await;
        }
        cluster.shutdown().await;
        (shared_state_dir, checkpoint_dirs)
    }

    /// Drop the cluster cleanly. Tempdirs are removed.
    pub async fn shutdown(self) {
        let _ = self.shutdown_keep_dirs().await;
    }
}

/// Load the cluster-wide vnode assignment from `store`, falling back to
/// computing a fresh round-robin and CAS-creating it. Returns the
/// assignment ready to hand to [`VnodeRegistry::set_assignment`] AND
/// the snapshot's `version` — used by Phase 1.4's split-brain fence to
/// seed each backend's authoritative generation.
///
/// Implements the Phase 1.2 boot-time contract:
/// - If a snapshot is already stored, every caller loads and uses it
///   unchanged — even partitioned halves of a running cluster arrive
///   at the same assignment.
/// - If no snapshot exists, the first caller to reach
///   [`AssignmentSnapshotStore::save_if_absent`] wins; every losing
///   caller loads the winner. No caller writes a snapshot that would
///   disagree with another.
///
/// Out of scope here (covered by Phase 2.3): dynamic rebalancing when
/// membership changes, version bumping on leader promotion. The stored
/// snapshot is written once at first boot and then left alone.
async fn resolve_assignment(
    store: &AssignmentSnapshotStore,
    vnode_count: u32,
    peer_ids: &[NodeId],
) -> (Arc<[NodeId]>, u64) {
    if let Some(snap) = store.load().await.expect("load snapshot") {
        return (snap.to_vnode_vec(vnode_count).into(), snap.version);
    }

    let fresh = round_robin_assignment(vnode_count, peer_ids);
    let snap = AssignmentSnapshot::empty().next(AssignmentSnapshot::vnodes_from_vec(&fresh));
    match store.save_if_absent(&snap).await.expect("save_if_absent") {
        Some(winner) => (fresh, winner.version),
        None => {
            let loaded = store
                .load()
                .await
                .expect("load after CAS loss")
                .expect("snapshot present after CAS loss");
            (loaded.to_vnode_vec(vnode_count).into(), loaded.version)
        }
    }
}

/// Build a `file:///` URL from an absolute filesystem path.
///
/// On Windows this produces `file:///C:/path/with/forward/slashes`;
/// on Unix `file:///abs/path`. Matches the syntax accepted by
/// `laminar_storage::object_store_builder::build_object_store`.
fn file_url_from_path(path: &std::path::Path) -> String {
    let abs = path.to_string_lossy().into_owned();
    let normalized = abs.replace('\\', "/");
    if normalized.starts_with('/') {
        format!("file://{normalized}")
    } else {
        format!("file:///{normalized}")
    }
}

/// Diagnostic tuple `(instance_id, owned_vnodes)` — used by tests that
/// compare the per-node view of the cluster's vnode assignment.
pub type NodeIdView = (NodeId, Vec<u32>);

// ── Test-side helpers shared between smoke and failures suites ────

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

/// `(key BIGINT, value BIGINT)` schema used by every Phase 0 test.
#[must_use]
pub fn input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Build a `(key, value)` batch where `value = key * 10`.
#[must_use]
pub fn input_batch(keys: &[i64]) -> RecordBatch {
    let values: Vec<i64> = keys.iter().map(|k| k * 10).collect();
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(Int64Array::from(keys.to_vec())),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("input_batch")
}

/// Compute the vnode a single `key` value lands on under the same
/// `arrow::row` + `key_hash` machinery [`ClusterRepartitionExec`] uses.
/// Used by the smoke test to pre-compute keys whose hashes split
/// across both owners (otherwise `target_partitions=4` plus 4 vnodes
/// could let everything pile on the leader and the test would pass
/// without ever exercising the cross-node shuffle).
#[must_use]
pub fn vnode_for_key(key: i64, vnode_count: u32) -> u32 {
    use arrow::row::{RowConverter, SortField};
    use laminar_core::state::key_hash;

    let col: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from(vec![key]));
    let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).expect("RowConverter");
    let rows = converter.convert_columns(&[col]).expect("convert_columns");
    #[allow(clippy::cast_possible_truncation)]
    let v = (key_hash(rows.row(0).as_ref()) % u64::from(vnode_count)) as u32;
    v
}

/// Pick `per_owner` keys from `0..1000` whose `vnode_for_key % vnode_count`
/// lands on each `owned` partition. Returns an error string when the
/// search range is too small (raise the upper bound).
///
/// # Errors
/// Returns `Err` when 1000 candidates aren't enough to fill every owner.
pub fn pick_keys_per_owner(
    vnode_count: u32,
    owners: &[(NodeId, Vec<u32>)],
    per_owner: usize,
) -> Result<Vec<(NodeId, Vec<i64>)>, String> {
    let mut out: Vec<(NodeId, Vec<i64>)> = owners.iter().map(|(id, _)| (*id, Vec::new())).collect();
    for k in 0i64..1000 {
        let v = vnode_for_key(k, vnode_count);
        for ((_, vnodes), (_, bucket)) in owners.iter().zip(out.iter_mut()) {
            if vnodes.contains(&v) && bucket.len() < per_owner {
                bucket.push(k);
                break;
            }
        }
        if out.iter().all(|(_, b)| b.len() >= per_owner) {
            return Ok(out);
        }
    }
    let summary: Vec<String> = out
        .iter()
        .map(|(id, b)| format!("{:?}={}", id, b.len()))
        .collect();
    Err(format!(
        "ran out of candidates in 0..1000 wanting {per_owner}/owner: {}",
        summary.join(", ")
    ))
}

/// Latest persisted manifest epoch on this engine, or `0` when no
/// checkpoint exists yet. Reads through the engine's own
/// `CheckpointStore` so the result reflects what would be loaded on
/// restart — the right signal for per-node epoch-drift assertions.
pub async fn manifest_epoch(db: &LaminarDB) -> u64 {
    let store = match db.checkpoint_store() {
        Some(s) => s,
        None => return 0,
    };
    store
        .load_latest()
        .await
        .ok()
        .flatten()
        .map_or(0, |m| m.epoch)
}

/// `SELECT key, total FROM <mv>` on a single engine, returning the
/// materialized rows as `(key, total)` tuples sorted by key.
///
/// Reads via the engine's `SessionContext` directly — same path
/// DataFusion uses internally — so the call hits `MvTableProvider::scan`
/// (a one-shot snapshot) instead of subscribing to the per-cycle stream.
/// Snapshot reads are what the assertions want; the streaming-subscription
/// path would require coordinating with cycle boundaries.
pub async fn read_mv_sums(db: &LaminarDB, mv: &str) -> Vec<(i64, i64)> {
    let sql = format!("SELECT key, total FROM {mv}");
    let df = db.session_context().sql(&sql).await.expect("plan SELECT");
    let batches = df.collect().await.expect("collect SELECT");
    let mut rows: Vec<(i64, i64)> = Vec::new();
    for batch in batches {
        let key = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("key Int64");
        let total = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("total Int64");
        for i in 0..batch.num_rows() {
            rows.push((key.value(i), total.value(i)));
        }
    }
    rows.sort_by_key(|(k, _)| *k);
    rows
}
