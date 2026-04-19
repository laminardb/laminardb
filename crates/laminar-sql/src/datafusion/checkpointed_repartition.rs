//! Wraps [`ClusterRepartitionExec`] with checkpoint persistence: every
//! partial-state batch flowing to Final is mirrored into a per-partition
//! buffer. On barrier alignment the buffer is concatenated, serialised
//! as Arrow IPC, and written via `state_backend.write_partial`. Startup
//! can pre-seed from a prior epoch via [`CheckpointedRepartitionExec::with_recovery_epoch`].
//!
//! We checkpoint the shuffled partials rather than `GroupsAccumulator`'s
//! internal state because DataFusion doesn't expose the accumulator.

#![allow(clippy::disallowed_types)]

use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::DataFusionError;
use futures::stream::{self, StreamExt};
use laminar_core::serialization::{deserialize_batch_stream, serialize_batch_stream};
use laminar_core::state::{StateBackend, VnodeRegistry};
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::cluster_repartition::ClusterRepartitionExec;

/// Sentinel stored in the recovery-epoch handle to mean "no recovery
/// epoch yet known." Zero is safe: checkpoint epochs start at 1.
const RECOVERY_EPOCH_UNSET: u64 = 0;

/// Passes every partition's batches through unchanged and mirrors them
/// into a per-partition buffer that's flushed to `state_backend` on
/// each aligned checkpoint.
pub struct CheckpointedRepartitionExec {
    inner: Arc<ClusterRepartitionExec>,
    state_backend: Arc<dyn StateBackend>,
    /// `owned_vnodes[i]` is the vnode whose partial partition `i`
    /// persists to.
    owned_vnodes: Vec<u32>,
    buffers: Arc<Mutex<Vec<Vec<RecordBatch>>>>,
    schema: SchemaRef,
    last_checkpoint: watch::Sender<u64>,
    /// Shared recovery-epoch handle. Read lazily at every `execute()`
    /// so a plan that was compiled (and cached) before recovery
    /// completed still picks up the freshest value. A separate
    /// `.recovery_epoch = Some(N)` field would have frozen the value
    /// at construction time and left cached plans stale. The sentinel
    /// [`RECOVERY_EPOCH_UNSET`] means "no recovery needed / not yet
    /// known" and skips the replay stream.
    recovery_epoch: Arc<AtomicU64>,
    runtime: OnceLock<Arc<RuntimeState>>,
}

struct RuntimeState {
    _snapshotter: JoinHandle<()>,
}

impl CheckpointedRepartitionExec {
    /// Wrap a `ClusterRepartitionExec` with per-epoch snapshotting.
    #[must_use]
    pub fn new(
        inner: Arc<ClusterRepartitionExec>,
        state_backend: Arc<dyn StateBackend>,
        owned_vnodes: Vec<u32>,
    ) -> Self {
        let buffers = Arc::new(Mutex::new(vec![
            Vec::<RecordBatch>::new();
            owned_vnodes.len()
        ]));
        let schema = inner.schema();
        let (tx, _) = watch::channel::<u64>(0);
        Self {
            inner,
            state_backend,
            owned_vnodes,
            buffers,
            schema,
            last_checkpoint: tx,
            recovery_epoch: Arc::new(AtomicU64::new(RECOVERY_EPOCH_UNSET)),
            runtime: OnceLock::new(),
        }
    }

    /// Pre-seed each partition from `read_partial(vnode, epoch)` on
    /// first `execute()`. One replay batch per partition, before live
    /// data.
    ///
    /// Takes `self` and stores into the exec's private handle. Use
    /// [`with_recovery_epoch_handle`](Self::with_recovery_epoch_handle)
    /// when the caller wants to set (or later update) the epoch from
    /// outside the plan — e.g., `DistributedAggregateRule` sharing one
    /// handle across every exec it mints so that a `load` from a
    /// `CheckpointStore` that completes after DDL can still reach
    /// already-constructed plans.
    #[must_use]
    pub fn with_recovery_epoch(self, epoch: u64) -> Self {
        self.recovery_epoch.store(epoch, Ordering::Release);
        self
    }

    /// Swap the exec's recovery-epoch handle for a shared one. The
    /// caller retains a clone and can publish the epoch at any time;
    /// the exec picks up the current value on every `execute()` call.
    #[must_use]
    pub fn with_recovery_epoch_handle(mut self, handle: Arc<AtomicU64>) -> Self {
        self.recovery_epoch = handle;
        self
    }

    /// Shared handle to the recovery-epoch atomic. Lets callers writing
    /// recovery-state orchestration (e.g., `db.start()` post-loading
    /// `last_committed_epoch` from the checkpoint store) publish into
    /// every cached plan that was compiled against the same rule.
    #[must_use]
    pub fn recovery_epoch_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.recovery_epoch)
    }

    /// Subscribe to checkpoint-completion events.
    #[must_use]
    pub fn checkpoint_watch(&self) -> watch::Receiver<u64> {
        self.last_checkpoint.subscribe()
    }

    fn init_runtime(&self) -> Arc<RuntimeState> {
        Arc::clone(self.runtime.get_or_init(|| {
            let aligned_rx = self
                .inner
                .aligned_epoch_watch()
                .expect("inner exec runtime initialised before snapshot task");
            let buffers = Arc::clone(&self.buffers);
            let owned = self.owned_vnodes.clone();
            let schema = Arc::clone(&self.schema);
            let backend = Arc::clone(&self.state_backend);
            let registry = Arc::clone(self.inner.registry());
            let tx = self.last_checkpoint.clone();
            let handle = tokio::spawn(async move {
                snapshot_loop(aligned_rx, buffers, owned, schema, backend, registry, tx)
                    .await;
            });
            Arc::new(RuntimeState {
                _snapshotter: handle,
            })
        }))
    }
}

impl Debug for CheckpointedRepartitionExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CheckpointedRepartitionExec")
            .field("owned_vnodes", &self.owned_vnodes.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CheckpointedRepartitionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "CheckpointedRepartitionExec: owned_vnodes={}",
                self.owned_vnodes.len(),
            ),
            DisplayFormatType::TreeRender => write!(f, "CheckpointedRepartitionExec"),
        }
    }
}

impl ExecutionPlan for CheckpointedRepartitionExec {
    fn name(&self) -> &'static str {
        "CheckpointedRepartitionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Not exposed for re-planning — this wrapper is installed
        // post-optimisation.
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "CheckpointedRepartitionExec takes no public children".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Inner execute() must run first so `aligned_epoch_watch()` is
        // populated before we spawn the snapshot task.
        let upstream = self.inner.execute(partition, context)?;
        let _ = self.init_runtime();

        let buffers = Arc::clone(&self.buffers);
        let schema = Arc::clone(&self.schema);

        // Replay is prepended; not mirrored into the buffer — the data
        // is already durable and would double-count next epoch. The
        // recovery epoch is read from the shared atomic here, not
        // captured at construction — so a plan compiled before
        // recovery completed still picks up the freshest value.
        let recovery_fut = {
            let backend = Arc::clone(&self.state_backend);
            let vnode = self.owned_vnodes[partition];
            let epoch_snapshot = self.recovery_epoch.load(Ordering::Acquire);
            async move {
                if epoch_snapshot == RECOVERY_EPOCH_UNSET {
                    return None;
                }
                let bytes = backend.read_partial(vnode, epoch_snapshot).await.ok()??;
                deserialize_batch_stream(&bytes).ok()
            }
        };
        let recovery_stream = stream::once(recovery_fut)
            .filter_map(|opt| async move { opt.map(Ok) });

        let intercepted = upstream.map(move |res| {
            if let Ok(ref batch) = res {
                buffers.lock()[partition].push(batch.clone());
            }
            res
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            recovery_stream.chain(intercepted),
        )))
    }
}

/// Bound on concurrent per-vnode `write_partial` calls during a single
/// snapshot fire. Checkpoint time scales with `owned_vnodes.len() /
/// SNAPSHOT_CONCURRENCY` instead of linearly: at 256 vnodes × 100ms
/// S3 latency, serial was ~25s/epoch; with 32-way concurrency it's
/// ~800ms. The cap also protects against flooding an object store
/// whose throttle behavior gets nasty under a thundering herd.
const SNAPSHOT_CONCURRENCY: usize = 32;

/// On each aligned-epoch fire, serialise every partition's buffered
/// batches and `write_partial(vnode, epoch, assignment_version, bytes)`.
/// The assignment version is re-read from the registry at write time
/// so the Phase 1.4 fence sees the freshest generation this instance
/// believes it owns the vnode at.
///
/// Per-vnode work runs up to [`SNAPSHOT_CONCURRENCY`]-way in parallel
/// via `buffer_unordered`. The previous serial loop put the state
/// write I/O directly on the barrier critical path, so a 256-vnode
/// deployment spent 25s/checkpoint awaiting S3 PUTs one at a time.
async fn snapshot_loop(
    mut aligned_rx: watch::Receiver<u64>,
    buffers: Arc<Mutex<Vec<Vec<RecordBatch>>>>,
    owned_vnodes: Vec<u32>,
    schema: SchemaRef,
    backend: Arc<dyn StateBackend>,
    registry: Arc<VnodeRegistry>,
    last_checkpoint: watch::Sender<u64>,
) {
    loop {
        if aligned_rx.changed().await.is_err() {
            return; // inner exec dropped
        }
        let epoch = *aligned_rx.borrow();
        if epoch == 0 {
            continue; // spurious wakeup — initial value
        }
        // Swap in fresh empties so the intercept path never indexes
        // into a shorter Vec.
        let snapshots: Vec<Vec<RecordBatch>> = {
            let mut guard = buffers.lock();
            std::mem::replace(&mut *guard, vec![Vec::new(); owned_vnodes.len()])
        };

        // Build the concurrent work stream. Each future captures only
        // cheap Arc clones — `schema`, `backend`, `registry` — so the
        // fan-out is memory-light even with hundreds of vnodes.
        stream::iter(snapshots.into_iter().enumerate())
            .filter_map(|(idx, batches)| {
                let vnode = owned_vnodes[idx];
                async move {
                    if batches.is_empty() {
                        None
                    } else {
                        Some((vnode, batches))
                    }
                }
            })
            .map(|(vnode, batches)| {
                let schema = Arc::clone(&schema);
                let backend = Arc::clone(&backend);
                let registry = Arc::clone(&registry);
                async move {
                    let combined = match concat_batches(&schema, &batches) {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!(
                                vnode, epoch, error = %e,
                                "checkpointed repartition: concat_batches failed; \
                                 skipping partial write (state will lag by one epoch)",
                            );
                            return;
                        }
                    };
                    let bytes = match serialize_batch_stream(&combined) {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::error!(
                                vnode, epoch, error = %e,
                                "checkpointed repartition: Arrow IPC encode failed",
                            );
                            return;
                        }
                    };
                    let caller_version = registry.assignment_version();
                    if let Err(e) = backend
                        .write_partial(vnode, epoch, caller_version, Bytes::from(bytes))
                        .await
                    {
                        tracing::error!(
                            vnode, epoch, caller_version, error = %e,
                            "checkpointed repartition: write_partial failed — \
                             peer's durability gate will reject this epoch",
                        );
                    }
                }
            })
            .buffer_unordered(SNAPSHOT_CONCURRENCY)
            .for_each(|()| async {})
            .await;

        let _ = last_checkpoint.send(epoch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::cluster_repartition::ClusterRepartitionExec;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::{SessionStateBuilder, TaskContext};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionConfig;
    use laminar_core::checkpoint::barrier::{flags, CheckpointBarrier};
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{InProcessBackend, NodeId, VnodeRegistry};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn batch(rows: impl IntoIterator<Item = (i64, i64)>) -> RecordBatch {
        let (keys, vals): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap()
    }

    fn ctx() -> Arc<TaskContext> {
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .build();
        Arc::new(TaskContext::from(&state))
    }

    /// Solo-cluster flow: one node owns every vnode, data routes
    /// entirely locally, barrier fires locally, snapshot writes the
    /// buffered partial state to the backend.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn snapshot_writes_partial_state_on_alignment() {
        // Global guard against indefinite hangs — makes failures
        // actionable instead of leaving a zombie test binary.
        let test = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            snapshot_writes_partial_state_on_alignment_body(),
        );
        test.await.expect("test timed out");
    }

    async fn snapshot_writes_partial_state_on_alignment_body() {
        // 4 vnodes, single owner → all output partitions are local.
        let registry = Arc::new(VnodeRegistry::single_owner(4, NodeId(1)));
        let recv = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );
        let sender = Arc::new(ShuffleSender::new(1));
        let backend: Arc<dyn StateBackend> = Arc::new(InProcessBackend::new(4));

        let inp: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
            &[vec![
                batch([(1, 10), (2, 20), (3, 30), (4, 40)]),
                batch([(5, 50), (6, 60)]),
            ]],
            schema(),
            None,
        )
        .unwrap();

        let inner = Arc::new(
            ClusterRepartitionExec::try_new(
                inp,
                vec![0], // hash by key
                Arc::clone(&registry),
                sender,
                recv,
                NodeId(1),
            )
            .unwrap(),
        );
        let owned: Vec<u32> = (0..4).collect();
        let wrap = Arc::new(CheckpointedRepartitionExec::new(
            Arc::clone(&inner),
            Arc::clone(&backend),
            owned.clone(),
        ));

        // Start every output partition so the inner router drives
        // input → per-partition channels, and the intercept populates
        // the buffers. Each stream stays alive (production streams
        // don't naturally terminate); we poll only enough to let a
        // batch or two flow through.
        let n = wrap.properties().partitioning.partition_count();
        let mut streams: Vec<_> = (0..n).map(|p| wrap.execute(p, ctx()).unwrap()).collect();
        // Consume a few items non-blockingly from each stream.
        for s in &mut streams {
            for _ in 0..4 {
                let tick = tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    s.next(),
                )
                .await;
                if !matches!(tick, Ok(Some(Ok(_)))) {
                    break;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut checkpoint_rx = wrap.checkpoint_watch();
        inner
            .inject_barrier(CheckpointBarrier {
                checkpoint_id: 42,
                epoch: 42,
                flags: flags::FULL_SNAPSHOT,
            })
            .unwrap();

        // Wait for the snapshot loop to land the checkpoint.
        tokio::time::timeout(std::time::Duration::from_secs(3), checkpoint_rx.changed())
            .await
            .expect("checkpoint deadline")
            .unwrap();
        assert_eq!(*checkpoint_rx.borrow(), 42);

        // Every vnode that saw data now has a partial for epoch 42.
        // At least one vnode should have data (6 rows distributed
        // across 4 vnodes won't all land on one).
        let mut nonempty = 0;
        for v in 0..4 {
            if backend.read_partial(v, 42).await.unwrap().is_some() {
                nonempty += 1;
            }
        }
        assert!(
            nonempty >= 1,
            "expected at least one vnode partial persisted on MinIO/in-process backend",
        );
    }

    /// Exercises the Phase E.1 replay path: seed the state backend
    /// with a canonical RecordBatch under (vnode, epoch); start a
    /// fresh exec with `with_recovery_epoch(epoch)`; assert the first
    /// batch the stream yields is the recovered state, before any live
    /// upstream data.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_replay_emits_prior_state_first() {
        let test = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            recovery_replay_body(),
        );
        test.await.expect("test timed out");
    }

    async fn recovery_replay_body() {
        use laminar_core::serialization::serialize_batch_stream;

        // 1-vnode topology: keeps the partition-to-vnode map trivial.
        let registry = Arc::new(VnodeRegistry::single_owner(1, NodeId(1)));
        let recv = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );
        let sender = Arc::new(ShuffleSender::new(1));
        let backend: Arc<dyn StateBackend> = Arc::new(InProcessBackend::new(1));

        // Pre-seed (vnode=0, epoch=9) with a canonical marker batch.
        let seed = batch([(42, 420), (43, 430)]);
        let seed_bytes = Bytes::from(serialize_batch_stream(&seed).unwrap());
        backend.write_partial(0, 9, 0, seed_bytes).await.unwrap();

        // Fresh exec, empty upstream — the only output should be the
        // recovery batch.
        let inp: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![]], schema(), None).unwrap();
        let inner = Arc::new(
            ClusterRepartitionExec::try_new(
                inp,
                vec![0],
                Arc::clone(&registry),
                sender,
                recv,
                NodeId(1),
            )
            .unwrap(),
        );
        let wrap = CheckpointedRepartitionExec::new(inner, backend, vec![0])
            .with_recovery_epoch(9);

        let mut s = wrap.execute(0, ctx()).unwrap();
        let first = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            s.next(),
        )
        .await
        .expect("deadline")
        .expect("stream closed")
        .expect("error in stream");

        assert_eq!(first.num_rows(), 2, "recovery batch must contain 2 rows");
        let keys = first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(keys, vec![42, 43]);
    }

    /// Covers the "DDL compiled before recovery completed" bonus bug:
    /// the exec is constructed with no recovery epoch known (sentinel
    /// 0), and only later does the checkpoint store hand back the
    /// `last_committed_epoch`. The publish into the shared handle
    /// must reach the already-constructed exec — the previous
    /// `Option<u64>` field would have frozen the value at construct
    /// time and lost the late publish.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_epoch_published_after_construct_reaches_exec() {
        use laminar_core::serialization::serialize_batch_stream;

        let registry = Arc::new(VnodeRegistry::single_owner(1, NodeId(1)));
        let recv = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );
        let sender = Arc::new(ShuffleSender::new(1));
        let backend: Arc<dyn StateBackend> = Arc::new(InProcessBackend::new(1));

        // Pre-seed (vnode=0, epoch=9) — the recovery target.
        let seed = batch([(99, 990)]);
        let seed_bytes = Bytes::from(serialize_batch_stream(&seed).unwrap());
        backend.write_partial(0, 9, 0, seed_bytes).await.unwrap();

        // Construct the exec WITHOUT knowing the recovery epoch (DDL
        // compiles first, checkpoint-store load races alongside).
        let inp: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![]], schema(), None).unwrap();
        let inner = Arc::new(
            ClusterRepartitionExec::try_new(
                inp,
                vec![0],
                Arc::clone(&registry),
                sender,
                recv,
                NodeId(1),
            )
            .unwrap(),
        );
        let wrap = CheckpointedRepartitionExec::new(inner, backend, vec![0]);
        // Stash a shared handle — stands in for the rule-side handle
        // that `db.start()` would write to after loading the epoch.
        let handle = wrap.recovery_epoch_handle();

        // Publish the epoch AFTER construction — the key scenario.
        handle.store(9, Ordering::Release);

        let mut s = wrap.execute(0, ctx()).unwrap();
        let first = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            s.next(),
        )
        .await
        .expect("deadline")
        .expect("stream closed")
        .expect("error in stream");

        assert_eq!(first.num_rows(), 1, "recovery batch must have the seeded row");
        let keys = first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            keys,
            vec![99],
            "late publish into the handle must reach the already-\
             constructed exec — otherwise the recovery stream is empty",
        );
    }
}
