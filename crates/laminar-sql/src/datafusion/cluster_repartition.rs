//! Cross-instance hash repartition. Replaces DataFusion's in-process
//! `RepartitionExec::Hash` between `AggregateExec::Partial` and
//! `AggregateExec::FinalPartitioned`. One output partition per owned
//! vnode; remote rows ship via `ShuffleSender`.

#![allow(clippy::disallowed_types)] // this is a cluster-only ExecutionPlan; not hot-path

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, OnceLock};

use arrow::compute::take;
use arrow_array::{RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::DataFusionError;
use futures::stream::{self, StreamExt};
use laminar_core::checkpoint::barrier::CheckpointBarrier;
use laminar_core::shuffle::{BarrierTracker, ShufflePeerId, ShuffleReceiver, ShuffleSender};
use laminar_core::state::{owned_vnodes, NodeId, VnodeRegistry};
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

/// Cross-instance hash repartition.
pub struct ClusterRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    /// Column indices hashed to pick a vnode. Only plain columns today;
    /// computed hash keys would need `PhysicalExpr` evaluation.
    hash_columns: Vec<usize>,
    registry: Arc<VnodeRegistry>,
    sender: Arc<ShuffleSender>,
    receiver: Arc<ShuffleReceiver>,
    self_id: NodeId,
    /// Peers we fan barriers out to; frozen at construction.
    peers: Vec<ShufflePeerId>,
    /// One output partition per owned vnode.
    owned: Vec<u32>,
    vnode_to_partition: HashMap<u32, usize>,
    schema: SchemaRef,
    properties: PlanProperties,
    runtime: OnceLock<Arc<RuntimeState>>,
}

struct RuntimeState {
    /// One receiver per output partition; claimed once by `execute(p)`.
    receivers: AsyncMutex<Vec<Option<mpsc::Receiver<RecordBatch>>>>,
    inject_barrier_tx: mpsc::UnboundedSender<CheckpointBarrier>,
    /// Carries the aligned checkpoint id for downstream wrappers.
    aligned_epoch_watch: watch::Receiver<u64>,
    _router: JoinHandle<()>,
    _dispatcher: JoinHandle<()>,
}

impl ClusterRepartitionExec {
    /// Handle on the vnode registry. Callers read `assignment_version()`
    /// to stamp state writes for the split-brain fence.
    #[must_use]
    pub fn registry(&self) -> &Arc<VnodeRegistry> {
        &self.registry
    }

    /// Construct the exec.
    ///
    /// # Errors
    /// Out-of-range hash columns or zero owned vnodes.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        hash_columns: Vec<usize>,
        registry: Arc<VnodeRegistry>,
        sender: Arc<ShuffleSender>,
        receiver: Arc<ShuffleReceiver>,
        self_id: NodeId,
    ) -> Result<Self> {
        let schema = input.schema();
        if let Some(&bad) = hash_columns.iter().find(|&&i| i >= schema.fields().len()) {
            return Err(DataFusionError::Plan(format!(
                "ClusterRepartitionExec: hash column {bad} out of range (schema has {} fields)",
                schema.fields().len()
            )));
        }

        let owned = owned_vnodes(&registry, self_id);
        if owned.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "ClusterRepartitionExec: instance {:?} owns no vnodes",
                self_id
            )));
        }
        let vnode_to_partition = owned.iter().enumerate().map(|(i, &v)| (v, i)).collect();

        // Enumerate distinct peers from the registry (everyone who
        // owns at least one vnode and isn't us). Frozen at construction
        // — dynamic membership is deferred work.
        let mut peer_set: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
        for v in 0..registry.vnode_count() {
            let o = registry.owner(v);
            if !o.is_unassigned() && o != self_id {
                peer_set.insert(o.0);
            }
        }
        let peers: Vec<ShufflePeerId> = peer_set.into_iter().collect();

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            // We advertise hash partitioning. Satisfies
            // `FinalPartitioned`'s required-input-distribution.
            Partitioning::UnknownPartitioning(owned.len()),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );

        Ok(Self {
            input,
            hash_columns,
            registry,
            sender,
            receiver,
            self_id,
            peers,
            owned,
            vnode_to_partition,
            schema,
            properties,
            runtime: OnceLock::new(),
        })
    }

    /// Inject a checkpoint barrier, fanning out to peers.
    ///
    /// # Errors
    /// Before `execute()` or after the router exits.
    pub fn inject_barrier(&self, barrier: CheckpointBarrier) -> Result<()> {
        let Some(runtime) = self.runtime.get() else {
            return Err(DataFusionError::Execution(
                "ClusterRepartitionExec::inject_barrier before execute()".into(),
            ));
        };
        runtime
            .inject_barrier_tx
            .send(barrier)
            .map_err(|_| DataFusionError::Execution("router task exited".into()))
    }

    /// `None` before `execute()`; otherwise a watch that fires each
    /// time the tracker aligns, carrying the checkpoint id.
    #[must_use]
    pub fn aligned_epoch_watch(&self) -> Option<watch::Receiver<u64>> {
        self.runtime.get().map(|rt| rt.aligned_epoch_watch.clone())
    }

    /// Spawn the router + dispatcher tasks on the first `execute()`.
    fn init_runtime(&self, context: &Arc<TaskContext>) -> Result<Arc<RuntimeState>> {
        if let Some(existing) = self.runtime.get() {
            return Ok(Arc::clone(existing));
        }

        let n_partitions = self.owned.len();
        let mut partition_txs: Vec<mpsc::Sender<RecordBatch>> = Vec::with_capacity(n_partitions);
        let mut receivers = Vec::with_capacity(n_partitions);
        for _ in 0..n_partitions {
            let (tx, rx) = mpsc::channel::<RecordBatch>(16);
            partition_txs.push(tx);
            receivers.push(Some(rx));
        }

        let input_stream = self.input.execute(0, Arc::clone(context))?;
        let hash_columns = self.hash_columns.clone();
        let registry = Arc::clone(&self.registry);
        let self_id = self.self_id;
        let sender = Arc::clone(&self.sender);
        let receiver = Arc::clone(&self.receiver);
        let vnode_to_partition = self.vnode_to_partition.clone();
        let peers = self.peers.clone();

        // Barrier plumbing:
        //   - inject_barrier_(tx|rx): external trigger → router
        //   - peer_barrier_(tx|rx):   dispatcher → router (after peer gossip)
        //   - aligned_(tx|rx):        router → subscribers (watch channel)
        let (inject_tx, inject_rx) = mpsc::unbounded_channel::<CheckpointBarrier>();
        let (peer_tx, peer_rx) = mpsc::unbounded_channel::<(ShufflePeerId, CheckpointBarrier)>();
        let (aligned_tx, aligned_rx) = watch::channel::<u64>(0);

        let router_txs = partition_txs.clone();
        let router_registry = Arc::clone(&registry);
        let router_vtp = vnode_to_partition.clone();
        let router_sender = Arc::clone(&sender);
        let router_peers = peers.clone();
        let router = tokio::spawn(async move {
            route_input_stream(
                input_stream,
                hash_columns,
                router_registry,
                self_id,
                router_vtp,
                router_sender,
                router_txs,
                router_peers,
                inject_rx,
                peer_rx,
                aligned_tx,
            )
            .await;
        });

        let dispatcher_txs = partition_txs;
        let dispatcher = tokio::spawn(async move {
            dispatch_inbound(receiver, vnode_to_partition, dispatcher_txs, peer_tx).await;
        });

        let state = Arc::new(RuntimeState {
            receivers: AsyncMutex::new(receivers),
            inject_barrier_tx: inject_tx,
            aligned_epoch_watch: aligned_rx,
            _router: router,
            _dispatcher: dispatcher,
        });
        Ok(Arc::clone(self.runtime.get_or_init(|| state)))
    }
}

impl Debug for ClusterRepartitionExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterRepartitionExec")
            .field("self_id", &self.self_id)
            .field("hash_columns", &self.hash_columns)
            .field("owned_vnodes", &self.owned.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ClusterRepartitionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "ClusterRepartitionExec: owned_vnodes={}, hash_columns={:?}",
                self.owned.len(),
                self.hash_columns
            ),
            DisplayFormatType::TreeRender => write!(f, "ClusterRepartitionExec"),
        }
    }
}

impl ExecutionPlan for ClusterRepartitionExec {
    fn name(&self) -> &'static str {
        "ClusterRepartitionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "ClusterRepartitionExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: children.swap_remove(0),
            hash_columns: self.hash_columns.clone(),
            registry: Arc::clone(&self.registry),
            sender: Arc::clone(&self.sender),
            receiver: Arc::clone(&self.receiver),
            self_id: self.self_id,
            peers: self.peers.clone(),
            owned: self.owned.clone(),
            vnode_to_partition: self.vnode_to_partition.clone(),
            schema: Arc::clone(&self.schema),
            properties: self.properties.clone(),
            // Fresh runtime on re-plan.
            runtime: OnceLock::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.owned.len() {
            return Err(DataFusionError::Plan(format!(
                "ClusterRepartitionExec: partition {partition} >= output partitions {}",
                self.owned.len(),
            )));
        }

        let runtime = self.init_runtime(&context)?;

        // Claim this partition's receiver. DataFusion contracts execute
        // each partition exactly once per plan execution; a second call
        // would have previously returned an empty stream silently,
        // masking plan-reuse bugs. Now we return a typed error so the
        // caller sees something went wrong.
        let schema = Arc::clone(&self.schema);
        let fut = async move {
            let mut guard = runtime.receivers.lock().await;
            guard[partition].take().ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(format!(
                    "ClusterRepartitionExec::execute called twice for \
                         partition {partition}; receivers are single-use"
                ))
            })
        };
        let stream = stream::once(fut).flat_map(move |maybe_rx| match maybe_rx {
            Ok(rx) => tokio_stream::wrappers::ReceiverStream::new(rx)
                .map(Ok)
                .boxed(),
            Err(e) => stream::once(async move { Err(e) }).boxed(),
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Per-row vnode, hashed via `arrow::row::RowConverter` on the key
/// columns (works for any Arrow type).
fn row_vnodes(batch: &RecordBatch, hash_columns: &[usize], vnode_count: u32) -> Vec<u32> {
    use arrow::row::{RowConverter, SortField};
    use laminar_core::state::key_hash;

    let cols: Vec<_> = hash_columns
        .iter()
        .map(|&i| Arc::clone(batch.column(i)))
        .collect();
    let fields: Vec<_> = cols
        .iter()
        .map(|c| SortField::new(c.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields).expect("row converter");
    let rows = converter.convert_columns(&cols).expect("convert rows");

    (0..batch.num_rows())
        .map(|row| {
            #[allow(clippy::cast_possible_truncation)]
            let v = (key_hash(rows.row(row).as_ref()) % u64::from(vnode_count)) as u32;
            v
        })
        .collect()
}

fn slice_for_vnode(batch: &RecordBatch, vnodes: &[u32], target_vnode: u32) -> Option<RecordBatch> {
    let indices: UInt32Array = vnodes
        .iter()
        .enumerate()
        .filter_map(|(i, &v)| {
            if v == target_vnode {
                u32::try_from(i).ok()
            } else {
                None
            }
        })
        .collect();
    if indices.is_empty() {
        return None;
    }
    let new_cols = batch
        .columns()
        .iter()
        .map(|c| take(c, &indices, None).expect("take"))
        .collect::<Vec<_>>();
    Some(RecordBatch::try_new(batch.schema(), new_cols).expect("rebuild"))
}

/// Drains data batches and barriers (local + peer), routes data to
/// local partitions or peer senders, and publishes aligned-epoch ids
/// on `aligned_tx`.
#[allow(clippy::too_many_arguments)]
async fn route_input_stream(
    input: SendableRecordBatchStream,
    hash_columns: Vec<usize>,
    registry: Arc<VnodeRegistry>,
    self_id: NodeId,
    vnode_to_partition: HashMap<u32, usize>,
    sender: Arc<ShuffleSender>,
    partition_txs: Vec<mpsc::Sender<RecordBatch>>,
    peers: Vec<ShufflePeerId>,
    mut inject_rx: mpsc::UnboundedReceiver<CheckpointBarrier>,
    mut peer_rx: mpsc::UnboundedReceiver<(ShufflePeerId, CheckpointBarrier)>,
    aligned_tx: watch::Sender<u64>,
) {
    let vnode_count = registry.vnode_count();
    let n_inputs = peers.len() + 1;
    let tracker = BarrierTracker::new(n_inputs);
    let peer_port: HashMap<ShufflePeerId, usize> =
        peers.iter().enumerate().map(|(i, &p)| (p, i + 1)).collect();

    // Once the local input stream ends we drop it so the select arm
    // becomes `pending` forever. Partition senders are also dropped
    // at input EOS so the downstream output streams terminate cleanly
    // — the router stays alive purely for barrier coordination.
    let mut input: Option<SendableRecordBatchStream> = Some(input);
    let mut partition_txs: Option<Vec<mpsc::Sender<RecordBatch>>> = Some(partition_txs);

    loop {
        tokio::select! {
            biased;
            // 1. External barrier trigger.
            Some(barrier) = inject_rx.recv() => {
                let _ = sender.fan_out_barrier(&peers, barrier).await;
                if let Some(aligned) = tracker.observe(0, barrier) {
                    let _ = aligned_tx.send(aligned.checkpoint_id);
                }
            }
            // 2. Peer barrier forwarded from the dispatcher.
            Some((from, barrier)) = peer_rx.recv() => {
                if let Some(&port) = peer_port.get(&from) {
                    if let Some(aligned) = tracker.observe(port, barrier) {
                        let _ = aligned_tx.send(aligned.checkpoint_id);
                    }
                }
            }
            // 3. Data from the local input. `pending` forever once the
            // input is exhausted so this arm drops out of the select.
            next = async {
                match input.as_mut() {
                    Some(s) => s.next().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(Ok(batch)) = next {
                    if batch.num_rows() == 0 { continue; }
                    if partition_txs.is_none() { continue; }
                    let row_vn = row_vnodes(&batch, &hash_columns, vnode_count);
                    let mut seen = row_vn.clone();
                    seen.sort_unstable();
                    seen.dedup();
                    let mut downstream_dropped = false;
                    for v in seen {
                        let Some(slice) = slice_for_vnode(&batch, &row_vn, v) else {
                            continue;
                        };
                        let owner = registry.owner(v);
                        if owner == self_id {
                            if let Some(&idx) = vnode_to_partition.get(&v) {
                                let send_res = partition_txs.as_ref().unwrap()[idx]
                                    .send(slice)
                                    .await;
                                if send_res.is_err() {
                                    downstream_dropped = true;
                                    break;
                                }
                            }
                        } else if !owner.is_unassigned() {
                            let msg = laminar_core::shuffle::ShuffleMessage::VnodeData(
                                v, slice,
                            );
                            // Drop on unreachable peer.
                            let _ = sender.send_to(owner.0, &msg).await;
                        }
                    }
                    if downstream_dropped {
                        partition_txs = None;
                        input = None;
                    }
                } else {
                    // EOS or error: drop senders so output streams
                    // terminate; keep router alive for barriers.
                    input = None;
                    partition_txs = None;
                }
            }
        }
    }
}

/// Consumes inbound shuffle frames: `VnodeData` to local partitions,
/// `Barrier` to the router's peer-barrier channel.
async fn dispatch_inbound(
    receiver: Arc<ShuffleReceiver>,
    vnode_to_partition: HashMap<u32, usize>,
    partition_txs: Vec<mpsc::Sender<RecordBatch>>,
    peer_barrier_tx: mpsc::UnboundedSender<(ShufflePeerId, CheckpointBarrier)>,
) {
    use laminar_core::shuffle::ShuffleMessage;
    while let Some((from, msg)) = receiver.recv().await {
        match msg {
            ShuffleMessage::VnodeData(vnode, batch) => {
                if batch.num_rows() == 0 {
                    continue;
                }
                let Some(&idx) = vnode_to_partition.get(&vnode) else {
                    // Sender mis-routed to a vnode we don't own — drop
                    // (a future observability hook can count these).
                    continue;
                };
                if partition_txs[idx].send(batch).await.is_err() {
                    return; // downstream dropped
                }
            }
            ShuffleMessage::Barrier(b) if peer_barrier_tx.send((from, b)).is_err() => return,
            // Hello is consumed by per_peer_loop; Close closes the
            // reader. Barriers that succeed fall through here.
            _ => {}
        }
    }
}
