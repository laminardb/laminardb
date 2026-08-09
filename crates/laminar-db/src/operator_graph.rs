//! Operator graph: wires streaming SQL operators into a DAG and drives them in topological order.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use laminar_connectors::connector::{schema_with_source_row_positions, strip_source_row_positions};
use laminar_core::state::{KeyGroupCount, DEFAULT_KEY_GROUP_COUNT};
use laminar_sql::datafusion::live_source::{LiveSourceHandle, LiveSourceProvider};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::config::BackpressurePolicy;
use crate::db::exact_table_reference;
use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
#[cfg(feature = "cluster")]
use crate::operator::capability::ClusterExecutionStatus;
use crate::operator::capability::{OperatorCapability, OperatorImplementation, OperatorStateClass};
#[cfg(feature = "cluster")]
use crate::operator::RetainedBatch;
use crate::sql_analysis::{
    apply_topk_filter, detect_stream_join_query, detect_unbounded_join_steps,
    extract_table_references, has_join_clause, join_clause_count, temporal_projection_sql,
    StreamJoinDetection,
};
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::{
    OrderOperatorConfig, TemporalJoinTranslatorConfig, WindowOperatorConfig,
};

#[cfg(feature = "cluster")]
mod vnode_transition;

/// Cached retained managed-state accounting reported by one operator.
///
/// These are operator-defined accounting bytes, not allocator usage or process RSS. Lifecycle
/// phases are separate so a prepared or retired replacement is not hidden by the live total.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ManagedStateAccountingSnapshot {
    pub(crate) live: usize,
    pub(crate) prepared: usize,
    pub(crate) retired: usize,
}

/// One vnode slot captured at an aligned checkpoint cut.
///
/// `None` means the slot is unchanged from the preceding committed checkpoint. The checkpoint
/// coordinator resolves it to that manifest's direct frame reference; operators never persist
/// ancestry or backend-specific metadata.
#[derive(Debug)]
pub(crate) struct CapturedVnodeState {
    pub(crate) vnode: u32,
    pub(crate) state: Option<StateFrameCapture>,
}

const GRAPH_CHECKPOINT_CAPTURE_OVERHEAD: u64 = 256;
const GRAPH_CHECKPOINT_ENTRY_OVERHEAD: u64 = 128;

type DeferredStateFrameEncoder =
    Box<dyn FnOnce(usize) -> Result<EncodedStateFrame, DbError> + Send + 'static>;

pub(crate) struct EncodedStateFrame {
    state: bytes::Bytes,
    retained_bytes: u64,
}

impl EncodedStateFrame {
    pub(crate) fn from_vec(state: Vec<u8>) -> Self {
        let retained_bytes = u64::try_from(state.capacity()).unwrap_or(u64::MAX);
        Self {
            state: bytes::Bytes::from(state),
            retained_bytes,
        }
    }

    pub(crate) fn shared(state: bytes::Bytes) -> Self {
        Self {
            state,
            retained_bytes: 0,
        }
    }

    pub(crate) fn payload_len(&self) -> usize {
        self.state.len()
    }

    pub(crate) fn bytes(&self) -> &bytes::Bytes {
        &self.state
    }

    #[cfg(test)]
    pub(crate) fn into_bytes(self) -> bytes::Bytes {
        self.state
    }
}

pub(crate) enum StateFrameCapture {
    Encoded {
        state: bytes::Bytes,
        retained_bytes: u64,
    },
    Deferred {
        retained_bytes: u64,
        encode: DeferredStateFrameEncoder,
    },
}

impl StateFrameCapture {
    pub(crate) fn encoded(state: Vec<u8>) -> Self {
        let state = EncodedStateFrame::from_vec(state);
        Self::Encoded {
            state: state.state,
            retained_bytes: state.retained_bytes,
        }
    }

    pub(crate) fn encoded_static(state: &'static [u8]) -> Self {
        Self::Encoded {
            state: bytes::Bytes::from_static(state),
            retained_bytes: 0,
        }
    }

    pub(crate) fn deferred(
        retained_bytes: u64,
        encode: impl FnOnce(usize) -> Result<EncodedStateFrame, DbError> + Send + 'static,
    ) -> Self {
        Self::Deferred {
            retained_bytes,
            encode: Box::new(encode),
        }
    }

    pub(crate) fn retained_bytes(&self) -> u64 {
        match self {
            Self::Encoded { retained_bytes, .. } | Self::Deferred { retained_bytes, .. } => {
                *retained_bytes
            }
        }
    }

    pub(crate) fn materialize(
        self,
        staged_bytes: &mut u64,
        max_staged_bytes: u64,
    ) -> Result<bytes::Bytes, DbError> {
        match self {
            Self::Encoded { state, .. } => Ok(state),
            Self::Deferred {
                retained_bytes,
                encode,
            } => {
                let headroom = max_staged_bytes.checked_sub(*staged_bytes).ok_or_else(|| {
                    DbError::Checkpoint(
                        "operator captures exceeded their staged-state budget".into(),
                    )
                })?;
                let limit = usize::try_from(headroom).unwrap_or(usize::MAX);
                let state = encode(limit)?;
                if state.retained_bytes > headroom {
                    return Err(DbError::Checkpoint(format!(
                        "operator state frame retains {} bytes; staged-state headroom is {headroom} bytes",
                        state.retained_bytes
                    )));
                }
                *staged_bytes = staged_bytes
                    .checked_sub(retained_bytes)
                    .and_then(|bytes| bytes.checked_add(state.retained_bytes))
                    .filter(|bytes| *bytes <= max_staged_bytes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(
                            "operator state ownership transfer exceeded its staged-state budget"
                                .into(),
                        )
                    })?;
                Ok(state.state)
            }
        }
    }
}

impl std::fmt::Debug for StateFrameCapture {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encoded {
                state,
                retained_bytes,
            } => formatter
                .debug_struct("EncodedStateFrame")
                .field("bytes", &state.len())
                .field("retained_bytes", retained_bytes)
                .finish(),
            Self::Deferred { retained_bytes, .. } => formatter
                .debug_struct("DeferredStateFrame")
                .field("retained_bytes", retained_bytes)
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct InputFrontier {
    pub(crate) watermark: Option<i64>,
    pub(crate) idle: bool,
}

impl InputFrontier {
    const fn from_watermark(watermark: i64) -> Self {
        Self {
            watermark: if watermark == i64::MIN {
                None
            } else {
                Some(watermark)
            },
            idle: false,
        }
    }

    const fn watermark_or_min(self) -> i64 {
        match self.watermark {
            Some(watermark) => watermark,
            None => i64::MIN,
        }
    }

    pub(crate) fn with_watermark_ceiling(mut self, ceiling: Option<i64>) -> Self {
        if let Some(ceiling) = ceiling {
            self.watermark = if ceiling == i64::MIN {
                None
            } else {
                self.watermark.map(|watermark| watermark.min(ceiling))
            };
        }
        self
    }

    pub(crate) fn held_at(mut self, hold: Option<i64>) -> Self {
        if let Some(hold) = hold {
            self = self.with_watermark_ceiling(Some(hold));
            self.idle = false;
        }
        self
    }
}

pub(crate) fn merge_input_frontiers(
    frontiers: &[InputFrontier],
    fallback_watermark: i64,
) -> InputFrontier {
    merge_input_frontier_iter(frontiers.iter().copied(), fallback_watermark)
}

pub(crate) fn merge_input_frontier_iter(
    frontiers: impl IntoIterator<Item = InputFrontier>,
    fallback_watermark: i64,
) -> InputFrontier {
    let mut active_seen = false;
    let mut channel_seen = false;
    let mut active_watermark = Some(i64::MAX);
    let mut idle_watermark = None;
    for frontier in frontiers {
        channel_seen = true;
        if frontier.idle {
            if let Some(watermark) = frontier.watermark {
                idle_watermark =
                    Some(idle_watermark.map_or(watermark, |known: i64| known.max(watermark)));
            }
        } else {
            active_seen = true;
            active_watermark = match (active_watermark, frontier.watermark) {
                (Some(current), Some(watermark)) => Some(current.min(watermark)),
                _ => None,
            };
        }
    }
    if active_seen {
        InputFrontier {
            watermark: active_watermark,
            idle: false,
        }
    } else if channel_seen {
        InputFrontier {
            watermark: idle_watermark,
            idle: true,
        }
    } else {
        InputFrontier::from_watermark(fallback_watermark)
    }
}

impl ManagedStateAccountingSnapshot {
    fn total_bytes(self) -> usize {
        self.live
            .saturating_add(self.prepared)
            .saturating_add(self.retired)
    }

    #[cfg(any(feature = "cluster", test))]
    fn observe_transient(&mut self, observation: Self) {
        self.prepared = self.prepared.max(observation.prepared);
        self.retired = self.retired.max(observation.retired);
    }
}

#[async_trait]
pub(crate) trait GraphOperator: Send {
    /// Admission-neutral inventory of this implementation's current cluster state shape.
    ///
    /// This method is intentionally mandatory: a new physical operator must be classified before
    /// it compiles. Cluster DDL admission does not consume this descriptor yet.
    fn cluster_capability(&self) -> OperatorCapability;

    /// Return cached retained-state accounting for cold-cadence metrics publication.
    ///
    /// Implementations must not scan per-key working state here. A bounded topology walk over
    /// cached counters is permitted. Operators without managed accounting return `None`, which is
    /// also the admission-neutral default.
    fn managed_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        None
    }

    /// Install the pipeline-wide retained-state limit for bounded restore preflight.
    fn set_managed_state_budget(&mut self, _bytes: usize) {}

    /// Build and install the operator's declared managed working state before recovery.
    ///
    /// The graph calls this only for operators whose capability declares a managed-state
    /// contract. The rejecting default makes a newly declared participant fail during startup
    /// instead of silently staging unvalidated checkpoint bytes.
    async fn initialize_managed_state(&mut self) -> Result<(), DbError> {
        Err(DbError::Checkpoint(
            "operator declares managed state but does not implement managed-state initialization"
                .into(),
        ))
    }

    /// `watermarks[i]` is the upstream output watermark for `inputs[i]`.
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError>;

    async fn process_with_frontiers(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontiers: &[InputFrontier],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let watermarks: smallvec::SmallVec<[i64; 2]> = frontiers
            .iter()
            .map(|frontier| frontier.watermark_or_min())
            .collect();
        self.process(inputs, &watermarks).await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError>;

    fn checkpoint_capture(
        &mut self,
        max_capture_bytes: u64,
    ) -> Result<Option<StateFrameCapture>, DbError> {
        let Some(checkpoint) = self.checkpoint()? else {
            return Ok(None);
        };
        let capture = StateFrameCapture::encoded(checkpoint.data);
        if capture.retained_bytes() > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "operator whole-state capture retains {} bytes; capture headroom is {max_capture_bytes} bytes",
                capture.retained_bytes()
            )));
        }
        Ok(Some(capture))
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Err(DbError::Checkpoint(
            "operator does not accept checkpoint state".into(),
        ))
    }

    /// Derive the output frontier from the merged input frontier.
    fn output_frontier(&self, input: InputFrontier) -> InputFrontier {
        input
    }

    /// Safe output frontier reconstructed from retained checkpoint work. Restore uses this seed
    /// before any cycle runs; returning a value asserts that the durable cut had reached it.
    #[cfg(feature = "cluster")]
    fn restored_output_frontier(&self) -> Option<InputFrontier> {
        None
    }

    /// Whether the operator can accept new input this cycle. When `false`, input
    /// stays buffered and the operator is still stepped with empty input to drain.
    fn wants_input(&self) -> bool {
        true
    }

    /// Whether barrier alignment retained shuffle input that must replay before vnode handoff.
    fn checkpoint_aligned_replay_pending(&self) -> bool {
        false
    }

    /// Whether non-snapshotable internal work must finish before checkpoint capture.
    fn checkpoint_drain_pending(&self) -> bool {
        false
    }

    /// Retain a peer-shipped shuffle batch as channel state outside the normal `process` path so
    /// the barrier-aligned row and its pending downstream emission enter the snapshot together.
    #[cfg(feature = "cluster")]
    fn stage_checkpointed_shuffle(
        &mut self,
        stage: &str,
        _batch: RetainedBatch,
        _watermark: i64,
    ) -> Result<(), DbError> {
        Err(DbError::Pipeline(format!(
            "operator does not accept checkpointed shuffle stage '{stage}'"
        )))
    }

    /// Apply one peer frontier after all earlier batches from that ordered stage were retained.
    #[cfg(feature = "cluster")]
    fn stage_checkpointed_shuffle_frontier(
        &mut self,
        stage: &str,
        _peer: u64,
        _frontier: InputFrontier,
        _assignment_version: u64,
        _recovery_gen: u64,
    ) -> Result<(), DbError> {
        Err(DbError::Pipeline(format!(
            "operator does not accept ordered shuffle frontier stage '{stage}'"
        )))
    }

    /// Capture the requested vnode slots in canonical order. `None` is reserved for operators
    /// without vnode-managed state.
    fn checkpoint_vnodes(
        &mut self,
        _required_vnodes: &[u32],
        _vnode_count: u32,
        _max_capture_bytes: u64,
    ) -> Result<Option<Vec<CapturedVnodeState>>, DbError> {
        Ok(None)
    }

    /// Restore one full vnode frame during startup before the graph accepts input.
    fn restore_vnode(
        &mut self,
        vnode: u32,
        _vnode_count: u32,
        _state: &[u8],
    ) -> Result<(), DbError> {
        Err(DbError::Checkpoint(format!(
            "operator does not accept vnode checkpoint state for vnode {vnode}"
        )))
    }

    /// Prepare this operator's exact revoke/restore batch without mutating live state.
    ///
    /// A successful implementation owns one unpublished prepared replacement until `abort` or
    /// `publish`. The assignment fence is part of the input so preparation cannot be reused under
    /// different ownership authority.
    #[cfg(feature = "cluster")]
    fn prepare_vnode_transition(
        &mut self,
        _transition: ManagedVnodeTransition<'_>,
    ) -> Result<(), DbError> {
        Err(DbError::Checkpoint(
            "operator declares managed vnode state but does not implement transition preparation"
                .into(),
        ))
    }

    /// Discard an unpublished prepared replacement. This must not mutate live state.
    #[cfg(feature = "cluster")]
    fn abort_vnode_transition(&mut self) {}

    /// Publish the previously prepared replacement synchronously.
    ///
    /// Preparation must reserve and validate everything needed by this operation. Publication is
    /// deliberately infallible so the graph can expose all managed operators at one authority cut.
    #[cfg(feature = "cluster")]
    fn publish_vnode_transition(&mut self) {
        panic!("operator published vnode state without a prepared lifecycle implementation");
    }

    /// Retire state displaced by publication after graph authority locks have been released.
    #[cfg(feature = "cluster")]
    fn finish_vnode_transition(&mut self) {}

    /// Test-only revocation adapter for fault-containment probes that mutate live state.
    #[cfg(all(feature = "cluster", test))]
    fn drop_owned_vnodes(&mut self, _revoked: &FxHashSet<u32>) -> Result<(), DbError> {
        Err(DbError::Checkpoint(
            "test operator does not implement vnode revocation".into(),
        ))
    }

    /// Force the next capture to include full bytes for every requested vnode. This is required
    /// after a destructive dirty capture fails before the checkpoint commits.
    fn force_full_vnode_capture(&mut self) {}
}

/// One authoritative full vnode image prepared for a managed operator.
#[cfg(feature = "cluster")]
pub(crate) struct ManagedVnodeRestore<'a> {
    pub(crate) participant_id: u64,
    pub(crate) vnode: u32,
    pub(crate) state: &'a [u8],
}

/// One donor's whole-operator frame used to establish a portable handoff cut.
#[cfg(feature = "cluster")]
pub(crate) struct ManagedWholeRestore<'a> {
    pub(crate) participant_id: u64,
    pub(crate) state: &'a [u8],
}

/// Whether a managed transition advances live state or initializes a new graph from the
/// immediately preceding committed assignment.
#[cfg(feature = "cluster")]
#[derive(Clone, Copy)]
pub(crate) enum ManagedVnodeTransitionMode<'a> {
    Live,
    CheckpointBootstrap {
        predecessor_owners: &'a [laminar_core::state::NodeId],
    },
}

/// Exact operator-local projection of one graph vnode transition.
#[cfg(feature = "cluster")]
pub(crate) struct ManagedVnodeTransition<'a> {
    pub(crate) predecessor: &'a laminar_core::checkpoint::CheckpointAssignmentFence,
    pub(crate) target: &'a laminar_core::checkpoint::CheckpointAssignmentFence,
    pub(crate) revoked: &'a FxHashSet<u32>,
    pub(crate) restores: &'a [ManagedVnodeRestore<'a>],
    pub(crate) whole_restores: &'a [ManagedWholeRestore<'a>],
    pub(crate) mode: ManagedVnodeTransitionMode<'a>,
}

pub(crate) struct OperatorCheckpoint {
    pub data: Vec<u8>,
}

enum GateDecision {
    Run,
    Skip,
    Fail,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GraphExecutionMode {
    Normal,
    CheckpointDrain,
}

const GRAPH_EXECUTION_POISON_REASON: &str =
    "operator graph execution was cancelled or panicked after potentially mutating state, failed \
     terminally after input admission, or a vnode lifecycle callback returned an indeterminate \
     outcome; recovery from the last committed checkpoint is required";

/// In cluster mode, assignment adoption may trust the installed-state binding only while this
/// graph generation remains usable. Clear that success marker before publishing poison so an
/// observer that sees the poison can never retain authority derived from indeterminate state.
#[cfg(feature = "cluster")]
fn publish_cluster_execution_poison(
    poisoned: &AtomicBool,
    installed_vnode_state: Option<&crate::vnode_transition_staging::InstalledVnodeStateHandle>,
    pending_vnode_transition: Option<(
        &crate::vnode_transition_staging::PendingVnodeTransitionHandle,
        &Arc<crate::vnode_transition_staging::PendingVnodeTransition>,
    )>,
) {
    if let Some(installed_vnode_state) = installed_vnode_state {
        installed_vnode_state.lock().take();
    }
    if let Some((handle, expected)) = pending_vnode_transition {
        crate::vnode_transition_staging::retire_exact_pending_vnode_transition(handle, expected);
    }
    poisoned.store(true, Ordering::Release);
}

/// A graph cycle may hold operator mutation and graph-owned input in different futures. Unwind or
/// cancellation before the explicit result boundary permanently fences this graph generation;
/// post-admission terminal results are fenced where they are classified.
struct GraphExecutionAttemptGuard {
    poisoned: Arc<AtomicBool>,
    #[cfg(feature = "cluster")]
    installed_vnode_state: Option<crate::vnode_transition_staging::InstalledVnodeStateHandle>,
    armed: bool,
}

impl GraphExecutionAttemptGuard {
    fn new(graph: &OperatorGraph) -> Self {
        Self {
            poisoned: Arc::clone(&graph.execution_poisoned),
            #[cfg(feature = "cluster")]
            installed_vnode_state: graph.installed_vnode_state.as_ref().map(Arc::clone),
            armed: true,
        }
    }

    fn complete(&mut self) {
        self.armed = false;
    }
}

impl Drop for GraphExecutionAttemptGuard {
    fn drop(&mut self) {
        if self.armed {
            #[cfg(feature = "cluster")]
            publish_cluster_execution_poison(
                &self.poisoned,
                self.installed_vnode_state.as_ref(),
                None,
            );
            #[cfg(not(feature = "cluster"))]
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

const STATS_SAMPLE_INTERVAL: u64 = 32;

/// Logical ABI for independently checksummed operator and vnode frames.
pub(crate) const STATE_FRAME_ABI_VERSION: u32 = 3;

#[derive(Debug)]
pub(crate) struct CapturedWholeState {
    pub(crate) operator_id: String,
    pub(crate) state: StateFrameCapture,
}

#[derive(Debug, Default)]
pub(crate) struct GraphStateCapture {
    pub(crate) whole: Vec<CapturedWholeState>,
    pub(crate) vnodes: Vec<(String, CapturedVnodeState)>,
    pub(crate) managed_vnode_operators: Vec<(String, OperatorStateClass)>,
    retained_bytes: u64,
}

impl GraphStateCapture {
    pub(crate) const fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }
}

struct GraphNode {
    name: Arc<str>,
    operator: Box<dyn GraphOperator>,
    capability: OperatorCapability,
    input_port_count: usize,
    output_routes: Vec<(usize, u8)>,
    removed: bool,
}

impl GraphNode {
    fn new(name: Arc<str>, operator: Box<dyn GraphOperator>, input_port_count: usize) -> Self {
        let capability = operator.cluster_capability();
        tracing::debug!(
            operator = %name,
            implementation = ?capability.implementation,
            state_class = ?capability.state_class,
            cluster_status = ?capability.cluster_status,
            "registered physical operator capability inventory"
        );
        Self {
            name,
            operator,
            capability,
            input_port_count,
            output_routes: Vec::new(),
            removed: false,
        }
    }

    fn replace_operator(&mut self, operator: Box<dyn GraphOperator>) {
        let capability = operator.cluster_capability();
        tracing::debug!(
            operator = %self.name,
            implementation = ?capability.implementation,
            state_class = ?capability.state_class,
            cluster_status = ?capability.cluster_status,
            "replaced physical operator capability inventory"
        );
        self.operator = operator;
        self.capability = capability;
    }
}

struct GraphEdge {
    source: usize,
    target: usize,
}

#[derive(Clone, Copy)]
enum SourceBatchView {
    Visible,
    Positioned,
}

struct SourceRoute {
    name: Arc<str>,
    node_id: usize,
    view: SourceBatchView,
}

struct SourcePassthrough;

#[async_trait]
impl GraphOperator for SourcePassthrough {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::fixed(OperatorImplementation::SourcePassthrough)
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(inputs.first().cloned().unwrap_or_default())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

struct TombstonedOperator;

#[async_trait]
impl GraphOperator for TombstonedOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::fixed(OperatorImplementation::Tombstoned)
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

struct SqlFilterOperator {
    filter_sql: String,
    ctx: SessionContext,
    tmp_table: String,
    cache: Option<crate::operator::LiveSqlCache>,
}

impl SqlFilterOperator {
    fn new(filter_sql: String, ctx: SessionContext, node_name: &str) -> Self {
        let tmp_table = format!(
            "__prefilter_{}",
            node_name.replace(|c: char| !c.is_alphanumeric(), "_")
        );
        Self {
            filter_sql,
            ctx,
            tmp_table,
            cache: None,
        }
    }
}

#[async_trait]
impl GraphOperator for SqlFilterOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::fixed(OperatorImplementation::SqlFilter)
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let batches = inputs.first().cloned().unwrap_or_default();
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(Vec::new());
        }

        if self.cache.is_none() {
            let schema = batches[0].schema();
            let sql = format!("SELECT * FROM {} WHERE {}", self.tmp_table, self.filter_sql);
            let cache = crate::operator::LiveSqlCache::build(
                &self.ctx,
                &self.tmp_table,
                schema,
                &sql,
                "pre-filter",
            )
            .await?;
            self.cache = Some(cache);
        }

        self.cache
            .as_ref()
            .unwrap()
            .apply("pre-filter", batches)
            .await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

/// Enriches an incremental MV's changelog with a static dimension, preserving `__weight`. Re-creates
/// the physical plan each cycle so the hash join's `OnceAsync` build side doesn't freeze on cycle-1 temp.
struct ChangelogEnrichOperator {
    join_sql: String,
    ctx: SessionContext,
    handle: Option<LiveSourceHandle>,
    logical: Option<datafusion_expr::LogicalPlan>,
}

impl ChangelogEnrichOperator {
    fn new(ctx: SessionContext, join_sql: String) -> Self {
        Self {
            join_sql,
            ctx,
            handle: None,
            logical: None,
        }
    }
}

#[async_trait]
impl GraphOperator for ChangelogEnrichOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::fixed(OperatorImplementation::ChangelogEnrich)
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let batches = inputs.first().cloned().unwrap_or_default();
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(Vec::new());
        }
        if self.handle.is_none() {
            let provider = LiveSourceProvider::new(batches[0].schema());
            self.handle = Some(provider.handle());
            let tmp = crate::sql_analysis::CHANGELOG_ENRICH_TMP;
            let _ = self.ctx.deregister_table(exact_table_reference(tmp));
            self.ctx
                .register_table(exact_table_reference(tmp), Arc::new(provider))
                .map_err(|e| DbError::Pipeline(format!("changelog-enrich register temp: {e}")))?;
            let logical = self
                .ctx
                .sql(&self.join_sql)
                .await
                .map_err(|e| DbError::query_pipeline("changelog-enrich", &e))?
                .logical_plan()
                .clone();
            self.logical = Some(logical);
        }
        self.handle.as_ref().unwrap().swap(batches);
        // Fresh physical plan each cycle resets the hash join's cached build side.
        let physical = self
            .ctx
            .state()
            .create_physical_plan(self.logical.as_ref().unwrap())
            .await
            .map_err(|e| DbError::query_pipeline("changelog-enrich", &e))?;
        datafusion::physical_plan::collect(physical, self.ctx.task_ctx())
            .await
            .map_err(|e| DbError::query_pipeline("changelog-enrich", &e))
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShuffleAlignmentOutcome {
    Aligned,
    Aborted,
    ScopeCancelledBeforeStaging,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OwnedVnodeRosterCacheKey {
    Local {
        vnode_count: u32,
    },
    #[cfg(feature = "cluster")]
    Cluster {
        assignment_version: u64,
        self_id: laminar_core::state::NodeId,
    },
    #[cfg(all(test, feature = "cluster"))]
    Test {
        vnode_count: u32,
    },
}

#[allow(clippy::struct_excessive_bools)] // distinct independent flags, not a state enum
pub(crate) struct OperatorGraph {
    nodes: Vec<GraphNode>,
    // Node indices are embedded in edges and parallel buffers. Live DDL runs at
    // the coordinator safe point, where a fully detached tombstone can be reused.
    free_node_ids: Vec<usize>,
    edges: Vec<GraphEdge>,
    topo_order: Vec<usize>,
    topo_dirty: bool,
    // Failure domain (connected component) per node; a fatal error is isolated to its domain.
    node_domain: Vec<usize>,
    domain_count: usize,
    // When set, `compute_node_domains` cuts at source nodes so shared-source queries become
    // distinct domains. Off: shared-source queries fuse into one domain.
    shared_source_isolation: bool,
    // Per-operator cap on the preserved input a faulted domain replays from. Past it, replay
    // is abandoned and the input dropped (EO recovery).
    max_replay_buffer_bytes: usize,
    // Local source names whose domain faulted this cycle; drained by `take_cycle_failures`.
    cycle_failed_sources: FxHashSet<Arc<str>>,
    cycle_any_failed: bool,
    // Local sources whose graph work remains buffered for replay. `cycle_any_deferred` also
    // covers remote-only cluster work, where there is no local source cursor to retain.
    cycle_deferred_sources: FxHashSet<Arc<str>>,
    cycle_any_deferred: bool,
    source_map: FxHashMap<Arc<str>, usize>,
    positioned_source_map: FxHashMap<Arc<str>, usize>,
    source_list: Vec<SourceRoute>,
    source_node_ids: FxHashSet<usize>,
    output_map: FxHashMap<Arc<str>, usize>,
    // Reverse of `output_map` (node id → is an output); rebuilt in `compute_topo_order`.
    output_node_ids: FxHashSet<usize>,
    input_bufs: Vec<Vec<Vec<RecordBatch>>>,
    input_buf_bytes: Vec<Vec<usize>>,
    input_sources: Vec<Vec<usize>>,
    output_watermarks: Vec<i64>,
    output_idle: Vec<bool>,
    max_input_buf_batches: usize,
    max_input_buf_bytes: Option<usize>,
    backpressure_policy: BackpressurePolicy,
    query_budget_ns: u64,
    deferred_scan_offset: usize,
    stats_tick: u64,
    // Pipeline-wide backend-neutral charged-byte envelope for all managed operators and lifecycle
    // phases. Runtime construction always replaces the unbounded test/default sentinel.
    max_managed_state_bytes: usize,
    temporal_join_idle_history_retention: Option<std::time::Duration>,
    // Since-last-sample high watermarks make synchronous prepare/publish/finish ownership visible
    // without invoking Prometheus inside the vnode publication section. Indices mirror `nodes`.
    managed_state_accounting_peaks: Vec<ManagedStateAccountingSnapshot>,
    key_group_count: KeyGroupCount,
    owned_vnodes_cache: Option<(OwnedVnodeRosterCacheKey, Arc<[u32]>)>,
    ctx: SessionContext,
    prom: Option<Arc<EngineMetrics>>,
    lookup_registry: Option<Arc<laminar_sql::datafusion::LookupTableRegistry>>,
    source_schemas: FxHashMap<String, SchemaRef>,
    depends_on_stream: FxHashSet<usize>,
    order_configs: FxHashMap<usize, OrderOperatorConfig>,
    // Covers source tables and intermediates (lazily created on first operator output).
    live_handles: FxHashMap<String, LiveSourceHandle>,
    // None unless [ai]/[models] are configured.
    ai_runtime: Option<Arc<crate::ai::AiRuntime>>,
    // Must be the main multi-threaded runtime; Ring-1 workers (AI, lookup-enrich) spawn here.
    main_runtime_handle: Option<tokio::runtime::Handle>,
    // Lookup table name → column names; routes lookup-enrich joins to the async operator.
    partial_lookup_tables: FxHashMap<String, Vec<String>>,
    // Changelog-producing intermediates used for consumer admission and changelog enrichment.
    changelog_tables: FxHashSet<String>,
    // Static reference/dimension table names — valid right sides of a changelog enrich join.
    reference_tables: FxHashSet<String>,
    // Plan-time errors from add_query (returns ()); surfaced by take_build_errors at start.
    build_errors: Vec<DbError>,
    // Whole-graph restore is a one-shot startup transition and closes before the first cycle.
    whole_restore_open: bool,
    // Sticky for this in-memory graph generation. A dropped/panicking execution attempt may have
    // advanced operator state while losing graph-local inputs/results; only fresh restore is safe.
    execution_poisoned: Arc<AtomicBool>,
    #[cfg(feature = "cluster")]
    cluster_shuffle: Option<crate::operator::sql_query::ClusterShuffleConfig>,
    #[cfg(feature = "cluster")]
    last_execution_assignment_version: Option<u64>,
    // Logical pipeline/state ABI bound into every managed vnode transition.
    #[cfg(feature = "cluster")]
    pipeline_identity: Option<laminar_core::checkpoint::PipelineIdentity>,
    // One immutable assignment transition, consumed only after complete lifecycle success.
    #[cfg(feature = "cluster")]
    pending_vnode_transition: Option<crate::vnode_transition_staging::PendingVnodeTransitionHandle>,
    // Success-only binding for the exact vnode state installed in this graph generation.
    #[cfg(feature = "cluster")]
    installed_vnode_state: Option<crate::vnode_transition_staging::InstalledVnodeStateHandle>,
    #[cfg(feature = "cluster")]
    rotation_execution_fence: Option<Arc<tokio::sync::RwLock<()>>>,
    #[cfg(all(test, feature = "cluster"))]
    test_owned_vnodes: Option<Vec<u32>>,
}

impl OperatorGraph {
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            nodes: Vec::new(),
            free_node_ids: Vec::new(),
            edges: Vec::new(),
            topo_order: Vec::new(),
            topo_dirty: true,
            node_domain: Vec::new(),
            domain_count: 0,
            shared_source_isolation: false,
            max_replay_buffer_bytes: usize::MAX,
            cycle_failed_sources: FxHashSet::default(),
            cycle_any_failed: false,
            cycle_deferred_sources: FxHashSet::default(),
            cycle_any_deferred: false,
            source_map: FxHashMap::default(),
            positioned_source_map: FxHashMap::default(),
            source_list: Vec::new(),
            source_node_ids: FxHashSet::default(),
            output_map: FxHashMap::default(),
            output_node_ids: FxHashSet::default(),
            input_bufs: Vec::new(),
            input_buf_bytes: Vec::new(),
            input_sources: Vec::new(),
            output_watermarks: Vec::new(),
            output_idle: Vec::new(),
            max_input_buf_batches: 0,
            max_input_buf_bytes: None,
            backpressure_policy: BackpressurePolicy::default(),
            query_budget_ns: 8_000_000,
            deferred_scan_offset: 0,
            stats_tick: 0,
            max_managed_state_bytes: usize::MAX,
            temporal_join_idle_history_retention: None,
            managed_state_accounting_peaks: Vec::new(),
            key_group_count: DEFAULT_KEY_GROUP_COUNT,
            owned_vnodes_cache: None,
            #[cfg(feature = "cluster")]
            cluster_shuffle: None,
            #[cfg(feature = "cluster")]
            last_execution_assignment_version: None,
            #[cfg(feature = "cluster")]
            pipeline_identity: None,
            #[cfg(feature = "cluster")]
            pending_vnode_transition: None,
            #[cfg(feature = "cluster")]
            installed_vnode_state: None,
            #[cfg(feature = "cluster")]
            rotation_execution_fence: None,
            #[cfg(all(test, feature = "cluster"))]
            test_owned_vnodes: None,
            ctx,
            prom: None,
            lookup_registry: None,
            source_schemas: FxHashMap::default(),
            depends_on_stream: FxHashSet::default(),
            order_configs: FxHashMap::default(),
            live_handles: FxHashMap::default(),
            ai_runtime: None,
            main_runtime_handle: None,
            partial_lookup_tables: FxHashMap::default(),
            changelog_tables: FxHashSet::default(),
            reference_tables: FxHashSet::default(),
            build_errors: Vec::new(),
            whole_restore_open: true,
            execution_poisoned: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Register the static reference/dimension table names (valid right sides of a changelog
    /// enrich join).
    pub fn set_reference_tables(&mut self, tables: FxHashSet<String>) {
        self.reference_tables = tables;
    }

    /// Seed changelog producers before operators are built so admission is build-order independent.
    pub fn set_changelog_tables(&mut self, tables: FxHashSet<String>) {
        self.changelog_tables = tables;
    }

    /// Install the AI subsystem and main runtime handle for inference workers.
    pub fn set_ai_runtime(
        &mut self,
        runtime: Arc<crate::ai::AiRuntime>,
        handle: tokio::runtime::Handle,
    ) {
        self.ai_runtime = Some(runtime);
        self.main_runtime_handle = Some(handle);
    }

    /// Install the main runtime handle for Ring-1 workers (lookup-enrich, AI).
    pub fn set_runtime_handle(&mut self, handle: tokio::runtime::Handle) {
        self.main_runtime_handle = Some(handle);
    }

    /// Register on-demand lookup tables so `add_query` can route lookup-enrich joins.
    pub fn set_partial_lookup_tables(&mut self, tables: FxHashMap<String, Vec<String>>) {
        self.partial_lookup_tables = tables;
    }

    /// Return the first plan-time build error, if any.
    ///
    /// # Errors
    ///
    /// Returns the first recorded [`DbError`] (unknown model, unsupported task, etc.).
    pub fn take_build_errors(&mut self) -> Result<(), DbError> {
        match self.build_errors.drain(..).next() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub fn set_shared_source_isolation(&mut self, on: bool, max_replay_buffer_bytes: usize) {
        self.shared_source_isolation = on;
        self.max_replay_buffer_bytes = max_replay_buffer_bytes;
    }

    pub fn set_max_input_buf_batches(&mut self, cap: usize) {
        self.max_input_buf_batches = cap;
    }

    pub fn set_max_input_buf_bytes(&mut self, cap: Option<usize>) {
        self.max_input_buf_bytes = cap;
    }

    pub fn set_backpressure_policy(&mut self, policy: BackpressurePolicy) {
        self.backpressure_policy = policy;
    }

    pub fn set_query_budget_ns(&mut self, ns: u64) {
        self.query_budget_ns = ns;
    }

    pub(crate) fn set_max_managed_state_bytes(&mut self, bytes: usize) {
        assert!(bytes > 0, "managed-state budget must be nonzero");
        self.max_managed_state_bytes = bytes;
        for node in &mut self.nodes {
            node.operator.set_managed_state_budget(bytes);
        }
    }

    pub(crate) fn set_temporal_join_idle_history_retention(
        &mut self,
        retention: Option<std::time::Duration>,
    ) {
        self.temporal_join_idle_history_retention = retention;
    }

    pub fn set_metrics(&mut self, m: Arc<EngineMetrics>) {
        self.prom = Some(m);
        self.publish_buffer_stats();
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn input_buf_pressure(&self) -> f64 {
        let cap = self.max_input_buf_batches;
        let max_bytes = self.max_input_buf_bytes;

        let count_ratio = if cap > 0 {
            let max_len = self
                .input_bufs
                .iter()
                .flat_map(|ports| ports.iter())
                .map(Vec::len)
                .max()
                .unwrap_or(0);
            max_len as f64 / cap as f64
        } else {
            0.0
        };

        let bytes_ratio = if let Some(max) = max_bytes {
            let max_bytes_used = self
                .input_buf_bytes
                .iter()
                .flat_map(|ports| ports.iter().copied())
                .max()
                .unwrap_or(0);
            max_bytes_used as f64 / max as f64
        } else {
            0.0
        };

        count_ratio.max(bytes_ratio).min(1.0)
    }

    pub(crate) fn has_deferred_work(&self) -> bool {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| !node.removed)
            .any(|(node_id, node)| {
                !node.operator.wants_input()
                    || (!self.source_node_ids.contains(&node_id)
                        && self.input_bufs[node_id].iter().any(|port| !port.is_empty()))
            })
    }

    /// Logical bytes queued on every live input port. Uses the maintained per-port byte counters
    /// rather than walking Arrow batches, so checkpoint drain polling is independent of the number
    /// of buffered batches.
    pub(crate) fn checkpoint_pending_input_bytes(&self) -> usize {
        self.input_buf_bytes
            .iter()
            .enumerate()
            .filter(|(node_id, _)| !self.nodes[*node_id].removed)
            .flat_map(|(_, port_bytes)| port_bytes.iter().copied())
            .fold(0usize, usize::saturating_add)
    }

    /// Whether an aligned checkpoint can snapshot without leaving queued graph input outside the
    /// cut. A staged vnode transition is pending graph work too: an idle pipeline must run one
    /// drain pass to publish operator state against the target assignment. Buffer presence is
    /// checked separately from bytes because Arrow permits positive-row record batches whose
    /// arrays occupy zero bytes.
    pub(crate) fn checkpoint_is_quiescent(&self) -> bool {
        #[cfg(feature = "cluster")]
        if !self.checkpoint_transition_is_applied() {
            return false;
        }
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| !node.removed)
            .all(|(node_id, node)| {
                !self.node_has_buffered_input(node_id) && !node.operator.checkpoint_drain_pending()
            })
    }

    /// Whether the graph can hand off vnode state without transferring retained shuffle replay.
    #[cfg(feature = "cluster")]
    pub(crate) fn handoff_is_quiescent(&self) -> bool {
        self.checkpoint_is_quiescent()
            && self
                .nodes
                .iter()
                .filter(|node| !node.removed)
                .all(|node| !node.operator.checkpoint_aligned_replay_pending())
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_transition_is_applied(&self) -> bool {
        !self.has_pending_vnode_transition()
            && self.cluster_shuffle.as_ref().is_none_or(|shuffle| {
                self.last_execution_assignment_version
                    == Some(shuffle.registry.assignment_version())
            })
    }

    #[cfg(feature = "cluster")]
    fn ensure_checkpoint_transition_is_applied(&self) -> Result<(), DbError> {
        if self.checkpoint_transition_is_applied() {
            Ok(())
        } else {
            Err(DbError::Checkpoint(
                "[LDB-6051] checkpoint capture requires the current vnode assignment transition \
                 to complete in a graph drain pass"
                    .into(),
            ))
        }
    }

    fn node_has_buffered_input(&self, node_id: usize) -> bool {
        self.input_bufs[node_id].iter().any(|port| !port.is_empty())
    }

    fn checkpoint_drain_nodes(&self) -> FxHashSet<usize> {
        let mut drain = FxHashSet::default();
        let mut pending = VecDeque::new();
        for (node_id, node) in self.nodes.iter().enumerate() {
            if !node.removed
                && (self.node_has_buffered_input(node_id)
                    || node.operator.checkpoint_aligned_replay_pending()
                    || node.operator.checkpoint_drain_pending())
            {
                drain.insert(node_id);
                pending.push_back(node_id);
            }
        }
        while let Some(node_id) = pending.pop_front() {
            for &(target, _) in &self.nodes[node_id].output_routes {
                if !self.nodes[target].removed && drain.insert(target) {
                    pending.push_back(target);
                }
            }
        }
        drain
    }

    pub fn set_lookup_registry(
        &mut self,
        registry: Arc<laminar_sql::datafusion::LookupTableRegistry>,
    ) {
        self.lookup_registry = Some(registry);
    }

    pub(crate) fn set_key_group_count(&mut self, key_group_count: KeyGroupCount) {
        if !self.nodes.is_empty() {
            self.build_errors.push(DbError::Config(
                "key-group topology must be installed before graph operators".into(),
            ));
            return;
        }
        self.key_group_count = key_group_count;
        self.owned_vnodes_cache = None;
    }

    /// Install the cluster shuffle config for streaming aggregates.
    #[cfg(feature = "cluster")]
    pub fn set_cluster_shuffle(
        &mut self,
        config: crate::operator::sql_query::ClusterShuffleConfig,
    ) {
        let vnode_count = config.registry.vnode_count();
        if vnode_count != u32::from(self.key_group_count) {
            self.build_errors.push(DbError::Config(format!(
                "cluster vnode registry count {vnode_count} does not match graph key-group topology {}",
                self.key_group_count.get()
            )));
            return;
        }
        self.cluster_shuffle = Some(config);
        self.owned_vnodes_cache = None;
    }

    /// Cluster shuffle config, if installed; reused by the pipeline callback for subscriptions.
    #[cfg(feature = "cluster")]
    pub(crate) fn cluster_shuffle_config(
        &self,
    ) -> Option<&crate::operator::sql_query::ClusterShuffleConfig> {
        self.cluster_shuffle.as_ref()
    }

    fn required_vnodes_for_capability(
        capability: OperatorCapability,
        owned_vnodes: &[u32],
    ) -> Result<&[u32], DbError> {
        if capability.managed_state.is_none() {
            return Ok(&[]);
        }
        match capability.state_class {
            OperatorStateClass::GlobalSingleton => Ok(if owned_vnodes.first() == Some(&0) {
                &owned_vnodes[..1]
            } else {
                &[]
            }),
            OperatorStateClass::VnodeKeyed => Ok(owned_vnodes),
            state_class => Err(DbError::Checkpoint(format!(
                "managed-state contract {:?} has unsupported placement {state_class:?}",
                capability.managed_state
            ))),
        }
    }

    #[cfg(feature = "cluster")]
    fn owned_vnodes_for_managed_state(&mut self) -> Result<Option<Arc<[u32]>>, DbError> {
        let has_participants = self
            .nodes
            .iter()
            .any(|node| !node.removed && node.capability.managed_state.is_some());
        if !has_participants {
            return Ok(None);
        }
        if let Some(config) = &self.cluster_shuffle {
            let assignment = config.registry.versioned_snapshot();
            if u32::try_from(assignment.owners().len()).ok()
                != Some(u32::from(self.key_group_count))
            {
                return Err(DbError::Checkpoint(
                    "managed-state capture vnode count does not match the active assignment".into(),
                ));
            }
            let cache_key = OwnedVnodeRosterCacheKey::Cluster {
                assignment_version: assignment.version(),
                self_id: config.self_id,
            };
            if let Some((cached_key, cached)) = &self.owned_vnodes_cache {
                if *cached_key == cache_key {
                    return Ok(Some(Arc::clone(cached)));
                }
            }
            let owned_vnodes = assignment
                .owners()
                .iter()
                .enumerate()
                .filter(|(_, owner)| **owner == config.self_id)
                .map(|(vnode, _)| u32::try_from(vnode).expect("vnode count is represented by u32"))
                .collect::<Vec<_>>();
            let owned_vnodes = Arc::<[u32]>::from(owned_vnodes);
            self.owned_vnodes_cache = Some((cache_key, Arc::clone(&owned_vnodes)));
            return Ok(Some(owned_vnodes));
        }
        #[cfg(test)]
        if let Some(vnodes) = &self.test_owned_vnodes {
            let cache_key = OwnedVnodeRosterCacheKey::Test {
                vnode_count: u32::from(self.key_group_count),
            };
            if let Some((cached_key, cached)) = &self.owned_vnodes_cache {
                if *cached_key == cache_key {
                    return Ok(Some(Arc::clone(cached)));
                }
            }
            let owned_vnodes = Arc::<[u32]>::from(vnodes.clone());
            self.owned_vnodes_cache = Some((cache_key, Arc::clone(&owned_vnodes)));
            return Ok(Some(owned_vnodes));
        }
        Ok(self.local_owned_vnodes_for_managed_state())
    }

    fn local_owned_vnodes_for_managed_state(&mut self) -> Option<Arc<[u32]>> {
        let has_participants = self
            .nodes
            .iter()
            .any(|node| !node.removed && node.capability.managed_state.is_some());
        if !has_participants {
            return None;
        }
        let vnode_count = u32::from(self.key_group_count);
        let cache_key = OwnedVnodeRosterCacheKey::Local { vnode_count };
        if let Some((cached_key, cached)) = &self.owned_vnodes_cache {
            if *cached_key == cache_key {
                return Some(Arc::clone(cached));
            }
        }
        let owned_vnodes = Arc::<[u32]>::from((0..vnode_count).collect::<Vec<_>>());
        self.owned_vnodes_cache = Some((cache_key, Arc::clone(&owned_vnodes)));
        Some(owned_vnodes)
    }

    /// Bind the graph to the logical pipeline and recovery-state ABI.
    #[cfg(feature = "cluster")]
    pub(crate) fn set_pipeline_identity(
        &mut self,
        identity: laminar_core::checkpoint::PipelineIdentity,
    ) {
        self.pipeline_identity = Some(identity);
    }

    /// Share the single immutable pending vnode transition slot.
    #[cfg(feature = "cluster")]
    pub(crate) fn set_pending_vnode_transition_handle(
        &mut self,
        pending: crate::vnode_transition_staging::PendingVnodeTransitionHandle,
    ) {
        self.pending_vnode_transition = Some(pending);
    }

    /// Share the success-only installed-state binding with assignment adoption.
    #[cfg(feature = "cluster")]
    pub(crate) fn set_installed_vnode_state_handle(
        &mut self,
        installed: crate::vnode_transition_staging::InstalledVnodeStateHandle,
    ) {
        self.installed_vnode_state = Some(installed);
    }

    #[cfg(feature = "cluster")]
    pub fn set_rotation_execution_fence(&mut self, fence: Arc<tokio::sync::RwLock<()>>) {
        self.rotation_execution_fence = Some(fence);
    }

    /// Hold assignment publication out of shuffle alignment and mutable checkpoint capture.
    /// The caller must drop the token before encoding or durable checkpoint-tail I/O and must not
    /// re-enter graph execution while it is held. Shuffle alignment remains inside the token and
    /// may perform bounded transport and authority-settlement reads.
    #[cfg(feature = "cluster")]
    pub(crate) async fn checkpoint_rotation_guard_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<Option<tokio::sync::OwnedRwLockReadGuard<()>>, DbError> {
        let Some(fence) = self.rotation_execution_fence.as_ref().map(Arc::clone) else {
            return Ok(None);
        };
        tokio::time::timeout_at(deadline, fence.read_owned())
            .await
            .map(Some)
            .map_err(|_| {
                DbError::Checkpoint(
                    "[LDB-6051] checkpoint capture timed out waiting for vnode assignment rotation"
                        .into(),
                )
            })
    }

    fn is_downstream_at_capacity(&self, node_id: usize) -> bool {
        let cap = self.max_input_buf_batches;
        let max_bytes = self.max_input_buf_bytes;
        if cap == 0 && max_bytes.is_none() {
            return false;
        }
        self.nodes[node_id]
            .output_routes
            .iter()
            .any(|&(target, port)| {
                let p = port as usize;
                let over_count = cap > 0 && self.input_bufs[target][p].len() >= cap;
                let over_bytes =
                    max_bytes.is_some_and(|max| self.input_buf_bytes[target][p] >= max);
                over_count || over_bytes
            })
    }

    fn shed_to_cap(&mut self, target: usize, port: u8) -> usize {
        if !matches!(self.backpressure_policy, BackpressurePolicy::ShedOldest) {
            return 0;
        }
        let cap = self.max_input_buf_batches;
        let max_bytes = self.max_input_buf_bytes;
        let p = port as usize;

        let mut drop_n = if cap > 0 && self.input_bufs[target][p].len() > cap {
            self.input_bufs[target][p].len() - cap
        } else {
            0
        };
        if let Some(max) = max_bytes {
            let buf = &self.input_bufs[target][p];
            let mut remaining = self.input_buf_bytes[target][p];
            for b in buf.iter().take(drop_n) {
                remaining = remaining.saturating_sub(b.get_array_memory_size());
            }
            while remaining > max && drop_n < buf.len() {
                remaining = remaining.saturating_sub(buf[drop_n].get_array_memory_size());
                drop_n += 1;
            }
        }
        if drop_n == 0 {
            return 0;
        }
        let mut bytes_removed = 0usize;
        let rows: usize = self.input_bufs[target][p]
            .drain(..drop_n)
            .map(|b| {
                bytes_removed += b.get_array_memory_size();
                b.num_rows()
            })
            .sum();
        let slot = &mut self.input_buf_bytes[target][p];
        *slot = slot.saturating_sub(bytes_removed);
        rows
    }

    fn gate_decision(&self, node_id: usize) -> GateDecision {
        if !self.is_downstream_at_capacity(node_id) {
            return GateDecision::Run;
        }
        match self.backpressure_policy {
            BackpressurePolicy::Backpressure => GateDecision::Skip,
            BackpressurePolicy::Fail => GateDecision::Fail,
            BackpressurePolicy::ShedOldest => GateDecision::Run,
        }
    }

    #[cfg(debug_assertions)]
    fn debug_assert_byte_sums(&self) {
        for (id, ports) in self.input_bufs.iter().enumerate() {
            for (port, buf) in ports.iter().enumerate() {
                let actual: usize = buf.iter().map(RecordBatch::get_array_memory_size).sum();
                debug_assert_eq!(
                    self.input_buf_bytes[id][port], actual,
                    "input_buf_bytes drift at node={} port={}",
                    &*self.nodes[id].name, port,
                );
            }
        }
    }

    fn push_to_port(&mut self, target: usize, port: u8, batches: Vec<RecordBatch>, bytes: usize) {
        let buf = &mut self.input_bufs[target][port as usize];
        if buf.is_empty() {
            *buf = batches;
        } else {
            buf.extend(batches);
        }
        self.input_buf_bytes[target][port as usize] += bytes;
        self.record_shed(target, port);
    }

    fn record_shed(&mut self, target: usize, port: u8) {
        let rows = self.shed_to_cap(target, port);
        if rows == 0 {
            return;
        }
        if let Some(ref prom) = self.prom {
            prom.shed_records_total
                .with_label_values(&[&self.nodes[target].name])
                .inc_by(rows as u64);
        }
    }

    pub fn register_source_schema(&mut self, name: String, schema: SchemaRef) {
        self.ensure_live_provider(&name, &schema);
        self.source_schemas.insert(name, schema);
    }

    /// Register a schema-only live provider for an intermediate stream before managed operators
    /// are prepared. The normal graph cycle fills the same provider when the upstream emits.
    pub(crate) fn register_intermediate_schema(&mut self, name: &str, schema: &SchemaRef) {
        self.ensure_live_provider(name, schema);
    }

    /// Initialize every declared managed-state participant before checkpoint recovery begins.
    ///
    /// This runs in embedded, single-node, and cluster pipelines. Cluster then layers vnode
    /// ownership and transition fencing over the same initialized operator state.
    pub(crate) async fn initialize_managed_state(mut self) -> Result<Self, DbError> {
        if !self.whole_restore_open {
            return Err(DbError::Checkpoint(
                "managed state must be initialized before graph recovery or execution".into(),
            ));
        }
        #[cfg(feature = "cluster")]
        let cluster_graph = self.cluster_shuffle.is_some();
        #[cfg(feature = "cluster")]
        if cluster_graph {
            for node in &self.nodes {
                if node.removed {
                    continue;
                }
                if let ClusterExecutionStatus::Rejected { reason } = node.capability.cluster_status
                {
                    return Err(DbError::Pipeline(format!(
                        "[{}] operator '{}' is not cluster-admissible: {reason}",
                        laminar_core::error_codes::CLUSTER_STATE_LIFECYCLE_UNSUPPORTED,
                        node.name
                    )));
                }
            }
        }
        for node in &mut self.nodes {
            if node.removed || node.capability.managed_state.is_none() {
                continue;
            }
            node.operator
                .initialize_managed_state()
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "managed-state initialization for operator '{}' failed: {error}",
                        node.name
                    ))
                })?;
            let resolved = node.operator.cluster_capability();
            #[cfg(feature = "cluster")]
            if cluster_graph && resolved != node.capability {
                let detail = match resolved.cluster_status {
                    ClusterExecutionStatus::Rejected { reason } => reason.to_string(),
                    _ => format!(
                        "managed-state initialization changed the capability descriptor from {:?} to {:?}",
                        node.capability, resolved
                    ),
                };
                return Err(DbError::Pipeline(format!(
                    "[{}] operator '{}' is not cluster-admissible after managed-state initialization: {detail}",
                    laminar_core::error_codes::CLUSTER_STATE_LIFECYCLE_UNSUPPORTED,
                    node.name
                )));
            }
            if resolved.implementation != node.capability.implementation {
                return Err(DbError::Checkpoint(format!(
                    "managed-state initialization changed operator '{}' implementation from {:?} to {:?}",
                    node.name, node.capability.implementation, resolved.implementation
                )));
            }
            node.capability = resolved;
        }
        self.validate_managed_state_budget("managed-state initialization")?;
        Ok(self)
    }

    fn managed_state_accounted_bytes(&self) -> usize {
        self.nodes
            .iter()
            .filter(|node| !node.removed)
            .filter_map(|node| node.operator.managed_state_accounting())
            .fold(0_usize, |total, accounting| {
                total.saturating_add(accounting.total_bytes())
            })
    }

    fn validate_managed_state_budget(&self, context: impl Into<String>) -> Result<(), DbError> {
        let accounted_bytes = self.managed_state_accounted_bytes();
        if accounted_bytes > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: context.into(),
                accounted_bytes,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        Ok(())
    }

    fn ensure_live_provider(&mut self, name: &str, schema: &SchemaRef) {
        if self.live_handles.contains_key(name) {
            return;
        }
        let provider = LiveSourceProvider::new(schema.clone());
        let handle = provider.handle();
        let _ = self.ctx.deregister_table(exact_table_reference(name));
        if let Err(e) = self
            .ctx
            .register_table(exact_table_reference(name), Arc::new(provider))
        {
            // Table was just deregistered, so re-registration should always succeed.
            tracing::error!(
                table = %name,
                error = %e,
                "BUG: Failed to register LiveSourceProvider after deregister"
            );
            return;
        }
        self.live_handles.insert(name.to_string(), handle);
    }

    fn find_node(&self, name: &str) -> Option<usize> {
        self.nodes
            .iter()
            .position(|n| &*n.name == name && !n.removed)
    }

    fn allocate_node(&mut self, mut node: GraphNode) -> usize {
        node.operator
            .set_managed_state_budget(self.max_managed_state_bytes);
        let input_port_count = node.input_port_count;
        if let Some(id) = self.free_node_ids.pop() {
            debug_assert!(self.nodes[id].removed);
            self.nodes[id] = node;
            self.input_bufs[id] = vec![Vec::new(); input_port_count];
            self.input_buf_bytes[id] = vec![0; input_port_count];
            self.input_sources[id] = vec![usize::MAX; input_port_count];
            self.output_watermarks[id] = i64::MIN;
            self.output_idle[id] = false;
            self.managed_state_accounting_peaks[id] = ManagedStateAccountingSnapshot::default();
            if let Some(domain) = self.node_domain.get_mut(id) {
                *domain = 0;
            }
            self.output_node_ids.remove(&id);
            self.source_node_ids.remove(&id);
            self.depends_on_stream.remove(&id);
            self.order_configs.remove(&id);
            id
        } else {
            let id = self.nodes.len();
            self.nodes.push(node);
            self.input_bufs.push(vec![Vec::new(); input_port_count]);
            self.input_buf_bytes.push(vec![0; input_port_count]);
            self.input_sources.push(vec![usize::MAX; input_port_count]);
            self.output_watermarks.push(i64::MIN);
            self.output_idle.push(false);
            self.managed_state_accounting_peaks
                .push(ManagedStateAccountingSnapshot::default());
            id
        }
    }

    fn ensure_source_node(&mut self, table_name: &str) -> usize {
        if let Some(&id) = self.source_map.get(table_name) {
            return id;
        }
        let name: Arc<str> = Arc::from(table_name);
        let node_id = self.allocate_node(GraphNode::new(
            Arc::clone(&name),
            Box::new(SourcePassthrough),
            1,
        ));
        self.source_map.insert(name, node_id);
        self.source_node_ids.insert(node_id);
        node_id
    }

    fn ensure_positioned_source_node(&mut self, table_name: &str) -> usize {
        if let Some(&id) = self.positioned_source_map.get(table_name) {
            return id;
        }
        let source_name: Arc<str> = Arc::from(table_name);
        let node_name: Arc<str> = Arc::from(format!("__temporal_source::{table_name}"));
        let node_id = self.allocate_node(GraphNode::new(node_name, Box::new(SourcePassthrough), 1));
        self.positioned_source_map.insert(source_name, node_id);
        self.source_node_ids.insert(node_id);
        node_id
    }

    fn insert_filter_node(&mut self, name: &str, filter_sql: String, source_id: usize) -> usize {
        let node_id = self.allocate_node(GraphNode::new(
            Arc::from(name),
            Box::new(SqlFilterOperator::new(filter_sql, self.ctx.clone(), name)),
            1,
        ));
        self.add_edge(source_id, node_id, 0);
        self.topo_dirty = true;
        node_id
    }

    fn add_edge(&mut self, source: usize, target: usize, target_port: u8) {
        self.edges.push(GraphEdge { source, target });
        self.nodes[source].output_routes.push((target, target_port));
        let port = target_port as usize;
        if port < self.input_sources[target].len() {
            self.input_sources[target][port] = source;
        }
    }

    fn ensure_query_source_nodes(
        &mut self,
        stream_join_config: Option<&laminar_sql::translator::StreamJoinConfig>,
        temporal_config: Option<&TemporalJoinTranslatorConfig>,
        table_refs: &FxHashSet<String>,
    ) {
        if let Some(sjc) = stream_join_config {
            self.find_node(&sjc.left_table)
                .unwrap_or_else(|| self.ensure_source_node(&sjc.left_table));
            self.find_node(&sjc.right_table)
                .unwrap_or_else(|| self.ensure_source_node(&sjc.right_table));
        } else if let Some(tc) = temporal_config {
            self.ensure_positioned_source_node(&tc.left_table);
            self.ensure_positioned_source_node(&tc.right_table);
        } else {
            for table_ref in table_refs {
                if self.find_node(table_ref).is_none() {
                    self.ensure_source_node(table_ref);
                }
            }
        }
    }

    // Returns true when the node depends on another query output (not just raw sources).
    fn wire_query_edges(
        &mut self,
        node_id: usize,
        stream_join_config: Option<&laminar_sql::translator::StreamJoinConfig>,
        stream_join_detection: Option<&StreamJoinDetection>,
        temporal_config: Option<&TemporalJoinTranslatorConfig>,
        table_refs: &FxHashSet<String>,
    ) -> bool {
        if let Some(sjc) = stream_join_config {
            let source_id = self.find_node(&sjc.left_table).expect("source ensured");

            let has_pre_filters = stream_join_detection
                .is_some_and(|d| d.left_pre_filter.is_some() || d.right_pre_filter.is_some());

            if sjc.left_table == sjc.right_table && has_pre_filters {
                let det = stream_join_detection.unwrap();

                let left_input = if let Some(ref filter_sql) = det.left_pre_filter {
                    self.insert_filter_node(
                        &format!("{}::left_prefilter", self.nodes[node_id].name),
                        filter_sql.clone(),
                        source_id,
                    )
                } else {
                    source_id
                };

                let right_input = if let Some(ref filter_sql) = det.right_pre_filter {
                    self.insert_filter_node(
                        &format!("{}::right_prefilter", self.nodes[node_id].name),
                        filter_sql.clone(),
                        source_id,
                    )
                } else {
                    source_id
                };

                self.add_edge(left_input, node_id, 0);
                self.add_edge(right_input, node_id, 1);
            } else {
                let right_id = self.find_node(&sjc.right_table).expect("source ensured");
                self.add_edge(source_id, node_id, 0);
                self.add_edge(right_id, node_id, 1);
            }
            false
        } else if let Some(tc) = temporal_config {
            let left_id = self.positioned_source_map[tc.left_table.as_str()];
            let right_id = self.positioned_source_map[tc.right_table.as_str()];
            self.add_edge(left_id, node_id, 0);
            self.add_edge(right_id, node_id, 1);
            false
        } else {
            let mut depends_on_query = false;
            for table_ref in table_refs {
                let upstream_id = self.find_node(table_ref).expect("source ensured");
                let already_connected = self.nodes[upstream_id]
                    .output_routes
                    .iter()
                    .any(|&(t, p)| t == node_id && p == 0);
                if !already_connected {
                    self.add_edge(upstream_id, node_id, 0);
                }
                if self.output_map.contains_key(table_ref.as_str()) {
                    depends_on_query = true;
                }
            }
            depends_on_query
        }
    }

    #[allow(
        clippy::too_many_lines,
        clippy::too_many_arguments,
        clippy::needless_pass_by_value
    )]
    pub fn add_query(
        &mut self,
        name: String,
        sql: String,
        emit_clause: Option<EmitClause>,
        window_config: Option<WindowOperatorConfig>,
        order_config: Option<OrderOperatorConfig>,
        join_config: Option<Vec<laminar_sql::translator::JoinOperatorConfig>>,
        incremental: bool,
    ) {
        use laminar_sql::translator::JoinOperatorConfig;

        if join_clause_count(&sql) > 1 {
            self.build_errors.push(DbError::InvalidOperation(
                "multi-way streaming joins require explicitly named two-way stages".to_string(),
            ));
            return;
        }

        let ai_calls = crate::sql_analysis::detect_ai_functions(&sql);
        if ai_calls.len() > 1 {
            self.build_errors.push(DbError::InvalidOperation(
                "v0.1 supports at most one AI function per query".to_string(),
            ));
            return;
        }
        if ai_calls.len() == 1 {
            match crate::sql_analysis::plan_ai_query(&sql) {
                Some(plan) => {
                    if let Err(e) = self.build_ai_operator_node(&name, &plan) {
                        self.build_errors.push(e);
                    }
                }
                None => self.build_errors.push(DbError::InvalidOperation(
                    "an AI function must be a top-level SELECT item with an `AS` alias over a \
                     single (un-joined) source"
                        .to_string(),
                )),
            }
            return;
        }

        if let Some(plan) = crate::sql_analysis::plan_frame_query(&sql) {
            self.build_frame_operator_node(&name, &plan);
            return;
        }

        let mut table_refs = extract_table_references(&sql);

        if window_config.is_some()
            && table_refs
                .iter()
                .any(|table| self.changelog_tables.contains(table))
        {
            self.build_errors.push(DbError::InvalidOperation(format!(
                "window aggregate '{name}' cannot safely consume a changelog; window aggregates do not apply input retractions"
            )));
            return;
        }

        // `changelog ⋈ static dim`: detected first so it wins over the generic processing-time
        // equi-join — a changelog left makes this a retraction-aware enrich, not a stream join.
        let changelog_enrich_config = if self.changelog_tables.is_empty() {
            None
        } else {
            crate::sql_analysis::detect_changelog_enrich_query(
                &sql,
                &self.changelog_tables,
                &self.reference_tables,
            )
        };
        let enrich = changelog_enrich_config.is_some();

        if has_join_clause(&sql)
            && !enrich
            && table_refs
                .iter()
                .any(|table| self.changelog_tables.contains(table))
        {
            self.build_errors.push(DbError::InvalidOperation(format!(
                "join '{name}' reads a changelog; only changelog-to-static-table enrichment is supported"
            )));
            return;
        }

        let temporal_config = if enrich {
            None
        } else {
            join_config
                .as_ref()
                .and_then(|configs| match configs.as_slice() {
                    [JoinOperatorConfig::Temporal(config)] => Some(config.clone()),
                    _ => None,
                })
        };
        let temporal_projection_sql = match temporal_config.as_ref() {
            Some(config) => match temporal_projection_sql(&sql, config) {
                Ok(projection) => Some(projection),
                Err(error) => {
                    self.build_errors.push(error);
                    return;
                }
            },
            None => None,
        };
        let needs_stream_detection = join_config.as_ref().is_none_or(|configs| {
            configs
                .iter()
                .any(|config| matches!(config, JoinOperatorConfig::StreamStream(_)))
        });
        let stream_join_detection =
            if !enrich && temporal_config.is_none() && needs_stream_detection {
                detect_stream_join_query(&sql)
            } else {
                None
            };
        let stream_join_config = stream_join_detection.as_ref().map(|d| d.config.clone());
        if let Some(config) = &stream_join_config {
            if config.time_bound.is_zero() || i64::try_from(config.time_bound.as_millis()).is_err()
            {
                self.build_errors.push(DbError::InvalidOperation(format!(
                    "streaming interval join '{name}' requires a positive finite time bound"
                )));
                return;
            }
        }
        let stream_join_projection_sql = stream_join_detection
            .as_ref()
            .map(|d| d.projection_sql.clone());

        // Lookup-enrich: only when no other specialized join (incl. changelog-enrich) matched.
        let (lookup_enrich_config, lookup_projection_sql) = if !enrich
            && temporal_config.is_none()
            && stream_join_config.is_none()
            && !self.partial_lookup_tables.is_empty()
        {
            crate::sql_analysis::detect_lookup_enrich_query(
                &sql,
                &self.partial_lookup_tables,
                &self.source_schemas,
            )
        } else {
            (None, None)
        };

        let unbounded_join_steps = (!enrich)
            .then(|| detect_unbounded_join_steps(&sql))
            .flatten();
        let unbounded_lookup_join = match unbounded_join_steps {
            Some(steps) => {
                let lookup_only = steps.iter().all(|(_, right)| {
                    self.reference_tables.contains(right)
                        || self.partial_lookup_tables.contains_key(right)
                });
                if !lookup_only {
                    let relations = steps
                        .iter()
                        .map(|(left, right)| format!("'{left}' and '{right}'"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    self.build_errors.push(DbError::InvalidOperation(format!(
                        "unbounded join between streaming relations {relations}; add a temporal predicate or use a lookup table"
                    )));
                    return;
                }
                true
            }
            None => false,
        };

        let projection_sql = temporal_projection_sql
            .or(stream_join_projection_sql)
            .or(lookup_projection_sql);

        let unrecognized_join = has_join_clause(&sql)
            && !enrich
            && temporal_config.is_none()
            && stream_join_config.is_none()
            && lookup_enrich_config.is_none()
            && !unbounded_lookup_join;
        if unrecognized_join {
            self.build_errors.push(DbError::InvalidOperation(format!(
                "stream join '{name}' could not be planned as a supported bounded interval or lookup join"
            )));
            return;
        }

        // Lookup-enrich reads its table from the registry, not as a graph input.
        if let Some(cfg) = &lookup_enrich_config {
            table_refs.remove(&cfg.table_name);
        }
        // ChangelogEnrich: only the changelog (left) is a graph input; the dimension is read from
        // the context, not wired as an edge.
        if let Some(cfg) = &changelog_enrich_config {
            table_refs.retain(|t| t == &cfg.changelog_table);
        }

        let operator = match self.create_operator(
            &name,
            &sql,
            emit_clause.as_ref(),
            window_config.as_ref(),
            temporal_config.as_ref(),
            stream_join_config.as_ref(),
            lookup_enrich_config,
            projection_sql.as_deref(),
            incremental,
            changelog_enrich_config,
        ) {
            Ok(operator) => operator,
            Err(error) => {
                self.build_errors.push(error);
                return;
            }
        };
        let input_port_count = if stream_join_config.is_some() || temporal_config.is_some() {
            2
        } else {
            1
        };

        self.ensure_query_source_nodes(
            stream_join_config.as_ref(),
            temporal_config.as_ref(),
            &table_refs,
        );
        let node_id = self.place_prepared_operator_node(name.as_str(), operator, input_port_count);
        let depends = self.wire_query_edges(
            node_id,
            stream_join_config.as_ref(),
            stream_join_detection.as_ref(),
            temporal_config.as_ref(),
            &table_refs,
        );
        if depends {
            self.depends_on_stream.insert(node_id);
        }
        if let Some(oc) = order_config {
            self.order_configs.insert(node_id, oc);
        }
        self.output_map.insert(Arc::from(name.as_str()), node_id);
        if incremental {
            self.changelog_tables.insert(name.clone());
        }
        self.topo_dirty = true;
    }

    // Replace a SourcePassthrough placeholder in place (preserving its id and outbound edges),
    // or append a fresh node. Callers must ensure source nodes before and wire edges after.
    #[cfg(test)]
    fn place_operator_node(
        &mut self,
        name: &str,
        operator: Box<dyn GraphOperator>,
        input_port_count: usize,
    ) -> Result<usize, DbError> {
        Ok(self.place_prepared_operator_node(name, operator, input_port_count))
    }

    // The caller has completed validation before making any graph mutation.
    fn place_prepared_operator_node(
        &mut self,
        name: &str,
        operator: Box<dyn GraphOperator>,
        input_port_count: usize,
    ) -> usize {
        if let Some(&id) = self.source_map.get(name) {
            self.nodes[id].replace_operator(operator);
            self.nodes[id].input_port_count = input_port_count;
            self.input_bufs[id] = vec![Vec::new(); input_port_count];
            self.input_buf_bytes[id] = vec![0; input_port_count];
            self.input_sources[id] = vec![usize::MAX; input_port_count];
            self.output_idle[id] = false;
            self.source_map.remove(name);
            self.source_node_ids.remove(&id);
            // Downstream nodes already wired to the placeholder now depend on this query.
            for &(target, _) in &self.nodes[id].output_routes {
                self.depends_on_stream.insert(target);
            }
            id
        } else {
            self.allocate_node(GraphNode::new(Arc::from(name), operator, input_port_count))
        }
    }

    fn build_ai_operator_node(
        &mut self,
        name: &str,
        plan: &crate::sql_analysis::AiQueryPlan,
    ) -> Result<(), DbError> {
        use crate::operator::ai_inference::{AiInferenceOperator, AiOperatorConfig};

        let handle = self.main_runtime_handle.clone().ok_or_else(|| {
            DbError::InvalidOperation("AI runtime handle is not configured".to_string())
        })?;
        let ctx = self.ctx.clone();

        let (operator, table_refs): (Box<dyn GraphOperator>, FxHashSet<String>) = {
            let runtime = self.ai_runtime.as_ref().ok_or_else(|| {
                DbError::InvalidOperation(
                    "AI functions require `[ai]` providers and `[models]` configuration"
                        .to_string(),
                )
            })?;

            crate::sql_analysis::validate_ai_calls(
                runtime.registry(),
                std::slice::from_ref(&plan.call),
            )?;

            let model_name = match &plan.call.model {
                Some(m) => m.clone(),
                None => runtime
                    .registry()
                    .default_for(plan.call.task)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        DbError::InvalidOperation(format!(
                            "no model given for task '{}' and no [ai.defaults] default is \
                             configured",
                            plan.call.task
                        ))
                    })?,
            };
            let resolved = runtime
                .resolve(&model_name)
                .map_err(|e| DbError::InvalidOperation(e.to_string()))?;

            let output_column = plan.call.output_alias.clone().ok_or_else(|| {
                DbError::InvalidOperation("AI function requires an `AS` alias".to_string())
            })?;
            let labels = plan.call.labels.clone().or_else(|| resolved.labels.clone());

            let config = AiOperatorConfig {
                task: plan.call.task,
                kind: resolved.kind,
                model_id: resolved.model_id,
                model: resolved.provider_model.clone(),
                input_column: plan.call.input.clone(),
                output_column,
                labels,
            };
            let operator: Box<dyn GraphOperator> = Box::new(AiInferenceOperator::new(
                name,
                config,
                Some(Arc::from(plan.projection_sql.as_str())),
                ctx,
                resolved.provider,
                Arc::clone(runtime.cache()),
                Arc::clone(runtime.call_log()),
                &handle,
            ));
            let mut table_refs = FxHashSet::default();
            table_refs.insert(plan.source_table.clone());
            (operator, table_refs)
        };

        self.ensure_query_source_nodes(None, None, &table_refs);
        let node_id = self.place_prepared_operator_node(name, operator, 1);
        let depends = self.wire_query_edges(node_id, None, None, None, &table_refs);
        if depends {
            self.depends_on_stream.insert(node_id);
        }
        self.output_map.insert(Arc::from(name), node_id);
        self.topo_dirty = true;
        Ok(())
    }

    fn build_frame_operator_node(
        &mut self,
        name: &str,
        plan: &crate::sql_analysis::FrameQueryPlan,
    ) {
        let operator: Box<dyn GraphOperator> =
            Box::new(crate::operator::window_frame::WindowFrameOperator::new(
                name,
                crate::operator::window_frame::MomentFrameConfig {
                    func: plan.func,
                    x_column: plan.x_column.clone(),
                    y_column: plan.y_column.clone(),
                    output_column: plan.output_alias.clone(),
                    retain: plan.retain,
                },
                Arc::from(plan.projection_sql.as_str()),
                self.ctx.clone(),
            ));
        let mut table_refs = FxHashSet::default();
        table_refs.insert(plan.source_table.clone());
        self.ensure_query_source_nodes(None, None, &table_refs);
        let node_id = self.place_prepared_operator_node(name, operator, 1);
        let depends = self.wire_query_edges(node_id, None, None, None, &table_refs);
        if depends {
            self.depends_on_stream.insert(node_id);
        }
        self.output_map.insert(Arc::from(name), node_id);
        self.topo_dirty = true;
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn create_operator(
        &self,
        name: &str,
        sql: &str,
        emit_clause: Option<&EmitClause>,
        window_config: Option<&WindowOperatorConfig>,
        temporal_config: Option<&TemporalJoinTranslatorConfig>,
        stream_join_config: Option<&laminar_sql::translator::StreamJoinConfig>,
        lookup_enrich_config: Option<crate::operator::lookup_enrich::LookupEnrichConfig>,
        projection_sql: Option<&str>,
        incremental: bool,
        changelog_enrich_config: Option<crate::sql_analysis::ChangelogEnrichConfig>,
    ) -> Result<Box<dyn GraphOperator>, DbError> {
        use crate::operator;

        // `changelog ⋈ static dim` — consume the changelog, join against the dimension (in the
        // graph context), preserve `__weight` → joined changelog.
        if let Some(cfg) = changelog_enrich_config {
            return Ok(Box::new(ChangelogEnrichOperator::new(
                self.ctx.clone(),
                cfg.projection_sql,
            )));
        }

        // Falls through to the DataFusion lookup path if the registry/handle is absent.
        if let Some(cfg) = lookup_enrich_config {
            if let (Some(reg), Some(handle)) = (&self.lookup_registry, &self.main_runtime_handle) {
                let op = operator::lookup_enrich::LookupEnrichOperator::new(
                    name,
                    cfg,
                    projection_sql.map(Arc::from),
                    self.ctx.clone(),
                    Arc::clone(reg),
                    handle.clone(),
                    self.prom.clone(),
                );
                return Ok(Box::new(op));
            }
        }

        if let Some(cfg) = temporal_config {
            let retention_ms = crate::config::temporal_join_idle_history_retention_ms(
                self.temporal_join_idle_history_retention,
            )
            .map_err(|reason| DbError::Config(format!("temporal join [{name}]: {reason}")))?;
            let limits =
                operator::temporal_join::TemporalJoinExecutionLimits::production(retention_ms);
            let left_schema = self.source_schemas.get(&cfg.left_table).ok_or_else(|| {
                DbError::Config(format!(
                    "temporal join [{name}] has no registered schema for left source '{}'",
                    cfg.left_table
                ))
            })?;
            let right_schema = self.source_schemas.get(&cfg.right_table).ok_or_else(|| {
                DbError::Config(format!(
                    "temporal join [{name}] has no registered schema for right source '{}'",
                    cfg.right_table
                ))
            })?;
            let left_schema = schema_with_source_row_positions(left_schema).map_err(|error| {
                DbError::Config(format!(
                    "temporal join [{name}] left source-position schema: {error}"
                ))
            })?;
            let right_schema = schema_with_source_row_positions(right_schema).map_err(|error| {
                DbError::Config(format!(
                    "temporal join [{name}] right source-position schema: {error}"
                ))
            })?;
            let op = operator::temporal_join::ManagedTemporalJoinOperator::try_new(
                name,
                cfg.clone(),
                projection_sql.map(Arc::from),
                self.ctx.clone(),
                left_schema,
                right_schema,
                self.key_group_count,
                limits,
            )?;
            #[cfg(feature = "cluster")]
            let mut op = op;
            #[cfg(feature = "cluster")]
            if let Some(scope) = &self.cluster_shuffle {
                debug_assert_eq!(
                    scope.registry.vnode_count(),
                    u32::from(self.key_group_count)
                );
                op.attach_cluster_shuffle(scope.clone());
            }
            return Ok(Box::new(op));
        }

        if let Some(cfg) = stream_join_config {
            let mut op = operator::interval_join::IntervalJoinOperator::new_with_key_groups(
                name,
                cfg.clone(),
                projection_sql.map(Arc::from),
                self.ctx.clone(),
                self.key_group_count,
            );
            if let (Some(left_schema), Some(right_schema)) = (
                self.source_schemas.get(&cfg.left_table),
                self.source_schemas.get(&cfg.right_table),
            ) {
                op.set_input_schemas(left_schema.clone(), right_schema.clone());
            }
            #[cfg(feature = "cluster")]
            if let Some(ref scope) = self.cluster_shuffle {
                debug_assert_eq!(
                    scope.registry.vnode_count(),
                    u32::from(self.key_group_count)
                );
                op.attach_cluster_shuffle(scope.clone());
            }
            return Ok(Box::new(op));
        }

        // Non-windowed now() is only valid as a retracting temporal filter under EMIT CHANGES;
        // anything else gets a typed LDB-1001 rejection.
        if window_config.is_none() {
            use crate::sql_analysis::TemporalFilterAnalysis as Tfa;
            match crate::sql_analysis::analyze_temporal_filter(sql) {
                Tfa::NotPresent => {}
                Tfa::Recognized(cfg) => {
                    let emit_changes =
                        emit_clause.is_some_and(|ec| matches!(ec, EmitClause::Changes));
                    if emit_changes {
                        return Ok(Box::new(
                            operator::temporal_filter::TemporalFilterOperator::new(
                                name,
                                sql,
                                *cfg,
                                self.prom.clone(),
                            ),
                        ));
                    }
                    return Ok(Box::new(operator::temporal_filter::RejectingOperator::new(
                        "[LDB-1001] a retracting temporal filter (time_col vs \
                         now() ± INTERVAL) must be declared `EMIT CHANGES`; \
                         append-only / EMIT ON WINDOW CLOSE / text SUBSCRIBE \
                         consumers cannot consume retractions",
                    )));
                }
                Tfa::PresentUnrecognized => {
                    return Ok(Box::new(operator::temporal_filter::RejectingOperator::new(
                        "[LDB-1001] now()/current_timestamp() in a non-windowed \
                         query is only supported as a retracting temporal filter \
                         `SELECT * FROM <src> WHERE time_col {>|>=|<|<=} now() ± \
                         INTERVAL` (or BETWEEN) declared `EMIT CHANGES`",
                    )));
                }
            }
        }

        let is_eowc = emit_clause
            .is_some_and(|ec| matches!(ec, EmitClause::OnWindowClose | EmitClause::Final));

        if is_eowc {
            let op = operator::eowc_query::EowcQueryOperator::new(
                name,
                sql,
                emit_clause.cloned(),
                window_config.cloned(),
                self.ctx.clone(),
                self.prom.clone(),
            );
            #[cfg(feature = "cluster")]
            let mut op = op;
            #[cfg(feature = "cluster")]
            if let Some(ref scope) = self.cluster_shuffle {
                op.attach_cluster_scope(scope.clone());
            }
            return Ok(Box::new(op));
        }

        // `EMIT CHANGES` is an explicit changelog; `incremental` drives the same dirty-only emit
        // internally for a terminal running-state aggregate MV.
        let emit_changelog =
            incremental || emit_clause.is_some_and(|ec| matches!(ec, EmitClause::Changes));

        let op = operator::sql_query::SqlQueryOperator::new_with_key_groups(
            name,
            sql,
            self.ctx.clone(),
            self.prom.clone(),
            emit_changelog,
            self.key_group_count,
        );
        #[cfg(feature = "cluster")]
        let mut op = op;
        #[cfg(feature = "cluster")]
        if let Some(ref cfg) = self.cluster_shuffle {
            debug_assert_eq!(cfg.registry.vnode_count(), u32::from(self.key_group_count));
            op.attach_cluster_shuffle(cfg.clone());
        }
        Ok(Box::new(op))
    }

    pub fn remove_query(&mut self, name: &str) {
        let prefix = format!("{name}::");
        let ids_to_remove: smallvec::SmallVec<[usize; 3]> = self
            .output_map
            .get(name)
            .copied()
            .into_iter()
            .chain(
                self.nodes
                    .iter()
                    .enumerate()
                    .filter(|(_, n)| !n.removed && n.name.starts_with(&prefix))
                    .map(|(i, _)| i),
            )
            .collect();

        for &id in &ids_to_remove {
            if let Some(ref prom) = self.prom {
                for phase in ["live", "prepared", "retired"] {
                    let _ = prom
                        .managed_state_accounted_bytes
                        .remove_label_values(&[&self.nodes[id].name, phase]);
                }
            }
            self.managed_state_accounting_peaks[id] = ManagedStateAccountingSnapshot::default();
            self.nodes[id].removed = true;
            self.nodes[id].replace_operator(Box::new(TombstonedOperator));
            self.nodes[id].output_routes.clear();
            for port_buf in &mut self.input_bufs[id] {
                port_buf.clear();
            }
            for slot in &mut self.input_buf_bytes[id] {
                *slot = 0;
            }
            self.order_configs.remove(&id);
            self.depends_on_stream.remove(&id);
            self.edges.retain(|e| e.source != id && e.target != id);
            self.input_sources[id].fill(usize::MAX);
            self.output_watermarks[id] = i64::MIN;
            self.output_idle[id] = false;
            self.free_node_ids.push(id);
        }

        for node in &mut self.nodes {
            node.output_routes
                .retain(|&(t, _)| !ids_to_remove.contains(&t));
        }

        self.output_map.remove(name);
        self.changelog_tables.remove(name);
        self.live_handles.remove(name);
        if !ids_to_remove.is_empty() {
            self.topo_dirty = true;
        }
    }

    #[cfg(test)]
    pub(crate) fn has_query(&self, name: &str) -> bool {
        self.output_map.contains_key(name)
    }

    #[cfg(test)]
    pub(crate) fn node_count(&self) -> usize {
        self.nodes.len()
    }

    fn compute_topo_order(&mut self) {
        let n = self.nodes.len();
        let mut in_degree = vec![0usize; n];
        let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n];

        for edge in &self.edges {
            if !self.nodes[edge.source].removed && !self.nodes[edge.target].removed {
                in_degree[edge.target] += 1;
                dependents[edge.source].push(edge.target);
            }
        }

        for deps in &mut dependents {
            deps.sort_unstable();
            deps.dedup();
        }

        in_degree.fill(0);
        for deps in &dependents {
            for &dep in deps {
                in_degree[dep] += 1;
            }
        }

        let mut queue = VecDeque::new();
        for (i, &deg) in in_degree.iter().enumerate() {
            if deg == 0 && !self.nodes[i].removed {
                queue.push_back(i);
            }
        }

        self.topo_order.clear();
        while let Some(idx) = queue.pop_front() {
            self.topo_order.push(idx);
            for &dep in &dependents[idx] {
                in_degree[dep] = in_degree[dep].saturating_sub(1);
                if in_degree[dep] == 0 {
                    queue.push_back(dep);
                }
            }
        }

        // Cycle detected: fall back to insertion order for remaining nodes.
        let active_count = self.nodes.iter().filter(|n| !n.removed).count();
        if self.topo_order.len() < active_count {
            tracing::warn!(
                ordered = self.topo_order.len(),
                total = active_count,
                "circular dependency in operator graph, \
                 falling back to insertion order for remaining nodes"
            );
            let in_order: FxHashSet<usize> = self.topo_order.iter().copied().collect();
            for i in 0..n {
                if !in_order.contains(&i) && !self.nodes[i].removed {
                    self.topo_order.push(i);
                }
            }
        }

        self.compute_node_domains();

        self.source_list.clear();
        self.source_list
            .extend(self.source_map.iter().map(|(name, node_id)| SourceRoute {
                name: Arc::clone(name),
                node_id: *node_id,
                view: SourceBatchView::Visible,
            }));
        self.source_list
            .extend(
                self.positioned_source_map
                    .iter()
                    .map(|(name, node_id)| SourceRoute {
                        name: Arc::clone(name),
                        node_id: *node_id,
                        view: SourceBatchView::Positioned,
                    }),
            );

        self.output_node_ids.clear();
        self.output_node_ids
            .extend(self.output_map.values().copied());

        self.topo_dirty = false;
    }

    /// Partition into failure domains (connected components) via union-find over undirected
    /// edges. Queries sharing a source node join one domain so they recover together —
    /// re-seeking a shared source for one would re-feed the other.
    fn compute_node_domains(&mut self) {
        fn find(parent: &mut [usize], mut x: usize) -> usize {
            while parent[x] != x {
                parent[x] = parent[parent[x]];
                x = parent[x];
            }
            x
        }

        let n = self.nodes.len();
        let mut parent: Vec<usize> = (0..n).collect();

        if !self.shared_source_isolation {
            for (name, visible) in &self.source_map {
                let Some(positioned) = self.positioned_source_map.get(name.as_ref()) else {
                    continue;
                };
                let a = find(&mut parent, *visible);
                let b = find(&mut parent, *positioned);
                if a != b {
                    parent[a] = b;
                }
            }
        }

        for edge in &self.edges {
            if self.nodes[edge.source].removed || self.nodes[edge.target].removed {
                continue;
            }
            // Cut at source nodes so two queries reading one source don't fuse into a single domain.
            // Sources have no incoming edges, so skipping their outgoing edges leaves each isolated.
            if self.shared_source_isolation && self.source_node_ids.contains(&edge.source) {
                continue;
            }
            let a = find(&mut parent, edge.source);
            let b = find(&mut parent, edge.target);
            if a != b {
                parent[a] = b;
            }
        }

        self.node_domain.clear();
        self.node_domain.resize(n, usize::MAX);
        let mut root_to_domain: FxHashMap<usize, usize> = FxHashMap::default();
        for i in 0..n {
            if self.nodes[i].removed {
                continue;
            }
            // Under isolation, sources are shared infrastructure, not a failure domain: a
            // consumer fault holds the source back, but the source itself never faults.
            // Leaving them unassigned (MAX) keeps `domain_count` equal to the number of
            // query domains, so the all-domains-failed check below stays exact.
            if self.shared_source_isolation && self.source_node_ids.contains(&i) {
                continue;
            }
            let root = find(&mut parent, i);
            let next = root_to_domain.len();
            self.node_domain[i] = *root_to_domain.entry(root).or_insert(next);
        }
        // When isolation is off this includes inert source-only domains, so
        // `failed_domains.len() == domain_count` is a conservative "all domains failed" test.
        self.domain_count = root_to_domain.len();
    }

    // A source is held back when its own domain faulted (isolation off / source unioned into a
    // consumer's domain) or, under isolation, when any domain it feeds faulted — the source is
    // cut out of every consumer's domain, so check its direct targets.
    fn source_feeds_failed_domain(&self, source_node: usize, failed: &FxHashSet<usize>) -> bool {
        if failed.contains(&self.node_domain[source_node]) {
            return true;
        }
        self.shared_source_isolation
            && self.nodes[source_node]
                .output_routes
                .iter()
                .any(|&(target, _)| failed.contains(&self.node_domain[target]))
    }

    fn visible_source_batches(
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, DbError> {
        let mut visible_sources = FxHashMap::default();
        visible_sources.reserve(source_batches.len());
        for (name, batches) in source_batches {
            if batches.is_empty() {
                continue;
            }
            let visible = batches
                .iter()
                .map(|batch| {
                    strip_source_row_positions(batch).map_err(|error| {
                        DbError::SchemaMismatch(format!(
                            "source '{name}' has invalid hidden metadata: {error}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            visible_sources.insert(Arc::clone(name), visible);
        }
        Ok(visible_sources)
    }

    fn register_source_tables(&mut self, visible_sources: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        for (name, batches) in visible_sources {
            // Lazily create the provider if register_source_schema wasn't called (e.g. tests).
            if !self.live_handles.contains_key(name.as_ref()) {
                let schema = batches[0].schema();
                self.ensure_live_provider(name, &schema);
            }
            if let Some(handle) = self.live_handles.get(name.as_ref()) {
                handle.swap(batches.clone());
            }
        }
    }

    fn finish_cycle(&mut self) {
        for handle in self.live_handles.values() {
            handle.clear();
        }
    }

    async fn execute_single_operator(
        &mut self,
        node_id: usize,
        current_watermark: i64,
        results: &mut FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<(), DbError> {
        let accept = self.nodes[node_id].operator.wants_input();
        let (mut inputs, mut input_bytes) = if accept {
            (
                std::mem::take(&mut self.input_bufs[node_id]),
                std::mem::take(&mut self.input_buf_bytes[node_id]),
            )
        } else {
            (Vec::new(), Vec::new())
        };

        let port_count = self.nodes[node_id].input_port_count;
        let frontiers: smallvec::SmallVec<[InputFrontier; 2]> = (0..port_count)
            .map(|port| {
                let upstream = self.input_sources[node_id][port];
                if upstream < self.output_watermarks.len() {
                    InputFrontier {
                        watermark: (self.output_watermarks[upstream] != i64::MIN)
                            .then_some(self.output_watermarks[upstream]),
                        idle: self.output_idle[upstream],
                    }
                } else {
                    InputFrontier::from_watermark(current_watermark)
                }
            })
            .collect();

        let output_result = self.nodes[node_id]
            .operator
            .process_with_frontiers(if accept { inputs.as_slice() } else { &[][..] }, &frontiers)
            .await;
        let output_result = match output_result {
            Ok(batches) if self.nodes[node_id].capability.managed_state.is_some() => self
                .validate_managed_state_budget(format!(
                    "operator '{}' record processing",
                    self.nodes[node_id].name
                ))
                .map(|()| batches),
            Ok(batches) => Ok(batches),
            Err(error) => Err(error),
        };

        let batches = match output_result {
            Ok(b) => {
                // Reuse the Vecs (clear preserves capacity); when !accept, leave buffers intact.
                if accept {
                    for v in &mut inputs {
                        v.clear();
                    }
                    input_bytes.fill(0);
                    self.input_bufs[node_id] = inputs;
                    self.input_buf_bytes[node_id] = input_bytes;
                    // A watermark may cross this operator only after the corresponding input was
                    // consumed. Operators return `wants_input == false` while graph-owned rows
                    // remain buffered, so advancing here would close downstream windows past data
                    // that has not run yet.
                    self.propagate_operator_frontier(node_id, &frontiers, current_watermark);
                }
                b
            }
            Err(e) => {
                if e.requires_pipeline_recovery() || e.requires_pipeline_halt() {
                    self.poison_after_terminal_error();
                    return Err(e);
                }
                // Defer (preserve input, keep the cycle alive) when the upstream
                // isn't ready, OR when a cross-node shuffle target isn't reachable
                // yet (cluster formation): aborting the whole cycle would also drop
                // co-located streams (e.g. a pass-through exactly-once sink) whose
                // source rows the generator has already advanced past — an EO gap.
                if accept && (self.depends_on_stream.contains(&node_id) || e.is_shuffle_not_ready())
                {
                    self.input_bufs[node_id] = inputs;
                    self.input_buf_bytes[node_id] = input_bytes;
                    tracing::debug!(
                        query = %self.nodes[node_id].name,
                        error = %e,
                        "Query deferred (upstream/shuffle not ready); batches preserved for retry"
                    );
                    return Ok(());
                }
                if !accept {
                    return Err(e);
                }
                // Under shared-source isolation, keep the faulted operator's input so the next cycle
                // replays it with new arrivals; returns Err to isolate the domain. Bounded by `max_replay_buffer_bytes`.
                if self.shared_source_isolation
                    && input_bytes.iter().sum::<usize>() <= self.max_replay_buffer_bytes
                {
                    self.input_bufs[node_id] = inputs;
                    self.input_buf_bytes[node_id] = input_bytes;
                    return Err(e);
                }
                for v in &mut inputs {
                    v.clear();
                }
                input_bytes.fill(0);
                self.input_bufs[node_id] = inputs;
                self.input_buf_bytes[node_id] = input_bytes;
                return Err(e);
            }
        };

        let batches = if let Some(oc) = self.order_configs.get(&node_id) {
            match oc {
                OrderOperatorConfig::TopK(c) => apply_topk_filter(&batches, c.k),
                OrderOperatorConfig::PerGroupTopK(c) => apply_topk_filter(&batches, c.k),
                _ => batches,
            }
        } else {
            batches
        };

        self.route_output(node_id, batches, results);

        Ok(())
    }

    /// Source nodes are pre-seeded in `execute_cycle`, so skip them here.
    fn propagate_operator_frontier(
        &mut self,
        node_id: usize,
        frontiers: &[InputFrontier],
        current_watermark: i64,
    ) {
        if self.source_node_ids.contains(&node_id) {
            return;
        }

        let input = merge_input_frontiers(frontiers, current_watermark);
        let output = self.nodes[node_id].operator.output_frontier(input);
        let watermark = self.output_watermarks[node_id].max(output.watermark_or_min());
        self.output_watermarks[node_id] = watermark;
        self.output_idle[node_id] = output.idle;
        if let Some(ref prom) = self.prom {
            prom.stream_watermark_ms
                .with_label_values(&[&self.nodes[node_id].name])
                .set(watermark);
        }
    }

    fn route_output(
        &mut self,
        node_id: usize,
        batches: Vec<RecordBatch>,
        results: &mut FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) {
        if batches.is_empty() {
            return;
        }
        let node_name = Arc::clone(&self.nodes[node_id].name);
        let has_routes = !self.nodes[node_id].output_routes.is_empty();
        let is_output = self.output_node_ids.contains(&node_id);

        if has_routes && !self.source_node_ids.contains(&node_id) {
            let name_ref = node_name.as_ref();
            if !self.live_handles.contains_key(name_ref) {
                let schema = batches[0].schema();
                self.ensure_live_provider(name_ref, &schema);
            }
            if let Some(handle) = self.live_handles.get(name_ref) {
                handle.swap(batches.clone());
            }
        }

        if is_output {
            results.insert(node_name, batches.clone());
        }

        let bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        let route_count = self.nodes[node_id].output_routes.len();
        if route_count == 1 {
            let (target, port) = self.nodes[node_id].output_routes[0];
            self.push_to_port(target, port, batches, bytes);
        } else if route_count > 1 {
            // Clone batches N-1 times; the last route takes ownership.
            for i in 0..route_count - 1 {
                let (target, port) = self.nodes[node_id].output_routes[i];
                self.push_to_port(target, port, batches.clone(), bytes);
            }
            let (target, port) = self.nodes[node_id].output_routes[route_count - 1];
            self.push_to_port(target, port, batches, bytes);
        }
    }

    pub(crate) async fn execute_cycle(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        current_watermark: i64,
        source_frontiers: Option<&FxHashMap<Arc<str>, InputFrontier>>,
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, DbError> {
        self.execute_cycle_with_mode(
            source_batches,
            current_watermark,
            source_frontiers,
            GraphExecutionMode::Normal,
        )
        .await
    }

    /// Complete only the staged vnode transition, without stepping operators on source or
    /// buffered graph input. This lets a fenced recovery drain the predecessor transition before
    /// adopting a newer assignment without reopening source intake.
    #[cfg(feature = "cluster")]
    pub(crate) async fn complete_pending_vnode_transition(&mut self) -> Result<bool, DbError> {
        let pending = self.has_pending_vnode_transition();
        if !pending {
            return Ok(false);
        }
        self.ensure_execution_not_poisoned()?;
        let rotation_fence = self.rotation_execution_fence.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6051] staged vnode completion requires the rotation execution fence".into(),
            )
        })?;
        let _rotation_guard = Arc::clone(rotation_fence).read_owned().await;

        // Waiting for assignment publication has not touched operator state. Cancellation after
        // this point is indeterminate for the same reason as a normal graph cycle.
        let mut attempt = GraphExecutionAttemptGuard::new(self);
        self.whole_restore_open = false;
        self.last_execution_assignment_version = None;
        let result = (|| {
            let final_owner_exit = self.has_pending_final_owner_exit();
            let execution_assignment_version = if final_owner_exit {
                None
            } else if let Some(config) = &self.cluster_shuffle {
                let assignment = config.registry.versioned_snapshot();
                let version = assignment.version();
                if assignment.owners().contains(&config.self_id) {
                    if config.sender.assignment_version() != version
                        || config.receiver.assignment_version() != version
                    {
                        return Err(DbError::ShuffleNotReady(format!(
                            "shuffle transport assignment does not match execution assignment \
                             {version}"
                        )));
                    }
                    Some(version)
                } else {
                    if config.sender.assignment_version() != 0
                        || config.receiver.assignment_version() != 0
                        || config.sender.active_assignment_digest().is_some()
                        || config.receiver.active_assignment_digest().is_some()
                    {
                        return Err(DbError::ShuffleNotReady(
                            "zero-owner vnode transition requires inactive shuffle transport"
                                .into(),
                        ));
                    }
                    None
                }
            } else {
                None
            };
            if final_owner_exit {
                self.apply_committed_final_owner_exit()?;
            } else {
                self.apply_pending_vnode_transition()?;
            }
            self.last_execution_assignment_version = execution_assignment_version;
            Ok(true)
        })();
        attempt.complete();
        result
    }

    /// Execute one aligned-checkpoint drain pass. This shares the normal cycle path but does not
    /// defer operators because the interactive query budget elapsed; backpressure gates and all
    /// operator error, watermark, state-limit, and routing behavior remain unchanged.
    pub(crate) async fn execute_checkpoint_drain_cycle(
        &mut self,
        current_watermark: i64,
        frozen_source_frontiers: Option<&FxHashMap<Arc<str>, InputFrontier>>,
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, DbError> {
        let source_batches = FxHashMap::default();
        self.execute_cycle_with_mode(
            &source_batches,
            current_watermark,
            frozen_source_frontiers,
            GraphExecutionMode::CheckpointDrain,
        )
        .await
    }

    async fn execute_cycle_with_mode(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        current_watermark: i64,
        source_frontiers: Option<&FxHashMap<Arc<str>, InputFrontier>>,
        mode: GraphExecutionMode,
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, DbError> {
        self.ensure_execution_not_poisoned()?;
        #[cfg(feature = "cluster")]
        let _rotation_guard = match self.rotation_execution_fence.as_ref() {
            Some(fence) => Some(Arc::clone(fence).read_owned().await),
            None if self.has_pending_vnode_transition() => {
                return Err(DbError::Checkpoint(
                    "[LDB-6051] staged vnode execution requires the rotation execution fence"
                        .into(),
                ));
            }
            None => None,
        };

        // Waiting for the cluster rotation fence has not admitted input or touched operator state.
        // Arm only after it is held so cancellation while ownership is rotating is not poisoned.
        let mut attempt = GraphExecutionAttemptGuard::new(self);
        let result = self
            .execute_cycle_attempt(source_batches, current_watermark, source_frontiers, mode)
            .await;
        attempt.complete();
        result
    }

    async fn execute_cycle_attempt(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        current_watermark: i64,
        source_frontiers: Option<&FxHashMap<Arc<str>, InputFrontier>>,
        mode: GraphExecutionMode,
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, DbError> {
        let visible_source_batches = Self::visible_source_batches(source_batches)?;
        self.whole_restore_open = false;

        #[cfg(feature = "cluster")]
        {
            self.last_execution_assignment_version = None;
            let execution_assignment_version = if let Some(cfg) = &self.cluster_shuffle {
                let version = cfg.registry.versioned_snapshot().version();
                if cfg.sender.assignment_version() != version
                    || cfg.receiver.assignment_version() != version
                {
                    return Err(DbError::ShuffleNotReady(format!(
                        "shuffle transport assignment does not match execution assignment {version}"
                    )));
                }
                Some(version)
            } else {
                None
            };
            self.apply_pending_vnode_transition()?;
            self.last_execution_assignment_version = execution_assignment_version;
        }

        if self.topo_dirty {
            self.compute_topo_order();
        }

        #[cfg(feature = "cluster")]
        if let Err(error) = self.pump_ordered_shuffle_prefix(current_watermark) {
            if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
                self.poison_after_terminal_error();
            }
            return Err(error);
        }

        self.register_source_tables(&visible_source_batches);
        self.prime_sources(
            source_batches,
            &visible_source_batches,
            current_watermark,
            source_frontiers,
        );

        let checkpoint_drain_nodes = if mode == GraphExecutionMode::CheckpointDrain {
            Some(self.checkpoint_drain_nodes())
        } else {
            None
        };

        let mut results = FxHashMap::default();
        let cycle_start = std::time::Instant::now();
        let topo_len = self.topo_order.len();

        self.cycle_failed_sources.clear();
        self.cycle_any_failed = false;
        self.cycle_deferred_sources.clear();
        self.cycle_any_deferred = false;
        let mut failed_domains: FxHashSet<usize> = FxHashSet::default();
        let mut first_error: Option<DbError> = None;

        for i in 0..topo_len {
            let node_id = self.topo_order[i];

            if self.nodes[node_id].removed {
                continue;
            }
            if checkpoint_drain_nodes
                .as_ref()
                .is_some_and(|drain| !drain.contains(&node_id))
            {
                continue;
            }

            // Skip a faulted domain; downstream nodes share it, so this cascades.
            if failed_domains.contains(&self.node_domain[node_id]) {
                continue;
            }

            match self.gate_decision(node_id) {
                GateDecision::Run => {}
                GateDecision::Skip => continue,
                GateDecision::Fail => {
                    self.finish_cycle();
                    self.poison_after_terminal_error();
                    return Err(DbError::BackpressureFail(format!(
                        "input buffer at capacity downstream of '{}'",
                        self.nodes[node_id].name
                    )));
                }
            }

            if mode == GraphExecutionMode::Normal && i > 0 {
                #[allow(clippy::cast_possible_truncation)]
                let elapsed_ns = cycle_start.elapsed().as_nanos() as u64;
                if elapsed_ns > self.query_budget_ns {
                    tracing::debug!(
                        skipped = topo_len - i,
                        elapsed_ms = elapsed_ns / 1_000_000,
                        "per-query budget exceeded — deferring remaining operators"
                    );

                    if let Err(e) = self
                        .run_one_deferred_operator(
                            i,
                            topo_len,
                            current_watermark,
                            &mut results,
                            &mut failed_domains,
                            &mut first_error,
                        )
                        .await
                    {
                        self.finish_cycle();
                        return Err(e);
                    }

                    break;
                }
            }

            if let Err(e) = self
                .execute_single_operator(node_id, current_watermark, &mut results)
                .await
            {
                if e.requires_pipeline_recovery() || e.requires_pipeline_halt() {
                    self.finish_cycle();
                    return Err(e);
                }
                let domain = self.node_domain[node_id];
                tracing::warn!(
                    query = %self.nodes[node_id].name,
                    error = %e,
                    domain,
                    "[LDB-3023] operator faulted; isolating its failure domain"
                );
                failed_domains.insert(domain);
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        self.record_cycle_deferrals();
        self.complete_cycle(&failed_domains, first_error)?;

        Ok(results)
    }

    fn ensure_execution_not_poisoned(&self) -> Result<(), DbError> {
        match self.execution_poison_reason() {
            Some(reason) => Err(DbError::StatefulOperatorPartialApply(reason.to_string())),
            None => Ok(()),
        }
    }

    pub(crate) fn execution_poison_reason(&self) -> Option<&'static str> {
        self.execution_poisoned
            .load(Ordering::Acquire)
            .then_some(GRAPH_EXECUTION_POISON_REASON)
    }

    #[cfg(feature = "cluster")]
    fn poison_cluster_execution(&self) {
        let pending = self.pending_vnode_transition.as_ref().and_then(|handle| {
            handle
                .lock()
                .clone()
                .map(|pending| (Arc::clone(handle), pending))
        });
        if self.installed_vnode_state.is_none() && pending.is_none() {
            return;
        }
        publish_cluster_execution_poison(
            &self.execution_poisoned,
            self.installed_vnode_state.as_ref(),
            pending.as_ref().map(|(handle, pending)| (handle, pending)),
        );
    }

    fn poison_after_terminal_error(&self) {
        #[cfg(feature = "cluster")]
        self.poison_cluster_execution();
    }

    fn complete_cycle(
        &mut self,
        failed_domains: &FxHashSet<usize>,
        first_error: Option<DbError>,
    ) -> Result<(), DbError> {
        self.finish_cycle();

        #[cfg(debug_assertions)]
        self.debug_assert_byte_sums();

        self.sample_buffer_stats();
        if failed_domains.is_empty() {
            return Ok(());
        }

        self.cycle_any_failed = true;
        let failed_names: Vec<Arc<str>> = self
            .source_list
            .iter()
            .filter(|route| self.source_feeds_failed_domain(route.node_id, failed_domains))
            .map(|route| Arc::clone(&route.name))
            .collect();
        self.cycle_failed_sources.extend(failed_names);
        if failed_domains.len() == self.domain_count {
            return Err(first_error.unwrap_or_else(|| {
                DbError::Pipeline("all operator failure domains failed without an error".into())
            }));
        }
        Ok(())
    }

    /// `(any domain faulted, local source names whose domain faulted)` from the last
    /// `execute_cycle`, draining the set. The coordinator holds back these sources' offsets.
    pub fn take_cycle_failures(&mut self) -> (bool, FxHashSet<Arc<str>>) {
        (
            self.cycle_any_failed,
            std::mem::take(&mut self.cycle_failed_sources),
        )
    }

    /// `(any retained graph work, local source names whose cursor must be withheld)` from the
    /// last cycle. A remote-only cluster deferral has an empty source set but still returns true.
    pub fn take_cycle_deferrals(&mut self) -> (bool, FxHashSet<Arc<str>>) {
        (
            self.cycle_any_deferred,
            std::mem::take(&mut self.cycle_deferred_sources),
        )
    }

    fn record_cycle_deferrals(&mut self) {
        let deferred_nodes: FxHashSet<usize> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(node_id, node)| {
                !node.removed
                    && (self.node_has_buffered_input(*node_id)
                        || node.operator.checkpoint_aligned_replay_pending()
                        || node.operator.checkpoint_drain_pending())
            })
            .map(|(node_id, _)| node_id)
            .collect();
        if deferred_nodes.is_empty() {
            return;
        }
        self.cycle_any_deferred = true;
        let deferred_domains: FxHashSet<usize> = deferred_nodes
            .iter()
            .filter(|node_id| !self.source_node_ids.contains(node_id))
            .map(|node_id| self.node_domain[*node_id])
            .collect();
        let deferred_sources: Vec<Arc<str>> = self
            .source_list
            .iter()
            .filter(|route| {
                deferred_nodes.contains(&route.node_id)
                    || self.source_feeds_failed_domain(route.node_id, &deferred_domains)
            })
            .map(|route| Arc::clone(&route.name))
            .collect();
        self.cycle_deferred_sources.extend(deferred_sources);
    }

    fn prime_sources(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        visible_source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        current_watermark: i64,
        source_frontiers: Option<&FxHashMap<Arc<str>, InputFrontier>>,
    ) {
        for route in &self.source_list {
            let batches = match route.view {
                SourceBatchView::Visible => visible_source_batches.get(&route.name),
                SourceBatchView::Positioned => source_batches.get(&route.name),
            };
            if let Some(batches) = batches {
                if !batches.is_empty() {
                    let bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
                    self.input_bufs[route.node_id][0].extend(batches.iter().cloned());
                    self.input_buf_bytes[route.node_id][0] += bytes;
                }
            }
            let frontier = source_frontiers
                .and_then(|frontiers| frontiers.get(&route.name).copied())
                .unwrap_or_else(|| InputFrontier::from_watermark(current_watermark));
            let watermark = self.output_watermarks[route.node_id].max(frontier.watermark_or_min());
            self.output_watermarks[route.node_id] = watermark;
            self.output_idle[route.node_id] = frontier.idle;
            if let Some(ref prom) = self.prom {
                prom.stream_watermark_ms
                    .with_label_values(&[&route.name])
                    .set(watermark);
            }
        }
    }

    /// Round-robin one deferred operator so a budget overrun can't starve the tail.
    /// Skips and records failed domains exactly like the main loop; only a backpressure
    /// `Fail` returns `Err` (whole-cycle halt).
    async fn run_one_deferred_operator(
        &mut self,
        i: usize,
        topo_len: usize,
        current_watermark: i64,
        results: &mut FxHashMap<Arc<str>, Vec<RecordBatch>>,
        failed_domains: &mut FxHashSet<usize>,
        first_error: &mut Option<DbError>,
    ) -> Result<(), DbError> {
        let deferred_count = topo_len - i;
        let start = self.deferred_scan_offset % deferred_count;
        for offset in 0..deferred_count {
            let j = i + (start + offset) % deferred_count;
            let deferred_id = self.topo_order[j];
            if self.nodes[deferred_id].removed {
                continue;
            }
            if failed_domains.contains(&self.node_domain[deferred_id]) {
                continue;
            }
            let has_input = self.input_bufs[deferred_id]
                .iter()
                .any(|port| !port.is_empty());
            if !has_input {
                continue;
            }
            match self.gate_decision(deferred_id) {
                GateDecision::Skip => continue,
                GateDecision::Fail => {
                    self.poison_after_terminal_error();
                    return Err(DbError::BackpressureFail(format!(
                        "input buffer at capacity downstream of '{}'",
                        self.nodes[deferred_id].name
                    )));
                }
                GateDecision::Run => {}
            }
            if let Err(e) = self
                .execute_single_operator(deferred_id, current_watermark, results)
                .await
            {
                if e.requires_pipeline_recovery() || e.requires_pipeline_halt() {
                    return Err(e);
                }
                let domain = self.node_domain[deferred_id];
                tracing::warn!(
                    query = %self.nodes[deferred_id].name,
                    error = %e,
                    domain,
                    "[LDB-3023] deferred operator faulted; isolating its failure domain"
                );
                failed_domains.insert(domain);
                if first_error.is_none() {
                    *first_error = Some(e);
                }
            }
            self.deferred_scan_offset = self.deferred_scan_offset.wrapping_add(1);
            break;
        }
        Ok(())
    }

    fn sample_buffer_stats(&mut self) {
        self.stats_tick = self.stats_tick.wrapping_add(1);
        if !self.stats_tick.is_multiple_of(STATS_SAMPLE_INTERVAL) {
            return;
        }
        self.publish_buffer_stats();
    }

    fn publish_buffer_stats(&mut self) {
        if let Some(ref prom) = self.prom {
            for (id, ports) in self.input_buf_bytes.iter().enumerate() {
                if self.nodes[id].removed {
                    continue;
                }
                let total: usize = ports.iter().sum();
                prom.input_buf_bytes
                    .with_label_values(&[&self.nodes[id].name])
                    .set(i64::try_from(total).unwrap_or(i64::MAX));
            }
            for (id, node) in self.nodes.iter().enumerate() {
                if node.removed {
                    for phase in ["live", "prepared", "retired"] {
                        let _ = prom
                            .managed_state_accounted_bytes
                            .remove_label_values(&[&node.name, phase]);
                    }
                    self.managed_state_accounting_peaks[id] =
                        ManagedStateAccountingSnapshot::default();
                    continue;
                }
                let Some(mut accounting) = node.operator.managed_state_accounting() else {
                    continue;
                };
                let peaks = std::mem::take(&mut self.managed_state_accounting_peaks[id]);
                accounting.prepared = accounting.prepared.max(peaks.prepared);
                accounting.retired = accounting.retired.max(peaks.retired);
                for (phase, bytes) in [
                    ("live", accounting.live),
                    ("prepared", accounting.prepared),
                    ("retired", accounting.retired),
                ] {
                    prom.managed_state_accounted_bytes
                        .with_label_values(&[&node.name, phase])
                        .set(i64::try_from(bytes).unwrap_or(i64::MAX));
                }
            }
        }
    }

    /// Retain transient managed-state ownership until the next cold metrics sample.
    ///
    /// This performs no Prometheus work and is called only from vnode lifecycle paths.
    #[cfg(feature = "cluster")]
    fn observe_managed_state_accounting(&mut self, node_indices: &[usize]) {
        for &node_idx in node_indices {
            let node = &self.nodes[node_idx];
            if node.removed {
                continue;
            }
            if let Some(accounting) = node.operator.managed_state_accounting() {
                self.managed_state_accounting_peaks[node_idx].observe_transient(accounting);
            }
        }
    }

    #[cfg(feature = "cluster")]
    fn shuffle_stage_node(&self, stage: &str) -> Result<usize, DbError> {
        if let Some(idx) = self.find_node(stage) {
            return Ok(idx);
        }
        let node_name = stage
            .strip_suffix("::left")
            .or_else(|| stage.strip_suffix("::right"));
        node_name
            .and_then(|name| (!name.is_empty()).then_some(name))
            .and_then(|name| self.find_node(name))
            .ok_or_else(|| {
                DbError::ShuffleTerminal(format!(
                    "shuffle frame targets unknown or removed stage '{stage}'"
                ))
            })
    }

    #[cfg(feature = "cluster")]
    fn stage_checkpointed_shuffle(
        &mut self,
        stage: &str,
        batch: RetainedBatch,
        watermark: i64,
    ) -> Result<(), DbError> {
        let idx = self.shuffle_stage_node(stage)?;
        let result = self.nodes[idx]
            .operator
            .stage_checkpointed_shuffle(stage, batch, watermark);
        if result.is_ok() {
            self.output_watermarks[idx] = self.output_watermarks[idx].min(watermark);
            self.output_idle[idx] = false;
        }
        result
    }

    #[cfg(feature = "cluster")]
    fn stage_checkpointed_shuffle_frontier(
        &mut self,
        stage: &str,
        peer: u64,
        frontier: InputFrontier,
        assignment_version: u64,
        recovery_gen: u64,
    ) -> Result<(), DbError> {
        let idx = self.shuffle_stage_node(stage)?;
        self.nodes[idx]
            .operator
            .stage_checkpointed_shuffle_frontier(
                stage,
                peer,
                frontier,
                assignment_version,
                recovery_gen,
            )
    }

    #[cfg(feature = "cluster")]
    fn validate_current_received_frontier(
        &self,
        cfg: &crate::operator::sql_query::ClusterShuffleConfig,
        received: &laminar_core::shuffle::ReceivedShuffle,
    ) -> Result<(), DbError> {
        let laminar_core::shuffle::ShuffleMessage::Frontier {
            stage, watermark, ..
        } = received.message()
        else {
            return Err(DbError::ShuffleTerminal(
                "non-frontier frame entered ordered shuffle frontier staging".into(),
            ));
        };
        if stage.is_empty() || *watermark == Some(i64::MIN) {
            return Err(DbError::ShuffleTerminal(
                "shuffle frontier has a non-canonical stage or watermark".into(),
            ));
        }
        let current_assignment = cfg.registry.assignment_version();
        let current_recovery = cfg.receiver.recovery_gen();
        if received.peer() == cfg.self_id.0
            || received.stream_id().is_nil()
            || received.receiver_incarnation() != cfg.receiver.incarnation()
            || received.assignment_version() != current_assignment
            || received.assignment_version() != cfg.sender.assignment_version()
            || received.assignment_version() != cfg.receiver.assignment_version()
            || received.recovery_gen() != current_recovery
            || received.recovery_gen() != cfg.sender.recovery_gen()
        {
            return Err(DbError::Checkpoint(format!(
                "shuffle frontier from peer {} is outside current assignment {current_assignment} recovery {current_recovery}",
                received.peer()
            )));
        }
        self.shuffle_stage_node(stage)?;
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn validate_received_frontier_cuts(
        &self,
        cfg: &crate::operator::sql_query::ClusterShuffleConfig,
        cuts: &[laminar_core::shuffle::ReceivedFrontierCut],
    ) -> Result<(), DbError> {
        for cut in cuts {
            let frontier = cut.frontier();
            self.validate_current_received_frontier(cfg, frontier)?;
            for batch in cut.preceding() {
                if batch.peer() != frontier.peer()
                    || batch.sender_incarnation() != frontier.sender_incarnation()
                    || batch.receiver_incarnation() != frontier.receiver_incarnation()
                    || batch.stream_id() != frontier.stream_id()
                    || batch.assignment_version() != frontier.assignment_version()
                    || batch.recovery_gen() != frontier.recovery_gen()
                    || batch.checkpoint_sequence() >= frontier.checkpoint_sequence()
                {
                    return Err(DbError::Checkpoint(format!(
                        "shuffle frontier cut from peer {} is not one ordered stream prefix",
                        frontier.peer()
                    )));
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn post_dequeue_shuffle_error(context: &str, error: DbError) -> DbError {
        if error.requires_pipeline_halt() || error.requires_pipeline_recovery() {
            error
        } else {
            DbError::StatefulOperatorPartialApply(format!("{context}: {error}"))
        }
    }

    #[cfg(feature = "cluster")]
    fn dispatch_received_frontier_cut(
        &mut self,
        cut: laminar_core::shuffle::ReceivedFrontierCut,
        batch_watermark: i64,
    ) -> Result<(), DbError> {
        let (preceding, received) = cut.into_parts();
        let peer = received.peer();
        let assignment_version = received.assignment_version();
        let recovery_gen = received.recovery_gen();
        let laminar_core::shuffle::ShuffleMessage::Frontier {
            stage,
            watermark,
            idle,
        } = received.message()
        else {
            return Err(DbError::ShuffleTerminal(
                "non-frontier frame entered ordered shuffle frontier dispatch".into(),
            ));
        };
        for batch in preceding {
            self.stage_checkpointed_shuffle(
                stage,
                RetainedBatch::from_received(batch),
                batch_watermark,
            )
            .map_err(|error| {
                Self::post_dequeue_shuffle_error("ordered shuffle batch staging", error)
            })?;
        }
        self.stage_checkpointed_shuffle_frontier(
            stage,
            peer,
            InputFrontier {
                watermark: *watermark,
                idle: *idle,
            },
            assignment_version,
            recovery_gen,
        )
        .map_err(|error| {
            Self::post_dequeue_shuffle_error("ordered shuffle frontier staging", error)
        })
    }

    #[cfg(feature = "cluster")]
    fn dispatch_validated_frontier_cuts(
        &mut self,
        cuts: Vec<laminar_core::shuffle::ReceivedFrontierCut>,
        batch_watermark: i64,
    ) -> Result<(), DbError> {
        for cut in cuts {
            self.dispatch_received_frontier_cut(cut, batch_watermark)?;
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn pump_ordered_shuffle_prefix(&mut self, watermark: i64) -> Result<(), DbError> {
        let Some(cfg) = self.cluster_shuffle.clone() else {
            return Ok(());
        };
        Self::ensure_shuffle_delivery_intact(&cfg)
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;

        // The receiver stops at the first ordered control and caps this pass by its admitted
        // holdover, so one graph cycle cannot chase an unbounded live queue.
        let staged = cfg.receiver.drain_checkpointed_staged();
        let cuts = cfg.receiver.drain_staged_frontiers();
        for (stage, _) in &staged {
            self.shuffle_stage_node(stage)?;
        }
        self.validate_received_frontier_cuts(&cfg, &cuts)?;

        for (stage, batch) in staged {
            self.stage_checkpointed_shuffle(&stage, RetainedBatch::from_received(batch), watermark)
                .map_err(|error| {
                    Self::post_dequeue_shuffle_error("checkpointed shuffle batch staging", error)
                })?;
        }
        self.dispatch_validated_frontier_cuts(cuts, watermark)?;
        Self::ensure_shuffle_delivery_intact(&cfg)
            .map_err(|error| DbError::Checkpoint(error.to_string()))
    }

    #[cfg(feature = "cluster")]
    fn stage_received_shuffle_data(
        &mut self,
        received: laminar_core::shuffle::ReceivedShuffle,
        watermark: i64,
    ) -> Result<(), DbError> {
        let peer = received.peer();
        let assignment_version = received.assignment_version();
        let recovery_gen = received.recovery_gen();
        let (message, admission) = received.into_parts();
        let laminar_core::shuffle::ShuffleMessage::Data {
            stage,
            routed_vnodes,
            batch,
        } = message
        else {
            return Err(DbError::Pipeline(
                "non-data frame entered shuffle data staging".into(),
            ));
        };
        self.stage_checkpointed_shuffle(
            &stage,
            RetainedBatch::admitted(
                batch,
                admission,
                peer,
                assignment_version,
                recovery_gen,
                routed_vnodes,
            ),
            watermark,
        )
    }

    #[cfg(feature = "cluster")]
    fn stage_received_shuffle_frontier(
        &mut self,
        received: &laminar_core::shuffle::ReceivedShuffle,
    ) -> Result<(), DbError> {
        let peer = received.peer();
        let assignment_version = received.assignment_version();
        let recovery_gen = received.recovery_gen();
        let laminar_core::shuffle::ShuffleMessage::Frontier {
            stage,
            watermark,
            idle,
        } = received.message()
        else {
            return Err(DbError::Pipeline(
                "non-frontier frame entered shuffle frontier staging".into(),
            ));
        };
        self.stage_checkpointed_shuffle_frontier(
            stage,
            peer,
            InputFrontier {
                watermark: *watermark,
                idle: *idle,
            },
            assignment_version,
            recovery_gen,
        )
    }

    #[cfg(feature = "cluster")]
    fn stage_received_ordered_shuffle(
        &mut self,
        received: laminar_core::shuffle::ReceivedShuffle,
        watermark: i64,
    ) -> Result<(), DbError> {
        match received.message() {
            laminar_core::shuffle::ShuffleMessage::Data { .. } => {
                self.stage_received_shuffle_data(received, watermark)
            }
            laminar_core::shuffle::ShuffleMessage::Frontier { .. } => {
                self.stage_received_shuffle_frontier(&received)
            }
            laminar_core::shuffle::ShuffleMessage::Barrier(_) => Err(DbError::Pipeline(
                "barrier entered ordered shuffle data/frontier staging".into(),
            )),
        }
    }

    #[cfg(feature = "cluster")]
    fn validate_shuffle_attempt_scope(
        cfg: &crate::operator::sql_query::ClusterShuffleConfig,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        recovery_gen: u64,
        controller: Option<&laminar_core::cluster::control::ClusterController>,
    ) -> Result<(), DbError> {
        if !assignment_fence.is_canonical() || !assignment_fence.contains(cfg.self_id.0) {
            return Err(DbError::Pipeline(
                "shuffle alignment has a non-canonical or incomplete assignment certificate".into(),
            ));
        }
        let assignment = cfg.registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        let local_incarnation = assignment_fence.participant_incarnation(cfg.self_id.0);
        if assignment.version() != assignment_fence.assignment_version
            || !assignment_fence.matches_owner_map(&owners)
            || cfg.sender.assignment_version() != assignment_fence.assignment_version
            || cfg.receiver.assignment_version() != assignment_fence.assignment_version
            || local_incarnation != Some(cfg.sender.incarnation())
            || local_incarnation != Some(cfg.receiver.incarnation())
        {
            return Err(DbError::Pipeline(format!(
                "shuffle assignment differs from admitted certificate version {}",
                assignment_fence.assignment_version
            )));
        }
        if cfg.sender.recovery_gen() != recovery_gen || cfg.receiver.recovery_gen() != recovery_gen
        {
            return Err(DbError::Pipeline(format!(
                "shuffle recovery generation changed during alignment from {recovery_gen}"
            )));
        }
        if let Some(controller) = controller {
            let current = controller
                .checkpoint_assignment_fence(assignment_fence.assignment_version)
                .ok_or_else(|| {
                    DbError::Pipeline(format!(
                        "shuffle assignment {} is no longer checkpoint-ready",
                        assignment_fence.assignment_version
                    ))
                })?;
            if current != *assignment_fence {
                return Err(DbError::Pipeline(format!(
                    "shuffle assignment certificate changed at version {}",
                    assignment_fence.assignment_version
                )));
            }
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn validate_received_shuffle_scope(
        received: &laminar_core::shuffle::ReceivedShuffle,
        self_id: u64,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        recovery_gen: u64,
    ) -> Result<(), DbError> {
        let peer = received.peer();
        let expected_sender = assignment_fence.participant_incarnation(peer);
        let expected_receiver = assignment_fence.participant_incarnation(self_id);
        if peer == self_id
            || !assignment_fence.contains(peer)
            || expected_sender != Some(received.sender_incarnation())
            || expected_receiver != Some(received.receiver_incarnation())
            || received.stream_id().is_nil()
            || received.assignment_version() != assignment_fence.assignment_version
            || received.recovery_gen() != recovery_gen
        {
            return Err(DbError::Pipeline(format!(
                "shuffle frame from peer {peer} has the wrong assignment, recovery, or stream scope"
            )));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn validate_received_batch_scope(
        received: &laminar_core::shuffle::ReceivedBatch,
        self_id: u64,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        recovery_gen: u64,
    ) -> Result<(), DbError> {
        let peer = received.peer();
        let expected_sender = assignment_fence.participant_incarnation(peer);
        let expected_receiver = assignment_fence.participant_incarnation(self_id);
        if peer == self_id
            || !assignment_fence.contains(peer)
            || expected_sender != Some(received.sender_incarnation())
            || expected_receiver != Some(received.receiver_incarnation())
            || received.stream_id().is_nil()
            || received.assignment_version() != assignment_fence.assignment_version
            || received.recovery_gen() != recovery_gen
        {
            return Err(DbError::Pipeline(format!(
                "shuffle batch from peer {peer} has the wrong assignment, recovery, or stream scope"
            )));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn compare_shuffle_attempts(
        expected: laminar_core::checkpoint::CheckpointAttempt,
        observed: laminar_core::checkpoint::CheckpointAttempt,
    ) -> Result<std::cmp::Ordering, DbError> {
        match observed.relation_to(expected) {
            laminar_core::checkpoint::CheckpointAttemptRelation::Exact => {
                Ok(std::cmp::Ordering::Equal)
            }
            laminar_core::checkpoint::CheckpointAttemptRelation::Newer => {
                Ok(std::cmp::Ordering::Greater)
            }
            laminar_core::checkpoint::CheckpointAttemptRelation::Older => {
                Ok(std::cmp::Ordering::Less)
            }
            laminar_core::checkpoint::CheckpointAttemptRelation::Conflict => {
                Err(DbError::Pipeline(format!(
                    "shuffle barrier attempt mismatch: expected {expected:?}, received {observed:?}"
                )))
            }
        }
    }

    #[cfg(feature = "cluster")]
    fn is_shuffle_alignment_terminal_hint(
        attempt: laminar_core::checkpoint::CheckpointAttempt,
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
        ignored: Option<(u64, u64, laminar_core::cluster::control::Phase)>,
    ) -> bool {
        use laminar_core::cluster::control::Phase;

        if ignored
            == Some((
                announcement.epoch,
                announcement.checkpoint_id,
                announcement.phase,
            ))
            || !matches!(announcement.phase, Phase::Commit | Phase::Abort)
        {
            return false;
        }
        let announced = laminar_core::checkpoint::CheckpointAttempt::new(
            announcement.epoch,
            announcement.checkpoint_id,
        );
        !matches!(
            announced.relation_to(attempt),
            laminar_core::checkpoint::CheckpointAttemptRelation::Older
        )
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_shuffle_alignment_terminal_hint(
        controller: Option<&laminar_core::cluster::control::ClusterController>,
        attempt: laminar_core::checkpoint::CheckpointAttempt,
        ignored: Option<(u64, u64, laminar_core::cluster::control::Phase)>,
        deadline: tokio::time::Instant,
    ) -> Result<Option<laminar_core::cluster::control::BarrierAnnouncement>, DbError> {
        let Some(controller) = controller else {
            tokio::time::sleep_until(deadline).await;
            return Ok(None);
        };
        controller
            .wait_for_barrier(
                |announcement| {
                    Self::is_shuffle_alignment_terminal_hint(attempt, announcement, ignored)
                },
                deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "shuffle barrier control observation failed: {error}"
                ))
            })
    }

    #[cfg(feature = "cluster")]
    async fn audit_shuffle_alignment_settlement(
        controller: Option<&laminar_core::cluster::control::ClusterController>,
        attempt: laminar_core::checkpoint::CheckpointAttempt,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    ) -> Result<Option<ShuffleAlignmentOutcome>, DbError> {
        let Some(controller) = controller else {
            return Ok(None);
        };

        // Announcements only wake this audit; immutable authority alone settles alignment.
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Pipeline(format!(
                "shuffle barrier terminal authority is unavailable: {error}"
            ))
        })?;
        let durable = authority
            .cluster_attempt_settlement(attempt)
            .await
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "shuffle barrier terminal outcome audit failed: {error}"
                ))
            })?;
        let Some(durable) = durable else {
            return Ok(None);
        };
        if durable.scope != laminar_core::checkpoint_decision::CheckpointScope::Cluster {
            return Err(DbError::Pipeline(format!(
                "shuffle barrier settlement for checkpoint {} epoch {} has non-cluster scope {:?}",
                attempt.checkpoint_id, attempt.epoch, durable.scope
            )));
        }
        let durable_attempt =
            laminar_core::checkpoint::CheckpointAttempt::new(durable.epoch, durable.checkpoint_id);
        match durable_attempt.relation_to(attempt) {
            laminar_core::checkpoint::CheckpointAttemptRelation::Exact => {
                if durable.assignment_fence.as_ref() != Some(assignment_fence) {
                    return Err(DbError::Pipeline(format!(
                        "shuffle barrier terminal outcome for checkpoint {} epoch {} has a different assignment certificate",
                        attempt.checkpoint_id, attempt.epoch
                    )));
                }
                if durable.verdict != laminar_core::checkpoint_decision::CheckpointVerdict::Abort {
                    return Err(DbError::Pipeline(format!(
                        "shuffle barrier alignment for checkpoint {} epoch {} observed durable {:?} instead of Abort",
                        attempt.checkpoint_id, attempt.epoch, durable.verdict
                    )));
                }
                Ok(Some(ShuffleAlignmentOutcome::Aborted))
            }
            laminar_core::checkpoint::CheckpointAttemptRelation::Newer => {
                Err(DbError::Pipeline(format!(
                    "checkpoint {} epoch {} was superseded by durable terminal checkpoint {} epoch {} ({:?})",
                    attempt.checkpoint_id,
                    attempt.epoch,
                    durable.checkpoint_id,
                    durable.epoch,
                    durable.verdict
                )))
            }
            laminar_core::checkpoint::CheckpointAttemptRelation::Older
            | laminar_core::checkpoint::CheckpointAttemptRelation::Conflict => {
                Err(DbError::Pipeline(format!(
                    "shuffle barrier authority returned invalid settlement {durable_attempt:?} for pending attempt {attempt:?}"
                )))
            }
        }
    }

    #[cfg(feature = "cluster")]
    fn ensure_shuffle_delivery_intact(
        cfg: &crate::operator::sql_query::ClusterShuffleConfig,
    ) -> Result<(), DbError> {
        if cfg.receiver.has_unrecovered_delivery_loss() {
            Err(DbError::Pipeline(
                "shuffle delivery-domain or transit loss requires recovery".into(),
            ))
        } else {
            Ok(())
        }
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_remaining_shuffle_barriers(
        &mut self,
        cfg: &crate::operator::sql_query::ClusterShuffleConfig,
        attempt: laminar_core::checkpoint::CheckpointAttempt,
        watermark: i64,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        deadline: tokio::time::Instant,
        controller: Option<&laminar_core::cluster::control::ClusterController>,
        recovery_gen: u64,
        mut remaining: rustc_hash::FxHashSet<u64>,
        mut barrier_cuts: rustc_hash::FxHashMap<u64, u64>,
    ) -> Result<ShuffleAlignmentOutcome, DbError> {
        use laminar_core::shuffle::ShuffleMessage;

        const RECHECK: std::time::Duration = std::time::Duration::from_millis(500);
        let mut check_interval =
            tokio::time::interval_at(tokio::time::Instant::now() + RECHECK, RECHECK);
        check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut ignored_terminal_hint = None;
        let terminal_hint = Self::wait_for_shuffle_alignment_terminal_hint(
            controller,
            attempt,
            ignored_terminal_hint,
            deadline,
        );
        tokio::pin!(terminal_hint);
        loop {
            tokio::select! {
                res = cfg.receiver.recv() => {
                    let received = res.ok_or_else(|| DbError::Pipeline(
                        "shuffle receiver closed during barrier alignment".into(),
                    ))?;
                    Self::validate_received_shuffle_scope(
                        &received,
                        cfg.self_id.0,
                        assignment_fence,
                        recovery_gen,
                    )?;
                    if matches!(
                        received.message(),
                        ShuffleMessage::Data { .. } | ShuffleMessage::Frontier { .. }
                    ) {
                        let peer = received.peer();
                        let sequence = received.checkpoint_sequence();
                        if let Some(cut) = barrier_cuts.get(&peer) {
                            if let Some(outcome) = Self::audit_shuffle_alignment_settlement(
                                controller,
                                attempt,
                                assignment_fence,
                            )
                            .await?
                            {
                                self.stage_received_ordered_shuffle(received, watermark)?;
                                return Ok(outcome);
                            }
                            return Err(DbError::Pipeline(format!(
                                "shuffle ordered frame sequence {sequence} from peer {peer} arrived after its checkpoint barrier high-water {cut} while peers {remaining:?} were still outstanding"
                            )));
                        }
                        self.stage_received_ordered_shuffle(received, watermark)?;
                        continue;
                    }

                    let ShuffleMessage::Barrier(barrier) = received.message() else {
                        unreachable!("shuffle message variants are exhaustive");
                    };
                    if received.assignment_digest() != Some(assignment_fence.digest()) {
                        return Err(DbError::Pipeline(format!(
                            "shuffle barrier from peer {} has the wrong assignment certificate",
                            received.peer()
                        )));
                    }
                    let observed = laminar_core::checkpoint::CheckpointAttempt::new(
                        barrier.epoch,
                        barrier.checkpoint_id,
                    );
                    match Self::compare_shuffle_attempts(attempt, observed)? {
                        std::cmp::Ordering::Equal => {
                            let peer = received.peer();
                            let cut = received.checkpoint_sequence();
                            if let Some(previous) = barrier_cuts.insert(peer, cut) {
                                if previous != cut {
                                    return Err(DbError::Pipeline(format!(
                                        "shuffle peer {peer} repeated checkpoint barrier with conflicting high-water sequences {previous} and {cut}"
                                    )));
                                }
                            }
                            let first_observation = remaining.remove(&peer);
                            if first_observation && remaining.is_empty() {
                                break;
                            }
                        }
                        std::cmp::Ordering::Greater => {
                            cfg.receiver.stash_barrier(received);
                        }
                        std::cmp::Ordering::Less => {}
                    }
                }
                hint = &mut terminal_hint => {
                    let Some(hint) = hint? else {
                        return Err(DbError::Checkpoint(format!(
                            "shuffle barrier control wait exhausted the absolute deadline for checkpoint {} epoch {}",
                            attempt.checkpoint_id, attempt.epoch
                        )));
                    };
                    ignored_terminal_hint = Some((
                        hint.epoch,
                        hint.checkpoint_id,
                        hint.phase,
                    ));
                    Self::validate_shuffle_attempt_scope(
                        cfg,
                        assignment_fence,
                        recovery_gen,
                        controller,
                    )?;
                    Self::ensure_shuffle_delivery_intact(cfg)?;
                    if let Some(outcome) = Self::audit_shuffle_alignment_settlement(
                        controller,
                        attempt,
                        assignment_fence,
                    )
                    .await?
                    {
                        return Ok(outcome);
                    }
                    check_interval.reset_at(tokio::time::Instant::now() + RECHECK);
                    terminal_hint.set(Self::wait_for_shuffle_alignment_terminal_hint(
                        controller,
                        attempt,
                        ignored_terminal_hint,
                        deadline,
                    ));
                }
                _ = check_interval.tick() => {
                    Self::validate_shuffle_attempt_scope(
                        cfg,
                        assignment_fence,
                        recovery_gen,
                        controller,
                    )?;
                    Self::ensure_shuffle_delivery_intact(cfg)?;
                    if let Some(outcome) =
                        Self::audit_shuffle_alignment_settlement(
                            controller,
                            attempt,
                            assignment_fence,
                        )
                        .await?
                    {
                        return Ok(outcome);
                    }
                }
            }
        }
        Self::validate_shuffle_attempt_scope(cfg, assignment_fence, recovery_gen, controller)?;
        Self::ensure_shuffle_delivery_intact(cfg)?;
        if let Some(outcome) =
            Self::audit_shuffle_alignment_settlement(controller, attempt, assignment_fence).await?
        {
            return Ok(outcome);
        }
        tracing::debug!(
            checkpoint_id = attempt.checkpoint_id,
            epoch = attempt.epoch,
            "shuffle align: complete"
        );
        Ok(ShuffleAlignmentOutcome::Aligned)
    }
    /// Aligned shuffle checkpointing: fan out an in-band barrier, retain each peer's pre-barrier
    /// rows as channel state, and wait until every peer's barrier is observed before snapshotting.
    /// A barrier closes that peer's current-attempt channel: data from an already aligned peer while
    /// other peers are outstanding is a protocol violation and requires recovery.
    ///
    /// An exact, authority-validated leader Abort is a normal terminal outcome: rows dequeued
    /// before observing it remain staged in the live graph, and no snapshot was captured. A
    /// transport scope cancellation before local staging preserves the complete holdover for
    /// exact-attempt cleanup and a later checkpoint. Every failure after staging still requires
    /// coordinated recovery because partial staging may otherwise lose or double-apply data.
    ///
    /// # Errors
    /// Returns a recovery-classified error for timeout, loss, scope conflict, or supersession.
    #[cfg(feature = "cluster")]
    pub(crate) async fn align_shuffle_barriers(
        &mut self,
        attempt: laminar_core::checkpoint::CheckpointAttempt,
        watermark: i64,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        deadline: tokio::time::Instant,
        controller: Option<&laminar_core::cluster::control::ClusterController>,
    ) -> Result<ShuffleAlignmentOutcome, DbError> {
        use laminar_core::checkpoint::barrier::CheckpointBarrier;
        use laminar_core::shuffle::ShuffleMessage;
        use rustc_hash::{FxHashMap, FxHashSet};

        let Some(cfg) = self.cluster_shuffle.clone() else {
            return Ok(ShuffleAlignmentOutcome::Aligned);
        };
        let alignment = tokio::time::timeout_at(deadline, async {
            if cfg.receiver.assignment_version() == 0 || cfg.sender.assignment_version() == 0 {
                return Ok(ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging);
            }
            let recovery_gen = cfg.receiver.recovery_gen();
            Self::validate_shuffle_attempt_scope(
                &cfg,
                assignment_fence,
                recovery_gen,
                controller,
            )?;
            Self::ensure_shuffle_delivery_intact(&cfg)?;
            let peers: Vec<u64> = assignment_fence
                .participants
                .iter()
                .map(|participant| participant.node_id)
                .filter(|peer| *peer != cfg.self_id.0)
                .collect();
            if peers.is_empty() {
                return Ok(ShuffleAlignmentOutcome::Aligned);
            }

            let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);
            if let Err(error) = cfg
                .sender
                .fan_out_barrier(&peers, barrier, assignment_fence)
                .await
            {
                if laminar_core::shuffle::is_scope_cancelled(&error) {
                    return Ok(ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging);
                }
                return Err(DbError::Pipeline(format!(
                    "shuffle barrier fan-out: {error}"
                )));
            }

            // Fan-out can cancel while waiting on admission. Keep holdover ownership in the
            // receiver until every failed peer has either accepted or explicitly left the scope.
            let exposed_frontiers = cfg.receiver.drain_staged_frontiers();
            self.validate_received_frontier_cuts(&cfg, &exposed_frontiers)?;
            self.dispatch_validated_frontier_cuts(exposed_frontiers, watermark)?;
            let staged_batches = match cfg.receiver.drain_checkpointed_holdover() {
                Ok(staged) => staged,
                Err(error) if laminar_core::shuffle::is_scope_cancelled(&error) => {
                    return Ok(ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Err(DbError::Pipeline(
                        "shuffle frontier holdover did not settle before checkpoint transfer"
                            .into(),
                    ));
                }
                Err(error) => {
                    return Err(DbError::Pipeline(format!(
                        "shuffle checkpoint holdover drain: {error}"
                    )));
                }
            };
            Self::ensure_shuffle_delivery_intact(&cfg)?;

            let mut remaining: FxHashSet<u64> = peers.iter().copied().collect();
            let mut barrier_cuts: FxHashMap<u64, u64> = FxHashMap::default();
            tracing::debug!(
                checkpoint_id = attempt.checkpoint_id,
                epoch = attempt.epoch,
                self_id = cfg.self_id.0,
                peers = ?peers,
                "shuffle align: start"
            );

            for received in cfg.receiver.drain_staged_barriers() {
                Self::validate_received_shuffle_scope(
                    &received,
                    cfg.self_id.0,
                    assignment_fence,
                    recovery_gen,
                )?;
                if received.assignment_digest() != Some(assignment_fence.digest()) {
                    return Err(DbError::Pipeline(format!(
                        "shuffle barrier from peer {} has the wrong assignment certificate",
                        received.peer()
                    )));
                }
                let ShuffleMessage::Barrier(barrier) = received.message() else {
                    return Err(DbError::Pipeline(
                        "non-barrier frame entered shuffle barrier holdover".into(),
                    ));
                };
                let observed = laminar_core::checkpoint::CheckpointAttempt::new(
                    barrier.epoch,
                    barrier.checkpoint_id,
                );
                match Self::compare_shuffle_attempts(attempt, observed)? {
                    std::cmp::Ordering::Equal => {
                        let peer = received.peer();
                        let cut = received.checkpoint_sequence();
                        if let Some(previous) = barrier_cuts.insert(peer, cut) {
                            if previous != cut {
                                return Err(DbError::Pipeline(format!(
                                    "shuffle peer {peer} repeated checkpoint barrier with conflicting high-water sequences {previous} and {cut}"
                                )));
                            }
                        }
                        remaining.remove(&peer);
                    }
                    std::cmp::Ordering::Greater => cfg.receiver.stash_barrier(received),
                    std::cmp::Ordering::Less => {}
                }
            }

            // A normal execution cycle may already have bucketed data and a later barrier into
            // separate holdovers. The transport sequence reconstructs their per-peer order: data
            // below the barrier's exclusive high-water belongs to this cut; data at/above it does
            // not and cannot be sealed into the current snapshot.
            let mut post_cut_batches = Vec::new();
            let mut post_cut_error = None;
            for (stage, received) in staged_batches {
                Self::validate_received_batch_scope(
                    &received,
                    cfg.self_id.0,
                    assignment_fence,
                    recovery_gen,
                )?;
                let peer = received.peer();
                let sequence = received.checkpoint_sequence();
                if let Some(cut) = barrier_cuts.get(&peer) {
                    if sequence >= *cut {
                        post_cut_error.get_or_insert_with(|| {
                            format!(
                                "shuffle data sequence {sequence} from peer {peer} is at or after its checkpoint barrier high-water {cut}"
                            )
                        });
                        post_cut_batches.push((stage, received));
                        continue;
                    }
                }
                self.stage_checkpointed_shuffle(
                    &stage,
                    RetainedBatch::from_received(received),
                    watermark,
                )?;
            }
            Self::ensure_shuffle_delivery_intact(&cfg)?;
            if let Some(error) = post_cut_error {
                if let Some(outcome) =
                    Self::audit_shuffle_alignment_settlement(
                        controller,
                        attempt,
                        assignment_fence,
                    )
                    .await?
                {
                    for (stage, received) in post_cut_batches {
                        self.stage_checkpointed_shuffle(
                            &stage,
                            RetainedBatch::from_received(received),
                            watermark,
                        )?;
                    }
                    return Ok(outcome);
                }
                return Err(DbError::Pipeline(error));
            }
            if remaining.is_empty() {
                Self::validate_shuffle_attempt_scope(
                    &cfg,
                    assignment_fence,
                    recovery_gen,
                    controller,
                )?;
                Self::ensure_shuffle_delivery_intact(&cfg)?;
                if let Some(outcome) =
                    Self::audit_shuffle_alignment_settlement(
                        controller,
                        attempt,
                        assignment_fence,
                    )
                    .await?
                {
                    return Ok(outcome);
                }
                return Ok(ShuffleAlignmentOutcome::Aligned);
            }

            return self
                .wait_for_remaining_shuffle_barriers(
                    &cfg,
                    attempt,
                    watermark,
                    assignment_fence,
                    deadline,
                    controller,
                    recovery_gen,
                    remaining,
                    barrier_cuts,
                )
                .await;
        })
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "shuffle barrier alignment exhausted the absolute deadline for checkpoint {} epoch {}",
                attempt.checkpoint_id, attempt.epoch
            ))
        })?;
        alignment.map_err(|error| {
            DbError::Checkpoint(format!(
                "shuffle barrier alignment for checkpoint {} epoch {} requires recovery: {error}",
                attempt.checkpoint_id, attempt.epoch
            ))
        })
    }

    #[cfg(test)]
    pub(crate) fn push_test_node(&mut self, name: &str, operator: Box<dyn GraphOperator>) {
        self.allocate_node(GraphNode::new(Arc::from(name), operator, 1));
        self.topo_dirty = true;
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) fn set_test_vnode_count(&mut self, vnode_count: u32) {
        self.set_test_owned_vnodes(vnode_count, (0..vnode_count).collect());
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) fn set_test_owned_vnodes(&mut self, vnode_count: u32, owned_vnodes: Vec<u32>) {
        assert!(
            !owned_vnodes.iter().any(|vnode| *vnode >= vnode_count)
                && !owned_vnodes.windows(2).any(|pair| pair[0] >= pair[1]),
            "test owned-vnode roster must be canonical and in range"
        );
        self.key_group_count = KeyGroupCount::try_from(vnode_count)
            .expect("test vnode count must fit the checkpoint key-group ABI");
        self.test_owned_vnodes = Some(owned_vnodes);
        self.owned_vnodes_cache = None;
    }

    pub(crate) fn capture_state(
        &mut self,
        max_capture_bytes: u64,
    ) -> Result<GraphStateCapture, DbError> {
        self.ensure_execution_not_poisoned()?;
        #[cfg(feature = "cluster")]
        self.ensure_checkpoint_transition_is_applied()?;

        let mut remaining = max_capture_bytes
            .checked_sub(GRAPH_CHECKPOINT_CAPTURE_OVERHEAD)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "operator checkpoint metadata exceeds its staged-state cap of {max_capture_bytes} bytes"
                ))
            })?;

        let vnode_count = u32::from(self.key_group_count);
        #[cfg(feature = "cluster")]
        let owned_vnodes = self.owned_vnodes_for_managed_state()?;
        #[cfg(not(feature = "cluster"))]
        let owned_vnodes = self.local_owned_vnodes_for_managed_state();
        let owned_vnodes = owned_vnodes.as_deref().unwrap_or(&[]);
        let mut capture = GraphStateCapture::default();
        let mut names = std::collections::BTreeSet::new();

        for node in &mut self.nodes {
            if node.removed {
                continue;
            }
            let name = node.name.to_string();
            if !names.insert(name.clone()) {
                return Err(DbError::Checkpoint(format!(
                    "checkpoint capture repeats operator name '{name}'"
                )));
            }

            let entry_charge = GRAPH_CHECKPOINT_ENTRY_OVERHEAD
                .checked_add(u64::try_from(name.len()).unwrap_or(u64::MAX))
                .ok_or_else(|| {
                    DbError::Checkpoint("operator checkpoint metadata overflowed u64".into())
                })?;
            let state_budget = remaining.saturating_sub(entry_charge);
            if let Some(state) = node.operator.checkpoint_capture(state_budget)? {
                let charge = entry_charge
                    .checked_add(state.retained_bytes())
                    .ok_or_else(|| {
                        DbError::Checkpoint("operator checkpoint capture overflowed u64".into())
                    })?;
                remaining = remaining.checked_sub(charge).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "operator '{name}' whole-state capture exceeded the remaining staged-state budget"
                    ))
                })?;
                capture.whole.push(CapturedWholeState {
                    operator_id: name.clone(),
                    state,
                });
            }

            if node.capability.managed_state.is_none() {
                continue;
            }
            remaining = remaining.checked_sub(entry_charge).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "operator '{name}' managed-state inventory exceeded the remaining staged-state budget"
                ))
            })?;
            capture
                .managed_vnode_operators
                .push((name.clone(), node.capability.state_class));
            let required = Self::required_vnodes_for_capability(node.capability, owned_vnodes)?;
            if required.is_empty() {
                continue;
            }
            let states = node
                .operator
                .checkpoint_vnodes(required, vnode_count, remaining)?
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "managed operator '{name}' did not capture its required vnode state"
                    ))
                })?;
            if states.windows(2).any(|pair| pair[0].vnode >= pair[1].vnode)
                || states
                    .iter()
                    .any(|captured| required.binary_search(&captured.vnode).is_err())
            {
                let actual = states.iter().map(|state| state.vnode).collect::<Vec<_>>();
                return Err(DbError::Checkpoint(format!(
                    "managed operator '{name}' captured invalid sparse vnode roster {actual:?}; owned roster is {required:?}"
                )));
            }
            let vnode_entry_charge = entry_charge
                .checked_mul(u64::try_from(states.len()).unwrap_or(u64::MAX))
                .ok_or_else(|| {
                    DbError::Checkpoint("operator vnode capture metadata overflowed u64".into())
                })?;
            remaining = remaining.checked_sub(vnode_entry_charge).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "operator '{name}' vnode metadata exceeded the remaining staged-state budget"
                ))
            })?;
            for captured in &states {
                if let Some(state) = captured.state.as_ref() {
                    remaining = remaining
                        .checked_sub(state.retained_bytes())
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "operator '{name}' vnode {} capture exceeded the remaining staged-state budget",
                                captured.vnode
                            ))
                        })?;
                }
            }
            capture
                .vnodes
                .extend(states.into_iter().map(|state| (name.clone(), state)));
        }

        capture
            .whole
            .sort_unstable_by(|left, right| left.operator_id.cmp(&right.operator_id));
        capture.vnodes.sort_unstable_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.vnode.cmp(&right.1.vnode))
        });
        capture
            .managed_vnode_operators
            .sort_unstable_by(|left, right| left.0.cmp(&right.0));
        capture.retained_bytes = max_capture_bytes - remaining;
        Ok(capture)
    }

    pub(crate) fn force_full_vnode_capture(&mut self) {
        for node in &mut self.nodes {
            if !node.removed {
                node.operator.force_full_vnode_capture();
            }
        }
    }

    /// Restore independently checksummed whole-operator and vnode frames into a newly built graph.
    /// The graph is consumed so a late operator failure drops the partial image.
    pub(crate) fn restore_state_frames(
        mut self,
        whole: &[(String, bytes::Bytes)],
        vnodes: &[(String, u32, bytes::Bytes)],
        vnode_count: u32,
    ) -> Result<(Self, usize), DbError> {
        if !self.whole_restore_open {
            return Err(DbError::Checkpoint(
                "[LDB-6029] operator graph restore is only valid before the first execution cycle"
                    .into(),
            ));
        }
        if vnode_count != u32::from(self.key_group_count) {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6043] checkpoint vnode domain {vnode_count} does not match graph domain {}",
                u32::from(self.key_group_count)
            )));
        }

        let mut whole_names = std::collections::BTreeSet::new();
        for (name, _) in whole {
            if !whole_names.insert(name.as_str()) {
                return Err(DbError::Checkpoint(format!(
                    "checkpoint repeats whole state for operator '{name}'"
                )));
            }
            if !self
                .nodes
                .iter()
                .any(|node| !node.removed && &*node.name == name)
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6029] checkpoint requires missing operator '{name}'"
                )));
            }
        }

        #[cfg(feature = "cluster")]
        let owned_vnodes = self.owned_vnodes_for_managed_state()?;
        #[cfg(not(feature = "cluster"))]
        let owned_vnodes = self.local_owned_vnodes_for_managed_state();
        let owned_vnodes = owned_vnodes.as_deref().unwrap_or(&[]);
        let mut actual_vnodes: FxHashMap<&str, Vec<u32>> = FxHashMap::default();
        for (name, vnode, _) in vnodes {
            let node = self
                .nodes
                .iter()
                .find(|node| !node.removed && &*node.name == name)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "[LDB-6029] checkpoint requires missing operator '{name}'"
                    ))
                })?;
            if node.capability.managed_state.is_none() {
                return Err(DbError::Checkpoint(format!(
                    "checkpoint supplies vnode {vnode} for unmanaged operator '{name}'"
                )));
            }
            actual_vnodes.entry(name).or_default().push(*vnode);
        }
        for vnodes in actual_vnodes.values_mut() {
            vnodes.sort_unstable();
            if vnodes.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(DbError::Checkpoint(
                    "checkpoint repeats a logical operator vnode frame".into(),
                ));
            }
        }
        for node in self.nodes.iter().filter(|node| !node.removed) {
            if node.capability.managed_state.is_none() {
                continue;
            }
            let required = Self::required_vnodes_for_capability(node.capability, owned_vnodes)?;
            let actual = actual_vnodes
                .get(&*node.name)
                .map_or(&[][..], Vec::as_slice);
            if actual != required {
                return Err(DbError::Checkpoint(format!(
                    "managed operator '{}' restore has vnode roster {actual:?}; expected {required:?}",
                    node.name
                )));
            }
        }

        let mut restored = 0;
        for (name, bytes) in whole {
            let node_id = self
                .nodes
                .iter()
                .position(|node| !node.removed && &*node.name == name)
                .expect("whole-frame names were validated");
            let node = &mut self.nodes[node_id];
            node.operator
                .restore(OperatorCheckpoint {
                    data: bytes.to_vec(),
                })
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6029] operator '{}' restore failed: {error}",
                        node.name
                    ))
                })?;
            restored += 1;
        }
        for (name, vnode, bytes) in vnodes {
            let node_id = self
                .nodes
                .iter()
                .position(|node| !node.removed && &*node.name == name)
                .expect("vnode-frame names were validated");
            let node = &mut self.nodes[node_id];
            node.operator
                .restore_vnode(*vnode, vnode_count, bytes)
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6029] operator '{}' vnode {vnode} restore failed: {error}",
                        node.name
                    ))
                })?;
            restored += 1;
        }
        #[cfg(feature = "cluster")]
        for (node_id, node) in self.nodes.iter_mut().enumerate() {
            if !node.removed {
                if let Some(frontier) = node.operator.restored_output_frontier() {
                    self.output_watermarks[node_id] = frontier.watermark_or_min();
                    self.output_idle[node_id] = frontier.idle;
                }
            }
        }
        self.validate_managed_state_budget("whole-graph restore")?;
        self.whole_restore_open = false;
        Ok((self, restored))
    }
}

pub(crate) fn try_evaluate_compiled(
    proj: &crate::aggregate_state::CompiledProjection,
    batches: &[RecordBatch],
) -> Result<Vec<RecordBatch>, crate::error::DbError> {
    let mut result = Vec::with_capacity(batches.len());
    for batch in batches {
        let b = proj.evaluate(batch)?;
        if b.num_rows() > 0 {
            result.push(b);
        }
    }
    Ok(result)
}

#[cfg(test)]
#[allow(clippy::redundant_closure_for_method_calls)]
mod tests;
