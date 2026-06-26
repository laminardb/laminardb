//! Checkpoint recovery: loads the latest manifest and restores sources, sinks,
//! and operator state. Falls back to older checkpoints if the latest is corrupt.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;

use bytes::Bytes;
use laminar_core::state::StateBackend;
use laminar_core::storage::checkpoint_manifest::{CheckpointManifest, SinkCommitStatus};
use laminar_core::storage::checkpoint_store::CheckpointStore;
use laminar_core::storage::ValidationResult;
use tracing::{debug, error, info, warn};

use crate::checkpoint_coordinator::{
    connector_to_source_checkpoint, RegisteredSink, RegisteredSource,
};
use crate::error::DbError;

/// Result of a successful recovery from a checkpoint.
#[derive(Debug)]
pub struct RecoveredState {
    /// Manifest that was restored from.
    pub manifest: CheckpointManifest,
    /// Number of sources successfully restored.
    pub sources_restored: usize,
    /// Number of table sources successfully restored.
    pub tables_restored: usize,
    /// Number of sinks rolled back.
    pub sinks_rolled_back: usize,
    /// Sources that failed to restore (name → error message).
    pub source_errors: HashMap<String, String>,
    /// Sinks that failed to roll back (name → error message).
    pub sink_errors: HashMap<String, String>,
}

impl RecoveredState {
    /// Returns the recovered epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.manifest.epoch
    }

    /// Returns the recovered watermark.
    #[must_use]
    pub fn watermark(&self) -> Option<i64> {
        self.manifest.watermark
    }

    /// Returns `true` if any source or sink failed during recovery.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.source_errors.is_empty() || !self.sink_errors.is_empty()
    }

    /// Returns the recovered operator states.
    #[must_use]
    pub fn operator_states(
        &self,
    ) -> &HashMap<String, laminar_core::storage::checkpoint_manifest::OperatorCheckpoint> {
        &self.manifest.operator_states
    }

    /// Table store checkpoint path, if recorded in the manifest.
    #[must_use]
    pub fn table_store_checkpoint_path(&self) -> Option<&str> {
        self.manifest.table_store_checkpoint_path.as_deref()
    }
}

/// Outcome of rehydrating a set of vnodes from durable state.
///
/// Per-vnode read failures land in `errors` rather than aborting the rest, so
/// a transient object-store hiccup can't strand an entire rebalance.
#[derive(Debug, Default)]
pub struct VnodeRehydration {
    /// Committed epoch the partials were read from. `None` when the backend
    /// has no committed epoch — every vnode starts fresh.
    pub epoch: Option<u64>,
    /// vnode → recovery chain (decoded-as-bytes partials, oldest→newest): a FULL base followed by
    /// any delta partials. A simple/reference vnode resolves to a single-element chain.
    pub restored: HashMap<u32, Vec<Bytes>>,
    /// Vnodes with no durable partial at `epoch`; resume from empty state.
    pub missing: Vec<u32>,
    /// vnode → error for reads that failed.
    pub errors: HashMap<u32, String>,
}

impl VnodeRehydration {
    /// Number of vnodes successfully read back.
    #[must_use]
    pub fn restored_count(&self) -> usize {
        self.restored.len()
    }

    /// Returns `true` if any per-vnode read failed.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

/// Reads the latest committed `partial.bin` for each requested vnode so a
/// newly-acquired node resumes from the last committed epoch rather than
/// empty state. Applying the bytes is the caller's responsibility.
pub struct VnodeRehydrator<'a> {
    backend: &'a dyn StateBackend,
}

impl<'a> VnodeRehydrator<'a> {
    /// Create a rehydrator over `backend`.
    #[must_use]
    pub fn new(backend: &'a dyn StateBackend) -> Self {
        Self { backend }
    }

    /// Read the latest committed partial for each vnode in `vnodes`.
    ///
    /// Resolves the highest committed epoch once, then reads each vnode's
    /// `partial.bin` at that epoch. A per-vnode failure is recorded in
    /// [`VnodeRehydration::errors`] without aborting the rest. Returns an
    /// empty report when the store has no committed epoch.
    pub async fn rehydrate(&self, vnodes: &[u32]) -> VnodeRehydration {
        let mut report = VnodeRehydration::default();
        if vnodes.is_empty() {
            return report;
        }

        let epoch = match self.backend.latest_committed_epoch().await {
            Ok(Some(epoch)) => epoch,
            Ok(None) => {
                debug!(
                    vnodes = ?vnodes,
                    "rehydrate: no committed epoch on backend — vnodes start fresh"
                );
                report.missing = vnodes.to_vec();
                return report;
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "[LDB-6050] rehydrate: latest_committed_epoch failed — \
                     vnodes start fresh"
                );
                report.missing = vnodes.to_vec();
                return report;
            }
        };
        report.epoch = Some(epoch);

        for &vnode in vnodes {
            match self.collect_chain(vnode, epoch).await {
                Ok(Some(chain)) => {
                    debug!(vnode, epoch, links = chain.len(), "rehydrated vnode chain");
                    report.restored.insert(vnode, chain);
                }
                Ok(None) => {
                    debug!(
                        vnode,
                        epoch, "rehydrate: no partial for vnode at committed epoch"
                    );
                    report.missing.push(vnode);
                }
                Err(e) => {
                    warn!(
                        vnode, epoch, error = %e,
                        "[LDB-6051] rehydrate: chain read failed — vnode starts fresh"
                    );
                    report.errors.insert(vnode, e);
                }
            }
        }

        info!(
            epoch,
            restored = report.restored.len(),
            missing = report.missing.len(),
            errors = report.errors.len(),
            "vnode rehydration complete"
        );
        report
    }

    /// Resolve a vnode's recovery chain at `epoch`: collapse leading Lever-1 reference hops, then
    /// walk `base_epoch` back collecting delta partials until every delta operator at the head has
    /// its FULL base (or the chain ends / a link is missing). Returns oldest→newest decoded bytes.
    async fn collect_chain(&self, vnode: u32, epoch: u64) -> Result<Option<Vec<Bytes>>, String> {
        use crate::vnode_partial::VnodePartial;
        let Some(mut bytes) = self
            .backend
            .read_partial(vnode, epoch)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok(None);
        };

        // Follow reference hops (no data, just a base pointer) to the real partial.
        let mut cur_epoch = epoch;
        loop {
            let Ok(p) = VnodePartial::decode(&bytes) else {
                // Undecodable → pass through; the apply path skips it (prior behavior).
                return Ok(Some(vec![bytes]));
            };
            if p.operators.is_empty() && p.deltas.is_empty() {
                if let Some(base) = p.base_epoch {
                    // base_epoch always points to an older epoch; a non-decreasing pointer
                    // is corruption — bail rather than loop forever.
                    if base >= cur_epoch {
                        return Ok(None);
                    }
                    match self
                        .backend
                        .read_partial(vnode, base)
                        .await
                        .map_err(|e| e.to_string())?
                    {
                        Some(b) => {
                            bytes = b;
                            cur_epoch = base;
                            continue;
                        }
                        None => return Ok(None), // reference base missing → start fresh
                    }
                }
            }
            break;
        }

        // `bytes` is a FULL or DELTA partial. Walk back until each delta operator has its FULL.
        let head = VnodePartial::decode(&bytes).map_err(|e| e.to_string())?;
        let mut need: std::collections::HashSet<String> = head
            .deltas
            .iter()
            .map(|(n, _)| n.clone())
            .filter(|n| !head.operators.iter().any(|(on, _)| on == n))
            .collect();
        let mut chain_rev: Vec<Bytes> = vec![bytes];
        let mut cur = head;
        while !need.is_empty() {
            let Some(parent) = cur.base_epoch else { break };
            if parent >= cur_epoch {
                break; // non-decreasing base → corruption; stop (unresolved ops start fresh)
            }
            let Some(pbytes) = self
                .backend
                .read_partial(vnode, parent)
                .await
                .map_err(|e| e.to_string())?
            else {
                break; // missing link → unresolved operators start fresh (conservative)
            };
            let Ok(pp) = VnodePartial::decode(&pbytes) else {
                break;
            };
            for (n, _) in &pp.operators {
                need.remove(n);
            }
            chain_rev.push(pbytes);
            cur_epoch = parent;
            cur = pp;
        }
        chain_rev.reverse();
        Ok(Some(chain_rev))
    }
}

/// One operator's resolved recovery chain: FULL base bytes + ordered `(changed, tombstones)` deltas.
#[cfg(feature = "cluster")]
pub(crate) type ResolvedOpChain<'a> = (&'a [u8], Vec<(&'a [u8], &'a [u8])>);

/// From a vnode's recovery chain (oldest→newest decoded partials), resolve one operator's FULL base
/// bytes + ordered delta payloads. Returns `None` when no FULL for `op` is present (start fresh).
#[cfg(feature = "cluster")]
#[must_use]
pub(crate) fn resolve_op_chain<'a>(
    chain: &'a [crate::vnode_partial::VnodePartial],
    op: &str,
) -> Option<ResolvedOpChain<'a>> {
    let base_idx = chain
        .iter()
        .rposition(|p| p.operators.iter().any(|(n, _)| n == op))?;
    let base = chain[base_idx]
        .operators
        .iter()
        .find(|(n, _)| n == op)
        .map(|(_, b)| b.as_slice())?;
    let mut deltas: Vec<(&[u8], &[u8])> = Vec::new();
    for p in &chain[base_idx + 1..] {
        if let Some((_, d)) = p.deltas.iter().find(|(n, _)| n == op) {
            deltas.push((d.changed.as_slice(), d.tombstones_ipc.as_slice()));
        }
    }
    Some((base, deltas))
}

/// Loads the latest [`CheckpointManifest`] and restores sources, sinks, and
/// tables. In strict mode any restore failure aborts and triggers fallback; in
/// lenient mode failures are recorded and recovery continues.
pub struct RecoveryManager<'a> {
    store: &'a dyn CheckpointStore,
    strict: bool,
}

impl<'a> RecoveryManager<'a> {
    /// Create a strict recovery manager (any restore failure triggers fallback).
    ///
    /// Use [`Self::lenient`] to allow partial recovery.
    #[must_use]
    pub fn new(store: &'a dyn CheckpointStore) -> Self {
        Self {
            store,
            strict: true,
        }
    }

    /// Create a lenient recovery manager.
    ///
    /// Restore failures are recorded in `RecoveredState` but do not abort
    /// recovery; the pipeline resumes with potentially mismatched offsets.
    #[must_use]
    pub fn lenient(store: &'a dyn CheckpointStore) -> Self {
        Self {
            store,
            strict: false,
        }
    }

    /// Recover from the latest checkpoint, falling back to older ones on failure.
    ///
    /// Returns `Ok(None)` for a fresh start (no checkpoint found).
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if the store fails or strict-mode
    /// restore errors exhaust all available checkpoints.
    pub(crate) async fn recover(
        &self,
        sources: &[RegisteredSource],
        sinks: &[RegisteredSink],
        table_sources: &[RegisteredSource],
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<Option<RecoveredState>, DbError> {
        // Fast path: the latest checkpoint.
        match self.store.load_latest().await {
            Ok(Some(manifest)) => {
                if let Some(state) = self
                    .try_restore(manifest, sources, sinks, table_sources, decision_store)
                    .await
                {
                    return Ok(Some(state));
                }
            }
            Ok(None) => {
                info!("no checkpoint found, starting fresh");
                return Ok(None);
            }
            Err(e) => warn!(error = %e, "latest checkpoint load failed, trying fallback"),
        }

        // Fallback: older checkpoints, newest first.
        let mut checkpoints = self.store.list().await.map_err(DbError::from)?;
        checkpoints.reverse();
        self.restore_first(&checkpoints, sources, sinks, table_sources, decision_store)
            .await
    }

    /// Recover from the newest viable checkpoint with `epoch <= target_epoch` — the
    /// coordinated-restart target, which may be older than this node's local latest.
    ///
    /// # Errors
    /// Returns `DbError::Checkpoint` if the store fails.
    pub(crate) async fn recover_to_epoch(
        &self,
        target_epoch: u64,
        sources: &[RegisteredSource],
        sinks: &[RegisteredSink],
        table_sources: &[RegisteredSource],
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<Option<RecoveredState>, DbError> {
        let mut checkpoints = self.store.list().await.map_err(DbError::from)?;
        checkpoints.retain(|&(_, epoch)| epoch <= target_epoch);
        checkpoints.sort_by_key(|&(_, epoch)| std::cmp::Reverse(epoch)); // newest eligible first
        self.restore_first(&checkpoints, sources, sinks, table_sources, decision_store)
            .await
    }

    /// Resolve external operator states from the sidecar file into inline entries.
    ///
    /// Returns `true` if all resolved successfully; `false` on missing/truncated
    /// sidecar. In strict mode the caller treats `false` as corruption.
    async fn resolve_external_states(&self, manifest: &mut CheckpointManifest) -> bool {
        let external_ops: Vec<String> = manifest
            .operator_states
            .iter()
            .filter(|(_, op)| op.external)
            .map(|(name, _)| name.clone())
            .collect();

        if external_ops.is_empty() {
            return true;
        }

        let state_data = match self.store.load_state_data(manifest.checkpoint_id).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                error!(
                    checkpoint_id = manifest.checkpoint_id,
                    operators = ?external_ops,
                    "[LDB-6010] sidecar state.bin missing — external operator states \
                     cannot be resolved; operators will start with empty state"
                );
                // Clear external flag so recovery doesn't attempt to
                // dereference invalid offsets later
                for name in &external_ops {
                    if let Some(op) = manifest.operator_states.get_mut(name) {
                        *op =
                            laminar_core::storage::checkpoint_manifest::OperatorCheckpoint::inline(
                                &[],
                            );
                    }
                }
                return false;
            }
            Err(e) => {
                error!(
                    checkpoint_id = manifest.checkpoint_id,
                    error = %e,
                    "[LDB-6010] failed to load sidecar state.bin"
                );
                for name in &external_ops {
                    if let Some(op) = manifest.operator_states.get_mut(name) {
                        *op =
                            laminar_core::storage::checkpoint_manifest::OperatorCheckpoint::inline(
                                &[],
                            );
                    }
                }
                return false;
            }
        };

        let mut all_resolved = true;
        for (name, op) in &mut manifest.operator_states {
            if op.external {
                // Checked arithmetic: a corrupt manifest can't overflow the length check.
                let range = match (
                    usize::try_from(op.external_offset),
                    usize::try_from(op.external_length),
                ) {
                    (Ok(start), Ok(len)) => start.checked_add(len).map(|end| (start, end)),
                    _ => None,
                }
                .filter(|&(_, end)| end <= state_data.len());
                if let Some((start, end)) = range {
                    let external_offset = op.external_offset;
                    let external_length = op.external_length;
                    let data = &state_data[start..end];
                    *op = laminar_core::storage::checkpoint_manifest::OperatorCheckpoint::inline(
                        data,
                    );
                    debug!(
                        operator = %name,
                        offset = external_offset,
                        length = external_length,
                        "resolved external operator state from sidecar"
                    );
                } else {
                    error!(
                        operator = %name,
                        offset = op.external_offset,
                        length = op.external_length,
                        sidecar_len = state_data.len(),
                        "[LDB-6010] sidecar too small or offset/length out of \
                         range for external operator state — operator will \
                         start with empty state"
                    );
                    *op =
                        laminar_core::storage::checkpoint_manifest::OperatorCheckpoint::inline(&[]);
                    all_resolved = false;
                }
            }
        }
        all_resolved
    }

    /// Inner restore logic shared by the fast path and the fallback loop.
    async fn restore_from(
        &self,
        mut manifest: CheckpointManifest,
        sources: &[RegisteredSource],
        sinks: &[RegisteredSink],
        table_sources: &[RegisteredSource],
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> RecoveredState {
        // In strict mode an unresolved sidecar is recorded so check_strict() rejects this checkpoint.
        let sidecar_ok = self.resolve_external_states(&mut manifest).await;
        if !sidecar_ok && self.strict {
            warn!(
                checkpoint_id = manifest.checkpoint_id,
                "[LDB-6010] sidecar resolution failed in strict mode — \
                 checkpoint will be rejected"
            );
        }

        // DEFAULT_VNODE_COUNT is a placeholder; the runtime registry isn't threaded here yet.
        let validation_errors =
            manifest.validate(laminar_core::storage::checkpoint_manifest::DEFAULT_VNODE_COUNT);
        if !validation_errors.is_empty() {
            for err in &validation_errors {
                warn!(
                    checkpoint_id = manifest.checkpoint_id,
                    error = %err,
                    "manifest validation warning"
                );
            }
        }

        Self::warn_topology_changes(&manifest, sources, sinks);

        info!(
            checkpoint_id = manifest.checkpoint_id,
            epoch = manifest.epoch,
            validation_warnings = validation_errors.len(),
            "recovering from checkpoint"
        );

        let mut result = RecoveredState {
            manifest: manifest.clone(),
            sources_restored: 0,
            tables_restored: 0,
            sinks_rolled_back: 0,
            source_errors: HashMap::new(),
            sink_errors: HashMap::new(),
        };

        if !sidecar_ok {
            result.source_errors.insert(
                "__sidecar__".into(),
                "[LDB-6010] sidecar state.bin missing or truncated — \
                 operator state cannot be fully restored"
                    .into(),
            );
        }

        Self::restore_replayable_sources(sources, &manifest, &mut result).await;

        for table_source in table_sources {
            if let Some(cp) = manifest.table_offsets.get(&table_source.name) {
                let source_cp = connector_to_source_checkpoint(cp);
                let mut connector = table_source.connector.lock().await;
                match connector.restore(&source_cp).await {
                    Ok(()) => {
                        result.tables_restored += 1;
                        debug!(table = %table_source.name, epoch = cp.epoch, "table source restored");
                    }
                    Err(e) => {
                        let msg = format!("table source restore failed: {e}");
                        warn!(table = %table_source.name, error = %e, "table source restore failed");
                        result.source_errors.insert(table_source.name.clone(), msg);
                    }
                }
            }
        }

        // Roll back exactly-once sinks that did not commit. Committed sinks are left alone.
        Self::rollback_uncommitted_sinks(sinks, &manifest, decision_store, &mut result).await;

        info!(
            checkpoint_id = manifest.checkpoint_id,
            epoch = manifest.epoch,
            sources_restored = result.sources_restored,
            tables_restored = result.tables_restored,
            sinks_rolled_back = result.sinks_rolled_back,
            errors = result.source_errors.len() + result.sink_errors.len(),
            "recovery complete"
        );

        result
    }

    fn warn_topology_changes(
        manifest: &CheckpointManifest,
        sources: &[RegisteredSource],
        sinks: &[RegisteredSink],
    ) {
        if !manifest.source_names.is_empty() {
            let mut current_sources: Vec<&str> = sources.iter().map(|s| s.name.as_str()).collect();
            current_sources.sort_unstable();
            let checkpoint_sources: Vec<&str> =
                manifest.source_names.iter().map(String::as_str).collect();
            let added: Vec<&&str> = current_sources
                .iter()
                .filter(|n| !checkpoint_sources.contains(n))
                .collect();
            let removed: Vec<&&str> = checkpoint_sources
                .iter()
                .filter(|n| !current_sources.contains(n))
                .collect();
            if !added.is_empty() {
                warn!(sources = ?added, "new sources added since checkpoint — no saved offsets");
            }
            if !removed.is_empty() {
                warn!(sources = ?removed, "sources removed since checkpoint — orphaned offsets");
            }
        }
        if !manifest.sink_names.is_empty() {
            let mut current_sinks: Vec<&str> = sinks.iter().map(|s| s.name.as_str()).collect();
            current_sinks.sort_unstable();
            let checkpoint_sinks: Vec<&str> =
                manifest.sink_names.iter().map(String::as_str).collect();
            let added: Vec<&&str> = current_sinks
                .iter()
                .filter(|n| !checkpoint_sinks.contains(n))
                .collect();
            let removed: Vec<&&str> = checkpoint_sinks
                .iter()
                .filter(|n| !current_sinks.contains(n))
                .collect();
            if !added.is_empty() {
                warn!(sinks = ?added, "new sinks added since checkpoint — no saved epoch");
            }
            if !removed.is_empty() {
                warn!(sinks = ?removed, "sinks removed since checkpoint — orphaned epochs");
            }
        }
    }

    async fn restore_replayable_sources(
        sources: &[RegisteredSource],
        manifest: &CheckpointManifest,
        result: &mut RecoveredState,
    ) {
        for source in sources {
            if !source.supports_replay {
                info!(
                    source = %source.name,
                    "skipping restore for non-replayable source (at-most-once)"
                );
                continue;
            }
            let Some(cp) = manifest.source_offsets.get(&source.name) else {
                continue;
            };
            let source_cp = connector_to_source_checkpoint(cp);
            let mut last_err = None;
            for attempt in 0..3u32 {
                let mut connector = source.connector.lock().await;
                match connector.restore(&source_cp).await {
                    Ok(()) => {
                        last_err = None;
                        break;
                    }
                    Err(e) => {
                        warn!(
                            source = %source.name, attempt,
                            error = %e, "source restore failed, retrying"
                        );
                        last_err = Some(e);
                        drop(connector);
                        if attempt < 2 {
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        }
                    }
                }
            }
            if let Some(e) = last_err {
                let msg = format!("source restore failed after 3 attempts: {e}");
                result.source_errors.insert(source.name.clone(), msg);
            } else {
                result.sources_restored += 1;
                debug!(source = %source.name, epoch = cp.epoch, "source restored");
            }
        }
    }

    async fn rollback_uncommitted_sinks(
        sinks: &[RegisteredSink],
        manifest: &CheckpointManifest,
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
        result: &mut RecoveredState,
    ) {
        // A marker-committed epoch committed its sinks (the manifest just wasn't updated
        // before the fault); rolling back here would abort a committed transaction.
        if let Some(ds) = decision_store {
            match ds.is_committed(manifest.epoch).await {
                Ok(true) => return,
                Ok(false) => {}
                Err(e) => warn!(epoch = manifest.epoch, error = %e,
                    "[LDB-6040] decision-store read failed; rolling back pending sinks"),
            }
        }
        for sink in sinks {
            if !sink.exactly_once {
                continue;
            }
            let already_committed = manifest
                .sink_commit_statuses
                .get(&sink.name)
                .is_some_and(|s| matches!(s, SinkCommitStatus::Committed));
            if already_committed {
                debug!(
                    sink = %sink.name,
                    epoch = manifest.epoch,
                    "sink already committed, skipping rollback"
                );
                continue;
            }
            match sink.handle.rollback_epoch(manifest.epoch).await {
                Ok(()) => {
                    result.sinks_rolled_back += 1;
                    debug!(sink = %sink.name, epoch = manifest.epoch, "sink rolled back");
                }
                Err(e) => {
                    result
                        .sink_errors
                        .insert(sink.name.clone(), format!("rollback failed: {e}"));
                    warn!(
                        sink = %sink.name,
                        epoch = manifest.epoch,
                        error = %e,
                        "[LDB-6016] sink rollback failed during recovery"
                    );
                }
            }
        }
    }

    /// Returns `true` if the checkpoint fails integrity validation.
    ///
    /// I/O errors during validation are treated as corruption — if the sidecar
    /// can't be verified, don't trust the checkpoint. Returns `false` when there
    /// is nothing to validate (no sidecar, no state checksum).
    async fn is_checkpoint_corrupt(&self, manifest: &CheckpointManifest) -> bool {
        if manifest.state_checksum.is_none() && manifest.operator_states.is_empty() {
            return false;
        }
        match self.store.validate_checkpoint(manifest.checkpoint_id).await {
            Ok(ValidationResult {
                valid: false,
                ref issues,
                ..
            }) => {
                error!(
                    checkpoint_id = manifest.checkpoint_id,
                    issues = ?issues,
                    "[LDB-6010] checkpoint integrity check failed"
                );
                true
            }
            Ok(_) => false, // valid
            Err(e) => {
                error!(
                    checkpoint_id = manifest.checkpoint_id,
                    error = %e,
                    "[LDB-6010] checkpoint validation I/O error — \
                     treating as corrupt for safety"
                );
                true
            }
        }
    }

    /// Returns `true` if any exactly-once sink has `Pending` commit status.
    ///
    /// A Pending sink means the manifest was persisted before commit finished;
    /// recovering from it advances source offsets past data the sink never wrote.
    fn has_pending_sinks(manifest: &CheckpointManifest) -> bool {
        manifest
            .sink_commit_statuses
            .values()
            .any(|s| matches!(s, SinkCommitStatus::Pending))
    }

    /// `true` when a checkpoint must be skipped for genuinely-uncommitted sinks: sinks are
    /// `Pending` AND the decision marker does not confirm the epoch committed. A
    /// pending-but-marker-committed epoch is NOT skipped — the sink committed but the
    /// manifest wasn't updated before the fault, and rewinding the source behind it would
    /// re-emit committed rows (duplicates); `reconcile_prepared_on_init` re-drives the commit.
    async fn pending_uncommitted(
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
        manifest: &CheckpointManifest,
    ) -> bool {
        if !Self::has_pending_sinks(manifest) {
            return false;
        }
        match decision_store {
            // A read error can't confirm the commit, so treat as uncommitted (skip) — but
            // surface it rather than silently rewinding past a maybe-committed checkpoint.
            Some(ds) => match ds.is_committed(manifest.epoch).await {
                Ok(committed) => !committed,
                Err(e) => {
                    warn!(epoch = manifest.epoch, error = %e,
                        "[LDB-6040] decision-store read failed; treating epoch as uncommitted");
                    true
                }
            },
            None => true,
        }
    }

    /// Restore from `manifest` if it's viable; `None` means try an older checkpoint
    /// (corrupt, genuinely-uncommitted sinks, or strict-mode restore errors).
    async fn try_restore(
        &self,
        manifest: CheckpointManifest,
        sources: &[RegisteredSource],
        sinks: &[RegisteredSink],
        table_sources: &[RegisteredSource],
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Option<RecoveredState> {
        let (checkpoint_id, epoch) = (manifest.checkpoint_id, manifest.epoch);
        if self.is_checkpoint_corrupt(&manifest).await {
            warn!(
                checkpoint_id,
                epoch, "[LDB-6010] checkpoint corrupt, trying older"
            );
            return None;
        }
        if Self::pending_uncommitted(decision_store, &manifest).await {
            warn!(
                checkpoint_id,
                epoch, "[LDB-6015] uncommitted sinks, trying older"
            );
            return None;
        }
        let state = self
            .restore_from(manifest, sources, sinks, table_sources, decision_store)
            .await;
        if let Err(e) = self.check_strict(&state) {
            warn!(checkpoint_id, epoch, error = %e, "strict restore errors, trying older");
            return None;
        }
        Some(state)
    }

    /// Restore from the first viable checkpoint in `candidates` (try-order); `Ok(None)`
    /// if none restore. `candidates` are `(checkpoint_id, epoch)` pairs.
    async fn restore_first(
        &self,
        candidates: &[(u64, u64)],
        sources: &[RegisteredSource],
        sinks: &[RegisteredSink],
        table_sources: &[RegisteredSource],
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<Option<RecoveredState>, DbError> {
        for &(checkpoint_id, _) in candidates {
            match self.store.load_by_id(checkpoint_id).await {
                Ok(Some(manifest)) => {
                    if let Some(state) = self
                        .try_restore(manifest, sources, sinks, table_sources, decision_store)
                        .await
                    {
                        return Ok(Some(state));
                    }
                }
                Ok(None) => {}
                Err(e) => warn!(checkpoint_id, error = %e, "checkpoint load failed, trying older"),
            }
        }
        Ok(None)
    }

    fn check_strict(&self, state: &RecoveredState) -> Result<(), DbError> {
        if !self.strict || !state.has_errors() {
            return Ok(());
        }
        let mut msgs: Vec<String> = state
            .source_errors
            .iter()
            .map(|(k, v)| format!("source '{k}': {v}"))
            .collect();
        for (k, v) in &state.sink_errors {
            msgs.push(format!("sink '{k}': {v}"));
        }
        Err(DbError::Checkpoint(format!(
            "strict recovery failed — {} restore error(s): {}",
            msgs.len(),
            msgs.join("; ")
        )))
    }

    /// Load the latest manifest without restoring state.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if the store fails.
    pub async fn load_latest(&self) -> Result<Option<CheckpointManifest>, DbError> {
        self.store.load_latest().await.map_err(DbError::from)
    }

    /// Load a checkpoint by ID.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if the store fails.
    pub async fn load_by_id(
        &self,
        checkpoint_id: u64,
    ) -> Result<Option<CheckpointManifest>, DbError> {
        self.store
            .load_by_id(checkpoint_id)
            .await
            .map_err(DbError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_core::storage::checkpoint_manifest::OperatorCheckpoint;
    use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;

    fn make_store(dir: &std::path::Path) -> FileSystemCheckpointStore {
        FileSystemCheckpointStore::new(dir, 3)
    }

    #[tokio::test]
    async fn test_recover_no_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let mgr = RecoveryManager::new(&store);

        let result = mgr.recover(&[], &[], &[], None).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_recover_empty_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Save a basic checkpoint
        let manifest = CheckpointManifest::new(1, 5);
        store.save(&manifest).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[], None).await.unwrap().unwrap();

        assert_eq!(result.epoch(), 5);
        assert_eq!(result.sources_restored, 0);
        assert_eq!(result.tables_restored, 0);
        assert_eq!(result.sinks_rolled_back, 0);
        assert!(!result.has_errors());
    }

    #[tokio::test]
    async fn recover_to_epoch_picks_newest_at_or_below_target() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        for (id, epoch) in [(1u64, 3u64), (2, 5), (3, 7)] {
            store
                .save(&CheckpointManifest::new(id, epoch))
                .await
                .unwrap();
        }
        let mgr = RecoveryManager::new(&store);

        assert_eq!(
            mgr.recover_to_epoch(7, &[], &[], &[], None)
                .await
                .unwrap()
                .unwrap()
                .epoch(),
            7
        );
        // A newer local epoch is rewound to the cluster-agreed target.
        assert_eq!(
            mgr.recover_to_epoch(6, &[], &[], &[], None)
                .await
                .unwrap()
                .unwrap()
                .epoch(),
            5
        );
        assert_eq!(
            mgr.recover_to_epoch(5, &[], &[], &[], None)
                .await
                .unwrap()
                .unwrap()
                .epoch(),
            5
        );
        // Nothing committed at or below the target → fresh start.
        assert!(mgr
            .recover_to_epoch(2, &[], &[], &[], None)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_recover_with_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut manifest = CheckpointManifest::new(1, 3);
        manifest.watermark = Some(42_000);
        store.save(&manifest).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[], None).await.unwrap().unwrap();

        assert_eq!(result.watermark(), Some(42_000));
    }

    #[tokio::test]
    async fn test_recover_with_operator_states() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut manifest = CheckpointManifest::new(1, 7);
        manifest
            .operator_states
            .insert("0".to_string(), OperatorCheckpoint::inline(b"window-state"));
        manifest
            .operator_states
            .insert("3".to_string(), OperatorCheckpoint::inline(b"filter-state"));
        store.save(&manifest).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[], None).await.unwrap().unwrap();

        assert_eq!(result.operator_states().len(), 2);
        let op0 = result.operator_states().get("0").unwrap();
        assert_eq!(op0.decode_inline().unwrap(), b"window-state");
    }

    #[tokio::test]
    async fn test_recover_table_store_path() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut manifest = CheckpointManifest::new(1, 1);
        manifest.table_store_checkpoint_path = Some("/data/table_store_cp_001".into());
        store.save(&manifest).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[], None).await.unwrap().unwrap();

        assert_eq!(
            result.table_store_checkpoint_path(),
            Some("/data/table_store_cp_001")
        );
    }

    #[tokio::test]
    async fn test_load_latest_no_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let mgr = RecoveryManager::new(&store);

        assert!(mgr.load_latest().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_load_by_id() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        store.save(&CheckpointManifest::new(1, 1)).await.unwrap();
        store.save(&CheckpointManifest::new(2, 2)).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let m = mgr.load_by_id(1).await.unwrap().unwrap();
        assert_eq!(m.checkpoint_id, 1);

        let m2 = mgr.load_by_id(2).await.unwrap().unwrap();
        assert_eq!(m2.checkpoint_id, 2);

        assert!(mgr.load_by_id(999).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_recover_fallback_to_previous_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Save two valid checkpoints
        let mut m1 = CheckpointManifest::new(1, 10);
        m1.watermark = Some(1000);
        store.save(&m1).await.unwrap();

        let mut m2 = CheckpointManifest::new(2, 20);
        m2.watermark = Some(2000);
        store.save(&m2).await.unwrap();

        // Corrupt the latest checkpoint by writing invalid JSON
        let latest_manifest_path = dir
            .path()
            .join("checkpoints")
            .join("checkpoint_000002")
            .join("manifest.json");
        std::fs::write(&latest_manifest_path, "not valid json!!!").unwrap();

        // Also corrupt latest.txt to point to the corrupt checkpoint
        // (it already does from the save, but the manifest file is now corrupt)

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[], None).await.unwrap();

        // Should fall back to checkpoint 1
        let recovered = result.expect("should recover from fallback checkpoint");
        assert_eq!(recovered.manifest.checkpoint_id, 1);
        assert_eq!(recovered.epoch(), 10);
        assert_eq!(recovered.watermark(), Some(1000));
    }

    #[tokio::test]
    async fn test_recover_all_checkpoints_corrupt_starts_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Save a checkpoint then corrupt it
        store.save(&CheckpointManifest::new(1, 5)).await.unwrap();

        let manifest_path = dir
            .path()
            .join("checkpoints")
            .join("checkpoint_000001")
            .join("manifest.json");
        std::fs::write(&manifest_path, "corrupt").unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[], None).await.unwrap();

        // All checkpoints corrupt → fresh start
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_recover_latest_ok_no_fallback_needed() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        store.save(&CheckpointManifest::new(1, 10)).await.unwrap();
        store.save(&CheckpointManifest::new(2, 20)).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[], None).await.unwrap().unwrap();

        // Should use the latest (no fallback needed)
        assert_eq!(result.manifest.checkpoint_id, 2);
        assert_eq!(result.epoch(), 20);
    }

    #[tokio::test]
    async fn test_recover_with_sidecar_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Build a manifest with an external operator state
        let mut manifest = CheckpointManifest::new(1, 5);
        let large_data = vec![0xAB; 2048];
        manifest
            .operator_states
            .insert("big-op".into(), OperatorCheckpoint::external(0, 2048));

        // Write sidecar first, then manifest
        store
            .save_state_data(1, &[bytes::Bytes::copy_from_slice(&large_data)])
            .await
            .unwrap();
        store.save(&manifest).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[], None).await.unwrap().unwrap();

        // External state should have been resolved to inline
        let op = result.operator_states().get("big-op").unwrap();
        assert!(!op.external, "external state should be resolved to inline");
        assert_eq!(op.decode_inline().unwrap(), large_data);
    }

    #[tokio::test]
    async fn test_recover_mixed_inline_and_external() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut manifest = CheckpointManifest::new(1, 3);
        // Small inline state
        manifest
            .operator_states
            .insert("small-op".into(), OperatorCheckpoint::inline(b"tiny"));
        // Large external state
        let large_data = vec![0xCD; 4096];
        manifest
            .operator_states
            .insert("big-op".into(), OperatorCheckpoint::external(0, 4096));

        store
            .save_state_data(1, &[bytes::Bytes::copy_from_slice(&large_data)])
            .await
            .unwrap();
        store.save(&manifest).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[], None).await.unwrap().unwrap();

        let small = result.operator_states().get("small-op").unwrap();
        assert_eq!(small.decode_inline().unwrap(), b"tiny");

        let big = result.operator_states().get("big-op").unwrap();
        assert_eq!(big.decode_inline().unwrap(), large_data);
    }

    #[tokio::test]
    async fn test_recover_missing_sidecar_graceful() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Manifest references external state but sidecar is missing
        let mut manifest = CheckpointManifest::new(1, 1);
        manifest
            .operator_states
            .insert("orphan".into(), OperatorCheckpoint::external(0, 100));
        store.save(&manifest).await.unwrap();

        // Use lenient mode — graceful degradation replaces missing
        // sidecar state with empty inline. Strict mode rejects this
        // checkpoint entirely (see test_recover_missing_sidecar_strict).
        let mgr = RecoveryManager::lenient(&store);
        let result = mgr.recover(&[], &[], &[], None).await.unwrap().unwrap();

        // Should still recover (gracefully) — external state replaced with
        // empty inline to avoid dangling offset references
        let op = result.operator_states().get("orphan").unwrap();
        assert!(
            !op.external,
            "unresolved external state replaced with inline empty"
        );
        assert!(
            op.state_b64.as_ref().is_none_or(String::is_empty),
            "replaced state should be empty"
        );
    }

    #[tokio::test]
    async fn test_recovered_state_has_errors() {
        let state = RecoveredState {
            manifest: CheckpointManifest::new(1, 1),
            sources_restored: 0,
            tables_restored: 0,
            sinks_rolled_back: 0,
            source_errors: HashMap::new(),
            sink_errors: HashMap::new(),
        };
        assert!(!state.has_errors());

        let state_with_errors = RecoveredState {
            manifest: CheckpointManifest::new(1, 1),
            sources_restored: 0,
            tables_restored: 0,
            sinks_rolled_back: 0,
            source_errors: HashMap::from([("source1".into(), "failed".into())]),
            sink_errors: HashMap::new(),
        };
        assert!(state_with_errors.has_errors());
    }

    #[tokio::test]
    async fn test_recover_missing_sidecar_strict_rejects() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Manifest references external state but sidecar is missing
        let mut manifest = CheckpointManifest::new(1, 1);
        manifest
            .operator_states
            .insert("orphan".into(), OperatorCheckpoint::external(0, 100));
        store.save(&manifest).await.unwrap();

        // Strict mode: missing sidecar causes the checkpoint to be rejected
        // and recovery falls back. With only one checkpoint, this means
        // fresh start.
        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[], None).await.unwrap();
        assert!(
            result.is_none(),
            "strict mode should reject checkpoint with missing sidecar"
        );
    }

    #[tokio::test]
    async fn test_recover_skips_pending_sinks_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Epoch 1: fully committed checkpoint (good).
        let mut m1 = CheckpointManifest::new(1, 1);
        m1.sink_commit_statuses
            .insert("delta_sink".into(), SinkCommitStatus::Committed);
        store.save(&m1).await.unwrap();

        // Epoch 2: crashed between manifest persist and sink commit (Pending).
        let mut m2 = CheckpointManifest::new(2, 2);
        m2.sink_commit_statuses
            .insert("delta_sink".into(), SinkCommitStatus::Pending);
        store.save(&m2).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[], None).await.unwrap();
        let state = result.expect("should recover from epoch 1 fallback");

        // Must fall back to epoch 1 (the last fully committed checkpoint),
        // not epoch 2 (which has uncommitted sink data).
        assert_eq!(
            state.epoch(),
            1,
            "recovery must skip checkpoint with Pending sinks"
        );
    }

    #[tokio::test]
    async fn test_recover_all_pending_starts_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Only checkpoint has Pending sinks — no safe fallback.
        let mut m = CheckpointManifest::new(1, 1);
        m.sink_commit_statuses
            .insert("sink".into(), SinkCommitStatus::Pending);
        store.save(&m).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[], None).await.unwrap();
        assert!(
            result.is_none(),
            "should start fresh when all checkpoints have pending sinks"
        );
    }
}

#[cfg(test)]
mod rehydration_tests {
    use super::*;
    use bytes::Bytes;
    use laminar_core::state::{InProcessBackend, ObjectStoreBackend};

    async fn seal_epoch(backend: &dyn StateBackend, epoch: u64, vnodes: &[u32], tag: &[u8]) {
        for &v in vnodes {
            backend
                .write_partial(v, epoch, 0, Bytes::copy_from_slice(tag))
                .await
                .unwrap();
        }
        assert!(backend.epoch_complete(epoch, vnodes, &[]).await.unwrap());
    }

    #[tokio::test]
    async fn rehydrate_reads_committed_partials() {
        let backend = InProcessBackend::new(4);
        seal_epoch(&backend, 7, &[0, 1, 2], b"v7").await;

        let report = VnodeRehydrator::new(&backend).rehydrate(&[0, 1, 3]).await;

        assert_eq!(report.epoch, Some(7));
        assert_eq!(report.restored_count(), 2);
        assert_eq!(report.restored.get(&0).map(|c| &c[0][..]), Some(&b"v7"[..]));
        assert_eq!(report.restored.get(&1).map(|c| &c[0][..]), Some(&b"v7"[..]));
        // vnode 3 was never written — fresh start, no error.
        assert_eq!(report.missing, vec![3]);
        assert!(!report.has_errors());
    }

    #[tokio::test]
    async fn rehydrate_reads_latest_committed_epoch() {
        let backend = InProcessBackend::new(4);
        seal_epoch(&backend, 3, &[0, 1], b"old").await;
        seal_epoch(&backend, 9, &[0, 1], b"new").await;

        let report = VnodeRehydrator::new(&backend).rehydrate(&[0, 1]).await;

        assert_eq!(report.epoch, Some(9), "must read the highest sealed epoch");
        assert_eq!(
            report.restored.get(&0).map(|c| &c[0][..]),
            Some(&b"new"[..])
        );
    }

    /// A reference partial resolves (one hop) to the
    /// full partial it points at.
    #[tokio::test]
    async fn rehydrate_resolves_reference_partials() {
        let backend = InProcessBackend::new(4);

        let full = crate::vnode_partial::VnodePartial {
            checkpoint_id: 1,
            operators: vec![("agg".into(), vec![1, 2, 3])],
            base_epoch: None,
            deltas: Vec::new(),
        };
        backend
            .write_partial(0, 5, 0, Bytes::from(full.encode().unwrap()))
            .await
            .unwrap();
        assert!(backend.epoch_complete(5, &[0], &[]).await.unwrap());

        let reference = crate::vnode_partial::VnodePartial {
            checkpoint_id: 2,
            operators: Vec::new(),
            base_epoch: Some(5),
            deltas: Vec::new(),
        };
        backend
            .write_partial(0, 6, 0, Bytes::from(reference.encode().unwrap()))
            .await
            .unwrap();
        assert!(backend.epoch_complete(6, &[0], &[]).await.unwrap());

        let report = VnodeRehydrator::new(&backend).rehydrate(&[0]).await;
        assert_eq!(report.epoch, Some(6));
        let chain = report.restored.get(&0).expect("vnode restored");
        assert_eq!(chain.len(), 1, "reference resolves to a single full base");
        let restored = crate::vnode_partial::VnodePartial::decode(&chain[0]).unwrap();
        assert_eq!(
            restored.base_epoch, None,
            "the resolved partial must be the full base, not the reference",
        );
        assert_eq!(restored.operators[0].1, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn rehydrate_no_committed_epoch_is_fresh() {
        let backend = InProcessBackend::new(4);
        let report = VnodeRehydrator::new(&backend).rehydrate(&[0, 1]).await;
        assert_eq!(report.epoch, None);
        assert!(report.restored.is_empty());
        assert_eq!(report.missing, vec![0, 1]);
    }

    #[tokio::test]
    async fn rehydrate_empty_request_is_noop() {
        let backend = InProcessBackend::new(4);
        seal_epoch(&backend, 1, &[0], b"x").await;
        let report = VnodeRehydrator::new(&backend).rehydrate(&[]).await;
        assert_eq!(report.epoch, None);
        assert!(report.restored.is_empty());
        assert!(report.missing.is_empty());
    }

    #[tokio::test]
    async fn rehydrate_over_object_store_backend() {
        use object_store::local::LocalFileSystem;
        use object_store::ObjectStore;

        let dir = tempfile::tempdir().unwrap();
        let store: std::sync::Arc<dyn ObjectStore> =
            std::sync::Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let backend = ObjectStoreBackend::new(store, "node-0", 4);
        seal_epoch(&backend, 5, &[0, 1], b"durable").await;

        let report = VnodeRehydrator::new(&backend).rehydrate(&[0, 1]).await;

        assert_eq!(report.epoch, Some(5));
        assert_eq!(report.restored_count(), 2);
        assert_eq!(
            report.restored.get(&1).map(|c| &c[0][..]),
            Some(&b"durable"[..])
        );
    }
}
