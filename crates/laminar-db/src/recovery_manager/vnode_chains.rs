//! Exact-seal vnode-chain loading for checkpoint recovery and ownership transfer.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use laminar_core::state::{
    CheckpointAttempt, CheckpointSealInventory, SealedVnodePartial, StateBackend,
    VnodePartialLineage,
};
use tracing::{debug, info};

use crate::error::DbError;

const VNODE_CHAIN_LOAD_CONCURRENCY: usize = 32;

type SealInventoryLoad = Result<Arc<CheckpointSealInventory>, String>;
type SealInventoryCell = tokio::sync::OnceCell<Arc<CheckpointSealInventory>>;

/// Immutable accounting for the checkpoint bodies declared and verified by one restore load.
///
/// The declared ceilings come from sealed transitive lineage metadata before any vnode body read.
/// They do not acquire memory or request permits. The actual counters include every verified body,
/// including reference-only artifacts omitted from the returned apply chain.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VnodeRestoreInputUsage {
    declared_lineage_bytes: u64,
    declared_lineage_artifacts: u64,
    verified_body_bytes: u64,
    verified_body_artifacts: u64,
}

impl VnodeRestoreInputUsage {
    /// Validate that the receipt can describe the returned vnode-chain collection.
    ///
    /// A nonempty load verifies at least one body per chain. It may consume less than its complete
    /// declared lineage ceiling when every requested operator finds a newer FULL base.
    pub(crate) fn validate_for_loaded_chains(self, chain_count: usize) -> Result<(), &'static str> {
        if chain_count == 0 {
            return if self == Self::default() {
                Ok(())
            } else {
                Err("an empty vnode restore must have zero input usage")
            };
        }
        let chain_count = u64::try_from(chain_count)
            .map_err(|_| "vnode restore chain count does not fit usage accounting")?;
        if self.declared_lineage_bytes == 0
            || self.verified_body_bytes == 0
            || self.declared_lineage_artifacts < chain_count
            || self.verified_body_artifacts < chain_count
        {
            return Err("a nonempty vnode restore has incomplete input usage");
        }
        if self.verified_body_bytes > self.declared_lineage_bytes
            || self.verified_body_artifacts > self.declared_lineage_artifacts
        {
            return Err("verified vnode restore input exceeds its declared lineage ceiling");
        }
        Ok(())
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) const fn declared_lineage_bytes(self) -> u64 {
        self.declared_lineage_bytes
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) const fn declared_lineage_artifacts(self) -> u64 {
        self.declared_lineage_artifacts
    }

    #[must_use]
    pub(crate) const fn verified_body_bytes(self) -> u64 {
        self.verified_body_bytes
    }

    #[must_use]
    pub(crate) const fn verified_body_artifacts(self) -> u64 {
        self.verified_body_artifacts
    }

    #[cfg(test)]
    pub(crate) const fn from_counts_for_test(
        declared_lineage_bytes: u64,
        declared_lineage_artifacts: u64,
        verified_body_bytes: u64,
        verified_body_artifacts: u64,
    ) -> Self {
        Self {
            declared_lineage_bytes,
            declared_lineage_artifacts,
            verified_body_bytes,
            verified_body_artifacts,
        }
    }

    fn add_verified_chain(&mut self, chain: ChainInputUsage) -> Result<(), DbError> {
        self.verified_body_bytes = self
            .verified_body_bytes
            .checked_add(chain.bytes)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6051] verified vnode restore body byte accounting overflow".into(),
                )
            })?;
        self.verified_body_artifacts = self
            .verified_body_artifacts
            .checked_add(chain.artifacts)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6051] verified vnode restore artifact accounting overflow".into(),
                )
            })?;
        Ok(())
    }
}

/// Vnode recovery chains loaded from one exact sealed attempt.
#[derive(Debug, Default)]
pub(crate) struct LoadedVnodeChains {
    /// Exact sealed attempt the partials were read from. `None` only for an empty request.
    pub(crate) attempt: Option<CheckpointAttempt>,
    /// vnode → recovery chain (oldest→newest): a FULL base followed by any delta partials.
    pub(crate) chains: HashMap<u32, Vec<Bytes>>,
    /// Restore input declared by seals and actually verified from immutable bodies.
    usage: VnodeRestoreInputUsage,
}

impl LoadedVnodeChains {
    /// Number of vnode chains successfully loaded.
    #[must_use]
    pub(crate) fn chain_count(&self) -> usize {
        self.chains.len()
    }

    #[must_use]
    pub(crate) const fn input_usage(&self) -> VnodeRestoreInputUsage {
        self.usage
    }

    #[cfg(test)]
    pub(crate) fn from_chains_for_test(
        attempt: Option<CheckpointAttempt>,
        chains: HashMap<u32, Vec<Bytes>>,
    ) -> Self {
        let (bytes, artifacts) = chains.values().flat_map(|chain| chain.iter()).fold(
            (0_u64, 0_u64),
            |(bytes, artifacts), body| {
                (
                    bytes
                        .checked_add(u64::try_from(body.len()).unwrap())
                        .unwrap(),
                    artifacts.checked_add(1).unwrap(),
                )
            },
        );
        Self {
            attempt,
            chains,
            usage: VnodeRestoreInputUsage::from_counts_for_test(bytes, artifacts, bytes, artifacts),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_parts_with_usage_for_test(
        attempt: Option<CheckpointAttempt>,
        chains: HashMap<u32, Vec<Bytes>>,
        usage: VnodeRestoreInputUsage,
    ) -> Self {
        Self {
            attempt,
            chains,
            usage,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ChainLineageCeiling {
    bytes: u64,
    artifacts: u64,
}

#[derive(Debug)]
struct ChainLoadRequest {
    vnode: u32,
    head: SealedVnodePartial,
    lineage_ceiling: ChainLineageCeiling,
}

#[derive(Debug)]
struct RestoreInputPreflight {
    requests: Vec<ChainLoadRequest>,
    usage: VnodeRestoreInputUsage,
}

#[derive(Debug, Default, Clone, Copy)]
struct ChainInputUsage {
    bytes: u64,
    artifacts: u64,
}

/// Reads exact Commit-outcome-bound `partial.bin` chains for requested vnodes.
/// Applying the bytes is the caller's responsibility.
pub(crate) struct SealedVnodeChainReader<'a> {
    backend: &'a dyn StateBackend,
    seal_cache: tokio::sync::Mutex<HashMap<CheckpointAttempt, Arc<SealInventoryCell>>>,
    validated_head_attempt: Option<CheckpointAttempt>,
    max_partial_bytes: u64,
    max_artifacts_per_vnode_chain: usize,
    committed_full_cut_lineage_payload_bytes: u64,
    committed_full_cut_lineage_artifacts: u64,
}

impl<'a> SealedVnodeChainReader<'a> {
    /// Create an unrestricted test reader over `backend`.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn new(backend: &'a dyn StateBackend) -> Self {
        Self {
            backend,
            seal_cache: tokio::sync::Mutex::new(HashMap::new()),
            validated_head_attempt: None,
            max_partial_bytes: u64::MAX,
            max_artifacts_per_vnode_chain: usize::MAX,
            committed_full_cut_lineage_payload_bytes: u64::MAX,
            committed_full_cut_lineage_artifacts: u64::MAX,
        }
    }

    /// Create a reader pinned to the exact seal already validated with the committed source cut.
    #[cfg(feature = "cluster")]
    pub(crate) fn from_committed_head(
        backend: &'a dyn StateBackend,
        head: &crate::checkpoint_coordinator::ValidatedVnodeRestoreHead,
    ) -> Result<Self, DbError> {
        let contract = head.contract();
        let max_partial_bytes = contract.limits.max_partial_payload_bytes;
        let max_artifacts_per_vnode_chain =
            usize::try_from(contract.limits.max_artifacts_per_vnode_chain).map_err(|_| {
                DbError::Checkpoint(
                    "[LDB-6050] durable vnode chain limit does not fit this runtime".into(),
                )
            })?;
        Self::from_head_limits(
            backend,
            head,
            max_partial_bytes,
            max_artifacts_per_vnode_chain,
            contract.exact_cluster_lineage_payload_bytes,
            contract.exact_cluster_lineage_artifacts,
        )
    }

    #[cfg(all(feature = "cluster", test))]
    pub(crate) fn from_validated_head(
        backend: &'a dyn StateBackend,
        head: &crate::checkpoint_coordinator::ValidatedVnodeRestoreHead,
        max_partial_bytes: u64,
        max_artifacts_per_vnode_chain: usize,
    ) -> Result<Self, DbError> {
        let artifact_multiplier = u64::try_from(max_artifacts_per_vnode_chain).map_err(|_| {
            DbError::Checkpoint(
                "[LDB-6050] vnode chain artifact limit does not fit accounting".into(),
            )
        })?;
        let test_full_cut_lineage_payload_bytes = max_partial_bytes
            .checked_mul(artifact_multiplier)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6050] test vnode restore payload envelope overflows u64".into(),
                )
            })?;
        let test_full_cut_lineage_artifacts = u64::try_from(head.inventory().required_vnodes.len())
            .ok()
            .and_then(|vnodes| vnodes.checked_mul(artifact_multiplier))
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6050] test vnode restore artifact envelope overflows u64".into(),
                )
            })?;
        Self::from_head_limits(
            backend,
            head,
            max_partial_bytes,
            max_artifacts_per_vnode_chain,
            test_full_cut_lineage_payload_bytes,
            test_full_cut_lineage_artifacts,
        )
    }

    #[cfg(feature = "cluster")]
    fn from_head_limits(
        backend: &'a dyn StateBackend,
        head: &crate::checkpoint_coordinator::ValidatedVnodeRestoreHead,
        max_partial_bytes: u64,
        max_artifacts_per_vnode_chain: usize,
        committed_full_cut_lineage_payload_bytes: u64,
        committed_full_cut_lineage_artifacts: u64,
    ) -> Result<Self, DbError> {
        if max_partial_bytes == 0 {
            return Err(DbError::Checkpoint(
                "[LDB-6050] vnode partial artifact limit must be nonzero".into(),
            ));
        }
        if max_artifacts_per_vnode_chain == 0 {
            return Err(DbError::Checkpoint(
                "[LDB-6050] vnode chain artifact limit must be nonzero".into(),
            ));
        }
        let attempt = head.attempt();
        Self::require_canonical_attempt(attempt)?;
        let inventory = head.inventory();
        inventory.validate_vnode_partials().map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6050] validated vnode restore head has an invalid seal inventory: {error}"
            ))
        })?;
        let mut seal_cache = HashMap::new();
        for (cached_attempt, cached_inventory) in head.lineage_inventories() {
            let inventory_cell = SealInventoryCell::new();
            inventory_cell.set(cached_inventory).map_err(|_| {
                DbError::Checkpoint("[LDB-6050] validated seal cache collision".into())
            })?;
            if seal_cache
                .insert(cached_attempt, Arc::new(inventory_cell))
                .is_some()
            {
                return Err(DbError::Checkpoint(
                    "[LDB-6050] duplicate validated seal-cache attempt".into(),
                ));
            }
        }
        if !seal_cache.contains_key(&attempt) {
            return Err(DbError::Checkpoint(
                "[LDB-6050] validated seal cache is missing its committed head".into(),
            ));
        }
        Ok(Self {
            backend,
            seal_cache: tokio::sync::Mutex::new(seal_cache),
            validated_head_attempt: Some(attempt),
            max_partial_bytes,
            max_artifacts_per_vnode_chain,
            committed_full_cut_lineage_payload_bytes,
            committed_full_cut_lineage_artifacts,
        })
    }

    fn require_canonical_attempt(attempt: CheckpointAttempt) -> Result<(), DbError> {
        if attempt.is_canonical() {
            return Ok(());
        }
        Err(DbError::Checkpoint(format!(
            "[LDB-6050] vnode chain load requires one nonzero canonical checkpoint ID; received epoch {} checkpoint {}",
            attempt.epoch, attempt.checkpoint_id
        )))
    }

    pub(super) async fn sealed_inventory(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Arc<CheckpointSealInventory>, DbError> {
        Self::require_canonical_attempt(attempt)?;
        let cell = {
            let mut cache = self.seal_cache.lock().await;
            Arc::clone(
                cache
                    .entry(attempt)
                    .or_insert_with(|| Arc::new(SealInventoryCell::new())),
            )
        };
        let loaded = cell
            .get_or_try_init(|| async { self.fetch_sealed_inventory(attempt).await })
            .await;
        match loaded {
            Ok(inventory) => Ok(Arc::clone(inventory)),
            Err(error) => Err(DbError::Checkpoint(error)),
        }
    }

    async fn fetch_sealed_inventory(&self, attempt: CheckpointAttempt) -> SealInventoryLoad {
        let inventory = self
            .backend
            .checkpoint_seal_inventory(attempt)
            .await
            .map_err(|error| {
                format!(
                    "[LDB-6050] failed to read exact state seal for checkpoint {} epoch {}: {error}",
                    attempt.checkpoint_id, attempt.epoch
                )
            })?
            .ok_or_else(|| {
                format!(
                    "[LDB-6050] checkpoint {} epoch {} has no exact state seal",
                    attempt.checkpoint_id, attempt.epoch
                )
            })?;
        if inventory.attempt != attempt {
            return Err(format!(
                "[LDB-6050] requested state attempt {attempt:?} does not match seal inventory attempt {:?}",
                inventory.attempt
            ));
        }
        inventory.validate_vnode_partials().map_err(|error| {
            format!(
                "[LDB-6050] checkpoint {} epoch {} has an invalid sealed vnode inventory: {error}",
                attempt.checkpoint_id, attempt.epoch
            )
        })?;
        Ok(Arc::new(inventory))
    }

    fn sealed_attestation(
        inventory: &CheckpointSealInventory,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<SealedVnodePartial, DbError> {
        let index = inventory
            .sealed_partials
            .binary_search_by_key(&vnode, |partial| partial.vnode)
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] vnode {vnode} is absent from the exact state seal for checkpoint {} epoch {}",
                    attempt.checkpoint_id, attempt.epoch
                ))
            })?;
        Ok(inventory.sealed_partials[index].clone())
    }

    async fn attestation_at(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<SealedVnodePartial, DbError> {
        let inventory = self.sealed_inventory(attempt).await?;
        Self::sealed_attestation(&inventory, attempt, vnode)
    }

    async fn read_verified_partial(
        &self,
        attempt: CheckpointAttempt,
        attestation: &SealedVnodePartial,
        lineage_ceiling: ChainLineageCeiling,
        usage: &mut ChainInputUsage,
    ) -> Result<Bytes, DbError> {
        let vnode = attestation.vnode;
        let next_bytes = usage
            .bytes
            .checked_add(attestation.payload_len)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} verified body byte accounting overflow"
                ))
            })?;
        let next_artifacts = usage.artifacts.checked_add(1).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "[LDB-6051] vnode {vnode} verified body artifact accounting overflow"
            ))
        })?;
        if next_bytes > lineage_ceiling.bytes || next_artifacts > lineage_ceiling.artifacts {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] vnode {vnode} recovery traversal exceeds its sealed lineage ceiling of {} bytes and {} artifacts",
                lineage_ceiling.bytes, lineage_ceiling.artifacts
            )));
        }
        let bytes = self
            .backend
            .read_sealed_partial_bounded(attempt, attestation, self.max_partial_bytes)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] failed to read vnode {vnode} at sealed epoch {} checkpoint {}: {error}",
                    attempt.epoch, attempt.checkpoint_id
                ))
            })?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} is missing at sealed epoch {} checkpoint {}",
                    attempt.epoch, attempt.checkpoint_id
                ))
            })?;
        if u64::try_from(bytes.len()).ok() != Some(attestation.payload_len) {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] vnode {vnode} backend returned {} bytes for a sealed {}-byte payload at {attempt:?}",
                bytes.len(), attestation.payload_len
            )));
        }
        usage.bytes = next_bytes;
        usage.artifacts = next_artifacts;
        Ok(bytes)
    }

    async fn verified_parent_attestation(
        &self,
        current_attempt: CheckpointAttempt,
        vnode: u32,
        current: &SealedVnodePartial,
        parent_attempt: CheckpointAttempt,
    ) -> Result<SealedVnodePartial, DbError> {
        let parent = self.attestation_at(parent_attempt, vnode).await?;
        let expected = VnodePartialLineage::extend(
            current_attempt,
            current.payload_len,
            parent_attempt,
            parent.lineage,
        )
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6051] vnode {vnode} has invalid sealed lineage at {current_attempt:?}: {error}"
            ))
        })?;
        if current.lineage != expected {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] vnode {vnode} sealed lineage at {current_attempt:?} does not exactly extend parent {parent_attempt:?}"
            )));
        }
        Ok(parent)
    }

    fn preflight_restore_inputs(
        &self,
        inventory: &CheckpointSealInventory,
        attempt: CheckpointAttempt,
        requested_vnodes: &[u32],
    ) -> Result<RestoreInputPreflight, DbError> {
        let mut requests = Vec::new();
        requests
            .try_reserve_exact(requested_vnodes.len())
            .map_err(|_| {
                DbError::Checkpoint(
                    "[LDB-6050] vnode restore request-vector allocation failed".into(),
                )
            })?;
        let mut declared_lineage_bytes = 0_u64;
        let mut declared_lineage_artifacts = 0_u64;
        for &vnode in requested_vnodes {
            let attestation = Self::sealed_attestation(inventory, attempt, vnode)?;
            let lineage_artifacts =
                usize::try_from(attestation.lineage.artifact_count()).map_err(|_| {
                    DbError::Checkpoint(format!(
                        "[LDB-6050] vnode {vnode} lineage artifact count does not fit this runtime"
                    ))
                })?;
            if lineage_artifacts > self.max_artifacts_per_vnode_chain {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6050] vnode {vnode} sealed lineage declares {lineage_artifacts} artifacts, exceeding the writer-derived limit of {} artifacts",
                    self.max_artifacts_per_vnode_chain
                )));
            }
            let lineage_bytes = attestation.lineage.total_payload_bytes();
            let lineage_artifacts_u64 = u64::from(attestation.lineage.artifact_count());
            declared_lineage_bytes = declared_lineage_bytes
                .checked_add(lineage_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "[LDB-6050] vnode restore declared lineage byte total overflow".into(),
                    )
                })?;
            declared_lineage_artifacts = declared_lineage_artifacts
                .checked_add(lineage_artifacts_u64)
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "[LDB-6050] vnode restore declared lineage artifact total overflow".into(),
                    )
                })?;
            requests.push(ChainLoadRequest {
                vnode,
                head: attestation,
                lineage_ceiling: ChainLineageCeiling {
                    bytes: lineage_bytes,
                    artifacts: lineage_artifacts_u64,
                },
            });
        }
        if declared_lineage_bytes > self.committed_full_cut_lineage_payload_bytes {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] vnode restore declares {declared_lineage_bytes} lineage bytes, exceeding the committed full-cut total of {} bytes",
                self.committed_full_cut_lineage_payload_bytes
            )));
        }
        if declared_lineage_artifacts > self.committed_full_cut_lineage_artifacts {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] vnode restore declares {declared_lineage_artifacts} lineage artifacts, exceeding the committed full-cut total of {} artifacts",
                self.committed_full_cut_lineage_artifacts
            )));
        }
        Ok(RestoreInputPreflight {
            requests,
            usage: VnodeRestoreInputUsage {
                declared_lineage_bytes,
                declared_lineage_artifacts,
                ..VnodeRestoreInputUsage::default()
            },
        })
    }

    /// Load each vnode's partial chain pinned at `attempt`, a committed cut chosen by the caller.
    ///
    /// # Errors
    /// Returns a checkpoint error unless every requested vnode has a complete, decodable chain.
    pub(crate) async fn load_at(
        &self,
        vnodes: &[u32],
        attempt: CheckpointAttempt,
    ) -> Result<LoadedVnodeChains, DbError> {
        Self::require_canonical_attempt(attempt)?;
        if let Some(validated) = self.validated_head_attempt {
            if validated != attempt {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6050] requested vnode restore attempt {attempt:?} does not match the validated committed head {validated:?}"
                )));
            }
        }
        let mut loaded = LoadedVnodeChains::default();
        if vnodes.is_empty() {
            return Ok(loaded);
        }
        let mut requested_vnodes = vnodes.to_vec();
        requested_vnodes.sort_unstable();
        if requested_vnodes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DbError::Checkpoint(
                "[LDB-6050] vnode restore request contains duplicate vnodes".into(),
            ));
        }
        let inventory = self.sealed_inventory(attempt).await?;
        let preflight = self.preflight_restore_inputs(&inventory, attempt, &requested_vnodes)?;
        loaded.attempt = Some(attempt);
        loaded.usage = preflight.usage;

        let chains =
            futures::stream::iter(preflight.requests.into_iter().map(|request| async move {
                let (chain, usage) = self
                    .collect_chain(
                        request.vnode,
                        attempt,
                        request.head,
                        request.lineage_ceiling,
                    )
                    .await?;
                Ok::<_, DbError>((request.vnode, chain, usage))
            }))
            .buffer_unordered(VNODE_CHAIN_LOAD_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        for (vnode, chain, chain_usage) in chains {
            debug!(
                vnode,
                epoch = attempt.epoch,
                checkpoint_id = attempt.checkpoint_id,
                links = chain.len(),
                "loaded sealed vnode chain"
            );
            loaded.usage.add_verified_chain(chain_usage)?;
            loaded.chains.insert(vnode, chain);
        }
        loaded
            .usage
            .validate_for_loaded_chains(loaded.chains.len())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] invalid vnode restore input receipt: {error}"
                ))
            })?;

        info!(
            epoch = attempt.epoch,
            checkpoint_id = attempt.checkpoint_id,
            chains = loaded.chains.len(),
            declared_lineage_bytes = loaded.usage.declared_lineage_bytes,
            verified_body_bytes = loaded.usage.verified_body_bytes,
            verified_body_artifacts = loaded.usage.verified_body_artifacts,
            "sealed vnode chain load complete"
        );
        Ok(loaded)
    }

    /// Resolve a vnode's recovery chain at an exact attempt: collapse leading reference hops,
    /// then walk exact parent attempts until each head op has its FULL base (oldest→newest).
    async fn collect_chain(
        &self,
        vnode: u32,
        attempt: CheckpointAttempt,
        head_attestation: SealedVnodePartial,
        lineage_ceiling: ChainLineageCeiling,
    ) -> Result<(Vec<Bytes>, ChainInputUsage), DbError> {
        use crate::vnode_partial::VnodePartial;

        let mut usage = ChainInputUsage::default();
        let (bytes, mut current, head, mut current_attestation) = self
            .resolve_reference_head(
                vnode,
                attempt,
                head_attestation,
                lineage_ceiling,
                &mut usage,
            )
            .await?;
        let mut need: std::collections::HashSet<String> = head
            .deltas
            .iter()
            .map(|(name, _)| name.clone())
            .filter(|name| !head.operators.iter().any(|(op_name, _)| op_name == name))
            .collect();
        let mut chain_rev: Vec<Bytes> = vec![bytes];
        let mut current_partial = head;
        while !need.is_empty() {
            let Some(parent) = current_partial.base else {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} delta chain has no FULL base for operators {need:?}"
                )));
            };
            let parent_attempt = parent;
            let parent_attestation = self
                .verified_parent_attestation(
                    current,
                    vnode,
                    &current_attestation,
                    parent_attempt,
                )
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6051] vnode {vnode} delta parent {parent_attempt:?} failed seal verification: {error}"
                    ))
                })?;
            let parent_bytes = self
                .read_verified_partial(
                    parent_attempt,
                    &parent_attestation,
                    lineage_ceiling,
                    &mut usage,
                )
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6051] vnode {vnode} delta parent {parent_attempt:?} failed seal verification: {error}"
                    ))
                })?;
            let parent_partial = VnodePartial::decode(&parent_bytes).map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} delta parent {parent_attempt:?} is invalid: {error}"
                ))
            })?;
            Self::verify_decoded_parent(
                vnode,
                parent_attempt,
                &parent_partial,
                &parent_attestation,
            )?;
            for (name, _) in &parent_partial.operators {
                need.remove(name);
            }
            chain_rev.push(parent_bytes);
            current = parent_attempt;
            current_partial = parent_partial;
            current_attestation = parent_attestation;
        }
        chain_rev.reverse();
        Ok((chain_rev, usage))
    }

    /// Collapse reference-only partials and return the first FULL or DELTA head without decoding
    /// that head twice on the recovery path.
    async fn resolve_reference_head(
        &self,
        vnode: u32,
        attempt: CheckpointAttempt,
        mut attestation: SealedVnodePartial,
        lineage_ceiling: ChainLineageCeiling,
        usage: &mut ChainInputUsage,
    ) -> Result<
        (
            Bytes,
            CheckpointAttempt,
            crate::vnode_partial::VnodePartial,
            SealedVnodePartial,
        ),
        DbError,
    > {
        use crate::vnode_partial::VnodePartial;

        let mut bytes = self
            .read_verified_partial(attempt, &attestation, lineage_ceiling, usage)
            .await?;
        let mut current = attempt;
        loop {
            let partial = VnodePartial::decode(&bytes).map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} has an invalid partial at epoch {} checkpoint {}: \
                     {error}",
                    current.epoch, current.checkpoint_id
                ))
            })?;
            Self::verify_decoded_parent(vnode, current, &partial, &attestation)?;
            if !partial.operators.is_empty() || !partial.deltas.is_empty() {
                return Ok((bytes, current, partial, attestation));
            }
            let Some(base) = partial.base else {
                return Ok((bytes, current, partial, attestation));
            };
            let base_attempt = base;
            let base_attestation = self
                .verified_parent_attestation(current, vnode, &attestation, base_attempt)
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6051] vnode {vnode} reference base {base_attempt:?} failed seal verification: {error}"
                    ))
                })?;
            bytes = self
                .read_verified_partial(base_attempt, &base_attestation, lineage_ceiling, usage)
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6051] vnode {vnode} reference base {base_attempt:?} failed seal verification: {error}"
                    ))
                })?;
            current = base_attempt;
            attestation = base_attestation;
        }
    }

    fn verify_decoded_parent(
        vnode: u32,
        attempt: CheckpointAttempt,
        partial: &crate::vnode_partial::VnodePartial,
        attestation: &SealedVnodePartial,
    ) -> Result<(), DbError> {
        if partial.base != attestation.lineage.parent() {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] vnode {vnode} partial at {attempt:?} names base {:?}, but its sealed lineage names {:?}",
                partial.base,
                attestation.lineage.parent()
            )));
        }
        Ok(())
    }
}

/// One operator's resolved recovery chain: FULL base bytes + ordered changed-state deltas.
#[cfg(feature = "cluster")]
pub(crate) type ResolvedOpChain<'a> = (&'a [u8], Vec<&'a [u8]>);

/// Resolve one operator's FULL base and ordered delta payloads from a vnode recovery chain.
#[cfg(feature = "cluster")]
#[must_use]
pub(crate) fn resolve_op_chain<'a>(
    chain: &'a [crate::vnode_partial::VnodePartial],
    op: &str,
) -> Option<ResolvedOpChain<'a>> {
    let base_idx = chain
        .iter()
        .rposition(|partial| partial.operators.iter().any(|(name, _)| name == op))?;
    let base = chain[base_idx]
        .operators
        .iter()
        .find(|(name, _)| name == op)
        .map(|(_, bytes)| bytes.as_slice())?;
    let mut deltas = Vec::new();
    for partial in &chain[base_idx + 1..] {
        if let Some((_, delta)) = partial.deltas.iter().find(|(name, _)| name == op) {
            deltas.push(delta.as_slice());
        }
    }
    Some((base, deltas))
}
