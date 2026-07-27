//! Exact-seal vnode-chain loading for checkpoint recovery and ownership transfer.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use laminar_core::state::{CheckpointAttempt, CheckpointSealInventory, StateBackend};
use tracing::{debug, info};

use crate::error::DbError;

const VNODE_CHAIN_LOAD_CONCURRENCY: usize = 32;

/// Vnode recovery chains loaded from one exact sealed attempt.
#[derive(Debug, Default)]
pub(crate) struct LoadedVnodeChains {
    /// Exact sealed attempt the partials were read from. `None` only for an empty request.
    pub(crate) attempt: Option<CheckpointAttempt>,
    /// vnode → recovery chain (oldest→newest): a FULL base followed by any delta partials.
    pub(crate) chains: HashMap<u32, Vec<Bytes>>,
}

impl LoadedVnodeChains {
    /// Number of vnode chains successfully loaded.
    #[must_use]
    pub(crate) fn chain_count(&self) -> usize {
        self.chains.len()
    }
}

/// Reads exact Commit-outcome-bound `partial.bin` chains for requested vnodes.
/// Applying the bytes is the caller's responsibility.
pub(crate) struct SealedVnodeChainReader<'a> {
    backend: &'a dyn StateBackend,
    seal_cache: tokio::sync::Mutex<HashMap<CheckpointAttempt, Arc<CheckpointSealInventory>>>,
    validated_head_attempt: Option<CheckpointAttempt>,
    max_partial_bytes: u64,
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
        }
    }

    /// Create a reader pinned to the exact seal already validated with the committed source cut.
    #[cfg(feature = "cluster")]
    pub(crate) fn from_validated_head(
        backend: &'a dyn StateBackend,
        head: &crate::checkpoint_coordinator::ValidatedVnodeRestoreHead,
        max_partial_bytes: u64,
    ) -> Result<Self, DbError> {
        if max_partial_bytes == 0 {
            return Err(DbError::Checkpoint(
                "[LDB-6050] vnode partial artifact limit must be nonzero".into(),
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
        seal_cache.insert(attempt, inventory);
        Ok(Self {
            backend,
            seal_cache: tokio::sync::Mutex::new(seal_cache),
            validated_head_attempt: Some(attempt),
            max_partial_bytes,
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
        if let Some(inventory) = self.seal_cache.lock().await.get(&attempt).cloned() {
            return Ok(inventory);
        }
        let inventory = self
            .backend
            .checkpoint_seal_inventory(attempt)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] failed to read exact state seal for checkpoint {} epoch {}: {error}",
                    attempt.checkpoint_id, attempt.epoch
                ))
            })?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] checkpoint {} epoch {} has no exact state seal",
                    attempt.checkpoint_id, attempt.epoch
                ))
            })?;
        if inventory.attempt != attempt {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] requested state attempt {attempt:?} does not match seal inventory attempt {:?}",
                inventory.attempt
            )));
        }
        inventory.validate_vnode_partials().map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6050] checkpoint {} epoch {} has an invalid sealed vnode inventory: {error}",
                attempt.checkpoint_id, attempt.epoch
            ))
        })?;
        let inventory = Arc::new(inventory);
        self.seal_cache
            .lock()
            .await
            .insert(attempt, Arc::clone(&inventory));
        Ok(inventory)
    }

    async fn read_verified_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<Bytes, DbError> {
        let inventory = self.sealed_inventory(attempt).await?;
        let index = inventory
            .sealed_partials
            .binary_search_by_key(&vnode, |partial| partial.vnode)
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] vnode {vnode} is absent from the exact state seal for checkpoint {} epoch {}",
                    attempt.checkpoint_id, attempt.epoch
                ))
        })?;
        let attestation = &inventory.sealed_partials[index];
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
        Ok(bytes)
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
        if let Some(unsealed) = requested_vnodes
            .iter()
            .find(|vnode| inventory.required_vnodes.binary_search(vnode).is_err())
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] vnode {unsealed} is absent from the exact state seal for checkpoint {} epoch {}",
                attempt.checkpoint_id, attempt.epoch
            )));
        }
        loaded.attempt = Some(attempt);

        let chains = futures::stream::iter(requested_vnodes.into_iter().map(|vnode| async move {
            let chain = self.collect_chain(vnode, attempt).await?;
            Ok::<_, DbError>((vnode, chain))
        }))
        .buffer_unordered(VNODE_CHAIN_LOAD_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
        for (vnode, chain) in chains {
            debug!(
                vnode,
                epoch = attempt.epoch,
                checkpoint_id = attempt.checkpoint_id,
                links = chain.len(),
                "loaded sealed vnode chain"
            );
            loaded.chains.insert(vnode, chain);
        }

        info!(
            epoch = attempt.epoch,
            checkpoint_id = attempt.checkpoint_id,
            chains = loaded.chains.len(),
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
    ) -> Result<Vec<Bytes>, DbError> {
        use crate::vnode_partial::VnodePartial;

        let (bytes, mut current, head) = self.resolve_reference_head(vnode, attempt).await?;
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
            if parent_attempt.epoch >= current.epoch
                || parent_attempt.checkpoint_id >= current.checkpoint_id
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} has non-decreasing delta link \
                     {current:?}->{parent_attempt:?}"
                )));
            }
            let parent_bytes = self
                .read_verified_partial(parent_attempt, vnode)
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
            for (name, _) in &parent_partial.operators {
                need.remove(name);
            }
            chain_rev.push(parent_bytes);
            current = parent_attempt;
            current_partial = parent_partial;
        }
        chain_rev.reverse();
        Ok(chain_rev)
    }

    /// Collapse reference-only partials and return the first FULL or DELTA head without decoding
    /// that head twice on the recovery path.
    async fn resolve_reference_head(
        &self,
        vnode: u32,
        attempt: CheckpointAttempt,
    ) -> Result<(Bytes, CheckpointAttempt, crate::vnode_partial::VnodePartial), DbError> {
        use crate::vnode_partial::VnodePartial;

        let mut bytes = self.read_verified_partial(attempt, vnode).await?;
        let mut current = attempt;
        loop {
            let partial = VnodePartial::decode(&bytes).map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} has an invalid partial at epoch {} checkpoint {}: \
                     {error}",
                    current.epoch, current.checkpoint_id
                ))
            })?;
            if !partial.operators.is_empty() || !partial.deltas.is_empty() {
                return Ok((bytes, current, partial));
            }
            let Some(base) = partial.base else {
                return Ok((bytes, current, partial));
            };
            let base_attempt = base;
            if base_attempt.epoch >= current.epoch
                || base_attempt.checkpoint_id >= current.checkpoint_id
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} has non-decreasing reference \
                     {current:?}->{base_attempt:?}"
                )));
            }
            bytes = self
                .read_verified_partial(base_attempt, vnode)
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6051] vnode {vnode} reference base {base_attempt:?} failed seal verification: {error}"
                    ))
                })?;
            current = base_attempt;
        }
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
