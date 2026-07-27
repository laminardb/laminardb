//! Exact-seal vnode-chain loading for checkpoint recovery and ownership transfer.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use laminar_core::state::{
    CheckpointAttempt, CheckpointSealInventory, SealedVnodePartial, StateBackend,
};
use tracing::{debug, info};

use crate::error::DbError;

const VNODE_CHAIN_LOAD_CONCURRENCY: usize = 32;

type SealedPartialKey = (CheckpointAttempt, u32);

#[derive(Debug)]
struct SealedPartialRead {
    attestation: SealedVnodePartial,
    bytes: tokio::sync::OnceCell<Bytes>,
}

#[derive(Default)]
struct PayloadBudgetState {
    reserved_bytes: u64,
    reads: HashMap<SealedPartialKey, Arc<SealedPartialRead>>,
}

/// Reader-local admission budget for unique sealed payloads.
///
/// This bounds the sum of sealed payload lengths admitted by one chain load. It is not an RSS
/// bound: object envelopes, decoded values, allocator overhead, and alignment copies are outside
/// this accounting.
struct UniquePayloadBudget {
    limit_bytes: u64,
    state: parking_lot::Mutex<PayloadBudgetState>,
}

impl UniquePayloadBudget {
    fn new(limit_bytes: u64) -> Self {
        Self {
            limit_bytes,
            state: parking_lot::Mutex::new(PayloadBudgetState::default()),
        }
    }

    fn reserve(
        &self,
        attempt: CheckpointAttempt,
        attestation: &SealedVnodePartial,
    ) -> Result<Arc<SealedPartialRead>, DbError> {
        let key = (attempt, attestation.vnode);
        let mut state = self.state.lock();
        if let Some(existing) = state.reads.get(&key) {
            if existing.attestation != *attestation {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] sealed vnode {:?} was observed with conflicting attestations during one chain load",
                    key
                )));
            }
            return Ok(Arc::clone(existing));
        }

        let reserved_bytes = state
            .reserved_bytes
            .checked_add(attestation.payload_len)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] unique sealed vnode payload accounting overflowed while reserving vnode {} at {attempt:?}",
                    attestation.vnode
                ))
            })?;
        if reserved_bytes > self.limit_bytes {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] unique sealed vnode payloads require at least {reserved_bytes} bytes; recovery budget is {} bytes",
                self.limit_bytes
            )));
        }

        let read = Arc::new(SealedPartialRead {
            attestation: attestation.clone(),
            bytes: tokio::sync::OnceCell::new(),
        });
        state.reserved_bytes = reserved_bytes;
        state.reads.insert(key, Arc::clone(&read));
        Ok(read)
    }

    /// Atomically reserve a known set before any associated body read starts.
    fn reserve_batch(
        &self,
        attempt: CheckpointAttempt,
        attestations: &[&SealedVnodePartial],
    ) -> Result<(), DbError> {
        let mut state = self.state.lock();
        let mut additions = Vec::new();
        let mut reserved_bytes = state.reserved_bytes;
        for attestation in attestations {
            let key = (attempt, attestation.vnode);
            if let Some(existing) = state.reads.get(&key) {
                if existing.attestation != **attestation {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6051] sealed vnode {key:?} was observed with conflicting attestations during one chain load"
                    )));
                }
                continue;
            }
            reserved_bytes = reserved_bytes
                .checked_add(attestation.payload_len)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "[LDB-6050] unique sealed vnode payload accounting overflowed while reserving vnode {} at {attempt:?}",
                        attestation.vnode
                    ))
                })?;
            additions.push((*attestation).clone());
        }
        if reserved_bytes > self.limit_bytes {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] unique sealed vnode payloads require at least {reserved_bytes} bytes; recovery budget is {} bytes",
                self.limit_bytes
            )));
        }
        for attestation in additions {
            let key = (attempt, attestation.vnode);
            state.reads.insert(
                key,
                Arc::new(SealedPartialRead {
                    attestation,
                    bytes: tokio::sync::OnceCell::new(),
                }),
            );
        }
        state.reserved_bytes = reserved_bytes;
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
    payload_budget: UniquePayloadBudget,
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
            payload_budget: UniquePayloadBudget::new(u64::MAX),
        }
    }

    /// Create a reader pinned to the exact seal already validated with the committed source cut.
    #[cfg(feature = "cluster")]
    pub(crate) fn from_validated_head(
        backend: &'a dyn StateBackend,
        head: &crate::checkpoint_coordinator::ValidatedVnodeRestoreHead,
        max_payload_bytes: u64,
    ) -> Result<Self, DbError> {
        if max_payload_bytes == 0 {
            return Err(DbError::Checkpoint(
                "[LDB-6050] vnode restore payload budget must be nonzero".into(),
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
            payload_budget: UniquePayloadBudget::new(max_payload_bytes),
        })
    }

    fn require_canonical_attempt(attempt: CheckpointAttempt) -> Result<(), DbError> {
        if attempt.is_canonical() {
            return Ok(());
        }
        Err(DbError::Checkpoint(format!(
            "[LDB-6050] vnode rehydration requires one nonzero canonical checkpoint ID; received epoch {} checkpoint {}",
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
        parent_link: Option<crate::vnode_partial::SealedVnodeParentLink>,
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
        if let Some(link) = parent_link {
            if !link.matches(attempt, attestation)? {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} parent attestation does not match its sealed link at {attempt:?}"
                )));
            }
        }
        let read = self.payload_budget.reserve(attempt, attestation)?;
        let bytes = read
            .bytes
            .get_or_try_init(|| async {
                let bytes = self
                    .backend
                    .read_sealed_partial_bounded(
                        attempt,
                        &read.attestation,
                        read.attestation.payload_len,
                    )
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
                if u64::try_from(bytes.len()).ok() != Some(read.attestation.payload_len) {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6051] vnode {vnode} backend returned {} bytes for a sealed {}-byte payload at {attempt:?}",
                        bytes.len(), read.attestation.payload_len
                    )));
                }
                Ok(bytes)
            })
            .await?;
        Ok(bytes.clone())
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
        let head_attestations = requested_vnodes
            .iter()
            .map(|vnode| {
                inventory.sealed_vnode_partial(*vnode).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "[LDB-6050] vnode {vnode} has no attestation in the exact state seal for {attempt:?}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Reserve every known head atomically so a head-total overflow performs no body reads.
        self.payload_budget
            .reserve_batch(attempt, &head_attestations)?;
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
            let parent_attempt = parent.attempt;
            if parent_attempt.epoch >= current.epoch
                || parent_attempt.checkpoint_id >= current.checkpoint_id
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} has non-decreasing delta link \
                     {current:?}->{parent_attempt:?}"
                )));
            }
            let parent_bytes = self
                .read_verified_partial(parent_attempt, vnode, Some(parent))
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

        let mut bytes = self.read_verified_partial(attempt, vnode, None).await?;
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
            let base_attempt = base.attempt;
            if base_attempt.epoch >= current.epoch
                || base_attempt.checkpoint_id >= current.checkpoint_id
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} has non-decreasing reference \
                     {current:?}->{base_attempt:?}"
                )));
            }
            bytes = self
                .read_verified_partial(base_attempt, vnode, Some(base))
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

#[cfg(test)]
mod budget_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn attestation(vnode: u32, payload_len: u64) -> SealedVnodePartial {
        SealedVnodePartial {
            vnode,
            assignment_version: 1,
            writer: None,
            payload_len,
            payload_sha256: "00".repeat(32),
        }
    }

    #[test]
    fn unique_payload_budget_counts_once_and_rejects_conflicts_and_overflow() {
        let attempt = CheckpointAttempt::canonical(7);
        let budget = UniquePayloadBudget::new(5);
        let sealed = attestation(0, 5);

        let first = budget.reserve(attempt, &sealed).unwrap();
        let duplicate = budget.reserve(attempt, &sealed).unwrap();
        assert!(Arc::ptr_eq(&first, &duplicate));
        assert_eq!(budget.state.lock().reserved_bytes, 5);

        let mut conflicting = sealed;
        conflicting.payload_sha256 = "11".repeat(32);
        let error = budget.reserve(attempt, &conflicting).unwrap_err();
        assert!(error.to_string().contains("conflicting attestations"));

        let error = budget.reserve(attempt, &attestation(1, 1)).unwrap_err();
        assert!(error.to_string().contains("recovery budget is 5 bytes"));

        let overflow = UniquePayloadBudget::new(u64::MAX);
        overflow
            .reserve(attempt, &attestation(0, u64::MAX))
            .unwrap();
        let error = overflow.reserve(attempt, &attestation(1, 1)).unwrap_err();
        assert!(error.to_string().contains("accounting overflowed"));
    }

    #[tokio::test]
    async fn sealed_payload_read_is_single_flight_and_retryable_after_failure_or_cancellation() {
        let attempt = CheckpointAttempt::canonical(7);
        let budget = UniquePayloadBudget::new(5);
        let read = budget.reserve(attempt, &attestation(0, 5)).unwrap();
        let duplicate = budget.reserve(attempt, &attestation(0, 5)).unwrap();
        let calls = AtomicUsize::new(0);

        let first = read.bytes.get_or_try_init(|| async {
            calls.fetch_add(1, Ordering::AcqRel);
            tokio::task::yield_now().await;
            Ok::<Bytes, DbError>(Bytes::from_static(b"state"))
        });
        let second = duplicate.bytes.get_or_try_init(|| async {
            calls.fetch_add(1, Ordering::AcqRel);
            Ok::<Bytes, DbError>(Bytes::from_static(b"state"))
        });
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.unwrap(), &Bytes::from_static(b"state"));
        assert_eq!(second.unwrap(), &Bytes::from_static(b"state"));
        assert_eq!(calls.load(Ordering::Acquire), 1);

        let retry_budget = UniquePayloadBudget::new(5);
        let retry = retry_budget.reserve(attempt, &attestation(0, 5)).unwrap();
        let error = retry
            .bytes
            .get_or_try_init(|| async {
                Err::<Bytes, DbError>(DbError::Checkpoint("transient read failure".into()))
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("transient read failure"));
        assert!(retry.bytes.get().is_none());

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let cancelled_read = Arc::clone(&retry);
        let cancelled = tokio::spawn(async move {
            cancelled_read
                .bytes
                .get_or_try_init(|| async move {
                    let _ = started_tx.send(());
                    std::future::pending::<Result<Bytes, DbError>>().await
                })
                .await
                .map(Bytes::clone)
        });
        started_rx.await.unwrap();
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        assert!(retry.bytes.get().is_none());
        assert_eq!(retry_budget.state.lock().reserved_bytes, 5);

        let bytes = retry
            .bytes
            .get_or_try_init(|| async { Ok::<Bytes, DbError>(Bytes::from_static(b"state")) })
            .await
            .unwrap();
        assert_eq!(bytes, &Bytes::from_static(b"state"));
        assert_eq!(retry_budget.state.lock().reserved_bytes, 5);
    }
}
