//! CAS-backed leader lease with TTL and a monotonic fencing token.
//! One object per sequence at `control/leader-lease/v{seq:016}.json`,
//! written with `PutMode::Create` so the CAS works on every backend
//! (`LocalFileSystem` included). The lease prevents split-brain: a
//! stale leader whose lease has expired loses the CAS to whoever
//! acquires next, and the fencing `token` advances only on an owner
//! change so followers can reject writes carrying a stale token.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_stream::StreamExt;

use crate::cluster::discovery::NodeId;

const LEASE_PREFIX: &str = "control/leader-lease/";

fn lease_path(seq: u64) -> OsPath {
    // Fixed-width so lexicographic list order matches numeric order.
    OsPath::from(format!("{LEASE_PREFIX}v{seq:016}.json"))
}

// Used only by the renewal loop, which is cluster-gated.
#[cfg(feature = "cluster-unstable")]
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// A durable leader lease record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderLease {
    /// CAS sequence; bumps on every write.
    pub seq: u64,
    /// Fencing token; bumps only when ownership changes.
    pub token: u64,
    /// Current lease holder.
    pub owner: NodeId,
    /// Expiry, millis since epoch. The lease is dead once `now >= this`.
    pub expires_at_ms: i64,
}

impl LeaderLease {
    /// Whether the lease has expired as of `now_ms`.
    #[must_use]
    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expires_at_ms <= now_ms
    }
}

/// Result of a [`LeaderLeaseStore::try_acquire`] attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseOutcome {
    /// We now hold the lease (fresh acquire or renew).
    Acquired(LeaderLease),
    /// A live lease is held by someone else; the attached record is the
    /// current durable lease.
    Held(LeaderLease),
}

/// Errors loading or saving a [`LeaderLease`].
#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    /// Underlying object store I/O failure.
    #[error("object store I/O: {0}")]
    Io(String),
    /// JSON de/serialization failure.
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
}

/// I/O wrapper for [`LeaderLease`] on an object store.
pub struct LeaderLeaseStore {
    store: Arc<dyn ObjectStore>,
    ttl_ms: i64,
}

impl std::fmt::Debug for LeaderLeaseStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderLeaseStore")
            .field("ttl_ms", &self.ttl_ms)
            .finish_non_exhaustive()
    }
}

impl LeaderLeaseStore {
    /// Wrap a pre-constructed object store with the given lease TTL.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, ttl_ms: i64) -> Self {
        Self { store, ttl_ms }
    }

    /// Scan the lease prefix and return every stored sequence in
    /// ascending order.
    async fn list_seqs(&self) -> Result<Vec<u64>, LeaseError> {
        let prefix = OsPath::from(LEASE_PREFIX);
        let mut entries = self.store.list(Some(&prefix));
        let mut seqs: Vec<u64> = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|e| LeaseError::Io(e.to_string()))?;
            let loc = entry.location.as_ref();
            let Some(rest) = loc.strip_prefix(LEASE_PREFIX) else {
                continue;
            };
            let Some(num) = rest.strip_prefix('v').and_then(|s| s.strip_suffix(".json")) else {
                continue;
            };
            if let Ok(s) = num.parse::<u64>() {
                seqs.push(s);
            }
        }
        seqs.sort_unstable();
        Ok(seqs)
    }

    /// Load the current (highest-seq) lease; `Ok(None)` if none exists.
    ///
    /// # Errors
    /// Object-store I/O or JSON decode failure.
    pub async fn load(&self) -> Result<Option<LeaderLease>, LeaseError> {
        let seqs = self.list_seqs().await?;
        let Some(&latest) = seqs.last() else {
            return Ok(None);
        };
        self.load_seq(latest).await
    }

    /// Load a specific sequence's lease. `Ok(None)` if it was never
    /// written.
    ///
    /// # Errors
    /// Object-store I/O or JSON decode failure.
    pub async fn load_seq(&self, seq: u64) -> Result<Option<LeaderLease>, LeaseError> {
        let path = lease_path(seq);
        match self.store.get(&path).await {
            Ok(res) => {
                let bytes = res
                    .bytes()
                    .await
                    .map_err(|e| LeaseError::Io(e.to_string()))?;
                let lease = serde_json::from_slice(&bytes)?;
                Ok(Some(lease))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(LeaseError::Io(e.to_string())),
        }
    }

    /// Attempt to acquire or renew the lease for `me` as of `now_ms`.
    ///
    /// Takes `now_ms` explicitly so the decision stays deterministic and
    /// unit-testable; the renewal manager supplies wall-clock time.
    ///
    /// # Errors
    /// Object-store I/O or JSON encode failure.
    pub async fn try_acquire(&self, me: NodeId, now_ms: i64) -> Result<LeaseOutcome, LeaseError> {
        let cur = self.load().await?;
        let candidate = match cur {
            None => LeaderLease {
                seq: 1,
                token: 1,
                owner: me,
                expires_at_ms: now_ms + self.ttl_ms,
            },
            Some(ref cur) if cur.owner == me || cur.is_expired(now_ms) => {
                let token = if cur.owner == me {
                    cur.token
                } else {
                    cur.token + 1
                };
                LeaderLease {
                    seq: cur.seq + 1,
                    token,
                    owner: me,
                    expires_at_ms: now_ms + self.ttl_ms,
                }
            }
            // Live lease held by another node; back off.
            Some(cur) => return Ok(LeaseOutcome::Held(cur)),
        };

        let path = lease_path(candidate.seq);
        let bytes = serde_json::to_vec_pretty(&candidate)?;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&path, PutPayload::from(Bytes::from(bytes)), opts)
            .await
        {
            Ok(_) => Ok(LeaseOutcome::Acquired(candidate)),
            // A racer landed at our seq first. A single reload settles
            // it; force the caller to re-evaluate next tick rather than
            // spin here.
            Err(object_store::Error::AlreadyExists { .. }) => {
                let latest = self.load().await?.ok_or_else(|| {
                    LeaseError::Io("CAS conflict but load of winner returned None".into())
                })?;
                Ok(LeaseOutcome::Held(latest))
            }
            Err(e) => Err(LeaseError::Io(e.to_string())),
        }
    }
}

/// Tunables for the lease renewal loop.
#[derive(Debug, Clone, Copy)]
pub struct LeaderLeaseConfig {
    /// Lease lifetime; a lease is dead once `now >= expires_at_ms`.
    pub ttl: Duration,
    /// How often the manager re-acquires/renews. Must be well under
    /// `ttl` so the holder renews before expiry.
    pub renew_interval: Duration,
}

impl Default for LeaderLeaseConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(5),
            renew_interval: Duration::from_secs(2),
        }
    }
}

/// True iff `lease` is held by `me` and not expired at `now_ms`.
#[must_use]
pub fn lease_grants_leadership(lease: &Option<LeaderLease>, me: NodeId, now_ms: i64) -> bool {
    matches!(lease, Some(l) if l.owner == me && !l.is_expired(now_ms))
}

/// Periodically renews the leader lease and publishes the latest record
/// on a watch channel so other tasks can gate on the fencing token.
pub struct LeaderLeaseManager {
    store: Arc<LeaderLeaseStore>,
    me: NodeId,
    config: LeaderLeaseConfig,
    tx: watch::Sender<Option<LeaderLease>>,
}

impl std::fmt::Debug for LeaderLeaseManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderLeaseManager")
            .field("me", &self.me)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl LeaderLeaseManager {
    /// Build a manager. The watch starts at `None` until the first tick.
    #[must_use]
    pub fn new(store: Arc<LeaderLeaseStore>, me: NodeId, config: LeaderLeaseConfig) -> Self {
        let (tx, _rx) = watch::channel(None);
        Self {
            store,
            me,
            config,
            tx,
        }
    }

    /// Subscribe to the latest observed lease.
    #[must_use]
    pub fn lease_watch(&self) -> watch::Receiver<Option<LeaderLease>> {
        self.tx.subscribe()
    }

    /// Spawn the renewal loop. Every `renew_interval` it `try_acquire`s
    /// and publishes the resulting lease — `Acquired` when we own it,
    /// otherwise the `Held` record so followers learn the current
    /// fencing token. Errors are logged and retried next tick. Stops
    /// when `shutdown` is cancelled.
    #[cfg(feature = "cluster-unstable")]
    #[must_use]
    pub fn spawn(
        self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.config.renew_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    _ = ticker.tick() => {}
                }
                match self.store.try_acquire(self.me, now_millis()).await {
                    Ok(LeaseOutcome::Acquired(lease) | LeaseOutcome::Held(lease)) => {
                        self.tx.send_replace(Some(lease));
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "leader lease renewal failed");
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn store(ttl_ms: i64) -> LeaderLeaseStore {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        LeaderLeaseStore::new(mem, ttl_ms)
    }

    #[tokio::test]
    async fn first_acquire_on_empty_store() {
        let s = store(1_000);
        let me = NodeId(1);
        match s.try_acquire(me, 0).await.unwrap() {
            LeaseOutcome::Acquired(l) => {
                assert_eq!(l.seq, 1);
                assert_eq!(l.token, 1);
                assert_eq!(l.owner, me);
                assert_eq!(l.expires_at_ms, 1_000);
            }
            LeaseOutcome::Held(_) => panic!("empty store must yield Acquired"),
        }
    }

    #[tokio::test]
    async fn same_owner_renew_keeps_token() {
        let s = store(1_000);
        let me = NodeId(1);
        s.try_acquire(me, 0).await.unwrap();
        match s.try_acquire(me, 500).await.unwrap() {
            LeaseOutcome::Acquired(l) => {
                assert_eq!(l.seq, 2, "seq bumps on every write");
                assert_eq!(l.token, 1, "same owner keeps fencing token");
                assert_eq!(l.expires_at_ms, 1_500, "expiry extended");
            }
            LeaseOutcome::Held(_) => panic!("owner renewal must be Acquired"),
        }
    }

    #[tokio::test]
    async fn different_node_blocked_while_live() {
        let s = store(1_000);
        let owner = NodeId(1);
        s.try_acquire(owner, 0).await.unwrap();
        // Challenger arrives before expiry.
        match s.try_acquire(NodeId(2), 500).await.unwrap() {
            LeaseOutcome::Held(l) => {
                assert_eq!(l.owner, owner, "live lease keeps its owner");
                assert_eq!(l.token, 1);
            }
            LeaseOutcome::Acquired(_) => panic!("must not steal a live lease"),
        }
    }

    #[tokio::test]
    async fn different_node_takes_over_after_expiry() {
        let s = store(1_000);
        let owner = NodeId(1);
        s.try_acquire(owner, 0).await.unwrap();
        // Challenger arrives at expiry boundary (>= expires_at_ms).
        match s.try_acquire(NodeId(2), 1_000).await.unwrap() {
            LeaseOutcome::Acquired(l) => {
                assert_eq!(l.owner, NodeId(2));
                assert_eq!(l.token, 2, "owner change bumps fencing token");
                assert_eq!(l.seq, 2);
            }
            LeaseOutcome::Held(_) => panic!("expired lease must be acquirable"),
        }
    }

    #[tokio::test]
    async fn grants_leadership_only_for_live_owner() {
        let me = NodeId(1);
        let live = Some(LeaderLease {
            seq: 1,
            token: 1,
            owner: me,
            expires_at_ms: 1_000,
        });
        assert!(lease_grants_leadership(&live, me, 0));
        assert!(!lease_grants_leadership(&live, me, 1_000), "expired");
        assert!(!lease_grants_leadership(&live, NodeId(2), 0), "not owner");
        assert!(!lease_grants_leadership(&None, me, 0), "no lease");
    }

    #[tokio::test]
    async fn pre_seeded_live_incumbent_blocks_challenger() {
        // Pre-seed seq=1 directly (as a racer's write would land), then a
        // challenger sees the live incumbent on load and backs off.
        let s = store(10_000);
        let incumbent = LeaderLease {
            seq: 1,
            token: 1,
            owner: NodeId(9),
            expires_at_ms: 10_000,
        };
        let bytes = serde_json::to_vec_pretty(&incumbent).unwrap();
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        s.store
            .put_opts(&lease_path(1), PutPayload::from(Bytes::from(bytes)), opts)
            .await
            .unwrap();

        match s.try_acquire(NodeId(2), 0).await.unwrap() {
            LeaseOutcome::Held(l) => assert_eq!(l, incumbent),
            LeaseOutcome::Acquired(_) => panic!("live incumbent must block"),
        }
    }
}
