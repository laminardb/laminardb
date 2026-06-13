//! The cold tier's off-compute worker.
//!
//! Mirrors the lookup-enrich / AI-operator decoupling: the compute thread
//! `try_send`s requests into a bounded channel and drains replies on later
//! cycles; this worker (a task on the main tokio runtime, never the compute
//! thread) services them one at a time, pushing each synchronous fjall call
//! into `spawn_blocking`. Single-flight is deliberate — it serializes tier
//! I/O the way the demotion/promotion protocol expects, and matches the
//! dev-box finding that read tails degrade under concurrent write pressure.
//!
//! The request channel is `crossfire` (a multi-producer mpsc, like the
//! engine's other compute-thread/runtime plumbing). The per-request replies
//! stay `tokio::oneshot`: the promotion operator stores its in-flight reply
//! receivers in a field, and that operator must be `Sync` (its `process`
//! future holds `&self` across an await), which crossfire's `!Sync`
//! `RxOneshot` cannot satisfy — the same reason the lookup-enrich and AI
//! workers store tokio receivers.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::oneshot;

use super::StateTierStore;
use crate::error::DbError;

/// Multi-producer sender for cold-tier requests — the compute thread
/// (promotion), the coordinator (forced-full re-uploads), and the pipeline
/// callback (demotion) all submit through one channel.
pub(crate) type TierTx = crossfire::MAsyncTx<crossfire::mpsc::Array<TierRequest>>;
type TierRx = crossfire::AsyncRx<crossfire::mpsc::Array<TierRequest>>;

/// One request to the tier. Every variant carries a reply channel: even
/// fire-and-forget callers need the completion signal before they may act
/// on it (a demotion must not drop groups from memory until the slice is
/// confirmed written).
pub(crate) enum TierRequest {
    /// Write a demoted slice.
    Demote {
        operator: Arc<str>,
        vnode: u32,
        bytes: Bytes,
        reply: oneshot::Sender<Result<(), DbError>>,
    },
    /// Read a slice for promotion.
    Fetch {
        operator: Arc<str>,
        vnode: u32,
        reply: oneshot::Sender<Result<Option<Bytes>, DbError>>,
    },
    /// Drop a promoted (or released) slice.
    Drop {
        operator: Arc<str>,
        vnode: u32,
        reply: oneshot::Sender<Result<(), DbError>>,
    },
}

/// Spawn the worker on `runtime` and return its request channel.
///
/// The channel is bounded; submitters on the compute thread must use
/// `try_send` and treat a full channel as backpressure (defer and retry
/// next cycle), never block.
pub(crate) fn spawn_worker(
    runtime: &tokio::runtime::Handle,
    store: Arc<StateTierStore>,
    queue_capacity: usize,
) -> TierTx {
    let (tx, rx) = crossfire::mpsc::bounded_async(queue_capacity);
    runtime.spawn(run_worker(store, rx));
    tx
}

async fn run_worker(store: Arc<StateTierStore>, rx: TierRx) {
    // `recv` errors once every sender has dropped (pipeline shutdown).
    while let Ok(req) = rx.recv().await {
        let store = Arc::clone(&store);
        match req {
            TierRequest::Demote {
                operator,
                vnode,
                bytes,
                reply,
            } => {
                let res = tokio::task::spawn_blocking(move || {
                    store.put(operator.as_ref(), vnode, &bytes)
                })
                .await
                .unwrap_or_else(|e| Err(DbError::Storage(format!("state tier worker: {e}"))));
                let _ = reply.send(res);
            }
            TierRequest::Fetch {
                operator,
                vnode,
                reply,
            } => {
                let res = tokio::task::spawn_blocking(move || store.get(operator.as_ref(), vnode))
                    .await
                    .unwrap_or_else(|e| Err(DbError::Storage(format!("state tier worker: {e}"))));
                let _ = reply.send(res);
            }
            TierRequest::Drop {
                operator,
                vnode,
                reply,
            } => {
                let res =
                    tokio::task::spawn_blocking(move || store.remove(operator.as_ref(), vnode))
                        .await
                        .unwrap_or_else(|e| {
                            Err(DbError::Storage(format!("state tier worker: {e}")))
                        });
                let _ = reply.send(res);
            }
        }
    }
}
