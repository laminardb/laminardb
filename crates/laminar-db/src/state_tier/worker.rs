//! The cold tier's off-compute worker.
//!
//! The compute thread `try_send`s requests onto a bounded channel; this
//! worker (a task on the main runtime) services them one at a time via
//! `spawn_blocking`, so a synchronous fjall call never stalls compute.
//! Single-flight serializes tier I/O as the demotion/promotion protocol
//! expects.
//!
//! Requests use a `crossfire` channel; replies stay `tokio::oneshot`
//! because the promotion operator stores reply receivers in a field and so
//! must be `Sync`, which crossfire's `!Sync` `RxOneshot` is not.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::oneshot;

use super::StateTierStore;
use crate::error::DbError;

/// Multi-producer sender for cold-tier requests.
pub(crate) type TierTx = crossfire::MAsyncTx<crossfire::mpsc::Array<TierRequest>>;
type TierRx = crossfire::AsyncRx<crossfire::mpsc::Array<TierRequest>>;

/// One request to the cold tier. Demotion awaits the reply before releasing memory;
/// Drop callers may ignore it for best-effort cleanup.
pub(crate) enum TierRequest {
    Demote {
        operator: Arc<str>,
        vnode: u32,
        bytes: Bytes,
        reply: oneshot::Sender<Result<(), DbError>>,
    },
    Fetch {
        operator: Arc<str>,
        vnode: u32,
        reply: oneshot::Sender<Result<Option<Bytes>, DbError>>,
    },
    Drop {
        operator: Arc<str>,
        vnode: u32,
        reply: oneshot::Sender<Result<(), DbError>>,
    },
}

/// Spawn the worker on `runtime` and return its request channel.
/// The channel is bounded; compute-thread submitters must use `try_send` and back off on full.
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
