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
    /// Store one demoted group (v2 group granularity).
    DemoteGroup {
        operator: Arc<str>,
        vnode: u32,
        group: Vec<u8>,
        bytes: Bytes,
        reply: oneshot::Sender<Result<(), DbError>>,
    },
    /// Fetch one demoted group for promotion (v2 group granularity).
    FetchGroup {
        operator: Arc<str>,
        vnode: u32,
        group: Vec<u8>,
        reply: oneshot::Sender<Result<Option<Bytes>, DbError>>,
    },
    /// Drop one demoted group after promotion (v2 group granularity).
    DropGroup {
        operator: Arc<str>,
        vnode: u32,
        group: Vec<u8>,
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
            } => dispatch(reply, move || store.put(operator.as_ref(), vnode, &bytes)).await,
            TierRequest::Fetch {
                operator,
                vnode,
                reply,
            } => dispatch(reply, move || store.get(operator.as_ref(), vnode)).await,
            TierRequest::Drop {
                operator,
                vnode,
                reply,
            } => dispatch(reply, move || store.remove(operator.as_ref(), vnode)).await,
            TierRequest::DemoteGroup {
                operator,
                vnode,
                group,
                bytes,
                reply,
            } => {
                dispatch(reply, move || {
                    store.put_group(operator.as_ref(), vnode, &group, &bytes)
                })
                .await;
            }
            TierRequest::FetchGroup {
                operator,
                vnode,
                group,
                reply,
            } => {
                dispatch(reply, move || {
                    store.get_group(operator.as_ref(), vnode, &group)
                })
                .await;
            }
            TierRequest::DropGroup {
                operator,
                vnode,
                group,
                reply,
            } => {
                dispatch(reply, move || {
                    store.remove_group(operator.as_ref(), vnode, &group)
                })
                .await;
            }
        }
    }
}

/// Run one blocking tier op off the async worker and reply, mapping a join error uniformly.
async fn dispatch<T: Send + 'static>(
    reply: oneshot::Sender<Result<T, DbError>>,
    f: impl FnOnce() -> Result<T, DbError> + Send + 'static,
) {
    let res = tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|e| Err(DbError::Storage(format!("state tier worker: {e}"))));
    let _ = reply.send(res);
}
