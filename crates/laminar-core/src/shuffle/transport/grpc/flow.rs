//! Outbound byte reservation and enqueue, and what they report: how long sends to each peer
//! waited, what they carried, and how much of each node byte budget is held.

use std::time::{Duration, Instant};

use tokio::sync::TryAcquireError;

use super::*;

/// Totals of one peer's outbound shuffle stream since its sender was created. They survive
/// reconnects to the peer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShufflePeerFlow {
    /// The peer's node id.
    pub peer: ShufflePeerId,
    /// Time sends waited for the peer's or the node's outbound byte budget, or for a slot in the
    /// peer's stream queue. Sends to one peer take turns and the turn is not counted, so this
    /// tracks how long the peer's stream was blocked. A send cancelled while waiting still counts.
    pub send_wait: Duration,
    /// Arrow IPC payload bytes of data frames queued to the peer's stream; control frames are
    /// excluded.
    pub sent_bytes: u64,
    /// Rows of those data frames.
    pub sent_rows: u64,
}

#[derive(Default)]
pub(super) struct PeerSendCounters {
    waited_ns: AtomicU64,
    sent_bytes: AtomicU64,
    sent_rows: AtomicU64,
}

/// Adds its lifetime to the peer's wait on drop, so every exit of a wait is counted.
struct WaitClock<'a> {
    counters: &'a PeerSendCounters,
    since: Instant,
}

impl Drop for WaitClock<'_> {
    fn drop(&mut self) {
        let waited = u64::try_from(self.since.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.counters.waited_ns.fetch_add(waited, Ordering::Relaxed);
    }
}

impl PeerSendCounters {
    fn start_wait(&self) -> WaitClock<'_> {
        WaitClock {
            counters: self,
            since: Instant::now(),
        }
    }
}

pub(super) struct OutboundReservation {
    pub(super) peer: OwnedSemaphorePermit,
    pub(super) node: OwnedSemaphorePermit,
}

impl OutboundReservation {
    pub(super) fn shrink_to(&mut self, retained_bytes: usize) -> io::Result<()> {
        let peer_bytes = self.peer.num_permits();
        let node_bytes = self.node.num_permits();
        if peer_bytes != node_bytes || retained_bytes > peer_bytes {
            return Err(io::Error::other(
                "shuffle outbound reservation accounting mismatch",
            ));
        }
        let release = peer_bytes - retained_bytes;
        if release != 0 {
            let peer = self
                .peer
                .split(release)
                .expect("validated outbound peer reservation split");
            let node = self
                .node
                .split(release)
                .expect("validated outbound node reservation split");
            drop((peer, node));
        }
        Ok(())
    }
}

/// Only a grant that cannot be made at once reads the clock.
async fn acquire(
    budget: &Arc<Semaphore>,
    permits: u32,
    cancel: &CancellationToken,
    counters: &PeerSendCounters,
    closed: &'static str,
) -> io::Result<OwnedSemaphorePermit> {
    if cancel.is_cancelled() {
        return Err(scope_cancelled_io());
    }
    match Arc::clone(budget).try_acquire_many_owned(permits) {
        Ok(permit) => return Ok(permit),
        Err(TryAcquireError::Closed) => {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, closed));
        }
        Err(TryAcquireError::NoPermits) => {}
    }
    let _wait = counters.start_wait();
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(scope_cancelled_io()),
        permit = Arc::clone(budget).acquire_many_owned(permits) => {
            permit.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, closed))
        }
    }
}

impl ShuffleSender {
    /// Per-peer totals, ascending by peer.
    #[must_use]
    pub fn peer_flow(&self) -> Vec<ShufflePeerFlow> {
        let mut flows = self
            .peer_counters
            .lock()
            .iter()
            .map(|(&peer, counters)| ShufflePeerFlow {
                peer,
                send_wait: Duration::from_nanos(counters.waited_ns.load(Ordering::Relaxed)),
                sent_bytes: counters.sent_bytes.load(Ordering::Relaxed),
                sent_rows: counters.sent_rows.load(Ordering::Relaxed),
            })
            .collect::<Vec<_>>();
        flows.sort_unstable_by_key(|flow| flow.peer);
        flows
    }

    /// Bytes of the node's outbound data budget held by frames being encoded or queued,
    /// including the share already granted to a send still waiting for the rest. The separate
    /// checkpoint-barrier budget is excluded.
    #[must_use]
    pub fn outbound_budget_used_bytes(&self) -> usize {
        OUTBOUND_NODE_BUDGET_BYTES.saturating_sub(self.node_budget.available_permits())
    }

    pub(super) fn counters_for(&self, peer: ShufflePeerId) -> Arc<PeerSendCounters> {
        Arc::clone(self.peer_counters.lock().entry(peer).or_default())
    }

    /// Reserve the peer's and then the node's byte budget: the control budgets for a barrier,
    /// the data budgets otherwise.
    pub(super) async fn reserve_outbound(
        &self,
        conn: &PeerConn,
        msg: &ShuffleMessage,
        admission_bytes: usize,
        cancel: &CancellationToken,
    ) -> io::Result<OutboundReservation> {
        let permits = u32::try_from(admission_bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "shuffle admission is too large",
            )
        })?;
        let (peer_budget, node_budget) = if matches!(msg, ShuffleMessage::Barrier(_)) {
            (
                &conn.control_byte_budget,
                &self.checkpointed_control_node_budget,
            )
        } else {
            (&conn.byte_budget, &self.node_budget)
        };
        let counters = &conn.counters;
        let peer = acquire(
            peer_budget,
            permits,
            cancel,
            counters,
            "shuffle byte budget closed",
        )
        .await?;
        let node = acquire(
            node_budget,
            permits,
            cancel,
            counters,
            "shuffle node byte budget closed",
        )
        .await?;
        Ok(OutboundReservation { peer, node })
    }

    /// Queue `out` on the peer's ordered stream, then count what it carried.
    pub(super) async fn enqueue(
        &self,
        conn: &PeerConn,
        peer: ShufflePeerId,
        msg: &ShuffleMessage,
        out: Outbound,
        cancel: &CancellationToken,
    ) -> io::Result<()> {
        let sent_bytes = match &out.msg {
            PreparedMessage::Data { arrow_ipc, .. } => arrow_ipc.len(),
            PreparedMessage::Barrier(_) | PreparedMessage::Frontier { .. } => 0,
        };
        match conn.tx.try_send(out) {
            Ok(()) => {}
            Err(crossfire::TrySendError::Full(out)) => {
                let _wait = conn.counters.start_wait();
                tokio::select! {
                    biased;
                    () = self.process_lease.wait_until_lost() => {
                        return Err(process_lease_expired_io());
                    }
                    () = cancel.cancelled() => return Err(scope_cancelled_io()),
                    result = conn.tx.send(out) => result.map_err(|_| io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        format!("shuffle stream to peer {peer} closed"),
                    ))?,
                }
            }
            Err(crossfire::TrySendError::Disconnected(_)) => {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("shuffle stream to peer {peer} closed"),
                ));
            }
        }
        if let ShuffleMessage::Data { batch, .. } = msg {
            let counters = &conn.counters;
            counters
                .sent_bytes
                .fetch_add(sent_bytes as u64, Ordering::Relaxed);
            counters
                .sent_rows
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        }
        Ok(())
    }
}

impl ShuffleReceiver {
    /// Bytes of the node's inbound budget held by received frames until the operator reading
    /// them has consumed them.
    #[must_use]
    pub fn inbound_budget_used_bytes(&self) -> usize {
        INBOUND_NODE_BUDGET_BYTES.saturating_sub(self.inbound_budget.node.available_permits())
    }
}
