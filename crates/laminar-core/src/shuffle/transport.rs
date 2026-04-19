//! TCP shuffle: a per-peer connection pool for senders, an accept loop
//! for receivers. Each frame carries a node id in its handshake so the
//! receiver can attribute incoming traffic. See
//! `docs/plans/shuffle-protocol.md` for the wire format.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rustc_hash::FxHashMap;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;

use super::message::{read_message, write_message, ShuffleMessage};
use crate::checkpoint::barrier::CheckpointBarrier;

#[cfg(feature = "cluster-unstable")]
use crate::cluster::control::ClusterKv;

/// Gossip KV key used by [`ShuffleReceiver::bind_with_kv`] to publish
/// the listener's socket address, and by [`ShuffleSender`] to discover
/// peer addresses on first contact. Value: the bound socket address
/// formatted via `SocketAddr::to_string()`.
#[cfg(feature = "cluster-unstable")]
pub const SHUFFLE_ADDR_KEY: &str = "shuffle:addr";

/// Peer-local identifier on the wire. Matches
/// `cluster::discovery::NodeId`'s inner type for seamless conversion.
pub type ShufflePeerId = u64;

/// One active TCP connection in the shuffle fabric. Internal to the
/// transport — callers hold a [`ShuffleSender`] or [`ShuffleReceiver`].
struct ShuffleConnection {
    /// Write half. Parked behind a mutex so multiple operators can
    /// share one connection without interleaving frames.
    writer: Mutex<tokio::io::WriteHalf<TcpStream>>,
    /// The reader task. Kept so dropping the connection cancels it.
    reader: JoinHandle<()>,
    /// Liveness flag shared with the reader task. Flipped to `false`
    /// when the reader exits (peer closed the socket cleanly OR an
    /// IO error hit). [`ShuffleSender::connection_for`] consults it
    /// before handing out the cached `Arc`; dead entries are purged
    /// and the next send reconnects. Phase 2.2.
    alive: Arc<AtomicBool>,
}

impl ShuffleConnection {
    async fn send(&self, msg: &ShuffleMessage) -> io::Result<()> {
        let mut w = self.writer.lock().await;
        write_message(&mut *w, msg).await
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }
}

impl Drop for ShuffleConnection {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

/// Lazy pool of outbound connections, keyed by peer id. Addresses go
/// in via `register_peer` (manual) or via the KV on first send.
pub struct ShuffleSender {
    local_id: ShufflePeerId,
    peers: RwLock<FxHashMap<ShufflePeerId, SocketAddr>>,
    pool: RwLock<FxHashMap<ShufflePeerId, Arc<ShuffleConnection>>>,
    #[cfg(feature = "cluster-unstable")]
    kv: Option<Arc<dyn ClusterKv>>,
}

impl std::fmt::Debug for ShuffleSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShuffleSender")
            .field("local_id", &self.local_id)
            .finish_non_exhaustive()
    }
}

impl ShuffleSender {
    /// Empty sender. Peers must be added via `register_peer` or
    /// discovered via the KV in `with_kv` before any `send_to`.
    #[must_use]
    pub fn new(local_id: ShufflePeerId) -> Self {
        Self {
            local_id,
            peers: RwLock::new(FxHashMap::default()),
            pool: RwLock::new(FxHashMap::default()),
            #[cfg(feature = "cluster-unstable")]
            kv: None,
        }
    }

    /// Construct a sender that falls back to `kv` when `send_to` is
    /// called for a peer not previously registered. The KV is read
    /// from the peer's own state at [`SHUFFLE_ADDR_KEY`].
    #[cfg(feature = "cluster-unstable")]
    #[must_use]
    pub fn with_kv(local_id: ShufflePeerId, kv: Arc<dyn ClusterKv>) -> Self {
        let mut s = Self::new(local_id);
        s.kv = Some(kv);
        s
    }

    /// Register (or update) a peer's shuffle address. Must be called
    /// before `send_to(peer, ..)`.
    pub async fn register_peer(&self, peer: ShufflePeerId, addr: SocketAddr) {
        self.peers.write().await.insert(peer, addr);
    }

    /// Send `msg` to `peer`, opening a connection if necessary.
    ///
    /// # Errors
    /// Returns `io::Error` on connect failure, peer-unregistered, or
    /// frame write failure.
    pub async fn send_to(&self, peer: ShufflePeerId, msg: &ShuffleMessage) -> io::Result<()> {
        let conn = self.connection_for(peer).await?;
        conn.send(msg).await
    }

    /// Ship `barrier` to every peer in order. Short-circuits on the
    /// first send failure; the gossip side-channel is the
    /// authoritative announcement so a partial fan-out is tolerable.
    ///
    /// # Errors
    /// Returns the first `io::Error` from any peer's `send_to`.
    pub async fn fan_out_barrier(
        &self,
        peers: &[ShufflePeerId],
        barrier: CheckpointBarrier,
    ) -> io::Result<()> {
        let msg = ShuffleMessage::Barrier(barrier);
        for &peer in peers {
            self.send_to(peer, &msg).await?;
        }
        Ok(())
    }

    /// Look up `peer`'s shuffle address from the cluster KV and
    /// register it on success. Returns `None` when no KV is attached,
    /// the peer has no entry yet, or the entry can't be parsed.
    #[cfg(feature = "cluster-unstable")]
    async fn discover_peer(&self, peer: ShufflePeerId) -> Option<SocketAddr> {
        let kv = self.kv.as_ref()?;
        let raw = kv
            .read_from(
                crate::cluster::discovery::NodeId(peer),
                SHUFFLE_ADDR_KEY,
            )
            .await?;
        let addr: SocketAddr = raw.parse().ok()?;
        self.peers.write().await.insert(peer, addr);
        Some(addr)
    }

    async fn connection_for(&self, peer: ShufflePeerId) -> io::Result<Arc<ShuffleConnection>> {
        // Fast path: hand out the cached connection if its reader task
        // is still alive. A dead connection is one whose reader has
        // exited (peer closed or IO error) — the socket's write half is
        // useless even though the `Arc` still exists.
        if let Some(existing) = self.pool.read().await.get(&peer).cloned() {
            if existing.is_alive() {
                return Ok(existing);
            }
        }

        // Purge the dead pool entry so we reconnect below. We do NOT
        // touch `peers` here: the caller (gossip watcher / explicit
        // `register_peer`) owns the address mapping. If the cached
        // address is stale, `TcpStream::connect` will surface that
        // as a connect error and the caller retries after re-register;
        // if callers use KV discovery, a missing `peers` entry triggers
        // `discover_peer` in the next block. Phase 2.2.
        {
            let mut pool = self.pool.write().await;
            if let Some(c) = pool.get(&peer) {
                if !c.is_alive() {
                    pool.remove(&peer);
                }
            }
        }

        let addr = if let Some(a) = self.peers.read().await.get(&peer).copied() {
            a
        } else {
            #[cfg(feature = "cluster-unstable")]
            let discovered = self.discover_peer(peer).await;
            #[cfg(not(feature = "cluster-unstable"))]
            let discovered: Option<SocketAddr> = None;
            discovered.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("peer {peer} has no registered shuffle address"),
                )
            })?
        };

        // Open + handshake without holding the pool write lock.
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (mut reader_half, mut writer_half) = tokio::io::split(stream);
        write_message(&mut writer_half, &ShuffleMessage::Hello(self.local_id)).await?;

        // Outbound connection's read half: drain frames until the peer
        // closes or the socket errors. Nothing currently acts on reply
        // traffic on the outbound side (credit frames are handled by
        // the inbound `ShuffleReceiver` on the other instance), so we
        // just discard. Flip `alive` to `false` on exit so the next
        // `connection_for` call evicts this entry and reconnects.
        let alive = Arc::new(AtomicBool::new(true));
        let alive_for_reader = Arc::clone(&alive);
        let reader = tokio::spawn(async move {
            let _ = peer;
            loop {
                match read_message(&mut reader_half).await {
                    Ok(ShuffleMessage::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            alive_for_reader.store(false, Ordering::Release);
        });

        let conn = Arc::new(ShuffleConnection {
            writer: Mutex::new(writer_half),
            reader,
            alive,
        });

        // Race: another task may have created a connection in the
        // meantime. Cheap to discard ours — but only if the winner is
        // still alive. A dead winner (raced with an earlier reader
        // exit) must be superseded.
        let mut pool = self.pool.write().await;
        if let Some(winner) = pool.get(&peer).cloned() {
            if winner.is_alive() {
                return Ok(winner);
            }
        }
        pool.insert(peer, Arc::clone(&conn));
        Ok(conn)
    }
}

/// Inbound side of the shuffle fabric.
///
/// Binds a `TcpListener` and surfaces every frame received from any
/// peer — prefixed with that peer's id — on the channel returned by
/// [`Self::subscribe`].
pub struct ShuffleReceiver {
    local_id: ShufflePeerId,
    local_addr: SocketAddr,
    accept: JoinHandle<()>,
    /// Per-peer reader tasks spawned by the accept loop. Tracked so
    /// [`Drop`] can abort them — otherwise detached tasks keep the
    /// socket open, peers never see EOF, and senders can't detect
    /// that we went away. Phase 2.2.
    peer_tasks: Arc<parking_lot::Mutex<Vec<JoinHandle<()>>>>,
    rx: Mutex<mpsc::UnboundedReceiver<(ShufflePeerId, ShuffleMessage)>>,
}

impl Drop for ShuffleReceiver {
    fn drop(&mut self) {
        // Abort accept first so no new peer tasks are spawned, then
        // abort any in-flight peer tasks. Aborting drops each socket,
        // which surfaces as EOF on the sender's reader half — exactly
        // what the stale-connection purge in `connection_for` relies on.
        self.accept.abort();
        for h in self.peer_tasks.lock().drain(..) {
            h.abort();
        }
    }
}

impl std::fmt::Debug for ShuffleReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShuffleReceiver")
            .field("local_id", &self.local_id)
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

impl ShuffleReceiver {
    /// Bind on `addr` and start accepting peer connections. The bound
    /// socket address is surfaced via [`Self::local_addr`] so callers
    /// using an ephemeral port can register themselves with peers.
    ///
    /// # Errors
    /// Returns `io::Error` on bind failure.
    pub async fn bind(local_id: ShufflePeerId, addr: SocketAddr) -> io::Result<Self> {
        Self::bind_impl(local_id, addr).await
    }

    /// Bind + publish the listener's address into `kv` under
    /// [`SHUFFLE_ADDR_KEY`] so that peer [`ShuffleSender`]s can
    /// discover us via [`ShuffleSender::with_kv`].
    ///
    /// # Errors
    /// Returns `io::Error` on bind failure.
    #[cfg(feature = "cluster-unstable")]
    pub async fn bind_with_kv(
        local_id: ShufflePeerId,
        addr: SocketAddr,
        kv: Arc<dyn ClusterKv>,
    ) -> io::Result<Self> {
        let recv = Self::bind_impl(local_id, addr).await?;
        kv.write(SHUFFLE_ADDR_KEY, recv.local_addr.to_string())
            .await;
        Ok(recv)
    }

    async fn bind_impl(local_id: ShufflePeerId, addr: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let (tx, rx) = mpsc::unbounded_channel();

        let peer_tasks: Arc<parking_lot::Mutex<Vec<JoinHandle<()>>>> =
            Arc::new(parking_lot::Mutex::new(Vec::new()));
        let accept = tokio::spawn(Self::accept_loop(
            listener,
            tx,
            Arc::clone(&peer_tasks),
        ));

        Ok(Self {
            local_id,
            local_addr,
            accept,
            peer_tasks,
            rx: Mutex::new(rx),
        })
    }

    /// Local socket address the listener is bound to.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Await the next `(peer_id, msg)` from any connected peer.
    pub async fn recv(&self) -> Option<(ShufflePeerId, ShuffleMessage)> {
        self.rx.lock().await.recv().await
    }

    /// Drain every currently-available `(peer_id, msg)` without blocking.
    /// Returns immediately when the internal queue is empty.
    ///
    /// Used by the row-shuffle aggregator path to pull remote rows into
    /// the current streaming cycle without waiting for more. Uses
    /// `tokio::sync::Mutex::try_lock` so a concurrent `recv()` doesn't
    /// block us — we just skip this tick when contended (next tick picks
    /// up the messages).
    pub fn drain_available(&self) -> Vec<(ShufflePeerId, ShuffleMessage)> {
        let Ok(mut guard) = self.rx.try_lock() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        while let Ok(msg) = guard.try_recv() {
            out.push(msg);
        }
        out
    }

    async fn accept_loop(
        listener: TcpListener,
        tx: mpsc::UnboundedSender<(ShufflePeerId, ShuffleMessage)>,
        peer_tasks: Arc<parking_lot::Mutex<Vec<JoinHandle<()>>>>,
    ) {
        loop {
            let Ok((stream, _peer_addr)) = listener.accept().await else { break };
            if stream.set_nodelay(true).is_err() {
                continue;
            }
            let tx = tx.clone();
            let handle = tokio::spawn(Self::per_peer_loop(stream, tx));
            // Sweep finished tasks so the vec doesn't grow unbounded
            // under a long-lived receiver with churning peers, then
            // track the fresh one so Drop can abort it.
            let mut tasks = peer_tasks.lock();
            tasks.retain(|h| !h.is_finished());
            tasks.push(handle);
        }
    }

    async fn per_peer_loop(
        stream: TcpStream,
        tx: mpsc::UnboundedSender<(ShufflePeerId, ShuffleMessage)>,
    ) {
        let (mut reader_half, _writer_half) = tokio::io::split(stream);
        // Expect Hello first. Anything else means the peer is broken.
        let Ok(ShuffleMessage::Hello(peer)) = read_message(&mut reader_half).await else {
            return;
        };
        loop {
            match read_message(&mut reader_half).await {
                Ok(ShuffleMessage::Close(_)) | Err(_) => break,
                Ok(msg) => {
                    if tx.send((peer, msg)).is_err() {
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn bind_on_loopback(local_id: ShufflePeerId) -> ShuffleReceiver {
        ShuffleReceiver::bind(local_id, "127.0.0.1:0".parse().unwrap())
            .await
            .expect("bind")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sender_to_receiver_delivers_with_peer_attribution() {
        let recv = bind_on_loopback(2).await;
        let recv_addr = recv.local_addr();

        let sender = ShuffleSender::new(1);
        sender.register_peer(2, recv_addr).await;
        sender
            .send_to(2, &ShuffleMessage::Hello(1234))
            .await
            .unwrap();

        let (from, msg) = recv.recv().await.unwrap();
        assert_eq!(from, 1, "receiver attributes frame to sender id");
        assert_eq!(msg, ShuffleMessage::Hello(1234));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sender_reuses_connection_across_sends() {
        let recv = bind_on_loopback(2).await;
        let sender = ShuffleSender::new(1);
        sender.register_peer(2, recv.local_addr()).await;

        for delta in [10u64, 20, 30, 40] {
            sender
                .send_to(2, &ShuffleMessage::Hello(delta))
                .await
                .unwrap();
        }

        let mut got = Vec::new();
        for _ in 0..4 {
            got.push(recv.recv().await.unwrap().1);
        }
        assert_eq!(
            got,
            vec![
                ShuffleMessage::Hello(10),
                ShuffleMessage::Hello(20),
                ShuffleMessage::Hello(30),
                ShuffleMessage::Hello(40),
            ]
        );
        // Pool holds exactly one connection to peer 2.
        assert_eq!(sender.pool.read().await.len(), 1);
    }

    #[cfg(feature = "cluster-unstable")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn discover_peer_reads_registered_address_from_kv() {
        use crate::cluster::control::{ClusterKv, InMemoryKv};
        use crate::cluster::discovery::NodeId;

        // Seed node 2's address into node 1's local KV, then verify
        // `discover_peer` pulls it out and caches it. Covers the
        // discovery glue without involving real TCP.
        let kv = Arc::new(InMemoryKv::new(NodeId(1)));
        kv.seed(NodeId(2), SHUFFLE_ADDR_KEY, "127.0.0.1:54321".into());
        let sender = ShuffleSender::with_kv(1, kv as Arc<dyn ClusterKv>);

        let expected: SocketAddr = "127.0.0.1:54321".parse().unwrap();
        let addr = sender.discover_peer(2).await.expect("peer found");
        assert_eq!(addr, expected);
        assert_eq!(sender.peers.read().await.get(&2).copied(), Some(expected));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_to_unregistered_peer_errors() {
        let sender = ShuffleSender::new(1);
        let err = sender
            .send_to(99, &ShuffleMessage::Hello(1))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    /// Phase 2.2 AC: when a peer restarts at a different address, the
    /// sender's cached connection flips dead (reader exits on EOF), the
    /// next `send_to` purges the stale entry and reconnects against the
    /// freshly-registered address.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_reconnects_after_peer_restart_at_new_address() {
        // 1. Peer binds on an ephemeral port.
        let recv_v1 = bind_on_loopback(2).await;
        let addr_v1 = recv_v1.local_addr();

        let sender = ShuffleSender::new(1);
        sender.register_peer(2, addr_v1).await;

        // 2. First send establishes the pooled connection.
        sender
            .send_to(2, &ShuffleMessage::Hello(111))
            .await
            .unwrap();
        let (from, msg) = recv_v1.recv().await.unwrap();
        assert_eq!(from, 1);
        assert_eq!(msg, ShuffleMessage::Hello(111));

        // Pool has exactly the one connection, and it's alive.
        {
            let pool = sender.pool.read().await;
            assert_eq!(pool.len(), 1, "one pooled connection");
            assert!(pool.get(&2).unwrap().is_alive(), "alive after first send");
        }

        // 3. "Crash" the peer: drop the receiver so the listener and
        //    the per-peer task go away. The sender's reader half will
        //    observe EOF and flip `alive=false`.
        drop(recv_v1);

        // Spin until the reader task notices the shutdown. Bounded
        // wait so a hung test fails loudly instead of running forever.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let alive = {
                let pool = sender.pool.read().await;
                pool.get(&2).is_none_or(|c| c.is_alive())
            };
            if !alive {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "reader task did not flip alive=false within 5s",
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

        // 4. Peer "restarts" on a fresh ephemeral port — almost
        //    certainly different from addr_v1. Re-register its new
        //    address on the sender (mimicking gossip discovery).
        let recv_v2 = bind_on_loopback(2).await;
        let addr_v2 = recv_v2.local_addr();
        assert_ne!(
            addr_v1, addr_v2,
            "ephemeral rebind must pick a different port",
        );
        sender.register_peer(2, addr_v2).await;

        // 5. `send_to` must purge the dead entry and reconnect against
        //    addr_v2. If the purge is broken, we'd either reuse the
        //    dead writer (io error) or dial the stale addr_v1.
        sender
            .send_to(2, &ShuffleMessage::Hello(222))
            .await
            .expect("reconnect after restart");

        let (from, msg) = recv_v2.recv().await.unwrap();
        assert_eq!(from, 1);
        assert_eq!(
            msg,
            ShuffleMessage::Hello(222),
            "delivered to the restarted peer, not the dead one",
        );

        // Pool still holds exactly one connection — the fresh one.
        let pool = sender.pool.read().await;
        assert_eq!(pool.len(), 1);
        assert!(pool.get(&2).unwrap().is_alive());
    }
}
