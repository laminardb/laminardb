use crate::config::ReplicationConfig;
use crate::error::{PgWireError, Result};
use crate::lsn::Lsn;

use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;

use tokio::sync::{mpsc, oneshot, watch, Semaphore};
use tokio::task::JoinHandle;

use std::sync::Arc;
use std::time::Duration;

#[cfg(not(feature = "tls-rustls"))]
use crate::config::SslMode;

use super::worker::{ReplicationEvent, ReplicationEventReceiver, SharedProgress, WorkerState};

const DROP_SHUTDOWN_GRACE: Duration = Duration::from_millis(100);

/// PostgreSQL logical replication client.
///
/// This client spawns a background worker task that maintains the replication
/// connection and streams events to the consumer via a bounded channel.
///
/// # Example
///
/// ```no_run
/// use pgwire_replication::client::{ReplicationClient, ReplicationEvent};
/// use pgwire_replication::config::ReplicationConfig;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = ReplicationConfig::new(
///         "localhost",
///         "postgres",
///         "password",
///         "mydb",
///         "my_slot",
///         "my_pub",
///     );
///
///     let mut client = ReplicationClient::connect(config).await?;
///
///     while let Some(ev) = client.recv().await? {
///         match ev {
///             ReplicationEvent::XLogData { data, wal_end, .. } => {
///                 process_change(&data);
///                 client.update_applied_lsn(wal_end);
///             }
///             ReplicationEvent::KeepAlive { .. } => {}
///             ReplicationEvent::StoppedAt { reached } => {
///                 println!("Reached stop LSN: {reached}");
///                 break;
///             }
///             _ => {}
///         }
///     }
///
///     Ok(())
/// }
///
/// fn process_change(_data: &bytes::Bytes) {
///     // user-defined
/// }
/// ```
pub struct ReplicationClient {
    rx: ReplicationEventReceiver,
    progress: Arc<SharedProgress>,
    stop_tx: watch::Sender<bool>,
    join: Option<JoinHandle<std::result::Result<(), PgWireError>>>,
    runtime: tokio::runtime::Handle,
}

/// Aborts a not-yet-published replication worker when `connect()` is
/// cancelled or returns an error. Once startup succeeds, ownership moves into
/// `ReplicationClient` and its normal shutdown path applies.
struct StartupWorker {
    join: Option<JoinHandle<std::result::Result<(), PgWireError>>>,
}

impl StartupWorker {
    fn new(join: JoinHandle<std::result::Result<(), PgWireError>>) -> Self {
        Self { join: Some(join) }
    }

    fn take(&mut self) -> JoinHandle<std::result::Result<(), PgWireError>> {
        self.join
            .take()
            .expect("startup worker ownership is transferred once")
    }
}

impl Drop for StartupWorker {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

impl ReplicationClient {
    /// Connect to PostgreSQL and start streaming replication events.
    ///
    /// This establishes a TCP connection (optionally upgrading to TLS),
    /// authenticates, and starts the replication stream. Events are buffered
    /// in a channel of size `config.buffer_events`, with raw payload ownership
    /// additionally capped by `config.max_in_flight_bytes`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - TCP connection fails
    /// - TLS handshake fails (when enabled)
    /// - Authentication fails
    /// - Replication slot doesn't exist
    /// - Publication doesn't exist
    /// - Unix socket does not exist (when host starts with `/`)
    /// - TLS requested with Unix socket connection
    pub async fn connect(cfg: ReplicationConfig) -> Result<Self> {
        Self::connect_with_worker_lifetime(cfg, ()).await
    }

    /// Connect while retaining `worker_lifetime` for the exact lifetime of the
    /// spawned replication worker.
    ///
    /// This lets callers fence replacement work until a cancelled startup
    /// worker has actually been destroyed, rather than merely requested to
    /// abort.
    pub async fn connect_with_worker_lifetime<G>(
        cfg: ReplicationConfig,
        worker_lifetime: G,
    ) -> Result<Self>
    where
        G: Send + 'static,
    {
        validate_buffer_limits(&cfg)?;
        let (tx, rx) = mpsc::channel(cfg.buffer_events);
        let wire_byte_budget = Arc::new(Semaphore::new(cfg.max_in_flight_bytes));

        // Progress is shared via atomics: cheap, monotonic, no async backpressure.
        let progress = Arc::new(SharedProgress::new(cfg.start_lsn));

        let (stop_tx, stop_rx) = watch::channel(false);
        let (startup_tx, startup_rx) = oneshot::channel();

        let progress_for_worker = Arc::clone(&progress);
        let cfg_for_worker = cfg.clone();

        let join = tokio::spawn(async move {
            let _worker_lifetime = worker_lifetime;
            let mut worker = WorkerState::new(
                cfg_for_worker,
                progress_for_worker,
                stop_rx,
                tx,
                wire_byte_budget,
            );
            worker.install_startup_notifier(startup_tx);
            let res = run_worker(&mut worker, &cfg).await;
            if let Err(ref e) = res {
                // Covers TCP, Unix-socket, and TLS failures that happen before
                // `WorkerState::run_on_stream` can publish its own result.
                worker.report_startup_failure(e);
                tracing::error!("replication worker terminated with error: {e}");
            }
            res
        });

        let mut startup_worker = StartupWorker::new(join);
        match startup_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(PgWireError::Task(
                    "replication worker terminated before publishing startup readiness".into(),
                ));
            }
        }

        Ok(Self {
            rx,
            progress,
            stop_tx,
            join: Some(startup_worker.take()),
            runtime: tokio::runtime::Handle::current(),
        })
    }

    /// Receive the next replication event.
    ///
    /// - `Ok(Some(event))` => received an event
    /// - `Ok(None)`        => replication ended normally (stop requested or stop_at_lsn reached)
    /// - `Err(e)`          => replication ended abnormally
    pub async fn recv(&mut self) -> Result<Option<ReplicationEvent>> {
        match self.rx.recv().await {
            Some(Ok(ev)) => Ok(Some(ev)),
            Some(Err(e)) => Err(e),
            None => self.handle_worker_shutdown().await,
        }
    }

    async fn handle_worker_shutdown(&mut self) -> Result<Option<ReplicationEvent>> {
        let join = self
            .join
            .take()
            .ok_or_else(|| PgWireError::Internal("replication worker already joined".into()))?;

        match join.await {
            Ok(Ok(())) => Ok(None),
            Ok(Err(e)) => Err(e),
            Err(join_err) => Err(PgWireError::Task(format!(
                "replication worker panicked: {join_err}"
            ))),
        }
    }

    /// Update the applied/durable LSN reported to the server.
    ///
    /// Semantics: call this only once you have durably persisted all events up to `lsn`.
    /// This update is monotonic and cheap; wire feedback is still governed by the worker’s
    /// `status_interval` and keepalive reply requests.
    #[inline]
    pub fn update_applied_lsn(&self, lsn: Lsn) {
        self.progress.update_applied(lsn);
    }

    /// Request the worker to stop gracefully.
    ///
    /// After calling this, [`recv()`](Self::recv) will return remaining buffered
    /// events, then `Ok(None)` once the worker exits cleanly.
    ///
    /// This sends a CopyDone message to the server to cleanly terminate
    /// the replication stream.
    #[inline]
    pub fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }

    pub fn is_running(&self) -> bool {
        self.join
            .as_ref()
            .map(|j| !j.is_finished())
            .unwrap_or(false)
    }

    /// Wait for the worker task to complete and return its result.
    ///
    /// This consumes the client. Use this for diagnostics or to ensure
    /// clean shutdown after calling [`stop()`](Self::stop).
    pub async fn join(mut self) -> Result<()> {
        let join = self
            .join
            .take()
            .ok_or_else(|| PgWireError::Task("worker already joined".into()))?;

        match join.await {
            Ok(inner) => inner,
            Err(e) => Err(PgWireError::Task(format!("join error: {e}"))),
        }
    }

    /// Abort the worker task immediately.
    ///
    /// This is a hard cancel and does not send CopyDone.
    /// Prefer `stop()`/`shutdown()` for graceful termination.
    pub fn abort(&mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }

    /// Request a graceful stop and wait for the worker to exit.
    pub async fn shutdown(&mut self) -> Result<()> {
        self.stop();

        // Drain events until the worker closes the channel.
        while let Some(msg) = self.rx.recv().await {
            match msg {
                Ok(_ev) => {} //discard; caller can drain themselves if they need events
                Err(e) => return Err(e),
            }
        }

        self.join_mut().await
    }

    /// Wait for the worker task to complete and return its result.
    async fn join_mut(&mut self) -> Result<()> {
        let join = self
            .join
            .take()
            .ok_or_else(|| PgWireError::Task("worker already joined".into()))?;

        match join.await {
            Ok(inner) => inner,
            Err(e) => Err(PgWireError::Task(format!("join error: {e}"))),
        }
    }
}

fn validate_buffer_limits(cfg: &ReplicationConfig) -> Result<()> {
    if cfg.buffer_events == 0 {
        return Err(PgWireError::Configuration(
            "buffer_events must be greater than zero".into(),
        ));
    }
    if cfg.max_message_bytes == 0 {
        return Err(PgWireError::Configuration(
            "max_message_bytes must be greater than zero".into(),
        ));
    }
    if cfg.max_in_flight_bytes == 0 {
        return Err(PgWireError::Configuration(
            "max_in_flight_bytes must be greater than zero".into(),
        ));
    }
    if cfg.max_message_bytes > cfg.max_in_flight_bytes {
        return Err(PgWireError::Configuration(format!(
            "max_message_bytes ({}) must not exceed max_in_flight_bytes ({})",
            cfg.max_message_bytes, cfg.max_in_flight_bytes
        )));
    }
    if cfg.max_message_bytes > u32::MAX as usize {
        return Err(PgWireError::Configuration(format!(
            "max_message_bytes must not exceed {}",
            u32::MAX
        )));
    }
    if cfg.max_in_flight_bytes > Semaphore::MAX_PERMITS {
        return Err(PgWireError::Configuration(format!(
            "max_in_flight_bytes must not exceed {}",
            Semaphore::MAX_PERMITS
        )));
    }
    if cfg.status_interval.is_zero() {
        return Err(PgWireError::Configuration(
            "status_interval must be greater than zero".into(),
        ));
    }
    if cfg.idle_wakeup_interval.is_zero() {
        return Err(PgWireError::Configuration(
            "idle_wakeup_interval must be greater than zero".into(),
        ));
    }
    Ok(())
}

impl Drop for ReplicationClient {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);

        if let Some(join) = self.join.take() {
            drop(self.runtime.spawn(reap_dropped_worker(join)));
        }
    }
}

async fn reap_dropped_worker(mut join: JoinHandle<std::result::Result<(), PgWireError>>) {
    if tokio::time::timeout(DROP_SHUTDOWN_GRACE, &mut join)
        .await
        .is_err()
    {
        join.abort();
        let _ = join.await;
    }
}

async fn run_worker(worker: &mut WorkerState, cfg: &ReplicationConfig) -> Result<()> {
    #[cfg(unix)]
    if cfg.is_unix_socket() {
        if cfg.tls.mode.requires_tls() {
            return Err(PgWireError::Tls(
                "TLS is not supported over Unix domain sockets".into(),
            ));
        }

        let path = cfg.unix_socket_path();
        let mut stream = UnixStream::connect(&path).await.map_err(|e| {
            PgWireError::Io(std::sync::Arc::new(std::io::Error::new(
                e.kind(),
                format!("failed to connect to Unix socket {}: {e}", path.display()),
            )))
        })?;

        return worker.run_on_stream(&mut stream).await;
    }

    let tcp = TcpStream::connect((cfg.host.as_str(), cfg.port)).await?;
    tcp.set_nodelay(true)?;

    #[cfg(feature = "tls-rustls")]
    {
        use crate::tls::rustls::{maybe_upgrade_to_tls, MaybeTlsStream};
        let upgraded = maybe_upgrade_to_tls(tcp, &cfg.tls, &cfg.host).await?;
        match upgraded {
            MaybeTlsStream::Plain(mut s) => worker.run_on_stream(&mut s).await,
            MaybeTlsStream::Tls(mut s) => worker.run_on_stream(s.as_mut()).await,
        }
    }

    #[cfg(not(feature = "tls-rustls"))]
    {
        if !matches!(cfg.tls.mode, SslMode::Disable) {
            return Err(PgWireError::Tls("tls-rustls feature not enabled".into()));
        }
        let mut s = tcp;
        worker.run_on_stream(&mut s).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, oneshot, watch};

    use super::{validate_buffer_limits, ReplicationClient, DROP_SHUTDOWN_GRACE};
    use crate::client::worker::{ReplicationEvent, SharedProgress};
    use crate::config::ReplicationConfig;
    use crate::error::PgWireError;
    use crate::lsn::Lsn;

    #[test]
    fn buffering_config_requires_a_fittable_frame_and_nonzero_channel() {
        let mut config = ReplicationConfig::default();
        config.buffer_events = 0;
        assert!(validate_buffer_limits(&config).is_err());

        config.buffer_events = 1;
        config.max_message_bytes = 9;
        config.max_in_flight_bytes = 8;
        let error = validate_buffer_limits(&config).unwrap_err();
        assert!(error.to_string().contains("must not exceed"), "{error}");

        config.max_message_bytes = 8;
        assert!(validate_buffer_limits(&config).is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drop_reaps_a_worker_stalled_after_stop() {
        struct WorkerLifetime {
            active: Arc<AtomicBool>,
            terminated: Option<oneshot::Sender<()>>,
        }

        impl Drop for WorkerLifetime {
            fn drop(&mut self) {
                self.active.store(false, Ordering::Release);
                if let Some(terminated) = self.terminated.take() {
                    let _ = terminated.send(());
                }
            }
        }

        let (events_tx, rx) =
            mpsc::channel::<std::result::Result<ReplicationEvent, PgWireError>>(1);
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let (started_tx, started_rx) = oneshot::channel();
        let (saw_stop_tx, saw_stop_rx) = oneshot::channel();
        let (terminated_tx, terminated_rx) = oneshot::channel();
        let active = Arc::new(AtomicBool::new(false));
        let worker_active = Arc::clone(&active);
        let join = tokio::spawn(async move {
            let _events_tx = events_tx;
            let _lifetime = WorkerLifetime {
                active: Arc::clone(&worker_active),
                terminated: Some(terminated_tx),
            };
            worker_active.store(true, Ordering::Release);
            let _ = started_tx.send(());
            stop_rx.changed().await.unwrap();
            assert!(*stop_rx.borrow());
            let _ = saw_stop_tx.send(());
            std::future::pending::<()>().await;
            Ok::<(), PgWireError>(())
        });
        let client = ReplicationClient {
            rx,
            progress: Arc::new(SharedProgress::new(Lsn::ZERO)),
            stop_tx,
            join: Some(join),
            runtime: tokio::runtime::Handle::current(),
        };
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .expect("stalled worker must start")
            .unwrap();
        assert!(active.load(Ordering::Acquire));

        drop(client);

        tokio::time::timeout(Duration::from_secs(1), saw_stop_rx)
            .await
            .expect("Drop must request graceful stop")
            .unwrap();
        tokio::time::timeout(DROP_SHUTDOWN_GRACE + Duration::from_secs(1), terminated_rx)
            .await
            .expect("stalled worker must be aborted and reaped within the cleanup bound")
            .unwrap();
        assert!(!active.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn cancelled_connect_retains_lifetime_until_startup_worker_is_destroyed() {
        struct WorkerLifetime(Option<oneshot::Sender<()>>);

        impl Drop for WorkerLifetime {
            fn drop(&mut self) {
                if let Some(terminated) = self.0.take() {
                    let _ = terminated.send(());
                }
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = ReplicationConfig::default();
        config.port = listener.local_addr().unwrap().port();
        let (terminated_tx, terminated_rx) = oneshot::channel();

        let connect = tokio::spawn(ReplicationClient::connect_with_worker_lifetime(
            config,
            WorkerLifetime(Some(terminated_tx)),
        ));
        let (_socket, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("startup worker must connect")
            .unwrap();

        connect.abort();
        let error = match connect.await {
            Err(error) => error,
            Ok(_) => panic!("connect task must be cancelled"),
        };
        assert!(error.is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), terminated_rx)
            .await
            .expect("worker lifetime must end after startup cancellation")
            .unwrap();
    }
}
