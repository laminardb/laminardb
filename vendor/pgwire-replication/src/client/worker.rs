use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::config::ReplicationConfig;
use crate::error::{PgWireError, Result};
use crate::lsn::Lsn;
use crate::protocol::framing::{
    read_backend_message_with_limit, write_copy_data, write_copy_done, write_password_message,
    write_query, write_startup_message, MessageReader,
};
use crate::protocol::messages::{parse_auth_request, parse_error_response};
use crate::protocol::replication::{
    encode_standby_status_update, parse_copy_data, ReplicationCopyData, PG_EPOCH_MICROS,
};

const SOCKET_READ_AHEAD_BYTES: usize = 128 * 1024;

/// Shared replication progress updated by the consumer and read by the worker.
///
/// Stored as an AtomicU64 so progress updates are cheap and monotonic
/// without async backpressure.
pub struct SharedProgress {
    applied: AtomicU64,
}

/// Reservation for the raw backend allocation retained by an event.
///
/// Clones share one reservation because cloned [`Bytes`] values share the same
/// backing allocation. Dropping the final guard returns the bytes to the
/// connection-wide in-flight budget.
#[derive(Clone)]
pub struct WireBytesGuard {
    inner: Arc<WireBytesReservation>,
}

struct WireBytesReservation {
    _permit: OwnedSemaphorePermit,
    retained_bytes: usize,
}

impl WireBytesGuard {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        let retained_bytes = permit.num_permits();
        Self {
            inner: Arc::new(WireBytesReservation {
                _permit: permit,
                retained_bytes,
            }),
        }
    }

    /// Declared backend payload bytes covered by this reservation.
    #[inline]
    pub fn retained_bytes(&self) -> usize {
        self.inner.retained_bytes
    }
}

impl std::fmt::Debug for WireBytesGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WireBytesGuard")
            .field("retained_bytes", &self.retained_bytes())
            .finish_non_exhaustive()
    }
}

impl SharedProgress {
    pub fn new(start: Lsn) -> Self {
        Self {
            applied: AtomicU64::new(start.as_u64()),
        }
    }

    #[inline]
    pub fn load_applied(&self) -> Lsn {
        Lsn::from_u64(self.applied.load(Ordering::Acquire))
    }

    /// Monotonic update: if `lsn` is lower than the currently stored applied LSN,
    /// this is a no-op.
    #[inline]
    pub fn update_applied(&self, lsn: Lsn) {
        let new = lsn.as_u64();
        let mut cur = self.applied.load(Ordering::Relaxed);

        while new > cur {
            match self
                .applied
                .compare_exchange_weak(cur, new, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }
}

/// Events emitted by the replication worker.
#[derive(Debug, Clone)]
pub enum ReplicationEvent {
    /// Server heartbeat message.
    KeepAlive {
        /// Current server WAL end position
        wal_end: Lsn,
        /// Whether server requested a reply (already handled internally)
        reply_requested: bool,
        /// Server timestamp (microseconds since 2000-01-01)
        server_time_micros: i64,
    },

    /// Start of a transaction (pgoutput Begin message).
    Begin {
        final_lsn: Lsn,
        xid: u32,
        commit_time_micros: i64,
    },

    /// WAL data containing transaction changes.
    XLogData {
        /// WAL position where this data starts
        wal_start: Lsn,
        /// WAL end position (may be 0 for mid-transaction messages)
        wal_end: Lsn,
        /// Server timestamp (microseconds since 2000-01-01)
        server_time_micros: i64,
        /// pgoutput-encoded change data
        data: Bytes,
        /// Aggregate byte-budget reservation for `data`'s backing wire frame.
        wire_bytes: WireBytesGuard,
    },

    /// End of a transaction (pgoutput Commit message).
    Commit {
        lsn: Lsn,
        end_lsn: Lsn,
        commit_time_micros: i64,
    },

    /// Logical decoding message emitted via `pg_logical_emit_message()`.
    ///
    /// Transactional messages are delivered only after the enclosing
    /// transaction commits. Non-transactional messages are delivered
    /// immediately.
    Message {
        /// Whether the message was emitted inside a transaction.
        transactional: bool,
        /// LSN of the message in the WAL.
        lsn: Lsn,
        /// UTF-8 application-defined message prefix, sliced from the wire frame.
        prefix: Bytes,
        /// Raw message content bytes.
        content: Bytes,
        /// Aggregate byte-budget reservation for the retained wire frame.
        wire_bytes: WireBytesGuard,
    },

    /// Emitted when `stop_at_lsn` has been reached.
    ///
    /// After this event, no more events will be emitted and the
    /// replication stream will be closed.
    StoppedAt {
        /// The LSN that triggered the stop condition
        reached: Lsn,
    },
}

/// Channel receiver type for replication events.
pub type ReplicationEventReceiver =
    mpsc::Receiver<std::result::Result<ReplicationEvent, PgWireError>>;

/// Internal worker state.
pub struct WorkerState {
    cfg: ReplicationConfig,
    progress: Arc<SharedProgress>,
    stop_rx: watch::Receiver<bool>,
    out: mpsc::Sender<std::result::Result<ReplicationEvent, PgWireError>>,
    wire_byte_budget: Arc<Semaphore>,
    startup_tx: Option<oneshot::Sender<Result<()>>>,
}

impl WorkerState {
    pub fn new(
        cfg: ReplicationConfig,
        progress: Arc<SharedProgress>,
        stop_rx: watch::Receiver<bool>,
        out: mpsc::Sender<std::result::Result<ReplicationEvent, PgWireError>>,
        wire_byte_budget: Arc<Semaphore>,
    ) -> Self {
        Self {
            cfg,
            progress,
            stop_rx,
            out,
            wire_byte_budget,
            startup_tx: None,
        }
    }

    pub(super) fn install_startup_notifier(&mut self, tx: oneshot::Sender<Result<()>>) {
        debug_assert!(self.startup_tx.is_none());
        self.startup_tx = Some(tx);
    }

    pub(super) fn report_startup_failure(&mut self, error: &PgWireError) {
        self.publish_startup(Err(error.clone()));
    }

    fn publish_startup(&mut self, result: Result<()>) {
        if let Some(tx) = self.startup_tx.take() {
            let _ = tx.send(result);
        }
    }

    /// Run the replication protocol on the given stream.
    pub async fn run_on_stream<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
    ) -> Result<()> {
        // Wrap in a 128KB read buffer to batch multiple WAL messages into fewer
        // recv() syscalls. BufReader delegates AsyncWrite to the inner stream,
        // so writes (standby status replies, etc.) are unaffected.
        let mut stream = BufReader::with_capacity(SOCKET_READ_AHEAD_BYTES, stream);
        let startup = async {
            self.startup(&mut stream).await?;
            self.authenticate(&mut stream).await?;
            self.validate_recovery_identity(&mut stream).await?;
            self.start_replication(&mut stream).await
        }
        .await;
        if let Err(error) = startup {
            self.report_startup_failure(&error);
            return Err(error);
        }
        // CopyBothResponse is PostgreSQL's acceptance point for
        // START_REPLICATION. Do not let the public connect future return before
        // this exact protocol boundary.
        self.publish_startup(Ok(()));
        self.stream_loop(&mut stream).await
    }

    /// Send startup message with replication parameters.
    async fn startup<S: AsyncWrite + Unpin>(&self, stream: &mut S) -> Result<()> {
        let params = [
            ("user", self.cfg.user.as_str()),
            ("database", self.cfg.database.as_str()),
            ("replication", "database"),
            ("client_encoding", "UTF8"),
            ("application_name", "pgwire-replication"),
        ];
        write_startup_message(stream, 196608, &params).await
    }

    /// Start the logical replication stream.
    async fn start_replication<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
    ) -> Result<()> {
        let sql = start_replication_query(&self.cfg);
        write_query(stream, &sql).await?;

        // Wait for CopyBothResponse
        loop {
            let msg = read_backend_message_with_limit(stream, self.cfg.max_message_bytes).await?;
            match msg.tag {
                b'W' => return Ok(()), // CopyBothResponse - ready to stream
                b'E' => return Err(PgWireError::Server(parse_error_response(&msg.payload))),
                b'N' | b'S' | b'K' => continue, // Notice, ParameterStatus, BackendKeyData
                _ => continue,
            }
        }
    }

    async fn validate_recovery_identity<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
    ) -> Result<()> {
        if self.cfg.expected_recovery_identity.is_none() {
            return Ok(());
        }

        let identity = self
            .query_single_text_row(stream, "IDENTIFY_SYSTEM", "IDENTIFY_SYSTEM")
            .await?;
        validate_identify_system(&self.cfg, &identity)?;

        let slot = escape_string_literal(&self.cfg.slot);
        let sql = format!(
            "SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots \
             WHERE slot_name = {slot}"
        );
        let slot_row = self
            .query_single_text_row(stream, &sql, "replication-slot recovery cursor")
            .await?;
        validate_slot_cursor(&self.cfg, &slot_row)
    }

    async fn query_single_text_row<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        sql: &str,
        context: &str,
    ) -> Result<Vec<Option<String>>> {
        write_query(stream, sql).await?;
        let mut row = None;
        loop {
            let message =
                read_backend_message_with_limit(stream, self.cfg.max_message_bytes).await?;
            match message.tag {
                b'D' => {
                    if row.is_some() {
                        return Err(PgWireError::Protocol(format!(
                            "{context} returned more than one row"
                        )));
                    }
                    row = Some(parse_text_data_row(&message.payload, context)?);
                }
                b'E' => return Err(PgWireError::Server(parse_error_response(&message.payload))),
                b'Z' => {
                    return row.ok_or_else(|| {
                        PgWireError::Configuration(format!("{context} returned no rows"))
                    });
                }
                b'T' | b'C' | b'N' | b'S' | b'A' => {}
                tag => {
                    return Err(PgWireError::Protocol(format!(
                        "unexpected backend message 0x{tag:02X} while reading {context}"
                    )));
                }
            }
        }
    }

    /// Main replication streaming loop.
    ///
    /// Uses a two-phase approach for throughput:
    /// 1. **Drain phase**: while the BufReader has buffered data, read messages
    ///    in a tight loop without `select!` or timeout overhead.
    /// 2. **Wait phase**: when the buffer is empty, fall back to `select!` with
    ///    timeout + stop signal to handle idle keepalives and graceful shutdown.
    ///
    /// Reads use [`MessageReader`], which preserves partial-read state across
    /// dropped futures so the wait-phase `select!` is cancellation-safe.
    async fn stream_loop<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut BufReader<S>,
    ) -> Result<()> {
        let mut last_status_sent = Instant::now() - self.cfg.status_interval;
        let mut last_applied = self.progress.load_applied();
        // Cancellation-safe message reader, partial reads survive dropped futures.
        // Start with no payload allocation: each declared payload is reserved
        // from `wire_byte_budget` before the reader grows this buffer.
        let mut reader = MessageReader::with_capacity_and_limit(0, self.cfg.max_message_bytes);
        // How many messages to process in the tight loop before checking
        // stop signal and sending periodic status feedback.
        const DRAIN_BATCH: usize = 256;

        loop {
            // Update applied LSN from client
            let current_applied = self.progress.load_applied();
            if current_applied != last_applied {
                last_applied = current_applied;
            }

            // Send periodic status feedback
            if last_status_sent.elapsed() >= self.cfg.status_interval {
                self.send_feedback(stream, last_applied, false).await?;
                last_status_sent = Instant::now();
            }

            // ── Drain phase: tight loop while BufReader has buffered data ──
            // The BufReader has a 128KB internal buffer. When the kernel delivers
            // a large TCP segment, many WAL messages are available without syscalls.
            // Drain in batches, while keeping stop and feedback deadlines live
            // if either socket input or the wire-byte budget stalls.
            let mut drained = 0usize;
            while stream.buffer().len() >= 5 && drained < DRAIN_BATCH {
                let feedback_deadline = last_status_sent + self.cfg.status_interval;
                let (msg, wire_permit) = tokio::select! {
                    biased;

                    changed = self.stop_rx.changed() => {
                        if changed.is_err() || *self.stop_rx.borrow() {
                            let _ = write_copy_done(stream).await;
                            return Ok(());
                        }
                        continue;
                    }

                    result = reader.read_with_budget(stream, &self.wire_byte_budget) => result?,

                    _ = tokio::time::sleep_until(feedback_deadline) => {
                        let applied = self.progress.load_applied();
                        last_applied = applied;
                        self.send_feedback(stream, applied, false).await?;
                        last_status_sent = Instant::now();
                        continue;
                    }
                };
                drained += 1;
                if msg.tag == b'E' {
                    return Err(PgWireError::Server(parse_error_response(&msg.payload)));
                }
                if msg.tag == b'd'
                    && self
                        .handle_copy_data(
                            stream,
                            msg.payload,
                            wire_permit,
                            &mut last_applied,
                            &mut last_status_sent,
                        )
                        .await?
                {
                    return Ok(());
                }
            }

            // If we drained messages, loop back to check stop/status before
            // potentially blocking on the next read.
            if drained > 0 {
                // Check stop signal without blocking
                if self.stop_rx.has_changed().unwrap_or(false) && *self.stop_rx.borrow() {
                    let _ = write_copy_done(stream).await;
                    return Ok(());
                }
                continue;
            }

            // ── Wait phase: buffer empty, need to wait for socket data ──
            //
            // Both `stop_rx.changed()` and the timeout can drop the read future
            // mid-message. `MessageReader::read` is cancellation-safe — partial
            // header/payload state lives on `reader` and is preserved across the
            // drop, so the next iteration resumes the read without losing bytes.
            let feedback_after = self
                .cfg
                .status_interval
                .saturating_sub(last_status_sent.elapsed());
            let wake_after = self.cfg.idle_wakeup_interval.min(feedback_after);
            let (msg, wire_permit) = tokio::select! {
                biased;

                _ = self.stop_rx.changed() => {
                    if *self.stop_rx.borrow() {
                        let _ = write_copy_done(stream).await;
                        return Ok(());
                    }
                    continue;
                }

                msg_result = tokio::time::timeout(
                    wake_after,
                    reader.read_with_budget(stream, &self.wire_byte_budget),
                ) => {
                    match msg_result {
                        Ok(res) => res?,
                        Err(_) => {
                            let applied = self.progress.load_applied();
                            last_applied = applied;
                            self.send_feedback(stream, applied, false).await?;
                            last_status_sent = Instant::now();
                            continue;
                        }
                    }
                }
            };

            if msg.tag == b'E' {
                return Err(PgWireError::Server(parse_error_response(&msg.payload)));
            }
            if msg.tag == b'd'
                && self
                    .handle_copy_data(
                        stream,
                        msg.payload,
                        wire_permit,
                        &mut last_applied,
                        &mut last_status_sent,
                    )
                    .await?
            {
                return Ok(());
            }
        }
    }

    /// Handle a CopyData message. Returns true if we should stop.
    async fn handle_copy_data<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut BufReader<S>,
        payload: Bytes,
        wire_permit: OwnedSemaphorePermit,
        last_applied: &mut Lsn,
        last_status_sent: &mut Instant,
    ) -> Result<bool> {
        let cd = parse_copy_data(payload)?;

        match cd {
            ReplicationCopyData::KeepAlive {
                wal_end,
                server_time_micros,
                reply_requested,
            } => {
                // KeepAlive contains no retained raw slice.
                drop(wire_permit);
                // Respond immediately if server requests it
                if reply_requested {
                    let applied = self.progress.load_applied();
                    *last_applied = applied;
                    self.send_feedback(stream, applied, true).await?;
                    *last_status_sent = Instant::now();
                }

                if !self
                    .send_event_with_feedback(
                        stream,
                        ReplicationEvent::KeepAlive {
                            wal_end,
                            reply_requested,
                            server_time_micros,
                        },
                        last_applied,
                        last_status_sent,
                    )
                    .await?
                {
                    return Ok(true);
                }

                Ok(false)
            }
            ReplicationCopyData::XLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data,
            } => {
                let wire_bytes = WireBytesGuard::new(wire_permit);
                // If the payload is a pgoutput Begin/Commit message, emit only the boundary event.
                let (boundary_ev, wire_bytes) = match data.first().copied() {
                    Some(b'B' | b'C') => (parse_pgoutput_boundary(&data, None)?, None),
                    Some(b'M') => (parse_pgoutput_boundary(&data, Some(wire_bytes))?, None),
                    _ => (None, Some(wire_bytes)),
                };
                if let Some(boundary_ev) = boundary_ev {
                    let reached_lsn = match boundary_ev {
                        ReplicationEvent::Begin { final_lsn, .. } => final_lsn,
                        ReplicationEvent::Commit { end_lsn, .. } => end_lsn,
                        _ => wal_end, // should never happen if parser only returns Begin/Commit
                    };

                    if !self
                        .send_event_with_feedback(
                            stream,
                            boundary_ev,
                            last_applied,
                            last_status_sent,
                        )
                        .await?
                    {
                        return Ok(true);
                    }

                    // Stop condition (prefer boundary LSN semantics when available)
                    if let Some(stop_lsn) = self.cfg.stop_at_lsn {
                        if reached_lsn >= stop_lsn {
                            if !self
                                .send_event_with_feedback(
                                    stream,
                                    ReplicationEvent::StoppedAt {
                                        reached: reached_lsn,
                                    },
                                    last_applied,
                                    last_status_sent,
                                )
                                .await?
                            {
                                return Ok(true);
                            }
                            let _ = write_copy_done(stream).await;
                            return Ok(true); // should stop.
                        }
                    }

                    return Ok(false);
                }
                // Otherwise, emit raw payload
                let wire_bytes = wire_bytes.expect("raw XLogData retains its wire reservation");
                // Check stop condition
                if let Some(stop_lsn) = self.cfg.stop_at_lsn {
                    if wal_end >= stop_lsn {
                        // Send final event, then stop signal
                        if !self
                            .send_event_with_feedback(
                                stream,
                                ReplicationEvent::XLogData {
                                    wal_start,
                                    wal_end,
                                    server_time_micros,
                                    data,
                                    wire_bytes,
                                },
                                last_applied,
                                last_status_sent,
                            )
                            .await?
                        {
                            return Ok(true);
                        }

                        if !self
                            .send_event_with_feedback(
                                stream,
                                ReplicationEvent::StoppedAt { reached: wal_end },
                                last_applied,
                                last_status_sent,
                            )
                            .await?
                        {
                            return Ok(true);
                        }

                        let _ = write_copy_done(stream).await;
                        return Ok(true);
                    }
                }

                if !self
                    .send_event_with_feedback(
                        stream,
                        ReplicationEvent::XLogData {
                            wal_start,
                            wal_end,
                            server_time_micros,
                            data,
                            wire_bytes,
                        },
                        last_applied,
                        last_status_sent,
                    )
                    .await?
                {
                    return Ok(true);
                }

                Ok(false)
            }
        }
    }

    /// Sends an event without starving feedback while downstream is backpressured.
    async fn send_event_with_feedback<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut BufReader<S>,
        event: ReplicationEvent,
        last_applied: &mut Lsn,
        last_status_sent: &mut Instant,
    ) -> Result<bool> {
        let send = self.out.send(Ok(event));
        tokio::pin!(send);

        loop {
            let feedback_deadline = *last_status_sent + self.cfg.status_interval;
            tokio::select! {
                biased;

                changed = self.stop_rx.changed() => {
                    if changed.is_err() || *self.stop_rx.borrow() {
                        let _ = write_copy_done(stream).await;
                        return Ok(false);
                    }
                }

                result = &mut send => {
                    if result.is_err() {
                        tracing::debug!("event channel closed, stopping replication worker");
                        let _ = write_copy_done(stream).await;
                        return Ok(false);
                    }
                    return Ok(true);
                }

                _ = tokio::time::sleep_until(feedback_deadline) => {
                    let applied = self.progress.load_applied();
                    *last_applied = applied;
                    self.send_feedback(stream, applied, false).await?;
                    *last_status_sent = Instant::now();
                }
            }
        }
    }

    /// Handle PostgreSQL authentication exchange.
    async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
    ) -> Result<()> {
        loop {
            let msg = read_backend_message_with_limit(stream, self.cfg.max_message_bytes).await?;
            match msg.tag {
                b'R' => {
                    let (code, rest) = parse_auth_request(&msg.payload)?;
                    self.handle_auth_request(stream, code, rest).await?;
                }
                b'E' => return Err(PgWireError::Server(parse_error_response(&msg.payload))),
                b'S' | b'K' => {}      // ParameterStatus, BackendKeyData - ignore
                b'Z' => return Ok(()), // ReadyForQuery - auth complete
                _ => {}
            }
        }
    }

    /// Handle a specific authentication request.
    async fn handle_auth_request<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
        code: i32,
        data: &[u8],
    ) -> Result<()> {
        match code {
            0 => Ok(()), // AuthenticationOk
            3 => {
                // Cleartext password
                let mut payload = Vec::from(self.cfg.password.as_bytes());
                payload.push(0);
                write_password_message(stream, &payload).await
            }
            10 => {
                // SASL (SCRAM-SHA-256)
                self.auth_scram(stream, data).await
            }
            #[cfg(feature = "md5")]
            5 => {
                // MD5 password
                if data.len() != 4 {
                    return Err(PgWireError::Protocol(
                        "MD5 auth: expected 4-byte salt".into(),
                    ));
                }
                let mut salt = [0u8; 4];
                salt.copy_from_slice(&data[..4]);

                let hash = postgres_md5(&self.cfg.password, &self.cfg.user, &salt);
                let mut payload = hash.into_bytes();
                payload.push(0);
                write_password_message(stream, &payload).await
            }
            _ => Err(PgWireError::Auth(format!(
                "unsupported auth method code: {code}"
            ))),
        }
    }

    /// Perform SCRAM-SHA-256 authentication.
    async fn auth_scram<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
        mechanisms_data: &[u8],
    ) -> Result<()> {
        // Parse offered mechanisms
        let mechanisms = parse_sasl_mechanisms(mechanisms_data);

        if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
            return Err(PgWireError::Auth(format!(
                "server doesn't offer SCRAM-SHA-256, available: {mechanisms:?}"
            )));
        }

        #[cfg(not(feature = "scram"))]
        return Err(PgWireError::Auth(
            "SCRAM authentication required but 'scram' feature not enabled".into(),
        ));

        #[cfg(feature = "scram")]
        {
            use crate::auth::scram::ScramClient;

            let scram = ScramClient::new(&self.cfg.user);

            // Send SASLInitialResponse
            let mut init = Vec::new();
            init.extend_from_slice(b"SCRAM-SHA-256\0");
            init.extend_from_slice(&(scram.client_first.len() as i32).to_be_bytes());
            init.extend_from_slice(scram.client_first.as_bytes());
            write_password_message(stream, &init).await?;

            // Receive AuthenticationSASLContinue (code 11)
            let server_first = read_auth_data(stream, 11, self.cfg.max_message_bytes).await?;
            let server_first_str = String::from_utf8_lossy(&server_first);

            // Compute and send client-final
            let (client_final, auth_message, salted_password) =
                scram.client_final(&self.cfg.password, &server_first_str)?;
            write_password_message(stream, client_final.as_bytes()).await?;

            // Receive and verify AuthenticationSASLFinal (code 12)
            let server_final = read_auth_data(stream, 12, self.cfg.max_message_bytes).await?;
            let server_final_str = String::from_utf8_lossy(&server_final);
            ScramClient::verify_server_final(&server_final_str, &salted_password, &auth_message)?;

            Ok(())
        }
    }

    /// Send standby status update to server.
    async fn send_feedback<S: AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        applied: Lsn,
        reply_requested: bool,
    ) -> Result<()> {
        let client_time = current_pg_timestamp();
        let payload = encode_standby_status_update(applied, client_time, reply_requested);
        write_copy_data(stream, &payload).await
    }
}

fn escape_string_literal(value: &str) -> String {
    format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn parse_text_data_row(payload: &[u8], context: &str) -> Result<Vec<Option<String>>> {
    if payload.len() < 2 {
        return Err(PgWireError::Protocol(format!(
            "{context} returned a truncated DataRow"
        )));
    }
    let column_count = i16::from_be_bytes([payload[0], payload[1]]);
    let column_count = usize::try_from(column_count).map_err(|_| {
        PgWireError::Protocol(format!(
            "{context} returned a negative DataRow column count"
        ))
    })?;
    let mut cursor = 2_usize;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let length_end = cursor
            .checked_add(4)
            .ok_or_else(|| PgWireError::Protocol(format!("{context} DataRow length overflow")))?;
        let length_bytes = payload.get(cursor..length_end).ok_or_else(|| {
            PgWireError::Protocol(format!("{context} returned a truncated DataRow length"))
        })?;
        cursor = length_end;
        let length = i32::from_be_bytes(length_bytes.try_into().expect("four bytes checked"));
        if length == -1 {
            columns.push(None);
            continue;
        }
        let length = usize::try_from(length).map_err(|_| {
            PgWireError::Protocol(format!("{context} returned an invalid DataRow length"))
        })?;
        let value_end = cursor.checked_add(length).ok_or_else(|| {
            PgWireError::Protocol(format!("{context} DataRow value length overflow"))
        })?;
        let value = payload.get(cursor..value_end).ok_or_else(|| {
            PgWireError::Protocol(format!("{context} returned a truncated DataRow value"))
        })?;
        let value = std::str::from_utf8(value).map_err(|error| {
            PgWireError::Protocol(format!("{context} returned non-UTF-8 text: {error}"))
        })?;
        columns.push(Some(value.to_owned()));
        cursor = value_end;
    }
    if cursor != payload.len() {
        return Err(PgWireError::Protocol(format!(
            "{context} DataRow contains trailing bytes"
        )));
    }
    Ok(columns)
}

fn required_text_column<'a>(
    row: &'a [Option<String>],
    index: usize,
    label: &str,
) -> Result<&'a str> {
    row.get(index)
        .and_then(Option::as_deref)
        .ok_or_else(|| PgWireError::Configuration(format!("PostgreSQL {label} is NULL or missing")))
}

fn validate_identify_system(config: &ReplicationConfig, row: &[Option<String>]) -> Result<Lsn> {
    if row.len() != 4 {
        return Err(PgWireError::Protocol(format!(
            "IDENTIFY_SYSTEM returned {} columns, expected 4",
            row.len()
        )));
    }
    let expected = config.expected_recovery_identity.ok_or_else(|| {
        PgWireError::Internal("recovery identity validation was not configured".into())
    })?;
    let system_identifier = required_text_column(row, 0, "system identifier")?
        .parse::<u64>()
        .map_err(|error| PgWireError::Protocol(format!("invalid system identifier: {error}")))?;
    let timeline_id = required_text_column(row, 1, "timeline")?
        .parse::<u32>()
        .map_err(|error| PgWireError::Protocol(format!("invalid timeline: {error}")))?;
    let wal_end = required_text_column(row, 2, "WAL flush position")?
        .parse::<Lsn>()
        .map_err(|error| PgWireError::Protocol(format!("invalid WAL flush position: {error}")))?;
    let database = required_text_column(row, 3, "database name")?;

    if system_identifier != expected.system_identifier {
        return Err(PgWireError::Configuration(format!(
            "PostgreSQL system identifier drifted: expected {}, got {system_identifier}",
            expected.system_identifier
        )));
    }
    if timeline_id != expected.timeline_id {
        return Err(PgWireError::Configuration(format!(
            "PostgreSQL timeline drifted: expected {}, got {timeline_id}; failover and timeline ancestry are not certified",
            expected.timeline_id
        )));
    }
    if database != config.database {
        return Err(PgWireError::Configuration(format!(
            "PostgreSQL replication socket connected to database '{database}', expected '{}'",
            config.database
        )));
    }
    if config.start_lsn > wal_end {
        return Err(PgWireError::Configuration(format!(
            "PostgreSQL recovery LSN {} is ahead of the exact server WAL flush position {wal_end}",
            config.start_lsn
        )));
    }
    Ok(wal_end)
}

fn validate_slot_cursor(config: &ReplicationConfig, row: &[Option<String>]) -> Result<()> {
    if row.len() != 1 {
        return Err(PgWireError::Protocol(format!(
            "replication-slot recovery cursor returned {} columns, expected 1",
            row.len()
        )));
    }
    let confirmed = required_text_column(row, 0, "slot confirmed_flush_lsn")?
        .parse::<Lsn>()
        .map_err(|error| {
            PgWireError::Protocol(format!("invalid slot confirmed_flush_lsn: {error}"))
        })?;
    if confirmed > config.start_lsn {
        return Err(PgWireError::Configuration(format!(
            "PostgreSQL slot '{}' has advanced to {confirmed}, beyond recovery LSN {}; required WAL is unavailable",
            config.slot, config.start_lsn
        )));
    }
    Ok(())
}

fn start_replication_query(config: &ReplicationConfig) -> String {
    format!(
        "START_REPLICATION SLOT {} LOGICAL {} \
         (proto_version '1', publication_names '{}', messages 'false')",
        config.slot, config.start_lsn, config.publication
    )
}

/// Parse SASL mechanism list from auth data.
fn parse_sasl_mechanisms(data: &[u8]) -> Vec<String> {
    let mut mechanisms = Vec::new();
    let mut remaining = data;

    while !remaining.is_empty() {
        if let Some(pos) = remaining.iter().position(|&x| x == 0) {
            if pos == 0 {
                break; // Empty string terminates list
            }
            mechanisms.push(String::from_utf8_lossy(&remaining[..pos]).to_string());
            remaining = &remaining[pos + 1..];
        } else {
            break;
        }
    }

    mechanisms
}

fn parse_pgoutput_boundary(
    data: &Bytes,
    wire_bytes: Option<WireBytesGuard>,
) -> Result<Option<ReplicationEvent>> {
    if data.is_empty() {
        return Ok(None);
    }

    let tag = data[0];
    let mut p = &data[1..];

    fn take_i8(p: &mut &[u8]) -> Result<i8> {
        if p.is_empty() {
            return Err(PgWireError::Protocol("pgoutput: truncated i8".into()));
        }
        let v = p[0] as i8;
        *p = &p[1..];
        Ok(v)
    }

    fn take_i32(p: &mut &[u8]) -> Result<i32> {
        if p.len() < 4 {
            return Err(PgWireError::Protocol("pgoutput: truncated i32".into()));
        }
        let (head, tail) = p.split_at(4);
        *p = tail;
        Ok(i32::from_be_bytes(head.try_into().unwrap()))
    }

    fn take_i64(p: &mut &[u8]) -> Result<i64> {
        if p.len() < 8 {
            return Err(PgWireError::Protocol("pgoutput: truncated i64".into()));
        }
        let (head, tail) = p.split_at(8);
        *p = tail;
        Ok(i64::from_be_bytes(head.try_into().unwrap()))
    }

    fn require_end(p: &[u8], message: &str) -> Result<()> {
        if p.is_empty() {
            Ok(())
        } else {
            Err(PgWireError::Protocol(format!(
                "pgoutput {message}: {} trailing bytes",
                p.len()
            )))
        }
    }

    match tag {
        b'B' => {
            let final_lsn = Lsn::from_u64(take_i64(&mut p)? as u64);
            let commit_time_micros = take_i64(&mut p)?;
            let xid = take_i32(&mut p)? as u32;
            require_end(p, "Begin")?;

            Ok(Some(ReplicationEvent::Begin {
                final_lsn,
                commit_time_micros,
                xid,
            }))
        }
        b'C' => {
            let flags = take_i8(&mut p)?;
            if flags != 0 {
                return Err(PgWireError::Protocol(format!(
                    "pgoutput Commit: unsupported flags {flags}"
                )));
            }
            let lsn = Lsn::from_u64(take_i64(&mut p)? as u64);
            let end_lsn = Lsn::from_u64(take_i64(&mut p)? as u64);
            let commit_time_micros = take_i64(&mut p)?;
            require_end(p, "Commit")?;

            Ok(Some(ReplicationEvent::Commit {
                lsn,
                end_lsn,
                commit_time_micros,
            }))
        }
        b'M' => {
            // Logical decoding message (pg_logical_emit_message)
            // Wire: flags(1) + lsn(8) + prefix(null-terminated) + content_len(4) + content(n)
            let flags = take_i8(&mut p)?;
            if flags & !1 != 0 {
                return Err(PgWireError::Protocol(format!(
                    "pgoutput Message: unsupported flags {flags}"
                )));
            }
            let transactional = (flags & 1) != 0;
            let lsn = Lsn::from_u64(take_i64(&mut p)? as u64);

            // Read null-terminated prefix string
            let prefix_end = p.iter().position(|&b| b == 0).ok_or_else(|| {
                PgWireError::Protocol("pgoutput Message: missing null terminator for prefix".into())
            })?;
            let prefix_start = data.len().saturating_sub(p.len());
            let prefix = data.slice(prefix_start..prefix_start + prefix_end);
            std::str::from_utf8(&prefix).map_err(|error| {
                PgWireError::Protocol(format!(
                    "pgoutput Message: prefix is not valid UTF-8: {error}"
                ))
            })?;
            p = &p[prefix_end + 1..]; // advance past null byte

            let content_len_raw = take_i32(&mut p)?;
            let content_len = usize::try_from(content_len_raw).map_err(|_| {
                PgWireError::Protocol(format!(
                    "pgoutput Message: negative content length {content_len_raw}"
                ))
            })?;
            if p.len() != content_len {
                return Err(PgWireError::Protocol(format!(
                    "pgoutput Message: expected exactly {} content bytes, got {}",
                    content_len,
                    p.len()
                )));
            }
            let content_start = data.len().saturating_sub(p.len());
            let content = data.slice(content_start..content_start + content_len);

            Ok(Some(ReplicationEvent::Message {
                transactional,
                lsn,
                prefix,
                content,
                wire_bytes: wire_bytes.ok_or_else(|| {
                    PgWireError::Internal(
                        "logical decoding message is missing its wire-byte reservation".into(),
                    )
                })?,
            }))
        }
        _ => Ok(None),
    }
}

/// Read authentication response data for a specific auth code.
async fn read_auth_data<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    expected_code: i32,
    max_message_bytes: usize,
) -> Result<Vec<u8>> {
    loop {
        let msg = read_backend_message_with_limit(stream, max_message_bytes).await?;
        match msg.tag {
            b'R' => {
                let (code, data) = parse_auth_request(&msg.payload)?;
                if code == expected_code {
                    return Ok(data.to_vec());
                }
                return Err(PgWireError::Auth(format!(
                    "unexpected auth code {code}, expected {expected_code}"
                )));
            }
            b'E' => return Err(PgWireError::Server(parse_error_response(&msg.payload))),
            _ => {} // Skip other messages
        }
    }
}

/// Get current time as PostgreSQL timestamp (microseconds since 2000-01-01).
fn current_pg_timestamp() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let unix_micros = (now.as_secs() as i64) * 1_000_000 + (now.subsec_micros() as i64);
    unix_micros - PG_EPOCH_MICROS
}

/// Compute PostgreSQL MD5 password hash.
#[cfg(feature = "md5")]
fn postgres_md5(password: &str, user: &str, salt: &[u8; 4]) -> String {
    fn md5_hex(data: &[u8]) -> String {
        format!("{:x}", md5::compute(data))
    }

    // First hash: md5(password + username)
    let inner = md5_hex(format!("{password}{user}").as_bytes());

    // Second hash: md5(inner_hash + salt)
    let mut outer_input = inner.into_bytes();
    outer_input.extend_from_slice(salt);

    format!("md5{}", md5_hex(&outer_input))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read_frontend_message(peer: &mut tokio::io::DuplexStream, tagged: bool) -> Vec<u8> {
        use tokio::io::AsyncReadExt;

        if tagged {
            let mut tag = [0_u8; 1];
            peer.read_exact(&mut tag).await.unwrap();
        }
        let mut length = [0_u8; 4];
        peer.read_exact(&mut length).await.unwrap();
        let length = i32::from_be_bytes(length);
        assert!(length >= 4);
        let mut payload = vec![0_u8; usize::try_from(length - 4).unwrap()];
        peer.read_exact(&mut payload).await.unwrap();
        payload
    }

    async fn write_backend_message(peer: &mut tokio::io::DuplexStream, tag: u8, payload: &[u8]) {
        use tokio::io::AsyncWriteExt;

        peer.write_all(&[tag]).await.unwrap();
        let length = i32::try_from(payload.len() + 4).unwrap();
        peer.write_all(&length.to_be_bytes()).await.unwrap();
        peer.write_all(payload).await.unwrap();
        peer.flush().await.unwrap();
    }

    fn text_data_row(values: &[Option<&str>]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&i16::try_from(values.len()).unwrap().to_be_bytes());
        for value in values {
            match value {
                Some(value) => {
                    payload.extend_from_slice(&i32::try_from(value.len()).unwrap().to_be_bytes());
                    payload.extend_from_slice(value.as_bytes());
                }
                None => payload.extend_from_slice(&(-1_i32).to_be_bytes()),
            }
        }
        payload
    }

    fn startup_worker_with_config(
        config: ReplicationConfig,
        startup_tx: oneshot::Sender<Result<()>>,
    ) -> (WorkerState, watch::Sender<bool>) {
        let (out, _rx) = mpsc::channel(1);
        let (stop_tx, stop_rx) = watch::channel(false);
        let progress = Arc::new(SharedProgress::new(config.start_lsn));
        let budget = Arc::new(Semaphore::new(config.max_in_flight_bytes));
        let mut worker = WorkerState::new(config, progress, stop_rx, out, budget);
        worker.install_startup_notifier(startup_tx);
        (worker, stop_tx)
    }

    fn startup_worker(
        startup_tx: oneshot::Sender<Result<()>>,
    ) -> (WorkerState, watch::Sender<bool>) {
        startup_worker_with_config(ReplicationConfig::default(), startup_tx)
    }

    #[tokio::test]
    async fn strict_startup_validates_exact_socket_before_copy_both_readiness() {
        let (startup_tx, mut startup_rx) = oneshot::channel();
        let mut config = ReplicationConfig::default();
        config.database = "orders".into();
        config.slot = "orders_slot".into();
        config.start_lsn = Lsn::from_u64(0x100);
        config.expected_recovery_identity = Some(crate::config::ExpectedRecoveryIdentity {
            system_identifier: 42,
            timeline_id: 7,
        });
        let (mut worker, stop_tx) = startup_worker_with_config(config, startup_tx);
        let (mut worker_stream, mut peer) = tokio::io::duplex(4096);
        let worker_task =
            tokio::spawn(async move { worker.run_on_stream(&mut worker_stream).await });

        read_frontend_message(&mut peer, false).await;
        write_backend_message(&mut peer, b'R', &0_i32.to_be_bytes()).await;
        write_backend_message(&mut peer, b'Z', b"I").await;

        let identify = read_frontend_message(&mut peer, true).await;
        assert_eq!(&identify[..identify.len() - 1], b"IDENTIFY_SYSTEM");
        write_backend_message(
            &mut peer,
            b'D',
            &text_data_row(&[Some("42"), Some("7"), Some("0/200"), Some("orders")]),
        )
        .await;
        write_backend_message(&mut peer, b'C', b"IDENTIFY_SYSTEM\0").await;
        write_backend_message(&mut peer, b'Z', b"I").await;

        let slot_query = read_frontend_message(&mut peer, true).await;
        let slot_query = std::str::from_utf8(&slot_query[..slot_query.len() - 1]).unwrap();
        assert!(slot_query.contains("confirmed_flush_lsn"), "{slot_query}");
        assert!(slot_query.contains("orders_slot"), "{slot_query}");
        write_backend_message(&mut peer, b'D', &text_data_row(&[Some("0/F0")])).await;
        write_backend_message(&mut peer, b'C', b"SELECT 1\0").await;
        write_backend_message(&mut peer, b'Z', b"I").await;

        let start = read_frontend_message(&mut peer, true).await;
        let start = std::str::from_utf8(&start[..start.len() - 1]).unwrap();
        assert!(start.starts_with("START_REPLICATION"), "{start}");
        assert!(start.contains("messages 'false'"), "{start}");
        assert!(matches!(
            startup_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        write_backend_message(&mut peer, b'W', &[]).await;
        startup_rx.await.unwrap().unwrap();

        stop_tx.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), worker_task)
            .await
            .expect("replication worker must stop")
            .unwrap()
            .unwrap();
    }

    #[test]
    fn exact_socket_identity_and_lsn_mismatches_fail_closed() {
        let mut config = ReplicationConfig::default();
        config.database = "orders".into();
        config.slot = "orders_slot".into();
        config.start_lsn = Lsn::from_u64(0x100);
        config.expected_recovery_identity = Some(crate::config::ExpectedRecoveryIdentity {
            system_identifier: 42,
            timeline_id: 7,
        });
        let valid = vec![
            Some("42".into()),
            Some("7".into()),
            Some("0/200".into()),
            Some("orders".into()),
        ];
        validate_identify_system(&config, &valid).unwrap();

        for (index, value, expected) in [
            (0, "43", "system identifier drifted"),
            (1, "8", "timeline drifted"),
            (3, "other", "connected to database"),
        ] {
            let mut row = valid.clone();
            row[index] = Some(value.into());
            let error = validate_identify_system(&config, &row).unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }

        let mut future = valid;
        future[2] = Some("0/FF".into());
        let error = validate_identify_system(&config, &future).unwrap_err();
        assert!(error.to_string().contains("ahead"), "{error}");

        validate_slot_cursor(&config, &[Some("0/100".into())]).unwrap();
        let error = validate_slot_cursor(&config, &[Some("0/101".into())]).unwrap_err();
        assert!(error.to_string().contains("advanced"), "{error}");
        let error = validate_slot_cursor(&config, &[None]).unwrap_err();
        assert!(error.to_string().contains("NULL"), "{error}");
    }

    #[tokio::test]
    async fn startup_readiness_waits_for_copy_both_response() {
        let (startup_tx, mut startup_rx) = oneshot::channel();
        let (mut worker, stop_tx) = startup_worker(startup_tx);
        let (mut worker_stream, mut peer) = tokio::io::duplex(4096);
        let worker_task =
            tokio::spawn(async move { worker.run_on_stream(&mut worker_stream).await });

        read_frontend_message(&mut peer, false).await;
        write_backend_message(&mut peer, b'R', &0_i32.to_be_bytes()).await;
        write_backend_message(&mut peer, b'Z', b"I").await;
        read_frontend_message(&mut peer, true).await;

        assert!(matches!(
            startup_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        write_backend_message(&mut peer, b'W', &[]).await;
        startup_rx.await.unwrap().unwrap();

        stop_tx.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), worker_task)
            .await
            .expect("replication worker must stop")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn startup_server_error_is_published_before_copy_both_response() {
        let (startup_tx, startup_rx) = oneshot::channel();
        let (mut worker, _stop_tx) = startup_worker(startup_tx);
        let (mut worker_stream, mut peer) = tokio::io::duplex(4096);
        let worker_task =
            tokio::spawn(async move { worker.run_on_stream(&mut worker_stream).await });

        read_frontend_message(&mut peer, false).await;
        write_backend_message(&mut peer, b'R', &0_i32.to_be_bytes()).await;
        write_backend_message(&mut peer, b'Z', b"I").await;
        read_frontend_message(&mut peer, true).await;
        write_backend_message(
            &mut peer,
            b'E',
            b"Mpublication missing_publication does not exist\0C42704\0\0",
        )
        .await;

        let startup_error = startup_rx.await.unwrap().unwrap_err();
        let worker_error = worker_task.await.unwrap().unwrap_err();
        assert!(startup_error.is_server());
        assert_eq!(startup_error.to_string(), worker_error.to_string());
        assert!(startup_error.to_string().contains("SQLSTATE 42704"));
    }

    #[test]
    fn parse_sasl_mechanisms_single() {
        let data = b"SCRAM-SHA-256\0\0";
        let mechs = parse_sasl_mechanisms(data);
        assert_eq!(mechs, vec!["SCRAM-SHA-256"]);
    }

    #[test]
    fn parse_sasl_mechanisms_multiple() {
        let data = b"SCRAM-SHA-256\0SCRAM-SHA-256-PLUS\0\0";
        let mechs = parse_sasl_mechanisms(data);
        assert_eq!(mechs, vec!["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"]);
    }

    #[test]
    fn parse_sasl_mechanisms_empty() {
        let mechs = parse_sasl_mechanisms(b"\0");
        assert!(mechs.is_empty());
    }

    #[test]
    fn pgoutput_boundaries_reject_trailing_bytes_and_unknown_flags() {
        let mut begin = Vec::from([b'B']);
        begin.extend_from_slice(&1_i64.to_be_bytes());
        begin.extend_from_slice(&2_i64.to_be_bytes());
        begin.extend_from_slice(&3_i32.to_be_bytes());
        assert!(parse_pgoutput_boundary(&Bytes::from(begin.clone()), None).is_ok());
        begin.push(0);
        assert!(parse_pgoutput_boundary(&Bytes::from(begin), None).is_err());

        let mut commit = Vec::from([b'C', 0]);
        commit.extend_from_slice(&1_i64.to_be_bytes());
        commit.extend_from_slice(&2_i64.to_be_bytes());
        commit.extend_from_slice(&3_i64.to_be_bytes());
        assert!(parse_pgoutput_boundary(&Bytes::from(commit.clone()), None).is_ok());
        commit[1] = 1;
        assert!(parse_pgoutput_boundary(&Bytes::from(commit), None).is_err());

        let mut message = Vec::from([b'M', 0]);
        message.extend_from_slice(&1_i64.to_be_bytes());
        message.extend_from_slice(b"prefix\0");
        message.extend_from_slice(&3_i32.to_be_bytes());
        message.extend_from_slice(b"abc");
        let permit = Arc::new(Semaphore::new(message.len()))
            .try_acquire_many_owned(message.len() as u32)
            .unwrap();
        let parsed = parse_pgoutput_boundary(
            &Bytes::from(message.clone()),
            Some(WireBytesGuard::new(permit)),
        );
        assert!(parsed.is_ok(), "{parsed:?}");
        message.push(0);
        assert!(parse_pgoutput_boundary(&Bytes::from(message.clone()), None).is_err());
        message[1] = 2;
        assert!(parse_pgoutput_boundary(&Bytes::from(message), None).is_err());
    }

    #[test]
    #[cfg(feature = "md5")]
    fn postgres_md5_known_value() {
        // Test vector: user="md5_user", password="md5_pass", salt=[0x01, 0x02, 0x03, 0x04]
        // Can verify with: SELECT 'md5' || md5(md5('md5_passmd5_user') || E'\\x01020304');
        let hash = postgres_md5("md5_pass", "md5_user", &[0x01, 0x02, 0x03, 0x04]);
        assert!(hash.starts_with("md5"));
        assert_eq!(hash.len(), 35); // "md5" + 32 hex chars
    }

    #[test]
    fn current_pg_timestamp_is_positive() {
        // Any time after 2000-01-01 should be positive
        let ts = current_pg_timestamp();
        assert!(ts > 0);
    }

    #[test]
    fn replication_query_uses_validated_replication_identifiers() {
        let config = ReplicationConfig::default();
        let query = start_replication_query(&config);

        assert!(query.contains("SLOT slot LOGICAL"), "{query}");
        assert!(query.contains("publication_names 'pub'"), "{query}");
        assert!(query.contains("messages 'false'"), "{query}");
    }

    #[tokio::test]
    async fn raw_event_reservation_survives_channel_receive_and_clone() {
        let budget = Arc::new(Semaphore::new(4));
        let permit = Arc::clone(&budget).acquire_many_owned(4).await.unwrap();
        let guard = WireBytesGuard::new(permit);
        let event = ReplicationEvent::XLogData {
            wal_start: Lsn::ZERO,
            wal_end: Lsn::ZERO,
            server_time_micros: 0,
            data: Bytes::from_static(b"data"),
            wire_bytes: guard,
        };
        let (tx, mut rx) = mpsc::channel::<Result<ReplicationEvent>>(1);

        tx.send(Ok(event)).await.unwrap();
        assert_eq!(budget.available_permits(), 0, "queued event owns bytes");
        let received = rx.recv().await.unwrap().unwrap();
        assert_eq!(budget.available_permits(), 0, "consumer owns bytes");
        let cloned = received.clone();
        drop(received);
        assert_eq!(
            budget.available_permits(),
            0,
            "shared backing allocation keeps one reservation"
        );
        drop(cloned);
        assert_eq!(budget.available_permits(), 4);
    }

    #[tokio::test]
    async fn full_event_channel_does_not_starve_standby_feedback() {
        use tokio::io::AsyncReadExt;

        let mut config = ReplicationConfig::default();
        config.status_interval = std::time::Duration::from_millis(10);
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(Ok(ReplicationEvent::KeepAlive {
            wal_end: Lsn::ZERO,
            reply_requested: false,
            server_time_micros: 0,
        }))
        .await
        .unwrap();
        let (_stop_tx, stop_rx) = watch::channel(false);
        let progress = Arc::new(SharedProgress::new(Lsn::ZERO));
        let budget = Arc::new(Semaphore::new(config.max_in_flight_bytes));
        let mut worker = WorkerState::new(config, progress, stop_rx, tx, budget);
        let (worker_stream, mut peer) = tokio::io::duplex(128);

        let handle = tokio::spawn(async move {
            let mut stream = BufReader::new(worker_stream);
            let mut applied = Lsn::ZERO;
            let mut last_status = Instant::now();
            worker
                .send_event_with_feedback(
                    &mut stream,
                    ReplicationEvent::KeepAlive {
                        wal_end: Lsn::ZERO,
                        reply_requested: false,
                        server_time_micros: 1,
                    },
                    &mut applied,
                    &mut last_status,
                )
                .await
        });

        let mut feedback = [0_u8; 39];
        tokio::time::timeout(
            std::time::Duration::from_millis(250),
            peer.read_exact(&mut feedback),
        )
        .await
        .expect("feedback must not wait for event-channel capacity")
        .unwrap();
        assert_eq!(feedback[0], b'd');
        assert_eq!(feedback[5], b'r');

        drop(rx.recv().await.unwrap());
        assert!(handle.await.unwrap().unwrap());
    }
}
