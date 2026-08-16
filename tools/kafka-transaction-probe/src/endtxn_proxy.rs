//! Validation-only Kafka v1 `EndTxn` matched-fault actuator.
//!
//! This is intentionally not a general Kafka proxy. It supports one loopback PLAINTEXT broker,
//! preserves one downstream connection per upstream connection, forwards complete non-target
//! frames byte-for-byte during the active lifecycle, and parses only enough of request-header v1
//! and `EndTxn` v1 to prove the two ambiguity branches used by the standalone probe.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

const END_TXN_API_KEY: i16 = 26;
const REQUIRED_END_TXN_VERSION: i16 = 1;
const MAX_FRAME_PAYLOAD_BYTES: usize = 2 * 1024 * 1024;
const SOCKET_POLL: Duration = Duration::from_millis(100);

type ProxyResult<T> = Result<T, String>;

// This isolated validation executable intentionally stays std-only. The lock protects bounded
// validation transcript/correlation state outside LaminarDB's runtime hot path; adding a
// runtime-oriented lock crate would enlarge the evidence subject.
#[allow(clippy::disallowed_types)]
type ProbeMutex<T> = std::sync::Mutex<T>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultOutcome {
    AppliedResponseLost,
    UnappliedRequestHeld,
}

impl FaultOutcome {
    pub fn classification(self) -> &'static str {
        match self {
            Self::AppliedResponseLost => "FORWARDED_SUCCESS_RESPONSE_LOST",
            Self::UnappliedRequestHeld => "PRE_FORWARD_REJECTION",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetSpec {
    pub client_id: String,
    pub transactional_id: String,
    pub outcome: FaultOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndTxnEvidence {
    pub classification: &'static str,
    pub connection_id: u64,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub committed: bool,
    pub request_frame: Vec<u8>,
    pub request_sha256: String,
    pub request_upstream_bytes: usize,
    pub response_frame: Option<Vec<u8>>,
    pub response_sha256: Option<String>,
    pub response_throttle_ms: Option<i32>,
    pub response_error_code: Option<i16>,
    pub response_downstream_bytes: usize,
    pub events: Vec<String>,
}

impl EndTxnEvidence {
    pub fn request_hex(&self) -> String {
        lower_hex(&self.request_frame)
    }

    pub fn response_hex(&self) -> Option<String> {
        self.response_frame.as_deref().map(lower_hex)
    }
}

pub struct EndTxnProxy {
    listen: SocketAddr,
    shared: Arc<Shared>,
    accept_thread: Option<JoinHandle<()>>,
}

impl EndTxnProxy {
    pub fn start(
        listen: SocketAddr,
        upstream: SocketAddr,
        target: TargetSpec,
    ) -> ProxyResult<Self> {
        if !listen.ip().is_loopback()
            || !upstream.ip().is_loopback()
            || listen.port() == 0
            || upstream.port() == 0
            || listen == upstream
        {
            return Err("proxy endpoints must be distinct nonzero loopback sockets".to_owned());
        }
        if target.client_id.is_empty() || target.transactional_id.is_empty() {
            return Err("proxy target identities must be nonempty".to_owned());
        }
        let listener = TcpListener::bind(listen)
            .map_err(|error| format!("proxy bind {listen} failed: {error}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| format!("proxy nonblocking listener setup failed: {error}"))?;
        let actual = listener
            .local_addr()
            .map_err(|error| format!("proxy local address failed: {error}"))?;
        if actual != listen {
            return Err(format!(
                "proxy bound unexpected address {actual}, expected {listen}"
            ));
        }

        let shared = Arc::new(Shared::new(target));
        let thread_shared = Arc::clone(&shared);
        let accept_thread = thread::Builder::new()
            .name("ldb-endtxn-proxy-accept".to_owned())
            .spawn(move || accept_loop(listener, upstream, thread_shared))
            .map_err(|error| format!("proxy accept thread spawn failed: {error}"))?;
        Ok(Self {
            listen,
            shared,
            accept_thread: Some(accept_thread),
        })
    }

    pub fn arm(&self) -> ProxyResult<()> {
        self.shared.arm()
    }

    pub fn wait_for_actuation(&self, timeout: Duration) -> ProxyResult<()> {
        let outcome = self.shared.target.outcome;
        self.shared
            .wait_for(timeout, move |state| state.actuated(outcome))
    }

    pub fn wait_for_target_connections_closed(&self, timeout: Duration) -> ProxyResult<()> {
        self.shared.wait_for(timeout, |state| {
            state.request.is_some() && state.target_connections.is_empty()
        })
    }

    pub fn finish_target(&self) -> ProxyResult<EndTxnEvidence> {
        self.shared.finish_target()
    }

    pub fn shutdown(mut self) -> ProxyResult<()> {
        self.stop_and_join();
        self.shared.require_no_fatal()
    }

    fn stop_and_join(&mut self) {
        self.shared.stop.store(true, Ordering::SeqCst);
        self.shared.changed.notify_all();
        let _ = TcpStream::connect_timeout(&self.listen, SOCKET_POLL);
        if let Some(handle) = self.accept_thread.take() {
            if handle.join().is_err() {
                self.shared
                    .set_fatal("proxy accept thread panicked".to_owned());
            }
        }
    }
}

impl Drop for EndTxnProxy {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

struct Shared {
    target: TargetSpec,
    started: Instant,
    state: ProbeMutex<TargetState>,
    changed: Condvar,
    stop: AtomicBool,
    next_connection: AtomicU64,
}

impl Shared {
    fn new(target: TargetSpec) -> Self {
        Self {
            target,
            started: Instant::now(),
            state: ProbeMutex::new(TargetState::default()),
            changed: Condvar::new(),
            stop: AtomicBool::new(false),
            next_connection: AtomicU64::new(1),
        }
    }

    fn arm(&self) -> ProxyResult<()> {
        let mut state = self.lock_state()?;
        if state.armed || state.request.is_some() || state.finished {
            return Err("proxy target cannot be armed in its current state".to_owned());
        }
        state.armed = true;
        self.event(&mut state, "target-armed");
        self.changed.notify_all();
        Ok(())
    }

    fn wait_for(
        &self,
        timeout: Duration,
        predicate: impl Fn(&TargetState) -> bool,
    ) -> ProxyResult<()> {
        let deadline = Instant::now() + timeout;
        let mut state = self.lock_state()?;
        loop {
            if let Some(error) = &state.fatal {
                return Err(format!("proxy failed: {error}"));
            }
            if predicate(&state) {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(format!("proxy wait timed out; events={:?}", state.events));
            }
            let (next, wait) = self
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| "proxy state mutex poisoned while waiting".to_owned())?;
            state = next;
            if wait.timed_out() && !predicate(&state) {
                return Err(format!("proxy wait timed out; events={:?}", state.events));
            }
        }
    }

    fn finish_target(&self) -> ProxyResult<EndTxnEvidence> {
        let mut state = self.lock_state()?;
        if let Some(error) = &state.fatal {
            return Err(format!("proxy failed: {error}"));
        }
        if !state.actuated(self.target.outcome) {
            return Err("proxy target was not completely actuated".to_owned());
        }
        if !state.target_connections.is_empty() {
            return Err(format!(
                "target producer connections remain open: {:?}",
                state.target_connections
            ));
        }
        if state.finished {
            return Err("proxy target evidence was already finished".to_owned());
        }
        state.armed = false;
        state.finished = true;
        self.event(&mut state, "target-finished-after-client-close");
        let request = state
            .request
            .as_ref()
            .ok_or_else(|| "proxy target request missing".to_owned())?;
        let response = state.response.as_ref();
        Ok(EndTxnEvidence {
            classification: self.target.outcome.classification(),
            connection_id: request.connection_id,
            api_version: request.api_version,
            correlation_id: request.correlation_id,
            client_id: request.client_id.clone(),
            transactional_id: request.transactional_id.clone(),
            producer_id: request.producer_id,
            producer_epoch: request.producer_epoch,
            committed: request.committed,
            request_frame: request.frame.clone(),
            request_sha256: sha256_hex(&request.frame),
            request_upstream_bytes: state.request_upstream_bytes,
            response_frame: response.map(|value| value.frame.clone()),
            response_sha256: response.map(|value| sha256_hex(&value.frame)),
            response_throttle_ms: response.map(|value| value.throttle_ms),
            response_error_code: response.map(|value| value.error_code),
            response_downstream_bytes: state.response_downstream_bytes,
            events: state.events.clone(),
        })
    }

    fn classify_request(&self, connection_id: u64, frame: &[u8]) -> ProxyResult<RequestAction> {
        let header = parse_request_header(frame)?;
        let mut state = self.lock_state()?;
        if header.client_id.as_deref() == Some(self.target.client_id.as_str()) {
            if state.finished {
                let error = "target client sent traffic after evidence finalization".to_owned();
                state.fatal = Some(error.clone());
                self.changed.notify_all();
                return Err(error);
            }
            state.target_connections.insert(connection_id);
        }

        if header.api_key != END_TXN_API_KEY
            || header.client_id.as_deref() != Some(self.target.client_id.as_str())
        {
            return Ok(RequestAction::ForwardNormal {
                correlation_id: header.correlation_id,
            });
        }
        if !state.armed {
            if state.request.is_some() {
                let error = "target client sent a second EndTxn after matched actuation".to_owned();
                state.fatal = Some(error.clone());
                self.changed.notify_all();
                return Err(error);
            }
            return Ok(RequestAction::ForwardNormal {
                correlation_id: header.correlation_id,
            });
        }
        if state.request.is_some() {
            let error = "target client retried the selected EndTxn".to_owned();
            state.fatal = Some(error.clone());
            self.changed.notify_all();
            return Err(error);
        }
        if header.api_version != REQUIRED_END_TXN_VERSION {
            let error = format!(
                "target EndTxn version was {}, expected {}",
                header.api_version, REQUIRED_END_TXN_VERSION
            );
            state.fatal = Some(error.clone());
            self.changed.notify_all();
            return Err(error);
        }
        let parsed = parse_endtxn_v1(connection_id, frame, &header)?;
        if parsed.transactional_id != self.target.transactional_id || !parsed.committed {
            let error = format!(
                "armed EndTxn target mismatch: transactional_id={} committed={}",
                parsed.transactional_id, parsed.committed
            );
            state.fatal = Some(error.clone());
            self.changed.notify_all();
            return Err(error);
        }
        self.event(
            &mut state,
            &format!(
                "target-request-buffered connection={} correlation={} bytes={} sha256={}",
                connection_id,
                parsed.correlation_id,
                parsed.frame.len(),
                sha256_hex(&parsed.frame)
            ),
        );
        state.request = Some(parsed);
        match self.target.outcome {
            FaultOutcome::AppliedResponseLost => {
                self.changed.notify_all();
                Ok(RequestAction::ForwardTarget {
                    correlation_id: header.correlation_id,
                })
            }
            FaultOutcome::UnappliedRequestHeld => {
                self.event(&mut state, "target-request-upstream-bytes=0");
                self.changed.notify_all();
                Ok(RequestAction::SuppressTarget)
            }
        }
    }

    fn target_request_written(&self, bytes: usize) -> ProxyResult<()> {
        let mut state = self.lock_state()?;
        let expected = state
            .request
            .as_ref()
            .ok_or_else(|| "target write completed without target request".to_owned())?
            .frame
            .len();
        if bytes != expected || state.request_upstream_bytes != 0 {
            return Err(format!(
                "target upstream write mismatch: expected={expected} observed={bytes} previous={}",
                state.request_upstream_bytes
            ));
        }
        state.request_upstream_bytes = bytes;
        self.event(
            &mut state,
            &format!("target-request-upstream-write-complete bytes={bytes}"),
        );
        self.changed.notify_all();
        Ok(())
    }

    fn capture_target_response(&self, connection_id: u64, frame: Vec<u8>) -> ProxyResult<()> {
        let response = parse_endtxn_v1_response(connection_id, &frame)?;
        let mut state = self.lock_state()?;
        let request = state
            .request
            .as_ref()
            .ok_or_else(|| "target response arrived without request".to_owned())?;
        if request.connection_id != connection_id
            || request.correlation_id != response.correlation_id
        {
            return Err(format!(
                "target response correlation mismatch: request=({},{}) response=({},{})",
                request.connection_id,
                request.correlation_id,
                response.connection_id,
                response.correlation_id
            ));
        }
        if state.response.is_some() {
            return Err("duplicate target response".to_owned());
        }
        if response.error_code != 0 {
            return Err(format!(
                "target broker response rejected EndTxn with error {}",
                response.error_code
            ));
        }
        self.event(
            &mut state,
            &format!(
                "target-success-response-buffered connection={} correlation={} bytes={} sha256={}",
                connection_id,
                response.correlation_id,
                response.frame.len(),
                sha256_hex(&response.frame)
            ),
        );
        state.response = Some(response);
        state.response_downstream_bytes = 0;
        self.event(&mut state, "target-response-downstream-bytes=0");
        self.changed.notify_all();
        Ok(())
    }

    fn close_connection(&self, connection_id: u64) {
        if let Ok(mut state) = self.state.lock() {
            if state.target_connections.remove(&connection_id) {
                self.event(
                    &mut state,
                    &format!("target-client-connection-closed id={connection_id}"),
                );
            }
            self.changed.notify_all();
        }
    }

    fn set_fatal(&self, error: String) {
        if let Ok(mut state) = self.state.lock() {
            if state.fatal.is_none() {
                self.event(&mut state, &format!("fatal={error}"));
                state.fatal = Some(error);
            }
            self.changed.notify_all();
        }
    }

    fn require_no_fatal(&self) -> ProxyResult<()> {
        let state = self.lock_state()?;
        match &state.fatal {
            Some(error) => Err(format!("proxy failed: {error}")),
            None => Ok(()),
        }
    }

    fn lock_state(&self) -> ProxyResult<std::sync::MutexGuard<'_, TargetState>> {
        self.state
            .lock()
            .map_err(|_| "proxy state mutex poisoned".to_owned())
    }

    fn event(&self, state: &mut TargetState, description: &str) {
        let sequence = state.events.len() + 1;
        let elapsed_us = self.started.elapsed().as_micros();
        state
            .events
            .push(format!("{sequence}:{elapsed_us}us:{description}"));
    }
}

#[derive(Default)]
struct TargetState {
    armed: bool,
    finished: bool,
    request: Option<ParsedEndTxnRequest>,
    request_upstream_bytes: usize,
    response: Option<ParsedEndTxnResponse>,
    response_downstream_bytes: usize,
    target_connections: BTreeSet<u64>,
    fatal: Option<String>,
    events: Vec<String>,
}

impl TargetState {
    fn actuated(&self, outcome: FaultOutcome) -> bool {
        let Some(request) = &self.request else {
            return false;
        };
        match outcome {
            FaultOutcome::AppliedResponseLost => self.response.as_ref().is_some_and(|response| {
                self.request_upstream_bytes == request.frame.len()
                    && response.error_code == 0
                    && self.response_downstream_bytes == 0
            }),
            FaultOutcome::UnappliedRequestHeld => {
                self.request_upstream_bytes == 0 && self.response.is_none()
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RequestHeader {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
    body_offset: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedEndTxnRequest {
    connection_id: u64,
    api_version: i16,
    correlation_id: i32,
    client_id: String,
    transactional_id: String,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
    frame: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedEndTxnResponse {
    connection_id: u64,
    correlation_id: i32,
    throttle_ms: i32,
    error_code: i16,
    frame: Vec<u8>,
}

enum RequestAction {
    ForwardNormal { correlation_id: i32 },
    ForwardTarget { correlation_id: i32 },
    SuppressTarget,
}

#[derive(Clone, Copy)]
enum PendingKind {
    Normal,
    Target,
}

fn accept_loop(listener: TcpListener, upstream: SocketAddr, shared: Arc<Shared>) {
    let mut handlers = Vec::new();
    while !shared.stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((downstream, _)) => {
                if shared.stop.load(Ordering::SeqCst) {
                    let _ = downstream.shutdown(Shutdown::Both);
                    break;
                }
                let connection_id = shared.next_connection.fetch_add(1, Ordering::SeqCst);
                let connection_shared = Arc::clone(&shared);
                match thread::Builder::new()
                    .name(format!("ldb-endtxn-proxy-{connection_id}"))
                    .spawn(move || {
                        if let Err(error) = handle_connection(
                            connection_id,
                            downstream,
                            upstream,
                            Arc::clone(&connection_shared),
                        ) {
                            if !connection_shared.stop.load(Ordering::SeqCst) {
                                connection_shared.set_fatal(format!(
                                    "connection {connection_id} failed: {error}"
                                ));
                            }
                        }
                        connection_shared.close_connection(connection_id);
                    }) {
                    Ok(handle) => handlers.push(handle),
                    Err(error) => shared.set_fatal(format!(
                        "connection {connection_id} thread spawn failed: {error}"
                    )),
                }
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => {
                shared.set_fatal(format!("proxy accept failed: {error}"));
                break;
            }
        }
    }
    shared.stop.store(true, Ordering::SeqCst);
    for handle in handlers {
        if handle.join().is_err() {
            shared.set_fatal("proxy connection thread panicked".to_owned());
        }
    }
}

fn handle_connection(
    connection_id: u64,
    downstream: TcpStream,
    upstream_addr: SocketAddr,
    shared: Arc<Shared>,
) -> ProxyResult<()> {
    configure_stream(&downstream)?;
    let upstream = TcpStream::connect_timeout(&upstream_addr, Duration::from_secs(2))
        .map_err(|error| format!("upstream connect {upstream_addr} failed: {error}"))?;
    configure_stream(&upstream)?;

    let downstream_read = downstream
        .try_clone()
        .map_err(|error| format!("downstream clone failed: {error}"))?;
    let upstream_write = upstream
        .try_clone()
        .map_err(|error| format!("upstream clone failed: {error}"))?;
    let pending = Arc::new(ProbeMutex::new(BTreeMap::<i32, PendingKind>::new()));
    let closed = Arc::new(AtomicBool::new(false));

    let request_shared = Arc::clone(&shared);
    let request_pending = Arc::clone(&pending);
    let request_closed = Arc::clone(&closed);
    let request_thread = thread::Builder::new()
        .name(format!("ldb-endtxn-proxy-request-{connection_id}"))
        .spawn(move || {
            let result = request_pump(
                connection_id,
                downstream_read,
                upstream_write,
                request_pending,
                Arc::clone(&request_shared),
                Arc::clone(&request_closed),
            );
            request_closed.store(true, Ordering::SeqCst);
            result
        })
        .map_err(|error| format!("request pump spawn failed: {error}"))?;

    let response_result = response_pump(
        connection_id,
        upstream,
        downstream,
        pending,
        Arc::clone(&shared),
        Arc::clone(&closed),
    );
    closed.store(true, Ordering::SeqCst);
    let request_result = request_thread
        .join()
        .map_err(|_| "request pump panicked".to_owned())?;
    request_result?;
    response_result
}

fn request_pump(
    connection_id: u64,
    mut downstream: TcpStream,
    mut upstream: TcpStream,
    pending: Arc<ProbeMutex<BTreeMap<i32, PendingKind>>>,
    shared: Arc<Shared>,
    closed: Arc<AtomicBool>,
) -> ProxyResult<()> {
    while !should_stop(&shared, &closed) {
        let Some(frame) = read_frame(&mut downstream, &shared.stop, &closed)? else {
            break;
        };
        match shared.classify_request(connection_id, &frame)? {
            RequestAction::SuppressTarget => {}
            RequestAction::ForwardNormal { correlation_id } => {
                insert_pending(&pending, correlation_id, PendingKind::Normal)?;
                upstream
                    .write_all(&frame)
                    .map_err(|error| format!("normal upstream write failed: {error}"))?;
            }
            RequestAction::ForwardTarget { correlation_id } => {
                insert_pending(&pending, correlation_id, PendingKind::Target)?;
                upstream
                    .write_all(&frame)
                    .map_err(|error| format!("target upstream write failed: {error}"))?;
                shared.target_request_written(frame.len())?;
            }
        }
    }
    let _ = upstream.shutdown(Shutdown::Write);
    Ok(())
}

fn response_pump(
    connection_id: u64,
    mut upstream: TcpStream,
    mut downstream: TcpStream,
    pending: Arc<ProbeMutex<BTreeMap<i32, PendingKind>>>,
    shared: Arc<Shared>,
    closed: Arc<AtomicBool>,
) -> ProxyResult<()> {
    while !should_stop(&shared, &closed) {
        let Some(frame) = read_frame(&mut upstream, &shared.stop, &closed)? else {
            break;
        };
        let correlation_id = parse_response_correlation(&frame)?;
        let kind = pending
            .lock()
            .map_err(|_| "pending-response mutex poisoned".to_owned())?
            .remove(&correlation_id)
            .ok_or_else(|| format!("unknown response correlation {correlation_id}"))?;
        match kind {
            PendingKind::Normal => downstream
                .write_all(&frame)
                .map_err(|error| format!("normal downstream write failed: {error}"))?,
            PendingKind::Target => {
                shared.capture_target_response(connection_id, frame)?;
            }
        }
    }
    let _ = downstream.shutdown(Shutdown::Write);
    Ok(())
}

fn insert_pending(
    pending: &ProbeMutex<BTreeMap<i32, PendingKind>>,
    correlation_id: i32,
    kind: PendingKind,
) -> ProxyResult<()> {
    let previous = pending
        .lock()
        .map_err(|_| "pending-request mutex poisoned".to_owned())?
        .insert(correlation_id, kind);
    if previous.is_some() {
        return Err(format!(
            "duplicate in-flight correlation {correlation_id} on one connection"
        ));
    }
    Ok(())
}

fn configure_stream(stream: &TcpStream) -> ProxyResult<()> {
    stream
        .set_nodelay(true)
        .map_err(|error| format!("TCP_NODELAY setup failed: {error}"))?;
    stream
        .set_read_timeout(Some(SOCKET_POLL))
        .map_err(|error| format!("read-timeout setup failed: {error}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| format!("write-timeout setup failed: {error}"))
}

fn should_stop(shared: &Shared, closed: &AtomicBool) -> bool {
    shared.stop.load(Ordering::SeqCst) || closed.load(Ordering::SeqCst)
}

fn read_frame(
    stream: &mut TcpStream,
    global_stop: &AtomicBool,
    connection_stop: &AtomicBool,
) -> ProxyResult<Option<Vec<u8>>> {
    let mut prefix = [0_u8; 4];
    if !read_exact_poll(stream, &mut prefix, global_stop, connection_stop)? {
        return Ok(None);
    }
    let signed = i32::from_be_bytes(prefix);
    let length =
        usize::try_from(signed).map_err(|_| format!("Kafka frame had negative length {signed}"))?;
    if length == 0 || length > MAX_FRAME_PAYLOAD_BYTES {
        return Err(format!(
            "Kafka frame payload length {length} is outside 1..={MAX_FRAME_PAYLOAD_BYTES}"
        ));
    }
    let mut frame = Vec::with_capacity(4 + length);
    frame.extend_from_slice(&prefix);
    frame.resize(4 + length, 0);
    if !read_exact_poll(stream, &mut frame[4..], global_stop, connection_stop)? {
        if global_stop.load(Ordering::SeqCst) || connection_stop.load(Ordering::SeqCst) {
            return Ok(None);
        }
        return Err("Kafka frame ended after its length prefix".to_owned());
    }
    Ok(Some(frame))
}

fn read_exact_poll(
    stream: &mut TcpStream,
    buffer: &mut [u8],
    global_stop: &AtomicBool,
    connection_stop: &AtomicBool,
) -> ProxyResult<bool> {
    let mut read = 0;
    while read < buffer.len() {
        if global_stop.load(Ordering::SeqCst) || connection_stop.load(Ordering::SeqCst) {
            return Ok(false);
        }
        match stream.read(&mut buffer[read..]) {
            Ok(0) if read == 0 => return Ok(false),
            Ok(0) => return Err(format!("TCP EOF after {read}/{} bytes", buffer.len())),
            Ok(count) => read += count,
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(error) => return Err(format!("TCP read failed: {error}")),
        }
    }
    Ok(true)
}

fn parse_request_header(frame: &[u8]) -> ProxyResult<RequestHeader> {
    require_wire_length(frame)?;
    if frame.len() < 14 {
        return Err("Kafka request frame is shorter than request-header v1".to_owned());
    }
    let api_key = read_i16(frame, 4)?;
    let api_version = read_i16(frame, 6)?;
    let correlation_id = read_i32(frame, 8)?;
    let client_length = read_i16(frame, 12)?;
    let (client_id, body_offset) = match client_length {
        -1 => (None, 14),
        value if value >= 0 => {
            let length = usize::try_from(value).map_err(|_| "client ID length overflow")?;
            let end = 14_usize
                .checked_add(length)
                .ok_or_else(|| "client ID bound overflow".to_owned())?;
            let bytes = frame
                .get(14..end)
                .ok_or_else(|| "client ID exceeds Kafka request frame".to_owned())?;
            let value = std::str::from_utf8(bytes)
                .map_err(|_| "client ID was not UTF-8".to_owned())?
                .to_owned();
            (Some(value), end)
        }
        value => return Err(format!("invalid nullable client ID length {value}")),
    };
    Ok(RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
        body_offset,
    })
}

fn parse_endtxn_v1(
    connection_id: u64,
    frame: &[u8],
    header: &RequestHeader,
) -> ProxyResult<ParsedEndTxnRequest> {
    if header.api_key != END_TXN_API_KEY || header.api_version != REQUIRED_END_TXN_VERSION {
        return Err("selected request is not EndTxn v1".to_owned());
    }
    let transactional_length = read_i16(frame, header.body_offset)?;
    if transactional_length <= 0 {
        return Err(format!(
            "EndTxn transactional ID length must be positive, got {transactional_length}"
        ));
    }
    let length = usize::try_from(transactional_length)
        .map_err(|_| "transactional ID length overflow".to_owned())?;
    let start = header.body_offset + 2;
    let end = start
        .checked_add(length)
        .ok_or_else(|| "transactional ID bound overflow".to_owned())?;
    let transactional_id = std::str::from_utf8(
        frame
            .get(start..end)
            .ok_or_else(|| "transactional ID exceeds EndTxn frame".to_owned())?,
    )
    .map_err(|_| "transactional ID was not UTF-8".to_owned())?
    .to_owned();
    let producer_id = read_i64(frame, end)?;
    let producer_epoch = read_i16(frame, end + 8)?;
    let committed = match frame.get(end + 10) {
        Some(0) => false,
        Some(1) => true,
        Some(value) => {
            return Err(format!(
                "EndTxn committed byte was {value}, expected 0 or 1"
            ))
        }
        None => return Err("EndTxn committed byte is missing".to_owned()),
    };
    if end + 11 != frame.len() {
        return Err(format!(
            "EndTxn v1 frame had {} trailing bytes",
            frame.len().saturating_sub(end + 11)
        ));
    }
    Ok(ParsedEndTxnRequest {
        connection_id,
        api_version: header.api_version,
        correlation_id: header.correlation_id,
        client_id: header
            .client_id
            .clone()
            .ok_or_else(|| "target EndTxn client ID was null".to_owned())?,
        transactional_id,
        producer_id,
        producer_epoch,
        committed,
        frame: frame.to_vec(),
    })
}

fn parse_response_correlation(frame: &[u8]) -> ProxyResult<i32> {
    require_wire_length(frame)?;
    if frame.len() < 8 {
        return Err("Kafka response frame is shorter than correlation header".to_owned());
    }
    read_i32(frame, 4)
}

fn parse_endtxn_v1_response(connection_id: u64, frame: &[u8]) -> ProxyResult<ParsedEndTxnResponse> {
    require_wire_length(frame)?;
    if frame.len() != 14 || read_i32(frame, 0)? != 10 {
        return Err(format!(
            "EndTxn v1 response must be exactly 14 wire bytes, got {}",
            frame.len()
        ));
    }
    Ok(ParsedEndTxnResponse {
        connection_id,
        correlation_id: read_i32(frame, 4)?,
        throttle_ms: read_i32(frame, 8)?,
        error_code: read_i16(frame, 12)?,
        frame: frame.to_vec(),
    })
}

fn require_wire_length(frame: &[u8]) -> ProxyResult<()> {
    if frame.len() < 4 {
        return Err("Kafka frame is shorter than its length prefix".to_owned());
    }
    let signed = read_i32(frame, 0)?;
    let length =
        usize::try_from(signed).map_err(|_| format!("Kafka frame had negative length {signed}"))?;
    if length == 0 || length > MAX_FRAME_PAYLOAD_BYTES || frame.len() != length + 4 {
        return Err(format!(
            "Kafka frame length mismatch: prefix={length} wire={}",
            frame.len()
        ));
    }
    Ok(())
}

fn read_i16(bytes: &[u8], offset: usize) -> ProxyResult<i16> {
    let value = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| format!("missing i16 at offset {offset}"))?;
    Ok(i16::from_be_bytes([value[0], value[1]]))
}

fn read_i32(bytes: &[u8], offset: usize) -> ProxyResult<i32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| format!("missing i32 at offset {offset}"))?;
    Ok(i32::from_be_bytes([value[0], value[1], value[2], value[3]]))
}

fn read_i64(bytes: &[u8], offset: usize) -> ProxyResult<i64> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| format!("missing i64 at offset {offset}"))?;
    Ok(i64::from_be_bytes([
        value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
    ]))
}

fn sha256_hex(bytes: &[u8]) -> String {
    lower_hex(&Sha256::digest(bytes))
}

fn lower_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLIENT: &str = "ldb-kafka-ambiguity-marker-applied-a";
    const TX_ID: &str =
        "ldb.tx.v1.c49ace6d02eb21ec7a2dc4424d8c3b9680fc3cd828cd754fec079b800a37411a";

    fn request(correlation: i32, version: i16, committed: u8) -> Vec<u8> {
        request_for(CLIENT, TX_ID, correlation, version, committed)
    }

    fn request_for(
        client_id: &str,
        transactional_id: &str,
        correlation: i32,
        version: i16,
        committed: u8,
    ) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&END_TXN_API_KEY.to_be_bytes());
        body.extend_from_slice(&version.to_be_bytes());
        body.extend_from_slice(&correlation.to_be_bytes());
        body.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
        body.extend_from_slice(client_id.as_bytes());
        body.extend_from_slice(&(transactional_id.len() as i16).to_be_bytes());
        body.extend_from_slice(transactional_id.as_bytes());
        body.extend_from_slice(&42_i64.to_be_bytes());
        body.extend_from_slice(&7_i16.to_be_bytes());
        body.push(committed);
        let mut frame = Vec::new();
        frame.extend_from_slice(&(body.len() as i32).to_be_bytes());
        frame.extend_from_slice(&body);
        frame
    }

    fn response(correlation: i32, error: i16) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&10_i32.to_be_bytes());
        frame.extend_from_slice(&correlation.to_be_bytes());
        frame.extend_from_slice(&0_i32.to_be_bytes());
        frame.extend_from_slice(&error.to_be_bytes());
        frame
    }

    fn target(outcome: FaultOutcome) -> Arc<Shared> {
        Arc::new(Shared::new(TargetSpec {
            client_id: CLIENT.to_owned(),
            transactional_id: TX_ID.to_owned(),
            outcome,
        }))
    }

    #[test]
    fn exact_endtxn_v1_request_and_response_parse() {
        let frame = request(123, 1, 1);
        let header = parse_request_header(&frame).unwrap();
        let parsed = parse_endtxn_v1(9, &frame, &header).unwrap();
        assert_eq!(parsed.connection_id, 9);
        assert_eq!(parsed.api_version, 1);
        assert_eq!(parsed.correlation_id, 123);
        assert_eq!(parsed.client_id, CLIENT);
        assert_eq!(parsed.transactional_id, TX_ID);
        assert_eq!(parsed.producer_id, 42);
        assert_eq!(parsed.producer_epoch, 7);
        assert!(parsed.committed);

        let parsed_response = parse_endtxn_v1_response(9, &response(123, 0)).unwrap();
        assert_eq!(parsed_response.connection_id, 9);
        assert_eq!(parsed_response.correlation_id, 123);
        assert_eq!(parsed_response.throttle_ms, 0);
        assert_eq!(parsed_response.error_code, 0);
    }

    #[test]
    fn parsers_reject_version_commit_length_and_response_drift() {
        let version = request(1, 2, 1);
        let header = parse_request_header(&version).unwrap();
        assert!(parse_endtxn_v1(1, &version, &header).is_err());

        let commit = request(1, 1, 2);
        let header = parse_request_header(&commit).unwrap();
        assert!(parse_endtxn_v1(1, &commit, &header).is_err());

        let mut trailing = request(1, 1, 1);
        trailing.push(0);
        let length = i32::try_from(trailing.len() - 4).unwrap();
        trailing[..4].copy_from_slice(&length.to_be_bytes());
        let header = parse_request_header(&trailing).unwrap();
        assert!(parse_endtxn_v1(1, &trailing, &header).is_err());

        let mut bad_response = response(1, 0);
        bad_response[0..4].copy_from_slice(&9_i32.to_be_bytes());
        assert!(parse_endtxn_v1_response(1, &bad_response).is_err());
    }

    #[test]
    fn applied_and_unapplied_state_require_exact_byte_dispositions() {
        let applied = target(FaultOutcome::AppliedResponseLost);
        applied.arm().unwrap();
        let frame = request(11, 1, 1);
        assert!(matches!(
            applied.classify_request(3, &frame).unwrap(),
            RequestAction::ForwardTarget { correlation_id: 11 }
        ));
        applied.target_request_written(frame.len()).unwrap();
        applied.capture_target_response(3, response(11, 0)).unwrap();
        assert!(applied
            .state
            .lock()
            .unwrap()
            .actuated(FaultOutcome::AppliedResponseLost));

        let unapplied = target(FaultOutcome::UnappliedRequestHeld);
        unapplied.arm().unwrap();
        assert!(matches!(
            unapplied.classify_request(4, &request(12, 1, 1)).unwrap(),
            RequestAction::SuppressTarget
        ));
        let state = unapplied.state.lock().unwrap();
        assert!(state.actuated(FaultOutcome::UnappliedRequestHeld));
        assert_eq!(state.request_upstream_bytes, 0);
        assert!(state.response.is_none());
    }

    #[test]
    fn retry_and_wrong_target_fail_closed() {
        let shared = target(FaultOutcome::UnappliedRequestHeld);
        shared.arm().unwrap();
        let frame = request(13, 1, 1);
        shared.classify_request(1, &frame).unwrap();
        assert!(shared.classify_request(1, &frame).is_err());

        let wrong = target(FaultOutcome::UnappliedRequestHeld);
        wrong.arm().unwrap();
        let abort = request(14, 1, 0);
        assert!(wrong.classify_request(1, &abort).is_err());
    }

    #[test]
    fn target_identity_version_response_and_correlation_drift_fail_closed() {
        let wrong_client = target(FaultOutcome::UnappliedRequestHeld);
        wrong_client.arm().unwrap();
        assert!(matches!(
            wrong_client
                .classify_request(1, &request_for("other-client", TX_ID, 16, 1, 1))
                .unwrap(),
            RequestAction::ForwardNormal { correlation_id: 16 }
        ));
        assert!(wrong_client.state.lock().unwrap().request.is_none());

        let wrong_tx = target(FaultOutcome::UnappliedRequestHeld);
        wrong_tx.arm().unwrap();
        assert!(wrong_tx
            .classify_request(1, &request_for(CLIENT, &"f".repeat(74), 17, 1, 1))
            .is_err());

        let wrong_version = target(FaultOutcome::UnappliedRequestHeld);
        wrong_version.arm().unwrap();
        assert!(wrong_version
            .classify_request(1, &request(18, 3, 1))
            .is_err());

        let mismatch = target(FaultOutcome::AppliedResponseLost);
        mismatch.arm().unwrap();
        let frame = request(19, 1, 1);
        mismatch.classify_request(1, &frame).unwrap();
        mismatch.target_request_written(frame.len()).unwrap();
        assert!(mismatch
            .capture_target_response(1, response(20, 0))
            .is_err());

        let rejected = target(FaultOutcome::AppliedResponseLost);
        rejected.arm().unwrap();
        let frame = request(20, 1, 1);
        rejected.classify_request(1, &frame).unwrap();
        rejected.target_request_written(frame.len()).unwrap();
        assert!(rejected
            .capture_target_response(1, response(20, 47))
            .is_err());

        let partial_write = target(FaultOutcome::AppliedResponseLost);
        partial_write.arm().unwrap();
        let frame = request(21, 1, 1);
        partial_write.classify_request(1, &frame).unwrap();
        assert!(partial_write
            .target_request_written(frame.len() - 1)
            .is_err());

        let pending = ProbeMutex::new(BTreeMap::new());
        insert_pending(&pending, 22, PendingKind::Normal).unwrap();
        assert!(insert_pending(&pending, 22, PendingKind::Target).is_err());
    }

    #[test]
    fn frame_bounds_reject_negative_zero_partial_and_trailing_lengths() {
        for frame in [
            (-1_i32).to_be_bytes().to_vec(),
            0_i32.to_be_bytes().to_vec(),
            vec![0, 0, 0, 10, 0],
            vec![0, 0, 0, 1, 0, 0],
        ] {
            assert!(require_wire_length(&frame).is_err());
        }
    }

    #[test]
    fn target_client_traffic_after_finish_is_fatal() {
        let shared = target(FaultOutcome::UnappliedRequestHeld);
        shared.arm().unwrap();
        shared.classify_request(1, &request(21, 1, 1)).unwrap();
        shared.close_connection(1);
        shared.finish_target().unwrap();

        let mut late = request(22, 1, 1);
        late[4..6].copy_from_slice(&3_i16.to_be_bytes());
        assert!(shared.classify_request(2, &late).is_err());
        assert!(shared.state.lock().unwrap().fatal.is_some());
    }

    #[test]
    fn framed_reader_handles_byte_fragmentation_and_rejects_oversize() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let frame = request(15, 1, 1);
        let writer_frame = frame.clone();
        let writer = thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            for byte in writer_frame {
                stream.write_all(&[byte]).unwrap();
            }
        });
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(SOCKET_POLL)).unwrap();
        let global = AtomicBool::new(false);
        let local = AtomicBool::new(false);
        assert_eq!(
            read_frame(&mut stream, &global, &local).unwrap(),
            Some(frame)
        );
        writer.join().unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let writer = thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            stream
                .write_all(&((MAX_FRAME_PAYLOAD_BYTES as i32) + 1).to_be_bytes())
                .unwrap();
        });
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(SOCKET_POLL)).unwrap();
        assert!(read_frame(&mut stream, &global, &local).is_err());
        writer.join().unwrap();
    }

    #[test]
    fn framed_reader_distinguishes_teardown_cancellation_from_partial_eof() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let writer = thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            stream.write_all(&10_i32.to_be_bytes()).unwrap();
            stream.write_all(&[0]).unwrap();
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(SOCKET_POLL)).unwrap();
        ready_rx.recv().unwrap();
        let global = Arc::new(AtomicBool::new(false));
        let local = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&local);
        let stopper = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            stop.store(true, Ordering::SeqCst);
        });
        let result = read_frame(&mut stream, &global, &local);
        stopper.join().unwrap();
        release_tx.send(()).unwrap();
        writer.join().unwrap();
        assert_eq!(result.unwrap(), None);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let writer = thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            stream.write_all(&10_i32.to_be_bytes()).unwrap();
            stream.write_all(&[0]).unwrap();
        });
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(SOCKET_POLL)).unwrap();
        let global = AtomicBool::new(false);
        let local = AtomicBool::new(false);
        assert!(read_frame(&mut stream, &global, &local).is_err());
        writer.join().unwrap();
    }
}
