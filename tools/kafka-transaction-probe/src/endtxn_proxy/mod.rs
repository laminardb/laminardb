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

mod wire;

#[cfg(test)]
use wire::require_wire_length;
use wire::{
    lower_hex, parse_endtxn_v1, parse_endtxn_v1_response, parse_request_header,
    parse_response_correlation, read_frame, sha256_hex,
};

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

#[cfg(test)]
mod tests;
